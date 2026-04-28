using System.Collections.Concurrent;
using Google.Cloud.BigQuery.Storage.V1;
using Google.Protobuf;
using Grpc.Core;

namespace BigQuery.InMemoryEmulator.StorageApi;

/// <summary>
/// Custom gRPC <see cref="CallInvoker"/> that intercepts BigQuery Storage Read API calls
/// and serves data from an <see cref="InMemoryDataStore"/>.
/// </summary>
/// <remarks>
/// Ref: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1
///   "BigQuery Read API — The Read API can be used to read data from BigQuery."
///
/// This interceptor handles three RPCs:
///   - CreateReadSession — creates a session with streams for reading table data
///   - ReadRows — streams rows from a given read stream
///   - SplitReadStream — splits a stream into two child streams
///
/// Instead of going over the network, all calls read directly from the in-memory data store.
/// Uses the <see cref="CallInvoker"/> extension point so that the standard
/// <see cref="BigQueryReadClient"/> wrapper (via <see cref="BigQueryReadClientBuilder"/>)
/// creates proper <see cref="BigQueryReadClient.ReadRowsStream"/> instances — no reflection required.
/// </remarks>
internal sealed class InMemoryBigQueryReadCallInvoker : CallInvoker
{
	private readonly InMemoryDataStore _store;

	/// <summary>
	/// Tracks active read sessions: sessionName → session metadata.
	/// </summary>
	private readonly ConcurrentDictionary<string, ReadSessionState> _sessions = new();

	private int _sessionCounter;

	public InMemoryBigQueryReadCallInvoker(InMemoryDataStore store)
	{
		_store = store ?? throw new ArgumentNullException(nameof(store));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryRead.CreateReadSession
	//   "Creates a new read session."
	public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
		Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
	{
		object response = method.Name switch
		{
			"CreateReadSession" => HandleCreateReadSession((CreateReadSessionRequest)(object)request),
			"SplitReadStream" => HandleSplitReadStream((SplitReadStreamRequest)(object)request),
			_ => throw new RpcException(new Status(StatusCode.Unimplemented, $"Method not implemented: {method.Name}"))
		};

		return new AsyncUnaryCall<TResponse>(
			Task.FromResult((TResponse)response),
			Task.FromResult(new Metadata()),
			() => Status.DefaultSuccess,
			() => [],
			() => { });
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryRead.ReadRows
	//   "Reads rows from the stream in the format prescribed by the ReadSession."
	public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
		Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
	{
		if (method.Name != "ReadRows")
			throw new RpcException(new Status(StatusCode.Unimplemented, $"Method not implemented: {method.Name}"));

		var readRowsRequest = (ReadRowsRequest)(object)request;
		var responseStream = CreateReadRowsStream(readRowsRequest);

		return new AsyncServerStreamingCall<TResponse>(
			new InMemoryAsyncStreamReader<TResponse>(responseStream.Cast<TResponse>()),
			Task.FromResult(new Metadata()),
			() => Status.DefaultSuccess,
			() => [],
			() => { });
	}

	public override TResponse BlockingUnaryCall<TRequest, TResponse>(
		Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
	{
		// Forward to async implementation
		return AsyncUnaryCall(method, host, options, request).ResponseAsync.GetAwaiter().GetResult();
	}

	public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
		Method<TRequest, TResponse> method, string? host, CallOptions options)
	{
		throw new RpcException(new Status(StatusCode.Unimplemented, "Client streaming is not supported"));
	}

	public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
		Method<TRequest, TResponse> method, string? host, CallOptions options)
	{
		throw new RpcException(new Status(StatusCode.Unimplemented, "Duplex streaming is not supported"));
	}

	private ReadSession HandleCreateReadSession(CreateReadSessionRequest request)
	{
		var readSession = request.ReadSession;
		var tableName = readSession.Table;

		// Ref: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readsession
		//   "Table should be of the form: projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
		var parts = tableName.Split('/');
		if (parts.Length < 6)
			throw new RpcException(new Status(StatusCode.InvalidArgument,
				$"Invalid table reference: {tableName}. Expected format: projects/{{project}}/datasets/{{dataset}}/tables/{{table}}"));

		var datasetId = parts[3];
		var tableId = parts[5];

		if (!_store.Datasets.TryGetValue(datasetId, out var dataset))
			throw new RpcException(new Status(StatusCode.NotFound, $"Dataset not found: {datasetId}"));

		if (!dataset.Tables.TryGetValue(tableId, out var table))
			throw new RpcException(new Status(StatusCode.NotFound, $"Table not found: {datasetId}.{tableId}"));

		// Determine stream count
		int rowCount;
		lock (table.RowLock)
		{
			rowCount = table.Rows.Count;
		}

		var maxStreams = request.MaxStreamCount > 0 ? request.MaxStreamCount : 1;
		var streamCount = Math.Min(maxStreams, Math.Max(1, rowCount));

		// Create session
		var sessionId = Interlocked.Increment(ref _sessionCounter);
		var sessionName = $"projects/{_store.ProjectId}/locations/us/sessions/{sessionId}";

		// Determine selected fields
		var selectedFields = readSession.ReadOptions?.SelectedFields;
		var rowFilter = readSession.ReadOptions?.RowRestriction;

		// Build Avro schema — filtered to selected fields if specified
		var effectiveSchema = selectedFields is { Count: > 0 }
			? FilterSchema(table.Schema, selectedFields.ToArray())
			: table.Schema;
		var avroSchemaJson = AvroSerializer.ToAvroSchemaJson(effectiveSchema);

		var session = new ReadSession
		{
			Name = sessionName,
			Table = tableName,
			AvroSchema = new AvroSchema { Schema = avroSchemaJson },
			DataFormat = DataFormat.Avro,
		};

		// Create stream assignments: partition rows evenly
		var streamStates = new List<StreamState>();
		var rowsPerStream = rowCount > 0 ? (int)Math.Ceiling((double)rowCount / streamCount) : 0;

		for (int i = 0; i < streamCount; i++)
		{
			var start = i * rowsPerStream;
			var end = Math.Min(start + rowsPerStream, rowCount);
			if (start >= rowCount && i > 0) break;

			var streamName = $"{sessionName}/streams/{i}";
			session.Streams.Add(new ReadStream { Name = streamName });
			streamStates.Add(new StreamState(streamName, start, end));
		}

		// If no rows but we need at least one stream for schema discovery
		if (session.Streams.Count == 0)
		{
			var streamName = $"{sessionName}/streams/0";
			session.Streams.Add(new ReadStream { Name = streamName });
			streamStates.Add(new StreamState(streamName, 0, 0));
		}

		var sessionState = new ReadSessionState(
			datasetId, tableId, table.Schema,
			streamStates.ToArray(),
			selectedFields?.ToArray(),
			rowFilter);

		_sessions[sessionName] = sessionState;

		return session;
	}

	private List<ReadRowsResponse> CreateReadRowsStream(ReadRowsRequest request)
	{
		var streamName = request.ReadStream;
		var offset = request.Offset;

		// Find the session for this stream
		var sessionName = streamName[..streamName.LastIndexOf("/streams/", StringComparison.Ordinal)];
		if (!_sessions.TryGetValue(sessionName, out var sessionState))
			throw new RpcException(new Status(StatusCode.NotFound, $"Session not found for stream: {streamName}"));

		var streamState = Array.Find(sessionState.Streams, s => s.Name == streamName);
		if (streamState is null)
			throw new RpcException(new Status(StatusCode.NotFound, $"Stream not found: {streamName}"));

		if (!_store.Datasets.TryGetValue(sessionState.DatasetId, out var dataset))
			throw new RpcException(new Status(StatusCode.NotFound, $"Dataset not found: {sessionState.DatasetId}"));

		if (!dataset.Tables.TryGetValue(sessionState.TableId, out var table))
			throw new RpcException(new Status(StatusCode.NotFound, $"Table not found: {sessionState.TableId}"));

		// Get relevant rows
		List<InMemoryRow> rows;
		lock (table.RowLock)
		{
			rows = table.Rows
				.Skip(streamState.StartRow)
				.Take(streamState.EndRow - streamState.StartRow)
				.Skip((int)offset)
				.ToList();
		}

		// Build the effective schema for serialization
		var effectiveSchema = sessionState.SelectedFields is { Length: > 0 }
			? FilterSchema(sessionState.Schema, sessionState.SelectedFields)
			: sessionState.Schema;

		// Filter rows if needed
		if (sessionState.SelectedFields is { Length: > 0 })
		{
			rows = rows.Select(r =>
			{
				var filtered = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
				foreach (var field in sessionState.SelectedFields)
				{
					if (r.Fields.TryGetValue(field, out var value))
						filtered[field] = value;
					else
						filtered[field] = null;
				}
				return new InMemoryRow(filtered);
			}).ToList();
		}

		// Serialize to Avro and create responses
		var responses = new List<ReadRowsResponse>();
		if (rows.Count > 0)
		{
			var avroBytes = AvroSerializer.SerializeRows(rows, effectiveSchema);
			responses.Add(new ReadRowsResponse
			{
				AvroRows = new AvroRows
				{
					SerializedBinaryRows = ByteString.CopyFrom(avroBytes),
				},
				RowCount = rows.Count,
			});
		}

		return responses;
	}

	private SplitReadStreamResponse HandleSplitReadStream(SplitReadStreamRequest request)
	{
		var streamName = request.Name;
		var fraction = request.Fraction;

		// Find the session for this stream
		var sessionName = streamName[..streamName.LastIndexOf("/streams/", StringComparison.Ordinal)];
		if (!_sessions.TryGetValue(sessionName, out var sessionState))
			throw new RpcException(new Status(StatusCode.NotFound, $"Session not found for stream: {streamName}"));

		var streamState = Array.Find(sessionState.Streams, s => s.Name == streamName);
		if (streamState is null)
			throw new RpcException(new Status(StatusCode.NotFound, $"Stream not found: {streamName}"));

		var totalRows = streamState.EndRow - streamState.StartRow;
		if (totalRows <= 1)
		{
			// Cannot split a stream with 0 or 1 rows — return empty
			// Ref: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#splitreadstreamresponse
			//   "If the original stream can not be split, primary and remainder will both be unset."
			return new SplitReadStreamResponse();
		}

		var splitPoint = streamState.StartRow + (int)(totalRows * Math.Max(0.1, Math.Min(0.9, fraction)));
		if (splitPoint <= streamState.StartRow) splitPoint = streamState.StartRow + 1;
		if (splitPoint >= streamState.EndRow) splitPoint = streamState.EndRow - 1;

		var primaryName = streamName + "_primary";
		var remainderName = streamName + "_remainder";

		var primaryState = new StreamState(primaryName, streamState.StartRow, splitPoint);
		var remainderState = new StreamState(remainderName, splitPoint, streamState.EndRow);

		// Add new streams to the session
		var newStreams = sessionState.Streams.Append(primaryState).Append(remainderState).ToArray();
		_sessions[sessionName] = sessionState with { Streams = newStreams };

		return new SplitReadStreamResponse
		{
			PrimaryStream = new ReadStream { Name = primaryName },
			RemainderStream = new ReadStream { Name = remainderName },
		};
	}

	private static Google.Apis.Bigquery.v2.Data.TableSchema FilterSchema(
		Google.Apis.Bigquery.v2.Data.TableSchema schema, string[] selectedFields)
	{
		var fieldSet = new HashSet<string>(selectedFields, StringComparer.OrdinalIgnoreCase);
		return new Google.Apis.Bigquery.v2.Data.TableSchema
		{
			Fields = schema.Fields.Where(f => fieldSet.Contains(f.Name)).ToList()
		};
	}

	/// <summary>
	/// State for a read session.
	/// </summary>
	internal sealed record ReadSessionState(
		string DatasetId,
		string TableId,
		Google.Apis.Bigquery.v2.Data.TableSchema Schema,
		StreamState[] Streams,
		string[]? SelectedFields,
		string? RowFilter);

	/// <summary>
	/// State for a read stream — tracks the row range assigned to it.
	/// </summary>
	internal sealed record StreamState(string Name, int StartRow, int EndRow);

	/// <summary>
	/// In-memory <see cref="IAsyncStreamReader{T}"/> that yields pre-computed items.
	/// </summary>
	private sealed class InMemoryAsyncStreamReader<T> : IAsyncStreamReader<T>
	{
		private readonly IEnumerator<T> _enumerator;

		public InMemoryAsyncStreamReader(IEnumerable<T> items)
		{
			_enumerator = items.GetEnumerator();
		}

		public T Current => _enumerator.Current;

		public Task<bool> MoveNext(CancellationToken cancellationToken = default)
		{
			return Task.FromResult(_enumerator.MoveNext());
		}
	}
}
