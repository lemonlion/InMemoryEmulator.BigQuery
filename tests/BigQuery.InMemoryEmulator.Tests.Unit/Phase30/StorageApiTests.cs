using BigQuery.InMemoryEmulator.StorageApi;
using Google.Cloud.BigQuery.Storage.V1;
using Xunit;

using TableSchema = Google.Apis.Bigquery.v2.Data.TableSchema;
using TableFieldSchema = Google.Apis.Bigquery.v2.Data.TableFieldSchema;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase30;

/// <summary>
/// Phase 30 Storage API: BigQuery Storage Read API (gRPC) via in-memory CallInvoker.
/// Tests cover CreateReadSession, ReadRows, and SplitReadStream.
/// </summary>
public class StorageApiTests
{
	private static (InMemoryBigQueryResult result, string tableRef) CreateTestSetup()
	{
		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "score", Type = "FLOAT", Mode = "NULLABLE" },
			]
		};

		var result = InMemoryBigQuery.Create("test-project", "test_ds", ds =>
		{
			ds.AddTable("items", schema);
		});

		// Insert test data via the backing store
		var table = result.Store.Datasets["test_ds"].Tables["items"];
		lock (table.RowLock)
		{
			table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 1L, ["name"] = "alpha", ["score"] = 1.5 }));
			table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 2L, ["name"] = "beta", ["score"] = 2.5 }));
			table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 3L, ["name"] = "gamma", ["score"] = 3.5 }));
		}

		var tableRef = "projects/test-project/datasets/test_ds/tables/items";
		return (result, tableRef);
	}

	#region CreateReadSession

	// Ref: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryRead.CreateReadSession
	//   "Creates a new read session. A read session divides the contents of a BigQuery table
	//    into one or more streams."
	[Fact]
	public void CreateReadSession_ReturnsSessionWithStreams()
	{
		var (bqResult, tableRef) = CreateTestSetup();
		var readClient = bqResult.CreateReadClient();

		var session = readClient.CreateReadSession(
			parent: $"projects/{bqResult.Store.ProjectId}",
			readSession: new ReadSession
			{
				Table = tableRef,
				DataFormat = DataFormat.Avro,
			},
			maxStreamCount: 1);

		Assert.NotNull(session);
		Assert.NotEmpty(session.Name);
		Assert.Single(session.Streams);
		Assert.NotNull(session.AvroSchema);
		Assert.Contains("\"type\":\"record\"", session.AvroSchema.Schema);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#createsessionrequest
	//   "max_stream_count: Max initial number of streams."
	[Fact]
	public void CreateReadSession_WithMaxStreamCount_CreatesMultipleStreams()
	{
		var (bqResult, tableRef) = CreateTestSetup();
		var readClient = bqResult.CreateReadClient();

		var session = readClient.CreateReadSession(
			parent: $"projects/{bqResult.Store.ProjectId}",
			readSession: new ReadSession
			{
				Table = tableRef,
				DataFormat = DataFormat.Avro,
			},
			maxStreamCount: 3);

		Assert.NotNull(session);
		Assert.Equal(3, session.Streams.Count);
	}

	// Ref: Table not found should produce a gRPC NOT_FOUND error.
	[Fact]
	public void CreateReadSession_NonExistentTable_ThrowsNotFound()
	{
		var (bqResult, _) = CreateTestSetup();
		var readClient = bqResult.CreateReadClient();

		var ex = Assert.Throws<Grpc.Core.RpcException>(() =>
			readClient.CreateReadSession(
				parent: $"projects/{bqResult.Store.ProjectId}",
				readSession: new ReadSession
				{
					Table = "projects/test-project/datasets/test_ds/tables/nonexistent",
					DataFormat = DataFormat.Avro,
				},
				maxStreamCount: 1));

		Assert.Equal(Grpc.Core.StatusCode.NotFound, ex.StatusCode);
	}

	[Fact]
	public void CreateReadSession_EmptyTable_ReturnsSessionWithOneStream()
	{
		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
			]
		};

		var result = InMemoryBigQuery.Create("test-project", "empty_ds", ds =>
		{
			ds.AddTable("empty_table", schema);
		});

		var readClient = result.CreateReadClient();
		var session = readClient.CreateReadSession(
			parent: "projects/test-project",
			readSession: new ReadSession
			{
				Table = "projects/test-project/datasets/empty_ds/tables/empty_table",
				DataFormat = DataFormat.Avro,
			},
			maxStreamCount: 1);

		Assert.Single(session.Streams);
	}

	#endregion

	#region ReadRows

	// Ref: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryRead.ReadRows
	//   "Reads rows from the stream in the format prescribed by the ReadSession."
	[Fact]
	public async Task ReadRows_ReturnsAllData()
	{
		var (bqResult, tableRef) = CreateTestSetup();
		var readClient = bqResult.CreateReadClient();

		var session = readClient.CreateReadSession(
			parent: $"projects/{bqResult.Store.ProjectId}",
			readSession: new ReadSession
			{
				Table = tableRef,
				DataFormat = DataFormat.Avro,
			},
			maxStreamCount: 1);

		using var stream = readClient.ReadRows(new ReadRowsRequest
		{
			ReadStream = session.Streams[0].Name,
			Offset = 0,
		});

		var responses = new List<ReadRowsResponse>();
		var responseStream = stream.GetResponseStream();
		while (await responseStream.MoveNextAsync(default))
		{
			responses.Add(responseStream.Current);
		}

		Assert.NotEmpty(responses);
		Assert.True(responses[0].RowCount > 0);
		Assert.NotNull(responses[0].AvroRows);
		Assert.True(responses[0].AvroRows.SerializedBinaryRows.Length > 0);
	}

	[Fact]
	public async Task ReadRows_AvroData_CanBeDeserialized()
	{
		var (bqResult, tableRef) = CreateTestSetup();
		var readClient = bqResult.CreateReadClient();

		var session = readClient.CreateReadSession(
			parent: $"projects/{bqResult.Store.ProjectId}",
			readSession: new ReadSession
			{
				Table = tableRef,
				DataFormat = DataFormat.Avro,
			},
			maxStreamCount: 1);

		using var stream = readClient.ReadRows(new ReadRowsRequest
		{
			ReadStream = session.Streams[0].Name,
		});

		var responseStream = stream.GetResponseStream();
		await responseStream.MoveNextAsync(default);
		var response = responseStream.Current;

		// Manually decode Avro binary to verify format correctness.
		// Schema: id (REQUIRED long), name (nullable string), score (nullable double)
		using var ms = new MemoryStream(response.AvroRows.SerializedBinaryRows.ToByteArray());

		// Row 1: id=1, name="alpha", score=1.5
		var id1 = AvroSerializer.ReadLong(ms);        // REQUIRED long
		var nameUnionIdx1 = AvroSerializer.ReadLong(ms); // union index (1 = non-null)
		var name1 = AvroSerializer.ReadString(ms);     // string value
		var scoreUnionIdx1 = AvroSerializer.ReadLong(ms); // union index (1 = non-null)
		var score1 = AvroSerializer.ReadDouble(ms);    // double value

		Assert.Equal(1L, id1);
		Assert.Equal(1L, nameUnionIdx1);
		Assert.Equal("alpha", name1);
		Assert.Equal(1L, scoreUnionIdx1);
		Assert.Equal(1.5, score1, 10);

		// Row 2: id=2, name="beta", score=2.5
		var id2 = AvroSerializer.ReadLong(ms);
		AvroSerializer.ReadLong(ms); // union index
		var name2 = AvroSerializer.ReadString(ms);
		AvroSerializer.ReadLong(ms); // union index
		var score2 = AvroSerializer.ReadDouble(ms);

		Assert.Equal(2L, id2);
		Assert.Equal("beta", name2);
		Assert.Equal(2.5, score2, 10);

		// Row 3: id=3, name="gamma", score=3.5
		var id3 = AvroSerializer.ReadLong(ms);
		AvroSerializer.ReadLong(ms);
		var name3 = AvroSerializer.ReadString(ms);
		AvroSerializer.ReadLong(ms);
		var score3 = AvroSerializer.ReadDouble(ms);

		Assert.Equal(3L, id3);
		Assert.Equal("gamma", name3);
		Assert.Equal(3.5, score3, 10);

		// Should be at end of stream
		Assert.Equal(ms.Length, ms.Position);
	}

	[Fact]
	public async Task ReadRows_WithOffset_SkipsRows()
	{
		var (bqResult, tableRef) = CreateTestSetup();
		var readClient = bqResult.CreateReadClient();

		var session = readClient.CreateReadSession(
			parent: $"projects/{bqResult.Store.ProjectId}",
			readSession: new ReadSession
			{
				Table = tableRef,
				DataFormat = DataFormat.Avro,
			},
			maxStreamCount: 1);

		using var stream = readClient.ReadRows(new ReadRowsRequest
		{
			ReadStream = session.Streams[0].Name,
			Offset = 2, // skip first 2 rows
		});

		var responseStream = stream.GetResponseStream();
		await responseStream.MoveNextAsync(default);
		var response = responseStream.Current;

		Assert.Equal(1, response.RowCount);
	}

	[Fact]
	public async Task ReadRows_EmptyTable_ReturnsNoResponses()
	{
		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
			]
		};

		var result = InMemoryBigQuery.Create("test-project", "empty_ds", ds =>
		{
			ds.AddTable("empty_table", schema);
		});

		var readClient = result.CreateReadClient();
		var session = readClient.CreateReadSession(
			parent: "projects/test-project",
			readSession: new ReadSession
			{
				Table = "projects/test-project/datasets/empty_ds/tables/empty_table",
				DataFormat = DataFormat.Avro,
			},
			maxStreamCount: 1);

		using var stream = readClient.ReadRows(new ReadRowsRequest
		{
			ReadStream = session.Streams[0].Name,
		});

		var responseStream = stream.GetResponseStream();
		var hasData = await responseStream.MoveNextAsync(default);
		Assert.False(hasData);
	}

	[Fact]
	public async Task ReadRows_MultipleStreams_PartitionData()
	{
		var (bqResult, tableRef) = CreateTestSetup();
		var readClient = bqResult.CreateReadClient();

		var session = readClient.CreateReadSession(
			parent: $"projects/{bqResult.Store.ProjectId}",
			readSession: new ReadSession
			{
				Table = tableRef,
				DataFormat = DataFormat.Avro,
			},
			maxStreamCount: 3);

		Assert.Equal(3, session.Streams.Count);

		long totalRows = 0;
		foreach (var readStream in session.Streams)
		{
			using var stream = readClient.ReadRows(new ReadRowsRequest
			{
				ReadStream = readStream.Name,
			});

			var responseStream = stream.GetResponseStream();
			while (await responseStream.MoveNextAsync(default))
			{
				totalRows += responseStream.Current.RowCount;
			}
		}

		// Total across all streams should equal table row count
		Assert.Equal(3, totalRows);
	}

	#endregion

	#region SplitReadStream

	// Ref: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryRead.SplitReadStream
	//   "Splits a given ReadStream into two ReadStream objects."
	[Fact]
	public void SplitReadStream_SplitsStreamInTwo()
	{
		var (bqResult, tableRef) = CreateTestSetup();
		var readClient = bqResult.CreateReadClient();

		var session = readClient.CreateReadSession(
			parent: $"projects/{bqResult.Store.ProjectId}",
			readSession: new ReadSession
			{
				Table = tableRef,
				DataFormat = DataFormat.Avro,
			},
			maxStreamCount: 1);

		var splitResponse = readClient.SplitReadStream(new SplitReadStreamRequest
		{
			Name = session.Streams[0].Name,
			Fraction = 0.5,
		});

		Assert.NotNull(splitResponse.PrimaryStream);
		Assert.NotNull(splitResponse.RemainderStream);
		Assert.NotEmpty(splitResponse.PrimaryStream.Name);
		Assert.NotEmpty(splitResponse.RemainderStream.Name);
	}

	[Fact]
	public async Task SplitReadStream_ChildStreams_ContainAllData()
	{
		var (bqResult, tableRef) = CreateTestSetup();
		var readClient = bqResult.CreateReadClient();

		var session = readClient.CreateReadSession(
			parent: $"projects/{bqResult.Store.ProjectId}",
			readSession: new ReadSession
			{
				Table = tableRef,
				DataFormat = DataFormat.Avro,
			},
			maxStreamCount: 1);

		var splitResponse = readClient.SplitReadStream(new SplitReadStreamRequest
		{
			Name = session.Streams[0].Name,
			Fraction = 0.5,
		});

		long totalRows = 0;

		// Read primary stream
		using var primary = readClient.ReadRows(new ReadRowsRequest
		{
			ReadStream = splitResponse.PrimaryStream.Name,
		});
		var primaryStream = primary.GetResponseStream();
		while (await primaryStream.MoveNextAsync(default))
			totalRows += primaryStream.Current.RowCount;

		// Read remainder stream
		using var remainder = readClient.ReadRows(new ReadRowsRequest
		{
			ReadStream = splitResponse.RemainderStream.Name,
		});
		var remainderStream = remainder.GetResponseStream();
		while (await remainderStream.MoveNextAsync(default))
			totalRows += remainderStream.Current.RowCount;

		Assert.Equal(3, totalRows);
	}

	#endregion

	#region AvroSchema

	[Fact]
	public void AvroSchema_ContainsAllFields()
	{
		var (bqResult, tableRef) = CreateTestSetup();
		var readClient = bqResult.CreateReadClient();

		var session = readClient.CreateReadSession(
			parent: $"projects/{bqResult.Store.ProjectId}",
			readSession: new ReadSession
			{
				Table = tableRef,
				DataFormat = DataFormat.Avro,
			},
			maxStreamCount: 1);

		var schemaJson = session.AvroSchema.Schema;
		Assert.Contains("\"name\":\"id\"", schemaJson);
		Assert.Contains("\"name\":\"name\"", schemaJson);
		Assert.Contains("\"name\":\"score\"", schemaJson);
		// REQUIRED field: type is just "long", not nullable union
		Assert.Contains("\"type\":\"long\"", schemaJson);
		// NULLABLE fields: type is ["null","type"] union
		Assert.Contains("[\"null\",\"string\"]", schemaJson);
		Assert.Contains("[\"null\",\"double\"]", schemaJson);
	}

	#endregion

	#region SelectedFields

	// Ref: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#tablereadoptions
	//   "selected_fields: Names of the fields in the table to read."
	[Fact]
	public async Task CreateReadSession_WithSelectedFields_ReturnsOnlySelectedData()
	{
		var (bqResult, tableRef) = CreateTestSetup();
		var readClient = bqResult.CreateReadClient();

		var readOptions = new ReadSession.Types.TableReadOptions();
		readOptions.SelectedFields.Add("id");
		readOptions.SelectedFields.Add("name");

		var session = readClient.CreateReadSession(
			parent: $"projects/{bqResult.Store.ProjectId}",
			readSession: new ReadSession
			{
				Table = tableRef,
				DataFormat = DataFormat.Avro,
				ReadOptions = readOptions,
			},
			maxStreamCount: 1);

		// Schema should only contain selected fields
		Assert.Contains("\"name\":\"id\"", session.AvroSchema.Schema);
		Assert.Contains("\"name\":\"name\"", session.AvroSchema.Schema);
		Assert.DoesNotContain("\"name\":\"score\"", session.AvroSchema.Schema);

		// Read rows and verify we can decode them
		using var stream = readClient.ReadRows(new ReadRowsRequest
		{
			ReadStream = session.Streams[0].Name,
		});

		var responseStream = stream.GetResponseStream();
		await responseStream.MoveNextAsync(default);
		var response = responseStream.Current;
		Assert.Equal(3, response.RowCount);
	}

	#endregion

	#region CreateReadClient convenience

	[Fact]
	public void CreateReadClient_FromResult_Works()
	{
		var result = InMemoryBigQuery.Create("test-project", "ds");
		var readClient = result.CreateReadClient();
		Assert.NotNull(readClient);
	}

	[Fact]
	public void CreateReadClient_FromFactory_Works()
	{
		var store = new InMemoryDataStore("test-project");
		var readClient = InMemoryBigQueryReadClientFactory.Create(store);
		Assert.NotNull(readClient);
	}

	#endregion
}
