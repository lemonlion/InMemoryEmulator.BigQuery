using BigQuery.InMemoryEmulator;
using BigQuery.InMemoryEmulator.StorageApi;
using BigQuery.InMemoryEmulator.Tests.Infrastructure;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.Storage.V1;
using Google.Cloud.BigQuery.V2;
using Xunit;

using TableSchema = Google.Apis.Bigquery.v2.Data.TableSchema;
using TableFieldSchema = Google.Apis.Bigquery.v2.Data.TableFieldSchema;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for BigQuery Storage Read API (gRPC).
/// These tests set up data through the BigQueryClient SDK pipeline, then read
/// it back via the BigQueryReadClient Storage API.
/// InMemoryOnly because the Storage API interceptor only works with the in-memory store.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class StorageApiIntegrationTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private InMemoryBigQueryResult _result = null!;
	private BigQueryClient _client = null!;
	private string _datasetId = null!;

	public StorageApiIntegrationTests(BigQuerySession session)
	{
		_session = session;
	}

	public async ValueTask InitializeAsync()
	{
		_datasetId = $"storage_{Guid.NewGuid():N}"[..30];

		_result = InMemoryBigQuery.Create("test-project", _datasetId, ds =>
		{
			ds.AddTable("test_table", new TableSchema
			{
				Fields =
				[
					new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
					new TableFieldSchema { Name = "value", Type = "STRING", Mode = "NULLABLE" },
				]
			});
		});
		_client = _result.Client;

		// Insert data through the SDK pipeline
		var tableRef = _client.GetTableReference(_datasetId, "test_table");
		var rows = new[]
		{
			new BigQueryInsertRow { { "id", 1 }, { "value", "one" } },
			new BigQueryInsertRow { { "id", 2 }, { "value", "two" } },
			new BigQueryInsertRow { { "id", 3 }, { "value", "three" } },
		};
		await _client.InsertRowsAsync(tableRef, rows);
	}

	public ValueTask DisposeAsync()
	{
		_result?.Dispose();
		return ValueTask.CompletedTask;
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1
	//   "BigQuery Read API — The Read API can be used to read data from BigQuery."
	[Fact]
	public void CreateReadSession_ViaStorageApi_ReturnsSessionForTable()
	{
		var readClient = _result.CreateReadClient();
		var tableRef = $"projects/test-project/datasets/{_datasetId}/tables/test_table";

		var session = readClient.CreateReadSession(
			parent: "projects/test-project",
			readSession: new ReadSession
			{
				Table = tableRef,
				DataFormat = DataFormat.Avro,
			},
			maxStreamCount: 1);

		Assert.NotNull(session);
		Assert.NotEmpty(session.Streams);
		Assert.NotNull(session.AvroSchema);
		Assert.Contains("\"name\":\"id\"", session.AvroSchema.Schema);
		Assert.Contains("\"name\":\"value\"", session.AvroSchema.Schema);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryRead.ReadRows
	//   "Reads rows from the stream in the format prescribed by the ReadSession."
	[Fact]
	public async Task ReadRows_ViaStorageApi_ReturnsAllInsertedData()
	{
		var readClient = _result.CreateReadClient();
		var tableRef = $"projects/test-project/datasets/{_datasetId}/tables/test_table";

		var session = readClient.CreateReadSession(
			parent: "projects/test-project",
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

		long totalRows = 0;
		var responseStream = stream.GetResponseStream();
		while (await responseStream.MoveNextAsync(default))
		{
			totalRows += responseStream.Current.RowCount;
			Assert.NotNull(responseStream.Current.AvroRows);
			Assert.True(responseStream.Current.AvroRows.SerializedBinaryRows.Length > 0);
		}

		Assert.Equal(3, totalRows);
	}

	// Ref: Rest + Storage API interop: data set up via REST, read via Storage API.
	[Fact]
	public async Task DataInsertedViaRest_CanBeReadViaStorageApi()
	{
		// Insert a new row via REST API
		var tableRef = _client.GetTableReference(_datasetId, "test_table");
		await _client.InsertRowsAsync(tableRef, [
			new BigQueryInsertRow { { "id", 4 }, { "value", "four" } },
		]);

		// Read via Storage API
		var readClient = _result.CreateReadClient();
		var session = readClient.CreateReadSession(
			parent: "projects/test-project",
			readSession: new ReadSession
			{
				Table = $"projects/test-project/datasets/{_datasetId}/tables/test_table",
				DataFormat = DataFormat.Avro,
			},
			maxStreamCount: 1);

		using var stream = readClient.ReadRows(new ReadRowsRequest
		{
			ReadStream = session.Streams[0].Name,
		});

		long totalRows = 0;
		var responseStream = stream.GetResponseStream();
		while (await responseStream.MoveNextAsync(default))
			totalRows += responseStream.Current.RowCount;

		// Original 3 rows + 1 new row
		Assert.Equal(4, totalRows);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#splitreadstreamrequest
	//   "Splits a given ReadStream into two ReadStream objects."
	[Fact]
	public async Task SplitReadStream_ViaStorageApi_CoversAllData()
	{
		var readClient = _result.CreateReadClient();
		var tableRef = $"projects/test-project/datasets/{_datasetId}/tables/test_table";

		var session = readClient.CreateReadSession(
			parent: "projects/test-project",
			readSession: new ReadSession
			{
				Table = tableRef,
				DataFormat = DataFormat.Avro,
			},
			maxStreamCount: 1);

		var split = readClient.SplitReadStream(new SplitReadStreamRequest
		{
			Name = session.Streams[0].Name,
			Fraction = 0.5,
		});

		Assert.NotNull(split.PrimaryStream);
		Assert.NotNull(split.RemainderStream);

		long totalRows = 0;

		using var primary = readClient.ReadRows(new ReadRowsRequest { ReadStream = split.PrimaryStream.Name });
		var ps = primary.GetResponseStream();
		while (await ps.MoveNextAsync(default))
			totalRows += ps.Current.RowCount;

		using var remainder = readClient.ReadRows(new ReadRowsRequest { ReadStream = split.RemainderStream.Name });
		var rs = remainder.GetResponseStream();
		while (await rs.MoveNextAsync(default))
			totalRows += rs.Current.RowCount;

		Assert.Equal(3, totalRows);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#tablereadoptions
	//   "selected_fields: Names of the fields in the table to read."
	[Fact]
	public async Task SelectedFields_ViaStorageApi_FiltersColumns()
	{
		var readClient = _result.CreateReadClient();
		var tableRef = $"projects/test-project/datasets/{_datasetId}/tables/test_table";

		var readOptions = new ReadSession.Types.TableReadOptions();
		readOptions.SelectedFields.Add("id");

		var session = readClient.CreateReadSession(
			parent: "projects/test-project",
			readSession: new ReadSession
			{
				Table = tableRef,
				DataFormat = DataFormat.Avro,
				ReadOptions = readOptions,
			},
			maxStreamCount: 1);

		// Schema should only contain id field
		Assert.Contains("\"name\":\"id\"", session.AvroSchema.Schema);
		Assert.DoesNotContain("\"name\":\"value\"", session.AvroSchema.Schema);

		using var stream = readClient.ReadRows(new ReadRowsRequest
		{
			ReadStream = session.Streams[0].Name,
		});

		var responseStream = stream.GetResponseStream();
		await responseStream.MoveNextAsync(default);
		Assert.Equal(3, responseStream.Current.RowCount);
	}
}
