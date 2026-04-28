using BigQuery.InMemoryEmulator.Tests.Infrastructure;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for load jobs (UploadCsv/UploadJson) via the BigQueryClient SDK.
/// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#JobConfigurationLoad
///   "Configures a load job."
/// These use the SDK's resumable upload flow through the in-process test server.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class LoadJobIntegrationTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public LoadJobIntegrationTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_load_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public ValueTask DisposeAsync() => _fixture.DisposeAsync();

	[Fact]
	public async Task UploadCsv_InsertsRowsIntoNewTable()
	{
		var client = await _fixture.GetClientAsync();

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
			]
		};

		var csv = "1,Alice\n2,Bob\n3,Charlie\n";
		using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(csv));

		var job = await client.UploadCsvAsync(
			client.GetTableReference(_datasetId, "csv_load"),
			schema,
			stream);

		Assert.NotNull(job);
		Assert.Equal("DONE", job.Status.State);

		var allRows = client.ListRows(_datasetId, "csv_load", schema).ToList();
		Assert.Equal(3, allRows.Count);
	}

	[Fact]
	public async Task UploadCsv_WithSkipLeadingRows_SkipsHeaders()
	{
		var client = await _fixture.GetClientAsync();

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
			]
		};

		var csv = "id,name\n1,Alice\n2,Bob\n";
		using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(csv));

		var options = new UploadCsvOptions { SkipLeadingRows = 1 };
		await client.UploadCsvAsync(
			client.GetTableReference(_datasetId, "csv_skip"),
			schema,
			stream,
			options);

		var allRows = client.ListRows(_datasetId, "csv_skip", schema).ToList();
		Assert.Equal(2, allRows.Count);
	}

	[Fact]
	public async Task UploadJson_InsertsRowsIntoNewTable()
	{
		var client = await _fixture.GetClientAsync();

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
			]
		};

		var json = "{\"id\":1,\"name\":\"Alice\"}\n{\"id\":2,\"name\":\"Bob\"}\n";
		using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

		var job = await client.UploadJsonAsync(
			client.GetTableReference(_datasetId, "json_load"),
			schema,
			stream);

		Assert.NotNull(job);
		Assert.Equal("DONE", job.Status.State);

		var allRows = client.ListRows(_datasetId, "json_load", schema).ToList();
		Assert.Equal(2, allRows.Count);
	}

	[Fact]
	public async Task UploadCsv_DataIsQueryable()
	{
		var client = await _fixture.GetClientAsync();

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "city", Type = "STRING", Mode = "NULLABLE" },
			]
		};

		var csv = "1,London\n2,Paris\n3,Berlin\n";
		using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(csv));

		await client.UploadCsvAsync(
			client.GetTableReference(_datasetId, "csv_query"),
			schema,
			stream);

		var result = await client.ExecuteQueryAsync(
			$"SELECT city FROM {_datasetId}.csv_query WHERE id > 1 ORDER BY city",
			parameters: null);

		var cities = result.Select(row => (string)row["city"]).ToList();
		Assert.Equal(["Berlin", "Paris"], cities);
	}

	[Fact]
	public async Task UploadJson_DataIsQueryable()
	{
		var client = await _fixture.GetClientAsync();

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "score", Type = "FLOAT", Mode = "NULLABLE" },
			]
		};

		var json = "{\"id\":42,\"name\":\"test\",\"score\":3.14}\n";
		using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

		await client.UploadJsonAsync(
			client.GetTableReference(_datasetId, "json_query"),
			schema,
			stream);

		var result = await client.ExecuteQueryAsync(
			$"SELECT id, name, score FROM {_datasetId}.json_query",
			parameters: null);

		var allRows = result.ToList();
		Assert.Single(allRows);
		Assert.Equal(42L, allRows[0]["id"]);
		Assert.Equal("test", allRows[0]["name"]);
		Assert.Equal(3.14, (double)allRows[0]["score"], 2);
	}
}
