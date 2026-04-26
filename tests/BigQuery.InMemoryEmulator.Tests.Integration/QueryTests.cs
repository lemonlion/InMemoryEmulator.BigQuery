using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for SQL query execution (Phase 4).
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class QueryTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public QueryTests(BigQuerySession session)
	{
		_session = session;
	}

	private static TableSchema SimpleSchema() => new()
	{
		Fields =
		[
			new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
			new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
			new TableFieldSchema { Name = "score", Type = "FLOAT", Mode = "NULLABLE" },
		]
	};

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_qry_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.CreateTableAsync(_datasetId, "users", SimpleSchema());

		// Insert test data
		var rows = new[]
		{
			new BigQueryInsertRow("r1") { ["id"] = 1, ["name"] = "Alice", ["score"] = 90.5 },
			new BigQueryInsertRow("r2") { ["id"] = 2, ["name"] = "Bob", ["score"] = 85.0 },
			new BigQueryInsertRow("r3") { ["id"] = 3, ["name"] = "Charlie", ["score"] = 92.0 },
		};
		await client.InsertRowsAsync(_datasetId, "users", rows);
	}

	public async ValueTask DisposeAsync()
	{
		try
		{
			var client = await _fixture.GetClientAsync();
			await client.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true });
		}
		catch { }
		await _fixture.DisposeAsync();
	}

	[Fact]
	public async Task Client_ExecuteQuery_SelectStar_ReturnsAllRows()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT * FROM `{_datasetId}.users`",
			parameters: null);

		var rows = results.ToList();
		Assert.Equal(3, rows.Count);
	}

	[Fact]
	public async Task Client_ExecuteQuery_WhereFilter_ReturnsFiltered()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT * FROM `{_datasetId}.users` WHERE name = 'Alice'",
			parameters: null);

		var rows = results.ToList();
		Assert.Single(rows);
		Assert.Equal("Alice", (string)rows[0]["name"]);
	}

	[Fact]
	public async Task Client_ExecuteQuery_OrderBy_ReturnsSorted()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT * FROM `{_datasetId}.users` ORDER BY name",
			parameters: null);

		var rows = results.ToList();
		Assert.Equal(3, rows.Count);
		Assert.Equal("Alice", (string)rows[0]["name"]);
		Assert.Equal("Bob", (string)rows[1]["name"]);
		Assert.Equal("Charlie", (string)rows[2]["name"]);
	}

	[Fact]
	public async Task Client_ExecuteQuery_Limit_ReturnsLimited()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT * FROM `{_datasetId}.users` LIMIT 2",
			parameters: null);

		var rows = results.ToList();
		Assert.Equal(2, rows.Count);
	}

	[Fact]
	public async Task Client_ExecuteQuery_WithParameters_Works()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[]
		{
			new BigQueryParameter("target_id", BigQueryDbType.Int64, 2),
		};

		var results = await client.ExecuteQueryAsync(
			$"SELECT * FROM `{_datasetId}.users` WHERE id = @target_id",
			parameters: parameters);

		var rows = results.ToList();
		Assert.Single(rows);
		Assert.Equal("Bob", (string)rows[0]["name"]);
	}

	[Fact]
	public async Task Client_CreateQueryJob_ThenGetResults_Works()
	{
		var client = await _fixture.GetClientAsync();
		var job = await client.CreateQueryJobAsync(
			$"SELECT * FROM `{_datasetId}.users`",
			parameters: null);

		Assert.NotNull(job);
		var results = await client.GetQueryResultsAsync(job.Reference);
		var rows = results.ToList();
		Assert.Equal(3, rows.Count);
	}

	[Fact]
	public async Task Client_ExecuteQuery_CountStar_ReturnsCount()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT COUNT(*) FROM `{_datasetId}.users`",
			parameters: null);

		var rows = results.ToList();
		Assert.Single(rows);
	}
}
