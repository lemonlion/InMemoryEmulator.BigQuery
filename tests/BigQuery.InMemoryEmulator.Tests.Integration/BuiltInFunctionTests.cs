using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for built-in functions (Phase 6).
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class BuiltInFunctionTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public BuiltInFunctionTests(BigQuerySession session)
	{
		_session = session;
	}

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_fn_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "value", Type = "INTEGER", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "score", Type = "FLOAT", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "data", schema);

		var rows = new[]
		{
			new BigQueryInsertRow("r1") { ["name"] = "Alice", ["value"] = 10, ["score"] = 90.5 },
			new BigQueryInsertRow("r2") { ["name"] = "Bob", ["value"] = -20, ["score"] = 85.0 },
			new BigQueryInsertRow("r3") { ["name"] = "Charlie", ["value"] = 30, ["score"] = 92.3 },
		};
		await client.InsertRowsAsync(_datasetId, "data", rows);
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
	public async Task String_Concat()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT CONCAT(name, '!') AS greeting FROM `{_datasetId}.data` WHERE name = 'Alice'",
			parameters: null);
		var rows = results.ToList();
		Assert.Single(rows);
		Assert.Equal("Alice!", (string)rows[0]["greeting"]);
	}

	[Fact]
	public async Task String_Upper()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT UPPER(name) AS upper_name FROM `{_datasetId}.data` WHERE name = 'Alice'",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal("ALICE", (string)rows[0]["upper_name"]);
	}

	[Fact]
	public async Task Math_Abs()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT ABS(value) AS abs_val FROM `{_datasetId}.data` WHERE name = 'Bob'",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal("20", rows[0]["abs_val"].ToString());
	}

	[Fact]
	public async Task Math_Round()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT ROUND(score, 0) AS rounded FROM `{_datasetId}.data` WHERE name = 'Alice'",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal("91", rows[0]["rounded"].ToString());
	}

	[Fact]
	public async Task Conditional_Coalesce()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT COALESCE(NULL, 'default') AS result",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal("default", (string)rows[0]["result"]);
	}

	[Fact]
	public async Task String_Substr()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT SUBSTR(name, 1, 3) AS short_name FROM `{_datasetId}.data` WHERE name = 'Charlie'",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal("Cha", (string)rows[0]["short_name"]);
	}

	[Fact]
	public async Task String_Replace()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT REPLACE(name, 'li', 'LI') AS modified FROM `{_datasetId}.data` WHERE name = 'Alice'",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal("ALIce", (string)rows[0]["modified"]);
	}

	[Fact]
	public async Task Aggregate_StringAgg()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT STRING_AGG(name) AS names FROM `{_datasetId}.data`",
			parameters: null);
		var rows = results.ToList();
		var names = (string)rows[0]["names"];
		Assert.Contains("Alice", names);
		Assert.Contains("Bob", names);
	}
}
