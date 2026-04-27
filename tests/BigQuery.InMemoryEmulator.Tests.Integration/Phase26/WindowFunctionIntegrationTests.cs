using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration.Phase26;

/// <summary>
/// Integration tests for Phase 26: Window/numbering/navigation functions.
/// NTILE, PERCENT_RANK, CUME_DIST, FIRST_VALUE, LAST_VALUE, LAG, LEAD.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class WindowFunctionIntegrationTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public WindowFunctionIntegrationTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_p26w_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "x", Type = "INTEGER", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "grp", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "val", Type = "INTEGER", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "win", schema);
		await client.InsertRowsAsync(_datasetId, "win", new[]
		{
			new BigQueryInsertRow("r1") { ["x"] = 1, ["grp"] = "A", ["val"] = 10, ["name"] = "a" },
			new BigQueryInsertRow("r2") { ["x"] = 2, ["grp"] = "A", ["val"] = 20, ["name"] = "b" },
			new BigQueryInsertRow("r3") { ["x"] = 2, ["grp"] = "A", ["val"] = 30, ["name"] = "c" },
			new BigQueryInsertRow("r4") { ["x"] = 3, ["grp"] = "B", ["val"] = 40, ["name"] = "d" },
			new BigQueryInsertRow("r5") { ["x"] = 4, ["grp"] = "B", ["val"] = 50, ["name"] = "e" },
		});
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

	#region NTILE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions#ntile
	//   "Divides the rows into constant_integer_expression buckets based on row ordering
	//    and returns the 1-based bucket number that is assigned to each row."
	[Fact]
	public async Task Ntile_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT x, NTILE(2) OVER (ORDER BY val) AS bucket FROM `{_datasetId}.win` ORDER BY val",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(5, rows.Count);
		// 5 rows / 2 buckets → sizes 3, 2
		Assert.Equal(1L, Convert.ToInt64(rows[0]["bucket"]));
		Assert.Equal(1L, Convert.ToInt64(rows[1]["bucket"]));
		Assert.Equal(1L, Convert.ToInt64(rows[2]["bucket"]));
		Assert.Equal(2L, Convert.ToInt64(rows[3]["bucket"]));
		Assert.Equal(2L, Convert.ToInt64(rows[4]["bucket"]));
	}

	[Fact]
	public async Task Ntile_UnevenDistribution()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT val, NTILE(3) OVER (ORDER BY val) AS bucket FROM `{_datasetId}.win` ORDER BY val",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(1L, Convert.ToInt64(rows[0]["bucket"]));
		Assert.Equal(1L, Convert.ToInt64(rows[1]["bucket"]));
		Assert.Equal(2L, Convert.ToInt64(rows[2]["bucket"]));
		Assert.Equal(2L, Convert.ToInt64(rows[3]["bucket"]));
		Assert.Equal(3L, Convert.ToInt64(rows[4]["bucket"]));
	}

	#endregion

	#region PERCENT_RANK

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions#percent_rank
	//   "Return the percentile rank of a row defined as (RK-1)/(NR-1)."
	[Fact]
	public async Task PercentRank_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT x, PERCENT_RANK() OVER (ORDER BY x) AS pr FROM `{_datasetId}.win` ORDER BY x, val",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(0.0, Convert.ToDouble(rows[0]["pr"]), 5);
		Assert.Equal(0.25, Convert.ToDouble(rows[1]["pr"]), 5);
		Assert.Equal(0.25, Convert.ToDouble(rows[2]["pr"]), 5);
		Assert.Equal(0.75, Convert.ToDouble(rows[3]["pr"]), 5);
		Assert.Equal(1.0, Convert.ToDouble(rows[4]["pr"]), 5);
	}

	#endregion

	#region CUME_DIST

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions#cume_dist
	//   "Return the relative rank of a row defined as NP/NR."
	[Fact]
	public async Task CumeDist_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT x, CUME_DIST() OVER (ORDER BY x) AS cd FROM `{_datasetId}.win` ORDER BY x, val",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(0.2, Convert.ToDouble(rows[0]["cd"]), 5);
		Assert.Equal(0.6, Convert.ToDouble(rows[1]["cd"]), 5);
		Assert.Equal(0.6, Convert.ToDouble(rows[2]["cd"]), 5);
		Assert.Equal(0.8, Convert.ToDouble(rows[3]["cd"]), 5);
		Assert.Equal(1.0, Convert.ToDouble(rows[4]["cd"]), 5);
	}

	#endregion

	#region FIRST_VALUE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#first_value
	//   "Returns the value of the value_expression for the first row in the current window frame."
	[Fact]
	public async Task FirstValue_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT name, FIRST_VALUE(name) OVER (ORDER BY val) AS fv FROM `{_datasetId}.win` ORDER BY val",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal("a", rows[0]["fv"]?.ToString());
		Assert.Equal("a", rows[1]["fv"]?.ToString());
		Assert.Equal("a", rows[2]["fv"]?.ToString());
	}

	[Fact]
	public async Task FirstValue_WithPartition()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT grp, val, FIRST_VALUE(val) OVER (PARTITION BY grp ORDER BY val) AS fv FROM `{_datasetId}.win` ORDER BY grp, val",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(10L, Convert.ToInt64(rows[0]["fv"]));
		Assert.Equal(10L, Convert.ToInt64(rows[1]["fv"]));
		Assert.Equal(10L, Convert.ToInt64(rows[2]["fv"]));
		Assert.Equal(40L, Convert.ToInt64(rows[3]["fv"]));
		Assert.Equal(40L, Convert.ToInt64(rows[4]["fv"]));
	}

	#endregion

	#region LAG

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#lag
	//   "Returns the value of the value_expression on a preceding row."
	[Fact]
	public async Task Lag_Default()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT val, LAG(val) OVER (ORDER BY val) AS prev FROM `{_datasetId}.win` ORDER BY val",
			parameters: null);
		var rows = results.ToList();
		Assert.Null(rows[0]["prev"]);
		Assert.Equal(10L, Convert.ToInt64(rows[1]["prev"]));
		Assert.Equal(20L, Convert.ToInt64(rows[2]["prev"]));
	}

	[Fact]
	public async Task Lag_WithOffset()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT val, LAG(val, 2) OVER (ORDER BY val) AS prev FROM `{_datasetId}.win` ORDER BY val",
			parameters: null);
		var rows = results.ToList();
		Assert.Null(rows[0]["prev"]);
		Assert.Null(rows[1]["prev"]);
		Assert.Equal(10L, Convert.ToInt64(rows[2]["prev"]));
		Assert.Equal(20L, Convert.ToInt64(rows[3]["prev"]));
	}

	[Fact]
	public async Task Lag_WithDefault()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT val, LAG(val, 1, -1) OVER (ORDER BY val) AS prev FROM `{_datasetId}.win` ORDER BY val",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(-1L, Convert.ToInt64(rows[0]["prev"]));
		Assert.Equal(10L, Convert.ToInt64(rows[1]["prev"]));
	}

	#endregion

	#region LEAD

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#lead
	//   "Returns the value of the value_expression on a subsequent row."
	[Fact]
	public async Task Lead_Default()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT val, LEAD(val) OVER (ORDER BY val) AS nxt FROM `{_datasetId}.win` ORDER BY val",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(20L, Convert.ToInt64(rows[0]["nxt"]));
		Assert.Equal(30L, Convert.ToInt64(rows[1]["nxt"]));
		Assert.Null(rows[4]["nxt"]);
	}

	[Fact]
	public async Task Lead_WithOffset()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT val, LEAD(val, 2) OVER (ORDER BY val) AS nxt FROM `{_datasetId}.win` ORDER BY val",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(30L, Convert.ToInt64(rows[0]["nxt"]));
		Assert.Equal(40L, Convert.ToInt64(rows[1]["nxt"]));
		Assert.Null(rows[3]["nxt"]);
		Assert.Null(rows[4]["nxt"]);
	}

	[Fact]
	public async Task Lead_WithDefault()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT val, LEAD(val, 1, 99) OVER (ORDER BY val) AS nxt FROM `{_datasetId}.win` ORDER BY val",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(20L, Convert.ToInt64(rows[0]["nxt"]));
		Assert.Equal(99L, Convert.ToInt64(rows[4]["nxt"]));
	}

	#endregion
}
