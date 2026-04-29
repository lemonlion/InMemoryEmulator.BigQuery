using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Comprehensive integration tests for window functions.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class WindowFunctionComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public WindowFunctionComprehensiveTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_win_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.CreateTableAsync(_datasetId, "sales", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "dept", Type = "STRING" },
				new TableFieldSchema { Name = "amount", Type = "FLOAT" },
				new TableFieldSchema { Name = "dt", Type = "DATE" },
			]
		});
		await client.InsertRowsAsync(_datasetId, "sales", new[]
		{
			new BigQueryInsertRow("r1") { ["id"] = 1, ["dept"] = "A", ["amount"] = 100.0, ["dt"] = "2024-01-01" },
			new BigQueryInsertRow("r2") { ["id"] = 2, ["dept"] = "A", ["amount"] = 200.0, ["dt"] = "2024-01-02" },
			new BigQueryInsertRow("r3") { ["id"] = 3, ["dept"] = "A", ["amount"] = 150.0, ["dt"] = "2024-01-03" },
			new BigQueryInsertRow("r4") { ["id"] = 4, ["dept"] = "B", ["amount"] = 300.0, ["dt"] = "2024-01-01" },
			new BigQueryInsertRow("r5") { ["id"] = 5, ["dept"] = "B", ["amount"] = 250.0, ["dt"] = "2024-01-02" },
			new BigQueryInsertRow("r6") { ["id"] = 6, ["dept"] = "B", ["amount"] = 350.0, ["dt"] = "2024-01-03" },
		});
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		return result.ToList();
	}

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- ROW_NUMBER ----
	[Fact] public async Task RowNumber_Global()
	{
		var rows = await Query($"SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM `{_datasetId}.sales` ORDER BY id");
		Assert.Equal(6, rows.Count);
		Assert.Equal("1", rows[0]["rn"]?.ToString());
		Assert.Equal("6", rows[5]["rn"]?.ToString());
	}
	[Fact] public async Task RowNumber_Partitioned()
	{
		var rows = await Query($"SELECT id, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY id) AS rn FROM `{_datasetId}.sales` ORDER BY dept, id");
		Assert.Equal("1", rows[0]["rn"]?.ToString());
		Assert.Equal("3", rows[2]["rn"]?.ToString());
	}

	// ---- RANK / DENSE_RANK ----
	[Fact] public async Task Rank_WithTies()
	{
		var rows = await Query("SELECT val, RANK() OVER (ORDER BY val) AS r FROM (SELECT 1 AS val UNION ALL SELECT 1 UNION ALL SELECT 2) AS t ORDER BY val");
		Assert.Equal("1", rows[0]["r"]?.ToString());
		Assert.Equal("1", rows[1]["r"]?.ToString());
		Assert.Equal("3", rows[2]["r"]?.ToString());
	}
	[Fact] public async Task DenseRank_WithTies()
	{
		var rows = await Query("SELECT val, DENSE_RANK() OVER (ORDER BY val) AS r FROM (SELECT 1 AS val UNION ALL SELECT 1 UNION ALL SELECT 2) AS t ORDER BY val");
		Assert.Equal("1", rows[0]["r"]?.ToString());
		Assert.Equal("1", rows[1]["r"]?.ToString());
		Assert.Equal("2", rows[2]["r"]?.ToString());
	}
	[Fact] public async Task Rank_Partitioned()
	{
		var rows = await Query($"SELECT id, dept, RANK() OVER (PARTITION BY dept ORDER BY amount DESC) AS r FROM `{_datasetId}.sales` ORDER BY dept, r");
		Assert.Equal("1", rows[0]["r"]?.ToString()); // A: 200 is highest
	}

	// ---- NTILE ----
	[Fact(Skip = "Not yet supported")] public async Task Ntile_Basic()
	{
		var rows = await Query($"SELECT id, NTILE(3) OVER (ORDER BY id) AS tile FROM `{_datasetId}.sales` ORDER BY id");
		Assert.Equal(6, rows.Count);
		Assert.Equal("1", rows[0]["tile"]?.ToString());
		Assert.Equal("2", rows[2]["tile"]?.ToString());
		Assert.Equal("3", rows[4]["tile"]?.ToString());
	}

	// ---- PERCENT_RANK / CUME_DIST ----
	[Fact(Skip = "Not yet supported")] public async Task PercentRank_Basic()
	{
		var rows = await Query($"SELECT id, PERCENT_RANK() OVER (ORDER BY amount) AS pr FROM `{_datasetId}.sales` ORDER BY id");
		Assert.Equal("0.0", rows[0]["pr"]?.ToString()); // first always 0
	}
	[Fact(Skip = "Not yet supported")] public async Task CumeDist_Basic()
	{
		var rows = await Query($"SELECT id, CUME_DIST() OVER (ORDER BY amount) AS cd FROM `{_datasetId}.sales` ORDER BY amount");
		var lastCd = double.Parse(rows[5]["cd"]?.ToString() ?? "0");
		Assert.Equal(1.0, lastCd); // last always 1.0
	}

	// ---- LAG / LEAD ----
	[Fact] public async Task Lag_Basic()
	{
		var rows = await Query($"SELECT id, LAG(amount) OVER (ORDER BY id) AS prev FROM `{_datasetId}.sales` ORDER BY id");
		Assert.Null(rows[0]["prev"]); // first has no previous
		Assert.Equal("100", rows[1]["prev"]?.ToString());
	}
	[Fact] public async Task Lag_WithDefault()
	{
		var rows = await Query($"SELECT id, LAG(amount, 1, -1) OVER (ORDER BY id) AS prev FROM `{_datasetId}.sales` ORDER BY id");
		Assert.Equal("-1", rows[0]["prev"]?.ToString());
	}
	[Fact] public async Task Lag_Offset2()
	{
		var rows = await Query($"SELECT id, LAG(amount, 2) OVER (ORDER BY id) AS prev2 FROM `{_datasetId}.sales` ORDER BY id");
		Assert.Null(rows[0]["prev2"]);
		Assert.Null(rows[1]["prev2"]);
		Assert.Equal("100", rows[2]["prev2"]?.ToString());
	}
	[Fact] public async Task Lead_Basic()
	{
		var rows = await Query($"SELECT id, LEAD(amount) OVER (ORDER BY id) AS nxt FROM `{_datasetId}.sales` ORDER BY id");
		Assert.Null(rows[5]["nxt"]); // last has no next
		Assert.Equal("200", rows[0]["nxt"]?.ToString());
	}
	[Fact] public async Task Lead_WithDefault()
	{
		var rows = await Query($"SELECT id, LEAD(amount, 1, -1) OVER (ORDER BY id) AS nxt FROM `{_datasetId}.sales` ORDER BY id");
		Assert.Equal("-1", rows[5]["nxt"]?.ToString());
	}
	[Fact] public async Task Lead_Partitioned()
	{
		var rows = await Query($"SELECT id, dept, LEAD(amount) OVER (PARTITION BY dept ORDER BY id) AS nxt FROM `{_datasetId}.sales` ORDER BY dept, id");
		Assert.Equal("200", rows[0]["nxt"]?.ToString()); // A group: 100 -> 200
		Assert.Null(rows[2]["nxt"]); // last in A partition
	}

	// ---- FIRST_VALUE / LAST_VALUE ----
	[Fact] public async Task FirstValue_Basic()
	{
		var rows = await Query($"SELECT id, FIRST_VALUE(amount) OVER (ORDER BY id) AS fv FROM `{_datasetId}.sales` ORDER BY id");
		Assert.Equal("100", rows[0]["fv"]?.ToString());
		Assert.Equal("100", rows[5]["fv"]?.ToString()); // same for all
	}
	[Fact] public async Task FirstValue_Partitioned()
	{
		var rows = await Query($"SELECT id, dept, FIRST_VALUE(amount) OVER (PARTITION BY dept ORDER BY id) AS fv FROM `{_datasetId}.sales` ORDER BY dept, id");
		Assert.Equal("100", rows[0]["fv"]?.ToString()); // A: first is 100
		Assert.Equal("300", rows[3]["fv"]?.ToString()); // B: first is 300
	}
	[Fact] public async Task LastValue_WithFrame()
	{
		var rows = await Query($"SELECT id, LAST_VALUE(amount) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv FROM `{_datasetId}.sales` ORDER BY id");
		Assert.Equal("350", rows[0]["lv"]?.ToString()); // all should see 350
	}

	// ---- NTH_VALUE ----
	[Fact] public async Task NthValue_Second()
	{
		var rows = await Query($"SELECT id, NTH_VALUE(amount, 2) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nv FROM `{_datasetId}.sales` ORDER BY id");
		Assert.Equal("200", rows[0]["nv"]?.ToString());
	}

	// ---- SUM / AVG / COUNT as window functions ----
	[Fact] public async Task SumWindow_RunningTotal()
	{
		var rows = await Query($"SELECT id, SUM(amount) OVER (ORDER BY id) AS running FROM `{_datasetId}.sales` ORDER BY id");
		Assert.Equal(100.0, double.Parse(rows[0]["running"]!.ToString()!));
		Assert.Equal(300.0, double.Parse(rows[1]["running"]!.ToString()!));
	}
	[Fact] public async Task AvgWindow_Partitioned()
	{
		var rows = await Query($"SELECT id, dept, AVG(amount) OVER (PARTITION BY dept) AS avg_dept FROM `{_datasetId}.sales` ORDER BY dept, id");
		Assert.Equal("150", rows[0]["avg_dept"]?.ToString()); // A avg: (100+200+150)/3 = 150
		Assert.Equal("300", rows[3]["avg_dept"]?.ToString()); // B avg: (300+250+350)/3 = 300
	}
	[Fact] public async Task CountWindow_Partitioned()
	{
		var rows = await Query($"SELECT id, dept, COUNT(*) OVER (PARTITION BY dept) AS cnt FROM `{_datasetId}.sales` ORDER BY dept, id");
		Assert.Equal("3", rows[0]["cnt"]?.ToString());
		Assert.Equal("3", rows[3]["cnt"]?.ToString());
	}

	// ---- Window frame: ROWS BETWEEN ----
	[Fact] public async Task WindowFrame_Preceding()
	{
		var rows = await Query($"SELECT id, SUM(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM `{_datasetId}.sales` ORDER BY id");
		Assert.Equal(100.0, double.Parse(rows[0]["s"]!.ToString()!)); // only 100
		Assert.Equal(300.0, double.Parse(rows[1]["s"]!.ToString()!)); // 100 + 200
	}
	[Fact] public async Task WindowFrame_Following()
	{
		var rows = await Query($"SELECT id, SUM(amount) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS s FROM `{_datasetId}.sales` ORDER BY id");
		Assert.Equal(300.0, double.Parse(rows[0]["s"]!.ToString()!)); // 100 + 200
		Assert.Equal(600.0, double.Parse(rows[4]["s"]!.ToString()!)); // 250 + 350 (for ids 5,6)
	}

	// ---- QUALIFY ----
	[Fact(Skip = "Not yet supported")] public async Task Qualify_FilterByRowNumber()
	{
		var rows = await Query($"SELECT id, dept, amount FROM `{_datasetId}.sales` QUALIFY ROW_NUMBER() OVER (PARTITION BY dept ORDER BY amount DESC) = 1 ORDER BY dept");
		Assert.Equal(2, rows.Count);
	}

	// ---- PERCENTILE_CONT / PERCENTILE_DISC ----
	[Fact(Skip = "Not yet supported")] public async Task PercentileCont_Median()
	{
		var v = await Scalar($"SELECT PERCENTILE_CONT(amount, 0.5) OVER () FROM `{_datasetId}.sales` LIMIT 1");
		Assert.NotNull(v);
	}
	[Fact(Skip = "Not yet supported")] public async Task PercentileDisc_Median()
	{
		var v = await Scalar($"SELECT PERCENTILE_DISC(amount, 0.5) OVER () FROM `{_datasetId}.sales` LIMIT 1");
		Assert.NotNull(v);
	}

	// ---- Multiple windows in same query ----
	[Fact] public async Task MultipleWindows()
	{
		var rows = await Query($"SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn, SUM(amount) OVER (ORDER BY id) AS running, RANK() OVER (ORDER BY amount DESC) AS r FROM `{_datasetId}.sales` ORDER BY id");
		Assert.Equal(6, rows.Count);
		Assert.Equal("1", rows[0]["rn"]?.ToString());
		Assert.NotNull(rows[0]["running"]);
		Assert.NotNull(rows[0]["r"]);
	}

	// ---- Named window ----
	[Fact(Skip = "Not yet supported")] public async Task NamedWindow()
	{
		var rows = await Query($"SELECT id, ROW_NUMBER() OVER w AS rn FROM `{_datasetId}.sales` WINDOW w AS (ORDER BY id) ORDER BY id");
		Assert.Equal("1", rows[0]["rn"]?.ToString());
	}
}
