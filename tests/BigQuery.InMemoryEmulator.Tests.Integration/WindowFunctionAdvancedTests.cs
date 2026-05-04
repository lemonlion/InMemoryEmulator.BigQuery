using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Advanced window function tests covering OVER, PARTITION BY, ORDER BY, frames, and named windows.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class WindowFunctionAdvancedTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public WindowFunctionAdvancedTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_wfa_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.sales` (id INT64, region STRING, product STRING, amount FLOAT64, qty INT64, dt DATE)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.sales` (id, region, product, amount, qty, dt) VALUES
			(1,'East','A',100,10,DATE '2024-01-01'),(2,'East','B',200,20,DATE '2024-01-15'),
			(3,'East','A',150,15,DATE '2024-02-01'),(4,'West','A',300,30,DATE '2024-01-01'),
			(5,'West','B',250,25,DATE '2024-01-15'),(6,'West','A',350,35,DATE '2024-02-01'),
			(7,'East','C',50,5,DATE '2024-02-15'),(8,'West','C',75,8,DATE '2024-02-15'),
			(9,'East','A',120,12,DATE '2024-03-01'),(10,'West','B',280,28,DATE '2024-03-01')", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> Scalar(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Query(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- ROW_NUMBER ----
	[Fact] public async Task RowNumber_Simple()
	{
		var rows = await Query("SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("1", rows[0]["rn"]?.ToString());
		Assert.Equal("10", rows[9]["rn"]?.ToString());
	}
	[Fact] public async Task RowNumber_Partitioned()
	{
		var rows = await Query("SELECT id, region, ROW_NUMBER() OVER (PARTITION BY region ORDER BY id) AS rn FROM `{ds}.sales` ORDER BY region, id");
		var eastRows = rows.Where(r => r["region"]?.ToString() == "East").ToList();
		Assert.Equal("1", eastRows[0]["rn"]?.ToString());
		Assert.Equal("5", eastRows[4]["rn"]?.ToString());
	}

	// ---- RANK / DENSE_RANK ----
	[Fact] public async Task Rank_WithTies()
	{
		var rows = await Query("SELECT product, RANK() OVER (ORDER BY product) AS rnk FROM `{ds}.sales` ORDER BY product, id");
		Assert.Equal("1", rows[0]["rnk"]?.ToString()); // A
	}
	[Fact] public async Task DenseRank_Partitioned()
	{
		var rows = await Query("SELECT id, region, DENSE_RANK() OVER (PARTITION BY region ORDER BY product) AS dr FROM `{ds}.sales` ORDER BY region, product, id");
		Assert.NotNull(rows[0]["dr"]);
	}

	// ---- NTILE ----
	[Fact] public async Task Ntile_Basic()
	{
		var rows = await Query("SELECT id, NTILE(3) OVER (ORDER BY id) AS tile FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("1", rows[0]["tile"]?.ToString());
	}
	[Fact] public async Task Ntile_Partitioned()
	{
		var rows = await Query("SELECT id, NTILE(2) OVER (PARTITION BY region ORDER BY id) AS tile FROM `{ds}.sales` ORDER BY region, id");
		Assert.NotNull(rows[0]["tile"]);
	}

	// ---- LAG / LEAD ----
	[Fact] public async Task Lag_Default()
	{
		var rows = await Query("SELECT id, amount, LAG(amount) OVER (ORDER BY id) AS prev_amt FROM `{ds}.sales` ORDER BY id");
		Assert.Null(rows[0]["prev_amt"]);
		Assert.Equal("100", rows[1]["prev_amt"]?.ToString());
	}
	[Fact] public async Task Lag_WithOffset()
	{
		var rows = await Query("SELECT id, LAG(amount, 2) OVER (ORDER BY id) AS prev2 FROM `{ds}.sales` ORDER BY id");
		Assert.Null(rows[0]["prev2"]);
		Assert.Null(rows[1]["prev2"]);
		Assert.Equal("100", rows[2]["prev2"]?.ToString());
	}
	[Fact] public async Task Lag_WithDefault()
	{
		var rows = await Query("SELECT id, LAG(amount, 1, -1) OVER (ORDER BY id) AS prev FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("-1", rows[0]["prev"]?.ToString());
	}
	[Fact] public async Task Lead_Default()
	{
		var rows = await Query("SELECT id, amount, LEAD(amount) OVER (ORDER BY id) AS next_amt FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("200", rows[0]["next_amt"]?.ToString());
		Assert.Null(rows[9]["next_amt"]);
	}
	[Fact] public async Task Lead_Partitioned()
	{
		var rows = await Query("SELECT id, region, LEAD(amount) OVER (PARTITION BY region ORDER BY id) AS next_amt FROM `{ds}.sales` ORDER BY region, id");
		var eastRows = rows.Where(r => r["region"]?.ToString() == "East").ToList();
		Assert.Equal("200", eastRows[0]["next_amt"]?.ToString());
	}

	// ---- FIRST_VALUE / LAST_VALUE ----
	[Fact] public async Task FirstValue_Simple()
	{
		var rows = await Query("SELECT id, FIRST_VALUE(amount) OVER (ORDER BY id) AS first_amt FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("100", rows[0]["first_amt"]?.ToString());
		Assert.Equal("100", rows[9]["first_amt"]?.ToString());
	}
	[Fact] public async Task FirstValue_Partitioned()
	{
		var rows = await Query("SELECT id, region, FIRST_VALUE(amount) OVER (PARTITION BY region ORDER BY id) AS first_amt FROM `{ds}.sales` ORDER BY region, id");
		var westRows = rows.Where(r => r["region"]?.ToString() == "West").ToList();
		Assert.Equal("300", westRows[0]["first_amt"]?.ToString());
	}
	[Fact] public async Task LastValue_WithFrame()
	{
		var rows = await Query("SELECT id, LAST_VALUE(amount) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_amt FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("280", rows[0]["last_amt"]?.ToString());
	}

	// ---- NTH_VALUE ----
	[Fact] public async Task NthValue_Second()
	{
		var rows = await Query("SELECT id, NTH_VALUE(amount, 2) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_amt FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("200", rows[0]["second_amt"]?.ToString());
	}

	// ---- SUM / AVG / COUNT / MIN / MAX OVER ----
	[Fact] public async Task Sum_Over_All()
	{
		var v = await Scalar("SELECT SUM(amount) OVER () FROM `{ds}.sales` LIMIT 1");
		Assert.Equal("1875", v);
	}
	[Fact] public async Task Sum_Over_Partitioned()
	{
		var rows = await Query("SELECT id, region, SUM(amount) OVER (PARTITION BY region) AS region_total FROM `{ds}.sales` ORDER BY id");
		var eastTotal = rows.Where(r => r["region"]?.ToString() == "East").Select(r => r["region_total"]?.ToString()).First();
		Assert.Equal("620", eastTotal);
	}
	[Fact] public async Task Avg_Over_Ordered()
	{
		var rows = await Query("SELECT id, AVG(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS mavg FROM `{ds}.sales` ORDER BY id");
		Assert.NotNull(rows[0]["mavg"]);
	}
	[Fact] public async Task Count_Over_Partitioned()
	{
		var rows = await Query("SELECT id, region, COUNT(*) OVER (PARTITION BY region) AS cnt FROM `{ds}.sales` ORDER BY id");
		var eastCnt = rows.Where(r => r["region"]?.ToString() == "East").Select(r => r["cnt"]?.ToString()).First();
		Assert.Equal("5", eastCnt);
	}
	[Fact] public async Task Min_Over_Partitioned()
	{
		var rows = await Query("SELECT id, region, MIN(amount) OVER (PARTITION BY region) AS min_amt FROM `{ds}.sales` ORDER BY id");
		var eastMin = rows.Where(r => r["region"]?.ToString() == "East").Select(r => r["min_amt"]?.ToString()).First();
		Assert.Equal("50", eastMin);
	}
	[Fact] public async Task Max_Over_Partitioned()
	{
		var rows = await Query("SELECT id, region, MAX(amount) OVER (PARTITION BY region) AS max_amt FROM `{ds}.sales` ORDER BY id");
		var westMax = rows.Where(r => r["region"]?.ToString() == "West").Select(r => r["max_amt"]?.ToString()).First();
		Assert.Equal("350", westMax);
	}

	// ---- Running totals / cumulative ----
	[Fact] public async Task RunningSum()
	{
		var rows = await Query("SELECT id, amount, SUM(amount) OVER (ORDER BY id) AS running_total FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("100", rows[0]["running_total"]?.ToString());
		Assert.Equal("300", rows[1]["running_total"]?.ToString());
	}
	[Fact] public async Task RunningCount()
	{
		var rows = await Query("SELECT id, COUNT(*) OVER (ORDER BY id) AS running_cnt FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("1", rows[0]["running_cnt"]?.ToString());
		Assert.Equal("10", rows[9]["running_cnt"]?.ToString());
	}
	[Fact] public async Task RunningAvg()
	{
		var rows = await Query("SELECT id, AVG(amount) OVER (ORDER BY id) AS running_avg FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("100", rows[0]["running_avg"]?.ToString());
	}

	// ---- Window frames ----
	[Fact] public async Task Frame_RowsBetween()
	{
		var rows = await Query("SELECT id, SUM(qty) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("10", rows[0]["s"]?.ToString()); // Only first row
		Assert.Equal("30", rows[1]["s"]?.ToString()); // 10+20
		Assert.Equal("45", rows[2]["s"]?.ToString()); // 10+20+15
	}
	[Fact] public async Task Frame_RowsCurrentAndFollowing()
	{
		var rows = await Query("SELECT id, SUM(qty) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS s FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("30", rows[0]["s"]?.ToString()); // 10+20
	}
	[Fact] public async Task Frame_UnboundedPrecedingToCurrent()
	{
		var rows = await Query("SELECT id, MAX(amount) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("100", rows[0]["running_max"]?.ToString());
		Assert.Equal("200", rows[1]["running_max"]?.ToString());
		Assert.Equal("300", rows[3]["running_max"]?.ToString());
	}

	// ---- Multiple windows in same query ----
	[Fact] public async Task MultipleWindows()
	{
		var rows = await Query(@"SELECT id,
			ROW_NUMBER() OVER (ORDER BY id) AS rn,
			SUM(amount) OVER (PARTITION BY region) AS region_total,
			RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS region_rank
			FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("1", rows[0]["rn"]?.ToString());
		Assert.NotNull(rows[0]["region_total"]);
		Assert.NotNull(rows[0]["region_rank"]);
	}

	// ---- Window with WHERE ----
	[Fact] public async Task Window_WithWhere()
	{
		var rows = await Query("SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM `{ds}.sales` WHERE region = 'East' ORDER BY id");
		Assert.Equal(5, rows.Count);
		Assert.Equal("1", rows[0]["rn"]?.ToString());
		Assert.Equal("5", rows[4]["rn"]?.ToString());
	}

	// ---- CUME_DIST / PERCENT_RANK ----
	[Fact] public async Task CumeDist_Basic()
	{
		var rows = await Query("SELECT id, CUME_DIST() OVER (ORDER BY amount) AS cd FROM `{ds}.sales` ORDER BY amount");
		Assert.NotNull(rows[0]["cd"]);
	}
	[Fact] public async Task PercentRank_Basic()
	{
		var rows = await Query("SELECT id, PERCENT_RANK() OVER (ORDER BY amount) AS pr FROM `{ds}.sales` ORDER BY amount");
		Assert.Equal("0", rows[0]["pr"]?.ToString());
	}

	// ---- Window with DISTINCT aggregate ----
	[Fact] public async Task CountDistinct_Over()
	{
		var rows = await Query("SELECT id, COUNT(DISTINCT product) OVER (PARTITION BY region) AS distinct_products FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("3", rows[0]["distinct_products"]?.ToString());
	}
}
