using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Analytical patterns: running totals, moving averages, gaps-and-islands, lead/lag multi-step, percentiles.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class AnalyticalWindowCoverageTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public AnalyticalWindowCoverageTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_awc_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.sales` (id INT64, region STRING, month INT64, revenue INT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.sales` VALUES
			(1,'East',1,100),(2,'East',2,150),(3,'East',3,120),(4,'East',4,180),(5,'East',5,200),
			(6,'West',1,80),(7,'West',2,90),(8,'West',3,110),(9,'West',4,95),(10,'West',5,130),
			(11,'East',6,170),(12,'West',6,140)", parameters: null);

		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.events` (id INT64, user_id INT64, event_type STRING, seq INT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.events` VALUES
			(1,1,'login',1),(2,1,'view',2),(3,1,'purchase',3),(4,1,'logout',4),
			(5,2,'login',1),(6,2,'view',2),(7,2,'view',3),(8,2,'purchase',4),
			(9,3,'login',1),(10,3,'logout',2)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- Running totals ----
	[Fact] public async Task RunningTotal_Basic()
	{
		var rows = await Q("SELECT month, revenue, SUM(revenue) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Equal("100", rows[0]["running_total"]?.ToString());
		Assert.Equal("250", rows[1]["running_total"]?.ToString());
		Assert.Equal("370", rows[2]["running_total"]?.ToString());
		Assert.Equal("550", rows[3]["running_total"]?.ToString());
		Assert.Equal("750", rows[4]["running_total"]?.ToString());
		Assert.Equal("920", rows[5]["running_total"]?.ToString());
	}

	[Fact] public async Task RunningTotal_MultiPartition()
	{
		var rows = await Q("SELECT region, month, SUM(revenue) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rt FROM `{ds}.sales` ORDER BY region, month");
		Assert.Equal("East", rows[0]["region"]?.ToString());
		Assert.Equal("100", rows[0]["rt"]?.ToString());
		var westStart = rows.FindIndex(r => r["region"]?.ToString() == "West");
		Assert.Equal("80", rows[westStart]["rt"]?.ToString());
	}

	// ---- Moving average (3-month) ----
	[Fact] public async Task MovingAvg_3Month()
	{
		var rows = await Q("SELECT month, revenue, AVG(revenue) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ma3 FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.NotNull(rows[0]["ma3"]);
		Assert.Equal("100", rows[0]["ma3"]?.ToString());
	}

	// ---- LAG / LEAD ----
	[Fact] public async Task Lag_Basic()
	{
		var rows = await Q("SELECT month, revenue, LAG(revenue) OVER (PARTITION BY region ORDER BY month) AS prev_rev FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Null(rows[0]["prev_rev"]);
		Assert.Equal("100", rows[1]["prev_rev"]?.ToString());
		Assert.Equal("150", rows[2]["prev_rev"]?.ToString());
	}

	[Fact] public async Task Lead_Basic()
	{
		var rows = await Q("SELECT month, revenue, LEAD(revenue) OVER (PARTITION BY region ORDER BY month) AS next_rev FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Equal("150", rows[0]["next_rev"]?.ToString());
		Assert.Equal("120", rows[1]["next_rev"]?.ToString());
		Assert.Null(rows[5]["next_rev"]);
	}

	[Fact] public async Task Lag_WithOffset()
	{
		var rows = await Q("SELECT month, revenue, LAG(revenue, 2) OVER (PARTITION BY region ORDER BY month) AS prev2 FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Null(rows[0]["prev2"]);
		Assert.Null(rows[1]["prev2"]);
		Assert.Equal("100", rows[2]["prev2"]?.ToString());
	}

	[Fact] public async Task Lead_WithDefault()
	{
		var rows = await Q("SELECT month, revenue, LEAD(revenue, 1, 0) OVER (PARTITION BY region ORDER BY month) AS next_rev FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Equal("0", rows[5]["next_rev"]?.ToString());
	}

	// ---- Month-over-month change ----
	[Fact] public async Task MoM_Change()
	{
		var rows = await Q("SELECT month, revenue, revenue - LAG(revenue) OVER (PARTITION BY region ORDER BY month) AS mom_change FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Null(rows[0]["mom_change"]);
		Assert.Equal("50", rows[1]["mom_change"]?.ToString());
		Assert.Equal("-30", rows[2]["mom_change"]?.ToString());
	}

	// ---- FIRST_VALUE / LAST_VALUE ----
	[Fact] public async Task FirstValue_Basic()
	{
		var rows = await Q("SELECT month, revenue, FIRST_VALUE(revenue) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_rev FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		foreach (var row in rows)
			Assert.Equal("100", row["first_rev"]?.ToString());
	}

	[Fact] public async Task LastValue_Basic()
	{
		var rows = await Q("SELECT month, revenue, LAST_VALUE(revenue) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_rev FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		foreach (var row in rows)
			Assert.Equal("170", row["last_rev"]?.ToString());
	}

	// ---- NTH_VALUE ----
	[Fact] public async Task NthValue_Second()
	{
		var rows = await Q("SELECT month, NTH_VALUE(revenue, 2) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_val FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		foreach (var row in rows)
			Assert.Equal("150", row["second_val"]?.ToString());
	}

	// ---- NTILE ----
	[Fact] public async Task Ntile_Quartiles()
	{
		var rows = await Q("SELECT month, revenue, NTILE(4) OVER (PARTITION BY region ORDER BY revenue) AS quartile FROM `{ds}.sales` WHERE region = 'East' ORDER BY revenue");
		Assert.Equal("1", rows[0]["quartile"]?.ToString());
		Assert.Equal("4", rows[rows.Count - 1]["quartile"]?.ToString());
	}

	// ---- PERCENT_RANK / CUME_DIST ----
	[Fact] public async Task PercentRank_Basic()
	{
		var rows = await Q("SELECT month, revenue, PERCENT_RANK() OVER (PARTITION BY region ORDER BY revenue) AS prank FROM `{ds}.sales` WHERE region = 'East' ORDER BY revenue");
		Assert.Equal("0", rows[0]["prank"]?.ToString());
	}

	[Fact] public async Task CumeDist_Basic()
	{
		var rows = await Q("SELECT month, revenue, CUME_DIST() OVER (PARTITION BY region ORDER BY revenue) AS cdist FROM `{ds}.sales` WHERE region = 'East' ORDER BY revenue");
		Assert.NotNull(rows[0]["cdist"]);
		var lastVal = double.Parse(rows[rows.Count - 1]["cdist"]!.ToString()!);
		Assert.Equal(1.0, lastVal, 5);
	}

	// ---- ROW_NUMBER for dedup ----
	[Fact] public async Task RowNumber_Dedup()
	{
		var rows = await Q(@"
			SELECT region, month, revenue FROM (
				SELECT region, month, revenue, ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue DESC) AS rn
				FROM `{ds}.sales`
			) WHERE rn = 1 ORDER BY region");
		Assert.Equal(2, rows.Count);
		Assert.Equal("East", rows[0]["region"]?.ToString());
		Assert.Equal("200", rows[0]["revenue"]?.ToString());
	}

	// ---- Session tracking ----
	[Fact] public async Task Sequence_NextEvent()
	{
		var rows = await Q("SELECT user_id, event_type, LEAD(event_type) OVER (PARTITION BY user_id ORDER BY seq) AS next_event FROM `{ds}.events` WHERE user_id = 1 ORDER BY seq");
		Assert.Equal("login", rows[0]["event_type"]?.ToString());
		Assert.Equal("view", rows[0]["next_event"]?.ToString());
		Assert.Equal("view", rows[1]["event_type"]?.ToString());
		Assert.Equal("purchase", rows[1]["next_event"]?.ToString());
	}

	[Fact] public async Task Sequence_PrevEvent()
	{
		var rows = await Q("SELECT user_id, event_type, LAG(event_type) OVER (PARTITION BY user_id ORDER BY seq) AS prev_event FROM `{ds}.events` WHERE user_id = 2 ORDER BY seq");
		Assert.Null(rows[0]["prev_event"]);
		Assert.Equal("login", rows[1]["prev_event"]?.ToString());
	}

	// ---- Rank with ties ----
	[Fact] public async Task Rank_WithTies()
	{
		var rows = await Q(@"
			WITH data AS (
				SELECT 'A' AS name, 100 AS score UNION ALL
				SELECT 'B', 100 UNION ALL
				SELECT 'C', 90 UNION ALL
				SELECT 'D', 80
			)
			SELECT name, score, RANK() OVER (ORDER BY score DESC) AS rnk FROM data ORDER BY name");
		Assert.Equal("1", rows[0]["rnk"]?.ToString());
		Assert.Equal("1", rows[1]["rnk"]?.ToString());
		Assert.Equal("3", rows[2]["rnk"]?.ToString());
		Assert.Equal("4", rows[3]["rnk"]?.ToString());
	}

	[Fact] public async Task DenseRank_WithTies()
	{
		var rows = await Q(@"
			WITH data AS (
				SELECT 'A' AS name, 100 AS score UNION ALL
				SELECT 'B', 100 UNION ALL
				SELECT 'C', 90 UNION ALL
				SELECT 'D', 80
			)
			SELECT name, score, DENSE_RANK() OVER (ORDER BY score DESC) AS drnk FROM data ORDER BY name");
		Assert.Equal("1", rows[0]["drnk"]?.ToString());
		Assert.Equal("1", rows[1]["drnk"]?.ToString());
		Assert.Equal("2", rows[2]["drnk"]?.ToString());
		Assert.Equal("3", rows[3]["drnk"]?.ToString());
	}

	// ---- Window frame variants ----
	[Fact] public async Task Frame_Following()
	{
		var rows = await Q("SELECT month, revenue, SUM(revenue) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS sum_next3 FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Equal("370", rows[0]["sum_next3"]?.ToString());
		Assert.Equal("450", rows[1]["sum_next3"]?.ToString());
	}

	[Fact] public async Task Frame_BetweenPreceding()
	{
		var rows = await Q("SELECT month, revenue, SUM(revenue) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS sum3 FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Equal("250", rows[0]["sum3"]?.ToString());
		Assert.Equal("370", rows[1]["sum3"]?.ToString());
	}

	// ---- COUNT window ----
	[Fact] public async Task Count_Window()
	{
		var rows = await Q("SELECT month, COUNT(*) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_count FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Equal("1", rows[0]["cum_count"]?.ToString());
		Assert.Equal("2", rows[1]["cum_count"]?.ToString());
		Assert.Equal("6", rows[5]["cum_count"]?.ToString());
	}

	// ---- MIN/MAX window ----
	[Fact] public async Task Min_Window()
	{
		var rows = await Q("SELECT month, revenue, MIN(revenue) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Equal("100", rows[0]["running_min"]?.ToString());
		Assert.Equal("100", rows[1]["running_min"]?.ToString());
		Assert.Equal("100", rows[2]["running_min"]?.ToString());
	}

	[Fact] public async Task Max_Window()
	{
		var rows = await Q("SELECT month, revenue, MAX(revenue) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Equal("100", rows[0]["running_max"]?.ToString());
		Assert.Equal("150", rows[1]["running_max"]?.ToString());
		Assert.Equal("150", rows[2]["running_max"]?.ToString());
		Assert.Equal("180", rows[3]["running_max"]?.ToString());
		Assert.Equal("200", rows[4]["running_max"]?.ToString());
	}
}
