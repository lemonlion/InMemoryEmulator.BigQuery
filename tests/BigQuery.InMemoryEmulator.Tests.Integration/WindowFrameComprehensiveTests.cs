using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for window function frames: ROWS, RANGE, GROUPS, UNBOUNDED, CURRENT ROW, N PRECEDING/FOLLOWING.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class WindowFrameComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public WindowFrameComprehensiveTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_wf_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.sales` (id INT64, region STRING, month INT64, revenue FLOAT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.sales` VALUES
			(1,'East',1,100),(2,'East',2,150),(3,'East',3,120),(4,'East',4,200),
			(5,'West',1,80),(6,'West',2,110),(7,'West',3,90),(8,'West',4,170),
			(9,'East',5,180),(10,'West',5,130)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- ROW_NUMBER ----
	[Fact] public async Task RowNumber_Global()
	{
		var rows = await Q("SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM `{ds}.sales` ORDER BY id");
		Assert.Equal(10, rows.Count);
		Assert.Equal("1", rows[0]["rn"]?.ToString());
		Assert.Equal("10", rows[9]["rn"]?.ToString());
	}
	[Fact] public async Task RowNumber_Partitioned()
	{
		var rows = await Q("SELECT region, month, ROW_NUMBER() OVER (PARTITION BY region ORDER BY month) AS rn FROM `{ds}.sales` ORDER BY region, month");
		var eastRows = rows.Where(r => r["region"]?.ToString() == "East").ToList();
		Assert.Equal("1", eastRows[0]["rn"]?.ToString());
		Assert.Equal("5", eastRows[4]["rn"]?.ToString());
	}

	// ---- RANK / DENSE_RANK ----
	[Fact] public async Task Rank_Basic()
	{
		var rows = await Q("SELECT revenue, RANK() OVER (ORDER BY revenue DESC) AS rnk FROM `{ds}.sales` ORDER BY rnk");
		Assert.Equal("1", rows[0]["rnk"]?.ToString());
	}
	[Fact] public async Task DenseRank_Basic()
	{
		var rows = await Q("SELECT revenue, DENSE_RANK() OVER (ORDER BY revenue DESC) AS drnk FROM `{ds}.sales` ORDER BY drnk");
		Assert.Equal("1", rows[0]["drnk"]?.ToString());
	}
	[Fact] public async Task Rank_Partitioned()
	{
		var rows = await Q("SELECT region, revenue, RANK() OVER (PARTITION BY region ORDER BY revenue DESC) AS rnk FROM `{ds}.sales` ORDER BY region, rnk");
		var eastFirst = rows.First(r => r["region"]?.ToString() == "East");
		Assert.Equal("1", eastFirst["rnk"]?.ToString());
	}

	// ---- SUM OVER with frames ----
	[Fact] public async Task Sum_RunningTotal()
	{
		var rows = await Q(@"
			SELECT month, revenue,
				SUM(revenue) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
			FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Equal("100", rows[0]["running_total"]?.ToString()); // month 1: 100
		Assert.Equal("250", rows[1]["running_total"]?.ToString()); // month 2: 100+150
		Assert.Equal("370", rows[2]["running_total"]?.ToString()); // month 3: 100+150+120
	}
	[Fact] public async Task Sum_MovingWindow()
	{
		var rows = await Q(@"
			SELECT month, revenue,
				SUM(revenue) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_sum
			FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Equal("250", rows[0]["moving_sum"]?.ToString()); // 100+150 (no preceding for first)
		Assert.Equal("370", rows[1]["moving_sum"]?.ToString()); // 100+150+120
	}
	[Fact] public async Task Sum_EntirePartition()
	{
		var rows = await Q(@"
			SELECT month, revenue,
				SUM(revenue) OVER (PARTITION BY region) AS total
			FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Equal("750", rows[0]["total"]?.ToString()); // sum of all East: 100+150+120+200+180
		Assert.Equal("750", rows[4]["total"]?.ToString());
	}

	// ---- AVG OVER ----
	[Fact] public async Task Avg_RunningAvg()
	{
		var rows = await Q(@"
			SELECT month, revenue,
				AVG(revenue) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg
			FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Equal("100", rows[0]["running_avg"]?.ToString()); // 100/1
		Assert.Equal("125", rows[1]["running_avg"]?.ToString()); // 250/2
	}
	[Fact] public async Task Avg_MovingAvg()
	{
		var rows = await Q(@"
			SELECT month, revenue,
				AVG(revenue) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ma3
			FROM `{ds}.sales` WHERE region = 'West' ORDER BY month");
		Assert.Equal("80", rows[0]["ma3"]?.ToString()); // 80
		Assert.Equal("95", rows[1]["ma3"]?.ToString()); // (80+110)/2
	}

	// ---- COUNT OVER ----
	[Fact] public async Task Count_Running()
	{
		var rows = await Q(@"
			SELECT month,
				COUNT(*) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cnt
			FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Equal("1", rows[0]["cnt"]?.ToString());
		Assert.Equal("5", rows[4]["cnt"]?.ToString());
	}
	[Fact] public async Task Count_Total()
	{
		var rows = await Q("SELECT month, COUNT(*) OVER (PARTITION BY region) AS total FROM `{ds}.sales` WHERE region = 'West' ORDER BY month");
		Assert.Equal("5", rows[0]["total"]?.ToString());
	}

	// ---- MIN / MAX OVER ----
	[Fact] public async Task Min_Running()
	{
		var rows = await Q(@"
			SELECT month, revenue,
				MIN(revenue) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_so_far
			FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Equal("100", rows[0]["min_so_far"]?.ToString());
		Assert.Equal("100", rows[2]["min_so_far"]?.ToString()); // still 100
	}
	[Fact] public async Task Max_Running()
	{
		var rows = await Q(@"
			SELECT month, revenue,
				MAX(revenue) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_so_far
			FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Equal("100", rows[0]["max_so_far"]?.ToString());
		Assert.Equal("150", rows[1]["max_so_far"]?.ToString());
		Assert.Equal("200", rows[3]["max_so_far"]?.ToString());
	}

	// ---- LAG / LEAD ----
	[Fact] public async Task Lag_Basic()
	{
		var rows = await Q(@"
			SELECT month, revenue, LAG(revenue) OVER (PARTITION BY region ORDER BY month) AS prev_rev
			FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Null(rows[0]["prev_rev"]);
		Assert.Equal("100", rows[1]["prev_rev"]?.ToString());
	}
	[Fact] public async Task Lag_Offset2()
	{
		var rows = await Q(@"
			SELECT month, revenue, LAG(revenue, 2) OVER (PARTITION BY region ORDER BY month) AS prev2
			FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Null(rows[0]["prev2"]);
		Assert.Null(rows[1]["prev2"]);
		Assert.Equal("100", rows[2]["prev2"]?.ToString());
	}
	[Fact] public async Task Lead_Basic()
	{
		var rows = await Q(@"
			SELECT month, revenue, LEAD(revenue) OVER (PARTITION BY region ORDER BY month) AS next_rev
			FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Equal("150", rows[0]["next_rev"]?.ToString());
		Assert.Null(rows[4]["next_rev"]);
	}
	[Fact] public async Task Lead_WithDefault()
	{
		var rows = await Q(@"
			SELECT month, revenue, LEAD(revenue, 1, 0) OVER (PARTITION BY region ORDER BY month) AS next_rev
			FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Equal("0", rows[4]["next_rev"]?.ToString());
	}

	// ---- FIRST_VALUE / LAST_VALUE ----
	[Fact] public async Task FirstValue_Partition()
	{
		var rows = await Q(@"
			SELECT month, revenue, FIRST_VALUE(revenue) OVER (PARTITION BY region ORDER BY month) AS first_rev
			FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Equal("100", rows[0]["first_rev"]?.ToString());
		Assert.Equal("100", rows[4]["first_rev"]?.ToString());
	}
	[Fact] public async Task LastValue_WithFrame()
	{
		var rows = await Q(@"
			SELECT month, revenue,
				LAST_VALUE(revenue) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_rev
			FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Equal("180", rows[0]["last_rev"]?.ToString()); // last East month is 5 (180)
	}

	// ---- NTH_VALUE ----
	[Fact] public async Task NthValue_Second()
	{
		var rows = await Q(@"
			SELECT month, revenue,
				NTH_VALUE(revenue, 2) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rev
			FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Equal("150", rows[0]["second_rev"]?.ToString());
	}

	// ---- NTILE ----
	[Fact] public async Task Ntile_Basic()
	{
		var rows = await Q("SELECT month, NTILE(2) OVER (PARTITION BY region ORDER BY month) AS tile FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Equal("1", rows[0]["tile"]?.ToString());
		Assert.Equal("2", rows[4]["tile"]?.ToString());
	}

	// ---- CUME_DIST ----
	[Fact] public async Task CumeDist_Basic()
	{
		var rows = await Q(@"
			SELECT revenue, CUME_DIST() OVER (PARTITION BY region ORDER BY revenue) AS cd
			FROM `{ds}.sales` WHERE region = 'East' ORDER BY revenue");
		Assert.Equal("1", rows[4]["cd"]?.ToString()); // last row = 1.0
	}

	// ---- PERCENT_RANK ----
	[Fact] public async Task PercentRank_Basic()
	{
		var rows = await Q(@"
			SELECT revenue, PERCENT_RANK() OVER (PARTITION BY region ORDER BY revenue) AS pr
			FROM `{ds}.sales` WHERE region = 'East' ORDER BY revenue");
		Assert.Equal("0", rows[0]["pr"]?.ToString()); // first row = 0
		Assert.Equal("1", rows[4]["pr"]?.ToString()); // last row = 1
	}

	// ---- Multiple window functions ----
	[Fact] public async Task Multiple_WindowFuncs()
	{
		var rows = await Q(@"
			SELECT month, revenue,
				ROW_NUMBER() OVER (PARTITION BY region ORDER BY month) AS rn,
			SUM(revenue) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running,
				LAG(revenue) OVER (PARTITION BY region ORDER BY month) AS prev
			FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.Equal("1", rows[0]["rn"]?.ToString());
		Assert.Equal("100", rows[0]["running"]?.ToString());
		Assert.Null(rows[0]["prev"]);
	}

	// ---- Window with different ORDER BY ----
	[Fact] public async Task Window_OrderByDesc()
	{
		var rows = await Q(@"
			SELECT month, revenue,
				ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue DESC) AS rnk
			FROM `{ds}.sales` WHERE region = 'East' ORDER BY rnk");
		Assert.Equal("200", rows[0]["revenue"]?.ToString());
	}

	// ---- ROWS BETWEEN N PRECEDING AND N FOLLOWING ----
	[Fact] public async Task Rows_Custom()
	{
		var rows = await Q(@"
			SELECT month, revenue,
				SUM(revenue) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) AS custom_sum
			FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
		Assert.NotNull(rows[0]["custom_sum"]);
	}
}
