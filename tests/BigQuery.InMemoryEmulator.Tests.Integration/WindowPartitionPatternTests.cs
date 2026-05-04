using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Window function partition patterns: ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class WindowPartitionPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public WindowPartitionPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_wpp_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.sales` (id INT64, region STRING, product STRING, amount INT64, qty INT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.sales` VALUES
			(1,'East','Widget',100,5),(2,'East','Gadget',200,3),(3,'East','Widget',150,7),
			(4,'West','Widget',300,2),(5,'West','Gadget',250,8),(6,'West','Widget',175,4),
			(7,'North','Gadget',120,6),(8,'North','Widget',80,9),(9,'North','Gadget',90,3),
			(10,'East','Gadget',180,5)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- ROW_NUMBER ----
	[Fact] public async Task RowNumber_NoPartition()
	{
		var rows = await Q("SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("1", rows[0]["rn"]?.ToString());
		Assert.Equal("10", rows[9]["rn"]?.ToString());
	}
	[Fact] public async Task RowNumber_ByRegion()
	{
		var rows = await Q("SELECT id, region, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn FROM `{ds}.sales` ORDER BY region, rn");
		Assert.Equal("1", rows[0]["rn"]?.ToString()); // First in East
	}

	// ---- RANK ----
	[Fact] public async Task Rank_WithTies()
	{
		var rows = await Q(@"
			SELECT product, RANK() OVER (ORDER BY product) AS rnk
			FROM `{ds}.sales`
			ORDER BY rnk, product");
		// Gadget x4 all rank 1, Widget x6 all rank 5
		Assert.Equal("1", rows[0]["rnk"]?.ToString());
	}
	[Fact] public async Task Rank_ByRegion()
	{
		var rows = await Q(@"
			SELECT id, region, amount, RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS rnk
			FROM `{ds}.sales`
			ORDER BY region, rnk");
		Assert.Equal("1", rows[0]["rnk"]?.ToString());
	}

	// ---- DENSE_RANK ----
	[Fact] public async Task DenseRank_NoGaps()
	{
		var rows = await Q(@"
			SELECT product, DENSE_RANK() OVER (ORDER BY product) AS drnk
			FROM `{ds}.sales`
			ORDER BY product, drnk");
		Assert.Equal(10, rows.Count);
		Assert.Equal("1", rows[0]["drnk"]?.ToString()); // Gadget
	}

	// ---- NTILE ----
	[Fact] public async Task Ntile_Quartiles()
	{
		var rows = await Q("SELECT id, NTILE(4) OVER (ORDER BY amount) AS quartile FROM `{ds}.sales` ORDER BY amount");
		Assert.Equal("1", rows[0]["quartile"]?.ToString()); // Lowest in Q1
		Assert.Equal("4", rows[9]["quartile"]?.ToString()); // Highest in Q4
	}
	[Fact] public async Task Ntile_ByRegion()
	{
		var rows = await Q("SELECT id, region, NTILE(2) OVER (PARTITION BY region ORDER BY amount) AS half FROM `{ds}.sales` ORDER BY region, half, amount");
		Assert.NotNull(rows[0]["half"]?.ToString());
	}

	// ---- LAG ----
	[Fact] public async Task Lag_Basic()
	{
		var rows = await Q("SELECT id, amount, LAG(amount) OVER (ORDER BY id) AS prev_amount FROM `{ds}.sales` ORDER BY id");
		Assert.Null(rows[0]["prev_amount"]); // First row has no previous
		Assert.Equal("100", rows[1]["prev_amount"]?.ToString()); // Previous is row 1
	}
	[Fact] public async Task Lag_WithOffset()
	{
		var rows = await Q("SELECT id, amount, LAG(amount, 2) OVER (ORDER BY id) AS prev2_amount FROM `{ds}.sales` ORDER BY id");
		Assert.Null(rows[0]["prev2_amount"]);
		Assert.Null(rows[1]["prev2_amount"]);
		Assert.Equal("100", rows[2]["prev2_amount"]?.ToString());
	}
	[Fact] public async Task Lag_WithDefault()
	{
		var rows = await Q("SELECT id, LAG(amount, 1, 0) OVER (ORDER BY id) AS prev FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("0", rows[0]["prev"]?.ToString()); // Default 0
	}

	// ---- LEAD ----
	[Fact] public async Task Lead_Basic()
	{
		var rows = await Q("SELECT id, amount, LEAD(amount) OVER (ORDER BY id) AS next_amount FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("200", rows[0]["next_amount"]?.ToString()); // Next is row 2
		Assert.Null(rows[9]["next_amount"]); // Last row
	}
	[Fact] public async Task Lead_WithOffset()
	{
		var rows = await Q("SELECT id, LEAD(amount, 2) OVER (ORDER BY id) AS next2 FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("150", rows[0]["next2"]?.ToString()); // Skip to row 3
	}

	// ---- FIRST_VALUE ----
	[Fact] public async Task FirstValue_Global()
	{
		var rows = await Q(@"
			SELECT id, FIRST_VALUE(amount) OVER (ORDER BY amount ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_amt
			FROM `{ds}.sales` ORDER BY amount");
		Assert.Equal("80", rows[0]["first_amt"]?.ToString()); // Lowest amount
		Assert.Equal("80", rows[9]["first_amt"]?.ToString()); // Still lowest
	}
	[Fact] public async Task FirstValue_ByRegion()
	{
		var rows = await Q(@"
			SELECT id, region, FIRST_VALUE(amount) OVER (PARTITION BY region ORDER BY amount ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_amt
			FROM `{ds}.sales` ORDER BY region, amount");
		// First value in each region partition
		Assert.NotNull(rows[0]["first_amt"]?.ToString());
	}

	// ---- LAST_VALUE ----
	[Fact] public async Task LastValue_WithFrame()
	{
		var rows = await Q(@"
			SELECT id, LAST_VALUE(amount) OVER (ORDER BY amount ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_amt
			FROM `{ds}.sales` ORDER BY amount");
		Assert.Equal("300", rows[0]["last_amt"]?.ToString()); // Highest amount
	}

	// ---- SUM window ----
	[Fact] public async Task Sum_Running()
	{
		var rows = await Q(@"
			SELECT id, amount,
				SUM(amount) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum
			FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("100", rows[0]["running_sum"]?.ToString()); // Just first
		Assert.Equal("300", rows[1]["running_sum"]?.ToString()); // 100+200
	}
	[Fact] public async Task Sum_ByRegion()
	{
		var rows = await Q(@"
			SELECT id, region, amount,
				SUM(amount) OVER (PARTITION BY region) AS region_total
			FROM `{ds}.sales` ORDER BY region, id");
		// All East rows should have same total
		var eastTotal = rows[0]["region_total"]?.ToString();
		Assert.Equal(eastTotal, rows[1]["region_total"]?.ToString());
	}

	// ---- AVG window ----
	[Fact] public async Task Avg_ByRegion()
	{
		var rows = await Q(@"
			SELECT region, amount,
				AVG(amount) OVER (PARTITION BY region) AS avg_amt
			FROM `{ds}.sales` ORDER BY region, amount");
		Assert.NotNull(rows[0]["avg_amt"]?.ToString());
	}

	// ---- COUNT window ----
	[Fact] public async Task Count_ByRegion()
	{
		var rows = await Q(@"
			SELECT region,
				COUNT(*) OVER (PARTITION BY region) AS cnt
			FROM `{ds}.sales` ORDER BY region");
		// East has 4
		Assert.Equal("4", rows[0]["cnt"]?.ToString());
	}

	// ---- MIN/MAX window ----
	[Fact] public async Task Min_ByRegion()
	{
		var rows = await Q(@"
			SELECT region, amount,
				MIN(amount) OVER (PARTITION BY region) AS min_amt
			FROM `{ds}.sales` ORDER BY region, amount");
		Assert.NotNull(rows[0]["min_amt"]?.ToString());
	}
	[Fact] public async Task Max_ByRegion()
	{
		var rows = await Q(@"
			SELECT region, amount,
				MAX(amount) OVER (PARTITION BY region) AS max_amt
			FROM `{ds}.sales` ORDER BY region, amount");
		Assert.NotNull(rows[0]["max_amt"]?.ToString());
	}

	// ---- Multiple window functions ----
	[Fact] public async Task Multiple_Windows()
	{
		var rows = await Q(@"
			SELECT id, region,
				ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn,
				RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS rnk,
				SUM(amount) OVER (PARTITION BY region) AS total
			FROM `{ds}.sales` ORDER BY region, rn");
		Assert.Equal("1", rows[0]["rn"]?.ToString());
		Assert.Equal("1", rows[0]["rnk"]?.ToString());
		Assert.NotNull(rows[0]["total"]?.ToString());
	}

	// ---- Window with WHERE ----
	[Fact] public async Task Window_WithWhere()
	{
		var rows = await Q(@"
			SELECT id, ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn
			FROM `{ds}.sales`
			WHERE region = 'East'
			ORDER BY rn");
		Assert.Equal(4, rows.Count);
		Assert.Equal("1", rows[0]["rn"]?.ToString());
	}

	// ---- PERCENT_RANK ----
	[Fact] public async Task PercentRank()
	{
		var rows = await Q("SELECT id, PERCENT_RANK() OVER (ORDER BY amount) AS pr FROM `{ds}.sales` ORDER BY amount");
		// First row is 0
		var pr0 = rows[0]["pr"]?.ToString();
		Assert.NotNull(pr0);
		Assert.StartsWith("0", pr0);
	}

	// ---- CUME_DIST ----
	[Fact] public async Task CumeDist()
	{
		var rows = await Q("SELECT id, CUME_DIST() OVER (ORDER BY amount) AS cd FROM `{ds}.sales` ORDER BY amount");
		Assert.NotNull(rows[0]["cd"]?.ToString());
	}
}
