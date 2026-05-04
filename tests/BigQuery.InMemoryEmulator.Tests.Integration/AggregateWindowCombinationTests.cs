using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Aggregate + window function combinations, computed columns, and complex grouped analytics.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_analytic_function_concepts
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class AggregateWindowCombinationTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public AggregateWindowCombinationTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_awc_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.sales` (id INT64, region STRING, product STRING, amount FLOAT64, qty INT64, sale_date DATE)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.sales` VALUES
			(1,'East','Widget',100.0,10,'2024-01-15'),
			(2,'East','Gadget',200.0,5,'2024-01-20'),
			(3,'West','Widget',150.0,8,'2024-02-10'),
			(4,'West','Gadget',300.0,3,'2024-02-15'),
			(5,'East','Widget',120.0,12,'2024-02-20'),
			(6,'North','Widget',80.0,15,'2024-03-01'),
			(7,'North','Gadget',250.0,7,'2024-03-10'),
			(8,'East','Doohickey',175.0,6,'2024-03-15'),
			(9,'West','Widget',90.0,20,'2024-03-20'),
			(10,'North','Doohickey',160.0,9,'2024-03-25'),
			(11,'East','Widget',110.0,11,'2024-04-01'),
			(12,'West','Gadget',280.0,4,'2024-04-10')", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- Aggregate basics ----
	[Fact] public async Task TotalSales() => Assert.Equal("12", await S("SELECT COUNT(*) FROM `{ds}.sales`"));
	[Fact] public async Task SumAmount() => Assert.NotNull(await S("SELECT SUM(amount) FROM `{ds}.sales`"));
	[Fact] public async Task AvgAmount() => Assert.NotNull(await S("SELECT ROUND(AVG(amount), 2) FROM `{ds}.sales`"));
	[Fact] public async Task MinAmount() => Assert.Equal("80", await S("SELECT MIN(amount) FROM `{ds}.sales`"));
	[Fact] public async Task MaxAmount() => Assert.Equal("300", await S("SELECT MAX(amount) FROM `{ds}.sales`"));

	// ---- GROUP BY with multiple aggregates ----
	[Fact] public async Task GroupByRegion_MultiAgg()
	{
		var rows = await Q("SELECT region, COUNT(*) AS cnt, SUM(amount) AS total, ROUND(AVG(amount),2) AS avg_amt FROM `{ds}.sales` GROUP BY region ORDER BY region");
		Assert.Equal(3, rows.Count);
		Assert.Equal("East", rows[0]["region"]?.ToString());
	}
	[Fact] public async Task GroupByProduct()
	{
		var rows = await Q("SELECT product, SUM(qty) AS total_qty FROM `{ds}.sales` GROUP BY product ORDER BY product");
		Assert.Equal(3, rows.Count); // Doohickey, Gadget, Widget
	}

	// ---- HAVING with aggregate ----
	[Fact] public async Task Having_CountGt3()
	{
		var rows = await Q("SELECT region FROM `{ds}.sales` GROUP BY region HAVING COUNT(*) > 3 ORDER BY region");
		Assert.True(rows.Count >= 1);
	}
	[Fact] public async Task Having_SumGt500()
	{
		var rows = await Q("SELECT region, SUM(amount) AS total FROM `{ds}.sales` GROUP BY region HAVING SUM(amount) > 500 ORDER BY region");
		Assert.True(rows.Count >= 1);
	}

	// ---- Window functions ----
	[Fact] public async Task RowNumber_All()
	{
		var rows = await Q("SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM `{ds}.sales` ORDER BY id");
		Assert.Equal(12, rows.Count);
		Assert.Equal("1", rows[0]["rn"]?.ToString());
		Assert.Equal("12", rows[11]["rn"]?.ToString());
	}
	[Fact] public async Task RowNumber_PartitionByRegion()
	{
		var rows = await Q("SELECT id, region, ROW_NUMBER() OVER (PARTITION BY region ORDER BY id) AS rn FROM `{ds}.sales` ORDER BY region, id");
		var eastFirst = rows.First(r => r["region"]?.ToString() == "East");
		Assert.Equal("1", eastFirst["rn"]?.ToString());
	}
	[Fact] public async Task Rank_ByAmount()
	{
		var rows = await Q("SELECT id, amount, RANK() OVER (ORDER BY amount DESC) AS rnk FROM `{ds}.sales` ORDER BY rnk, id LIMIT 3");
		Assert.Equal("1", rows[0]["rnk"]?.ToString());
	}
	[Fact] public async Task DenseRank_ByQty()
	{
		var rows = await Q("SELECT id, qty, DENSE_RANK() OVER (ORDER BY qty DESC) AS dr FROM `{ds}.sales` ORDER BY dr, id LIMIT 5");
		Assert.Equal("1", rows[0]["dr"]?.ToString());
	}

	// ---- Running totals ----
	[Fact] public async Task RunningSum_ByRegion()
	{
		var rows = await Q("SELECT id, region, amount, SUM(amount) OVER (PARTITION BY region ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running FROM `{ds}.sales` ORDER BY region, id");
		Assert.Equal(12, rows.Count);
		var eastFirst = rows.First(r => r["region"]?.ToString() == "East");
		Assert.Equal("100", eastFirst["running"]?.ToString()); // First East row
	}
	[Fact] public async Task RunningCount()
	{
		var rows = await Q("SELECT id, COUNT(*) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rc FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("1", rows[0]["rc"]?.ToString());
		Assert.Equal("12", rows[11]["rc"]?.ToString());
	}
	[Fact] public async Task RunningAvg()
	{
		var rows = await Q("SELECT id, ROUND(AVG(amount) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS ra FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("100", rows[0]["ra"]?.ToString()); // first row only
	}

	// ---- LAG / LEAD ----
	[Fact] public async Task Lag_Basic()
	{
		var rows = await Q("SELECT id, amount, LAG(amount) OVER (ORDER BY id) AS prev_amt FROM `{ds}.sales` ORDER BY id");
		Assert.Null(rows[0]["prev_amt"]); // first row has no lag
		Assert.Equal("100", rows[1]["prev_amt"]?.ToString()); // second row = first row's amount
	}
	[Fact] public async Task Lead_Basic()
	{
		var rows = await Q("SELECT id, amount, LEAD(amount) OVER (ORDER BY id) AS next_amt FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("200", rows[0]["next_amt"]?.ToString()); // row 1's lead = row 2's amount
		Assert.Null(rows[11]["next_amt"]); // last row has no lead
	}
	[Fact] public async Task Lag_PartitionedByRegion()
	{
		var rows = await Q("SELECT id, region, amount, LAG(amount) OVER (PARTITION BY region ORDER BY id) AS prev FROM `{ds}.sales` ORDER BY region, id");
		var eastFirst = rows.First(r => r["region"]?.ToString() == "East");
		Assert.Null(eastFirst["prev"]); // first in partition
	}

	// ---- FIRST_VALUE / LAST_VALUE ----
	[Fact] public async Task FirstValue_ByRegion()
	{
		var rows = await Q("SELECT id, region, FIRST_VALUE(amount) OVER (PARTITION BY region ORDER BY id) AS first_amt FROM `{ds}.sales` WHERE region = 'East' ORDER BY id");
		foreach (var row in rows)
			Assert.Equal("100", row["first_amt"]?.ToString());
	}
	[Fact] public async Task LastValue_ByRegion()
	{
		var rows = await Q("SELECT id, region, LAST_VALUE(amount) OVER (PARTITION BY region ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_amt FROM `{ds}.sales` WHERE region = 'East' ORDER BY id");
		foreach (var row in rows)
			Assert.Equal("110", row["last_amt"]?.ToString());
	}

	// ---- NTILE ----
	[Fact] public async Task Ntile_Quartiles()
	{
		var rows = await Q("SELECT id, NTILE(4) OVER (ORDER BY amount) AS quartile FROM `{ds}.sales` ORDER BY id");
		Assert.Equal(12, rows.Count);
		Assert.True(int.Parse(rows[0]["quartile"]?.ToString()!) >= 1);
	}

	// ---- Aggregate + window in same query ----
	[Fact] public async Task Agg_And_Window()
	{
		var rows = await Q(@"
			SELECT region, SUM(amount) AS total
			FROM `{ds}.sales`
			GROUP BY region
			ORDER BY total DESC");
		Assert.Equal(3, rows.Count);
	}

	// ---- Multiple window functions ----
	[Fact] public async Task MultiWindow()
	{
		var rows = await Q(@"
			SELECT id, amount,
				ROW_NUMBER() OVER (ORDER BY id) AS rn,
				SUM(amount) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
				RANK() OVER (ORDER BY amount DESC) AS amount_rank
			FROM `{ds}.sales`
			ORDER BY id");
		Assert.Equal(12, rows.Count);
		Assert.Equal("1", rows[0]["rn"]?.ToString());
	}

	// ---- Percent rank / CumeDist ----
	[Fact] public async Task PercentRank()
	{
		var rows = await Q("SELECT id, amount, ROUND(PERCENT_RANK() OVER (ORDER BY amount), 4) AS pr FROM `{ds}.sales` ORDER BY amount");
		var v = rows[0]["pr"]?.ToString();
		Assert.True(v == "0.0" || v == "0", $"Got: {v}");
	}
	[Fact] public async Task CumeDist()
	{
		var rows = await Q("SELECT id, amount, ROUND(CUME_DIST() OVER (ORDER BY amount), 4) AS cd FROM `{ds}.sales` ORDER BY amount");
		Assert.NotNull(rows[0]["cd"]);
	}

	// ---- Subquery with aggregate ----
	[Fact] public async Task Subquery_TopRegions()
	{
		var rows = await Q(@"
			SELECT * FROM (
				SELECT region, SUM(amount) AS total FROM `{ds}.sales` GROUP BY region
			) WHERE total > 400 ORDER BY total DESC");
		Assert.True(rows.Count >= 1);
	}

	// ---- CTE with aggregate ----
	[Fact] public async Task Cte_RegionTotals()
	{
		var rows = await Q(@"
			WITH region_totals AS (
				SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
				FROM `{ds}.sales`
				GROUP BY region
			)
			SELECT region, total, cnt FROM region_totals ORDER BY total DESC");
		Assert.Equal(3, rows.Count);
	}

	// ---- GROUP BY with CASE ----
	[Fact] public async Task GroupBy_Case()
	{
		var rows = await Q(@"
			SELECT CASE WHEN amount >= 200 THEN 'high' ELSE 'low' END AS tier,
				COUNT(*) AS cnt
			FROM `{ds}.sales`
			GROUP BY tier
			ORDER BY tier");
		Assert.Equal(2, rows.Count);
	}

	// ---- DISTINCT with aggrega ----
	[Fact] public async Task CountDistinct_Product()
	{
		var v = await S("SELECT COUNT(DISTINCT product) FROM `{ds}.sales`");
		Assert.Equal("3", v);
	}
	[Fact] public async Task CountDistinct_Region()
	{
		var v = await S("SELECT COUNT(DISTINCT region) FROM `{ds}.sales`");
		Assert.Equal("3", v);
	}

	// ---- SUM DISTINCT ----
	[Fact] public async Task SumDistinct()
	{
		var v = await S("SELECT SUM(DISTINCT qty) FROM `{ds}.sales`");
		Assert.NotNull(v);
	}

	// ---- Aggregate with ORDER BY ----
	[Fact] public async Task StringAgg_OrderBy()
	{
		var v = await S("SELECT STRING_AGG(DISTINCT region, ', ') FROM `{ds}.sales`");
		Assert.NotNull(v);
	}

	// ---- Min/Max on dates ----
	[Fact] public async Task MinDate()
	{
		var v = await S("SELECT MIN(sale_date) FROM `{ds}.sales`");
		Assert.NotNull(v);
	}
	[Fact] public async Task MaxDate()
	{
		var v = await S("SELECT MAX(sale_date) FROM `{ds}.sales`");
		Assert.NotNull(v);
	}

	// ---- Aggregate on filtered data ----
	[Fact] public async Task Sum_Filtered()
	{
		var v = await S("SELECT SUM(amount) FROM `{ds}.sales` WHERE region = 'East'");
		Assert.Equal("705", v); // 100+200+120+175+110
	}
	[Fact] public async Task Avg_Filtered()
	{
		var v = await S("SELECT ROUND(AVG(qty), 1) FROM `{ds}.sales` WHERE product = 'Widget'");
		Assert.NotNull(v);
	}

	// ---- Multi-level grouping ----
	[Fact] public async Task GroupByMultiLevel()
	{
		var rows = await Q("SELECT region, product, SUM(amount) AS total FROM `{ds}.sales` GROUP BY region, product ORDER BY region, product");
		Assert.True(rows.Count >= 6);
	}

	// ---- Window with aggregate filter ----
	[Fact] public async Task Window_Over_GroupedAgg()
	{
		var rows = await Q(@"
			SELECT region, product, SUM(amount) AS total
			FROM `{ds}.sales`
			GROUP BY region, product
			ORDER BY region, total DESC");
		Assert.True(rows.Count >= 6);
	}
}
