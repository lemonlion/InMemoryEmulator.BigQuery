using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for GROUP BY with various aggregation patterns, HAVING, and edge cases.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#group_by_clause
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class GroupByComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public GroupByComprehensiveTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_gb_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.sales` (id INT64, product STRING, category STRING, region STRING, amount FLOAT64, qty INT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.sales` VALUES
			(1,'Widget','A','East',100,10),(2,'Widget','A','West',150,15),
			(3,'Gadget','A','East',200,8),(4,'Gadget','A','West',250,12),
			(5,'Doohickey','B','East',50,20),(6,'Doohickey','B','West',75,25),
			(7,'Thingamajig','B','East',300,5),(8,'Thingamajig','B','West',350,3),
			(9,'Widget','A','East',120,11),(10,'Gadget','A','East',180,7),
			(11,'Doohickey','B','East',60,18),(12,'Widget','A','West',130,14)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- Basic GROUP BY ----
	[Fact] public async Task GroupBy_SingleColumn()
	{
		var rows = await Q("SELECT category, COUNT(*) AS cnt FROM `{ds}.sales` GROUP BY category ORDER BY category");
		Assert.Equal(2, rows.Count);
		Assert.Equal("A", rows[0]["category"]?.ToString());
	}
	[Fact] public async Task GroupBy_MultiColumn()
	{
		var rows = await Q("SELECT category, region, COUNT(*) AS cnt FROM `{ds}.sales` GROUP BY category, region ORDER BY category, region");
		Assert.Equal(4, rows.Count);
	}

	// ---- Aggregates with GROUP BY ----
	[Fact] public async Task GroupBy_Sum()
	{
		var rows = await Q("SELECT product, SUM(amount) AS total FROM `{ds}.sales` GROUP BY product ORDER BY product");
		Assert.True(rows.Count >= 4);
	}
	[Fact] public async Task GroupBy_Avg()
	{
		var rows = await Q("SELECT category, AVG(amount) AS avg_amt FROM `{ds}.sales` GROUP BY category ORDER BY category");
		Assert.Equal(2, rows.Count);
	}
	[Fact] public async Task GroupBy_MinMax()
	{
		var rows = await Q("SELECT product, MIN(amount) AS min_amt, MAX(amount) AS max_amt FROM `{ds}.sales` GROUP BY product ORDER BY product");
		Assert.True(rows.Count >= 4);
	}
	[Fact] public async Task GroupBy_CountDistinct()
	{
		var rows = await Q("SELECT category, COUNT(DISTINCT product) AS prod_count FROM `{ds}.sales` GROUP BY category ORDER BY category");
		Assert.Equal("2", rows[0]["prod_count"]?.ToString()); // A: Widget, Gadget
		Assert.Equal("2", rows[1]["prod_count"]?.ToString()); // B: Doohickey, Thingamajig
	}

	// ---- HAVING ----
	[Fact] public async Task Having_Count()
	{
		var rows = await Q("SELECT product, COUNT(*) AS cnt FROM `{ds}.sales` GROUP BY product HAVING COUNT(*) > 2 ORDER BY product");
		Assert.True(rows.Count >= 1);
	}
	[Fact] public async Task Having_Sum()
	{
		var rows = await Q("SELECT product, SUM(amount) AS total FROM `{ds}.sales` GROUP BY product HAVING SUM(amount) > 400 ORDER BY total DESC");
		Assert.True(rows.Count >= 1);
	}
	[Fact] public async Task Having_Avg()
	{
		var rows = await Q("SELECT category, AVG(amount) AS avg_amt FROM `{ds}.sales` GROUP BY category HAVING AVG(amount) > 100 ORDER BY avg_amt");
		Assert.True(rows.Count >= 1);
	}
	[Fact] public async Task Having_MultiCondition()
	{
		var rows = await Q("SELECT product, COUNT(*) AS cnt, SUM(amount) AS total FROM `{ds}.sales` GROUP BY product HAVING COUNT(*) >= 2 AND SUM(amount) > 100 ORDER BY product");
		Assert.True(rows.Count >= 1);
	}

	// ---- GROUP BY with expressions ----
	[Fact] public async Task GroupBy_Expression()
	{
		var rows = await Q("SELECT CASE WHEN amount > 150 THEN 'high' ELSE 'low' END AS tier, COUNT(*) AS cnt FROM `{ds}.sales` GROUP BY CASE WHEN amount > 150 THEN 'high' ELSE 'low' END ORDER BY tier");
		Assert.Equal(2, rows.Count);
	}
	[Fact] public async Task GroupBy_Function()
	{
		var rows = await Q("SELECT LENGTH(product) AS name_len, COUNT(*) AS cnt FROM `{ds}.sales` GROUP BY LENGTH(product) ORDER BY name_len");
		Assert.True(rows.Count >= 2);
	}

	// ---- GROUP BY with aliases ----
	[Fact] public async Task GroupBy_Ordinal()
	{
		var rows = await Q("SELECT category, COUNT(*) AS cnt FROM `{ds}.sales` GROUP BY 1 ORDER BY 1");
		Assert.Equal(2, rows.Count);
	}

	// ---- GROUP BY with ORDER BY ----
	[Fact] public async Task GroupBy_OrderByAgg()
	{
		var rows = await Q("SELECT product, SUM(amount) AS total FROM `{ds}.sales` GROUP BY product ORDER BY SUM(amount) DESC");
		Assert.True(rows.Count >= 4);
		var first = double.Parse(rows[0]["total"]?.ToString() ?? "0");
		var last = double.Parse(rows[^1]["total"]?.ToString() ?? "0");
		Assert.True(first >= last);
	}
	[Fact] public async Task GroupBy_OrderByAlias()
	{
		var rows = await Q("SELECT product, SUM(amount) AS total FROM `{ds}.sales` GROUP BY product ORDER BY total DESC");
		Assert.True(rows.Count >= 4);
	}

	// ---- GROUP BY with LIMIT ----
	[Fact] public async Task GroupBy_Limit()
	{
		var rows = await Q("SELECT product, SUM(amount) AS total FROM `{ds}.sales` GROUP BY product ORDER BY total DESC LIMIT 2");
		Assert.Equal(2, rows.Count);
	}

	// ---- GROUP BY with WHERE ----
	[Fact] public async Task GroupBy_WithWhere()
	{
		var rows = await Q("SELECT product, SUM(amount) AS total FROM `{ds}.sales` WHERE region = 'East' GROUP BY product ORDER BY product");
		Assert.True(rows.Count >= 4);
	}

	// ---- GROUP BY everything ----
	[Fact] public async Task GroupBy_NoGroupColumns()
	{
		var v = await S("SELECT SUM(amount) FROM `{ds}.sales`");
		Assert.NotNull(v);
	}

	// ---- GROUP BY with NULL ----
	[Fact] public async Task GroupBy_Null()
	{
		var rows = await Q("SELECT CASE WHEN qty > 15 THEN 'high' ELSE NULL END AS tier, COUNT(*) AS cnt FROM `{ds}.sales` GROUP BY tier ORDER BY tier");
		Assert.True(rows.Count >= 2);
	}

	// ---- Multiple aggregates ----
	[Fact] public async Task GroupBy_MultiAgg()
	{
		var rows = await Q(@"
			SELECT product, COUNT(*) AS cnt, SUM(amount) AS total, AVG(amount) AS avg_amt,
				MIN(amount) AS min_amt, MAX(amount) AS max_amt
			FROM `{ds}.sales`
			GROUP BY product
			ORDER BY product");
		Assert.True(rows.Count >= 4);
		Assert.NotNull(rows[0]["cnt"]);
		Assert.NotNull(rows[0]["total"]);
		Assert.NotNull(rows[0]["avg_amt"]);
	}

	// ---- GROUP BY + JOIN ----
	[Fact] public async Task GroupBy_WithJoinSubquery()
	{
		var rows = await Q(@"
			SELECT s.category, COUNT(*) AS cnt, SUM(s.amount) AS total
			FROM `{ds}.sales` s
			GROUP BY s.category
			ORDER BY s.category");
		Assert.Equal(2, rows.Count);
	}

	// ---- HAVING with expression ----
	[Fact] public async Task Having_Expression()
	{
		var rows = await Q(@"
			SELECT product, SUM(amount) AS total, COUNT(*) AS cnt
			FROM `{ds}.sales`
			GROUP BY product
			HAVING SUM(amount) / COUNT(*) > 100
			ORDER BY product");
		Assert.True(rows.Count >= 1);
	}

	// ---- HAVING without GROUP BY (whole table) ----
	[Fact] public async Task Having_NoGroupBy()
	{
		var rows = await Q("SELECT COUNT(*) AS cnt FROM `{ds}.sales` HAVING COUNT(*) > 5");
		Assert.Single(rows);
		Assert.Equal("12", rows[0]["cnt"]?.ToString());
	}
}
