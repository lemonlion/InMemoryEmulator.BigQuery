using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Complex multi-table join patterns: 3+ way joins, self-joins with aggregation, join + subquery, join + CTE.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#join_types
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class MultiTableJoinPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public MultiTableJoinPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_mtj_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.customers` (cid INT64, name STRING, city STRING)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.customers` VALUES
			(1,'Alice','NYC'),(2,'Bob','LA'),(3,'Carol','NYC'),(4,'Dave','Chicago'),(5,'Eve','LA')", parameters: null);
		
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.orders` (oid INT64, cid INT64, product STRING, amount FLOAT64, odate DATE)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.orders` VALUES
			(101,1,'Widget',50.0,'2024-01-10'),(102,1,'Gadget',70.0,'2024-01-15'),
			(103,2,'Widget',60.0,'2024-02-01'),(104,3,'Doohickey',90.0,'2024-02-10'),
			(105,3,'Widget',40.0,'2024-03-01'),(106,4,'Gadget',80.0,'2024-03-05'),
			(107,5,'Widget',55.0,'2024-03-10'),(108,1,'Doohickey',100.0,'2024-04-01')", parameters: null);

		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.products` (pid INT64, product STRING, category STRING, unit_cost FLOAT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.products` VALUES
			(1,'Widget','Hardware',10.0),(2,'Gadget','Electronics',20.0),(3,'Doohickey','Hardware',15.0)", parameters: null);

		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.categories` (category STRING, department STRING)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.categories` VALUES
			('Hardware','Engineering'),('Electronics','R&D'),('Software','IT')", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- 3-way join ----
	[Fact] public async Task ThreeWayJoin()
	{
		var rows = await Q(@"
			SELECT c.name, o.product, p.category
			FROM `{ds}.customers` c
			JOIN `{ds}.orders` o ON c.cid = o.cid
			JOIN `{ds}.products` p ON o.product = p.product
			ORDER BY c.name, o.product");
		Assert.Equal(8, rows.Count);
	}

	// ---- 4-way join ----
	[Fact] public async Task FourWayJoin()
	{
		var rows = await Q(@"
			SELECT c.name, o.product, p.category, cat.department
			FROM `{ds}.customers` c
			JOIN `{ds}.orders` o ON c.cid = o.cid
			JOIN `{ds}.products` p ON o.product = p.product
			JOIN `{ds}.categories` cat ON p.category = cat.category
			ORDER BY c.name, o.product");
		Assert.Equal(8, rows.Count);
	}

	// ---- 3-way with LEFT JOIN ----
	[Fact] public async Task ThreeWay_LeftJoin()
	{
		var rows = await Q(@"
			SELECT c.name, o.product, p.category
			FROM `{ds}.customers` c
			LEFT JOIN `{ds}.orders` o ON c.cid = o.cid
			LEFT JOIN `{ds}.products` p ON o.product = p.product
			ORDER BY c.name, o.product");
		Assert.True(rows.Count >= 8); // all orders + unmatched customers
	}

	// ---- Self-join ----
	[Fact] public async Task SelfJoin_SameCity()
	{
		var rows = await Q(@"
			SELECT a.name AS person1, b.name AS person2
			FROM `{ds}.customers` a
			JOIN `{ds}.customers` b ON a.city = b.city AND a.cid < b.cid
			ORDER BY a.name, b.name");
		Assert.True(rows.Count >= 2); // NYC: Alice+Carol, LA: Bob+Eve
	}

	// ---- Join with aggregate ----
	[Fact] public async Task Join_WithGroupBy()
	{
		var rows = await Q(@"
			SELECT c.name, COUNT(o.oid) AS order_count, SUM(o.amount) AS total
			FROM `{ds}.customers` c
			JOIN `{ds}.orders` o ON c.cid = o.cid
			GROUP BY c.name
			ORDER BY total DESC");
		Assert.Equal(5, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString()); // 50+70+100=220
	}

	// ---- Join + HAVING ----
	[Fact] public async Task Join_Having()
	{
		var rows = await Q(@"
			SELECT c.name, COUNT(*) AS cnt
			FROM `{ds}.customers` c
			JOIN `{ds}.orders` o ON c.cid = o.cid
			GROUP BY c.name
			HAVING COUNT(*) > 1
			ORDER BY c.name");
		Assert.True(rows.Count >= 2); // Alice(3), Carol(2)
	}

	// ---- Join + subquery ----
	[Fact] public async Task Join_Subquery()
	{
		var rows = await Q(@"
			SELECT c.name, sub.total
			FROM `{ds}.customers` c
			JOIN (
				SELECT cid, SUM(amount) AS total FROM `{ds}.orders` GROUP BY cid
			) sub ON c.cid = sub.cid
			ORDER BY sub.total DESC");
		Assert.Equal(5, rows.Count);
	}

	// ---- Join + CTE ----
	[Fact] public async Task Join_Cte()
	{
		var rows = await Q(@"
			WITH order_totals AS (
				SELECT cid, SUM(amount) AS total FROM `{ds}.orders` GROUP BY cid
			)
			SELECT c.name, ot.total
			FROM `{ds}.customers` c
			JOIN order_totals ot ON c.cid = ot.cid
			ORDER BY ot.total DESC");
		Assert.Equal(5, rows.Count);
	}

	// ---- Join with WHERE ----
	[Fact] public async Task Join_WithWhere()
	{
		var rows = await Q(@"
			SELECT c.name, o.product, o.amount
			FROM `{ds}.customers` c
			JOIN `{ds}.orders` o ON c.cid = o.cid
			WHERE c.city = 'NYC' AND o.amount > 50
			ORDER BY o.amount DESC");
		Assert.True(rows.Count >= 2);
	}

	// ---- Join with ORDER BY + LIMIT ----
	[Fact] public async Task Join_OrderLimit()
	{
		var rows = await Q(@"
			SELECT c.name, o.amount
			FROM `{ds}.customers` c
			JOIN `{ds}.orders` o ON c.cid = o.cid
			ORDER BY o.amount DESC
			LIMIT 3");
		Assert.Equal(3, rows.Count);
		Assert.Equal("100", rows[0]["amount"]?.ToString());
	}

	// ---- CROSS JOIN ----
	[Fact] public async Task CrossJoin()
	{
		var rows = await Q(@"
			SELECT c.name, p.product
			FROM `{ds}.customers` c
			CROSS JOIN `{ds}.products` p
			WHERE c.cid <= 2
			ORDER BY c.name, p.product");
		Assert.Equal(6, rows.Count); // 2 customers * 3 products
	}

	// ---- Anti-join (LEFT JOIN + IS NULL) ----
	[Fact] public async Task AntiJoin_NoOrders()
	{
		// All customers have orders in this dataset, so result should be 0
		var rows = await Q(@"
			SELECT c.name
			FROM `{ds}.customers` c
			LEFT JOIN `{ds}.orders` o ON c.cid = o.cid
			WHERE o.oid IS NULL");
		Assert.Empty(rows);
	}

	// ---- Semi-join (EXISTS) ----
	[Fact] public async Task SemiJoin_Exists()
	{
		var rows = await Q(@"
			SELECT c.name
			FROM `{ds}.customers` c
			WHERE EXISTS (SELECT 1 FROM `{ds}.orders` o WHERE o.cid = c.cid AND o.amount > 80)
			ORDER BY c.name");
		Assert.True(rows.Count >= 2); // Alice(100), Carol(90), Dave(80)
	}

	// ---- Join with CASE ----
	[Fact] public async Task Join_WithCase()
	{
		var rows = await Q(@"
			SELECT c.name, o.amount,
				CASE WHEN o.amount >= 80 THEN 'high' ELSE 'normal' END AS tier
			FROM `{ds}.customers` c
			JOIN `{ds}.orders` o ON c.cid = o.cid
			ORDER BY o.amount DESC LIMIT 5");
		Assert.Equal(5, rows.Count);
		Assert.Equal("high", rows[0]["tier"]?.ToString());
	}

	// ---- Join + window function ----
	[Fact] public async Task Join_Window()
	{
		var rows = await Q(@"
			SELECT c.name, o.amount,
				ROW_NUMBER() OVER (PARTITION BY c.cid ORDER BY o.amount DESC) AS rn
			FROM `{ds}.customers` c
			JOIN `{ds}.orders` o ON c.cid = o.cid
			ORDER BY c.name, rn");
		Assert.Equal(8, rows.Count);
	}

	// ---- Top-N per group via window ----
	[Fact] public async Task TopN_PerCustomer()
	{
		var rows = await Q(@"
			SELECT name, product, amount FROM (
				SELECT c.name, o.product, o.amount,
					ROW_NUMBER() OVER (PARTITION BY c.cid ORDER BY o.amount DESC) AS rn
				FROM `{ds}.customers` c
				JOIN `{ds}.orders` o ON c.cid = o.cid
			) WHERE rn = 1
			ORDER BY amount DESC");
		Assert.Equal(5, rows.Count); // one per customer
	}

	// ---- Join on multiple conditions ----
	[Fact] public async Task Join_MultiCondition()
	{
		var rows = await Q(@"
			SELECT c.name, o.product
			FROM `{ds}.customers` c
			JOIN `{ds}.orders` o ON c.cid = o.cid AND o.amount > 60
			ORDER BY c.name");
		Assert.True(rows.Count >= 4);
	}

	// ---- NOT EXISTS (anti-pattern) ----
	[Fact] public async Task NotExists()
	{
		var rows = await Q(@"
			SELECT p.product
			FROM `{ds}.products` p
			WHERE NOT EXISTS (
				SELECT 1 FROM `{ds}.categories` cat WHERE cat.category = p.category
			)");
		Assert.Empty(rows); // all product categories exist in categories table
	}

	// ---- Join with DISTINCT ----
	[Fact] public async Task Join_Distinct()
	{
		var rows = await Q(@"
			SELECT DISTINCT c.city
			FROM `{ds}.customers` c
			JOIN `{ds}.orders` o ON c.cid = o.cid
			ORDER BY c.city");
		Assert.Equal(3, rows.Count); // NYC, LA, Chicago
	}

	// ---- Join with IN subquery ----
	[Fact] public async Task Join_InSubquery()
	{
		var rows = await Q(@"
			SELECT name FROM `{ds}.customers`
			WHERE cid IN (SELECT cid FROM `{ds}.orders` WHERE amount > 80)
			ORDER BY name");
		Assert.True(rows.Count >= 2);
	}

	// ---- Correlated subquery with join data ----
	[Fact] public async Task CorrelatedSubquery()
	{
		var rows = await Q(@"
			SELECT c.name,
				(SELECT COUNT(*) FROM `{ds}.orders` o WHERE o.cid = c.cid) AS order_cnt
			FROM `{ds}.customers` c
			ORDER BY order_cnt DESC, c.name");
		Assert.Equal(5, rows.Count);
	}

	// ---- LEFT JOIN with aggregate ----
	[Fact] public async Task LeftJoin_Aggregate()
	{
		var rows = await Q(@"
			SELECT c.name, COALESCE(SUM(o.amount), 0) AS total
			FROM `{ds}.customers` c
			LEFT JOIN `{ds}.orders` o ON c.cid = o.cid
			GROUP BY c.name
			ORDER BY total DESC");
		Assert.Equal(5, rows.Count);
	}

	// ---- JOIN + UNION ----
	[Fact] public async Task Join_Union()
	{
		var rows = await Q(@"
			SELECT c.name, 'order' AS source FROM `{ds}.customers` c JOIN `{ds}.orders` o ON c.cid = o.cid WHERE o.product = 'Widget'
			UNION ALL
			SELECT c.name, 'gadget' AS source FROM `{ds}.customers` c JOIN `{ds}.orders` o ON c.cid = o.cid WHERE o.product = 'Gadget'
			ORDER BY name, source");
		Assert.True(rows.Count >= 6);
	}
}
