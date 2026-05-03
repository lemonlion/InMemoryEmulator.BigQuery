using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Deep integration tests for subqueries, CTEs, correlated subqueries, and recursive CTEs.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class SubqueryCteDeepTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public SubqueryCteDeepTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_sub_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();

		// Create orders table
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.orders` (
				id INT64,
				customer_id INT64,
				amount FLOAT64,
				status STRING
			)", parameters: null);

		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.orders` (id, customer_id, amount, status) VALUES
			(1, 100, 50.0, 'shipped'),
			(2, 100, 75.0, 'pending'),
			(3, 200, 30.0, 'shipped'),
			(4, 200, 120.0, 'shipped'),
			(5, 300, 200.0, 'cancelled'),
			(6, 300, 15.0, 'shipped'),
			(7, 100, 90.0, 'shipped')", parameters: null);

		// Create customers table
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.customers` (
				id INT64,
				name STRING,
				region STRING
			)", parameters: null);

		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.customers` (id, name, region) VALUES
			(100, 'Alice', 'North'),
			(200, 'Bob', 'South'),
			(300, 'Charlie', 'North'),
			(400, 'Diana', 'East')", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
		return result.ToList();
	}

	// ============================================================
	// Scalar subqueries
	// ============================================================

	[Fact]
	public async Task ScalarSubquery_InSelect()
	{
		var result = await Scalar("SELECT (SELECT COUNT(*) FROM `{ds}.orders`)");
		Assert.Equal("7", result);
	}

	[Fact]
	public async Task ScalarSubquery_InWhere()
	{
		var rows = await Query("SELECT id FROM `{ds}.orders` WHERE amount > (SELECT AVG(amount) FROM `{ds}.orders`) ORDER BY id");
		Assert.Equal(3, rows.Count);
	}

	[Fact]
	public async Task ScalarSubquery_WithCorrelation()
	{
		var rows = await Query(@"
			SELECT c.name,
				(SELECT SUM(o.amount) FROM `{ds}.orders` o WHERE o.customer_id = c.id) AS total
			FROM `{ds}.customers` c
			WHERE c.id IN (100, 200, 300)
			ORDER BY c.name");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}

	[Fact]
	public async Task ScalarSubquery_ReturnsNull_WhenNoRows()
	{
		var result = await Scalar("SELECT (SELECT name FROM `{ds}.customers` WHERE id = 999)");
		Assert.Null(result);
	}

	// ============================================================
	// EXISTS / NOT EXISTS
	// ============================================================

	[Fact]
	public async Task Exists_True()
	{
		var result = await Scalar("SELECT EXISTS(SELECT 1 FROM `{ds}.orders` WHERE status = 'shipped')");
		Assert.Equal("True", result);
	}

	[Fact]
	public async Task Exists_False()
	{
		var result = await Scalar("SELECT EXISTS(SELECT 1 FROM `{ds}.orders` WHERE status = 'returned')");
		Assert.Equal("False", result);
	}

	[Fact]
	public async Task NotExists_InWhere()
	{
		var rows = await Query(@"
			SELECT name FROM `{ds}.customers` c
			WHERE NOT EXISTS (SELECT 1 FROM `{ds}.orders` o WHERE o.customer_id = c.id)
			ORDER BY name");
		Assert.Single(rows);
		Assert.Equal("Diana", rows[0]["name"]?.ToString());
	}

	[Fact]
	public async Task Exists_Correlated()
	{
		var rows = await Query(@"
			SELECT name FROM `{ds}.customers` c
			WHERE EXISTS (SELECT 1 FROM `{ds}.orders` o WHERE o.customer_id = c.id AND o.amount > 100)
			ORDER BY name");
		Assert.Equal(2, rows.Count); // Bob (120) and Charlie (200)
	}

	// ============================================================
	// IN subquery
	// ============================================================

	[Fact]
	public async Task In_Subquery()
	{
		var rows = await Query(@"
			SELECT name FROM `{ds}.customers`
			WHERE id IN (SELECT customer_id FROM `{ds}.orders` WHERE status = 'cancelled')
			ORDER BY name");
		Assert.Single(rows);
		Assert.Equal("Charlie", rows[0]["name"]?.ToString());
	}

	[Fact]
	public async Task NotIn_Subquery()
	{
		var rows = await Query(@"
			SELECT name FROM `{ds}.customers`
			WHERE id NOT IN (SELECT customer_id FROM `{ds}.orders`)
			ORDER BY name");
		Assert.Single(rows);
		Assert.Equal("Diana", rows[0]["name"]?.ToString());
	}

	// ============================================================
	// ARRAY subquery
	// ============================================================

	[Fact]
	public async Task ArraySubquery_Basic()
	{
		var result = await Scalar("SELECT ARRAY_LENGTH(ARRAY(SELECT id FROM `{ds}.orders` WHERE customer_id = 100))");
		Assert.Equal("3", result);
	}

	[Fact]
	public async Task ArraySubquery_WithOrderBy()
	{
		var result = await Scalar("SELECT (ARRAY(SELECT amount FROM `{ds}.orders` WHERE customer_id = 100 ORDER BY amount DESC))[OFFSET(0)]");
		Assert.Equal("90", result);
	}

	// ============================================================
	// CTEs (Common Table Expressions)
	// ============================================================

	[Fact]
	public async Task Cte_Simple()
	{
		var result = await Scalar(@"
			WITH order_totals AS (
				SELECT customer_id, SUM(amount) AS total
				FROM `{ds}.orders`
				GROUP BY customer_id
			)
			SELECT MAX(total) FROM order_totals");
		Assert.Equal("215", result);
	}

	[Fact]
	public async Task Cte_MultipleCtesChained()
	{
		var rows = await Query(@"
			WITH shipped AS (
				SELECT customer_id, SUM(amount) AS shipped_total
				FROM `{ds}.orders`
				WHERE status = 'shipped'
				GROUP BY customer_id
			),
			enriched AS (
				SELECT c.name, s.shipped_total
				FROM `{ds}.customers` c
				JOIN shipped s ON c.id = s.customer_id
			)
			SELECT name, shipped_total FROM enriched ORDER BY shipped_total DESC");
		Assert.Equal(3, rows.Count);
	}

	[Fact]
	public async Task Cte_ReferencedMultipleTimes()
	{
		var result = await Scalar(@"
			WITH nums AS (SELECT * FROM UNNEST([1,2,3,4,5]) AS n)
			SELECT (SELECT SUM(n) FROM nums) + (SELECT COUNT(*) FROM nums)");
		Assert.Equal("20", result);
	}

	[Fact]
	public async Task Cte_WithJoin()
	{
		var rows = await Query(@"
			WITH big_orders AS (
				SELECT * FROM `{ds}.orders` WHERE amount >= 75
			)
			SELECT c.name, b.amount
			FROM big_orders b JOIN `{ds}.customers` c ON b.customer_id = c.id
			ORDER BY b.amount DESC");
		Assert.True(rows.Count >= 3);
	}

	// ============================================================
	// Recursive CTEs
	// ============================================================

	[Fact]
	public async Task RecursiveCte_GenerateSequence()
	{
		var result = await Scalar(@"
			WITH RECURSIVE seq AS (
				SELECT 1 AS n
				UNION ALL
				SELECT n + 1 FROM seq WHERE n < 10
			)
			SELECT MAX(n) FROM seq");
		Assert.Equal("10", result);
	}

	[Fact]
	public async Task RecursiveCte_Sum()
	{
		var result = await Scalar(@"
			WITH RECURSIVE seq AS (
				SELECT 1 AS n
				UNION ALL
				SELECT n + 1 FROM seq WHERE n < 5
			)
			SELECT SUM(n) FROM seq");
		Assert.Equal("15", result);
	}

	[Fact]
	public async Task RecursiveCte_Factorial()
	{
		var result = await Scalar(@"
			WITH RECURSIVE fact AS (
				SELECT 1 AS n, 1 AS f
				UNION ALL
				SELECT n + 1, f * (n + 1) FROM fact WHERE n < 5
			)
			SELECT MAX(f) FROM fact");
		Assert.Equal("120", result);
	}

	// ============================================================
	// Derived tables / inline views
	// ============================================================

	[Fact]
	public async Task DerivedTable_InFrom()
	{
		var result = await Scalar(@"
			SELECT MAX(total) FROM (
				SELECT customer_id, SUM(amount) AS total
				FROM `{ds}.orders`
				GROUP BY customer_id
			)");
		Assert.Equal("215", result);
	}

	[Fact]
	public async Task DerivedTable_Nested()
	{
		var result = await Scalar(@"
			SELECT cnt FROM (
				SELECT COUNT(*) AS cnt FROM (
					SELECT * FROM `{ds}.orders` WHERE status = 'shipped'
				)
			)");
		Assert.Equal("5", result);
	}

	// ============================================================
	// Subqueries in SELECT list
	// ============================================================

	[Fact]
	public async Task SubqueryInSelect_Count()
	{
		var rows = await Query(@"
			SELECT c.name,
				(SELECT COUNT(*) FROM `{ds}.orders` o WHERE o.customer_id = c.id) AS order_count
			FROM `{ds}.customers` c
			ORDER BY order_count DESC");
		Assert.Equal(4, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}

	[Fact]
	public async Task SubqueryInSelect_MaxAmount()
	{
		var rows = await Query(@"
			SELECT c.name,
				(SELECT MAX(o.amount) FROM `{ds}.orders` o WHERE o.customer_id = c.id) AS max_order
			FROM `{ds}.customers` c
			WHERE c.id IN (100, 200)
			ORDER BY c.name");
		Assert.Equal(2, rows.Count);
	}

	// ============================================================
	// Subqueries in HAVING
	// ============================================================

	[Fact]
	public async Task SubqueryInHaving()
	{
		var rows = await Query(@"
			SELECT customer_id, SUM(amount) AS total
			FROM `{ds}.orders`
			GROUP BY customer_id
			HAVING SUM(amount) > (SELECT AVG(amount) * 2 FROM `{ds}.orders`)");
		Assert.True(rows.Count >= 1);
	}

	// ============================================================
	// LATERAL / correlated
	// ============================================================

	[Fact]
	public async Task CorrelatedSubquery_InOrderBy()
	{
		var rows = await Query(@"
			SELECT name FROM `{ds}.customers` c
			WHERE c.id IN (100, 200, 300)
			ORDER BY (SELECT SUM(amount) FROM `{ds}.orders` o WHERE o.customer_id = c.id) DESC");
		Assert.Equal(3, rows.Count);
	}

	// ============================================================
	// WITH in DML
	// ============================================================

	[Fact]
	public async Task Cte_WithInsert()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.summary` (customer_id INT64, total FLOAT64)", parameters: null);

		await client.ExecuteQueryAsync($@"
			WITH totals AS (
				SELECT customer_id, SUM(amount) AS total
				FROM `{_datasetId}.orders`
				GROUP BY customer_id
			)
			INSERT INTO `{_datasetId}.summary` (customer_id, total)
			SELECT customer_id, total FROM totals", parameters: null);

		var result = await Scalar("SELECT COUNT(*) FROM `{ds}.summary`");
		Assert.Equal("3", result);
	}

	// ============================================================
	// UNION/INTERSECT/EXCEPT in subqueries
	// ============================================================

	[Fact]
	public async Task Union_InSubquery()
	{
		var result = await Scalar(@"
			SELECT COUNT(*) FROM (
				SELECT id FROM `{ds}.orders` WHERE status = 'shipped'
				UNION ALL
				SELECT id FROM `{ds}.orders` WHERE status = 'pending'
			)");
		Assert.Equal("6", result);
	}

	[Fact]
	public async Task Except_InSubquery()
	{
		var result = await Scalar(@"
			SELECT COUNT(*) FROM (
				SELECT customer_id FROM `{ds}.orders`
				EXCEPT DISTINCT
				SELECT id FROM `{ds}.customers` WHERE region = 'North'
			)");
		Assert.Equal("1", result); // 200 is the only one not in North
	}

	// ============================================================
	// Nested CTEs and complex combinations
	// ============================================================

	[Fact]
	public async Task Cte_NestedSubqueryInCte()
	{
		var result = await Scalar(@"
			WITH active_customers AS (
				SELECT DISTINCT customer_id FROM `{ds}.orders` WHERE status != 'cancelled'
			),
			customer_stats AS (
				SELECT customer_id, SUM(amount) AS total
				FROM `{ds}.orders`
				WHERE customer_id IN (SELECT customer_id FROM active_customers)
				GROUP BY customer_id
			)
			SELECT COUNT(*) FROM customer_stats WHERE total > 50");
		Assert.True(int.Parse(result!) >= 2);
	}

	[Fact]
	public async Task SubqueryWithWindowFunction()
	{
		var rows = await Query(@"
			SELECT * FROM (
				SELECT id, amount, ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn
				FROM `{ds}.orders`
			) WHERE rn <= 3");
		Assert.Equal(3, rows.Count);
	}

	[Fact]
	public async Task Cte_WithWindowFunction()
	{
		var rows = await Query(@"
			WITH ranked AS (
				SELECT id, customer_id, amount,
					RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rnk
				FROM `{ds}.orders`
			)
			SELECT id, amount FROM ranked WHERE rnk = 1 ORDER BY amount DESC");
		Assert.Equal(3, rows.Count);
	}
}
