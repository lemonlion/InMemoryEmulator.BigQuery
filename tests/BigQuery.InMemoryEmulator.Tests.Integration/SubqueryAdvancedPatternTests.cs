using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for advanced subquery patterns: correlated, scalar, EXISTS, IN, lateral.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class SubqueryAdvancedPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public SubqueryAdvancedPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_sub_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.products` (id INT64, name STRING, category STRING, price FLOAT64)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.products` (id, name, category, price) VALUES
			(1, 'Laptop', 'Electronics', 999.99),
			(2, 'Phone', 'Electronics', 699.99),
			(3, 'Tablet', 'Electronics', 499.99),
			(4, 'Desk', 'Furniture', 299.99),
			(5, 'Chair', 'Furniture', 199.99),
			(6, 'Lamp', 'Furniture', 49.99),
			(7, 'Book', 'Education', 29.99),
			(8, 'Course', 'Education', 199.99)", parameters: null);

		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.orders` (id INT64, product_id INT64, quantity INT64, discount FLOAT64)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.orders` (id, product_id, quantity, discount) VALUES
			(1, 1, 2, 0.1), (2, 2, 5, 0.0), (3, 3, 3, 0.05),
			(4, 4, 1, 0.0), (5, 5, 4, 0.15), (6, 1, 1, 0.2),
			(7, 7, 10, 0.0), (8, 2, 3, 0.1)", parameters: null);
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

	// Scalar subqueries
	[Fact] public async Task ScalarSubquery_InSelect()
	{
		var result = await Scalar("SELECT (SELECT MAX(price) FROM `{ds}.products`)");
		Assert.Equal("999.99", result);
	}

	[Fact] public async Task ScalarSubquery_InWhere()
	{
		var rows = await Query("SELECT name FROM `{ds}.products` WHERE price > (SELECT AVG(price) FROM `{ds}.products`) ORDER BY price DESC");
		Assert.True(rows.Count >= 2);
		Assert.Equal("Laptop", rows[0]["name"].ToString());
	}

	[Fact] public async Task ScalarSubquery_WithFilter()
	{
		var result = await Scalar("SELECT (SELECT COUNT(*) FROM `{ds}.products` WHERE category = 'Electronics')");
		Assert.Equal("3", result);
	}

	// EXISTS subqueries
	[Fact] public async Task Exists_True()
	{
		var rows = await Query(@"
			SELECT p.name FROM `{ds}.products` p
			WHERE EXISTS (SELECT 1 FROM `{ds}.orders` o WHERE o.product_id = p.id)
			ORDER BY p.name");
		Assert.True(rows.Count >= 4); // Products that have orders
	}

	[Fact] public async Task NotExists_AntiJoin()
	{
		var rows = await Query(@"
			SELECT p.name FROM `{ds}.products` p
			WHERE NOT EXISTS (SELECT 1 FROM `{ds}.orders` o WHERE o.product_id = p.id)
			ORDER BY p.name");
		Assert.True(rows.Count >= 2); // Products without orders
	}

	// IN subqueries
	[Fact] public async Task In_Subquery()
	{
		var rows = await Query(@"
			SELECT name FROM `{ds}.products`
			WHERE id IN (SELECT product_id FROM `{ds}.orders` WHERE quantity > 3)
			ORDER BY name");
		Assert.True(rows.Count >= 2);
	}

	[Fact] public async Task NotIn_Subquery()
	{
		var rows = await Query(@"
			SELECT name FROM `{ds}.products`
			WHERE id NOT IN (SELECT product_id FROM `{ds}.orders`)
			ORDER BY name");
		Assert.True(rows.Count >= 2);
	}

	// Correlated subqueries
	[Fact] public async Task CorrelatedSubquery_InSelect()
	{
		var rows = await Query(@"
			SELECT p.name, (SELECT SUM(o.quantity) FROM `{ds}.orders` o WHERE o.product_id = p.id) AS total_ordered
			FROM `{ds}.products` p
			WHERE p.category = 'Electronics'
			ORDER BY p.name");
		Assert.Equal(3, rows.Count);
	}

	[Fact] public async Task CorrelatedSubquery_InWhere()
	{
		var rows = await Query(@"
			SELECT p.name FROM `{ds}.products` p
			WHERE p.price > (SELECT AVG(p2.price) FROM `{ds}.products` p2 WHERE p2.category = p.category)
			ORDER BY p.name");
		Assert.True(rows.Count >= 2);
	}

	// Subquery in FROM
	[Fact] public async Task Subquery_InFrom()
	{
		var rows = await Query(@"
			SELECT category, avg_price FROM (
				SELECT category, AVG(price) AS avg_price FROM `{ds}.products` GROUP BY category
			) ORDER BY avg_price DESC");
		Assert.Equal(3, rows.Count);
	}

	[Fact] public async Task Subquery_InFrom_WithJoin()
	{
		var rows = await Query(@"
			SELECT p.name, sub.order_count FROM `{ds}.products` p
			JOIN (SELECT product_id, COUNT(*) AS order_count FROM `{ds}.orders` GROUP BY product_id) sub
			ON p.id = sub.product_id
			ORDER BY sub.order_count DESC");
		Assert.True(rows.Count >= 4);
	}

	// Subquery with aggregation
	[Fact] public async Task Subquery_MaxPerCategory()
	{
		var rows = await Query(@"
			SELECT p.name, p.category, p.price FROM `{ds}.products` p
			WHERE p.price = (SELECT MAX(p2.price) FROM `{ds}.products` p2 WHERE p2.category = p.category)
			ORDER BY p.category");
		Assert.Equal(3, rows.Count); // One max per category
	}

	// Nested subqueries
	[Fact] public async Task Subquery_Nested()
	{
		var result = await Scalar(@"
			SELECT COUNT(*) FROM `{ds}.products`
			WHERE category IN (
				SELECT category FROM `{ds}.products`
				GROUP BY category
				HAVING COUNT(*) > (SELECT AVG(cnt) FROM (SELECT COUNT(*) AS cnt FROM `{ds}.products` GROUP BY category))
			)");
		Assert.NotNull(result);
	}

	// Subquery with LIMIT
	[Fact] public async Task Subquery_WithLimit()
	{
		var rows = await Query(@"
			SELECT * FROM (SELECT name, price FROM `{ds}.products` ORDER BY price DESC LIMIT 3) ORDER BY name");
		Assert.Equal(3, rows.Count);
	}

	// Subquery returning single column for IN
	[Fact] public async Task Subquery_InWithStrings()
	{
		var rows = await Query(@"
			SELECT name FROM `{ds}.products`
			WHERE category IN (SELECT DISTINCT category FROM `{ds}.products` WHERE price > 200)
			ORDER BY name");
		Assert.True(rows.Count >= 4);
	}

	// ARRAY subquery
	[Fact] public async Task ArraySubquery()
	{
		var result = await Scalar("SELECT ARRAY_LENGTH(ARRAY(SELECT id FROM `{ds}.products` WHERE category = 'Electronics'))");
		Assert.Equal("3", result);
	}

	// Subquery with DISTINCT
	[Fact] public async Task Subquery_Distinct()
	{
		var result = await Scalar("SELECT COUNT(*) FROM (SELECT DISTINCT category FROM `{ds}.products`)");
		Assert.Equal("3", result);
	}
}
