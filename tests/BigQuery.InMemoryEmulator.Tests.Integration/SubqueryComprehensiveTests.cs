using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Comprehensive tests for subquery patterns: scalar, correlated, IN, EXISTS, ARRAY, lateral.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class SubqueryComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public SubqueryComprehensiveTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_sq_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.orders` (id INT64, cust_id INT64, amount FLOAT64, status STRING)", parameters: null);
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.cust` (id INT64, name STRING, tier STRING)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.orders` VALUES
			(1,1,100,'completed'),(2,1,200,'completed'),(3,2,150,'pending'),
			(4,2,300,'completed'),(5,3,50,'cancelled'),(6,3,250,'completed'),
			(7,4,175,'pending'),(8,1,80,'cancelled')", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.cust` VALUES
			(1,'Alice','gold'),(2,'Bob','silver'),(3,'Carol','gold'),
			(4,'Dave','bronze'),(5,'Eve','silver')", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- Scalar subquery ----
	[Fact] public async Task Scalar_InSelect()
	{
		var v = await S("SELECT (SELECT COUNT(*) FROM `{ds}.orders`)");
		Assert.Equal("8", v);
	}
	[Fact] public async Task Scalar_InWhere()
	{
		var rows = await Q("SELECT name FROM `{ds}.cust` WHERE id = (SELECT cust_id FROM `{ds}.orders` WHERE amount = 300)");
		Assert.Single(rows);
		Assert.Equal("Bob", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task Scalar_WithAgg()
	{
		var rows = await Q("SELECT name FROM `{ds}.cust` c WHERE (SELECT SUM(amount) FROM `{ds}.orders` o WHERE o.cust_id = c.id) > 300 ORDER BY name");
		Assert.True(rows.Count >= 1); // Alice: 380, Bob: 450
	}

	// ---- Correlated subquery ----
	[Fact] public async Task Correlated_InSelect()
	{
		var rows = await Q(@"
			SELECT c.name, (SELECT COUNT(*) FROM `{ds}.orders` o WHERE o.cust_id = c.id) AS order_count
			FROM `{ds}.cust` c
			ORDER BY c.name");
		Assert.Equal(5, rows.Count);
		Assert.Equal("3", rows.First(r => r["name"]?.ToString() == "Alice")["order_count"]?.ToString());
	}
	[Fact] public async Task Correlated_MaxAmount()
	{
		var rows = await Q(@"
			SELECT c.name, (SELECT MAX(o.amount) FROM `{ds}.orders` o WHERE o.cust_id = c.id) AS max_amt
			FROM `{ds}.cust` c
			ORDER BY c.name");
		Assert.Equal("200", rows.First(r => r["name"]?.ToString() == "Alice")["max_amt"]?.ToString());
	}

	// ---- IN subquery ----
	[Fact] public async Task In_Basic()
	{
		var rows = await Q("SELECT name FROM `{ds}.cust` WHERE id IN (SELECT DISTINCT cust_id FROM `{ds}.orders`) ORDER BY name");
		Assert.Equal(4, rows.Count); // Alice, Bob, Carol, Dave
	}
	[Fact] public async Task NotIn_Basic()
	{
		var rows = await Q("SELECT name FROM `{ds}.cust` WHERE id NOT IN (SELECT DISTINCT cust_id FROM `{ds}.orders`) ORDER BY name");
		Assert.Single(rows);
		Assert.Equal("Eve", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task In_WithFilter()
	{
		var rows = await Q("SELECT name FROM `{ds}.cust` WHERE id IN (SELECT cust_id FROM `{ds}.orders` WHERE status = 'pending') ORDER BY name");
		Assert.Equal(2, rows.Count); // Bob, Dave
	}

	// ---- EXISTS subquery ----
	[Fact] public async Task Exists_Basic()
	{
		var rows = await Q("SELECT name FROM `{ds}.cust` c WHERE EXISTS (SELECT 1 FROM `{ds}.orders` o WHERE o.cust_id = c.id) ORDER BY name");
		Assert.Equal(4, rows.Count);
	}
	[Fact] public async Task NotExists_Basic()
	{
		var rows = await Q("SELECT name FROM `{ds}.cust` c WHERE NOT EXISTS (SELECT 1 FROM `{ds}.orders` o WHERE o.cust_id = c.id) ORDER BY name");
		Assert.Single(rows);
		Assert.Equal("Eve", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task Exists_WithCondition()
	{
		var rows = await Q(@"
			SELECT name FROM `{ds}.cust` c
			WHERE EXISTS (SELECT 1 FROM `{ds}.orders` o WHERE o.cust_id = c.id AND o.amount > 200)
			ORDER BY name");
		Assert.Equal(2, rows.Count); // Bob(300), Carol(250)
	}

	// ---- Subquery in FROM ----
	[Fact] public async Task From_BasicSubquery()
	{
		var rows = await Q(@"
			SELECT sub.cust_id, sub.total
			FROM (SELECT cust_id, SUM(amount) AS total FROM `{ds}.orders` GROUP BY cust_id) sub
			ORDER BY sub.total DESC");
		Assert.True(rows.Count >= 4);
	}
	[Fact] public async Task From_NestedSubquery()
	{
		var v = await S(@"
			SELECT MAX(total) FROM (
				SELECT cust_id, SUM(amount) AS total
				FROM `{ds}.orders`
				WHERE status = 'completed'
				GROUP BY cust_id
			)");
		Assert.NotNull(v);
	}
	[Fact] public async Task From_JoinWithSubquery()
	{
		var rows = await Q(@"
			SELECT c.name, sub.total
			FROM `{ds}.cust` c
			JOIN (SELECT cust_id, SUM(amount) AS total FROM `{ds}.orders` GROUP BY cust_id) sub ON sub.cust_id = c.id
			ORDER BY sub.total DESC");
		Assert.True(rows.Count >= 4);
	}

	// ---- Subquery with DISTINCT ----
	[Fact] public async Task Distinct_Subquery()
	{
		var rows = await Q("SELECT DISTINCT status FROM `{ds}.orders` WHERE cust_id IN (SELECT id FROM `{ds}.cust` WHERE tier = 'gold') ORDER BY status");
		Assert.True(rows.Count >= 2);
	}

	// ---- Subquery with ORDER BY and LIMIT ----
	[Fact] public async Task Subquery_OrderByLimit()
	{
		var rows = await Q(@"
			SELECT * FROM (
				SELECT id, amount FROM `{ds}.orders` ORDER BY amount DESC LIMIT 3
			) ORDER BY amount DESC");
		Assert.Equal(3, rows.Count);
	}

	// ---- Subquery in HAVING ----
	[Fact] public async Task Subquery_InHaving()
	{
		var rows = await Q(@"
			SELECT cust_id, SUM(amount) AS total
			FROM `{ds}.orders`
			GROUP BY cust_id
			HAVING SUM(amount) > (SELECT AVG(amount) FROM `{ds}.orders`)
			ORDER BY total DESC");
		Assert.True(rows.Count > 0);
	}

	// ---- Multiple subqueries ----
	[Fact] public async Task Multiple_InSelect()
	{
		var rows = await Q(@"
			SELECT
				c.name,
				(SELECT COUNT(*) FROM `{ds}.orders` o WHERE o.cust_id = c.id) AS cnt,
				(SELECT SUM(amount) FROM `{ds}.orders` o WHERE o.cust_id = c.id) AS total
			FROM `{ds}.cust` c
			ORDER BY c.name");
		Assert.Equal(5, rows.Count);
	}

	// ---- Subquery with CASE ----
	[Fact] public async Task Subquery_WithCase()
	{
		var rows = await Q(@"
			SELECT c.name,
				CASE WHEN (SELECT COUNT(*) FROM `{ds}.orders` o WHERE o.cust_id = c.id) > 2 THEN 'frequent' ELSE 'occasional' END AS category
			FROM `{ds}.cust` c
			ORDER BY c.name");
		Assert.Equal("frequent", rows.First(r => r["name"]?.ToString() == "Alice")["category"]?.ToString());
	}

	// ---- Subquery with GROUP BY ----
	[Fact] public async Task Subquery_GroupedFrom()
	{
		var rows = await Q(@"
			SELECT tier, AVG(total) AS avg_total FROM (
				SELECT c.tier, SUM(o.amount) AS total
				FROM `{ds}.cust` c
				JOIN `{ds}.orders` o ON o.cust_id = c.id
				GROUP BY c.tier, c.id
			)
			GROUP BY tier
			ORDER BY tier");
		Assert.True(rows.Count >= 2);
	}

	// ---- Subquery comparison operators ----
	[Fact] public async Task Subquery_GreaterThanAll()
	{
		var rows = await Q(@"
			SELECT name FROM `{ds}.cust` c
			WHERE (SELECT MAX(amount) FROM `{ds}.orders` o WHERE o.cust_id = c.id) > 200
			ORDER BY name");
		Assert.True(rows.Count >= 1); // Bob(300), Carol(250)
	}

	// ---- Nested EXISTS ----
	[Fact] public async Task Nested_Exists()
	{
		var rows = await Q(@"
			SELECT c.name FROM `{ds}.cust` c
			WHERE EXISTS (
				SELECT 1 FROM `{ds}.orders` o
				WHERE o.cust_id = c.id
				AND o.status = 'completed'
				AND o.amount > 100
			)
			ORDER BY c.name");
		Assert.True(rows.Count >= 2);
	}

	// ---- Subquery with UNION ----
	[Fact] public async Task Subquery_UnionInFrom()
	{
		var rows = await Q(@"
			SELECT name FROM (
				SELECT name FROM `{ds}.cust` WHERE tier = 'gold'
				UNION ALL
				SELECT name FROM `{ds}.cust` WHERE tier = 'silver'
			) ORDER BY name");
		Assert.Equal(4, rows.Count); // Alice, Carol (gold) + Bob, Eve (silver)
	}
}
