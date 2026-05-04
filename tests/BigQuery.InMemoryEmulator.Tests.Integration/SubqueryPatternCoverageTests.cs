using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Subquery patterns: scalar, correlated, IN, EXISTS, derived tables, lateral.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class SubqueryPatternCoverageTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public SubqueryPatternCoverageTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_sqpc_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.emp` (eid INT64, name STRING, dept STRING, salary INT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.emp` VALUES
			(1,'Alice','Eng',90000),(2,'Bob','Eng',75000),(3,'Carol','Sales',70000),
			(4,'Dave','Sales',65000),(5,'Eve','HR',60000),(6,'Frank','HR',58000),
			(7,'Grace','Eng',85000),(8,'Hank','Sales',72000)", parameters: null);
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.bonus` (eid INT64, amount INT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.bonus` VALUES (1,5000),(3,3000),(5,2000),(7,4000)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- Scalar subquery ----
	[Fact] public async Task Scalar_MaxSalary()
	{
		var v = await S("SELECT name FROM `{ds}.emp` WHERE salary = (SELECT MAX(salary) FROM `{ds}.emp`)");
		Assert.Equal("Alice", v);
	}
	[Fact] public async Task Scalar_MinSalary()
	{
		var v = await S("SELECT name FROM `{ds}.emp` WHERE salary = (SELECT MIN(salary) FROM `{ds}.emp`)");
		Assert.Equal("Frank", v);
	}
	[Fact] public async Task Scalar_AvgComparison()
	{
		var rows = await Q("SELECT name FROM `{ds}.emp` WHERE salary > (SELECT AVG(salary) FROM `{ds}.emp`) ORDER BY name");
		Assert.True(rows.Count >= 2); // Alice(90k), Grace(85k), Bob(75k) > avg(71875)
	}

	// ---- IN subquery ----
	[Fact] public async Task In_Subquery()
	{
		var rows = await Q("SELECT name FROM `{ds}.emp` WHERE eid IN (SELECT eid FROM `{ds}.bonus`) ORDER BY name");
		Assert.Equal(4, rows.Count); // Alice, Carol, Eve, Grace
	}
	[Fact] public async Task NotIn_Subquery()
	{
		var rows = await Q("SELECT name FROM `{ds}.emp` WHERE eid NOT IN (SELECT eid FROM `{ds}.bonus`) ORDER BY name");
		Assert.Equal(4, rows.Count); // Bob, Dave, Frank, Hank
	}

	// ---- EXISTS subquery ----
	[Fact] public async Task Exists_Subquery()
	{
		var rows = await Q("SELECT name FROM `{ds}.emp` e WHERE EXISTS (SELECT 1 FROM `{ds}.bonus` b WHERE b.eid = e.eid) ORDER BY name");
		Assert.Equal(4, rows.Count);
	}
	[Fact] public async Task NotExists_Subquery()
	{
		var rows = await Q("SELECT name FROM `{ds}.emp` e WHERE NOT EXISTS (SELECT 1 FROM `{ds}.bonus` b WHERE b.eid = e.eid) ORDER BY name");
		Assert.Equal(4, rows.Count);
	}

	// ---- Derived table (FROM subquery) ----
	[Fact] public async Task DerivedTable_Basic()
	{
		var rows = await Q("SELECT dept, total FROM (SELECT dept, SUM(salary) AS total FROM `{ds}.emp` GROUP BY dept) ORDER BY total DESC");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Eng", rows[0]["dept"]?.ToString());
	}
	[Fact] public async Task DerivedTable_WithAlias()
	{
		var v = await S("SELECT MAX(total) FROM (SELECT dept, SUM(salary) AS total FROM `{ds}.emp` GROUP BY dept) sub");
		Assert.Equal("250000", v);
	}

	// ---- Correlated scalar subquery ----
	[Fact] public async Task Correlated_DeptMax()
	{
		var rows = await Q(@"
			SELECT e.name, e.salary
			FROM `{ds}.emp` e
			WHERE e.salary = (SELECT MAX(e2.salary) FROM `{ds}.emp` e2 WHERE e2.dept = e.dept)
			ORDER BY e.name");
		Assert.Equal(3, rows.Count); // Top earner per dept
	}

	// ---- Subquery in SELECT ----
	[Fact] public async Task Subquery_InSelect()
	{
		var rows = await Q(@"
			SELECT e.name,
				(SELECT SUM(b.amount) FROM `{ds}.bonus` b WHERE b.eid = e.eid) AS bonus_total
			FROM `{ds}.emp` e
			WHERE e.eid IN (1, 3)
			ORDER BY e.name");
		Assert.Equal(2, rows.Count);
		Assert.Equal("5000", rows[0]["bonus_total"]?.ToString()); // Alice
		Assert.Equal("3000", rows[1]["bonus_total"]?.ToString()); // Carol
	}

	// ---- Nested subqueries ----
	[Fact] public async Task Nested_Subquery()
	{
		var rows = await Q(@"
			SELECT name FROM `{ds}.emp`
			WHERE dept IN (
				SELECT dept FROM (
					SELECT dept, SUM(salary) AS total FROM `{ds}.emp` GROUP BY dept
				) WHERE total > 200000
			) ORDER BY name");
		Assert.Equal(6, rows.Count); // Eng(3) + Sales(3) both > 200000
	}

	// ---- Subquery with LIMIT ----
	[Fact] public async Task Subquery_WithLimit()
	{
		var rows = await Q("SELECT name FROM (SELECT name, salary FROM `{ds}.emp` ORDER BY salary DESC LIMIT 3) ORDER BY name");
		Assert.Equal(3, rows.Count);
	}

	// ---- Subquery in HAVING ----
	[Fact] public async Task Subquery_InHaving()
	{
		var rows = await Q(@"
			SELECT dept, SUM(salary) AS total
			FROM `{ds}.emp` GROUP BY dept
			HAVING SUM(salary) > (SELECT AVG(salary) * 3 FROM `{ds}.emp`)
			ORDER BY dept");
		Assert.True(rows.Count >= 1);
	}

	// ---- Subquery with JOIN ----
	[Fact] public async Task Subquery_InJoin()
	{
		var rows = await Q(@"
			SELECT e.name, b.total_bonus
			FROM `{ds}.emp` e
			JOIN (SELECT eid, SUM(amount) AS total_bonus FROM `{ds}.bonus` GROUP BY eid) b ON e.eid = b.eid
			ORDER BY b.total_bonus DESC");
		Assert.Equal(4, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}

	// ---- Subquery with CASE ----
	[Fact] public async Task Subquery_InCase()
	{
		var rows = await Q(@"
			SELECT name,
				CASE WHEN salary > (SELECT AVG(salary) FROM `{ds}.emp`) THEN 'Above Avg' ELSE 'Below Avg' END AS tier
			FROM `{ds}.emp`
			ORDER BY name");
		Assert.Equal(8, rows.Count);
	}
}
