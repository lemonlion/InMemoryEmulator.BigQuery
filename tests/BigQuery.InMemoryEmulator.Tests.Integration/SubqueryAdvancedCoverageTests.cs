using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Subquery patterns: scalar subquery, IN subquery, EXISTS, correlated with comparison, derived tables.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class SubqueryAdvancedCoverageTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public SubqueryAdvancedCoverageTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_sac_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.emp` (id INT64, name STRING, dept STRING, salary INT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.emp` VALUES
			(1,'Alice','Eng',90000),(2,'Bob','Eng',75000),(3,'Carol','Sales',70000),
			(4,'Dave','Sales',65000),(5,'Eve','HR',60000),(6,'Frank','HR',58000),
			(7,'Grace','Eng',85000),(8,'Hank','Sales',72000)", parameters: null);

		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.bonus` (emp_id INT64, amount INT64, quarter INT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.bonus` VALUES
			(1,5000,1),(1,6000,2),(2,3000,1),(3,4000,1),(3,4500,2),
			(7,4000,1),(7,5000,2),(8,3500,1)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- Scalar subquery in SELECT ----
	[Fact] public async Task Scalar_MaxSalary()
	{
		var rows = await Q("SELECT name, salary, (SELECT MAX(salary) FROM `{ds}.emp`) AS max_sal FROM `{ds}.emp` ORDER BY name LIMIT 1");
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("90000", rows[0]["max_sal"]?.ToString());
	}
	[Fact] public async Task Scalar_AvgSalary()
	{
		var rows = await Q("SELECT name, salary, salary - (SELECT CAST(AVG(salary) AS INT64) FROM `{ds}.emp`) AS diff FROM `{ds}.emp` WHERE name = 'Alice'");
		Assert.NotNull(rows[0]["diff"]);
	}
	[Fact] public async Task Scalar_Count()
	{
		var rows = await Q("SELECT name, (SELECT COUNT(*) FROM `{ds}.bonus` b WHERE b.emp_id = e.id) AS bonus_count FROM `{ds}.emp` e ORDER BY name");
		Assert.Equal("2", rows[0]["bonus_count"]?.ToString()); // Alice has 2 bonuses
		Assert.Equal("1", rows[1]["bonus_count"]?.ToString()); // Bob has 1
	}

	// ---- Correlated subquery ----
	[Fact] public async Task Correlated_MaxBonus()
	{
		var rows = await Q("SELECT e.name, (SELECT MAX(b.amount) FROM `{ds}.bonus` b WHERE b.emp_id = e.id) AS max_bonus FROM `{ds}.emp` e ORDER BY e.name");
		Assert.Equal("6000", rows[0]["max_bonus"]?.ToString()); // Alice
		Assert.Equal("3000", rows[1]["max_bonus"]?.ToString()); // Bob
		Assert.Equal("4500", rows[2]["max_bonus"]?.ToString()); // Carol
	}
	[Fact] public async Task Correlated_SumBonus()
	{
		var rows = await Q("SELECT e.name, (SELECT SUM(b.amount) FROM `{ds}.bonus` b WHERE b.emp_id = e.id) AS total_bonus FROM `{ds}.emp` e WHERE e.dept = 'Eng' ORDER BY e.name");
		Assert.Equal("11000", rows[0]["total_bonus"]?.ToString()); // Alice: 5000+6000
		Assert.Equal("3000", rows[1]["total_bonus"]?.ToString()); // Bob: 3000
		Assert.Equal("9000", rows[2]["total_bonus"]?.ToString()); // Grace: 4000+5000
	}

	// ---- IN subquery ----
	[Fact] public async Task In_Subquery()
	{
		var rows = await Q("SELECT name FROM `{ds}.emp` WHERE id IN (SELECT emp_id FROM `{ds}.bonus`) ORDER BY name");
		Assert.Equal(5, rows.Count); // Alice, Bob, Carol, Grace, Hank
	}
	[Fact] public async Task NotIn_Subquery()
	{
		var rows = await Q("SELECT name FROM `{ds}.emp` WHERE id NOT IN (SELECT emp_id FROM `{ds}.bonus`) ORDER BY name");
		Assert.Equal(3, rows.Count); // Dave, Eve, Frank
	}
	[Fact] public async Task In_SubqueryWithCondition()
	{
		var rows = await Q("SELECT name FROM `{ds}.emp` WHERE id IN (SELECT emp_id FROM `{ds}.bonus` WHERE amount >= 4500) ORDER BY name");
		Assert.Equal(3, rows.Count); // Alice(6000), Carol(4500), Grace(5000)
	}

	// ---- EXISTS subquery ----
	[Fact] public async Task Exists_Basic()
	{
		var rows = await Q("SELECT name FROM `{ds}.emp` e WHERE EXISTS (SELECT 1 FROM `{ds}.bonus` b WHERE b.emp_id = e.id) ORDER BY name");
		Assert.Equal(5, rows.Count);
	}
	[Fact] public async Task NotExists_Basic()
	{
		var rows = await Q("SELECT name FROM `{ds}.emp` e WHERE NOT EXISTS (SELECT 1 FROM `{ds}.bonus` b WHERE b.emp_id = e.id) ORDER BY name");
		Assert.Equal(3, rows.Count); // Dave, Eve, Frank
	}
	[Fact] public async Task Exists_WithCondition()
	{
		var rows = await Q("SELECT name FROM `{ds}.emp` e WHERE EXISTS (SELECT 1 FROM `{ds}.bonus` b WHERE b.emp_id = e.id AND b.amount > 4000) ORDER BY name");
		Assert.Equal(3, rows.Count); // Alice(5000,6000), Carol(4500), Grace(5000)
	}

	// ---- Derived table (subquery in FROM) ----
	[Fact] public async Task Derived_Table()
	{
		var rows = await Q("SELECT dept, avg_sal FROM (SELECT dept, CAST(AVG(salary) AS INT64) AS avg_sal FROM `{ds}.emp` GROUP BY dept) ORDER BY dept");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Eng", rows[0]["dept"]?.ToString());
	}
	[Fact] public async Task Derived_JoinOriginal()
	{
		var rows = await Q(@"
			SELECT e.name, e.salary, sub.max_sal
			FROM `{ds}.emp` e
			JOIN (SELECT dept, MAX(salary) AS max_sal FROM `{ds}.emp` GROUP BY dept) sub
			ON e.dept = sub.dept
			WHERE e.salary = sub.max_sal
			ORDER BY e.name");
		Assert.Equal(3, rows.Count); // Alice(Eng), Carol(Sales), Eve(HR) are dept maxes
	}
	[Fact] public async Task Derived_Nested()
	{
		var v = await S("SELECT MAX(avg_sal) FROM (SELECT dept, CAST(AVG(salary) AS INT64) AS avg_sal FROM `{ds}.emp` GROUP BY dept)");
		Assert.NotNull(v);
	}

	// ---- Subquery in WHERE with comparison ----
	[Fact] public async Task Where_GtScalar()
	{
		var rows = await Q("SELECT name FROM `{ds}.emp` WHERE salary > (SELECT AVG(salary) FROM `{ds}.emp`) ORDER BY name");
		// avg = 71875. Above: Alice(90k), Bob(75k), Grace(85k), Hank(72k)
		Assert.Equal(4, rows.Count);
	}
	[Fact] public async Task Where_EqScalar()
	{
		var rows = await Q("SELECT name FROM `{ds}.emp` WHERE salary = (SELECT MAX(salary) FROM `{ds}.emp`)");
		Assert.Single(rows);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task Where_LtScalar()
	{
		var rows = await Q("SELECT name FROM `{ds}.emp` WHERE salary < (SELECT MIN(salary) FROM `{ds}.emp` WHERE dept = 'Eng') ORDER BY name");
		// Min Eng salary = 75000. Below: Carol(70k), Dave(65k), Eve(60k), Frank(58k), Hank(72k)
		Assert.Equal(5, rows.Count);
	}

	// ---- Subquery with LIMIT ----
	[Fact] public async Task Subquery_WithLimit()
	{
		var rows = await Q("SELECT name FROM `{ds}.emp` WHERE id IN (SELECT emp_id FROM `{ds}.bonus` ORDER BY amount DESC LIMIT 3) ORDER BY name");
		// Top 3 bonuses: Alice(6000), Alice(5000), Grace(5000) → emp_ids: 1, 1, 7
		// Distinct: Alice, Grace
		Assert.True(rows.Count >= 2);
	}

	// ---- Subquery with GROUP BY ----
	[Fact] public async Task Subquery_GroupBy()
	{
		var rows = await Q(@"
			SELECT name FROM `{ds}.emp` WHERE id IN (
				SELECT emp_id FROM `{ds}.bonus` GROUP BY emp_id HAVING SUM(amount) > 5000
			) ORDER BY name");
		// Alice: 11000, Carol: 8500, Grace: 9000 → all > 5000
		Assert.Equal(3, rows.Count);
	}

	// ---- Multiple subqueries ----
	[Fact] public async Task Multiple_Subqueries()
	{
		var rows = await Q(@"
			SELECT name, salary,
				(SELECT MAX(salary) FROM `{ds}.emp`) AS overall_max,
				(SELECT MAX(salary) FROM `{ds}.emp` e2 WHERE e2.dept = e.dept) AS dept_max
			FROM `{ds}.emp` e
			WHERE e.dept = 'Eng'
			ORDER BY e.name");
		Assert.Equal(3, rows.Count);
		Assert.Equal("90000", rows[0]["overall_max"]?.ToString());
		Assert.Equal("90000", rows[0]["dept_max"]?.ToString());
	}
}
