using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Correlated subquery patterns: scalar, EXISTS, IN, NOT EXISTS, NOT IN, with aggregation.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class CorrelatedSubqueryPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public CorrelatedSubqueryPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_csq_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.emp` (eid INT64, name STRING, dept STRING, salary FLOAT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.emp` VALUES
			(1,'Alice','Eng',90000),(2,'Bob','Eng',75000),(3,'Carol','Sales',70000),
			(4,'Dave','Sales',65000),(5,'Eve','HR',60000),(6,'Frank','HR',58000),
			(7,'Grace','Eng',85000),(8,'Hank','Sales',72000)", parameters: null);
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.dept` (dname STRING, budget FLOAT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.dept` VALUES
			('Eng',500000),('Sales',300000),('HR',200000),('Marketing',100000)", parameters: null);
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.projects` (pid INT64, eid INT64, pname STRING, hours INT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.projects` VALUES
			(1,1,'Alpha',40),(2,1,'Beta',20),(3,2,'Alpha',30),(4,3,'Gamma',50),
			(5,4,'Gamma',35),(6,7,'Beta',45),(7,7,'Alpha',15),(8,5,'Delta',40)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- Scalar correlated subquery ----
	[Fact] public async Task Scalar_MaxSalaryInDept()
	{
		var rows = await Q(@"
			SELECT e.name, e.salary,
				(SELECT MAX(e2.salary) FROM `{ds}.emp` e2 WHERE e2.dept = e.dept) AS dept_max
			FROM `{ds}.emp` e
			WHERE e.dept = 'Eng'
			ORDER BY e.name");
		Assert.Equal(3, rows.Count);
		Assert.Equal("90000", rows[0]["dept_max"]?.ToString());
	}

	[Fact] public async Task Scalar_CountProjects()
	{
		var rows = await Q(@"
			SELECT e.name,
				(SELECT COUNT(*) FROM `{ds}.projects` p WHERE p.eid = e.eid) AS proj_count
			FROM `{ds}.emp` e
			ORDER BY proj_count DESC, e.name");
		Assert.Equal(8, rows.Count);
	}

	[Fact] public async Task Scalar_AvgSalaryInDept()
	{
		var rows = await Q(@"
			SELECT e.name, e.dept,
				(SELECT ROUND(AVG(e2.salary), 0) FROM `{ds}.emp` e2 WHERE e2.dept = e.dept) AS dept_avg
			FROM `{ds}.emp` e
			WHERE e.dept = 'Sales'
			ORDER BY e.name");
		Assert.Equal(3, rows.Count);
	}

	// ---- EXISTS ----
	[Fact] public async Task Exists_HasProjects()
	{
		var rows = await Q(@"
			SELECT e.name FROM `{ds}.emp` e
			WHERE EXISTS (SELECT 1 FROM `{ds}.projects` p WHERE p.eid = e.eid)
			ORDER BY e.name");
		Assert.True(rows.Count >= 5);
	}

	[Fact] public async Task NotExists_NoProjects()
	{
		var rows = await Q(@"
			SELECT e.name FROM `{ds}.emp` e
			WHERE NOT EXISTS (SELECT 1 FROM `{ds}.projects` p WHERE p.eid = e.eid)
			ORDER BY e.name");
		Assert.True(rows.Count >= 1); // employees without projects
	}

	// ---- IN subquery ----
	[Fact] public async Task In_EmployeesOnAlpha()
	{
		var rows = await Q(@"
			SELECT e.name FROM `{ds}.emp` e
			WHERE e.eid IN (SELECT p.eid FROM `{ds}.projects` p WHERE p.pname = 'Alpha')
			ORDER BY e.name");
		Assert.True(rows.Count >= 2); // Alice, Bob, Grace
	}

	[Fact] public async Task NotIn_NotOnAlpha()
	{
		var rows = await Q(@"
			SELECT e.name FROM `{ds}.emp` e
			WHERE e.eid NOT IN (SELECT p.eid FROM `{ds}.projects` p WHERE p.pname = 'Alpha')
			ORDER BY e.name");
		Assert.True(rows.Count >= 3);
	}

	// ---- Correlated with aggregation ----
	[Fact] public async Task AboveAvg_InDept()
	{
		var rows = await Q(@"
			SELECT e.name, e.dept, e.salary FROM `{ds}.emp` e
			WHERE e.salary > (SELECT AVG(e2.salary) FROM `{ds}.emp` e2 WHERE e2.dept = e.dept)
			ORDER BY e.name");
		Assert.True(rows.Count >= 3);
	}

	// ---- Correlated in SELECT with arithmetic ----
	[Fact] public async Task Scalar_TotalHours()
	{
		var rows = await Q(@"
			SELECT e.name,
				(SELECT SUM(p.hours) FROM `{ds}.projects` p WHERE p.eid = e.eid) AS total_hours
			FROM `{ds}.emp` e
			ORDER BY e.eid");
		Assert.Equal(8, rows.Count);
		Assert.Equal("60", rows[0]["total_hours"]?.ToString()); // Alice: 40+20
	}

	// ---- EXISTS with dept table ----
	[Fact] public async Task Exists_DeptWithEmployees()
	{
		var rows = await Q(@"
			SELECT d.dname FROM `{ds}.dept` d
			WHERE EXISTS (SELECT 1 FROM `{ds}.emp` e WHERE e.dept = d.dname)
			ORDER BY d.dname");
		Assert.Equal(3, rows.Count); // Eng, HR, Sales
	}

	[Fact] public async Task NotExists_DeptNoEmployees()
	{
		var rows = await Q(@"
			SELECT d.dname FROM `{ds}.dept` d
			WHERE NOT EXISTS (SELECT 1 FROM `{ds}.emp` e WHERE e.dept = d.dname)
			ORDER BY d.dname");
		Assert.Single(rows);
		Assert.Equal("Marketing", rows[0]["dname"]?.ToString());
	}

	// ---- Scalar subquery in WHERE ----
	[Fact] public async Task Subquery_InWhere_MaxSalary()
	{
		var rows = await Q(@"
			SELECT name FROM `{ds}.emp`
			WHERE salary = (SELECT MAX(salary) FROM `{ds}.emp`)");
		Assert.Single(rows);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}

	// ---- Subquery in FROM (derived table) ----
	[Fact] public async Task DerivedTable()
	{
		var rows = await Q(@"
			SELECT dept, cnt FROM (
				SELECT dept, COUNT(*) AS cnt FROM `{ds}.emp` GROUP BY dept
			) WHERE cnt >= 3
			ORDER BY dept");
		Assert.True(rows.Count >= 2);
	}

	// ---- IN with multiple columns from subquery ----
	[Fact] public async Task In_DeptFromSubquery()
	{
		var rows = await Q(@"
			SELECT name FROM `{ds}.emp`
			WHERE dept IN (SELECT dname FROM `{ds}.dept` WHERE budget > 250000)
			ORDER BY name");
		Assert.True(rows.Count >= 5); // Eng(500k) + Sales(300k) employees
	}

	// ---- Nested subquery ----
	[Fact] public async Task Nested_Subquery()
	{
		var rows = await Q(@"
			SELECT name FROM `{ds}.emp`
			WHERE eid IN (
				SELECT eid FROM `{ds}.projects`
				WHERE pname IN (SELECT pname FROM `{ds}.projects` WHERE hours > 40)
			)
			ORDER BY name");
		Assert.True(rows.Count >= 1);
	}

	// ---- Subquery with DISTINCT ----
	[Fact] public async Task In_DistinctSubquery()
	{
		var rows = await Q(@"
			SELECT name FROM `{ds}.emp`
			WHERE dept IN (SELECT DISTINCT dept FROM `{ds}.emp` WHERE salary > 80000)
			ORDER BY name");
		Assert.True(rows.Count >= 3); // All Eng employees
	}

	// ---- Scalar subquery as column expression ----
	[Fact] public async Task Scalar_BudgetPerEmployee()
	{
		var rows = await Q(@"
			SELECT e.name, e.dept,
				ROUND(
					(SELECT d.budget FROM `{ds}.dept` d WHERE d.dname = e.dept) /
					(SELECT COUNT(*) FROM `{ds}.emp` e2 WHERE e2.dept = e.dept)
				, 0) AS budget_per_emp
			FROM `{ds}.emp` e
			WHERE e.dept = 'Eng'
			ORDER BY e.name");
		Assert.Equal(3, rows.Count);
	}

	// ---- EXISTS vs IN equivalence ----
	[Fact] public async Task Exists_Vs_In_Same()
	{
		var existsRows = await Q(@"
			SELECT name FROM `{ds}.emp` e
			WHERE EXISTS (SELECT 1 FROM `{ds}.projects` p WHERE p.eid = e.eid AND p.pname = 'Alpha')
			ORDER BY name");
		var inRows = await Q(@"
			SELECT name FROM `{ds}.emp`
			WHERE eid IN (SELECT eid FROM `{ds}.projects` WHERE pname = 'Alpha')
			ORDER BY name");
		Assert.Equal(existsRows.Count, inRows.Count);
	}

	// ---- Subquery with ORDER BY and LIMIT ----
	[Fact] public async Task Subquery_Limit()
	{
		var v = await S(@"
			SELECT name FROM `{ds}.emp`
			WHERE salary = (
				SELECT salary FROM `{ds}.emp` ORDER BY salary DESC LIMIT 1
			)");
		Assert.Equal("Alice", v);
	}
}
