using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for advanced JOIN patterns including self-joins, multiple joins, and complex conditions.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#join_types
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class JoinPatternAdvancedTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public JoinPatternAdvancedTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_join_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();

		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.employees` (id INT64, name STRING, dept_id INT64, manager_id INT64, salary FLOAT64)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.employees` (id, name, dept_id, manager_id, salary) VALUES
			(1, 'Alice', 1, NULL, 100000),
			(2, 'Bob', 1, 1, 80000),
			(3, 'Charlie', 2, 1, 90000),
			(4, 'Diana', 2, 3, 75000),
			(5, 'Eve', 3, 1, 95000),
			(6, 'Frank', 3, 5, 70000)", parameters: null);

		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.departments` (id INT64, name STRING, budget FLOAT64)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.departments` (id, name, budget) VALUES
			(1, 'Engineering', 500000),
			(2, 'Marketing', 300000),
			(3, 'Sales', 400000),
			(4, 'HR', 200000)", parameters: null);

		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.projects` (id INT64, name STRING, dept_id INT64, lead_id INT64)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.projects` (id, name, dept_id, lead_id) VALUES
			(1, 'Project Alpha', 1, 1),
			(2, 'Project Beta', 2, 3),
			(3, 'Project Gamma', 3, 5),
			(4, 'Project Delta', 1, 2)", parameters: null);

		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.skills` (employee_id INT64, skill STRING, level INT64)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.skills` (employee_id, skill, level) VALUES
			(1, 'Python', 5), (1, 'SQL', 5), (1, 'Java', 3),
			(2, 'Python', 4), (2, 'SQL', 4),
			(3, 'Marketing', 5), (3, 'SQL', 3),
			(4, 'Design', 4), (4, 'Marketing', 3),
			(5, 'Sales', 5), (5, 'SQL', 4),
			(6, 'Sales', 3)", parameters: null);
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
	// SELF JOINS
	// ============================================================
	[Fact] public async Task SelfJoin_EmployeeManager()
	{
		var rows = await Query("SELECT e.name, m.name AS manager FROM `{ds}.employees` e LEFT JOIN `{ds}.employees` m ON e.manager_id = m.id ORDER BY e.id");
		Assert.Equal(6, rows.Count);
		Assert.Null(rows[0]["manager"]); // Alice has no manager
		Assert.Equal("Alice", (string)rows[1]["manager"]); // Bob's manager is Alice
	}

	[Fact] public async Task SelfJoin_EmployeesWithSameManager()
	{
		var rows = await Query("SELECT DISTINCT e1.name FROM `{ds}.employees` e1 JOIN `{ds}.employees` e2 ON e1.manager_id = e2.manager_id AND e1.id < e2.id");
		Assert.True(rows.Count >= 2); // Bob, Charlie, Eve share Alice as manager
	}

	[Fact] public async Task SelfJoin_HigherSalaryThanManager()
	{
		var result = await Scalar("SELECT COUNT(*) FROM `{ds}.employees` e JOIN `{ds}.employees` m ON e.manager_id = m.id WHERE e.salary > m.salary");
		Assert.Equal("0", result); // No employee earns more than their manager given the test data
	}

	// ============================================================
	// MULTIPLE JOINS
	// ============================================================
	[Fact] public async Task ThreeTableJoin()
	{
		var rows = await Query(@"
			SELECT e.name, d.name AS dept, p.name AS project
			FROM `{ds}.employees` e
			JOIN `{ds}.departments` d ON e.dept_id = d.id
			JOIN `{ds}.projects` p ON p.lead_id = e.id
			ORDER BY e.name");
		Assert.True(rows.Count >= 3);
	}

	[Fact] public async Task FourTableJoin()
	{
		var rows = await Query(@"
			SELECT e.name, d.name AS dept, s.skill, p.name AS project
			FROM `{ds}.employees` e
			JOIN `{ds}.departments` d ON e.dept_id = d.id
			JOIN `{ds}.skills` s ON s.employee_id = e.id
			JOIN `{ds}.projects` p ON p.lead_id = e.id
			ORDER BY e.name, s.skill");
		Assert.True(rows.Count >= 3);
	}

	// ============================================================
	// LEFT JOIN variations
	// ============================================================
	[Fact] public async Task LeftJoin_WithNulls()
	{
		var result = await Scalar("SELECT COUNT(*) FROM `{ds}.departments` d LEFT JOIN `{ds}.employees` e ON e.dept_id = d.id");
		Assert.Equal("7", result); // 6 employees + HR department (no employees)
	}

	[Fact] public async Task LeftJoin_CountNulls()
	{
		var result = await Scalar("SELECT COUNT(*) FROM `{ds}.departments` d LEFT JOIN `{ds}.employees` e ON e.dept_id = d.id WHERE e.id IS NULL");
		Assert.Equal("1", result); // HR has no employees
	}

	[Fact] public async Task LeftJoin_AggregateCounts()
	{
		var rows = await Query(@"
			SELECT d.name, COUNT(e.id) AS emp_count
			FROM `{ds}.departments` d
			LEFT JOIN `{ds}.employees` e ON e.dept_id = d.id
			GROUP BY d.name
			ORDER BY d.name");
		Assert.Equal(4, rows.Count);
	}

	// ============================================================
	// RIGHT JOIN
	// ============================================================
	[Fact] public async Task RightJoin_DeptToEmployees()
	{
		var result = await Scalar("SELECT COUNT(*) FROM `{ds}.employees` e RIGHT JOIN `{ds}.departments` d ON e.dept_id = d.id");
		Assert.Equal("7", result);
	}

	// ============================================================
	// FULL OUTER JOIN
	// ============================================================
	[Fact] public async Task FullOuterJoin()
	{
		var result = await Scalar("SELECT COUNT(*) FROM `{ds}.employees` e FULL OUTER JOIN `{ds}.departments` d ON e.dept_id = d.id");
		Assert.Equal("7", result);
	}

	// ============================================================
	// CROSS JOIN
	// ============================================================
	[Fact] public async Task CrossJoin_AllPairs()
	{
		var result = await Scalar("SELECT COUNT(*) FROM `{ds}.departments` CROSS JOIN `{ds}.projects`");
		Assert.Equal("16", result); // 4 * 4
	}

	[Fact] public async Task CrossJoin_WithFilter()
	{
		var result = await Scalar("SELECT COUNT(*) FROM `{ds}.departments` d CROSS JOIN `{ds}.projects` p WHERE d.id = p.dept_id");
		Assert.Equal("4", result);
	}

	// ============================================================
	// JOIN with complex conditions
	// ============================================================
	[Fact] public async Task Join_MultipleConditions()
	{
		var rows = await Query(@"
			SELECT e.name, p.name AS project
			FROM `{ds}.employees` e
			JOIN `{ds}.projects` p ON p.dept_id = e.dept_id AND p.lead_id = e.id
			ORDER BY e.name");
		Assert.True(rows.Count >= 3);
	}

	[Fact] public async Task Join_InequalityCondition()
	{
		var result = await Scalar(@"
			SELECT COUNT(*)
			FROM `{ds}.employees` e1
			JOIN `{ds}.employees` e2 ON e1.dept_id = e2.dept_id AND e1.salary > e2.salary");
		Assert.NotNull(result);
		Assert.True(int.Parse(result!) > 0);
	}

	[Fact] public async Task Join_WithSubquery()
	{
		var rows = await Query(@"
			SELECT e.name, sub.avg_salary
			FROM `{ds}.employees` e
			JOIN (SELECT dept_id, AVG(salary) AS avg_salary FROM `{ds}.employees` GROUP BY dept_id) sub
			ON e.dept_id = sub.dept_id
			WHERE e.salary > sub.avg_salary
			ORDER BY e.name");
		Assert.True(rows.Count >= 1);
	}

	// ============================================================
	// JOIN with aggregations
	// ============================================================
	[Fact] public async Task Join_GroupByAfterJoin()
	{
		var rows = await Query(@"
			SELECT d.name, SUM(e.salary) AS total_salary
			FROM `{ds}.employees` e
			JOIN `{ds}.departments` d ON e.dept_id = d.id
			GROUP BY d.name
			HAVING SUM(e.salary) > 100000
			ORDER BY d.name");
		Assert.True(rows.Count >= 2);
	}

	[Fact] public async Task Join_CountDistinct()
	{
		var result = await Scalar(@"
			SELECT COUNT(DISTINCT s.skill)
			FROM `{ds}.employees` e
			JOIN `{ds}.departments` d ON e.dept_id = d.id
			JOIN `{ds}.skills` s ON s.employee_id = e.id
			WHERE d.name = 'Engineering'");
		Assert.True(int.Parse(result!) >= 3);
	}

	// ============================================================
	// Anti-join patterns (LEFT JOIN + NULL check)
	// ============================================================
	[Fact] public async Task AntiJoin_DeptWithoutProjects()
	{
		var result = await Scalar(@"
			SELECT COUNT(*)
			FROM `{ds}.departments` d
			LEFT JOIN `{ds}.projects` p ON d.id = p.dept_id
			WHERE p.id IS NULL");
		Assert.Equal("1", result); // HR has no projects
	}

	[Fact] public async Task AntiJoin_EmployeesNotLeading()
	{
		var result = await Scalar(@"
			SELECT COUNT(*)
			FROM `{ds}.employees` e
			LEFT JOIN `{ds}.projects` p ON p.lead_id = e.id
			WHERE p.id IS NULL");
		Assert.True(int.Parse(await Scalar(@"
			SELECT COUNT(*)
			FROM `{ds}.employees` e
			LEFT JOIN `{ds}.projects` p ON p.lead_id = e.id
			WHERE p.id IS NULL") ?? "0") >= 2);
	}

	// ============================================================
	// Semi-join patterns (EXISTS)
	// ============================================================
	[Fact] public async Task SemiJoin_EmployeesWithSkills()
	{
		var result = await Scalar(@"
			SELECT COUNT(*)
			FROM `{ds}.employees` e
			WHERE EXISTS (SELECT 1 FROM `{ds}.skills` s WHERE s.employee_id = e.id AND s.level >= 5)");
		Assert.True(int.Parse(result!) >= 3);
	}

	[Fact] public async Task SemiJoin_DeptWithHighSalary()
	{
		var result = await Scalar(@"
			SELECT COUNT(*)
			FROM `{ds}.departments` d
			WHERE EXISTS (SELECT 1 FROM `{ds}.employees` e WHERE e.dept_id = d.id AND e.salary > 90000)");
		Assert.True(int.Parse(result!) >= 2);
	}

	// ============================================================
	// JOIN ordering
	// ============================================================
	[Fact] public async Task Join_OrderByJoinedColumn()
	{
		var rows = await Query(@"
			SELECT e.name, d.name AS dept
			FROM `{ds}.employees` e
			JOIN `{ds}.departments` d ON e.dept_id = d.id
			ORDER BY d.name, e.name");
		Assert.Equal(6, rows.Count);
		Assert.Equal("Engineering", (string)rows[0]["dept"]);
	}

	[Fact] public async Task Join_LimitAfterJoin()
	{
		var rows = await Query(@"
			SELECT e.name, d.name AS dept
			FROM `{ds}.employees` e
			JOIN `{ds}.departments` d ON e.dept_id = d.id
			ORDER BY e.salary DESC
			LIMIT 3");
		Assert.Equal(3, rows.Count);
	}
}
