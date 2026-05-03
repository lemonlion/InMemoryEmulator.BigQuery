using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for advanced CTE patterns: recursive-style, multiple CTEs, CTE with DML.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_clause
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class CteAdvancedPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public CteAdvancedPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_cte_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.employees` (id INT64, name STRING, dept STRING, salary FLOAT64, manager_id INT64)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.employees` (id, name, dept, salary, manager_id) VALUES
			(1, 'Alice', 'Eng', 120000, NULL),
			(2, 'Bob', 'Eng', 100000, 1),
			(3, 'Charlie', 'Sales', 90000, 1),
			(4, 'Diana', 'Sales', 85000, 3),
			(5, 'Eve', 'Eng', 110000, 2),
			(6, 'Frank', 'HR', 80000, 1),
			(7, 'Grace', 'HR', 75000, 6),
			(8, 'Hank', 'Sales', 95000, 3)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
		return result.ToList();
	}

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// Basic CTE
	[Fact] public async Task Cte_BasicSelect()
	{
		var result = await Scalar(@"
			WITH eng AS (SELECT * FROM `{ds}.employees` WHERE dept = 'Eng')
			SELECT COUNT(*) FROM eng");
		Assert.Equal("3", result);
	}

	// Multiple CTEs
	[Fact] public async Task Cte_MultipleCtes()
	{
		var result = await Scalar(@"
			WITH eng AS (SELECT * FROM `{ds}.employees` WHERE dept = 'Eng'),
				 sales AS (SELECT * FROM `{ds}.employees` WHERE dept = 'Sales')
			SELECT (SELECT COUNT(*) FROM eng) + (SELECT COUNT(*) FROM sales)");
		Assert.Equal("6", result); // 3 eng + 3 sales
	}

	[Fact] public async Task Cte_CteReferencingCte()
	{
		var result = await Scalar(@"
			WITH all_emp AS (SELECT * FROM `{ds}.employees`),
				 high_salary AS (SELECT * FROM all_emp WHERE salary > 100000)
			SELECT COUNT(*) FROM high_salary");
		Assert.Equal("2", result); // Alice 120k, Eve 110k
	}

	[Fact] public async Task Cte_ThreeCtes()
	{
		var result = await Scalar(@"
			WITH a AS (SELECT * FROM `{ds}.employees` WHERE dept = 'Eng'),
				 b AS (SELECT * FROM `{ds}.employees` WHERE dept = 'Sales'),
				 c AS (SELECT * FROM a UNION ALL SELECT * FROM b)
			SELECT COUNT(*) FROM c");
		Assert.Equal("6", result);
	}

	// CTE with aggregation
	[Fact] public async Task Cte_WithAggregation()
	{
		var rows = await Query(@"
			WITH dept_stats AS (
				SELECT dept, AVG(salary) AS avg_salary, COUNT(*) AS emp_count
				FROM `{ds}.employees`
				GROUP BY dept
			)
			SELECT dept, avg_salary FROM dept_stats ORDER BY avg_salary DESC");
		Assert.Equal("Eng", rows[0]["dept"].ToString()); // Eng has highest avg
	}

	// CTE with JOIN
	[Fact] public async Task Cte_WithJoin()
	{
		var rows = await Query(@"
			WITH managers AS (
				SELECT id, name FROM `{ds}.employees` WHERE id IN (SELECT DISTINCT manager_id FROM `{ds}.employees` WHERE manager_id IS NOT NULL)
			)
			SELECT e.name AS employee, m.name AS manager
			FROM `{ds}.employees` e
			JOIN managers m ON e.manager_id = m.id
			ORDER BY e.name");
		Assert.True(rows.Count >= 5);
	}

	// CTE with window function
	[Fact] public async Task Cte_WithWindowFunction()
	{
		var rows = await Query(@"
			WITH ranked AS (
				SELECT name, dept, salary,
					RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS dept_rank
				FROM `{ds}.employees`
			)
			SELECT name, dept, salary FROM ranked WHERE dept_rank = 1 ORDER BY dept");
		Assert.Equal(3, rows.Count); // One highest per dept
	}

	// CTE with subquery
	[Fact] public async Task Cte_WithExistsSubquery()
	{
		var result = await Scalar(@"
			WITH has_reports AS (
				SELECT DISTINCT manager_id AS id FROM `{ds}.employees` WHERE manager_id IS NOT NULL
			)
			SELECT COUNT(*) FROM `{ds}.employees` e WHERE EXISTS (SELECT 1 FROM has_reports h WHERE h.id = e.id)");
		Assert.NotNull(result);
		Assert.True(int.Parse(result!) >= 3);
	}

	// CTE reuse (referenced multiple times)
	[Fact] public async Task Cte_ReferencedMultipleTimes()
	{
		var result = await Scalar(@"
			WITH high AS (SELECT * FROM `{ds}.employees` WHERE salary > 90000)
			SELECT (SELECT COUNT(*) FROM high) + (SELECT MAX(salary) FROM high)");
		Assert.NotNull(result);
	}

	// CTE with UNION
	[Fact] public async Task Cte_WithUnionAll()
	{
		var result = await Scalar(@"
			WITH combined AS (
				SELECT name, salary FROM `{ds}.employees` WHERE dept = 'Eng'
				UNION ALL
				SELECT name, salary FROM `{ds}.employees` WHERE dept = 'Sales'
			)
			SELECT COUNT(*) FROM combined");
		Assert.Equal("6", result);
	}

	// CTE with CASE
	[Fact] public async Task Cte_WithCaseExpression()
	{
		var rows = await Query(@"
			WITH categorized AS (
				SELECT name, salary,
					CASE WHEN salary >= 100000 THEN 'Senior' WHEN salary >= 85000 THEN 'Mid' ELSE 'Junior' END AS level
				FROM `{ds}.employees`
			)
			SELECT level, COUNT(*) AS cnt FROM categorized GROUP BY level ORDER BY level");
		Assert.True(rows.Count >= 2);
	}

	// CTE with ORDER BY and LIMIT
	[Fact] public async Task Cte_TopN()
	{
		var rows = await Query(@"
			WITH top3 AS (
				SELECT name, salary FROM `{ds}.employees` ORDER BY salary DESC LIMIT 3
			)
			SELECT * FROM top3 ORDER BY salary DESC");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Alice", rows[0]["name"].ToString());
	}

	// CTE with filtering
	[Fact] public async Task Cte_FilterInOuter()
	{
		var rows = await Query(@"
			WITH all_data AS (SELECT * FROM `{ds}.employees`)
			SELECT name FROM all_data WHERE dept = 'HR' ORDER BY name");
		Assert.Equal(2, rows.Count);
	}

	// CTE with computed columns
	[Fact] public async Task Cte_ComputedColumn()
	{
		var result = await Scalar(@"
			WITH bonus AS (
				SELECT name, salary, salary * 0.10 AS bonus_amount FROM `{ds}.employees`
			)
			SELECT bonus_amount FROM bonus WHERE name = 'Alice'");
		Assert.Equal("12000", result);
	}
}
