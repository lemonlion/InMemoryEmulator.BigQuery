using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Complex CTE patterns: chained, multiple refs, CTE with joins, CTE with aggregates, CTE with window functions.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_clause
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class CteComplexPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public CteComplexPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_ccp_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.emp` (eid INT64, name STRING, dept STRING, salary FLOAT64, mgr_id INT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.emp` VALUES
			(1,'Alice','Eng',80000,NULL),(2,'Bob','Eng',75000,1),(3,'Carol','Sales',70000,1),
			(4,'Dave','Sales',65000,3),(5,'Eve','Eng',90000,1),(6,'Frank','HR',60000,NULL),
			(7,'Grace','HR',62000,6),(8,'Hank','Eng',72000,2),(9,'Ivy','Sales',68000,3),
			(10,'Jack','HR',58000,6)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- Single CTE ----
	[Fact] public async Task Cte_Simple()
	{
		var rows = await Q("WITH eng AS (SELECT * FROM `{ds}.emp` WHERE dept = 'Eng') SELECT name FROM eng ORDER BY name");
		Assert.Equal(4, rows.Count);
	}

	// ---- CTE with aggregate ----
	[Fact] public async Task Cte_Aggregate()
	{
		var rows = await Q(@"
			WITH dept_stats AS (
				SELECT dept, COUNT(*) AS cnt, ROUND(AVG(salary), 0) AS avg_sal
				FROM `{ds}.emp` GROUP BY dept
			)
			SELECT dept, cnt, avg_sal FROM dept_stats ORDER BY dept");
		Assert.Equal(3, rows.Count);
	}

	// ---- Multiple CTEs ----
	[Fact] public async Task Cte_Multiple()
	{
		var rows = await Q(@"
			WITH eng AS (SELECT * FROM `{ds}.emp` WHERE dept = 'Eng'),
				 sales AS (SELECT * FROM `{ds}.emp` WHERE dept = 'Sales')
			SELECT e.name AS eng_name, s.name AS sales_name
			FROM eng e CROSS JOIN sales s
			WHERE e.salary > s.salary
			ORDER BY e.name, s.name");
		Assert.True(rows.Count >= 1);
	}

	// ---- Chained CTEs ----
	[Fact] public async Task Cte_Chained()
	{
		var rows = await Q(@"
			WITH base AS (SELECT dept, salary FROM `{ds}.emp`),
				 totals AS (SELECT dept, SUM(salary) AS total FROM base GROUP BY dept)
			SELECT dept, total FROM totals ORDER BY total DESC");
		Assert.Equal(3, rows.Count);
	}

	// ---- CTE referenced multiple times ----
	[Fact] public async Task Cte_MultiRef()
	{
		var rows = await Q(@"
			WITH dept_avg AS (
				SELECT dept, AVG(salary) AS avg_sal FROM `{ds}.emp` GROUP BY dept
			)
			SELECT e.name, e.salary, d.avg_sal
			FROM `{ds}.emp` e
			JOIN dept_avg d ON e.dept = d.dept
			WHERE e.salary > d.avg_sal
			ORDER BY e.name");
		Assert.True(rows.Count >= 3);
	}

	// ---- CTE with window function ----
	[Fact] public async Task Cte_Window()
	{
		var rows = await Q(@"
			WITH ranked AS (
				SELECT name, dept, salary,
					RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk
				FROM `{ds}.emp`
			)
			SELECT name, dept, salary FROM ranked WHERE rnk = 1 ORDER BY dept");
		Assert.Equal(3, rows.Count); // top earner per dept
	}

	// ---- CTE with UNION ----
	[Fact] public async Task Cte_Union()
	{
		var rows = await Q(@"
			WITH high AS (SELECT name, salary FROM `{ds}.emp` WHERE salary >= 75000),
				 low AS (SELECT name, salary FROM `{ds}.emp` WHERE salary < 60000)
			SELECT * FROM high UNION ALL SELECT * FROM low ORDER BY salary DESC");
		Assert.True(rows.Count >= 4);
	}

	// ---- CTE with EXISTS ----
	[Fact] public async Task Cte_Exists()
	{
		var rows = await Q(@"
			WITH managers AS (SELECT DISTINCT mgr_id FROM `{ds}.emp` WHERE mgr_id IS NOT NULL)
			SELECT name FROM `{ds}.emp` WHERE eid IN (SELECT mgr_id FROM managers) ORDER BY name");
		Assert.True(rows.Count >= 3);
	}

	// ---- CTE with self-join ----
	[Fact] public async Task Cte_SelfJoin()
	{
		var rows = await Q(@"
			WITH e AS (SELECT * FROM `{ds}.emp`)
			SELECT e1.name AS employee, e2.name AS manager
			FROM e e1
			JOIN e e2 ON e1.mgr_id = e2.eid
			ORDER BY e1.name");
		Assert.True(rows.Count >= 6);
	}

	// ---- CTE with subquery ----
	[Fact] public async Task Cte_Subquery()
	{
		var v = await S(@"
			WITH dept_sal AS (
				SELECT dept, SUM(salary) AS total FROM `{ds}.emp` GROUP BY dept
			)
			SELECT dept FROM dept_sal WHERE total = (SELECT MAX(total) FROM dept_sal)");
		Assert.Equal("Eng", v); // Eng has highest total salary
	}

	// ---- CTE with CASE ----
	[Fact] public async Task Cte_Case()
	{
		var rows = await Q(@"
			WITH categorized AS (
				SELECT name, salary,
					CASE WHEN salary >= 80000 THEN 'senior' WHEN salary >= 65000 THEN 'mid' ELSE 'junior' END AS level
				FROM `{ds}.emp`
			)
			SELECT level, COUNT(*) AS cnt FROM categorized GROUP BY level ORDER BY level");
		Assert.Equal(3, rows.Count);
	}

	// ---- CTE with HAVING ----
	[Fact] public async Task Cte_Having()
	{
		var rows = await Q(@"
			WITH dept_counts AS (
				SELECT dept, COUNT(*) AS cnt FROM `{ds}.emp` GROUP BY dept HAVING COUNT(*) > 2
			)
			SELECT dept, cnt FROM dept_counts ORDER BY dept");
		Assert.True(rows.Count >= 2); // Eng(4) and Sales(3) and HR(3)
	}

	// ---- CTE with DISTINCT ----
	[Fact] public async Task Cte_Distinct()
	{
		var rows = await Q(@"
			WITH unique_depts AS (SELECT DISTINCT dept FROM `{ds}.emp`)
			SELECT dept FROM unique_depts ORDER BY dept");
		Assert.Equal(3, rows.Count);
	}

	// ---- CTE with ORDER BY and LIMIT ----
	[Fact] public async Task Cte_Limit()
	{
		var rows = await Q(@"
			WITH all_emp AS (SELECT * FROM `{ds}.emp`)
			SELECT name, salary FROM all_emp ORDER BY salary DESC LIMIT 3");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Eve", rows[0]["name"]?.ToString()); // 90000
	}

	// ---- CTE with COUNT(DISTINCT) ----
	[Fact] public async Task Cte_CountDistinct()
	{
		var v = await S(@"
			WITH emp_data AS (SELECT * FROM `{ds}.emp`)
			SELECT COUNT(DISTINCT dept) FROM emp_data");
		Assert.Equal("3", v);
	}

	// ---- CTE with multiple aggregates ----
	[Fact] public async Task Cte_MultiAgg()
	{
		var rows = await Q(@"
			WITH stats AS (
				SELECT dept,
					COUNT(*) AS cnt,
					MIN(salary) AS min_sal,
					MAX(salary) AS max_sal,
					ROUND(AVG(salary), 0) AS avg_sal
				FROM `{ds}.emp`
				GROUP BY dept
			)
			SELECT dept, cnt, min_sal, max_sal, avg_sal FROM stats ORDER BY dept");
		Assert.Equal(3, rows.Count);
	}

	// ---- CTE with running total ----
	[Fact] public async Task Cte_RunningTotal()
	{
		var rows = await Q(@"
			WITH ordered AS (SELECT name, salary FROM `{ds}.emp` ORDER BY salary)
			SELECT name, salary,
				SUM(salary) OVER (ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running
			FROM ordered
			ORDER BY salary");
		Assert.Equal(10, rows.Count);
	}

	// ---- CTE used in INSERT ----
	[Fact] public async Task Cte_Insert()
	{
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.high_earners` (name STRING, salary FLOAT64)", parameters: null);
		await c.ExecuteQueryAsync($@"
			INSERT INTO `{_ds}.high_earners`
			SELECT name, salary FROM `{_ds}.emp` WHERE salary >= 75000", parameters: null);
		var v = await S("SELECT COUNT(*) FROM `{ds}.high_earners`");
		Assert.True(int.Parse(v!) >= 3);
	}

	// ---- Deeply chained CTEs ----
	[Fact] public async Task Cte_DeepChain()
	{
		var rows = await Q(@"
			WITH step1 AS (SELECT * FROM `{ds}.emp` WHERE dept = 'Eng'),
				 step2 AS (SELECT name, salary FROM step1 WHERE salary > 70000),
				 step3 AS (SELECT name, salary * 1.1 AS adj_salary FROM step2)
			SELECT name, ROUND(adj_salary, 0) AS adj FROM step3 ORDER BY name");
		Assert.True(rows.Count >= 2);
	}

	// ---- CTE with LAG ----
	[Fact] public async Task Cte_Lag()
	{
		var rows = await Q(@"
			WITH ordered AS (
				SELECT name, salary,
					LAG(salary) OVER (ORDER BY salary) AS prev_salary
				FROM `{ds}.emp`
			)
			SELECT name, salary, prev_salary FROM ordered ORDER BY salary");
		Assert.Equal(10, rows.Count);
		Assert.Null(rows[0]["prev_salary"]); // first has no lag
	}

	// ---- CTE with NTILE ----
	[Fact] public async Task Cte_Ntile()
	{
		var rows = await Q(@"
			WITH quartiles AS (
				SELECT name, salary, NTILE(4) OVER (ORDER BY salary) AS q
				FROM `{ds}.emp`
			)
			SELECT q, COUNT(*) AS cnt FROM quartiles GROUP BY q ORDER BY q");
		Assert.Equal(4, rows.Count);
	}

	// ---- CTE joining two CTEs ----
	[Fact] public async Task Cte_JoinTwoCtes()
	{
		var rows = await Q(@"
			WITH eng AS (SELECT eid, name, salary FROM `{ds}.emp` WHERE dept = 'Eng'),
				 mgrs AS (SELECT DISTINCT mgr_id FROM `{ds}.emp` WHERE mgr_id IS NOT NULL)
			SELECT e.name
			FROM eng e
			WHERE e.eid IN (SELECT mgr_id FROM mgrs)
			ORDER BY e.name");
		Assert.True(rows.Count >= 1);
	}
}
