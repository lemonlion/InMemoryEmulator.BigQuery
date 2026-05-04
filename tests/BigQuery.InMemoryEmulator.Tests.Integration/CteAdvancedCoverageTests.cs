using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// CTE (Common Table Expression) advanced patterns: recursive-like, multiple CTEs, nested, CTE with DML.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_clause
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class CteAdvancedCoverageTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public CteAdvancedCoverageTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_cac_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.emp` (id INT64, name STRING, dept STRING, salary INT64, mgr_id INT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.emp` VALUES
			(1,'Alice','Eng',90000,NULL),(2,'Bob','Eng',75000,1),(3,'Carol','Sales',70000,NULL),
			(4,'Dave','Sales',65000,3),(5,'Eve','HR',60000,NULL),(6,'Frank','Eng',85000,1),
			(7,'Grace','Sales',72000,3),(8,'Hank','HR',55000,5)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- Basic CTE ----
	[Fact] public async Task Cte_BasicSelect()
	{
		var rows = await Q("WITH eng AS (SELECT * FROM `{ds}.emp` WHERE dept = 'Eng') SELECT name FROM eng ORDER BY name");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task Cte_WithAgg()
	{
		var v = await S("WITH eng AS (SELECT * FROM `{ds}.emp` WHERE dept = 'Eng') SELECT SUM(salary) FROM eng");
		Assert.Equal("250000", v);
	}

	// ---- Multiple CTEs ----
	[Fact] public async Task MultipleCte_TwoTables()
	{
		var rows = await Q(@"
			WITH eng AS (SELECT * FROM `{ds}.emp` WHERE dept = 'Eng'),
			     sales AS (SELECT * FROM `{ds}.emp` WHERE dept = 'Sales')
			SELECT 'Eng' AS dept, COUNT(*) AS cnt FROM eng
			UNION ALL
			SELECT 'Sales', COUNT(*) FROM sales
			ORDER BY dept");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Eng", rows[0]["dept"]?.ToString());
		Assert.Equal("3", rows[0]["cnt"]?.ToString());
		Assert.Equal("Sales", rows[1]["dept"]?.ToString());
		Assert.Equal("3", rows[1]["cnt"]?.ToString());
	}
	[Fact] public async Task MultipleCte_Reference()
	{
		var rows = await Q(@"
			WITH dept_stats AS (
				SELECT dept, AVG(salary) AS avg_sal FROM `{ds}.emp` GROUP BY dept
			),
			above_avg AS (
				SELECT e.name, e.dept, e.salary
				FROM `{ds}.emp` e
				JOIN dept_stats d ON e.dept = d.dept
				WHERE e.salary > d.avg_sal
			)
			SELECT name, dept FROM above_avg ORDER BY name");
		Assert.True(rows.Count > 0);
		// Alice(90k) > avg_eng(83.3k)
		Assert.Contains(rows, r => r["name"]?.ToString() == "Alice");
	}

	// ---- CTE referencing another CTE ----
	[Fact] public async Task Cte_ChainedReference()
	{
		var rows = await Q(@"
			WITH base AS (
				SELECT dept, salary FROM `{ds}.emp`
			),
			dept_totals AS (
				SELECT dept, SUM(salary) AS total FROM base GROUP BY dept
			)
			SELECT dept, total FROM dept_totals ORDER BY total DESC");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Eng", rows[0]["dept"]?.ToString());
		Assert.Equal("250000", rows[0]["total"]?.ToString());
	}

	// ---- CTE with window functions ----
	[Fact] public async Task Cte_WithWindow()
	{
		var rows = await Q(@"
			WITH ranked AS (
				SELECT name, dept, salary, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk
				FROM `{ds}.emp`
			)
			SELECT name, dept, salary FROM ranked WHERE rnk = 1 ORDER BY dept");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString()); // Eng top (90k)
		Assert.Equal("Eve", rows[1]["name"]?.ToString()); // HR top (60k)
		Assert.Equal("Grace", rows[2]["name"]?.ToString()); // Sales top (72k)
	}
	[Fact] public async Task Cte_WindowRowNumber()
	{
		var rows = await Q(@"
			WITH numbered AS (
				SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
				FROM `{ds}.emp`
			)
			SELECT name FROM numbered WHERE rn <= 2 ORDER BY name");
		Assert.Equal(6, rows.Count); // top 2 per dept * 3 depts
	}

	// ---- CTE with JOIN ----
	[Fact] public async Task Cte_WithJoin()
	{
		var rows = await Q(@"
			WITH managers AS (
				SELECT id, name AS mgr_name FROM `{ds}.emp` WHERE mgr_id IS NULL
			)
			SELECT e.name, m.mgr_name
			FROM `{ds}.emp` e
			JOIN managers m ON e.mgr_id = m.id
			ORDER BY e.name");
		Assert.Equal(5, rows.Count);
		Assert.Equal("Bob", rows[0]["name"]?.ToString());
		Assert.Equal("Alice", rows[0]["mgr_name"]?.ToString());
	}

	// ---- CTE with filter and aggregation ----
	[Fact] public async Task Cte_FilterThenAgg()
	{
		var v = await S(@"
			WITH high_earners AS (
				SELECT * FROM `{ds}.emp` WHERE salary > 70000
			)
			SELECT COUNT(*) FROM high_earners");
		Assert.Equal("4", v); // Alice(90), Bob(75), Frank(85), Grace(72)
	}

	// ---- CTE with CASE ----
	[Fact] public async Task Cte_WithCase()
	{
		var rows = await Q(@"
			WITH categorized AS (
				SELECT name, salary,
					CASE WHEN salary >= 80000 THEN 'Senior'
					     WHEN salary >= 60000 THEN 'Mid'
					     ELSE 'Junior' END AS level
				FROM `{ds}.emp`
			)
			SELECT level, COUNT(*) AS cnt FROM categorized GROUP BY level ORDER BY level");
		Assert.Equal(3, rows.Count);
	}

	// ---- CTE used multiple times ----
	[Fact] public async Task Cte_UsedTwice()
	{
		var rows = await Q(@"
			WITH dept_stats AS (
				SELECT dept, COUNT(*) AS cnt, SUM(salary) AS total FROM `{ds}.emp` GROUP BY dept
			)
			SELECT a.dept, a.cnt, a.total, b.dept AS other_dept, b.total AS other_total
			FROM dept_stats a CROSS JOIN dept_stats b
			WHERE a.dept < b.dept
			ORDER BY a.dept, b.dept");
		Assert.Equal(3, rows.Count); // C(3,2) = 3 pairs
	}

	// ---- CTE with subquery in SELECT ----
	[Fact] public async Task Cte_SubquerySelect()
	{
		var rows = await Q(@"
			WITH avgs AS (
				SELECT dept, AVG(salary) AS avg_sal FROM `{ds}.emp` GROUP BY dept
			)
			SELECT e.name, e.salary, (SELECT avg_sal FROM avgs WHERE avgs.dept = e.dept) AS dept_avg
			FROM `{ds}.emp` e
			WHERE e.dept = 'Eng'
			ORDER BY e.name");
		Assert.Equal(3, rows.Count);
		// All should have same dept_avg
		Assert.Equal(rows[0]["dept_avg"]?.ToString(), rows[1]["dept_avg"]?.ToString());
	}

	// ---- CTE with UNION ALL ----
	[Fact] public async Task Cte_UnionAll()
	{
		var rows = await Q(@"
			WITH combined AS (
				SELECT name, 'Eng' AS source FROM `{ds}.emp` WHERE dept = 'Eng'
				UNION ALL
				SELECT name, 'Sales' FROM `{ds}.emp` WHERE dept = 'Sales'
			)
			SELECT COUNT(*) AS cnt FROM combined");
		Assert.Equal("6", rows[0]["cnt"]?.ToString());
	}

	// ---- CTE with DISTINCT ----
	[Fact] public async Task Cte_Distinct()
	{
		var rows = await Q(@"
			WITH all_depts AS (
				SELECT dept FROM `{ds}.emp`
			)
			SELECT DISTINCT dept FROM all_depts ORDER BY dept");
		Assert.Equal(3, rows.Count);
	}

	// ---- Deeply nested CTEs ----
	[Fact] public async Task Cte_ThreeLevel()
	{
		var v = await S(@"
			WITH l1 AS (SELECT * FROM `{ds}.emp` WHERE salary > 60000),
			     l2 AS (SELECT * FROM l1 WHERE dept = 'Eng'),
			     l3 AS (SELECT MAX(salary) AS max_sal FROM l2)
			SELECT max_sal FROM l3");
		Assert.Equal("90000", v);
	}

	// ---- CTE with EXISTS ----
	[Fact] public async Task Cte_WithExists()
	{
		var rows = await Q(@"
			WITH managers AS (
				SELECT DISTINCT mgr_id FROM `{ds}.emp` WHERE mgr_id IS NOT NULL
			)
			SELECT name FROM `{ds}.emp` e
			WHERE EXISTS (SELECT 1 FROM managers m WHERE m.mgr_id = e.id)
			ORDER BY name");
		Assert.Equal(3, rows.Count); // Alice(1), Carol(3), Eve(5)
	}
}
