using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// CTE (Common Table Expression) pattern coverage: basic, recursive, multiple CTEs, nested, with DML.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_clause
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class CtePatternCoverageTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public CtePatternCoverageTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_ctpc_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.emp` (eid INT64, name STRING, dept STRING, salary INT64, mgr_id INT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.emp` VALUES
			(1,'Alice','Eng',90000,NULL),(2,'Bob','Eng',75000,1),(3,'Carol','Sales',70000,1),
			(4,'Dave','Sales',65000,3),(5,'Eve','HR',60000,1),(6,'Frank','HR',58000,5),
			(7,'Grace','Eng',85000,2),(8,'Hank','Sales',72000,3)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- Basic CTE ----
	[Fact] public async Task Cte_Basic()
	{
		var rows = await Q(@"
			WITH eng AS (SELECT * FROM `{ds}.emp` WHERE dept = 'Eng')
			SELECT name FROM eng ORDER BY name");
		Assert.Equal(3, rows.Count);
	}
	[Fact] public async Task Cte_WithAggregate()
	{
		var v = await S(@"
			WITH totals AS (SELECT dept, SUM(salary) AS total FROM `{ds}.emp` GROUP BY dept)
			SELECT dept FROM totals ORDER BY total DESC LIMIT 1");
		Assert.Equal("Eng", v);
	}

	// ---- Multiple CTEs ----
	[Fact] public async Task MultiCte()
	{
		var rows = await Q(@"
			WITH eng AS (SELECT * FROM `{ds}.emp` WHERE dept = 'Eng'),
				 sales AS (SELECT * FROM `{ds}.emp` WHERE dept = 'Sales')
			SELECT name, dept FROM eng UNION ALL SELECT name, dept FROM sales
			ORDER BY name");
		Assert.Equal(6, rows.Count);
	}
	[Fact] public async Task MultiCte_Reference()
	{
		var rows = await Q(@"
			WITH base AS (SELECT dept, SUM(salary) AS total FROM `{ds}.emp` GROUP BY dept),
				 ranked AS (SELECT dept, total, ROW_NUMBER() OVER (ORDER BY total DESC) AS rn FROM base)
			SELECT dept, total FROM ranked WHERE rn <= 2 ORDER BY total DESC");
		Assert.Equal(2, rows.Count);
	}

	// ---- CTE used multiple times ----
	[Fact] public async Task Cte_UsedTwice()
	{
		var rows = await Q(@"
			WITH dept_summary AS (
				SELECT dept, COUNT(*) AS cnt, SUM(salary) AS total FROM `{ds}.emp` GROUP BY dept
			)
			SELECT a.dept, a.cnt, a.total FROM dept_summary a
			JOIN dept_summary b ON a.dept = b.dept
			ORDER BY a.dept");
		Assert.Equal(3, rows.Count);
	}

	// ---- CTE with window function ----
	[Fact] public async Task Cte_WithWindow()
	{
		var rows = await Q(@"
			WITH ranked AS (
				SELECT name, dept, salary,
					ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
				FROM `{ds}.emp`
			)
			SELECT name, dept FROM ranked WHERE rn = 1 ORDER BY dept");
		Assert.Equal(3, rows.Count); // Top earner per dept
	}

	// ---- CTE with HAVING ----
	[Fact] public async Task Cte_WithHaving()
	{
		var rows = await Q(@"
			WITH dept_counts AS (
				SELECT dept, COUNT(*) AS cnt FROM `{ds}.emp` GROUP BY dept HAVING COUNT(*) >= 3
			)
			SELECT dept FROM dept_counts ORDER BY dept");
		Assert.Equal(2, rows.Count); // Eng(3), Sales(3)
	}

	// ---- CTE with DISTINCT ----
	[Fact] public async Task Cte_WithDistinct()
	{
		var rows = await Q(@"
			WITH depts AS (SELECT DISTINCT dept FROM `{ds}.emp`)
			SELECT dept FROM depts ORDER BY dept");
		Assert.Equal(3, rows.Count);
	}

	// ---- CTE with JOIN ----
	[Fact] public async Task Cte_WithJoin()
	{
		var rows = await Q(@"
			WITH managers AS (SELECT eid, name FROM `{ds}.emp` WHERE mgr_id IS NULL)
			SELECT e.name AS emp, m.name AS mgr
			FROM `{ds}.emp` e JOIN managers m ON e.mgr_id = m.eid
			ORDER BY e.name");
		Assert.True(rows.Count >= 3); // Bob, Carol, Eve report to Alice
	}

	// ---- CTE with LIMIT ----
	[Fact] public async Task Cte_WithLimit()
	{
		var rows = await Q(@"
			WITH top_earners AS (
				SELECT name, salary FROM `{ds}.emp` ORDER BY salary DESC LIMIT 3
			)
			SELECT name FROM top_earners ORDER BY name");
		Assert.Equal(3, rows.Count);
	}

	// ---- CTE with UNION ALL ----
	[Fact] public async Task Cte_WithUnion()
	{
		var rows = await Q(@"
			WITH combined AS (
				SELECT name, salary FROM `{ds}.emp` WHERE dept = 'Eng'
				UNION ALL
				SELECT name, salary FROM `{ds}.emp` WHERE dept = 'HR'
			)
			SELECT COUNT(*) AS cnt FROM combined");
		Assert.Equal("5", rows[0]["cnt"]?.ToString()); // 3 Eng + 2 HR
	}

	// ---- CTE chaining ----
	[Fact] public async Task Cte_Chain()
	{
		var rows = await Q(@"
			WITH step1 AS (SELECT dept, AVG(salary) AS avg_sal FROM `{ds}.emp` GROUP BY dept),
				 step2 AS (SELECT dept, avg_sal FROM step1 WHERE avg_sal > 65000)
			SELECT dept FROM step2 ORDER BY dept");
		Assert.True(rows.Count >= 1); // Eng avg ~83333
	}

	// ---- CTE with CASE ----
	[Fact] public async Task Cte_WithCase()
	{
		var rows = await Q(@"
			WITH categorized AS (
				SELECT name, CASE WHEN salary >= 80000 THEN 'High' WHEN salary >= 65000 THEN 'Mid' ELSE 'Low' END AS tier
				FROM `{ds}.emp`
			)
			SELECT tier, COUNT(*) AS cnt FROM categorized GROUP BY tier ORDER BY tier");
		Assert.Equal(3, rows.Count);
	}

	// ---- CTE with subquery in SELECT ----
	[Fact] public async Task Cte_WithScalarSubquery()
	{
		var rows = await Q(@"
			WITH avg_by_dept AS (SELECT dept, AVG(salary) AS avg_sal FROM `{ds}.emp` GROUP BY dept)
			SELECT e.name, e.salary,
				(SELECT avg_sal FROM avg_by_dept a WHERE a.dept = e.dept) AS dept_avg
			FROM `{ds}.emp` e
			ORDER BY e.name LIMIT 3");
		Assert.Equal(3, rows.Count);
		Assert.NotNull(rows[0]["dept_avg"]?.ToString());
	}
}
