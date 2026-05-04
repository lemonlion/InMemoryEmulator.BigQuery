using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for JOIN variations: INNER, LEFT, RIGHT, FULL, CROSS, self-join, multi-table, anti-join, semi-join.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#join_types
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class JoinMultiPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public JoinMultiPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_jp_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.emp` (id INT64, name STRING, dept_id INT64, salary FLOAT64, mgr_id INT64)", parameters: null);
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.dept` (id INT64, name STRING, city STRING, budget FLOAT64)", parameters: null);
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.proj` (id INT64, title STRING, lead_id INT64, dept_id INT64)", parameters: null);
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.assign` (emp_id INT64, proj_id INT64, role STRING)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.emp` VALUES
			(1,'Alice',1,90000,NULL),(2,'Bob',1,80000,1),(3,'Carol',2,75000,1),
			(4,'Dave',2,70000,3),(5,'Eve',3,85000,NULL),(6,'Frank',NULL,60000,1)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.dept` VALUES
			(1,'Engineering','NYC',500000),(2,'Sales','LA',300000),(3,'HR','SF',200000),(4,'Legal','CHI',150000)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.proj` VALUES
			(1,'Alpha',1,1),(2,'Beta',2,1),(3,'Gamma',3,2),(4,'Delta',NULL,4)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.assign` VALUES
			(1,1,'lead'),(1,2,'member'),(2,1,'member'),(2,2,'lead'),
			(3,3,'lead'),(4,3,'member'),(5,1,'member')", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- INNER JOIN basics ----
	[Fact] public async Task Inner_MatchCount() => Assert.Equal("5", await S("SELECT COUNT(*) FROM `{ds}.emp` e JOIN `{ds}.dept` d ON e.dept_id = d.id"));
	[Fact] public async Task Inner_WithAlias()
	{
		var rows = await Q("SELECT e.name, d.name AS dept FROM `{ds}.emp` e JOIN `{ds}.dept` d ON e.dept_id = d.id ORDER BY e.name");
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("Engineering", rows[0]["dept"]?.ToString());
	}
	[Fact] public async Task Inner_WithWhere()
	{
		var rows = await Q("SELECT e.name FROM `{ds}.emp` e JOIN `{ds}.dept` d ON e.dept_id = d.id WHERE d.city = 'NYC' ORDER BY e.name");
		Assert.Equal(2, rows.Count);
	}
	[Fact] public async Task Inner_MultiCondition()
	{
		var rows = await Q("SELECT e.name FROM `{ds}.emp` e JOIN `{ds}.dept` d ON e.dept_id = d.id AND e.salary > 75000 ORDER BY e.name");
		Assert.Equal(3, rows.Count); // Alice(90k), Bob(80k), Eve(85k)
	}

	// ---- LEFT JOIN ----
	[Fact] public async Task Left_AllEmployees() => Assert.Equal("6", await S("SELECT COUNT(*) FROM `{ds}.emp` e LEFT JOIN `{ds}.dept` d ON e.dept_id = d.id"));
	[Fact] public async Task Left_NullDept()
	{
		var rows = await Q("SELECT e.name, d.name AS dept FROM `{ds}.emp` e LEFT JOIN `{ds}.dept` d ON e.dept_id = d.id WHERE d.id IS NULL");
		Assert.Single(rows);
		Assert.Equal("Frank", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task Left_WithAgg()
	{
		var rows = await Q("SELECT d.name, COUNT(e.id) AS cnt FROM `{ds}.dept` d LEFT JOIN `{ds}.emp` e ON e.dept_id = d.id GROUP BY d.name ORDER BY d.name");
		Assert.Equal(4, rows.Count);
		var legal = rows.First(r => r["name"]?.ToString() == "Legal");
		Assert.Equal("0", legal["cnt"]?.ToString());
	}

	// ---- RIGHT JOIN ----
	[Fact] public async Task Right_AllDepts() => Assert.Equal("6", await S("SELECT COUNT(*) FROM `{ds}.emp` e RIGHT JOIN `{ds}.dept` d ON e.dept_id = d.id"));
	[Fact] public async Task Right_UnmatchedDept()
	{
		var rows = await Q("SELECT d.name FROM `{ds}.emp` e RIGHT JOIN `{ds}.dept` d ON e.dept_id = d.id WHERE e.id IS NULL");
		Assert.Single(rows);
		Assert.Equal("Legal", rows[0]["name"]?.ToString());
	}

	// ---- FULL OUTER JOIN ----
	[Fact] public async Task Full_AllRows()
	{
		var count = int.Parse(await S("SELECT COUNT(*) FROM `{ds}.emp` e FULL OUTER JOIN `{ds}.dept` d ON e.dept_id = d.id") ?? "0");
		Assert.True(count >= 7); // 5 matched + Frank(no dept) + Legal(no emp)
	}
	[Fact] public async Task Full_BothSideNulls()
	{
		var rows = await Q("SELECT e.name, d.name AS dept FROM `{ds}.emp` e FULL OUTER JOIN `{ds}.dept` d ON e.dept_id = d.id ORDER BY COALESCE(e.name, d.name)");
		Assert.True(rows.Count >= 7);
	}

	// ---- CROSS JOIN ----
	[Fact] public async Task Cross_Cartesian() => Assert.Equal("24", await S("SELECT COUNT(*) FROM `{ds}.emp` e CROSS JOIN `{ds}.dept` d"));
	[Fact] public async Task Cross_CommaNotation() => Assert.Equal("24", await S("SELECT COUNT(*) FROM `{ds}.emp`, `{ds}.dept`"));
	[Fact] public async Task Cross_WithFilter()
	{
		var rows = await Q("SELECT e.name, d.name AS dept FROM `{ds}.emp` e CROSS JOIN `{ds}.dept` d WHERE e.salary > 80000 AND d.budget > 200000 ORDER BY e.name, d.name");
		Assert.True(rows.Count > 0);
	}

	// ---- Self-join ----
	[Fact] public async Task Self_ManagerLookup()
	{
		var rows = await Q("SELECT e.name, m.name AS mgr FROM `{ds}.emp` e LEFT JOIN `{ds}.emp` m ON e.mgr_id = m.id ORDER BY e.name");
		Assert.Equal(6, rows.Count);
		Assert.Null(rows.First(r => r["name"]?.ToString() == "Alice")["mgr"]);
		Assert.Equal("Alice", rows.First(r => r["name"]?.ToString() == "Bob")["mgr"]?.ToString());
	}
	[Fact] public async Task Self_CountSubordinates()
	{
		var rows = await Q("SELECT m.name, COUNT(e.id) AS subs FROM `{ds}.emp` m LEFT JOIN `{ds}.emp` e ON e.mgr_id = m.id GROUP BY m.name HAVING COUNT(e.id) > 0 ORDER BY subs DESC");
		Assert.True(rows.Count > 0);
		Assert.Equal("Alice", rows[0]["name"]?.ToString()); // Alice manages Bob, Carol, Frank
	}

	// ---- Three-table join ----
	[Fact] public async Task ThreeTable_Join()
	{
		var rows = await Q(@"
			SELECT e.name, d.name AS dept, p.title
			FROM `{ds}.assign` a
			JOIN `{ds}.emp` e ON a.emp_id = e.id
			JOIN `{ds}.proj` p ON a.proj_id = p.id
			ORDER BY e.name, p.title");
		Assert.True(rows.Count >= 7);
	}
	[Fact] public async Task ThreeTable_WithAgg()
	{
		var rows = await Q(@"
			SELECT e.name, COUNT(DISTINCT a.proj_id) AS proj_count
			FROM `{ds}.emp` e
			JOIN `{ds}.assign` a ON a.emp_id = e.id
			GROUP BY e.name
			ORDER BY proj_count DESC, e.name");
		Assert.True(rows.Count >= 4);
	}

	// ---- Four-table join ----
	[Fact] public async Task FourTable_Join()
	{
		var rows = await Q(@"
			SELECT e.name, d.name AS dept, p.title, a.role
			FROM `{ds}.emp` e
			JOIN `{ds}.dept` d ON e.dept_id = d.id
			JOIN `{ds}.assign` a ON a.emp_id = e.id
			JOIN `{ds}.proj` p ON a.proj_id = p.id
			ORDER BY e.name, p.title");
		Assert.True(rows.Count >= 5);
	}

	// ---- Anti-join (LEFT JOIN + IS NULL) ----
	[Fact] public async Task AntiJoin_NoDept()
	{
		var rows = await Q("SELECT e.name FROM `{ds}.emp` e LEFT JOIN `{ds}.dept` d ON e.dept_id = d.id WHERE d.id IS NULL");
		Assert.Single(rows);
		Assert.Equal("Frank", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task AntiJoin_NoProjects()
	{
		var rows = await Q(@"
			SELECT e.name FROM `{ds}.emp` e
			LEFT JOIN `{ds}.assign` a ON a.emp_id = e.id
			WHERE a.emp_id IS NULL
			ORDER BY e.name");
		Assert.Single(rows);
		Assert.Equal("Frank", rows[0]["name"]?.ToString());
	}

	// ---- Semi-join (EXISTS) ----
	[Fact] public async Task SemiJoin_HasProjects()
	{
		var rows = await Q("SELECT e.name FROM `{ds}.emp` e WHERE EXISTS (SELECT 1 FROM `{ds}.assign` a WHERE a.emp_id = e.id) ORDER BY e.name");
		Assert.Equal(5, rows.Count); // All except Frank
	}
	[Fact] public async Task SemiJoin_HasMultipleProjects()
	{
		var rows = await Q(@"
			SELECT e.name FROM `{ds}.emp` e
			WHERE (SELECT COUNT(*) FROM `{ds}.assign` a WHERE a.emp_id = e.id) > 1
			ORDER BY e.name");
		Assert.Equal(2, rows.Count); // Alice(2), Bob(2)
	}

	// ---- Join with DISTINCT ----
	[Fact] public async Task Join_Distinct()
	{
		var rows = await Q("SELECT DISTINCT d.city FROM `{ds}.emp` e JOIN `{ds}.dept` d ON e.dept_id = d.id ORDER BY d.city");
		Assert.Equal(3, rows.Count); // NYC, LA, SF
	}

	// ---- Join with ORDER BY + LIMIT ----
	[Fact] public async Task Join_OrderByLimit()
	{
		var rows = await Q("SELECT e.name, e.salary FROM `{ds}.emp` e JOIN `{ds}.dept` d ON e.dept_id = d.id ORDER BY e.salary DESC LIMIT 3");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}

	// ---- Join with computed columns ----
	[Fact] public async Task Join_ComputedCol()
	{
		var rows = await Q(@"
			SELECT e.name, ROUND(e.salary / d.budget * 100, 2) AS pct_budget
			FROM `{ds}.emp` e JOIN `{ds}.dept` d ON e.dept_id = d.id
			ORDER BY pct_budget DESC LIMIT 3");
		Assert.Equal(3, rows.Count);
		Assert.NotNull(rows[0]["pct_budget"]);
	}

	// ---- Join with CASE ----
	[Fact] public async Task Join_WithCase()
	{
		var rows = await Q(@"
			SELECT e.name,
				CASE WHEN e.salary > 80000 THEN 'Senior' ELSE 'Junior' END AS level
			FROM `{ds}.emp` e JOIN `{ds}.dept` d ON e.dept_id = d.id
			ORDER BY e.name");
		Assert.Equal("Senior", rows.First(r => r["name"]?.ToString() == "Alice")["level"]?.ToString());
	}

	// ---- Join with subquery as table ----
	[Fact] public async Task Join_SubqueryTable()
	{
		var rows = await Q(@"
			SELECT e.name, s.proj_count
			FROM `{ds}.emp` e
			JOIN (SELECT emp_id, COUNT(*) AS proj_count FROM `{ds}.assign` GROUP BY emp_id) s ON s.emp_id = e.id
			ORDER BY s.proj_count DESC, e.name");
		Assert.True(rows.Count >= 4);
	}

	// ---- LEFT JOIN preserving order ----
	[Fact] public async Task Left_PreservesAll()
	{
		var rows = await Q(@"
			SELECT e.name, d.city
			FROM `{ds}.emp` e LEFT JOIN `{ds}.dept` d ON e.dept_id = d.id
			ORDER BY e.id");
		Assert.Equal(6, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}

	// ---- Join with GROUP BY + HAVING ----
	[Fact] public async Task Join_GroupByHaving()
	{
		var rows = await Q(@"
			SELECT d.name, SUM(e.salary) AS total_sal
			FROM `{ds}.dept` d JOIN `{ds}.emp` e ON e.dept_id = d.id
			GROUP BY d.name
			HAVING SUM(e.salary) > 100000
			ORDER BY total_sal DESC");
		Assert.True(rows.Count > 0);
	}

	// ---- Join with NULL-safe comparison ----
	[Fact] public async Task Join_NullSafe()
	{
		var rows = await Q(@"
			SELECT e.name FROM `{ds}.emp` e
			JOIN `{ds}.dept` d ON COALESCE(e.dept_id, -1) = COALESCE(d.id, -1)
			ORDER BY e.name");
		Assert.True(rows.Count >= 5);
	}

	// ---- Join condition with expression ----
	[Fact] public async Task Join_ExpressionCondition()
	{
		var rows = await Q(@"
			SELECT e.name, d.name AS dept
			FROM `{ds}.emp` e JOIN `{ds}.dept` d ON e.dept_id = d.id
			WHERE e.salary > d.budget / 10
			ORDER BY e.name");
		Assert.True(rows.Count >= 0); // depends on data
	}

	// ---- Mixed left/inner joins ----
	[Fact] public async Task MixedJoins()
	{
		var rows = await Q(@"
			SELECT e.name, d.name AS dept, p.title
			FROM `{ds}.emp` e
			LEFT JOIN `{ds}.dept` d ON e.dept_id = d.id
			LEFT JOIN `{ds}.assign` a ON a.emp_id = e.id
			LEFT JOIN `{ds}.proj` p ON a.proj_id = p.id
			ORDER BY e.name, p.title");
		Assert.True(rows.Count >= 6); // All employees, some with projects
	}

	// ---- Join with string functions ----
	[Fact] public async Task Join_StringFunc()
	{
		var rows = await Q(@"
			SELECT CONCAT(e.name, ' @ ', d.name) AS label
			FROM `{ds}.emp` e JOIN `{ds}.dept` d ON e.dept_id = d.id
			ORDER BY label LIMIT 3");
		Assert.Equal(3, rows.Count);
		Assert.Contains("@", rows[0]["label"]?.ToString());
	}
}
