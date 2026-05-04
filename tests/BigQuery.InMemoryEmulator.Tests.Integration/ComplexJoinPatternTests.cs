using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Complex multi-table JOIN patterns: self-join, 3+ table joins, join with aggregate, join with window.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#join_types
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ComplexJoinPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ComplexJoinPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_cjp_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.dept` (id INT64, name STRING, budget INT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.dept` VALUES (1,'Engineering',500000),(2,'Sales',300000),(3,'HR',200000),(4,'Marketing',250000)", parameters: null);

		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.emp` (id INT64, name STRING, dept_id INT64, salary INT64, mgr_id INT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.emp` VALUES
			(1,'Alice',1,90000,NULL),(2,'Bob',1,75000,1),(3,'Carol',2,70000,NULL),
			(4,'Dave',2,65000,3),(5,'Eve',3,60000,NULL),(6,'Frank',1,85000,1),
			(7,'Grace',2,72000,3),(8,'Hank',3,55000,5)", parameters: null);

		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.proj` (id INT64, name STRING, dept_id INT64, lead_id INT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.proj` VALUES
			(1,'Alpha',1,1),(2,'Beta',1,2),(3,'Gamma',2,3),(4,'Delta',3,5),(5,'Epsilon',2,4)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- Self-join ----
	[Fact] public async Task SelfJoin_ManagerName()
	{
		var rows = await Q("SELECT e.name AS emp, m.name AS mgr FROM `{ds}.emp` e JOIN `{ds}.emp` m ON e.mgr_id = m.id ORDER BY e.name");
		Assert.Equal(5, rows.Count); // Bob, Dave, Frank, Grace, Hank
		Assert.Equal("Bob", rows[0]["emp"]?.ToString());
		Assert.Equal("Alice", rows[0]["mgr"]?.ToString());
	}
	[Fact] public async Task SelfJoin_WithManagers()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` e JOIN `{ds}.emp` m ON e.mgr_id = m.id");
		Assert.Equal("5", v);
	}
	[Fact] public async Task SelfJoin_SameDept()
	{
		var rows = await Q("SELECT a.name AS emp1, b.name AS emp2 FROM `{ds}.emp` a JOIN `{ds}.emp` b ON a.dept_id = b.dept_id AND a.id < b.id ORDER BY a.name, b.name");
		Assert.True(rows.Count > 0);
	}

	// ---- Three-table join ----
	[Fact] public async Task ThreeTable_EmpDeptProj()
	{
		var rows = await Q(@"
			SELECT e.name AS emp, d.name AS dept, p.name AS project
			FROM `{ds}.emp` e
			JOIN `{ds}.dept` d ON e.dept_id = d.id
			JOIN `{ds}.proj` p ON p.lead_id = e.id
			ORDER BY e.name");
		Assert.True(rows.Count >= 4);
		Assert.Equal("Alice", rows[0]["emp"]?.ToString());
		Assert.Equal("Engineering", rows[0]["dept"]?.ToString());
		Assert.Equal("Alpha", rows[0]["project"]?.ToString());
	}
	[Fact] public async Task ThreeTable_Count()
	{
		var v = await S(@"
			SELECT COUNT(*) FROM `{ds}.emp` e
			JOIN `{ds}.dept` d ON e.dept_id = d.id
			JOIN `{ds}.proj` p ON p.dept_id = d.id");
		Assert.NotNull(v);
		var cnt = int.Parse(v!);
		Assert.True(cnt > 0);
	}

	// ---- LEFT JOIN ----
	[Fact] public async Task LeftJoin_AllEmps()
	{
		var rows = await Q("SELECT e.name, d.name AS dept FROM `{ds}.emp` e LEFT JOIN `{ds}.dept` d ON e.dept_id = d.id ORDER BY e.id");
		Assert.Equal(8, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("Engineering", rows[0]["dept"]?.ToString());
	}
	[Fact] public async Task LeftJoin_NoProject()
	{
		var rows = await Q("SELECT e.name, p.name AS project FROM `{ds}.emp` e LEFT JOIN `{ds}.proj` p ON p.lead_id = e.id ORDER BY e.name");
		Assert.Equal(8, rows.Count);
		// Frank has no project
		var frank = rows.First(r => r["name"]?.ToString() == "Frank");
		Assert.Null(frank["project"]);
	}

	// ---- RIGHT JOIN ----
	[Fact] public async Task RightJoin_AllDepts()
	{
		var rows = await Q("SELECT d.name AS dept, COUNT(e.id) AS emp_count FROM `{ds}.emp` e RIGHT JOIN `{ds}.dept` d ON e.dept_id = d.id GROUP BY d.name ORDER BY d.name");
		Assert.Equal(4, rows.Count);
		// Marketing has 0 employees
		var mkt = rows.First(r => r["dept"]?.ToString() == "Marketing");
		Assert.Equal("0", mkt["emp_count"]?.ToString());
	}

	// ---- FULL OUTER JOIN ----
	[Fact] public async Task FullJoin_DeptEmps()
	{
		var rows = await Q("SELECT d.name AS dept, e.name AS emp FROM `{ds}.dept` d FULL OUTER JOIN `{ds}.emp` e ON e.dept_id = d.id ORDER BY d.name, e.name");
		Assert.True(rows.Count >= 8);
		// Marketing dept exists but no employees matched
		var mkt = rows.Where(r => r["dept"]?.ToString() == "Marketing").ToList();
		Assert.True(mkt.Count >= 1);
	}

	// ---- CROSS JOIN ----
	[Fact] public async Task CrossJoin_Count()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.dept` CROSS JOIN `{ds}.proj`");
		Assert.Equal("20", v); // 4 depts * 5 projects
	}
	[Fact] public async Task CrossJoin_WithFilter()
	{
		var rows = await Q("SELECT d.name, p.name AS proj FROM `{ds}.dept` d CROSS JOIN `{ds}.proj` p WHERE d.id = p.dept_id ORDER BY d.name, p.name");
		Assert.Equal(5, rows.Count);
	}

	// ---- JOIN with aggregation ----
	[Fact] public async Task JoinAgg_DeptSalary()
	{
		var rows = await Q("SELECT d.name, SUM(e.salary) AS total FROM `{ds}.emp` e JOIN `{ds}.dept` d ON e.dept_id = d.id GROUP BY d.name ORDER BY total DESC");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Engineering", rows[0]["name"]?.ToString()); // 90+75+85=250k
		Assert.Equal("250000", rows[0]["total"]?.ToString());
	}
	[Fact] public async Task JoinAgg_DeptCount()
	{
		var rows = await Q("SELECT d.name, COUNT(e.id) AS cnt FROM `{ds}.emp` e JOIN `{ds}.dept` d ON e.dept_id = d.id GROUP BY d.name ORDER BY cnt DESC");
		Assert.Equal("3", rows[0]["cnt"]?.ToString());
	}
	[Fact] public async Task JoinAgg_AvgSalary()
	{
		var rows = await Q("SELECT d.name, CAST(AVG(e.salary) AS INT64) AS avg_sal FROM `{ds}.emp` e JOIN `{ds}.dept` d ON e.dept_id = d.id GROUP BY d.name ORDER BY d.name");
		Assert.Equal(3, rows.Count);
	}

	// ---- JOIN with window function ----
	[Fact] public async Task JoinWindow_RankInDept()
	{
		var rows = await Q(@"
			SELECT e.name, d.name AS dept, e.salary,
				RANK() OVER (PARTITION BY d.name ORDER BY e.salary DESC) AS rnk
			FROM `{ds}.emp` e JOIN `{ds}.dept` d ON e.dept_id = d.id
			ORDER BY d.name, rnk");
		Assert.True(rows.Count == 8);
		// Eng first: Alice(90k) rank 1
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("1", rows[0]["rnk"]?.ToString());
	}

	// ---- JOIN with subquery ----
	[Fact] public async Task JoinSubquery_TopPerDept()
	{
		var rows = await Q(@"
			SELECT e.name, d.name AS dept
			FROM `{ds}.emp` e
			JOIN `{ds}.dept` d ON e.dept_id = d.id
			JOIN (SELECT dept_id, MAX(salary) AS max_sal FROM `{ds}.emp` GROUP BY dept_id) m
				ON e.dept_id = m.dept_id AND e.salary = m.max_sal
			ORDER BY e.name");
		Assert.Equal(3, rows.Count); // Alice(Eng:90k), Eve(HR:60k), Grace(Sales:72k)
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("Eve", rows[1]["name"]?.ToString());
		Assert.Equal("Grace", rows[2]["name"]?.ToString());
	}

	// ---- JOIN with USING ----
	[Fact] public async Task Join_Using()
	{
		var rows = await Q(@"
			WITH e AS (SELECT id, name, dept_id FROM `{ds}.emp`),
				 d AS (SELECT id AS dept_id, name AS dept_name FROM `{ds}.dept`)
			SELECT e.name FROM e JOIN d USING (dept_id) ORDER BY e.name");
		Assert.Equal(8, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}

	// ---- Anti-join (LEFT JOIN WHERE NULL) ----
	[Fact] public async Task AntiJoin_NoProject()
	{
		var rows = await Q(@"
			SELECT e.name FROM `{ds}.emp` e
			LEFT JOIN `{ds}.proj` p ON p.lead_id = e.id
			WHERE p.id IS NULL
			ORDER BY e.name");
		Assert.Equal(3, rows.Count); // Frank, Grace, Hank have no project lead assignments
	}

	// ---- Semi-join (EXISTS) ----
	[Fact] public async Task SemiJoin_HasProject()
	{
		var rows = await Q(@"
			SELECT e.name FROM `{ds}.emp` e
			WHERE EXISTS (SELECT 1 FROM `{ds}.proj` p WHERE p.lead_id = e.id)
			ORDER BY e.name");
		Assert.Equal(5, rows.Count); // Alice, Bob, Carol, Dave, Eve
	}
}
