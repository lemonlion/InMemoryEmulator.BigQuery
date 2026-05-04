using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Join pattern coverage: INNER, LEFT, RIGHT, FULL, CROSS, self-joins, multi-way, with aggregates.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#join_types
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class JoinPatternCoverageTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public JoinPatternCoverageTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_jpc_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.emp` (eid INT64, name STRING, did INT64, salary INT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.emp` VALUES
			(1,'Alice',1,90000),(2,'Bob',1,75000),(3,'Carol',2,70000),
			(4,'Dave',2,65000),(5,'Eve',3,60000),(6,'Frank',NULL,55000)", parameters: null);
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.dept` (did INT64, dname STRING, budget INT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.dept` VALUES
			(1,'Eng',500000),(2,'Sales',300000),(3,'HR',200000),(4,'Marketing',100000)", parameters: null);
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.proj` (pid INT64, eid INT64, pname STRING)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.proj` VALUES
			(1,1,'Alpha'),(2,1,'Beta'),(3,2,'Alpha'),(4,3,'Gamma'),(5,5,'Delta')", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- INNER JOIN ----
	[Fact] public async Task InnerJoin_Basic()
	{
		var rows = await Q("SELECT e.name, d.dname FROM `{ds}.emp` e INNER JOIN `{ds}.dept` d ON e.did = d.did ORDER BY e.name");
		Assert.Equal(5, rows.Count); // Frank has NULL did → no match
	}
	[Fact] public async Task InnerJoin_WithWhere()
	{
		var rows = await Q("SELECT e.name FROM `{ds}.emp` e INNER JOIN `{ds}.dept` d ON e.did = d.did WHERE d.dname = 'Eng' ORDER BY e.name");
		Assert.Equal(2, rows.Count); // Alice, Bob
	}
	[Fact] public async Task InnerJoin_WithAggregate()
	{
		var rows = await Q("SELECT d.dname, COUNT(*) AS cnt FROM `{ds}.emp` e INNER JOIN `{ds}.dept` d ON e.did = d.did GROUP BY d.dname ORDER BY d.dname");
		Assert.Equal(3, rows.Count);
	}

	// ---- LEFT JOIN ----
	[Fact] public async Task LeftJoin_Basic()
	{
		var rows = await Q("SELECT e.name, d.dname FROM `{ds}.emp` e LEFT JOIN `{ds}.dept` d ON e.did = d.did ORDER BY e.name");
		Assert.Equal(6, rows.Count); // All emps, Frank has NULL dname
		var frank = rows.First(r => r["name"]?.ToString() == "Frank");
		Assert.Null(frank["dname"]);
	}
	[Fact] public async Task LeftJoin_NoMatch()
	{
		// Marketing dept exists but no employees have did=4
		var rows = await Q("SELECT d.dname, e.name FROM `{ds}.dept` d LEFT JOIN `{ds}.emp` e ON e.did = d.did WHERE d.did = 4");
		Assert.Single(rows);
		Assert.Null(rows[0]["name"]);
	}

	// ---- RIGHT JOIN ----
	[Fact] public async Task RightJoin_Basic()
	{
		var rows = await Q("SELECT e.name, d.dname FROM `{ds}.emp` e RIGHT JOIN `{ds}.dept` d ON e.did = d.did ORDER BY d.dname");
		Assert.True(rows.Count >= 6); // All depts, Marketing has NULL emp
	}

	// ---- FULL OUTER JOIN ----
	[Fact] public async Task FullJoin_Basic()
	{
		var rows = await Q("SELECT e.name, d.dname FROM `{ds}.emp` e FULL OUTER JOIN `{ds}.dept` d ON e.did = d.did ORDER BY e.name");
		Assert.True(rows.Count >= 7); // 5 matches + Frank (no dept) + Marketing (no emp)
	}

	// ---- CROSS JOIN ----
	[Fact] public async Task CrossJoin_Basic()
	{
		var rows = await Q("SELECT e.name, d.dname FROM `{ds}.emp` e CROSS JOIN `{ds}.dept` d");
		Assert.Equal(24, rows.Count); // 6 * 4
	}
	[Fact] public async Task CrossJoin_WithWhere()
	{
		var rows = await Q("SELECT e.name, d.dname FROM `{ds}.emp` e CROSS JOIN `{ds}.dept` d WHERE e.did = d.did ORDER BY e.name");
		Assert.Equal(5, rows.Count); // Same as INNER JOIN
	}

	// ---- Self JOIN ----
	[Fact] public async Task SelfJoin_SameDept()
	{
		var rows = await Q(@"
			SELECT a.name AS emp1, b.name AS emp2
			FROM `{ds}.emp` a JOIN `{ds}.emp` b ON a.did = b.did
			WHERE a.eid < b.eid ORDER BY a.name, b.name");
		Assert.True(rows.Count > 0);
	}

	// ---- Multi-way JOIN ----
	[Fact] public async Task ThreeWayJoin()
	{
		var rows = await Q(@"
			SELECT e.name, d.dname, p.pname
			FROM `{ds}.emp` e
			JOIN `{ds}.dept` d ON e.did = d.did
			JOIN `{ds}.proj` p ON p.eid = e.eid
			ORDER BY e.name, p.pname");
		Assert.True(rows.Count >= 4); // Alice(Alpha,Beta), Bob(Alpha), Carol(Gamma)
	}

	// ---- JOIN with aliases ----
	[Fact] public async Task Join_WithAliases()
	{
		var rows = await Q("SELECT e.name AS employee, d.dname AS department FROM `{ds}.emp` e JOIN `{ds}.dept` d ON e.did = d.did ORDER BY employee");
		Assert.Equal(5, rows.Count);
		Assert.Equal("Alice", rows[0]["employee"]?.ToString());
	}

	// ---- JOIN with aggregate ----
	[Fact] public async Task Join_SumByDept()
	{
		var rows = await Q(@"
			SELECT d.dname, SUM(e.salary) AS total
			FROM `{ds}.emp` e JOIN `{ds}.dept` d ON e.did = d.did
			GROUP BY d.dname ORDER BY total DESC");
		Assert.Equal("Eng", rows[0]["dname"]?.ToString());
	}
	[Fact] public async Task Join_CountProjects()
	{
		var rows = await Q(@"
			SELECT e.name, COUNT(p.pid) AS proj_count
			FROM `{ds}.emp` e LEFT JOIN `{ds}.proj` p ON e.eid = p.eid
			GROUP BY e.name ORDER BY proj_count DESC, e.name");
		Assert.Equal("Alice", rows[0]["name"]?.ToString()); // 2 projects
	}

	// ---- JOIN with DISTINCT ----
	[Fact] public async Task Join_Distinct()
	{
		var rows = await Q("SELECT DISTINCT d.dname FROM `{ds}.emp` e JOIN `{ds}.dept` d ON e.did = d.did ORDER BY d.dname");
		Assert.Equal(3, rows.Count); // Eng, Sales, HR
	}

	// ---- JOIN with subquery ----
	[Fact] public async Task Join_Subquery()
	{
		var rows = await Q(@"
			SELECT e.name, sub.total FROM `{ds}.emp` e
			JOIN (SELECT did, SUM(salary) AS total FROM `{ds}.emp` GROUP BY did) sub
			ON e.did = sub.did
			ORDER BY e.name");
		Assert.Equal(5, rows.Count);
	}

	// ---- JOIN with ORDER BY and LIMIT ----
	[Fact] public async Task Join_OrderLimit()
	{
		var rows = await Q(@"
			SELECT e.name, d.dname FROM `{ds}.emp` e
			JOIN `{ds}.dept` d ON e.did = d.did
			ORDER BY e.salary DESC LIMIT 3");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString()); // Highest salary
	}

	// ---- LEFT JOIN with IS NULL (anti-join) ----
	[Fact] public async Task LeftJoin_AntiJoin()
	{
		var rows = await Q(@"
			SELECT d.dname FROM `{ds}.dept` d
			LEFT JOIN `{ds}.emp` e ON e.did = d.did
			WHERE e.eid IS NULL");
		Assert.Single(rows); // Marketing
		Assert.Equal("Marketing", rows[0]["dname"]?.ToString());
	}

	// ---- JOIN with COALESCE ----
	[Fact] public async Task Join_Coalesce()
	{
		var rows = await Q(@"
			SELECT e.name, COALESCE(d.dname, 'No Dept') AS dept
			FROM `{ds}.emp` e LEFT JOIN `{ds}.dept` d ON e.did = d.did
			ORDER BY e.name");
		Assert.Equal(6, rows.Count);
		var frank = rows.First(r => r["name"]?.ToString() == "Frank");
		Assert.Equal("No Dept", frank["dept"]?.ToString());
	}
}
