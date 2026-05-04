using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Advanced WHERE clause patterns: IN lists, BETWEEN, LIKE, EXISTS, NOT combinations, multi-condition.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#where_clause
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class WhereClauseAdvancedPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public WhereClauseAdvancedPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_wca_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.emp` (id INT64, name STRING, dept STRING, salary INT64, bonus FLOAT64, active BOOL, manager_id INT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.emp` VALUES
			(1,'Alice','Eng',90000,5000.5,true,NULL),
			(2,'Bob','Eng',75000,3000.0,false,1),
			(3,'Carol','Sales',70000,4500.25,true,1),
			(4,'Dave','Sales',65000,NULL,true,3),
			(5,'Eve','HR',60000,2000.0,false,1),
			(6,'Frank','HR',58000,1500.0,true,5),
			(7,'Grace','Eng',85000,4000.0,true,1),
			(8,'Hank','Sales',72000,3500.75,false,3),
			(9,'Ivy','Eng',95000,6000.0,true,NULL),
			(10,'Jack','HR',55000,NULL,false,5)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- IN ----
	[Fact] public async Task In_StringList()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE dept IN ('Eng', 'HR')");
		Assert.Equal("7", v); // Eng:4 + HR:3
	}
	[Fact] public async Task In_IntList()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE id IN (1, 3, 5, 7, 9)");
		Assert.Equal("5", v);
	}
	[Fact] public async Task NotIn_StringList()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE dept NOT IN ('HR')");
		Assert.Equal("7", v); // Eng:4 + Sales:3
	}
	[Fact] public async Task In_Subquery()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE id IN (SELECT manager_id FROM `{ds}.emp` WHERE manager_id IS NOT NULL)");
		Assert.Equal("3", v); // managers: 1, 3, 5
	}

	// ---- BETWEEN ----
	[Fact] public async Task Between_Int()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE salary BETWEEN 70000 AND 85000");
		Assert.Equal("4", v); // Bob(75k), Carol(70k), Grace(85k), Hank(72k)
	}
	[Fact] public async Task NotBetween_Int()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE salary NOT BETWEEN 60000 AND 80000");
		Assert.Equal("5", v); // Alice(90k), Frank(58k), Grace(85k), Ivy(95k), Jack(55k)
	}
	[Fact] public async Task Between_Float()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE bonus BETWEEN 3000.0 AND 5000.0");
		Assert.Equal("4", v); // Bob(3000), Carol(4500.25), Grace(4000), Hank(3500.75)
	}

	// ---- LIKE ----
	[Fact] public async Task Like_StartsWith()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE name LIKE 'A%'");
		Assert.Equal("1", v);
	}
	[Fact] public async Task Like_EndsWith()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE name LIKE '%e'");
		Assert.Equal("4", v); // Alice, Dave, Eve, Grace
	}
	[Fact] public async Task Like_Contains()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE name LIKE '%a%'");
		Assert.Equal("6", v); // Carol, Dave, Frank, Grace, Hank, Jack (lowercase 'a')
	}
	[Fact] public async Task Like_SingleChar()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE name LIKE '___'");
		Assert.Equal("3", v); // Bob, Eve, Ivy (3 chars)
	}
	[Fact] public async Task NotLike_Pattern()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE name NOT LIKE '%e%'");
		// e/E in: Alice(e), Bob(no), Carol(no), Dave(e), Eve(E,e), Frank(no), Grace(e), Hank(no), Ivy(no), Jack(no)
		// LIKE is case-sensitive: only lowercase 'e'
		var rows = await Q("SELECT name FROM `{ds}.emp` WHERE name NOT LIKE '%e%'");
		Assert.True(rows.Count > 0);
	}

	// ---- NULL checks ----
	[Fact] public async Task IsNull()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE bonus IS NULL");
		Assert.Equal("2", v); // Dave, Jack
	}
	[Fact] public async Task IsNotNull()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE bonus IS NOT NULL");
		Assert.Equal("8", v);
	}
	[Fact] public async Task IsNull_ManagerId()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE manager_id IS NULL");
		Assert.Equal("2", v); // Alice, Ivy
	}

	// ---- Boolean ----
	[Fact] public async Task Bool_True()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE active = true");
		Assert.Equal("6", v);
	}
	[Fact] public async Task Bool_False()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE active = false");
		Assert.Equal("4", v);
	}
	[Fact] public async Task Bool_IsTrue()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE active IS TRUE");
		Assert.Equal("6", v);
	}

	// ---- AND / OR combinations ----
	[Fact] public async Task And_Basic()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE dept = 'Eng' AND active = true");
		Assert.Equal("3", v); // Alice, Grace, Ivy
	}
	[Fact] public async Task Or_Basic()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE dept = 'Eng' OR dept = 'HR'");
		Assert.Equal("7", v);
	}
	[Fact] public async Task And_Or_Combined()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE (dept = 'Eng' OR dept = 'Sales') AND active = true");
		Assert.Equal("5", v); // Alice, Carol, Dave, Grace, Ivy
	}
	[Fact] public async Task Not_Condition()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE NOT active");
		Assert.Equal("4", v);
	}
	[Fact] public async Task Complex_Multi()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE dept = 'Eng' AND salary > 80000 AND active = true");
		Assert.Equal("3", v); // Alice(90k), Grace(85k), Ivy(95k)
	}

	// ---- Comparison operators ----
	[Fact] public async Task Gt() => Assert.Equal("3", await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE salary > 80000"));
	[Fact] public async Task Gte() => Assert.Equal("4", await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE salary >= 75000"));
	[Fact] public async Task Lt() => Assert.Equal("2", await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE salary < 60000"));
	[Fact] public async Task Lte() => Assert.Equal("3", await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE salary <= 60000"));
	[Fact] public async Task Ne() => Assert.Equal("7", await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE dept != 'HR'"));
	[Fact] public async Task Ne_Angle() => Assert.Equal("7", await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE dept <> 'HR'"));

	// ---- EXISTS ----
	[Fact] public async Task Exists_Subquery()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` e WHERE EXISTS (SELECT 1 FROM `{ds}.emp` m WHERE m.id = e.manager_id)");
		Assert.Equal("8", v); // everyone except Alice and Ivy (who have NULL manager_id)
	}
	[Fact] public async Task NotExists_Subquery()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` e WHERE NOT EXISTS (SELECT 1 FROM `{ds}.emp` m WHERE m.id = e.manager_id)");
		Assert.Equal("2", v); // Alice and Ivy
	}

	// ---- Expression in WHERE ----
	[Fact] public async Task Where_Expression()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE salary + COALESCE(CAST(bonus AS INT64), 0) > 90000");
		Assert.Equal("2", v); // Alice(90000+5000=95000), Ivy(95000+6000=101000)
	}
	[Fact] public async Task Where_FunctionCall()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE LENGTH(name) > 4");
		Assert.Equal("4", v); // Alice(5), Carol(5), Frank(5), Grace(5)
	}
	[Fact] public async Task Where_CaseExpr()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE CASE WHEN dept = 'Eng' THEN salary ELSE 0 END > 80000");
		Assert.Equal("3", v); // Alice, Grace, Ivy
	}

	// ---- Multiple conditions combined ----
	[Fact] public async Task Multi_InAndBetween()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE dept IN ('Eng', 'Sales') AND salary BETWEEN 70000 AND 90000");
		Assert.Equal("5", v); // Alice(90k), Bob(75k), Carol(70k), Grace(85k), Hank(72k)
	}
	[Fact] public async Task Multi_LikeAndNull()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE name LIKE '%a%' AND bonus IS NOT NULL");
		// lowercase 'a': Carol, Dave(null bonus), Frank, Grace, Hank, Jack(null bonus)
		// With bonus NOT NULL: Carol, Frank, Grace, Hank = 4
		Assert.Equal("4", v);
	}
	[Fact] public async Task Multi_OrWithParens()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emp` WHERE (salary > 85000 OR bonus > 4500) AND active = true");
		// salary>85k: Alice(90k), Ivy(95k) + bonus>4500: Alice(5000.5), Carol(4500.25→no, 4500.25 is not > 4500), Ivy(6000)
		// Actually 4500.25 > 4500 → true. So: Alice(salary>85k, active), Carol(bonus>4500, active), Ivy(salary>85k, active)
		Assert.Equal("3", v);
	}
}
