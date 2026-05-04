using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for WHERE clause: comparison, logical, BETWEEN, IN, LIKE, IS NULL, complex conditions.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#where_clause
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class WhereClauseComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public WhereClauseComprehensiveTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_wc_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.t` (id INT64, name STRING, score FLOAT64, grade STRING, active BOOL)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.t` VALUES
			(1,'Alice',95,'A',true),(2,'Bob',82,'B',true),(3,'Carol',78,'C',false),
			(4,'Dave',91,'A',true),(5,'Eve',65,'D',false),(6,'Frank',NULL,'F',NULL),
			(7,'Grace',88,'B',true),(8,'Hank',72,'C',false),(9,'Ivy',99,'A',true),
			(10,'Jack',55,'F',false)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- Comparison operators ----
	[Fact] public async Task Where_Equal() => Assert.Equal("1", await S("SELECT COUNT(*) FROM `{ds}.t` WHERE name = 'Alice'"));
	[Fact] public async Task Where_NotEqual() => Assert.Equal("9", await S("SELECT COUNT(*) FROM `{ds}.t` WHERE name != 'Alice'"));
	[Fact] public async Task Where_GreaterThan() => Assert.Equal("3", await S("SELECT COUNT(*) FROM `{ds}.t` WHERE score > 90"));
	[Fact] public async Task Where_GreaterEqual() => Assert.Equal("4", await S("SELECT COUNT(*) FROM `{ds}.t` WHERE score >= 88"));
	[Fact] public async Task Where_LessThan() => Assert.Equal("2", await S("SELECT COUNT(*) FROM `{ds}.t` WHERE score < 72"));
	[Fact] public async Task Where_LessEqual() => Assert.Equal("3", await S("SELECT COUNT(*) FROM `{ds}.t` WHERE score <= 72"));

	// ---- Logical operators ----
	[Fact] public async Task Where_And() => Assert.Equal("5", await S("SELECT COUNT(*) FROM `{ds}.t` WHERE score > 80 AND active = true"));
	[Fact] public async Task Where_Or() => Assert.Equal("5", await S("SELECT COUNT(*) FROM `{ds}.t` WHERE grade = 'A' OR grade = 'B'"));
	[Fact] public async Task Where_Not() => Assert.Equal("4", await S("SELECT COUNT(*) FROM `{ds}.t` WHERE NOT active"));
	[Fact] public async Task Where_AndOr()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.t` WHERE (grade = 'A' OR grade = 'B') AND active = true");
		Assert.True(int.Parse(v ?? "0") >= 4); // Alice, Bob, Dave, Grace, Ivy
	}
	[Fact] public async Task Where_NotAnd() => Assert.True(int.Parse(await S("SELECT COUNT(*) FROM `{ds}.t` WHERE NOT (grade = 'A' AND active = true)") ?? "0") >= 6);

	// ---- BETWEEN ----
	[Fact] public async Task Where_Between() => Assert.Equal("4", await S("SELECT COUNT(*) FROM `{ds}.t` WHERE score BETWEEN 70 AND 90"));
	[Fact] public async Task Where_NotBetween() => Assert.Equal("5", await S("SELECT COUNT(*) FROM `{ds}.t` WHERE score NOT BETWEEN 70 AND 90"));
	[Fact] public async Task Where_BetweenInclusive()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` WHERE score BETWEEN 88 AND 95 ORDER BY name");
		Assert.Contains(rows, r => r["name"]?.ToString() == "Alice"); // 95
		Assert.Contains(rows, r => r["name"]?.ToString() == "Grace"); // 88
	}

	// ---- IN ----
	[Fact] public async Task Where_InList()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` WHERE grade IN ('A', 'B') ORDER BY name");
		Assert.Equal(5, rows.Count);
	}
	[Fact] public async Task Where_NotIn()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` WHERE grade NOT IN ('A', 'B') ORDER BY name");
		Assert.Equal(5, rows.Count);
	}
	[Fact] public async Task Where_InNumbers()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` WHERE id IN (1, 3, 5, 7, 9) ORDER BY id");
		Assert.Equal(5, rows.Count);
	}

	// ---- LIKE ----
	[Fact] public async Task Where_LikeStart()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` WHERE name LIKE 'A%'");
		Assert.Single(rows);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task Where_LikeEnd()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` WHERE name LIKE '%e'");
		Assert.True(rows.Count >= 2); // Alice, Dave, Eve, Grace
	}
	[Fact] public async Task Where_LikeContains()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` WHERE name LIKE '%a%'");
		Assert.True(rows.Count >= 3); // Carol, Dave, Frank, Grace, Hank, Jack
	}
	[Fact] public async Task Where_LikeSingleChar()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` WHERE name LIKE '___'");
		Assert.True(rows.Count >= 2); // Bob, Eve, Ivy
	}
	[Fact] public async Task Where_NotLike()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` WHERE name NOT LIKE 'A%' ORDER BY name");
		Assert.Equal(9, rows.Count);
	}

	// ---- IS NULL / IS NOT NULL ----
	[Fact] public async Task Where_IsNull() => Assert.Equal("1", await S("SELECT COUNT(*) FROM `{ds}.t` WHERE score IS NULL"));
	[Fact] public async Task Where_IsNotNull() => Assert.Equal("9", await S("SELECT COUNT(*) FROM `{ds}.t` WHERE score IS NOT NULL"));
	[Fact] public async Task Where_BoolIsNull() => Assert.Equal("1", await S("SELECT COUNT(*) FROM `{ds}.t` WHERE active IS NULL"));

	// ---- Complex conditions ----
	[Fact] public async Task Where_NestedParens()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.t` WHERE (score > 80 AND grade = 'A') OR (score < 70 AND NOT active)");
		Assert.True(int.Parse(v ?? "0") >= 4);
	}
	[Fact] public async Task Where_CaseInWhere()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` WHERE CASE WHEN score > 90 THEN true ELSE false END ORDER BY name");
		Assert.True(rows.Count >= 3);
	}
	[Fact] public async Task Where_CoalesceFilter()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` WHERE COALESCE(score, 0) > 80 ORDER BY name");
		Assert.True(rows.Count >= 4);
	}
	[Fact] public async Task Where_FunctionInWhere()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` WHERE LENGTH(name) > 4 ORDER BY name");
		Assert.True(rows.Count >= 3); // Alice, Carol, Frank, Grace
	}
	[Fact] public async Task Where_ArithmeticExpr()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` WHERE score * 1.1 > 100 ORDER BY name");
		Assert.True(rows.Count >= 2); // 95*1.1=104.5, 91*1.1=100.1, 99*1.1=108.9
	}
	[Fact] public async Task Where_ConcatComparison()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` WHERE CONCAT(grade, '-', name) = 'A-Alice'");
		Assert.Single(rows);
	}

	// ---- WHERE with subquery ----
	[Fact] public async Task Where_ScalarSubquery()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` WHERE score > (SELECT AVG(score) FROM `{ds}.t`) ORDER BY name");
		Assert.True(rows.Count >= 3);
	}
	[Fact] public async Task Where_InSubquery()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` WHERE grade IN (SELECT grade FROM `{ds}.t` WHERE active = true) ORDER BY name");
		Assert.True(rows.Count >= 5);
	}

	// ---- WHERE with boolean column ----
	[Fact] public async Task Where_BoolDirect()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` WHERE active ORDER BY name");
		Assert.Equal(5, rows.Count);
	}
	[Fact] public async Task Where_BoolNegated()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` WHERE NOT active ORDER BY name");
		Assert.Equal(4, rows.Count);
	}
}
