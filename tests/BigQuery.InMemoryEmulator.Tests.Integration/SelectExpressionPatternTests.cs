using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// SELECT clause patterns: aliases, expressions, CASE, nested functions, STRUCT literal, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_list
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class SelectExpressionPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public SelectExpressionPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_sep_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.t` (id INT64, name STRING, dept STRING, salary INT64, rating FLOAT64, active BOOL)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.t` VALUES
			(1,'Alice','Eng',90000,4.5,true),(2,'Bob','Eng',75000,3.8,false),
			(3,'Carol','Sales',70000,4.2,true),(4,'Dave','Sales',65000,3.5,true),
			(5,'Eve','HR',60000,4.0,false),(6,'Frank','HR',58000,3.2,true),
			(7,'Grace','Eng',85000,4.7,true),(8,'Hank','Sales',72000,3.9,false)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- Alias ----
	[Fact] public async Task Alias_Column() { var rows = await Q("SELECT name AS employee FROM `{ds}.t` ORDER BY employee LIMIT 1"); Assert.Equal("Alice", rows[0]["employee"]?.ToString()); }
	[Fact] public async Task Alias_Expression() { var rows = await Q("SELECT salary * 12 AS annual FROM `{ds}.t` WHERE id = 1"); Assert.Equal("1080000", rows[0]["annual"]?.ToString()); }
	[Fact] public async Task Alias_InOrderBy() { var rows = await Q("SELECT name, salary AS sal FROM `{ds}.t` ORDER BY sal DESC LIMIT 1"); Assert.Equal("Alice", rows[0]["name"]?.ToString()); }

	// ---- Arithmetic ----
	[Fact] public async Task Arith_Add() => Assert.Equal("5", await S("SELECT 2 + 3"));
	[Fact] public async Task Arith_Sub() => Assert.Equal("7", await S("SELECT 10 - 3"));
	[Fact] public async Task Arith_Mul() => Assert.Equal("24", await S("SELECT 6 * 4"));
	[Fact] public async Task Arith_IntDiv() => Assert.Equal("3", await S("SELECT 7 / 2")); // integer div
	[Fact] public async Task Arith_FloatDiv() => Assert.Equal("3.5", await S("SELECT 7.0 / 2"));
	[Fact] public async Task Arith_Mod() => Assert.Equal("1", await S("SELECT MOD(7, 2)"));
	[Fact] public async Task Arith_Parens() => Assert.Equal("14", await S("SELECT (2 + 5) * 2"));
	[Fact] public async Task Arith_Negative() => Assert.Equal("-5", await S("SELECT -5"));
	[Fact] public async Task Arith_Complex() => Assert.Equal("11", await S("SELECT 3 * 4 - 1"));

	// ---- String expressions ----
	[Fact] public async Task Concat_Op() => Assert.Equal("hello world", await S("SELECT CONCAT('hello', ' ', 'world')"));
	[Fact] public async Task Concat_DoubleBar() => Assert.Equal("ab", await S("SELECT 'a' || 'b'"));
	[Fact] public async Task Repeat_Basic() => Assert.Equal("aaa", await S("SELECT REPEAT('a', 3)"));
	[Fact] public async Task Upper_Basic() => Assert.Equal("HELLO", await S("SELECT UPPER('hello')"));
	[Fact] public async Task Lower_Basic() => Assert.Equal("hello", await S("SELECT LOWER('HELLO')"));
	[Fact] public async Task Length_Basic() => Assert.Equal("5", await S("SELECT LENGTH('hello')"));
	[Fact] public async Task Substr_Basic() => Assert.Equal("ello", await S("SELECT SUBSTR('hello', 2)"));
	[Fact] public async Task Trim_Basic() => Assert.Equal("hello", await S("SELECT TRIM('  hello  ')"));

	// ---- CASE expressions ----
	[Fact] public async Task Case_Simple()
	{
		var rows = await Q("SELECT id, CASE dept WHEN 'Eng' THEN 'Engineering' WHEN 'Sales' THEN 'Sales Dept' ELSE 'Other' END AS full_dept FROM `{ds}.t` ORDER BY id LIMIT 3");
		Assert.Equal("Engineering", rows[0]["full_dept"]?.ToString());
		Assert.Equal("Engineering", rows[1]["full_dept"]?.ToString());
		Assert.Equal("Sales Dept", rows[2]["full_dept"]?.ToString());
	}
	[Fact] public async Task Case_Searched()
	{
		var rows = await Q("SELECT name, CASE WHEN salary >= 80000 THEN 'High' WHEN salary >= 65000 THEN 'Mid' ELSE 'Low' END AS tier FROM `{ds}.t` ORDER BY name LIMIT 3");
		Assert.Equal("High", rows[0]["tier"]?.ToString()); // Alice
		Assert.Equal("Mid", rows[1]["tier"]?.ToString());  // Bob (75k)
		Assert.Equal("Mid", rows[2]["tier"]?.ToString());  // Carol (70k)
	}
	[Fact] public async Task Case_NoElse()
	{
		var v = await S("SELECT CASE WHEN 1 = 2 THEN 'yes' END");
		Assert.Null(v);
	}
	[Fact] public async Task Case_Nested()
	{
		var v = await S("SELECT CASE WHEN true THEN CASE WHEN true THEN 'deep' END END");
		Assert.Equal("deep", v);
	}

	// ---- Nested functions ----
	[Fact] public async Task Nested_TrimUpper() => Assert.Equal("HELLO", await S("SELECT UPPER(TRIM('  hello  '))"));
	[Fact] public async Task Nested_SubstrLen() => Assert.Equal("3", await S("SELECT LENGTH(SUBSTR('hello', 1, 3))"));
	[Fact] public async Task Nested_ConcatRepeat() => Assert.Equal("aaa-bbb", await S("SELECT CONCAT(REPEAT('a',3), '-', REPEAT('b',3))"));
	[Fact] public async Task Nested_AbsSign() => Assert.Equal("1", await S("SELECT ABS(SIGN(-5))"));
	[Fact] public async Task Nested_RoundSqrt()
	{
		var v = await S("SELECT ROUND(SQRT(2), 2)");
		Assert.NotNull(v);
		Assert.StartsWith("1.41", v);
	}

	// ---- Column expressions ----
	[Fact] public async Task ColExpr_SalaryBonus()
	{
		var rows = await Q("SELECT name, salary + 5000 AS with_bonus FROM `{ds}.t` WHERE id = 1");
		Assert.Equal("95000", rows[0]["with_bonus"]?.ToString());
	}
	[Fact] public async Task ColExpr_SalaryPercent()
	{
		var rows = await Q("SELECT name, CAST(salary * 0.1 AS INT64) AS tax FROM `{ds}.t` WHERE id = 1");
		Assert.Equal("9000", rows[0]["tax"]?.ToString());
	}

	// ---- Literal types ----
	[Fact] public async Task Literal_Int() => Assert.Equal("42", await S("SELECT 42"));
	[Fact] public async Task Literal_Negative() => Assert.Equal("-1", await S("SELECT -1"));
	[Fact] public async Task Literal_Float() { var v = await S("SELECT 3.14"); Assert.NotNull(v); Assert.StartsWith("3.14", v); }
	[Fact] public async Task Literal_String() => Assert.Equal("test", await S("SELECT 'test'"));
	[Fact] public async Task Literal_Bool_True() => Assert.Equal("True", await S("SELECT true"));
	[Fact] public async Task Literal_Bool_False() => Assert.Equal("False", await S("SELECT false"));
	[Fact] public async Task Literal_Null() => Assert.Null(await S("SELECT NULL"));

	// ---- SELECT with WHERE ----
	[Fact] public async Task Select_FilteredCount()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.t` WHERE salary > 70000");
		Assert.Equal("4", v); // Alice(90), Bob(75), Grace(85), Hank(72)
	}
	[Fact] public async Task Select_FilteredSum()
	{
		var v = await S("SELECT SUM(salary) FROM `{ds}.t` WHERE dept = 'Eng'");
		Assert.Equal("250000", v);
	}

	// ---- SELECT DISTINCT ----
	[Fact] public async Task Select_Distinct()
	{
		var rows = await Q("SELECT DISTINCT dept FROM `{ds}.t` ORDER BY dept");
		Assert.Equal(3, rows.Count);
	}
	[Fact] public async Task Select_DistinctMulti()
	{
		var rows = await Q("SELECT DISTINCT dept, active FROM `{ds}.t` ORDER BY dept, active");
		Assert.True(rows.Count >= 4);
	}

	// ---- Star ----
	[Fact] public async Task Select_Star()
	{
		var rows = await Q("SELECT * FROM `{ds}.t` ORDER BY id");
		Assert.Equal(8, rows.Count);
		Assert.Equal("1", rows[0]["id"]?.ToString());
	}

	// ---- EXCEPT columns ----
	[Fact] public async Task Select_ExceptColumn()
	{
		var rows = await Q("SELECT * EXCEPT (rating, active) FROM `{ds}.t` WHERE id = 1");
		Assert.Single(rows);
	}

	// ---- Conditional ----
	[Fact] public async Task If_Basic() => Assert.Equal("yes", await S("SELECT IF(1 > 0, 'yes', 'no')"));
	[Fact] public async Task Coalesce_Basic() => Assert.Equal("hello", await S("SELECT COALESCE(NULL, 'hello')"));
	[Fact] public async Task NullIf_Basic() => Assert.Null(await S("SELECT NULLIF(1, 1)"));
	[Fact] public async Task IfNull_Basic() => Assert.Equal("0", await S("SELECT IFNULL(NULL, 0)"));

	// ---- Multiple columns ----
	[Fact] public async Task Multi_ColExpr()
	{
		var rows = await Q("SELECT name, salary, salary * 12 AS annual, ROUND(rating, 0) AS r FROM `{ds}.t` WHERE id = 1");
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("90000", rows[0]["salary"]?.ToString());
		Assert.Equal("1080000", rows[0]["annual"]?.ToString());
	}
}
