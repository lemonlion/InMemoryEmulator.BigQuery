using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for SELECT clause features: aliases, expressions, star, EXCEPT, REPLACE, computed columns.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_list
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class SelectClauseAdvancedTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public SelectClauseAdvancedTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_sc_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.t` (id INT64, name STRING, val FLOAT64, active BOOL)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.t` VALUES
			(1,'Alice',10.5,true),(2,'Bob',20.3,false),(3,'Carol',30.7,true),
			(4,'Dave',NULL,true),(5,'Eve',50.1,false)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- SELECT * ----
	[Fact] public async Task Star_AllColumns()
	{
		var rows = await Q("SELECT * FROM `{ds}.t` ORDER BY id");
		Assert.Equal(5, rows.Count);
	}

	// ---- SELECT with alias ----
	[Fact] public async Task Alias_Column()
	{
		var rows = await Q("SELECT name AS employee_name FROM `{ds}.t` ORDER BY id LIMIT 1");
		Assert.Equal("Alice", rows[0]["employee_name"]?.ToString());
	}
	[Fact] public async Task Alias_Expression()
	{
		var rows = await Q("SELECT val * 2 AS doubled FROM `{ds}.t` WHERE id = 1");
		Assert.Equal("21", rows[0]["doubled"]?.ToString());
	}
	[Fact] public async Task Alias_NoAS()
	{
		var rows = await Q("SELECT name employee_name FROM `{ds}.t` ORDER BY id LIMIT 1");
		Assert.Equal("Alice", rows[0]["employee_name"]?.ToString());
	}

	// ---- SELECT DISTINCT ----
	[Fact] public async Task Distinct_Basic()
	{
		var rows = await Q("SELECT DISTINCT active FROM `{ds}.t` ORDER BY active");
		Assert.Equal(2, rows.Count);
	}
	[Fact] public async Task Distinct_MultiColumn()
	{
		var rows = await Q("SELECT DISTINCT active, CASE WHEN val > 20 THEN 'high' ELSE 'low' END AS tier FROM `{ds}.t` ORDER BY active, tier");
		Assert.True(rows.Count >= 2);
	}

	// ---- SELECT with expressions ----
	[Fact] public async Task Expr_Arithmetic()
	{
		var rows = await Q("SELECT id, val + 100 AS val_plus FROM `{ds}.t` WHERE id = 1");
		Assert.Equal("110.5", rows[0]["val_plus"]?.ToString());
	}
	[Fact] public async Task Expr_Concat()
	{
		var rows = await Q("SELECT CONCAT('Employee: ', name) AS label FROM `{ds}.t` ORDER BY id LIMIT 1");
		Assert.Equal("Employee: Alice", rows[0]["label"]?.ToString());
	}
	[Fact] public async Task Expr_Case()
	{
		var rows = await Q("SELECT name, CASE WHEN active THEN 'Active' ELSE 'Inactive' END AS status FROM `{ds}.t` ORDER BY id");
		Assert.Equal("Active", rows[0]["status"]?.ToString());
		Assert.Equal("Inactive", rows[1]["status"]?.ToString());
	}

	// ---- SELECT with subquery ----
	[Fact] public async Task Subquery_Scalar()
	{
		var rows = await Q("SELECT name, (SELECT MAX(val) FROM `{ds}.t`) AS max_val FROM `{ds}.t` ORDER BY id LIMIT 1");
		Assert.Equal("50.1", rows[0]["max_val"]?.ToString());
	}

	// ---- SELECT with EXCEPT ----
	[Fact] public async Task Except_Basic()
	{
		var rows = await Q("SELECT * EXCEPT (active) FROM `{ds}.t` ORDER BY id LIMIT 1");
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}

	// ---- SELECT with REPLACE ----
	[Fact] public async Task Replace_Basic()
	{
		var rows = await Q("SELECT * REPLACE (UPPER(name) AS name) FROM `{ds}.t` ORDER BY id LIMIT 1");
		Assert.Equal("ALICE", rows[0]["name"]?.ToString());
	}

	// ---- SELECT literal values ----
	[Fact] public async Task Literal_Int() => Assert.Equal("42", await S("SELECT 42"));
	[Fact] public async Task Literal_Float() => Assert.Equal("3.14", await S("SELECT 3.14"));
	[Fact] public async Task Literal_String() => Assert.Equal("hello", await S("SELECT 'hello'"));
	[Fact] public async Task Literal_Bool() => Assert.Equal("True", await S("SELECT true"));
	[Fact] public async Task Literal_Null() => Assert.Null(await S("SELECT NULL"));

	// ---- SELECT multiple columns ----
	[Fact] public async Task MultiColumn_Basic()
	{
		var rows = await Q("SELECT id, name, val FROM `{ds}.t` ORDER BY id LIMIT 1");
		Assert.Equal("1", rows[0]["id"]?.ToString());
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("10.5", rows[0]["val"]?.ToString());
	}

	// ---- SELECT with ORDER BY computed expression ----
	[Fact] public async Task OrderBy_Expression()
	{
		var rows = await Q("SELECT name, val FROM `{ds}.t` WHERE val IS NOT NULL ORDER BY val * -1 LIMIT 1");
		Assert.Equal("Eve", rows[0]["name"]?.ToString());
	}

	// ---- SELECT with LIMIT and OFFSET ----
	[Fact] public async Task Limit_Basic()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY id LIMIT 2");
		Assert.Equal(2, rows.Count);
	}
	[Fact] public async Task LimitOffset_Basic()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY id LIMIT 2 OFFSET 1");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Bob", rows[0]["name"]?.ToString());
	}

	// ---- SELECT from values ----
	[Fact] public async Task FromValues()
	{
		var rows = await Q("SELECT x FROM UNNEST([1,2,3]) AS x ORDER BY x");
		Assert.Equal(3, rows.Count);
	}

	// ---- SELECT nested functions ----
	[Fact] public async Task NestedFunctions()
	{
		var v = await S("SELECT UPPER(TRIM(CONCAT(' hello', ' world ')))");
		Assert.Equal("HELLO WORLD", v);
	}

	// ---- Boolean expressions in SELECT ----
	[Fact] public async Task BoolExpr_Comparison() => Assert.Equal("True", await S("SELECT 5 > 3"));
	[Fact] public async Task BoolExpr_And() => Assert.Equal("True", await S("SELECT true AND true"));
	[Fact] public async Task BoolExpr_Or() => Assert.Equal("True", await S("SELECT false OR true"));
	[Fact] public async Task BoolExpr_Not() => Assert.Equal("True", await S("SELECT NOT false"));
	[Fact] public async Task BoolExpr_Complex() => Assert.Equal("True", await S("SELECT (1 < 2) AND (3 > 2) OR false"));

	// ---- Mathematical expressions ----
	[Fact] public async Task MathExpr_Addition() => Assert.Equal("15", await S("SELECT 10 + 5"));
	[Fact] public async Task MathExpr_Subtraction() => Assert.Equal("5", await S("SELECT 10 - 5"));
	[Fact] public async Task MathExpr_Multiplication() => Assert.Equal("50", await S("SELECT 10 * 5"));
	[Fact] public async Task MathExpr_Division() => Assert.Equal("2", await S("SELECT 10 / 5"));
	[Fact] public async Task MathExpr_Parentheses() => Assert.Equal("30", await S("SELECT (10 + 5) * 2"));
	[Fact] public async Task MathExpr_Nested() => Assert.Equal("100", await S("SELECT (2 + 3) * (10 + 10)"));
}
