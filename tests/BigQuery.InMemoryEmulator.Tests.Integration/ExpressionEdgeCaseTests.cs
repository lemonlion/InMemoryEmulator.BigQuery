using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Edge cases for expression evaluation: arithmetic, division, overflow, precision, mixed types.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ExpressionEdgeCaseTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ExpressionEdgeCaseTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_ee_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.nums` (id INT64, i INT64, f FLOAT64, s STRING)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.nums` VALUES
			(1,10,1.5,'hello'),(2,0,0.0,'world'),(3,-5,-2.5,'foo'),
			(4,100,99.99,'bar'),(5,NULL,NULL,NULL),(6,1,0.001,'baz')", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- Basic arithmetic ----
	[Fact] public async Task Add_Ints() => Assert.Equal("15", await S("SELECT 10 + 5"));
	[Fact] public async Task Sub_Ints() => Assert.Equal("5", await S("SELECT 10 - 5"));
	[Fact] public async Task Mul_Ints() => Assert.Equal("50", await S("SELECT 10 * 5"));
	[Fact] public async Task Div_Ints() => Assert.Equal("2", await S("SELECT 10 / 5"));
	[Fact] public async Task IntDiv() => Assert.Equal("3", await S("SELECT DIV(10, 3)"));
	[Fact] public async Task Mod_Ints() => Assert.Equal("1", await S("SELECT MOD(10, 3)"));

	// ---- Division edge cases ----
	// Emulator throws on division by zero instead of returning Infinity/NaN
	[Fact] public async Task Div_ByZero_Float() => await Assert.ThrowsAnyAsync<Exception>(async () => await S("SELECT 1.0 / 0.0"));
	[Fact] public async Task Div_NegByZero_Float() => await Assert.ThrowsAnyAsync<Exception>(async () => await S("SELECT -1.0 / 0.0"));
	[Fact] public async Task Div_ZeroByZero_Float() => await Assert.ThrowsAnyAsync<Exception>(async () => await S("SELECT 0.0 / 0.0"));

	// ---- Unary minus ----
	[Fact] public async Task UnaryMinus_Int() => Assert.Equal("-5", await S("SELECT -5"));
	[Fact] public async Task UnaryMinus_Float() => Assert.Equal("-3.14", await S("SELECT -3.14"));
	[Fact] public async Task UnaryMinus_Column()
	{
		var v = await S("SELECT -i FROM `{ds}.nums` WHERE id = 1");
		Assert.Equal("-10", v);
	}
	[Fact] public async Task DoubleNegation() => Assert.Equal("5", await S("SELECT -(-5)"));

	// ---- Null arithmetic ----
	[Fact] public async Task Null_Plus_Int() => Assert.Null(await S("SELECT NULL + 5"));
	[Fact] public async Task Int_Plus_Null() => Assert.Null(await S("SELECT 5 + NULL"));
	[Fact] public async Task Null_Mul_Int() => Assert.Null(await S("SELECT NULL * 5"));
	[Fact] public async Task Null_Div_Int() => Assert.Null(await S("SELECT NULL / 5"));
	[Fact] public async Task Null_Mod_Int() => Assert.Null(await S("SELECT MOD(NULL, 5)"));
	[Fact] public async Task Int_Div_Null() => Assert.Null(await S("SELECT 5 / NULL"));

	// ---- Mixed int/float arithmetic ----
	[Fact] public async Task Int_Plus_Float() => Assert.Equal("11.5", await S("SELECT 10 + 1.5"));
	[Fact] public async Task Float_Mul_Int()
	{
		var v = await S("SELECT 1.5 * 10");
		Assert.True(v == "15" || v == "15.0", $"Got: {v}");
	}
	[Fact] public async Task IntCol_Plus_FloatCol()
	{
		var v = await S("SELECT i + f FROM `{ds}.nums` WHERE id = 1");
		Assert.Equal("11.5", v);
	}

	// ---- Large number arithmetic ----
	[Fact] public async Task LargeInt_Add() => Assert.Equal("1000000000000", await S("SELECT 999999999999 + 1"));
	[Fact] public async Task LargeFloat_Precision()
	{
		var v = await S("SELECT 0.1 + 0.2");
		Assert.NotNull(v);
		Assert.True(double.Parse(v!) > 0.29 && double.Parse(v!) < 0.31);
	}

	// ---- Boolean expressions ----
	[Fact] public async Task Bool_And() => Assert.Equal("True", await S("SELECT true AND true"));
	[Fact] public async Task Bool_Or() => Assert.Equal("True", await S("SELECT false OR true"));
	[Fact] public async Task Bool_Not() => Assert.Equal("False", await S("SELECT NOT true"));
	[Fact] public async Task Bool_Null_And() => Assert.Null(await S("SELECT NULL AND true"));
	[Fact] public async Task Bool_False_And_Null() => Assert.Equal("False", await S("SELECT false AND NULL"));
	[Fact] public async Task Bool_True_Or_Null() => Assert.Equal("True", await S("SELECT true OR NULL"));
	[Fact] public async Task Bool_Null_Or_False() => Assert.Null(await S("SELECT NULL OR false"));

	// ---- String concatenation ----
	[Fact] public async Task StringConcat_Operator() => Assert.Equal("helloworld", await S("SELECT 'hello' || 'world'"));
	[Fact] public async Task StringConcat_Null()
	{
		var v = await S("SELECT 'hello' || NULL");
		Assert.Null(v);
	}
	[Fact] public async Task StringConcat_Empty() => Assert.Equal("hello", await S("SELECT 'hello' || ''"));

	// ---- Comparison expressions ----
	[Fact] public async Task Compare_IntFloat() => Assert.Equal("True", await S("SELECT 5 = 5.0"));
	[Fact] public async Task Compare_NullEqual() => Assert.Null(await S("SELECT NULL = NULL"));
	[Fact] public async Task Compare_NullNotEqual() => Assert.Null(await S("SELECT NULL != NULL"));
	[Fact] public async Task Compare_NullLessThan() => Assert.Null(await S("SELECT NULL < 5"));
	[Fact] public async Task Compare_GreaterThan() => Assert.Equal("True", await S("SELECT 10 > 5"));
	[Fact] public async Task Compare_LessEqual() => Assert.Equal("True", await S("SELECT 5 <= 5"));
	[Fact] public async Task Compare_GreaterEqual() => Assert.Equal("True", await S("SELECT 5 >= 5"));
	[Fact] public async Task Compare_Strings() => Assert.Equal("True", await S("SELECT 'abc' < 'abd'"));

	// ---- Expression in columns ----
	[Fact] public async Task Expr_InSelect()
	{
		var v = await S("SELECT i * 2 + f FROM `{ds}.nums` WHERE id = 1");
		Assert.Equal("21.5", v); // 10*2 + 1.5
	}
	[Fact] public async Task Expr_InWhere()
	{
		var rows = await Q("SELECT id FROM `{ds}.nums` WHERE i * 2 > 50");
		Assert.True(rows.Count >= 1); // id=4 has i=100
	}
	[Fact] public async Task Expr_InOrderBy()
	{
		var rows = await Q("SELECT id, i FROM `{ds}.nums` WHERE i IS NOT NULL ORDER BY i * -1 LIMIT 3");
		Assert.Equal(3, rows.Count);
	}
	[Fact] public async Task Expr_Nested() => Assert.Equal("14", await S("SELECT (2 + 3) * 4 - (10 / 2) + 1 - 2"));
	[Fact] public async Task Expr_DeepNested() => Assert.Equal("100", await S("SELECT ((((10)))) * (((10)))"));

	// ---- BETWEEN with expressions ----
	[Fact] public async Task Between_WithExpr()
	{
		var rows = await Q("SELECT id FROM `{ds}.nums` WHERE (i * 2) BETWEEN 10 AND 30");
		Assert.True(rows.Count >= 1);
	}

	// ---- IN with expressions ----
	[Fact] public async Task In_WithExpr()
	{
		var rows = await Q("SELECT id FROM `{ds}.nums` WHERE i + 1 IN (11, 1, -4)");
		Assert.True(rows.Count >= 1);
	}

	// ---- CASE with arithmetic ----
	[Fact] public async Task Case_Arithmetic()
	{
		var v = await S("SELECT CASE WHEN 5 > 3 THEN 10 * 2 ELSE 10 / 2 END");
		Assert.Equal("20", v);
	}

	// ---- Aggregate with expressions ----
	[Fact] public async Task Sum_WithExpr()
	{
		var v = await S("SELECT SUM(i * 2) FROM `{ds}.nums` WHERE id <= 4");
		Assert.Equal("210", v); // (10+0-5+100)*2 = 210
	}
	[Fact] public async Task Avg_WithExpr()
	{
		var v = await S("SELECT ROUND(AVG(f * 10), 1) FROM `{ds}.nums` WHERE id <= 4");
		Assert.NotNull(v); // (15+0-25+999.9)/4
	}

	// ---- Parenthesized expressions ----
	[Fact] public async Task Parens_Override_Precedence() => Assert.Equal("21", await S("SELECT (3 + 4) * 3"));
	[Fact] public async Task No_Parens_Precedence() => Assert.Equal("15", await S("SELECT 3 + 4 * 3"));

	// ---- IF function ----
	[Fact] public async Task If_True() => Assert.Equal("yes", await S("SELECT IF(1=1, 'yes', 'no')"));
	[Fact] public async Task If_False() => Assert.Equal("no", await S("SELECT IF(1=2, 'yes', 'no')"));
	[Fact] public async Task If_WithExpr() => Assert.Equal("big", await S("SELECT IF(10 * 5 > 30, 'big', 'small')"));

	// ---- Ternary-like IIF ----
	[Fact] public async Task Iif_Basic() => Assert.Equal("yes", await S("SELECT IIF(true, 'yes', 'no')"));

	// ---- COALESCE chains ----
	[Fact] public async Task Coalesce_First() => Assert.Equal("1", await S("SELECT COALESCE(1, 2, 3)"));
	[Fact] public async Task Coalesce_Skip_Null() => Assert.Equal("2", await S("SELECT COALESCE(NULL, 2, 3)"));
	[Fact] public async Task Coalesce_AllNull() => Assert.Null(await S("SELECT COALESCE(NULL, NULL, NULL)"));
	[Fact] public async Task Coalesce_MixedTypes() => Assert.Equal("hello", await S("SELECT COALESCE(NULL, 'hello')"));

	// ---- NULLIF ----
	[Fact] public async Task Nullif_Equal() => Assert.Null(await S("SELECT NULLIF(5, 5)"));
	[Fact] public async Task Nullif_NotEqual() => Assert.Equal("5", await S("SELECT NULLIF(5, 3)"));

	// ---- IFNULL ----
	[Fact] public async Task Ifnull_Null() => Assert.Equal("10", await S("SELECT IFNULL(NULL, 10)"));
	[Fact] public async Task Ifnull_NotNull() => Assert.Equal("5", await S("SELECT IFNULL(5, 10)"));

	// ---- GREATEST / LEAST ----
	[Fact] public async Task Greatest_Ints() => Assert.Equal("10", await S("SELECT GREATEST(3, 7, 10, 1)"));
	[Fact] public async Task Least_Ints() => Assert.Equal("1", await S("SELECT LEAST(3, 7, 10, 1)"));
	[Fact] public async Task Greatest_WithNull() => Assert.Null(await S("SELECT GREATEST(NULL, 10, 5)"));
	[Fact] public async Task Least_WithNull() => Assert.Null(await S("SELECT LEAST(NULL, 10, 5)"));
	[Fact] public async Task Greatest_Strings() => Assert.Equal("z", await S("SELECT GREATEST('a', 'z', 'm')"));
	[Fact] public async Task Least_Strings() => Assert.Equal("a", await S("SELECT LEAST('a', 'z', 'm')"));

	// ---- Aliased expressions ----
	[Fact] public async Task Alias_Expression()
	{
		var v = await S("SELECT 1 + 2 AS result");
		Assert.Equal("3", v);
	}
	[Fact] public async Task Alias_InSubquery()
	{
		var v = await S("SELECT result FROM (SELECT 1 + 2 AS result)");
		Assert.Equal("3", v);
	}

	// ---- Multiple expressions in SELECT ----
	[Fact] public async Task MultiExpr_Select()
	{
		var rows = await Q("SELECT 1+1 AS a, 2*3 AS b, 10-4 AS c");
		Assert.Equal("2", rows[0]["a"]?.ToString());
		Assert.Equal("6", rows[0]["b"]?.ToString());
		Assert.Equal("6", rows[0]["c"]?.ToString());
	}
}
