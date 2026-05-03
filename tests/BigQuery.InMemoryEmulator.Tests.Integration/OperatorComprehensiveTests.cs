using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Comprehensive tests for arithmetic, comparison, and logical operators.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class OperatorComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public OperatorComprehensiveTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- Addition ----
	[Fact] public async Task Add_Ints() => Assert.Equal("5", await Scalar("SELECT 2 + 3"));
	[Fact] public async Task Add_Floats() { var v = double.Parse(await Scalar("SELECT 1.5 + 2.5") ?? "0"); Assert.Equal(4.0, v); }
	[Fact] public async Task Add_NegativeInts() => Assert.Equal("-1", await Scalar("SELECT -3 + 2"));
	[Fact] public async Task Add_Zero() => Assert.Equal("5", await Scalar("SELECT 5 + 0"));
	[Fact] public async Task Add_LargeInts() => Assert.Equal("2000000", await Scalar("SELECT 1000000 + 1000000"));
	[Fact] public async Task Add_IntFloat() { var v = double.Parse(await Scalar("SELECT 1 + 2.5") ?? "0"); Assert.Equal(3.5, v); }

	// ---- Subtraction ----
	[Fact] public async Task Sub_Ints() => Assert.Equal("2", await Scalar("SELECT 5 - 3"));
	[Fact] public async Task Sub_Negative() => Assert.Equal("-3", await Scalar("SELECT 2 - 5"));
	[Fact] public async Task Sub_Zero() => Assert.Equal("5", await Scalar("SELECT 5 - 0"));
	[Fact] public async Task Sub_Self() => Assert.Equal("0", await Scalar("SELECT 42 - 42"));
	[Fact] public async Task Sub_Floats() { var v = double.Parse(await Scalar("SELECT 5.5 - 2.0") ?? "0"); Assert.Equal(3.5, v); }

	// ---- Multiplication ----
	[Fact] public async Task Mul_Ints() => Assert.Equal("15", await Scalar("SELECT 3 * 5"));
	[Fact] public async Task Mul_Zero() => Assert.Equal("0", await Scalar("SELECT 42 * 0"));
	[Fact] public async Task Mul_One() => Assert.Equal("42", await Scalar("SELECT 42 * 1"));
	[Fact] public async Task Mul_Negative() => Assert.Equal("-15", await Scalar("SELECT -3 * 5"));
	[Fact] public async Task Mul_BothNeg() => Assert.Equal("15", await Scalar("SELECT -3 * -5"));
	[Fact] public async Task Mul_Floats() { var v = double.Parse(await Scalar("SELECT 2.5 * 4.0") ?? "0"); Assert.Equal(10.0, v); }

	// ---- Division ----
	[Fact] public async Task Div_Exact() { var v = double.Parse(await Scalar("SELECT 10.0 / 2") ?? "0"); Assert.Equal(5.0, v); }
	[Fact] public async Task Div_Remainder() { var v = double.Parse(await Scalar("SELECT 7.0 / 2") ?? "0"); Assert.Equal(3.5, v); }
	[Fact] public async Task Div_IntegerDiv() => Assert.Equal("3", await Scalar("SELECT DIV(10, 3)"));
	[Fact] public async Task Div_One() { var v = double.Parse(await Scalar("SELECT 42.0 / 1") ?? "0"); Assert.Equal(42.0, v); }

	// ---- Modulo ----
	[Fact] public async Task Mod_Basic() => Assert.Equal("1", await Scalar("SELECT MOD(10, 3)"));
	[Fact] public async Task Mod_Even() => Assert.Equal("0", await Scalar("SELECT MOD(10, 5)"));
	[Fact] public async Task Mod_One() => Assert.Equal("0", await Scalar("SELECT MOD(10, 1)"));
	[Fact] public async Task Mod_NegDividend() => Assert.Equal("-1", await Scalar("SELECT MOD(-10, 3)"));

	// ---- Unary minus ----
	[Fact] public async Task UnaryMinus_Pos() => Assert.Equal("-5", await Scalar("SELECT -5"));
	[Fact] public async Task UnaryMinus_Neg() => Assert.Equal("5", await Scalar("SELECT -(-5)"));
	[Fact] public async Task UnaryMinus_Zero() => Assert.Equal("0", await Scalar("SELECT -0"));

	// ---- Comparison operators ----
	[Fact] public async Task Eq_True() => Assert.Equal("True", await Scalar("SELECT 1 = 1"));
	[Fact] public async Task Eq_False() => Assert.Equal("False", await Scalar("SELECT 1 = 2"));
	[Fact] public async Task Neq_True() => Assert.Equal("True", await Scalar("SELECT 1 != 2"));
	[Fact] public async Task Neq_False() => Assert.Equal("False", await Scalar("SELECT 1 != 1"));
	[Fact] public async Task Neq_Alt_True() => Assert.Equal("True", await Scalar("SELECT 1 <> 2"));
	[Fact] public async Task Neq_Alt_False() => Assert.Equal("False", await Scalar("SELECT 1 <> 1"));
	[Fact] public async Task Lt_True() => Assert.Equal("True", await Scalar("SELECT 1 < 2"));
	[Fact] public async Task Lt_False() => Assert.Equal("False", await Scalar("SELECT 2 < 1"));
	[Fact] public async Task Lt_Equal() => Assert.Equal("False", await Scalar("SELECT 1 < 1"));
	[Fact] public async Task Lte_True() => Assert.Equal("True", await Scalar("SELECT 1 <= 2"));
	[Fact] public async Task Lte_Equal() => Assert.Equal("True", await Scalar("SELECT 1 <= 1"));
	[Fact] public async Task Lte_False() => Assert.Equal("False", await Scalar("SELECT 2 <= 1"));
	[Fact] public async Task Gt_True() => Assert.Equal("True", await Scalar("SELECT 2 > 1"));
	[Fact] public async Task Gt_False() => Assert.Equal("False", await Scalar("SELECT 1 > 2"));
	[Fact] public async Task Gt_Equal() => Assert.Equal("False", await Scalar("SELECT 1 > 1"));
	[Fact] public async Task Gte_True() => Assert.Equal("True", await Scalar("SELECT 2 >= 1"));
	[Fact] public async Task Gte_Equal() => Assert.Equal("True", await Scalar("SELECT 1 >= 1"));
	[Fact] public async Task Gte_False() => Assert.Equal("False", await Scalar("SELECT 1 >= 2"));

	// ---- String comparison ----
	[Fact] public async Task StrEq_True() => Assert.Equal("True", await Scalar("SELECT 'abc' = 'abc'"));
	[Fact] public async Task StrEq_False() => Assert.Equal("False", await Scalar("SELECT 'abc' = 'def'"));
	[Fact] public async Task StrLt_True() => Assert.Equal("True", await Scalar("SELECT 'abc' < 'def'"));
	[Fact] public async Task StrLt_False() => Assert.Equal("False", await Scalar("SELECT 'def' < 'abc'"));
	[Fact] public async Task StrGt_True() => Assert.Equal("True", await Scalar("SELECT 'def' > 'abc'"));

	// ---- Logical operators ----
	[Fact] public async Task And_TrueTrue() => Assert.Equal("True", await Scalar("SELECT TRUE AND TRUE"));
	[Fact] public async Task And_TrueFalse() => Assert.Equal("False", await Scalar("SELECT TRUE AND FALSE"));
	[Fact] public async Task And_FalseTrue() => Assert.Equal("False", await Scalar("SELECT FALSE AND TRUE"));
	[Fact] public async Task And_FalseFalse() => Assert.Equal("False", await Scalar("SELECT FALSE AND FALSE"));
	[Fact] public async Task Or_TrueTrue() => Assert.Equal("True", await Scalar("SELECT TRUE OR TRUE"));
	[Fact] public async Task Or_TrueFalse() => Assert.Equal("True", await Scalar("SELECT TRUE OR FALSE"));
	[Fact] public async Task Or_FalseTrue() => Assert.Equal("True", await Scalar("SELECT FALSE OR TRUE"));
	[Fact] public async Task Or_FalseFalse() => Assert.Equal("False", await Scalar("SELECT FALSE OR FALSE"));
	[Fact] public async Task Not_True() => Assert.Equal("False", await Scalar("SELECT NOT TRUE"));
	[Fact] public async Task Not_False() => Assert.Equal("True", await Scalar("SELECT NOT FALSE"));

	// ---- BETWEEN ----
	[Fact] public async Task Between_InRange() => Assert.Equal("True", await Scalar("SELECT 5 BETWEEN 1 AND 10"));
	[Fact] public async Task Between_AtLow() => Assert.Equal("True", await Scalar("SELECT 1 BETWEEN 1 AND 10"));
	[Fact] public async Task Between_AtHigh() => Assert.Equal("True", await Scalar("SELECT 10 BETWEEN 1 AND 10"));
	[Fact] public async Task Between_Below() => Assert.Equal("False", await Scalar("SELECT 0 BETWEEN 1 AND 10"));
	[Fact] public async Task Between_Above() => Assert.Equal("False", await Scalar("SELECT 11 BETWEEN 1 AND 10"));
	[Fact] public async Task Between_String() => Assert.Equal("True", await Scalar("SELECT 'c' BETWEEN 'a' AND 'e'"));

	// ---- IN ----
	[Fact] public async Task In_Found() => Assert.Equal("True", await Scalar("SELECT 2 IN (1, 2, 3)"));
	[Fact] public async Task In_NotFound() => Assert.Equal("False", await Scalar("SELECT 4 IN (1, 2, 3)"));
	[Fact] public async Task In_Single() => Assert.Equal("True", await Scalar("SELECT 1 IN (1)"));
	[Fact] public async Task In_String() => Assert.Equal("True", await Scalar("SELECT 'b' IN ('a', 'b', 'c')"));
	[Fact] public async Task In_StringNotFound() => Assert.Equal("False", await Scalar("SELECT 'd' IN ('a', 'b', 'c')"));

	// ---- IS TRUE / IS FALSE / IS NULL ----
	[Fact] public async Task IsTrue_True() => Assert.Equal("True", await Scalar("SELECT TRUE IS TRUE"));
	[Fact] public async Task IsTrue_False() => Assert.Equal("False", await Scalar("SELECT FALSE IS TRUE"));
	[Fact] public async Task IsFalse_True() => Assert.Equal("True", await Scalar("SELECT FALSE IS FALSE"));
	[Fact] public async Task IsFalse_False() => Assert.Equal("False", await Scalar("SELECT TRUE IS FALSE"));

	// ---- LIKE ----
	[Fact] public async Task Like_Wildcard() => Assert.Equal("True", await Scalar("SELECT 'hello' LIKE '%ello'"));
	[Fact] public async Task Like_FullWild() => Assert.Equal("True", await Scalar("SELECT 'hello' LIKE '%'"));
	[Fact] public async Task Like_StartWild() => Assert.Equal("True", await Scalar("SELECT 'hello' LIKE 'hel%'"));
	[Fact] public async Task Like_MidWild() => Assert.Equal("True", await Scalar("SELECT 'hello' LIKE 'h%o'"));
	[Fact] public async Task Like_SingleChar() => Assert.Equal("True", await Scalar("SELECT 'hello' LIKE 'hell_'"));
	[Fact] public async Task Like_NoMatch() => Assert.Equal("False", await Scalar("SELECT 'hello' LIKE 'world'"));
	[Fact] public async Task Like_Exact() => Assert.Equal("True", await Scalar("SELECT 'hello' LIKE 'hello'"));
	[Fact] public async Task Like_CaseSensitive() => Assert.Equal("False", await Scalar("SELECT 'Hello' LIKE 'hello'"));
	[Fact] public async Task NotLike_True() => Assert.Equal("True", await Scalar("SELECT 'hello' NOT LIKE 'world'"));
	[Fact] public async Task NotLike_False() => Assert.Equal("False", await Scalar("SELECT 'hello' NOT LIKE 'hello'"));

	// ---- COALESCE / IFNULL / NULLIF / IF ----
	[Fact] public async Task Coalesce_First() => Assert.Equal("1", await Scalar("SELECT COALESCE(1, 2, 3)"));
	[Fact] public async Task Coalesce_SkipNull() => Assert.Equal("2", await Scalar("SELECT COALESCE(NULL, 2, 3)"));
	[Fact] public async Task Coalesce_AllNull() => Assert.Null(await Scalar("SELECT COALESCE(NULL, NULL)"));
	[Fact] public async Task Ifnull_NotNull() => Assert.Equal("5", await Scalar("SELECT IFNULL(5, 10)"));
	[Fact] public async Task Ifnull_Null() => Assert.Equal("10", await Scalar("SELECT IFNULL(NULL, 10)"));
	[Fact] public async Task Nullif_Eq() => Assert.Null(await Scalar("SELECT NULLIF(5, 5)"));
	[Fact] public async Task Nullif_Neq() => Assert.Equal("5", await Scalar("SELECT NULLIF(5, 10)"));

	// ---- CASE expression ----
	[Fact] public async Task Case_When_True() => Assert.Equal("yes", await Scalar("SELECT CASE WHEN 1=1 THEN 'yes' ELSE 'no' END"));
	[Fact] public async Task Case_When_False() => Assert.Equal("no", await Scalar("SELECT CASE WHEN 1=2 THEN 'yes' ELSE 'no' END"));
	[Fact] public async Task Case_MultiWhen() => Assert.Equal("two", await Scalar("SELECT CASE WHEN 1=2 THEN 'one' WHEN 2=2 THEN 'two' ELSE 'none' END"));
	[Fact] public async Task Case_NoMatch_Else() => Assert.Equal("none", await Scalar("SELECT CASE WHEN 1=2 THEN 'one' WHEN 2=3 THEN 'two' ELSE 'none' END"));
	[Fact] public async Task Case_NoElse_Null() => Assert.Null(await Scalar("SELECT CASE WHEN 1=2 THEN 'yes' END"));

	// ---- GREATEST / LEAST ----
	[Fact] public async Task Greatest_Three() => Assert.Equal("5", await Scalar("SELECT GREATEST(1, 5, 3)"));
	[Fact] public async Task Greatest_One() => Assert.Equal("5", await Scalar("SELECT GREATEST(5)"));
	[Fact] public async Task Greatest_Negative() => Assert.Equal("-1", await Scalar("SELECT GREATEST(-5, -1, -3)"));
	[Fact] public async Task Greatest_Strings() => Assert.Equal("c", await Scalar("SELECT GREATEST('a','c','b')"));
	[Fact] public async Task Least_Three() => Assert.Equal("1", await Scalar("SELECT LEAST(5, 1, 3)"));
	[Fact] public async Task Least_One() => Assert.Equal("5", await Scalar("SELECT LEAST(5)"));
	[Fact] public async Task Least_Negative() => Assert.Equal("-5", await Scalar("SELECT LEAST(-5, -1, -3)"));
	[Fact] public async Task Least_Strings() => Assert.Equal("a", await Scalar("SELECT LEAST('c','a','b')"));

	// ---- Operator precedence ----
	[Fact] public async Task Precedence_MulOverAdd() => Assert.Equal("7", await Scalar("SELECT 1 + 2 * 3"));
	[Fact] public async Task Precedence_ParensOverride() => Assert.Equal("9", await Scalar("SELECT (1 + 2) * 3"));
	[Fact] public async Task Precedence_DivOverSub() => Assert.Equal("3", await Scalar("SELECT 5 - 4 / 2"));
	[Fact] public async Task Precedence_NestedParens() => Assert.Equal("20", await Scalar("SELECT (2 + 3) * (1 + 3)"));
	[Fact] public async Task Precedence_UnaryInExpr() => Assert.Equal("-3", await Scalar("SELECT -1 - 2"));

	// ---- Expression in CASE ----
	[Fact] public async Task CaseExpr_Arithmetic() => Assert.Equal("a", await Scalar("SELECT CASE WHEN 2+3 > 4 THEN 'a' ELSE 'b' END"));
	[Fact] public async Task CaseExpr_StringCompare() => Assert.Equal("later", await Scalar("SELECT CASE WHEN 'b' > 'a' THEN 'later' ELSE 'earlier' END"));
	[Fact] public async Task CaseExpr_Nested() => Assert.Equal("inner", await Scalar("SELECT CASE WHEN TRUE THEN CASE WHEN TRUE THEN 'inner' END END"));
}
