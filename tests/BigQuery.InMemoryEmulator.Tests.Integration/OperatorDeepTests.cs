using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for comparison, logical, and conditional operators.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class OperatorDeepTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public OperatorDeepTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- Comparison: = ----
	[Fact] public async Task Eq_IntTrue() => Assert.Equal("True", await Scalar("SELECT 1 = 1"));
	[Fact] public async Task Eq_IntFalse() => Assert.Equal("False", await Scalar("SELECT 1 = 2"));
	[Fact] public async Task Eq_StrTrue() => Assert.Equal("True", await Scalar("SELECT 'a' = 'a'"));
	[Fact] public async Task Eq_StrFalse() => Assert.Equal("False", await Scalar("SELECT 'a' = 'b'"));
	[Fact] public async Task Eq_FloatTrue() => Assert.Equal("True", await Scalar("SELECT 1.5 = 1.5"));
	[Fact] public async Task Eq_FloatFalse() => Assert.Equal("False", await Scalar("SELECT 1.5 = 2.5"));
	[Fact] public async Task Eq_BoolTrue() => Assert.Equal("True", await Scalar("SELECT TRUE = TRUE"));
	[Fact] public async Task Eq_BoolFalse() => Assert.Equal("False", await Scalar("SELECT TRUE = FALSE"));

	// ---- Comparison: != / <> ----
	[Fact] public async Task Neq_IntTrue() => Assert.Equal("True", await Scalar("SELECT 1 != 2"));
	[Fact] public async Task Neq_IntFalse() => Assert.Equal("False", await Scalar("SELECT 1 != 1"));
	[Fact] public async Task Neq_Diamond_True() => Assert.Equal("True", await Scalar("SELECT 1 <> 2"));
	[Fact] public async Task Neq_Diamond_False() => Assert.Equal("False", await Scalar("SELECT 1 <> 1"));
	[Fact] public async Task Neq_StrTrue() => Assert.Equal("True", await Scalar("SELECT 'a' != 'b'"));
	[Fact] public async Task Neq_StrFalse() => Assert.Equal("False", await Scalar("SELECT 'a' != 'a'"));

	// ---- Comparison: <, >, <=, >= ----
	[Fact] public async Task Lt_True() => Assert.Equal("True", await Scalar("SELECT 1 < 2"));
	[Fact] public async Task Lt_False() => Assert.Equal("False", await Scalar("SELECT 2 < 1"));
	[Fact] public async Task Lt_Equal2() => Assert.Equal("False", await Scalar("SELECT 1 < 1"));
	[Fact] public async Task Gt_True() => Assert.Equal("True", await Scalar("SELECT 2 > 1"));
	[Fact] public async Task Gt_False() => Assert.Equal("False", await Scalar("SELECT 1 > 2"));
	[Fact] public async Task Gt_Equal2() => Assert.Equal("False", await Scalar("SELECT 1 > 1"));
	[Fact] public async Task Lte_True2() => Assert.Equal("True", await Scalar("SELECT 1 <= 2"));
	[Fact] public async Task Lte_Equal() => Assert.Equal("True", await Scalar("SELECT 1 <= 1"));
	[Fact] public async Task Lte_False() => Assert.Equal("False", await Scalar("SELECT 2 <= 1"));
	[Fact] public async Task Gte_True2() => Assert.Equal("True", await Scalar("SELECT 2 >= 1"));
	[Fact] public async Task Gte_Equal() => Assert.Equal("True", await Scalar("SELECT 1 >= 1"));
	[Fact] public async Task Gte_False() => Assert.Equal("False", await Scalar("SELECT 1 >= 2"));

	// ---- String comparison ----
	[Fact] public async Task StrLt_True() => Assert.Equal("True", await Scalar("SELECT 'abc' < 'abd'"));
	[Fact] public async Task StrLt_False() => Assert.Equal("False", await Scalar("SELECT 'abd' < 'abc'"));
	[Fact] public async Task StrGt_True() => Assert.Equal("True", await Scalar("SELECT 'b' > 'a'"));
	[Fact] public async Task StrGt_False() => Assert.Equal("False", await Scalar("SELECT 'a' > 'b'"));
	[Fact] public async Task StrLte_True() => Assert.Equal("True", await Scalar("SELECT 'a' <= 'a'"));
	[Fact] public async Task StrGte_True() => Assert.Equal("True", await Scalar("SELECT 'b' >= 'a'"));

	// ---- BETWEEN ----
	[Fact] public async Task Between_InRange() => Assert.Equal("True", await Scalar("SELECT 5 BETWEEN 1 AND 10"));
	[Fact] public async Task Between_Low() => Assert.Equal("True", await Scalar("SELECT 1 BETWEEN 1 AND 10"));
	[Fact] public async Task Between_High() => Assert.Equal("True", await Scalar("SELECT 10 BETWEEN 1 AND 10"));
	[Fact] public async Task Between_Below() => Assert.Equal("False", await Scalar("SELECT 0 BETWEEN 1 AND 10"));
	[Fact] public async Task Between_Above() => Assert.Equal("False", await Scalar("SELECT 11 BETWEEN 1 AND 10"));
	[Fact] public async Task NotBetween_Below() => Assert.Equal("True", await Scalar("SELECT 0 NOT BETWEEN 1 AND 10"));
	[Fact] public async Task NotBetween_InRange() => Assert.Equal("False", await Scalar("SELECT 5 NOT BETWEEN 1 AND 10"));
	[Fact] public async Task Between_Float() => Assert.Equal("True", await Scalar("SELECT 5.5 BETWEEN 1.0 AND 10.0"));
	[Fact] public async Task Between_String() => Assert.Equal("True", await Scalar("SELECT 'b' BETWEEN 'a' AND 'c'"));
	[Fact] public async Task Between_NegRange() => Assert.Equal("True", await Scalar("SELECT 0 BETWEEN -5 AND 5"));

	// ---- IN ----
	[Fact] public async Task In_Found() => Assert.Equal("True", await Scalar("SELECT 2 IN (1, 2, 3)"));
	[Fact] public async Task In_NotFound() => Assert.Equal("False", await Scalar("SELECT 5 IN (1, 2, 3)"));
	[Fact] public async Task In_First() => Assert.Equal("True", await Scalar("SELECT 1 IN (1, 2, 3)"));
	[Fact] public async Task In_Last() => Assert.Equal("True", await Scalar("SELECT 3 IN (1, 2, 3)"));
	[Fact] public async Task In_String() => Assert.Equal("True", await Scalar("SELECT 'b' IN ('a', 'b', 'c')"));
	[Fact] public async Task In_StringNotFound() => Assert.Equal("False", await Scalar("SELECT 'd' IN ('a', 'b', 'c')"));
	[Fact] public async Task NotIn_Found() => Assert.Equal("False", await Scalar("SELECT 2 NOT IN (1, 2, 3)"));
	[Fact] public async Task NotIn_NotFound() => Assert.Equal("True", await Scalar("SELECT 5 NOT IN (1, 2, 3)"));
	[Fact] public async Task In_Single() => Assert.Equal("True", await Scalar("SELECT 1 IN (1)"));
	[Fact] public async Task In_SingleFalse() => Assert.Equal("False", await Scalar("SELECT 2 IN (1)"));

	// ---- AND / OR / NOT ----
	[Fact] public async Task And_TT() => Assert.Equal("True", await Scalar("SELECT TRUE AND TRUE"));
	[Fact] public async Task And_TF() => Assert.Equal("False", await Scalar("SELECT TRUE AND FALSE"));
	[Fact] public async Task And_FF() => Assert.Equal("False", await Scalar("SELECT FALSE AND FALSE"));
	[Fact] public async Task Or_TT() => Assert.Equal("True", await Scalar("SELECT TRUE OR TRUE"));
	[Fact] public async Task Or_TF() => Assert.Equal("True", await Scalar("SELECT TRUE OR FALSE"));
	[Fact] public async Task Or_FF() => Assert.Equal("False", await Scalar("SELECT FALSE OR FALSE"));
	[Fact] public async Task Not_True() => Assert.Equal("False", await Scalar("SELECT NOT TRUE"));
	[Fact] public async Task Not_False() => Assert.Equal("True", await Scalar("SELECT NOT FALSE"));
	[Fact] public async Task And_Multi() => Assert.Equal("True", await Scalar("SELECT TRUE AND TRUE AND TRUE"));
	[Fact] public async Task Or_Multi() => Assert.Equal("True", await Scalar("SELECT FALSE OR FALSE OR TRUE"));
	[Fact] public async Task And_Or_Mix() => Assert.Equal("True", await Scalar("SELECT TRUE AND (FALSE OR TRUE)"));
	[Fact] public async Task Or_And_Mix() => Assert.Equal("True", await Scalar("SELECT FALSE OR (TRUE AND TRUE)"));

	// ---- IS NULL / IS NOT NULL ----
	[Fact] public async Task IsNull_True2() => Assert.Equal("True", await Scalar("SELECT NULL IS NULL"));
	[Fact] public async Task IsNull_False2() => Assert.Equal("False", await Scalar("SELECT 1 IS NULL"));
	[Fact] public async Task IsNotNull_True2() => Assert.Equal("True", await Scalar("SELECT 1 IS NOT NULL"));
	[Fact] public async Task IsNotNull_False2() => Assert.Equal("False", await Scalar("SELECT NULL IS NOT NULL"));
	[Fact] public async Task IsNull_String() => Assert.Equal("False", await Scalar("SELECT '' IS NULL"));
	[Fact] public async Task IsNull_Zero() => Assert.Equal("False", await Scalar("SELECT 0 IS NULL"));
	[Fact] public async Task IsNotNull_String() => Assert.Equal("True", await Scalar("SELECT '' IS NOT NULL"));
	[Fact] public async Task IsNotNull_Zero() => Assert.Equal("True", await Scalar("SELECT 0 IS NOT NULL"));

	// ---- COALESCE ----
	[Fact] public async Task Coalesce_First() => Assert.Equal("1", await Scalar("SELECT COALESCE(1, 2, 3)"));
	[Fact] public async Task Coalesce_Second() => Assert.Equal("2", await Scalar("SELECT COALESCE(NULL, 2, 3)"));
	[Fact] public async Task Coalesce_Third() => Assert.Equal("3", await Scalar("SELECT COALESCE(NULL, NULL, 3)"));
	[Fact] public async Task Coalesce_AllNull() => Assert.Null(await Scalar("SELECT COALESCE(NULL, NULL, NULL)"));
	[Fact] public async Task Coalesce_Single() => Assert.Equal("5", await Scalar("SELECT COALESCE(5)"));
	[Fact] public async Task Coalesce_String() => Assert.Equal("hello", await Scalar("SELECT COALESCE(NULL, 'hello')"));
	[Fact] public async Task Coalesce_Zero() => Assert.Equal("0", await Scalar("SELECT COALESCE(0, 1)"));
	[Fact] public async Task Coalesce_EmptyStr() => Assert.Equal("", await Scalar("SELECT COALESCE('', 'x')"));

	// ---- IFNULL / NULLIF ----
	[Fact] public async Task Ifnull_NonNull() => Assert.Equal("1", await Scalar("SELECT IFNULL(1, 2)"));
	[Fact] public async Task Ifnull_Null() => Assert.Equal("2", await Scalar("SELECT IFNULL(NULL, 2)"));
	[Fact] public async Task Ifnull_String() => Assert.Equal("hello", await Scalar("SELECT IFNULL('hello', 'world')"));
	[Fact] public async Task Ifnull_NullStr() => Assert.Equal("world", await Scalar("SELECT IFNULL(CAST(NULL AS STRING), 'world')"));
	[Fact] public async Task Nullif_Different() => Assert.Equal("1", await Scalar("SELECT NULLIF(1, 2)"));
	[Fact] public async Task Nullif_Same() => Assert.Null(await Scalar("SELECT NULLIF(1, 1)"));
	[Fact] public async Task Nullif_StrDiff() => Assert.Equal("a", await Scalar("SELECT NULLIF('a', 'b')"));
	[Fact] public async Task Nullif_StrSame() => Assert.Null(await Scalar("SELECT NULLIF('a', 'a')"));

	// ---- CASE WHEN ----
	[Fact] public async Task Case_First() => Assert.Equal("one", await Scalar("SELECT CASE WHEN 1=1 THEN 'one' WHEN 2=2 THEN 'two' END"));
	[Fact] public async Task Case_Second() => Assert.Equal("two", await Scalar("SELECT CASE WHEN 1=2 THEN 'one' WHEN 2=2 THEN 'two' END"));
	[Fact] public async Task Case_Else() =>Assert.Equal("other", await Scalar("SELECT CASE WHEN 1=2 THEN 'one' ELSE 'other' END"));
	[Fact] public async Task Case_NoMatch() => Assert.Null(await Scalar("SELECT CASE WHEN 1=2 THEN 'one' END"));
	[Fact] public async Task Case_Simple() => Assert.Equal("one", await Scalar("SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' END"));
	[Fact] public async Task Case_Simple2() => Assert.Equal("two", await Scalar("SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' END"));
	[Fact] public async Task Case_SimpleElse() => Assert.Equal("other", await Scalar("SELECT CASE 3 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END"));
	[Fact] public async Task Case_Nested() => Assert.Equal("big", await Scalar("SELECT CASE WHEN 10 > 5 THEN CASE WHEN 10 > 8 THEN 'big' ELSE 'medium' END ELSE 'small' END"));
	[Fact] public async Task Case_Int() => Assert.Equal("10", await Scalar("SELECT CASE WHEN TRUE THEN 10 ELSE 20 END"));
	[Fact] public async Task Case_Multi() => Assert.Equal("b", await Scalar("SELECT CASE WHEN 1=2 THEN 'a' WHEN 2=2 THEN 'b' WHEN 3=3 THEN 'c' END"));

	// ---- String concat operator || ----
	[Fact] public async Task Pipe_Concat() => Assert.Equal("ab", await Scalar("SELECT 'a' || 'b'"));
	[Fact] public async Task Pipe_Three() => Assert.Equal("abc", await Scalar("SELECT 'a' || 'b' || 'c'"));
	[Fact] public async Task Pipe_Empty() => Assert.Equal("ab", await Scalar("SELECT 'a' || '' || 'b'"));
	[Fact] public async Task Pipe_Space() => Assert.Equal("hello world", await Scalar("SELECT 'hello' || ' ' || 'world'"));

	// ---- Operator precedence ----
	[Fact] public async Task Prec_MulAdd() => Assert.Equal("7", await Scalar("SELECT 1 + 2 * 3"));
	[Fact] public async Task Prec_ParenAdd() => Assert.Equal("9", await Scalar("SELECT (1 + 2) * 3"));
	[Fact] public async Task Prec_DivSub() => Assert.Equal("8", await Scalar("SELECT 10 - 4 / 2"));
	[Fact] public async Task Prec_ParenSub() => Assert.Equal("3", await Scalar("SELECT (10 - 4) / 2"));
	[Fact] public async Task Prec_NotAnd() => Assert.Equal("False", await Scalar("SELECT NOT TRUE AND FALSE"));
	[Fact] public async Task Prec_NotOr() => Assert.Equal("True", await Scalar("SELECT NOT FALSE OR FALSE"));
	[Fact] public async Task Prec_Complex() => Assert.Equal("14", await Scalar("SELECT 2 + 3 * 4"));
	[Fact] public async Task Prec_UnaryNeg() => Assert.Equal("-7", await Scalar("SELECT -3 - 4"));
	[Fact] public async Task Prec_MultipleParen() => Assert.Equal("20", await Scalar("SELECT (2 + 3) * (1 + 3)"));

	// ---- Bitwise operators ----
	[Fact] public async Task BitAnd() => Assert.Equal("0", await Scalar("SELECT 5 & 2"));
	[Fact] public async Task BitOr() => Assert.Equal("7", await Scalar("SELECT 5 | 2"));
	[Fact] public async Task BitXor() => Assert.Equal("7", await Scalar("SELECT 5 ^ 2"));
	[Fact] public async Task BitNot() => Assert.Equal("-6", await Scalar("SELECT ~5"));
	[Fact] public async Task ShiftLeft() => Assert.Equal("20", await Scalar("SELECT 5 << 2"));
	[Fact] public async Task ShiftRight() => Assert.Equal("5", await Scalar("SELECT 20 >> 2"));
}
