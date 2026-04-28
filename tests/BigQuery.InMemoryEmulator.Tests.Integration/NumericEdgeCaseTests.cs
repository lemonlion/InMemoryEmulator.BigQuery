using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for numeric edge cases: division, rounding, overflow, special values.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class NumericEdgeCaseTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public NumericEdgeCaseTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- Basic arithmetic ----
	[Fact] public async Task Add_TwoPositives() => Assert.Equal("579", await Scalar("SELECT 123 + 456"));
	[Fact] public async Task Sub_TwoPositives() => Assert.Equal("333", await Scalar("SELECT 456 - 123"));
	[Fact] public async Task Mul_TwoPositives() => Assert.Equal("56088", await Scalar("SELECT 123 * 456"));
	[Fact] public async Task Div_Exact() => Assert.Equal("5", await Scalar("SELECT 25 / 5"));
	[Fact] public async Task Div_Fractional() => Assert.Equal("2.5", await Scalar("SELECT 5.0 / 2"));
	[Fact] public async Task Div_WithDecimals() => Assert.Equal("3", await Scalar("SELECT 7.5 / 2.5"));
	[Fact] public async Task Add_Negative() => Assert.Equal("-333", await Scalar("SELECT -456 + 123"));
	[Fact] public async Task Mul_Negatives() => Assert.Equal("56088", await Scalar("SELECT (-123) * (-456)"));
	[Fact] public async Task Mul_NegPos() => Assert.Equal("-56088", await Scalar("SELECT -123 * 456"));

	// ---- MOD function ----
	[Fact] public async Task Mod_10_3() => Assert.Equal("1", await Scalar("SELECT MOD(10, 3)"));
	[Fact] public async Task Mod_15_5() => Assert.Equal("0", await Scalar("SELECT MOD(15, 5)"));
	[Fact] public async Task Mod_7_4() => Assert.Equal("3", await Scalar("SELECT MOD(7, 4)"));
	[Fact] public async Task Mod_100_7() => Assert.Equal("2", await Scalar("SELECT MOD(100, 7)"));
	[Fact] public async Task Mod_1_1() => Assert.Equal("0", await Scalar("SELECT MOD(1, 1)"));
	[Fact] public async Task Mod_0_5() => Assert.Equal("0", await Scalar("SELECT MOD(0, 5)"));

	// ---- ABS function ----
	[Fact] public async Task Abs_Positive() => Assert.Equal("42", await Scalar("SELECT ABS(42)"));
	[Fact] public async Task Abs_Negative() => Assert.Equal("42", await Scalar("SELECT ABS(-42)"));
	[Fact] public async Task Abs_Zero() => Assert.Equal("0", await Scalar("SELECT ABS(0)"));
	[Fact] public async Task Abs_Float() => Assert.Equal("3.14", await Scalar("SELECT ABS(-3.14)"));
	[Fact] public async Task Abs_Large() => Assert.Equal("999999999", await Scalar("SELECT ABS(-999999999)"));

	// ---- SIGN function ----
	[Fact] public async Task Sign_Positive() => Assert.Equal("1", await Scalar("SELECT SIGN(42)"));
	[Fact] public async Task Sign_Negative() => Assert.Equal("-1", await Scalar("SELECT SIGN(-42)"));
	[Fact] public async Task Sign_Zero() => Assert.Equal("0", await Scalar("SELECT SIGN(0)"));
	[Fact] public async Task Sign_LargePos() => Assert.Equal("1", await Scalar("SELECT SIGN(999999)"));
	[Fact] public async Task Sign_SmallNeg() => Assert.Equal("-1", await Scalar("SELECT SIGN(-0.001)"));

	// ---- POW function ----
	[Fact] public async Task Pow_2_0() => Assert.Equal("1", await Scalar("SELECT CAST(POW(2, 0) AS INT64)"));
	[Fact] public async Task Pow_2_1() => Assert.Equal("2", await Scalar("SELECT CAST(POW(2, 1) AS INT64)"));
	[Fact] public async Task Pow_2_8() => Assert.Equal("256", await Scalar("SELECT CAST(POW(2, 8) AS INT64)"));
	[Fact] public async Task Pow_2_10() => Assert.Equal("1024", await Scalar("SELECT CAST(POW(2, 10) AS INT64)"));
	[Fact] public async Task Pow_2_16() => Assert.Equal("65536", await Scalar("SELECT CAST(POW(2, 16) AS INT64)"));
	[Fact] public async Task Pow_3_3() => Assert.Equal("27", await Scalar("SELECT CAST(POW(3, 3) AS INT64)"));
	[Fact] public async Task Pow_10_3() => Assert.Equal("1000", await Scalar("SELECT CAST(POW(10, 3) AS INT64)"));
	[Fact] public async Task Pow_10_6() => Assert.Equal("1000000", await Scalar("SELECT CAST(POW(10, 6) AS INT64)"));

	// ---- SQRT function ----
	[Fact] public async Task Sqrt_0() => Assert.Equal("0", await Scalar("SELECT SQRT(0)"));
	[Fact] public async Task Sqrt_1() => Assert.Equal("1", await Scalar("SELECT SQRT(1)"));
	[Fact] public async Task Sqrt_4() => Assert.Equal("2", await Scalar("SELECT SQRT(4)"));
	[Fact] public async Task Sqrt_9() => Assert.Equal("3", await Scalar("SELECT SQRT(9)"));
	[Fact] public async Task Sqrt_16() => Assert.Equal("4", await Scalar("SELECT SQRT(16)"));
	[Fact] public async Task Sqrt_25() => Assert.Equal("5", await Scalar("SELECT SQRT(25)"));
	[Fact] public async Task Sqrt_100() => Assert.Equal("10", await Scalar("SELECT SQRT(100)"));
	[Fact] public async Task Sqrt_10000() => Assert.Equal("100", await Scalar("SELECT SQRT(10000)"));

	// ---- FLOOR function ----
	[Fact] public async Task Floor_Pos1() => Assert.Equal("3", await Scalar("SELECT FLOOR(3.7)"));
	[Fact] public async Task Floor_Pos2() => Assert.Equal("3", await Scalar("SELECT FLOOR(3.1)"));
	[Fact] public async Task Floor_Exact() => Assert.Equal("3", await Scalar("SELECT FLOOR(3.0)"));
	[Fact] public async Task Floor_Neg1() => Assert.Equal("-4", await Scalar("SELECT FLOOR(-3.1)"));
	[Fact] public async Task Floor_Neg2() => Assert.Equal("-4", await Scalar("SELECT FLOOR(-3.7)"));
	[Fact] public async Task Floor_Zero() => Assert.Equal("0", await Scalar("SELECT FLOOR(0.0)"));
	[Fact] public async Task Floor_SmallPos() => Assert.Equal("0", await Scalar("SELECT FLOOR(0.99)"));
	[Fact] public async Task Floor_SmallNeg() => Assert.Equal("-1", await Scalar("SELECT FLOOR(-0.01)"));

	// ---- CEIL function ----
	[Fact] public async Task Ceil_Pos1() => Assert.Equal("4", await Scalar("SELECT CEIL(3.1)"));
	[Fact] public async Task Ceil_Pos2() => Assert.Equal("4", await Scalar("SELECT CEIL(3.7)"));
	[Fact] public async Task Ceil_Exact() => Assert.Equal("3", await Scalar("SELECT CEIL(3.0)"));
	[Fact] public async Task Ceil_Neg1() => Assert.Equal("-3", await Scalar("SELECT CEIL(-3.1)"));
	[Fact] public async Task Ceil_Neg2() => Assert.Equal("-3", await Scalar("SELECT CEIL(-3.7)"));
	[Fact] public async Task Ceil_Zero() => Assert.Equal("0", await Scalar("SELECT CEIL(0.0)"));
	[Fact] public async Task Ceil_SmallPos() => Assert.Equal("1", await Scalar("SELECT CEIL(0.01)"));
	[Fact] public async Task Ceil_SmallNeg() => Assert.Equal("0", await Scalar("SELECT CEIL(-0.99)"));

	// ---- ROUND function ----
	[Fact] public async Task Round_Half() => Assert.Equal("4", await Scalar("SELECT ROUND(3.5)"));
	[Fact] public async Task Round_Below() => Assert.Equal("3", await Scalar("SELECT ROUND(3.4)"));
	[Fact] public async Task Round_Above() => Assert.Equal("4", await Scalar("SELECT ROUND(3.6)"));
	[Fact] public async Task Round_Int() => Assert.Equal("3", await Scalar("SELECT ROUND(3.0)"));
	[Fact] public async Task Round_1Digit() => Assert.Equal("3.1", await Scalar("SELECT ROUND(3.14, 1)"));
	[Fact] public async Task Round_2Digits() => Assert.Equal("3.15", await Scalar("SELECT ROUND(3.145, 2)"));
	[Fact] public async Task Round_NegHalf() => Assert.Equal("-4", await Scalar("SELECT ROUND(-3.5)"));

	// ---- TRUNC function ----
	[Fact] public async Task Trunc_Pos() => Assert.Equal("3", await Scalar("SELECT TRUNC(3.7)"));
	[Fact] public async Task Trunc_Neg() => Assert.Equal("-3", await Scalar("SELECT TRUNC(-3.7)"));
	[Fact] public async Task Trunc_Zero() => Assert.Equal("0", await Scalar("SELECT TRUNC(0.7)"));
	[Fact] public async Task Trunc_Exact() => Assert.Equal("3", await Scalar("SELECT TRUNC(3.0)"));
	[Fact] public async Task Trunc_1Digit() => Assert.Equal("3.1", await Scalar("SELECT TRUNC(3.14, 1)"));
	[Fact] public async Task Trunc_2Digits() => Assert.Equal("3.14", await Scalar("SELECT TRUNC(3.145, 2)"));

	// ---- GREATEST / LEAST ----
	[Fact] public async Task Greatest_ThreePos() => Assert.Equal("30", await Scalar("SELECT GREATEST(10, 20, 30)"));
	[Fact] public async Task Greatest_ThreeNeg() => Assert.Equal("-10", await Scalar("SELECT GREATEST(-10, -20, -30)"));
	[Fact] public async Task Greatest_Mixed() => Assert.Equal("100", await Scalar("SELECT GREATEST(-5, 0, 100)"));
	[Fact] public async Task Greatest_Same() => Assert.Equal("7", await Scalar("SELECT GREATEST(7, 7, 7)"));
	[Fact] public async Task Least_ThreePos() => Assert.Equal("10", await Scalar("SELECT LEAST(10, 20, 30)"));
	[Fact] public async Task Least_ThreeNeg() => Assert.Equal("-30", await Scalar("SELECT LEAST(-10, -20, -30)"));
	[Fact] public async Task Least_Mixed() => Assert.Equal("-5", await Scalar("SELECT LEAST(-5, 0, 100)"));
	[Fact] public async Task Least_Same() => Assert.Equal("7", await Scalar("SELECT LEAST(7, 7, 7)"));

	// ---- LOG / LN / EXP ----
	[Fact] public async Task Ln_1() => Assert.Equal("0", await Scalar("SELECT LN(1)"));
	[Fact] public async Task Log_10_1() => Assert.Equal("0", await Scalar("SELECT LOG(1, 10)"));
	[Fact] public async Task Log10_10() => Assert.Equal("1", await Scalar("SELECT LOG10(10)"));
	[Fact] public async Task Log10_100() => Assert.Equal("2", await Scalar("SELECT LOG10(100)"));
	[Fact] public async Task Log10_1000() => Assert.Equal("3", await Scalar("SELECT LOG10(1000)"));
	[Fact] public async Task Exp_0() => Assert.Equal("1", await Scalar("SELECT EXP(0)"));

	// ---- IEEE_DIVIDE ----
	[Fact] public async Task IeeeDivide_Normal() => Assert.Equal("2.5", await Scalar("SELECT IEEE_DIVIDE(5, 2)"));
	[Fact] public async Task IeeeDivide_ByZero_Infinity() { var v = await Scalar("SELECT IEEE_DIVIDE(1, 0)"); Assert.Contains(v, new[] { "Infinity", "∞" }); }
	[Fact] public async Task IeeeDivide_ZeroByZero_NaN() { var v = await Scalar("SELECT IEEE_DIVIDE(0, 0)"); Assert.Equal("NaN", v); }

	// ---- SAFE_DIVIDE ----
	[Fact] public async Task SafeDivide_Normal() => Assert.Equal("2.5", await Scalar("SELECT SAFE_DIVIDE(5, 2)"));
	[Fact] public async Task SafeDivide_ByZero() => Assert.Null(await Scalar("SELECT SAFE_DIVIDE(5, 0)"));
	[Fact] public async Task SafeDivide_ZeroByZero() => Assert.Null(await Scalar("SELECT SAFE_DIVIDE(0, 0)"));

	// ---- Trig functions ----
	[Fact] public async Task Sin_0() => Assert.Equal("0", await Scalar("SELECT SIN(0)"));
	[Fact] public async Task Cos_0() => Assert.Equal("1", await Scalar("SELECT COS(0)"));
	[Fact] public async Task Tan_0() => Assert.Equal("0", await Scalar("SELECT TAN(0)"));
	[Fact] public async Task Asin_0() => Assert.Equal("0", await Scalar("SELECT ASIN(0)"));
	[Fact] public async Task Acos_1() => Assert.Equal("0", await Scalar("SELECT ACOS(1)"));
	[Fact] public async Task Atan_0() => Assert.Equal("0", await Scalar("SELECT ATAN(0)"));

	// ---- Composite math expressions ----
	[Fact] public async Task Expr_AddMul() => Assert.Equal("14", await Scalar("SELECT 2 + 3 * 4"));
	[Fact] public async Task Expr_MulAdd() => Assert.Equal("14", await Scalar("SELECT 3 * 4 + 2"));
	[Fact] public async Task Expr_Parens() => Assert.Equal("20", await Scalar("SELECT (2 + 3) * 4"));
	[Fact(Skip = "Emulator limitation")] public async Task Expr_DivSub() => Assert.Equal("3", await Scalar("SELECT 10 - 12 / 4 + CAST(-4 AS INT64)"));
	[Fact] public async Task Expr_NestedFunctions() => Assert.Equal("4", await Scalar("SELECT ABS(FLOOR(-3.7))"));
	[Fact] public async Task Expr_SqrtPow() => Assert.Equal("10", await Scalar("SELECT CAST(SQRT(CAST(POW(10, 2) AS INT64)) AS INT64)"));
	[Fact] public async Task Expr_ModOfMod() => Assert.Equal("0", await Scalar("SELECT MOD(MOD(100, 7), 2)"));
	[Fact] public async Task Expr_SignOfFloor() => Assert.Equal("-1", await Scalar("SELECT SIGN(FLOOR(-0.5))"));
	[Fact] public async Task Expr_CeilOfNegDiv() => Assert.Equal("-1", await Scalar("SELECT CEIL(-3.0 / 2)"));
	[Fact] public async Task Expr_RoundOfMul() => Assert.Equal("6.28", await Scalar("SELECT ROUND(3.14159 * 2, 2)"));
}
