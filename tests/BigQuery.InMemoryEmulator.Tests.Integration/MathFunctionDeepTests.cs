using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Extensive tests for math functions including ROUND, TRUNC, CEIL, FLOOR, ABS, MOD, SIGN, POW, SQRT, LOG, LN, EXP, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class MathFunctionDeepTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public MathFunctionDeepTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- ABS ----
	[Fact] public async Task Abs_Positive() => Assert.Equal("5", await Scalar("SELECT ABS(5)"));
	[Fact] public async Task Abs_Negative() => Assert.Equal("5", await Scalar("SELECT ABS(-5)"));
	[Fact] public async Task Abs_Zero() => Assert.Equal("0", await Scalar("SELECT ABS(0)"));
	[Fact] public async Task Abs_Large() => Assert.Equal("1000000", await Scalar("SELECT ABS(-1000000)"));
	[Fact] public async Task Abs_Float() => Assert.Equal("3.14", await Scalar("SELECT ABS(-3.14)"));
	[Fact] public async Task Abs_SmallFloat() => Assert.Equal("0.001", await Scalar("SELECT ABS(-0.001)"));

	// ---- SIGN ----
	[Fact] public async Task Sign_Positive() => Assert.Equal("1", await Scalar("SELECT SIGN(42)"));
	[Fact] public async Task Sign_Negative() => Assert.Equal("-1", await Scalar("SELECT SIGN(-42)"));
	[Fact] public async Task Sign_Zero() => Assert.Equal("0", await Scalar("SELECT SIGN(0)"));
	[Fact] public async Task Sign_Large() => Assert.Equal("1", await Scalar("SELECT SIGN(999999)"));
	[Fact] public async Task Sign_SmallNeg() => Assert.Equal("-1", await Scalar("SELECT SIGN(-1)"));
	[Fact] public async Task Sign_Float() => Assert.Equal("1", await Scalar("SELECT SIGN(0.5)"));
	[Fact] public async Task Sign_NegFloat() => Assert.Equal("-1", await Scalar("SELECT SIGN(-0.5)"));

	// ---- MOD ----
	[Fact] public async Task Mod_Basic() => Assert.Equal("1", await Scalar("SELECT MOD(10, 3)"));
	[Fact] public async Task Mod_Even() => Assert.Equal("0", await Scalar("SELECT MOD(10, 5)"));
	[Fact] public async Task Mod_One() => Assert.Equal("0", await Scalar("SELECT MOD(10, 1)"));
	[Fact] public async Task Mod_Large() => Assert.Equal("3", await Scalar("SELECT MOD(103, 10)"));
	[Fact] public async Task Mod_Small() => Assert.Equal("1", await Scalar("SELECT MOD(7, 2)"));
	[Fact] public async Task Mod_NegDividend() => Assert.Equal("-1", await Scalar("SELECT MOD(-7, 2)"));
	[Fact] public async Task Mod_NegDivisor() => Assert.Equal("1", await Scalar("SELECT MOD(7, -2)"));
	[Fact] public async Task Mod_ZeroDividend() => Assert.Equal("0", await Scalar("SELECT MOD(0, 5)"));

	// ---- CEIL / CEILING / FLOOR ----
	[Fact] public async Task Ceil_Positive() => Assert.Equal("4", await Scalar("SELECT CEIL(3.2)"));
	[Fact] public async Task Ceil_Exact() => Assert.Equal("3", await Scalar("SELECT CEIL(3.0)"));
	[Fact] public async Task Ceil_Negative() => Assert.Equal("-3", await Scalar("SELECT CEIL(-3.2)"));
	[Fact] public async Task Ceil_Half() => Assert.Equal("4", await Scalar("SELECT CEIL(3.5)"));
	[Fact] public async Task Ceil_Small() => Assert.Equal("1", await Scalar("SELECT CEIL(0.1)"));
	[Fact] public async Task Ceiling_Alias() => Assert.Equal("4", await Scalar("SELECT CEILING(3.2)"));
	[Fact] public async Task Floor_Positive() => Assert.Equal("3", await Scalar("SELECT FLOOR(3.9)"));
	[Fact] public async Task Floor_Exact() => Assert.Equal("3", await Scalar("SELECT FLOOR(3.0)"));
	[Fact] public async Task Floor_Negative() => Assert.Equal("-4", await Scalar("SELECT FLOOR(-3.2)"));
	[Fact] public async Task Floor_Half2() => Assert.Equal("3", await Scalar("SELECT FLOOR(3.5)"));
	[Fact] public async Task Floor_Small() => Assert.Equal("0", await Scalar("SELECT FLOOR(0.9)"));

	// ---- ROUND ----
	[Fact] public async Task Round_Default() => Assert.Equal("3", await Scalar("SELECT ROUND(3.4)"));
	[Fact] public async Task Round_Up2() => Assert.Equal("4", await Scalar("SELECT ROUND(3.5)"));
	[Fact] public async Task Round_Precise1() => Assert.Equal("3.1", await Scalar("SELECT ROUND(3.14, 1)"));
	[Fact] public async Task Round_Precise2() => Assert.Equal("3.15", await Scalar("SELECT ROUND(3.145, 2)"));
	[Fact] public async Task Round_Negative2() => Assert.Equal("-3", await Scalar("SELECT ROUND(-3.4)"));
	[Fact] public async Task Round_NegUp() => Assert.Equal("-4", await Scalar("SELECT ROUND(-3.5)"));
	[Fact] public async Task Round_Zero() => Assert.Equal("0", await Scalar("SELECT ROUND(0.4)"));
	[Fact] public async Task Round_Exact2() => Assert.Equal("5", await Scalar("SELECT ROUND(5.0)"));
	[Fact] public async Task Round_Large() => Assert.Equal("1000", await Scalar("SELECT ROUND(999.9)"));

	// ---- TRUNC ----
	[Fact] public async Task Trunc_Positive() => Assert.Equal("3", await Scalar("SELECT TRUNC(3.9)"));
	[Fact] public async Task Trunc_Negative() => Assert.Equal("-3", await Scalar("SELECT TRUNC(-3.9)"));
	[Fact] public async Task Trunc_Zero() => Assert.Equal("0", await Scalar("SELECT TRUNC(0.5)"));
	[Fact] public async Task Trunc_Exact() => Assert.Equal("5", await Scalar("SELECT TRUNC(5.0)"));
	[Fact] public async Task Trunc_Precise1() => Assert.Equal("3.1", await Scalar("SELECT TRUNC(3.14, 1)"));
	[Fact] public async Task Trunc_Precise2() => Assert.Equal("3.14", await Scalar("SELECT TRUNC(3.149, 2)"));
	[Fact] public async Task Trunc_NegPrecise() => Assert.Equal("-3.1", await Scalar("SELECT TRUNC(-3.19, 1)"));

	// ---- POW / POWER ----
	[Fact] public async Task Pow_Square2() => Assert.Equal("4", await Scalar("SELECT CAST(POW(2, 2) AS INT64)"));
	[Fact] public async Task Pow_Cube() => Assert.Equal("8", await Scalar("SELECT CAST(POW(2, 3) AS INT64)"));
	[Fact] public async Task Pow_One() => Assert.Equal("2", await Scalar("SELECT CAST(POW(2, 1) AS INT64)"));
	[Fact] public async Task Pow_Zero() => Assert.Equal("1", await Scalar("SELECT CAST(POW(2, 0) AS INT64)"));
	[Fact] public async Task Pow_Ten2() => Assert.Equal("100", await Scalar("SELECT CAST(POW(10, 2) AS INT64)"));
	[Fact] public async Task Power_Alias() => Assert.Equal("9", await Scalar("SELECT CAST(POWER(3, 2) AS INT64)"));

	// ---- SQRT ----
	[Fact] public async Task Sqrt_Four() => Assert.Equal("2", await Scalar("SELECT CAST(SQRT(4) AS INT64)"));
	[Fact] public async Task Sqrt_Nine() => Assert.Equal("3", await Scalar("SELECT CAST(SQRT(9) AS INT64)"));
	[Fact] public async Task Sqrt_Sixteen() => Assert.Equal("4", await Scalar("SELECT CAST(SQRT(16) AS INT64)"));
	[Fact] public async Task Sqrt_One() => Assert.Equal("1", await Scalar("SELECT CAST(SQRT(1) AS INT64)"));
	[Fact] public async Task Sqrt_Zero() => Assert.Equal("0", await Scalar("SELECT CAST(SQRT(0) AS INT64)"));
	[Fact] public async Task Sqrt_Hundred() => Assert.Equal("10", await Scalar("SELECT CAST(SQRT(100) AS INT64)"));

	// ---- LOG / LN / LOG10 ----
	[Fact] public async Task Ln_One() => Assert.Equal("0", await Scalar("SELECT CAST(LN(1) AS INT64)"));
	[Fact] public async Task Log_Base10() => Assert.Equal("2", await Scalar("SELECT CAST(LOG10(100) AS INT64)"));
	[Fact] public async Task Log_Base2() => Assert.Equal("3", await Scalar("SELECT CAST(LOG(8, 2) AS INT64)"));

	// ---- EXP ----
	[Fact] public async Task Exp_Zero() => Assert.Equal("1", await Scalar("SELECT CAST(EXP(0) AS INT64)"));

	// ---- GREATEST / LEAST ----
	[Fact] public async Task Greatest_Simple() => Assert.Equal("3", await Scalar("SELECT GREATEST(1, 2, 3)"));
	[Fact] public async Task Greatest_Neg() => Assert.Equal("1", await Scalar("SELECT GREATEST(-1, 0, 1)"));
	[Fact] public async Task Greatest_Same() => Assert.Equal("5", await Scalar("SELECT GREATEST(5, 5, 5)"));
	[Fact] public async Task Greatest_Two() => Assert.Equal("7", await Scalar("SELECT GREATEST(3, 7)"));
	[Fact] public async Task Greatest_Four() => Assert.Equal("10", await Scalar("SELECT GREATEST(1, 10, 5, 3)"));
	[Fact] public async Task Greatest_AllNeg() => Assert.Equal("-1", await Scalar("SELECT GREATEST(-3, -1, -5)"));
	[Fact] public async Task Least_Simple() => Assert.Equal("1", await Scalar("SELECT LEAST(1, 2, 3)"));
	[Fact] public async Task Least_Neg() => Assert.Equal("-1", await Scalar("SELECT LEAST(-1, 0, 1)"));
	[Fact] public async Task Least_Same() => Assert.Equal("5", await Scalar("SELECT LEAST(5, 5, 5)"));
	[Fact] public async Task Least_Two() => Assert.Equal("3", await Scalar("SELECT LEAST(3, 7)"));
	[Fact] public async Task Least_Four() => Assert.Equal("1", await Scalar("SELECT LEAST(1, 10, 5, 3)"));
	[Fact] public async Task Least_AllNeg() => Assert.Equal("-5", await Scalar("SELECT LEAST(-3, -1, -5)"));

	// ---- DIV ----
	[Fact] public async Task Div_Basic() => Assert.Equal("3", await Scalar("SELECT DIV(10, 3)"));
	[Fact] public async Task Div_Even() => Assert.Equal("5", await Scalar("SELECT DIV(10, 2)"));
	[Fact] public async Task Div_One() => Assert.Equal("10", await Scalar("SELECT DIV(10, 1)"));
	[Fact] public async Task Div_Large() => Assert.Equal("100", await Scalar("SELECT DIV(1000, 10)"));
	[Fact] public async Task Div_Neg() => Assert.Equal("-3", await Scalar("SELECT DIV(-10, 3)"));
	[Fact] public async Task Div_Zero_Dividend() => Assert.Equal("0", await Scalar("SELECT DIV(0, 5)"));

	// ---- IEEE_DIVIDE ----
	[Fact] public async Task IeeeDivide_Normal() => Assert.Equal("2.5", await Scalar("SELECT IEEE_DIVIDE(5, 2)"));
	[Fact] public async Task IeeeDivide_Whole() => Assert.Equal("5", await Scalar("SELECT CAST(IEEE_DIVIDE(10, 2) AS INT64)"));

	// ---- SAFE_DIVIDE / SAFE_NEGATE ----
	[Fact] public async Task SafeDivide_Normal() => Assert.Equal("2.5", await Scalar("SELECT SAFE_DIVIDE(5, 2)"));
	[Fact] public async Task SafeDivide_ByZero() => Assert.Null(await Scalar("SELECT SAFE_DIVIDE(5, 0)"));
	[Fact] public async Task SafeNegate_Pos() => Assert.Equal("-5", await Scalar("SELECT SAFE_NEGATE(5)"));
	[Fact] public async Task SafeNegate_Neg() => Assert.Equal("5", await Scalar("SELECT SAFE_NEGATE(-5)"));
	[Fact] public async Task SafeNegate_Zero() => Assert.Equal("0", await Scalar("SELECT SAFE_NEGATE(0)"));

	// ---- Arithmetic operators ----
	[Fact] public async Task Add_Ints() => Assert.Equal("5", await Scalar("SELECT 2 + 3"));
	[Fact] public async Task Sub_Ints() => Assert.Equal("7", await Scalar("SELECT 10 - 3"));
	[Fact] public async Task Mul_Ints() => Assert.Equal("12", await Scalar("SELECT 3 * 4"));
	[Fact] public async Task Add_Floats() => Assert.Equal("5.5", await Scalar("SELECT 2.5 + 3.0"));
	[Fact] public async Task Mul_Floats() => Assert.Equal("7.5", await Scalar("SELECT 2.5 * 3.0"));
	[Fact] public async Task Neg_Int() => Assert.Equal("-5", await Scalar("SELECT -5"));
	[Fact] public async Task DoubleNeg() => Assert.Equal("5", await Scalar("SELECT -(-5)"));

	// ---- Nested math ----
	[Fact] public async Task Nested_AbsCeil() => Assert.Equal("4", await Scalar("SELECT CEIL(ABS(-3.2))"));
	[Fact] public async Task Nested_FloorAbs() => Assert.Equal("3", await Scalar("SELECT FLOOR(ABS(-3.9))"));
	[Fact] public async Task Nested_RoundSqrt() => Assert.Equal("3", await Scalar("SELECT ROUND(SQRT(10))"));
	[Fact] public async Task Nested_AbsMod() => Assert.Equal("1", await Scalar("SELECT ABS(MOD(-7, 2))"));
	[Fact] public async Task Nested_SignAbs() => Assert.Equal("1", await Scalar("SELECT SIGN(ABS(-5))"));
	[Fact] public async Task Nested_CeilFloor() => Assert.Equal("4", await Scalar("SELECT CEIL(FLOOR(3.9) + 0.5)"));
	[Fact] public async Task Nested_PowSqrt() => Assert.Equal("5", await Scalar("SELECT CAST(SQRT(POW(5, 2)) AS INT64)"));
	[Fact] public async Task Nested_AbsSignMod() => Assert.Equal("1", await Scalar("SELECT ABS(SIGN(MOD(-7, 3)))"));

	// ---- RANGE_BUCKET ----
	[Fact] public async Task RangeBucket_Mid() => Assert.Equal("2", await Scalar("SELECT RANGE_BUCKET(15, [0, 10, 20, 30])"));
	[Fact] public async Task RangeBucket_Low() => Assert.Equal("0", await Scalar("SELECT RANGE_BUCKET(-5, [0, 10, 20])"));
	[Fact] public async Task RangeBucket_High() => Assert.Equal("3", await Scalar("SELECT RANGE_BUCKET(50, [0, 10, 20])"));
	[Fact] public async Task RangeBucket_Exact() => Assert.Equal("2", await Scalar("SELECT RANGE_BUCKET(10, [0, 10, 20])"));
	[Fact] public async Task RangeBucket_Zero() => Assert.Equal("1", await Scalar("SELECT RANGE_BUCKET(0, [0, 10, 20])"));
}
