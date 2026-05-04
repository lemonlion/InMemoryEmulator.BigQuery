using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for math and numeric functions: ABS, ROUND, CEIL, FLOOR, TRUNC, MOD, DIV, POW, SQRT, LOG, EXP, SIGN, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class MathFunctionComprehensiveExtTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public MathFunctionComprehensiveExtTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql, parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }

	// ---- ABS ----
	[Fact] public async Task Abs_Positive() => Assert.Equal("5", await S("SELECT ABS(5)"));
	[Fact] public async Task Abs_Negative() => Assert.Equal("5", await S("SELECT ABS(-5)"));
	[Fact] public async Task Abs_Zero() => Assert.Equal("0", await S("SELECT ABS(0)"));
	[Fact] public async Task Abs_Float() => Assert.Equal("3.14", await S("SELECT ABS(-3.14)"));

	// ---- ROUND ----
	[Fact] public async Task Round_Default() => Assert.Equal("3", await S("SELECT ROUND(3.14)"));
	[Fact] public async Task Round_Up() => Assert.Equal("4", await S("SELECT ROUND(3.5)"));
	[Fact] public async Task Round_Decimals() => Assert.Equal("3.14", await S("SELECT ROUND(3.14159, 2)"));
	[Fact] public async Task Round_Negative() => Assert.Equal("-3", await S("SELECT ROUND(-3.14)"));
	[Fact] public async Task Round_NegativeDecimals() => Assert.Equal("3100", await S("SELECT ROUND(3142, -2)"));

	// ---- CEIL / CEILING ----
	[Fact] public async Task Ceil_Positive() => Assert.Equal("4", await S("SELECT CEIL(3.1)"));
	[Fact] public async Task Ceil_Negative() => Assert.Equal("-3", await S("SELECT CEIL(-3.9)"));
	[Fact] public async Task Ceil_Whole() => Assert.Equal("5", await S("SELECT CEIL(5.0)"));
	[Fact] public async Task Ceiling_Alias() => Assert.Equal("4", await S("SELECT CEILING(3.1)"));

	// ---- FLOOR ----
	[Fact] public async Task Floor_Positive() => Assert.Equal("3", await S("SELECT FLOOR(3.9)"));
	[Fact] public async Task Floor_Negative() => Assert.Equal("-4", await S("SELECT FLOOR(-3.1)"));
	[Fact] public async Task Floor_Whole() => Assert.Equal("5", await S("SELECT FLOOR(5.0)"));

	// ---- TRUNC ----
	[Fact] public async Task Trunc_Positive() => Assert.Equal("3", await S("SELECT TRUNC(3.9)"));
	[Fact] public async Task Trunc_Negative() => Assert.Equal("-3", await S("SELECT TRUNC(-3.9)"));
	[Fact] public async Task Trunc_Decimals() => Assert.Equal("3.14", await S("SELECT TRUNC(3.14159, 2)"));

	// ---- MOD ----
	[Fact] public async Task Mod_Basic() => Assert.Equal("1", await S("SELECT MOD(10, 3)"));
	[Fact] public async Task Mod_Zero() => Assert.Equal("0", await S("SELECT MOD(10, 5)"));
	[Fact] public async Task Mod_Negative() => Assert.Equal("-1", await S("SELECT MOD(-10, 3)"));

	// ---- DIV ----
	[Fact] public async Task Div_Basic() => Assert.Equal("3", await S("SELECT DIV(10, 3)"));
	[Fact] public async Task Div_Exact() => Assert.Equal("5", await S("SELECT DIV(10, 2)"));
	[Fact] public async Task Div_Negative() => Assert.Equal("-3", await S("SELECT DIV(-10, 3)"));

	// ---- POW / POWER ----
	[Fact] public async Task Pow_Basic() => Assert.Equal("8", await S("SELECT POW(2, 3)"));
	[Fact] public async Task Pow_Zero() => Assert.Equal("1", await S("SELECT POW(5, 0)"));
	[Fact] public async Task Pow_Fraction() => Assert.Equal("2", await S("SELECT POW(4, 0.5)"));
	[Fact] public async Task Power_Alias() => Assert.Equal("9", await S("SELECT POWER(3, 2)"));

	// ---- SQRT ----
	[Fact] public async Task Sqrt_Perfect() => Assert.Equal("3", await S("SELECT SQRT(9)"));
	[Fact] public async Task Sqrt_One() => Assert.Equal("1", await S("SELECT SQRT(1)"));
	[Fact] public async Task Sqrt_Zero() => Assert.Equal("0", await S("SELECT SQRT(0)"));

	// ---- LOG / LN / LOG10 ----
	[Fact] public async Task Ln_E()
	{
		var v = double.Parse(await S("SELECT LN(EXP(1))") ?? "0");
		Assert.True(Math.Abs(v - 1.0) < 0.001);
	}
	[Fact] public async Task Log_Base2()
	{
		var v = double.Parse(await S("SELECT LOG(8, 2)") ?? "0");
		Assert.True(Math.Abs(v - 3.0) < 0.001);
	}
	[Fact] public async Task Log10_Hundred()
	{
		var v = double.Parse(await S("SELECT LOG10(100)") ?? "0");
		Assert.True(Math.Abs(v - 2.0) < 0.001);
	}

	// ---- EXP ----
	[Fact] public async Task Exp_Zero() => Assert.Equal("1", await S("SELECT EXP(0)"));
	[Fact] public async Task Exp_One()
	{
		var v = double.Parse(await S("SELECT EXP(1)") ?? "0");
		Assert.True(Math.Abs(v - Math.E) < 0.001);
	}

	// ---- SIGN ----
	[Fact] public async Task Sign_Positive() => Assert.Equal("1", await S("SELECT SIGN(42)"));
	[Fact] public async Task Sign_Negative() => Assert.Equal("-1", await S("SELECT SIGN(-42)"));
	[Fact] public async Task Sign_Zero() => Assert.Equal("0", await S("SELECT SIGN(0)"));

	// ---- Trigonometric ----
	[Fact] public async Task Sin_Zero() => Assert.Equal("0", await S("SELECT SIN(0)"));
	[Fact] public async Task Cos_Zero() => Assert.Equal("1", await S("SELECT COS(0)"));
	[Fact] public async Task Tan_Zero() => Assert.Equal("0", await S("SELECT TAN(0)"));
	[Fact] public async Task Asin_Zero() => Assert.Equal("0", await S("SELECT ASIN(0)"));
	[Fact] public async Task Acos_One() => Assert.Equal("0", await S("SELECT ACOS(1)"));
	[Fact] public async Task Atan_Zero() => Assert.Equal("0", await S("SELECT ATAN(0)"));
	[Fact] public async Task Atan2_Basic()
	{
		var v = double.Parse(await S("SELECT ATAN2(1, 1)") ?? "0");
		Assert.True(Math.Abs(v - Math.PI / 4) < 0.001);
	}

	// ---- IEEE ----
	[Fact] public async Task IsInf_True() => Assert.Equal("True", await S("SELECT IS_INF(CAST('inf' AS FLOAT64))"));
	[Fact] public async Task IsInf_False() => Assert.Equal("False", await S("SELECT IS_INF(1.0)"));
	[Fact] public async Task IsNan_True() => Assert.Equal("True", await S("SELECT IS_NAN(CAST('nan' AS FLOAT64))"));
	[Fact] public async Task IsNan_False() => Assert.Equal("False", await S("SELECT IS_NAN(1.0)"));
	[Fact] public async Task Ieee_Inf() { var v = await S("SELECT IEEE_DIVIDE(1, 0)"); Assert.True(v == "Infinity" || v == "\u221E", $"Expected Infinity or ∞, got: {v}"); }
	[Fact] public async Task Ieee_NegInf() { var v = await S("SELECT IEEE_DIVIDE(-1, 0)"); Assert.True(v == "-Infinity" || v == "-\u221E", $"Expected -Infinity or -∞, got: {v}"); }
	[Fact] public async Task Ieee_Nan() => Assert.Equal("NaN", await S("SELECT IEEE_DIVIDE(0, 0)"));

	// ---- SAFE_ functions ----
	[Fact] public async Task SafeDivide_Normal() => Assert.Equal("5", await S("SELECT SAFE_DIVIDE(10, 2)"));
	[Fact] public async Task SafeDivide_Zero() => Assert.Null(await S("SELECT SAFE_DIVIDE(10, 0)"));
	[Fact] public async Task SafeMultiply_Normal() => Assert.Equal("20", await S("SELECT SAFE_MULTIPLY(4, 5)"));
	[Fact] public async Task SafeNegate_Positive() => Assert.Equal("-5", await S("SELECT SAFE_NEGATE(5)"));
	[Fact] public async Task SafeAdd_Normal() => Assert.Equal("15", await S("SELECT SAFE_ADD(10, 5)"));
	[Fact] public async Task SafeSubtract_Normal() => Assert.Equal("5", await S("SELECT SAFE_SUBTRACT(10, 5)"));

	// ---- Combinatorial ----
	[Fact] public async Task Combo_AbsRound() => Assert.Equal("4", await S("SELECT ABS(ROUND(-3.7))"));
	[Fact] public async Task Combo_CeilFloor() => Assert.Equal("1", await S("SELECT CEIL(3.1) - FLOOR(3.1)"));
	[Fact] public async Task Combo_SqrtPow() => Assert.Equal("5", await S("SELECT SQRT(POW(3, 2) + POW(4, 2))"));
	[Fact] public async Task Combo_ModDiv() => Assert.Equal("10", await S("SELECT DIV(10, 3) * 3 + MOD(10, 3)"));

	// ---- NULL handling ----
	[Fact] public async Task Null_Abs() => Assert.Null(await S("SELECT ABS(CAST(NULL AS INT64))"));
	[Fact] public async Task Null_Round() => Assert.Null(await S("SELECT ROUND(CAST(NULL AS FLOAT64))"));
	[Fact] public async Task Null_Sqrt() => Assert.Null(await S("SELECT SQRT(CAST(NULL AS FLOAT64))"));
	[Fact] public async Task Null_Pow() => Assert.Null(await S("SELECT POW(NULL, 2)"));
}
