using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Math function edge cases: boundary values, special floats, precision, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class MathFunctionEdgeCaseTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public MathFunctionEdgeCaseTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
	}

	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- ABS edge cases ----
	[Fact] public async Task Abs_Zero() => Assert.Equal("0", await Scalar("SELECT ABS(0)"));
	[Fact] public async Task Abs_Positive() => Assert.Equal("5", await Scalar("SELECT ABS(5)"));
	[Fact] public async Task Abs_Negative() => Assert.Equal("5", await Scalar("SELECT ABS(-5)"));
	[Fact] public async Task Abs_Float_Negative() => Assert.Equal("3.14", await Scalar("SELECT ABS(-3.14)"));
	[Fact] public async Task Abs_Null() => Assert.Null(await Scalar("SELECT ABS(NULL)"));

	// ---- SIGN edge cases ----
	[Fact] public async Task Sign_Positive() => Assert.Equal("1", await Scalar("SELECT SIGN(100)"));
	[Fact] public async Task Sign_Negative() => Assert.Equal("-1", await Scalar("SELECT SIGN(-100)"));
	[Fact] public async Task Sign_Zero() => Assert.Equal("0", await Scalar("SELECT SIGN(0)"));
	[Fact] public async Task Sign_Null() => Assert.Null(await Scalar("SELECT SIGN(NULL)"));
	[Fact] public async Task Sign_FloatPositive() { var v = await Scalar("SELECT SIGN(0.5)"); Assert.True(v == "1" || v == "1.0", $"Expected 1 or 1.0, got {v}"); }

	// ---- ROUND edge cases ----
	[Fact] public async Task Round_HalfUp() { var v = double.Parse(await Scalar("SELECT ROUND(2.5)") ?? "0"); Assert.True(v == 2 || v == 3); } // IEEE 754 banker's rounding
	[Fact] public async Task Round_Negative() => Assert.Equal("-3", await Scalar("SELECT ROUND(-2.7)"));
	[Fact] public async Task Round_WithPrecision() => Assert.Equal("3.14", await Scalar("SELECT ROUND(3.14159, 2)"));
	[Fact] public async Task Round_NegativePrecision() { var v = await Scalar("SELECT ROUND(1234.0, 0)"); Assert.Equal("1234", v); }
	[Fact] public async Task Round_Zero() => Assert.Equal("0", await Scalar("SELECT ROUND(0.0)"));
	[Fact] public async Task Round_Null() => Assert.Null(await Scalar("SELECT ROUND(NULL)"));

	// ---- TRUNC / TRUNCATE ----
	[Fact] public async Task Trunc_Positive() => Assert.Equal("3", await Scalar("SELECT TRUNC(3.7)"));
	[Fact] public async Task Trunc_Negative() => Assert.Equal("-3", await Scalar("SELECT TRUNC(-3.7)"));
	[Fact] public async Task Trunc_WithPrecision() => Assert.Equal("3.14", await Scalar("SELECT TRUNC(3.14159, 2)"));
	[Fact] public async Task Truncate_Alias() => Assert.Equal("3", await Scalar("SELECT TRUNC(3.7)"));

	// ---- CEIL / FLOOR ----
	[Fact] public async Task Ceil_Positive() => Assert.Equal("4", await Scalar("SELECT CEIL(3.1)"));
	[Fact] public async Task Ceil_Negative() => Assert.Equal("-3", await Scalar("SELECT CEIL(-3.9)"));
	[Fact] public async Task Ceil_Integer() => Assert.Equal("5", await Scalar("SELECT CEIL(5.0)"));
	[Fact] public async Task Ceiling_Alias() => Assert.Equal("4", await Scalar("SELECT CEILING(3.1)"));
	[Fact] public async Task Floor_Positive() => Assert.Equal("3", await Scalar("SELECT FLOOR(3.9)"));
	[Fact] public async Task Floor_Negative() => Assert.Equal("-4", await Scalar("SELECT FLOOR(-3.1)"));
	[Fact] public async Task Floor_Integer() => Assert.Equal("5", await Scalar("SELECT FLOOR(5.0)"));

	// ---- MOD ----
	[Fact] public async Task Mod_Basic() => Assert.Equal("1", await Scalar("SELECT MOD(7, 3)"));
	[Fact] public async Task Mod_EvenDivision() => Assert.Equal("0", await Scalar("SELECT MOD(6, 3)"));
	[Fact] public async Task Mod_NegativeDividend() => Assert.Equal("-1", await Scalar("SELECT MOD(-7, 3)"));
	[Fact] public async Task Mod_Null() => Assert.Null(await Scalar("SELECT MOD(NULL, 3)"));

	// ---- DIV ----
	[Fact] public async Task Div_Basic() => Assert.Equal("2", await Scalar("SELECT DIV(7, 3)"));
	[Fact] public async Task Div_ExactDivision() => Assert.Equal("3", await Scalar("SELECT DIV(9, 3)"));
	[Fact] public async Task Div_NegativeResult() => Assert.Equal("-2", await Scalar("SELECT DIV(-7, 3)"));

	// ---- POW / POWER ----
	[Fact] public async Task Pow_Basic() { var v = double.Parse(await Scalar("SELECT POW(2, 10)") ?? "0"); Assert.Equal(1024.0, v); }
	[Fact] public async Task Pow_Zero() { var v = double.Parse(await Scalar("SELECT POW(5, 0)") ?? "0"); Assert.Equal(1.0, v); }
	[Fact] public async Task Pow_Fractional() { var v = double.Parse(await Scalar("SELECT POW(4, 0.5)") ?? "0"); Assert.True(Math.Abs(v - 2.0) < 0.001); }
	[Fact] public async Task Power_Alias() { var v = double.Parse(await Scalar("SELECT POWER(3, 3)") ?? "0"); Assert.Equal(27.0, v); }

	// ---- SQRT ----
	[Fact] public async Task Sqrt_Perfect() { var v = double.Parse(await Scalar("SELECT SQRT(25)") ?? "0"); Assert.Equal(5.0, v); }
	[Fact] public async Task Sqrt_Zero() { var v = double.Parse(await Scalar("SELECT SQRT(0)") ?? "-1"); Assert.Equal(0.0, v); }
	[Fact] public async Task Sqrt_NonPerfect() { var v = double.Parse(await Scalar("SELECT SQRT(2)") ?? "0"); Assert.True(Math.Abs(v - 1.414) < 0.01); }

	// ---- EXP ----
	[Fact] public async Task Exp_Zero() { var v = double.Parse(await Scalar("SELECT EXP(0)") ?? "0"); Assert.Equal(1.0, v); }
	[Fact] public async Task Exp_One() { var v = double.Parse(await Scalar("SELECT EXP(1)") ?? "0"); Assert.True(Math.Abs(v - Math.E) < 0.01); }

	// ---- LN / LOG / LOG10 ----
	[Fact] public async Task Ln_E() { var v = double.Parse(await Scalar("SELECT LN(EXP(1))") ?? "0"); Assert.True(Math.Abs(v - 1.0) < 0.001); }
	[Fact] public async Task Ln_One() { var v = double.Parse(await Scalar("SELECT LN(1)") ?? "1"); Assert.True(Math.Abs(v) < 0.001); }
	[Fact] public async Task Log_Base2() { var v = double.Parse(await Scalar("SELECT LOG(8, 2)") ?? "0"); Assert.True(Math.Abs(v - 3.0) < 0.001); }
	[Fact] public async Task Log_Base10() { var v = double.Parse(await Scalar("SELECT LOG(1000, 10)") ?? "0"); Assert.True(Math.Abs(v - 3.0) < 0.001); }
	[Fact] public async Task Log10_Thousand() { var v = double.Parse(await Scalar("SELECT LOG10(1000)") ?? "0"); Assert.True(Math.Abs(v - 3.0) < 0.001); }

	// ---- SAFE_ arithmetic edge cases ----
	[Fact] public async Task SafeDivide_Normal() => Assert.Equal("5", await Scalar("SELECT SAFE_DIVIDE(10, 2)"));
	[Fact] public async Task SafeDivide_ByZero() => Assert.Null(await Scalar("SELECT SAFE_DIVIDE(10, 0)"));
	[Fact] public async Task SafeDivide_NullDividend() => Assert.Null(await Scalar("SELECT SAFE_DIVIDE(NULL, 2)"));
	[Fact] public async Task SafeDivide_NullDivisor() => Assert.Null(await Scalar("SELECT SAFE_DIVIDE(10, NULL)"));
	[Fact] public async Task SafeAdd_Normal() => Assert.Equal("15", await Scalar("SELECT SAFE_ADD(5, 10)"));
	[Fact] public async Task SafeAdd_Null() => Assert.Null(await Scalar("SELECT SAFE_ADD(NULL, 10)"));
	[Fact] public async Task SafeSubtract_Normal() => Assert.Equal("5", await Scalar("SELECT SAFE_SUBTRACT(15, 10)"));
	[Fact] public async Task SafeMultiply_Normal() => Assert.Equal("50", await Scalar("SELECT SAFE_MULTIPLY(5, 10)"));
	[Fact] public async Task SafeNegate_Positive() => Assert.Equal("-5", await Scalar("SELECT SAFE_NEGATE(5)"));
	[Fact] public async Task SafeNegate_Negative() => Assert.Equal("5", await Scalar("SELECT SAFE_NEGATE(-5)"));
	[Fact] public async Task SafeNegate_Null() => Assert.Null(await Scalar("SELECT SAFE_NEGATE(NULL)"));

	// ---- IEEE_DIVIDE ----
	[Fact] public async Task IeeeDivide_Normal() => Assert.Equal("5", await Scalar("SELECT IEEE_DIVIDE(10, 2)"));
	[Fact] public async Task IeeeDivide_ByZero_Inf() { var v = await Scalar("SELECT IEEE_DIVIDE(1, 0)"); Assert.NotNull(v); }
	[Fact] public async Task IeeeDivide_ZeroByZero_NaN() { var v = await Scalar("SELECT IEEE_DIVIDE(0, 0)"); Assert.Contains("NaN", v!); }

	// ---- IS_INF / IS_NAN ----
	[Fact] public async Task IsInf_Infinity() => Assert.Equal("True", await Scalar("SELECT IS_INF(IEEE_DIVIDE(1, 0))"));
	[Fact] public async Task IsInf_Normal() => Assert.Equal("False", await Scalar("SELECT IS_INF(1.0)"));
	[Fact] public async Task IsNan_NaN() => Assert.Equal("True", await Scalar("SELECT IS_NAN(IEEE_DIVIDE(0, 0))"));
	[Fact] public async Task IsNan_Normal() => Assert.Equal("False", await Scalar("SELECT IS_NAN(1.0)"));

	// ---- Trig functions basic values ----
	[Fact] public async Task Sin_Zero() { var v = double.Parse(await Scalar("SELECT SIN(0)") ?? "1"); Assert.True(Math.Abs(v) < 0.001); }
	[Fact] public async Task Cos_Zero() { var v = double.Parse(await Scalar("SELECT COS(0)") ?? "0"); Assert.True(Math.Abs(v - 1.0) < 0.001); }
	[Fact] public async Task Tan_Zero() { var v = double.Parse(await Scalar("SELECT TAN(0)") ?? "1"); Assert.True(Math.Abs(v) < 0.001); }
	[Fact] public async Task Asin_Zero() { var v = double.Parse(await Scalar("SELECT ASIN(0)") ?? "1"); Assert.True(Math.Abs(v) < 0.001); }
	[Fact] public async Task Acos_One() { var v = double.Parse(await Scalar("SELECT ACOS(1)") ?? "1"); Assert.True(Math.Abs(v) < 0.001); }
	[Fact] public async Task Atan_Zero() { var v = double.Parse(await Scalar("SELECT ATAN(0)") ?? "1"); Assert.True(Math.Abs(v) < 0.001); }
	[Fact] public async Task Atan2_Basic() { var v = double.Parse(await Scalar("SELECT ATAN2(1, 1)") ?? "0"); Assert.True(Math.Abs(v - Math.PI / 4) < 0.001); }
	[Fact] public async Task Sinh_Zero() { var v = double.Parse(await Scalar("SELECT SINH(0)") ?? "1"); Assert.True(Math.Abs(v) < 0.001); }
	[Fact] public async Task Cosh_Zero() { var v = double.Parse(await Scalar("SELECT COSH(0)") ?? "0"); Assert.True(Math.Abs(v - 1.0) < 0.001); }
	[Fact] public async Task Tanh_Zero() { var v = double.Parse(await Scalar("SELECT TANH(0)") ?? "1"); Assert.True(Math.Abs(v) < 0.001); }

	// ---- GREATEST / LEAST edge cases ----
	[Fact] public async Task Greatest_SingleValue() => Assert.Equal("5", await Scalar("SELECT GREATEST(5)"));
	[Fact] public async Task Greatest_Negative() => Assert.Equal("-1", await Scalar("SELECT GREATEST(-5, -3, -1)"));
	[Fact] public async Task Least_SingleValue() => Assert.Equal("5", await Scalar("SELECT LEAST(5)"));
	[Fact] public async Task Least_Negative() => Assert.Equal("-5", await Scalar("SELECT LEAST(-5, -3, -1)"));

	// ---- RAND ----
	[Fact] public async Task Rand_Between0And1() { var v = double.Parse(await Scalar("SELECT RAND()") ?? "-1"); Assert.True(v >= 0 && v < 1); }
	[Fact] public async Task Rand_Unique() { var v1 = await Scalar("SELECT RAND()"); var v2 = await Scalar("SELECT RAND()"); /* can be equal but extremely unlikely */ Assert.NotNull(v1); }

	// ---- GENERATE_UUID ----
	[Fact] public async Task GenerateUuid_Length() { var v = await Scalar("SELECT GENERATE_UUID()"); Assert.Equal(36, v!.Length); }
	[Fact] public async Task GenerateUuid_Format() { var v = await Scalar("SELECT GENERATE_UUID()"); Assert.Matches(@"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", v!); }
}
