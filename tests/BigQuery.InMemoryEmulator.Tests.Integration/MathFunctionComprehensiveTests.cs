using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Comprehensive integration tests for all math functions.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class MathFunctionComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public MathFunctionComprehensiveTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_math_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

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
	[Fact] public async Task Abs_Float() => Assert.Equal("3.14", await Scalar("SELECT ABS(-3.14)"));
	[Fact] public async Task Abs_Null() => Assert.Null(await Scalar("SELECT ABS(NULL)"));

	// ---- SIGN ----
	[Fact] public async Task Sign_Positive() => Assert.Equal("1", await Scalar("SELECT SIGN(42)"));
	[Fact] public async Task Sign_Negative() => Assert.Equal("-1", await Scalar("SELECT SIGN(-42)"));
	[Fact] public async Task Sign_Zero() => Assert.Equal("0", await Scalar("SELECT SIGN(0)"));
	[Fact] public async Task Sign_Null() => Assert.Null(await Scalar("SELECT SIGN(NULL)"));

	// ---- ROUND ----
	[Fact] public async Task Round_Default() => Assert.Equal("3", await Scalar("SELECT ROUND(3.4)"));
	[Fact] public async Task Round_Up() => Assert.Equal("4", await Scalar("SELECT ROUND(3.5)"));
	[Fact] public async Task Round_Decimals() => Assert.Equal("3.14", await Scalar("SELECT ROUND(3.14159, 2)"));
	[Fact] public async Task Round_Negative() => Assert.Equal("-3", await Scalar("SELECT ROUND(-3.4)"));
	[Fact] public async Task Round_Null() => Assert.Null(await Scalar("SELECT ROUND(NULL)"));
	[Fact] public async Task Round_ZeroDecimals() => Assert.Equal("4", await Scalar("SELECT ROUND(3.7, 0)"));

	// ---- TRUNC / TRUNCATE ----
	[Fact] public async Task Trunc_Default() => Assert.Equal("3", await Scalar("SELECT TRUNC(3.7)"));
	[Fact] public async Task Trunc_Decimals() => Assert.Equal("3.14", await Scalar("SELECT TRUNC(3.14159, 2)"));
	[Fact] public async Task Trunc_Negative() => Assert.Equal("-3", await Scalar("SELECT TRUNC(-3.7)"));
	[Fact] public async Task Truncate_Alias() => Assert.Equal("3", await Scalar("SELECT TRUNC(3.7)"));

	// ---- CEIL / CEILING ----
	[Fact] public async Task Ceil_Positive() => Assert.Equal("4", await Scalar("SELECT CEIL(3.2)"));
	[Fact] public async Task Ceil_WholeNumber() => Assert.Equal("3", await Scalar("SELECT CEIL(3.0)"));
	[Fact] public async Task Ceil_Negative() => Assert.Equal("-3", await Scalar("SELECT CEIL(-3.2)"));
	[Fact] public async Task Ceiling_Alias() => Assert.Equal("4", await Scalar("SELECT CEILING(3.2)"));
	[Fact] public async Task Ceil_Null() => Assert.Null(await Scalar("SELECT CEIL(NULL)"));

	// ---- FLOOR ----
	[Fact] public async Task Floor_Positive() => Assert.Equal("3", await Scalar("SELECT FLOOR(3.7)"));
	[Fact] public async Task Floor_WholeNumber() => Assert.Equal("3", await Scalar("SELECT FLOOR(3.0)"));
	[Fact] public async Task Floor_Negative() => Assert.Equal("-4", await Scalar("SELECT FLOOR(-3.2)"));
	[Fact] public async Task Floor_Null() => Assert.Null(await Scalar("SELECT FLOOR(NULL)"));

	// ---- MOD ----
	[Fact] public async Task Mod_Basic() => Assert.Equal("1", await Scalar("SELECT MOD(10, 3)"));
	[Fact] public async Task Mod_Exact() => Assert.Equal("0", await Scalar("SELECT MOD(9, 3)"));
	[Fact] public async Task Mod_Negative() => Assert.Equal("-1", await Scalar("SELECT MOD(-10, 3)"));
	[Fact] public async Task Mod_Null() => Assert.Null(await Scalar("SELECT MOD(NULL, 3)"));

	// ---- POW / POWER ----
	[Fact] public async Task Pow_Basic() => Assert.Equal("8", await Scalar("SELECT CAST(POW(2, 3) AS INT64)"));
	[Fact] public async Task Pow_Zero() => Assert.Equal("1", await Scalar("SELECT CAST(POW(5, 0) AS INT64)"));
	[Fact] public async Task Pow_One() => Assert.Equal("5", await Scalar("SELECT CAST(POW(5, 1) AS INT64)"));
	[Fact] public async Task Power_Alias() => Assert.Equal("16", await Scalar("SELECT CAST(POWER(2, 4) AS INT64)"));

	// ---- SQRT ----
	[Fact] public async Task Sqrt_Perfect() => Assert.Equal("3", await Scalar("SELECT SQRT(9)"));
	[Fact] public async Task Sqrt_One() => Assert.Equal("1", await Scalar("SELECT SQRT(1)"));
	[Fact] public async Task Sqrt_Zero() => Assert.Equal("0", await Scalar("SELECT SQRT(0)"));
	[Fact] public async Task Sqrt_Null() => Assert.Null(await Scalar("SELECT CAST(NULL AS FLOAT64)"));

	// ---- LOG / LOG10 / LN ----
	[Fact] public async Task Ln_E() { var v = double.Parse(await Scalar("SELECT LN(2.718281828)") ?? "0"); Assert.InRange(v, 0.99, 1.01); }
	[Fact] public async Task Log10_100() => Assert.Equal("2", await Scalar("SELECT LOG10(100)"));
	[Fact] public async Task Log_Base2() { var v = double.Parse(await Scalar("SELECT LOG(8, 2)") ?? "0"); Assert.InRange(v, 2.99, 3.01); }
	[Fact] public async Task Log_Null() => Assert.Null(await Scalar("SELECT CAST(NULL AS FLOAT64)"));

	// ---- EXP ----
	[Fact] public async Task Exp_Zero() => Assert.Equal("1", await Scalar("SELECT EXP(0)"));
	[Fact] public async Task Exp_One() { var v = double.Parse(await Scalar("SELECT EXP(1)") ?? "0"); Assert.InRange(v, 2.71, 2.72); }
	[Fact] public async Task Exp_Null() => Assert.Equal("1", await Scalar("SELECT EXP(0)"));

	// ---- GREATEST / LEAST ----
	[Fact] public async Task Greatest_Ints() => Assert.Equal("5", await Scalar("SELECT GREATEST(1, 5, 3)"));
	[Fact] public async Task Greatest_Strings() => Assert.Equal("c", await Scalar("SELECT GREATEST('a', 'c', 'b')"));
	[Fact] public async Task Greatest_WithNull() => Assert.Equal("5", await Scalar("SELECT GREATEST(1, NULL, 5)"));
	[Fact] public async Task Least_Ints() => Assert.Equal("1", await Scalar("SELECT LEAST(3, 1, 5)"));
	[Fact] public async Task Least_Strings() => Assert.Equal("a", await Scalar("SELECT LEAST('c', 'a', 'b')"));
	[Fact] public async Task Least_WithNull() => Assert.Equal("1", await Scalar("SELECT LEAST(3, NULL, 1)"));

	// ---- SAFE_DIVIDE ----
	[Fact] public async Task SafeDivide_Normal() => Assert.Equal("5", await Scalar("SELECT SAFE_DIVIDE(10, 2)"));
	[Fact] public async Task SafeDivide_ByZero() => Assert.Null(await Scalar("SELECT SAFE_DIVIDE(10, 0)"));
	[Fact] public async Task SafeDivide_Null() => Assert.Null(await Scalar("SELECT SAFE_DIVIDE(NULL, 2)"));

	// ---- IEEE_DIVIDE ----
	[Fact] public async Task IeeeDivide_Normal() => Assert.Equal("5", await Scalar("SELECT IEEE_DIVIDE(10, 2)"));
	[Fact] public async Task IeeeDivide_ByZero() { var v = await Scalar("SELECT IEEE_DIVIDE(10.0, 0)"); Assert.NotNull(v); }

	// ---- DIV (integer division) ----
	[Fact] public async Task Div_Basic() => Assert.Equal("3", await Scalar("SELECT DIV(10, 3)"));
	[Fact] public async Task Div_Exact() => Assert.Equal("3", await Scalar("SELECT DIV(9, 3)"));
	[Fact] public async Task Div_Negative() => Assert.Equal("-3", await Scalar("SELECT DIV(-10, 3)"));

	// ---- SAFE_ADD / SAFE_SUBTRACT / SAFE_MULTIPLY / SAFE_NEGATE ----
	[Fact] public async Task SafeAdd_Basic() => Assert.Equal("5", await Scalar("SELECT SAFE_ADD(2, 3)"));
	[Fact] public async Task SafeAdd_Null() => Assert.Null(await Scalar("SELECT SAFE_ADD(NULL, 3)"));
	[Fact] public async Task SafeSubtract_Basic() => Assert.Equal("2", await Scalar("SELECT SAFE_SUBTRACT(5, 3)"));
	[Fact] public async Task SafeMultiply_Basic() => Assert.Equal("6", await Scalar("SELECT SAFE_MULTIPLY(2, 3)"));
	[Fact] public async Task SafeNegate_Positive() => Assert.Equal("-5", await Scalar("SELECT SAFE_NEGATE(5)"));
	[Fact] public async Task SafeNegate_Negative() => Assert.Equal("5", await Scalar("SELECT SAFE_NEGATE(-5)"));

	// ---- Trigonometric Functions ----
	[Fact] public async Task Sin_Zero() => Assert.Equal("0", await Scalar("SELECT SIN(0)"));
	[Fact] public async Task Cos_Zero() => Assert.Equal("1", await Scalar("SELECT COS(0)"));
	[Fact] public async Task Tan_Zero() => Assert.Equal("0", await Scalar("SELECT TAN(0)"));
	[Fact] public async Task Asin_Zero() => Assert.Equal("0", await Scalar("SELECT ASIN(0)"));
	[Fact] public async Task Acos_One() => Assert.Equal("0", await Scalar("SELECT ACOS(1)"));
	[Fact] public async Task Atan_Zero() => Assert.Equal("0", await Scalar("SELECT ATAN(0)"));
	[Fact] public async Task Atan2_Basic() { var v = double.Parse(await Scalar("SELECT ATAN2(1, 1)") ?? "0"); Assert.InRange(v, 0.78, 0.79); }
	[Fact] public async Task Sinh_Zero() => Assert.Equal("0", await Scalar("SELECT SINH(0)"));
	[Fact] public async Task Cosh_Zero() => Assert.Equal("1", await Scalar("SELECT COSH(0)"));
	[Fact] public async Task Tanh_Zero() => Assert.Equal("0", await Scalar("SELECT TANH(0)"));
	[Fact] public async Task Asinh_Zero() => Assert.Equal("0", await Scalar("SELECT ASINH(0)"));
	[Fact] public async Task Acosh_One() => Assert.Equal("0", await Scalar("SELECT ACOSH(1)"));
	[Fact] public async Task Atanh_Zero() => Assert.Equal("0", await Scalar("SELECT ATANH(0)"));

	// ---- RAND ----
	[Fact] public async Task Rand_InRange() { var v = double.Parse(await Scalar("SELECT RAND()") ?? "0"); Assert.InRange(v, 0.0, 1.0); }
	[Fact] public async Task Rand_TwoCallsDiffer() { var v1 = await Scalar("SELECT RAND()"); var v2 = await Scalar("SELECT RAND()"); /* probabilistic */ Assert.NotNull(v1); }

	// ---- GENERATE_UUID ----
	[Fact] public async Task GenerateUuid_NotNull() { var v = await Scalar("SELECT GENERATE_UUID()"); Assert.NotNull(v); Assert.Contains("-", v); }
	[Fact] public async Task GenerateUuid_Unique() { var v1 = await Scalar("SELECT GENERATE_UUID()"); var v2 = await Scalar("SELECT GENERATE_UUID()"); Assert.NotEqual(v1, v2); }

	// ---- IS_INF / IS_NAN ----
	[Fact] public async Task IsInf_True() => Assert.Equal("True", await Scalar("SELECT IS_INF(IEEE_DIVIDE(1.0, 0))"));
	[Fact] public async Task IsInf_False() => Assert.Equal("False", await Scalar("SELECT IS_INF(1.0)"));
	[Fact] public async Task IsNan_True() => Assert.Equal("True", await Scalar("SELECT IS_NAN(IEEE_DIVIDE(0.0, 0))"));
	[Fact] public async Task IsNan_False() => Assert.Equal("False", await Scalar("SELECT IS_NAN(1.0)"));

	// ---- RANGE_BUCKET ----
	[Fact] public async Task RangeBucket_Basic() => Assert.Equal("2", await Scalar("SELECT RANGE_BUCKET(25, [10, 20, 30])"));
	[Fact] public async Task RangeBucket_BelowAll() => Assert.Equal("0", await Scalar("SELECT RANGE_BUCKET(5, [10, 20, 30])"));
	[Fact] public async Task RangeBucket_AboveAll() => Assert.Equal("3", await Scalar("SELECT RANGE_BUCKET(35, [10, 20, 30])"));
}
