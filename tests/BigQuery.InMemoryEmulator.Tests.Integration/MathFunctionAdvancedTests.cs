using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for math edge cases: IEEE_DIVIDE, SAFE_* arithmetic,
/// RANGE_BUCKET, IS_INF, IS_NAN, BIT_COUNT, GENERATE_UUID, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class MathFunctionAdvancedTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public MathFunctionAdvancedTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_mtha_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- IEEE_DIVIDE ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#ieee_divide
	[Fact] public async Task IeeeDivide_Normal()
	{
		var v = await S("SELECT IEEE_DIVIDE(10, 3)");
		Assert.NotNull(v);
		Assert.StartsWith("3.33", v);
	}
	[Fact] public async Task IeeeDivide_ByZero()
	{
		var v = await S("SELECT CAST(IEEE_DIVIDE(1, 0) AS STRING)");
		Assert.Equal("inf", v);
	}
	[Fact] public async Task IeeeDivide_NegByZero()
	{
		var v = await S("SELECT CAST(IEEE_DIVIDE(-1, 0) AS STRING)");
		Assert.Equal("-inf", v);
	}
	[Fact] public async Task IeeeDivide_ZeroByZero()
	{
		var v = await S("SELECT IEEE_DIVIDE(0, 0)");
		Assert.Equal("NaN", v);
	}
	[Fact] public async Task IeeeDivide_Null()
	{
		var v = await S("SELECT IEEE_DIVIDE(NULL, 1)");
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#ieee_divide
		//   "If one of the input values is NULL, the result is NULL."
		Assert.Null(v);
	}

	// ---- SAFE_DIVIDE ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#safe_divide
	[Fact] public async Task SafeDivide_Normal() => Assert.Equal("5", await S("SELECT SAFE_DIVIDE(10, 2)"));
	[Fact] public async Task SafeDivide_ByZero() => Assert.Null(await S("SELECT SAFE_DIVIDE(10, 0)"));
	[Fact] public async Task SafeDivide_ZeroByZero() => Assert.Null(await S("SELECT SAFE_DIVIDE(0, 0)"));

	// ---- SAFE_ADD ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#safe_add
	[Fact] public async Task SafeAdd_Normal() => Assert.Equal("15", await S("SELECT SAFE_ADD(10, 5)"));
	[Fact] public async Task SafeAdd_Null() => Assert.Null(await S("SELECT SAFE_ADD(NULL, 5)"));

	// ---- SAFE_SUBTRACT ----
	[Fact] public async Task SafeSubtract_Normal() => Assert.Equal("5", await S("SELECT SAFE_SUBTRACT(10, 5)"));
	[Fact] public async Task SafeSubtract_Null() => Assert.Null(await S("SELECT SAFE_SUBTRACT(NULL, 5)"));

	// ---- SAFE_MULTIPLY ----
	[Fact] public async Task SafeMultiply_Normal() => Assert.Equal("50", await S("SELECT SAFE_MULTIPLY(10, 5)"));
	[Fact] public async Task SafeMultiply_Null() => Assert.Null(await S("SELECT SAFE_MULTIPLY(NULL, 5)"));

	// ---- SAFE_NEGATE ----
	[Fact] public async Task SafeNegate_Positive() => Assert.Equal("-5", await S("SELECT SAFE_NEGATE(5)"));
	[Fact] public async Task SafeNegate_Negative() => Assert.Equal("5", await S("SELECT SAFE_NEGATE(-5)"));
	[Fact] public async Task SafeNegate_Null() => Assert.Null(await S("SELECT SAFE_NEGATE(NULL)"));

	// ---- IS_INF ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#is_inf
	[Fact] public async Task IsInf_Infinity() => Assert.Equal("True", await S("SELECT IS_INF(IEEE_DIVIDE(1, 0))"));
	[Fact] public async Task IsInf_NegInfinity() => Assert.Equal("True", await S("SELECT IS_INF(IEEE_DIVIDE(-1, 0))"));
	[Fact] public async Task IsInf_Normal() => Assert.Equal("False", await S("SELECT IS_INF(1.0)"));
	[Fact] public async Task IsInf_NaN() => Assert.Equal("False", await S("SELECT IS_INF(IEEE_DIVIDE(0, 0))"));

	// ---- IS_NAN ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#is_nan
	[Fact] public async Task IsNan_NaN() => Assert.Equal("True", await S("SELECT IS_NAN(IEEE_DIVIDE(0, 0))"));
	[Fact] public async Task IsNan_Normal() => Assert.Equal("False", await S("SELECT IS_NAN(1.0)"));
	[Fact] public async Task IsNan_Infinity() => Assert.Equal("False", await S("SELECT IS_NAN(IEEE_DIVIDE(1, 0))"));

	// ---- BIT_COUNT ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/bit_functions#bit_count
	[Fact] public async Task BitCount_Zero() => Assert.Equal("0", await S("SELECT BIT_COUNT(0)"));
	[Fact] public async Task BitCount_One() => Assert.Equal("1", await S("SELECT BIT_COUNT(1)"));
	[Fact] public async Task BitCount_Seven() => Assert.Equal("3", await S("SELECT BIT_COUNT(7)")); // 111 in binary
	[Fact] public async Task BitCount_255() => Assert.Equal("8", await S("SELECT BIT_COUNT(255)")); // 11111111
	[Fact] public async Task BitCount_Null() => Assert.Null(await S("SELECT BIT_COUNT(NULL)"));

	// ---- RANGE_BUCKET ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#range_bucket
	[Fact] public async Task RangeBucket_InRange() => Assert.Equal("2", await S("SELECT RANGE_BUCKET(15, [0, 10, 20, 30])"));
	[Fact] public async Task RangeBucket_BelowAll() => Assert.Equal("0", await S("SELECT RANGE_BUCKET(-5, [0, 10, 20])"));
	[Fact] public async Task RangeBucket_AboveAll() => Assert.Equal("3", await S("SELECT RANGE_BUCKET(100, [0, 10, 20])"));
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#range_bucket
	//   Returns the index of the first element > point. For value=20, boundaries=[0,10,20,30], first > 20 is 30 at index 3.
	[Fact] public async Task RangeBucket_ExactBoundary() => Assert.Equal("3", await S("SELECT RANGE_BUCKET(20, [0, 10, 20, 30])"));
	[Fact] public async Task RangeBucket_Null() => Assert.Null(await S("SELECT RANGE_BUCKET(NULL, [0, 10, 20])"));

	// ---- GENERATE_UUID ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-all#generate_uuid
	[Fact] public async Task GenerateUuid_NotNull()
	{
		var v = await S("SELECT GENERATE_UUID()");
		Assert.NotNull(v);
		Assert.Matches(@"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", v);
	}
	[Fact] public async Task GenerateUuid_TwoCalls_Differ()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync("SELECT GENERATE_UUID() AS a, GENERATE_UUID() AS b", parameters: null);
		var rows = result.ToList();
		Assert.NotEqual(rows[0]["a"]?.ToString(), rows[0]["b"]?.ToString());
	}

	// ---- DIV (integer division) ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#div
	[Fact] public async Task Div_Normal() => Assert.Equal("3", await S("SELECT DIV(10, 3)"));
	[Fact] public async Task Div_Negative() => Assert.Equal("-3", await S("SELECT DIV(-10, 3)"));
	[Fact] public async Task Div_Exact() => Assert.Equal("5", await S("SELECT DIV(10, 2)"));

	// ---- MOD ----
	[Fact] public async Task Mod_Normal() => Assert.Equal("1", await S("SELECT MOD(10, 3)"));
	[Fact] public async Task Mod_Zero() => Assert.Equal("0", await S("SELECT MOD(10, 5)"));
	[Fact] public async Task Mod_Negative() => Assert.Equal("-1", await S("SELECT MOD(-10, 3)"));

	// ---- POW edge cases ----
	[Fact] public async Task Pow_ZeroExponent() => Assert.Equal("1", await S("SELECT POW(5, 0)"));
	[Fact] public async Task Pow_OneExponent() => Assert.Equal("5", await S("SELECT POW(5, 1)"));
	[Fact] public async Task Pow_Fractional()
	{
		var v = await S("SELECT ROUND(POW(2, 0.5), 4)");
		Assert.NotNull(v);
		Assert.StartsWith("1.414", v);
	}

	// ---- LOG edge cases ----
	[Fact] public async Task Log_Base2()
	{
		var v = await S("SELECT LOG(8, 2)");
		Assert.NotNull(v);
		Assert.Equal("3", v?.Split('.')[0]);
	}
	[Fact] public async Task Log10_100() => Assert.Equal("2", await S("SELECT CAST(LOG10(100) AS INT64)"));
	[Fact] public async Task Ln_E()
	{
		var v = await S("SELECT ROUND(LN(EXP(1)), 5)");
		Assert.NotNull(v);
		Assert.Equal("1", v?.Split('.')[0]);
	}

	// ---- Trig functions ----
	[Fact] public async Task Sin_Zero() => Assert.Equal("0", await S("SELECT CAST(SIN(0) AS INT64)"));
	[Fact] public async Task Cos_Zero() => Assert.Equal("1", await S("SELECT CAST(COS(0) AS INT64)"));
	[Fact] public async Task Tan_Zero() => Assert.Equal("0", await S("SELECT CAST(TAN(0) AS INT64)"));
	[Fact] public async Task Atan2_Basic()
	{
		var v = await S("SELECT ROUND(ATAN2(1, 1), 4)");
		Assert.NotNull(v);
		Assert.StartsWith("0.785", v); // pi/4 ≈ 0.7854
	}

	// ---- Hyperbolic ----
	[Fact] public async Task Sinh_Zero() => Assert.Equal("0", await S("SELECT CAST(SINH(0) AS INT64)"));
	[Fact] public async Task Cosh_Zero() => Assert.Equal("1", await S("SELECT CAST(COSH(0) AS INT64)"));
	[Fact] public async Task Tanh_Zero() => Assert.Equal("0", await S("SELECT CAST(TANH(0) AS INT64)"));

	// ---- ROUND/TRUNC/CEIL/FLOOR extras ----
	[Fact] public async Task Round_NegativeDigits() => Assert.Equal("1200", await S("SELECT CAST(ROUND(1234, -2) AS INT64)"));
	[Fact] public async Task Trunc_Positive() => Assert.Equal("3", await S("SELECT TRUNC(3.7)"));
	[Fact] public async Task Trunc_Negative() => Assert.Equal("-3", await S("SELECT TRUNC(-3.7)"));
	[Fact] public async Task Ceil_Negative() => Assert.Equal("-3", await S("SELECT CEIL(-3.1)"));
	[Fact] public async Task Floor_Positive() => Assert.Equal("3", await S("SELECT FLOOR(3.9)"));
	[Fact] public async Task Floor_Negative() => Assert.Equal("-4", await S("SELECT FLOOR(-3.1)"));

	// ---- GREATEST/LEAST with mixed types ----
	[Fact] public async Task Greatest_Ints() => Assert.Equal("5", await S("SELECT GREATEST(1, 5, 3)"));
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#greatest
	//   "Returns NULL if any of the inputs is NULL."
	[Fact] public async Task Greatest_WithNull() => Assert.Null(await S("SELECT GREATEST(1, NULL, 3)"));
	[Fact] public async Task Least_Ints() => Assert.Equal("1", await S("SELECT LEAST(1, 5, 3)"));
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#least
	//   "Returns NULL if any of the inputs is NULL."
	[Fact] public async Task Least_WithNull() => Assert.Null(await S("SELECT LEAST(1, NULL, 3)"));
	[Fact] public async Task Greatest_Strings() => Assert.Equal("c", await S("SELECT GREATEST('a', 'c', 'b')"));
	[Fact] public async Task Least_Strings() => Assert.Equal("a", await S("SELECT LEAST('a', 'c', 'b')"));

	// ---- RAND ----
	[Fact] public async Task Rand_Range()
	{
		var v = await S("SELECT RAND()");
		Assert.NotNull(v);
		var d = double.Parse(v);
		Assert.InRange(d, 0.0, 1.0);
	}

	// ---- ERROR function ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/debugging_functions#error
	[Fact] public async Task Error_ThrowsWithMessage()
	{
		var client = await _fixture.GetClientAsync();
		var ex = await Assert.ThrowsAnyAsync<Exception>(() =>
			client.ExecuteQueryAsync("SELECT ERROR('test error message')", parameters: null));
		Assert.Contains("test error message", ex.Message);
	}

	// ---- SESSION_USER ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-all#session_user
	[Fact] public async Task SessionUser_NotNull()
	{
		var v = await S("SELECT SESSION_USER()");
		Assert.NotNull(v);
	}

	// ---- PARSE_NUMERIC ----
	[Fact] public async Task ParseNumeric_Integer() => Assert.Equal("123", await S("SELECT CAST(PARSE_NUMERIC('123') AS INT64)"));
	[Fact] public async Task ParseNumeric_Decimal()
	{
		var v = await S("SELECT PARSE_NUMERIC('3.14')");
		Assert.NotNull(v);
		Assert.StartsWith("3.14", v);
	}

	// ---- PARSE_BIGNUMERIC ----
	[Fact] public async Task ParseBignumeric_Basic()
	{
		var v = await S("SELECT PARSE_BIGNUMERIC('12345.6789')");
		Assert.NotNull(v);
		Assert.Contains("12345", v);
	}

	// ---- Arithmetic operators ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#div
	//   "Returns the result of integer division of X by Y."
	[Fact] public async Task Arithmetic_IntegerDivision() => Assert.Equal("3", await S("SELECT DIV(10, 3)"));
	[Fact] public async Task Arithmetic_UnaryMinus() => Assert.Equal("-5", await S("SELECT -5"));
	[Fact] public async Task Arithmetic_Precedence() => Assert.Equal("14", await S("SELECT 2 + 3 * 4")); // 2 + 12 = 14
	[Fact] public async Task Arithmetic_Parentheses() => Assert.Equal("20", await S("SELECT (2 + 3) * 4")); // 5 * 4 = 20
}
