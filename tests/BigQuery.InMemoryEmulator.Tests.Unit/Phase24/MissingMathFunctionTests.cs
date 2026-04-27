using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase24;

/// <summary>
/// Phase 24: Missing math functions.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions
/// </summary>
public class MissingMathFunctionTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		return new QueryExecutor(store);
	}

	#region Trigonometric Functions

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#sin
	//   "Computes the sine of X where X is specified in radians. Never fails."
	[Theory]
	[InlineData("SELECT SIN(0)", "0")]
	public void Sin_Zero(string sql, string expected)
	{
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal(expected, rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Sin_PiOver2()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT SIN(ACOS(-1) / 2)");
		var val = Convert.ToDouble(rows[0].F[0].V);
		Assert.InRange(val, 0.999, 1.001);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#cos
	//   "Computes the cosine of X where X is specified in radians. Never fails."
	[Theory]
	[InlineData("SELECT COS(0)", "1")]
	public void Cos_Zero(string sql, string expected)
	{
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal(expected, rows[0].F[0].V?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#tan
	//   "Computes the tangent of X where X is specified in radians."
	[Fact]
	public void Tan_Zero()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT TAN(0)");
		Assert.Equal("0", rows[0].F[0].V?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#acos
	//   "Computes the principal value of the inverse cosine of X. Return value in [0,π]."
	[Fact]
	public void Acos_One()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ACOS(1)");
		Assert.Equal("0", rows[0].F[0].V?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#asin
	//   "Computes the principal value of the inverse sine of X. Return value in [-π/2,π/2]."
	[Fact]
	public void Asin_Zero()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ASIN(0)");
		Assert.Equal("0", rows[0].F[0].V?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#atan
	//   "Computes the principal value of the inverse tangent of X."
	[Fact]
	public void Atan_Zero()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ATAN(0)");
		Assert.Equal("0", rows[0].F[0].V?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#atan2
	//   "Calculates the principal value of the inverse tangent of X/Y using the signs."
	[Fact]
	public void Atan2_Basic()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ATAN2(1, 1)");
		var val = Convert.ToDouble(rows[0].F[0].V);
		Assert.InRange(val, 0.785, 0.786); // π/4 ≈ 0.7854
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#sinh
	//   "Computes the hyperbolic sine of X."
	[Fact]
	public void Sinh_Zero()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT SINH(0)");
		Assert.Equal("0", rows[0].F[0].V?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#cosh
	//   "Computes the hyperbolic cosine of X."
	[Fact]
	public void Cosh_Zero()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT COSH(0)");
		Assert.Equal("1", rows[0].F[0].V?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#tanh
	//   "Computes the hyperbolic tangent of X. Doesn't fail."
	[Fact]
	public void Tanh_Zero()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT TANH(0)");
		Assert.Equal("0", rows[0].F[0].V?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#acosh
	//   "Computes the inverse hyperbolic cosine of X."
	[Fact]
	public void Acosh_One()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ACOSH(1)");
		Assert.Equal("0", rows[0].F[0].V?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#asinh
	//   "Computes the inverse hyperbolic sine of X."
	[Fact]
	public void Asinh_Zero()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ASINH(0)");
		Assert.Equal("0", rows[0].F[0].V?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#atanh
	//   "Computes the inverse hyperbolic tangent of X."
	[Fact]
	public void Atanh_Zero()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ATANH(0)");
		Assert.Equal("0", rows[0].F[0].V?.ToString());
	}

	// All trig functions: NULL → NULL
	[Theory]
	[InlineData("SELECT SIN(NULL)")]
	[InlineData("SELECT COS(NULL)")]
	[InlineData("SELECT TAN(NULL)")]
	[InlineData("SELECT ACOS(NULL)")]
	[InlineData("SELECT ASIN(NULL)")]
	[InlineData("SELECT ATAN(NULL)")]
	[InlineData("SELECT SINH(NULL)")]
	[InlineData("SELECT COSH(NULL)")]
	[InlineData("SELECT TANH(NULL)")]
	[InlineData("SELECT ACOSH(NULL)")]
	[InlineData("SELECT ASINH(NULL)")]
	[InlineData("SELECT ATANH(NULL)")]
	[InlineData("SELECT ATAN2(NULL, 1)")]
	public void Trig_Null_ReturnsNull(string sql)
	{
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region IS_INF / IS_NAN

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#is_inf
	//   "Returns TRUE if the value is positive or negative infinity."
	[Fact]
	public void IsInf_PositiveInfinity()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT IS_INF(IEEE_DIVIDE(1, 0))");
		Assert.Equal("True", rows[0].F[0].V?.ToString(), StringComparer.OrdinalIgnoreCase);
	}

	[Fact]
	public void IsInf_FiniteValue()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT IS_INF(25)");
		Assert.Equal("False", rows[0].F[0].V?.ToString(), StringComparer.OrdinalIgnoreCase);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#is_nan
	//   "Returns TRUE if the value is a NaN value."
	[Fact]
	public void IsNan_NaN()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT IS_NAN(IEEE_DIVIDE(0, 0))");
		Assert.Equal("True", rows[0].F[0].V?.ToString(), StringComparer.OrdinalIgnoreCase);
	}

	[Fact]
	public void IsNan_FiniteValue()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT IS_NAN(25)");
		Assert.Equal("False", rows[0].F[0].V?.ToString(), StringComparer.OrdinalIgnoreCase);
	}

	#endregion

	#region SAFE_ADD / SAFE_MULTIPLY / SAFE_NEGATE / SAFE_SUBTRACT

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#safe_add
	//   "Equivalent to (+), but returns NULL if overflow occurs."
	[Fact]
	public void SafeAdd_Normal()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT SAFE_ADD(5, 4)");
		Assert.Equal("9", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void SafeAdd_Null_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT SAFE_ADD(NULL, 4)");
		Assert.Null(rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#safe_subtract
	//   "Equivalent to (-), but returns NULL if overflow occurs."
	[Fact]
	public void SafeSubtract_Normal()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT SAFE_SUBTRACT(5, 4)");
		Assert.Equal("1", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void SafeSubtract_Null_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT SAFE_SUBTRACT(NULL, 4)");
		Assert.Null(rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#safe_multiply
	//   "Equivalent to (*), but returns NULL if overflow occurs."
	[Fact]
	public void SafeMultiply_Normal()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT SAFE_MULTIPLY(20, 4)");
		Assert.Equal("80", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void SafeMultiply_Null_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT SAFE_MULTIPLY(NULL, 4)");
		Assert.Null(rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#safe_negate
	//   "Equivalent to unary minus (-), but returns NULL if overflow occurs."
	[Fact]
	public void SafeNegate_Normal()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT SAFE_NEGATE(1)");
		Assert.Equal("-1", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void SafeNegate_Null_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT SAFE_NEGATE(NULL)");
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region RANGE_BUCKET

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#range_bucket
	//   RANGE_BUCKET(20, [0, 10, 20, 30, 40]) => 3
	[Theory]
	[InlineData("SELECT RANGE_BUCKET(20, [0, 10, 20, 30, 40])", "3")]
	[InlineData("SELECT RANGE_BUCKET(25, [0, 10, 20, 30, 40])", "3")]
	[InlineData("SELECT RANGE_BUCKET(-10, [5, 10, 20, 30, 40])", "0")]
	[InlineData("SELECT RANGE_BUCKET(80, [0, 10, 20, 30, 40])", "5")]
	public void RangeBucket_Basic(string sql, string expected)
	{
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal(expected, rows[0].F[0].V?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#range_bucket
	//   "If the array is empty, returns 0."
	[Fact]
	public void RangeBucket_EmptyArray()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT RANGE_BUCKET(80, [])");
		Assert.Equal("0", rows[0].F[0].V?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#range_bucket
	//   "If the point is NULL or NaN, returns NULL."
	[Fact]
	public void RangeBucket_NullPoint_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT RANGE_BUCKET(NULL, [0, 10, 20])");
		Assert.Null(rows[0].F[0].V);
	}

	#endregion
}
