using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for advanced math functions and numeric operations.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class MathFunctionPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public MathFunctionPatternTests(BigQuerySession session) => _session = session;

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

	// Basic arithmetic
	[Fact] public async Task Add_Integers() => Assert.Equal("15", await Scalar("SELECT 10 + 5"));
	[Fact] public async Task Subtract_Integers() => Assert.Equal("5", await Scalar("SELECT 10 - 5"));
	[Fact] public async Task Multiply_Integers() => Assert.Equal("50", await Scalar("SELECT 10 * 5"));
	[Fact] public async Task Divide_Integers() => Assert.Equal("2", await Scalar("SELECT 10 / 5"));
	[Fact] public async Task Modulo_Integers() => Assert.Equal("1", await Scalar("SELECT MOD(10, 3)"));
	[Fact] public async Task Negate() => Assert.Equal("-5", await Scalar("SELECT -5"));
	[Fact] public async Task Parentheses() => Assert.Equal("20", await Scalar("SELECT (2 + 3) * 4"));

	// ABS
	[Fact] public async Task Abs_Positive() => Assert.Equal("5", await Scalar("SELECT ABS(5)"));
	[Fact] public async Task Abs_Negative() => Assert.Equal("5", await Scalar("SELECT ABS(-5)"));
	[Fact] public async Task Abs_Zero() => Assert.Equal("0", await Scalar("SELECT ABS(0)"));
	[Fact] public async Task Abs_Float() => Assert.Equal("3.14", await Scalar("SELECT ABS(-3.14)"));

	// SIGN
	[Fact] public async Task Sign_Positive() => Assert.Equal("1", await Scalar("SELECT SIGN(42)"));
	[Fact] public async Task Sign_Negative() => Assert.Equal("-1", await Scalar("SELECT SIGN(-7)"));
	[Fact] public async Task Sign_Zero() => Assert.Equal("0", await Scalar("SELECT SIGN(0)"));

	// ROUND / TRUNC / FLOOR / CEIL
	[Fact] public async Task Round_Default() => Assert.Equal("3", await Scalar("SELECT ROUND(3.4)"));
	[Fact] public async Task Round_Up() => Assert.Equal("4", await Scalar("SELECT ROUND(3.5)"));
	[Fact] public async Task Round_Precision() => Assert.Equal("3.14", await Scalar("SELECT ROUND(3.14159, 2)"));
	[Fact] public async Task Trunc_Positive() => Assert.Equal("3", await Scalar("SELECT TRUNC(3.7)"));
	[Fact] public async Task Trunc_Negative() => Assert.Equal("-3", await Scalar("SELECT TRUNC(-3.7)"));
	[Fact] public async Task Floor_Positive() => Assert.Equal("3", await Scalar("SELECT FLOOR(3.7)"));
	[Fact] public async Task Floor_Negative() => Assert.Equal("-4", await Scalar("SELECT FLOOR(-3.2)"));
	[Fact] public async Task Ceil_Positive() => Assert.Equal("4", await Scalar("SELECT CEIL(3.2)"));
	[Fact] public async Task Ceil_Negative() => Assert.Equal("-3", await Scalar("SELECT CEIL(-3.7)"));

	// POWER / SQRT / EXP / LN / LOG
	[Fact] public async Task Power_Basic() => Assert.Equal("8", await Scalar("SELECT POWER(2, 3)"));
	[Fact] public async Task Power_Square() => Assert.Equal("25", await Scalar("SELECT POWER(5, 2)"));
	[Fact] public async Task Sqrt_Perfect() => Assert.Equal("4", await Scalar("SELECT SQRT(16)"));
	[Fact] public async Task Sqrt_NonPerfect() => Assert.Contains("1.41", await Scalar("SELECT SQRT(2)") ?? "");
	[Fact] public async Task Exp_Zero() => Assert.Equal("1", await Scalar("SELECT EXP(0)"));
	[Fact] public async Task Exp_One() => Assert.Contains("2.71", await Scalar("SELECT EXP(1)") ?? "");
	[Fact] public async Task Ln_One() => Assert.Equal("0", await Scalar("SELECT LN(1)"));
	[Fact] public async Task Ln_E() => Assert.Contains("1", await Scalar("SELECT ROUND(LN(EXP(1)))") ?? "");
	[Fact] public async Task Log_Base10() => Assert.Equal("2", await Scalar("SELECT LOG(100, 10)"));
	[Fact] public async Task Log10_Basic() => Assert.Equal("3", await Scalar("SELECT LOG10(1000)"));

	// GREATEST / LEAST
	[Fact] public async Task Greatest_Ints() => Assert.Equal("5", await Scalar("SELECT GREATEST(1, 3, 5, 2, 4)"));
	[Fact] public async Task Greatest_WithNull() => Assert.Null(await Scalar("SELECT GREATEST(1, NULL, 5, 2)"));
	[Fact] public async Task Least_Ints() => Assert.Equal("1", await Scalar("SELECT LEAST(3, 1, 5, 2, 4)"));
	[Fact] public async Task Least_WithNull() => Assert.Null(await Scalar("SELECT LEAST(3, NULL, 1, 5)"));
	[Fact] public async Task Greatest_Strings() => Assert.Equal("c", await Scalar("SELECT GREATEST('a', 'c', 'b')"));
	[Fact] public async Task Least_Strings() => Assert.Equal("a", await Scalar("SELECT LEAST('b', 'a', 'c')"));

	// DIV (integer division)
	[Fact] public async Task Div_Basic() => Assert.Equal("3", await Scalar("SELECT DIV(10, 3)"));
	[Fact] public async Task Div_Exact() => Assert.Equal("5", await Scalar("SELECT DIV(10, 2)"));
	[Fact] public async Task Div_Negative() => Assert.Equal("-3", await Scalar("SELECT DIV(-10, 3)"));

	// MOD
	[Fact] public async Task Mod_Basic() => Assert.Equal("1", await Scalar("SELECT MOD(10, 3)"));
	[Fact] public async Task Mod_Zero() => Assert.Equal("0", await Scalar("SELECT MOD(10, 5)"));
	[Fact] public async Task Mod_Negative() => Assert.Equal("-1", await Scalar("SELECT MOD(-10, 3)"));

	// Trigonometric functions
	[Fact] public async Task Sin_Zero() => Assert.Equal("0", await Scalar("SELECT SIN(0)"));
	[Fact] public async Task Cos_Zero() => Assert.Equal("1", await Scalar("SELECT COS(0)"));
	[Fact] public async Task Tan_Zero() => Assert.Equal("0", await Scalar("SELECT TAN(0)"));
	[Fact] public async Task Asin_One() => Assert.Contains("1.57", await Scalar("SELECT ASIN(1)") ?? ""); // pi/2
	[Fact] public async Task Acos_One() => Assert.Equal("0", await Scalar("SELECT ACOS(1)"));
	[Fact] public async Task Atan_Zero() => Assert.Equal("0", await Scalar("SELECT ATAN(0)"));
	[Fact] public async Task Atan2_Basic() => Assert.Contains("0.78", await Scalar("SELECT ATAN2(1, 1)") ?? ""); // pi/4

	// Math constants via computation
	[Fact] public async Task Pi_Approx() => Assert.Contains("3.14", await Scalar("SELECT ACOS(-1)") ?? ""); // pi = acos(-1)

	// IEEE_DIVIDE
	[Fact] public async Task IeeeDivide_Normal() => Assert.Equal("5", await Scalar("SELECT IEEE_DIVIDE(10, 2)"));
	[Fact] public async Task IeeeDivide_ByZero()
	{
		// IEEE_DIVIDE(1, 0) returns +Infinity; verify the result is not null and represents infinity
		var result = await Scalar("SELECT IEEE_DIVIDE(1, 0)");
		Assert.NotNull(result);
		// .NET double.PositiveInfinity.ToString() produces "Infinity" or "∞"
		Assert.True(result!.Contains("Inf") || result.Contains("∞"), $"Expected infinity but got: {result}");
	}

	// RAND
	[Fact] public async Task Rand_ReturnsValue()
	{
		var result = await Scalar("SELECT RAND()");
		var val = double.Parse(result!, System.Globalization.CultureInfo.InvariantCulture);
		Assert.True(val >= 0 && val < 1);
	}

	// NULL propagation in math
	[Fact] public async Task Math_NullPropagation_Add() => Assert.Null(await Scalar("SELECT NULL + 5"));
	[Fact] public async Task Math_NullPropagation_Multiply() => Assert.Null(await Scalar("SELECT NULL * 5"));
	[Fact] public async Task Math_NullPropagation_Sqrt() => Assert.Null(await Scalar("SELECT SQRT(NULL)"));
	[Fact] public async Task Math_NullPropagation_Round() => Assert.Null(await Scalar("SELECT ROUND(NULL)"));
}
