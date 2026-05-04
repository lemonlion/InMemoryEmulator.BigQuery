using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Math function comprehensive coverage: ROUND, TRUNC, CEIL, FLOOR, SIGN, POW, SQRT, LOG, LN, EXP, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class MathFunctionCoverageTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public MathFunctionCoverageTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_mfc_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.nums` (id INT64, x FLOAT64, y FLOAT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.nums` VALUES
			(1,3.14159,2.71828),(2,-5.5,3.0),(3,0.0,1.0),(4,100.0,10.0),(5,1.5,-2.5),
			(6,2.0,8.0),(7,9.0,0.5),(8,16.0,4.0),(9,0.001,1000.0),(10,-3.7,-3.7)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }

	// ---- ROUND ----
	[Fact] public async Task Round_Int() => Assert.Equal("3", await S("SELECT ROUND(3.14)"));
	[Fact] public async Task Round_Dec2() { var v = await S("SELECT ROUND(3.14159, 2)"); Assert.NotNull(v); Assert.StartsWith("3.14", v); }
	[Fact] public async Task Round_Negative() => Assert.Equal("-4", await S("SELECT ROUND(-3.5)"));
	[Fact] public async Task Round_Zero() => Assert.Equal("0", await S("SELECT ROUND(0.4)"));
	[Fact] public async Task Round_Null() => Assert.Null(await S("SELECT ROUND(NULL)"));

	// ---- TRUNC ----
	[Fact] public async Task Trunc_Positive() => Assert.Equal("3", await S("SELECT TRUNC(3.9)"));
	[Fact] public async Task Trunc_Negative() => Assert.Equal("-3", await S("SELECT TRUNC(-3.9)"));
	[Fact] public async Task Trunc_Dec2() { var v = await S("SELECT TRUNC(3.14159, 2)"); Assert.NotNull(v); Assert.StartsWith("3.14", v); }
	[Fact] public async Task Trunc_Null() => Assert.Null(await S("SELECT TRUNC(NULL)"));

	// ---- CEIL / FLOOR ----
	[Fact] public async Task Ceil_Positive() => Assert.Equal("4", await S("SELECT CEIL(3.1)"));
	[Fact] public async Task Ceil_Negative() => Assert.Equal("-3", await S("SELECT CEIL(-3.9)"));
	[Fact] public async Task Ceil_Whole() => Assert.Equal("5", await S("SELECT CEIL(5.0)"));
	[Fact] public async Task Ceil_Null() => Assert.Null(await S("SELECT CEIL(NULL)"));
	[Fact] public async Task Floor_Positive() => Assert.Equal("3", await S("SELECT FLOOR(3.9)"));
	[Fact] public async Task Floor_Negative() => Assert.Equal("-4", await S("SELECT FLOOR(-3.1)"));
	[Fact] public async Task Floor_Whole() => Assert.Equal("5", await S("SELECT FLOOR(5.0)"));
	[Fact] public async Task Floor_Null() => Assert.Null(await S("SELECT FLOOR(NULL)"));

	// ---- ABS ----
	[Fact] public async Task Abs_Positive() => Assert.Equal("5", await S("SELECT ABS(5)"));
	[Fact] public async Task Abs_Negative() => Assert.Equal("5", await S("SELECT ABS(-5)"));
	[Fact] public async Task Abs_Zero() => Assert.Equal("0", await S("SELECT ABS(0)"));
	[Fact] public async Task Abs_Float() { var v = await S("SELECT ABS(-3.14)"); Assert.NotNull(v); Assert.StartsWith("3.14", v); }
	[Fact] public async Task Abs_Null() => Assert.Null(await S("SELECT ABS(NULL)"));

	// ---- SIGN ----
	[Fact] public async Task Sign_Positive() => Assert.Equal("1", await S("SELECT SIGN(42)"));
	[Fact] public async Task Sign_Negative() => Assert.Equal("-1", await S("SELECT SIGN(-42)"));
	[Fact] public async Task Sign_Zero() => Assert.Equal("0", await S("SELECT SIGN(0)"));
	[Fact] public async Task Sign_Null() => Assert.Null(await S("SELECT SIGN(NULL)"));

	// ---- POW / POWER ----
	[Fact] public async Task Pow_Basic() => Assert.Equal("8", await S("SELECT POW(2, 3)"));
	[Fact] public async Task Pow_Square() => Assert.Equal("25", await S("SELECT POW(5, 2)"));
	[Fact] public async Task Pow_Zero() => Assert.Equal("1", await S("SELECT POW(5, 0)"));
	[Fact] public async Task Pow_Fractional() => Assert.Equal("2", await S("SELECT POW(4, 0.5)"));
	[Fact] public async Task Pow_Null() => Assert.Null(await S("SELECT POW(NULL, 2)"));

	// ---- SQRT ----
	[Fact] public async Task Sqrt_Basic() => Assert.Equal("3", await S("SELECT SQRT(9)"));
	[Fact] public async Task Sqrt_Four() => Assert.Equal("4", await S("SELECT SQRT(16)"));
	[Fact] public async Task Sqrt_Two() { var v = await S("SELECT ROUND(SQRT(2), 5)"); Assert.NotNull(v); Assert.StartsWith("1.41421", v); }
	[Fact] public async Task Sqrt_Null() => Assert.Null(await S("SELECT SQRT(NULL)"));

	// ---- LOG / LN / LOG10 ----
	[Fact] public async Task Ln_E() { var v = await S("SELECT ROUND(LN(2.71828), 3)"); Assert.NotNull(v); Assert.StartsWith("1", v); }
	[Fact] public async Task Ln_One() => Assert.Equal("0", await S("SELECT LN(1)"));
	[Fact] public async Task Log_Base2() => Assert.Equal("3", await S("SELECT LOG(8, 2)"));
	[Fact] public async Task Log_Base10() => Assert.Equal("2", await S("SELECT LOG(100, 10)"));
	[Fact] public async Task Log10_Hundred() => Assert.Equal("2", await S("SELECT LOG10(100)"));
	[Fact] public async Task Log10_Thousand() => Assert.Equal("3", await S("SELECT LOG10(1000)"));
	[Fact] public async Task Ln_Null() => Assert.Null(await S("SELECT LN(NULL)"));

	// ---- EXP ----
	[Fact] public async Task Exp_Zero() => Assert.Equal("1", await S("SELECT EXP(0)"));
	[Fact] public async Task Exp_One() { var v = await S("SELECT ROUND(EXP(1), 3)"); Assert.NotNull(v); Assert.StartsWith("2.718", v); }
	[Fact] public async Task Exp_Null() => Assert.Null(await S("SELECT EXP(NULL)"));

	// ---- MOD ----
	[Fact] public async Task Mod_Basic() => Assert.Equal("1", await S("SELECT MOD(7, 3)"));
	[Fact] public async Task Mod_Even() => Assert.Equal("0", await S("SELECT MOD(8, 4)"));
	[Fact] public async Task Mod_Negative() => Assert.Equal("-1", await S("SELECT MOD(-7, 3)"));
	[Fact] public async Task Mod_Null() => Assert.Null(await S("SELECT MOD(NULL, 3)"));

	// ---- GREATEST / LEAST ----
	[Fact] public async Task Greatest_Ints() => Assert.Equal("5", await S("SELECT GREATEST(1, 3, 5, 2, 4)"));
	[Fact] public async Task Greatest_Negative() => Assert.Equal("-1", await S("SELECT GREATEST(-5, -3, -1)"));
	[Fact] public async Task Least_Ints() => Assert.Equal("1", await S("SELECT LEAST(5, 3, 1, 4, 2)"));
	[Fact] public async Task Least_Negative() => Assert.Equal("-5", await S("SELECT LEAST(-1, -3, -5)"));

	// ---- Trig functions ----
	[Fact] public async Task Sin_Zero() => Assert.Equal("0", await S("SELECT SIN(0)"));
	[Fact] public async Task Cos_Zero() => Assert.Equal("1", await S("SELECT COS(0)"));
	[Fact] public async Task Tan_Zero() => Assert.Equal("0", await S("SELECT TAN(0)"));
	[Fact] public async Task Asin_One() { var v = await S("SELECT ROUND(ASIN(1), 4)"); Assert.NotNull(v); Assert.StartsWith("1.570", v); }
	[Fact] public async Task Acos_One() => Assert.Equal("0", await S("SELECT ACOS(1)"));
	[Fact] public async Task Atan_Zero() => Assert.Equal("0", await S("SELECT ATAN(0)"));

	// ---- IEEE special values ----
	[Fact] public async Task IsNan_False() => Assert.Equal("False", await S("SELECT IS_NAN(1.0)"));
	[Fact] public async Task IsInf_False() => Assert.Equal("False", await S("SELECT IS_INF(1.0)"));

	// ---- Table queries ----
	[Fact] public async Task Table_AbsSum()
	{
		var v = await S("SELECT CAST(SUM(ABS(x)) AS INT64) FROM `{ds}.nums`");
		Assert.NotNull(v);
		var sum = int.Parse(v!);
		Assert.True(sum > 100);
	}
	[Fact] public async Task Table_RoundAll()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.nums` WHERE ROUND(x) = x");
		Assert.NotNull(v);
	}
	[Fact] public async Task Table_PowColumn()
	{
		var v = await S("SELECT POW(x, 2) FROM `{ds}.nums` WHERE id = 6");
		Assert.Equal("4", v); // 2^2 = 4
	}
	[Fact] public async Task Table_SqrtColumn()
	{
		var v = await S("SELECT SQRT(x) FROM `{ds}.nums` WHERE id = 8");
		Assert.Equal("4", v); // sqrt(16) = 4
	}
}
