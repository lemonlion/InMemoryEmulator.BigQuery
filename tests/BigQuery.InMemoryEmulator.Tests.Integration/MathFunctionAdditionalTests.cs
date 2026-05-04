using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Math function additional tests: ABS, MOD, CEIL, FLOOR, ROUND, TRUNC, SIGN, trig, log, IEEE, SAFE math, BIT ops.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class MathFunctionAdditionalTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public MathFunctionAdditionalTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_mfa_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }

	// ---- ABS ----
	[Fact] public async Task Abs_Positive() => Assert.Equal("5", await S("SELECT ABS(5)"));
	[Fact] public async Task Abs_Negative() => Assert.Equal("5", await S("SELECT ABS(-5)"));
	[Fact] public async Task Abs_Zero() => Assert.Equal("0", await S("SELECT ABS(0)"));
	[Fact] public async Task Abs_Float() { var v = await S("SELECT ABS(-3.14)"); Assert.NotNull(v); Assert.StartsWith("3.14", v); }
	[Fact] public async Task Abs_Null() => Assert.Null(await S("SELECT ABS(CAST(NULL AS INT64))"));

	// ---- MOD ----
	[Fact] public async Task Mod_Basic() => Assert.Equal("1", await S("SELECT MOD(10, 3)"));
	[Fact] public async Task Mod_Even() => Assert.Equal("0", await S("SELECT MOD(10, 5)"));
	[Fact] public async Task Mod_Negative() => Assert.Equal("-1", await S("SELECT MOD(-10, 3)"));
	[Fact] public async Task Mod_Null() => Assert.Null(await S("SELECT MOD(NULL, 3)"));

	// ---- CEIL ----
	[Fact] public async Task Ceil_Up() { var v = await S("SELECT CEIL(3.2)"); Assert.NotNull(v); Assert.Contains("4", v); }
	[Fact] public async Task Ceil_Neg() { var v = await S("SELECT CEIL(-3.8)"); Assert.NotNull(v); Assert.Contains("-3", v); }
	[Fact] public async Task Ceil_Exact() { var v = await S("SELECT CEIL(5.0)"); Assert.NotNull(v); Assert.Contains("5", v); }

	// ---- FLOOR ----
	[Fact] public async Task Floor_Down() { var v = await S("SELECT FLOOR(3.8)"); Assert.NotNull(v); Assert.Contains("3", v); }
	[Fact] public async Task Floor_Neg() { var v = await S("SELECT FLOOR(-3.2)"); Assert.NotNull(v); Assert.Contains("-4", v); }

	// ---- ROUND ----
	[Fact] public async Task Round_Up() { var v = await S("SELECT ROUND(3.5)"); Assert.NotNull(v); Assert.Contains("4", v); }
	[Fact] public async Task Round_TwoDec() { var v = await S("SELECT ROUND(3.14159, 2)"); Assert.NotNull(v); Assert.StartsWith("3.14", v); }
	[Fact] public async Task Round_Null() => Assert.Null(await S("SELECT ROUND(CAST(NULL AS FLOAT64))"));

	// ---- TRUNC ----
	[Fact] public async Task Trunc_Pos() { var v = await S("SELECT TRUNC(3.9)"); Assert.NotNull(v); Assert.Contains("3", v); }
	[Fact] public async Task Trunc_Neg() { var v = await S("SELECT TRUNC(-3.9)"); Assert.NotNull(v); Assert.Contains("-3", v); }

	// ---- POW ----
	[Fact] public async Task Pow_Basic() { var v = await S("SELECT POW(2, 3)"); Assert.NotNull(v); Assert.Contains("8", v); }
	[Fact] public async Task Pow_Zero() { var v = await S("SELECT POW(5, 0)"); Assert.NotNull(v); Assert.Contains("1", v); }

	// ---- SQRT ----
	[Fact] public async Task Sqrt_Perfect() { var v = await S("SELECT SQRT(16)"); Assert.NotNull(v); Assert.Contains("4", v); }
	[Fact] public async Task Sqrt_Zero() { var v = await S("SELECT SQRT(0)"); Assert.NotNull(v); Assert.Contains("0", v); }

	// ---- SIGN ----
	[Fact] public async Task Sign_Pos() => Assert.Equal("1", await S("SELECT SIGN(42)"));
	[Fact] public async Task Sign_Neg() => Assert.Equal("-1", await S("SELECT SIGN(-42)"));
	[Fact] public async Task Sign_Zero() => Assert.Equal("0", await S("SELECT SIGN(0)"));

	// ---- LOG / LN ----
	[Fact] public async Task Log10_100() { var v = await S("SELECT LOG10(100)"); Assert.NotNull(v); Assert.Contains("2", v); }
	[Fact] public async Task Log_Base2() { var v = await S("SELECT LOG(8, 2)"); Assert.NotNull(v); Assert.Contains("3", v); }

	// ---- EXP ----
	[Fact] public async Task Exp_Zero() { var v = await S("SELECT EXP(0)"); Assert.NotNull(v); Assert.Contains("1", v); }

	// ---- Trig ----
	[Fact] public async Task Sin_Zero() { var v = await S("SELECT SIN(0)"); Assert.NotNull(v); Assert.Contains("0", v); }
	[Fact] public async Task Cos_Zero() { var v = await S("SELECT COS(0)"); Assert.NotNull(v); Assert.Contains("1", v); }
	[Fact] public async Task Atan2_Basic() { var v = await S("SELECT ROUND(ATAN2(1, 1), 4)"); Assert.NotNull(v); Assert.StartsWith("0.785", v); }

	// ---- IEEE_DIVIDE ----
	[Fact] public async Task IeeeDivide_Normal() => Assert.Equal("2.5", await S("SELECT IEEE_DIVIDE(5, 2)"));

	// ---- SAFE functions ----
	[Fact] public async Task SafeDivide_Normal() => Assert.Equal("2.5", await S("SELECT SAFE_DIVIDE(5, 2)"));
	[Fact] public async Task SafeDivide_Zero() => Assert.Null(await S("SELECT SAFE_DIVIDE(5, 0)"));
	[Fact] public async Task SafeMultiply() => Assert.Equal("10", await S("SELECT SAFE_MULTIPLY(2, 5)"));
	[Fact] public async Task SafeAdd() => Assert.Equal("7", await S("SELECT SAFE_ADD(3, 4)"));
	[Fact] public async Task SafeSubtract() => Assert.Equal("3", await S("SELECT SAFE_SUBTRACT(7, 4)"));
	[Fact] public async Task SafeNegate() => Assert.Equal("-5", await S("SELECT SAFE_NEGATE(5)"));

	// ---- GREATEST / LEAST ----
	[Fact] public async Task Greatest_Basic() => Assert.Equal("5", await S("SELECT GREATEST(1, 3, 5, 2)"));
	[Fact] public async Task Least_Basic() => Assert.Equal("1", await S("SELECT LEAST(1, 3, 5, 2)"));

	// ---- BIT operations ----
	[Fact] public async Task BitAnd() => Assert.Equal("4", await S("SELECT 5 & 6"));
	[Fact] public async Task BitOr() => Assert.Equal("7", await S("SELECT 5 | 6"));
	[Fact] public async Task BitXor() => Assert.Equal("3", await S("SELECT 5 ^ 6"));
	[Fact] public async Task BitNot() => Assert.Equal("-6", await S("SELECT ~5"));
	[Fact] public async Task BitShiftLeft() => Assert.Equal("20", await S("SELECT 5 << 2"));
	[Fact] public async Task BitShiftRight() => Assert.Equal("2", await S("SELECT 10 >> 2"));

	// ---- Math in table queries ----
	[Fact] public async Task Mod_InQuery()
	{
		await Exec("CREATE TABLE `{ds}.nums` (id INT64, a INT64, b INT64)");
		await Exec("INSERT INTO `{ds}.nums` VALUES (1,10,3),(2,20,7),(3,15,5)");
		Assert.Equal("7", await S("SELECT SUM(MOD(a, b)) FROM `{ds}.nums`")); // 1+6+0
	}
	[Fact] public async Task Abs_InWhere()
	{
		await Exec("CREATE TABLE `{ds}.vals` (id INT64, x INT64)");
		await Exec("INSERT INTO `{ds}.vals` VALUES (1,-5),(2,3),(3,-10),(4,7)");
		Assert.Equal("3", await S("SELECT COUNT(*) FROM `{ds}.vals` WHERE ABS(x) > 4"));
	}

	private async Task Exec(string sql) { var c = await _fixture.GetClientAsync(); await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); }
}
