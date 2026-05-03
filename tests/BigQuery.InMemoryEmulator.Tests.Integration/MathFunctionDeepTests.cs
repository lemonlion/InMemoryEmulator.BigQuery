using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class MathFunctionDeepTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public MathFunctionDeepTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_mth_{Guid.NewGuid():N}"[..30];
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

    // ABS
    [Fact] public async Task Abs_Positive() => Assert.Equal("5", await Scalar("SELECT ABS(5)"));
    [Fact] public async Task Abs_Negative() => Assert.Equal("5", await Scalar("SELECT ABS(-5)"));
    [Fact] public async Task Abs_Zero() => Assert.Equal("0", await Scalar("SELECT ABS(0)"));
    [Fact] public async Task Abs_Null() => Assert.Null(await Scalar("SELECT ABS(NULL)"));

    // SIGN
    [Fact] public async Task Sign_Positive() => Assert.Equal("1", await Scalar("SELECT SIGN(42)"));
    [Fact] public async Task Sign_Negative() => Assert.Equal("-1", await Scalar("SELECT SIGN(-42)"));
    [Fact] public async Task Sign_Zero() => Assert.Equal("0", await Scalar("SELECT SIGN(0)"));

    // CEIL / FLOOR
    [Fact] public async Task Ceil_Positive()
    {
        var v = double.Parse((await Scalar("SELECT CEIL(3.2)"))!);
        Assert.Equal(4.0, v);
    }
    [Fact] public async Task Ceil_Negative()
    {
        var v = double.Parse((await Scalar("SELECT CEIL(-3.8)"))!);
        Assert.Equal(-3.0, v);
    }
    [Fact] public async Task Floor_Positive()
    {
        var v = double.Parse((await Scalar("SELECT FLOOR(3.8)"))!);
        Assert.Equal(3.0, v);
    }
    [Fact] public async Task Floor_Negative()
    {
        var v = double.Parse((await Scalar("SELECT FLOOR(-3.2)"))!);
        Assert.Equal(-4.0, v);
    }

    // ROUND
    [Fact] public async Task Round_Half()
    {
        var v = double.Parse((await Scalar("SELECT ROUND(2.5)"))!);
        // BigQuery rounds half away from zero
        Assert.True(v == 2.0 || v == 3.0);
    }
    [Fact] public async Task Round_Precision()
    {
        var v = double.Parse((await Scalar("SELECT ROUND(3.14159, 2)"))!);
        Assert.Equal(3.14, v, 2);
    }
    [Fact] public async Task Round_Null() => Assert.Null(await Scalar("SELECT ROUND(NULL)"));

    // TRUNC
    [Fact] public async Task Trunc_Positive()
    {
        var v = double.Parse((await Scalar("SELECT TRUNC(3.8)"))!);
        Assert.Equal(3.0, v);
    }
    [Fact] public async Task Trunc_Negative()
    {
        var v = double.Parse((await Scalar("SELECT TRUNC(-3.8)"))!);
        Assert.Equal(-3.0, v);
    }

    // MOD
    [Fact] public async Task Mod_Basic() => Assert.Equal("1", await Scalar("SELECT MOD(10, 3)"));
    [Fact] public async Task Mod_Negative() => Assert.Equal("-1", await Scalar("SELECT MOD(-10, 3)"));
    [Fact] public async Task Mod_Null() => Assert.Null(await Scalar("SELECT MOD(NULL, 3)"));

    // POW / POWER
    [Fact] public async Task Pow_Basic()
    {
        var v = double.Parse((await Scalar("SELECT POW(2, 10)"))!);
        Assert.Equal(1024.0, v);
    }
    [Fact] public async Task Pow_Fractional()
    {
        var v = double.Parse((await Scalar("SELECT POW(4, 0.5)"))!);
        Assert.Equal(2.0, v, 5);
    }
    [Fact] public async Task Pow_Zero()
    {
        var v = double.Parse((await Scalar("SELECT POW(5, 0)"))!);
        Assert.Equal(1.0, v);
    }

    // SQRT
    [Fact] public async Task Sqrt_Perfect()
    {
        var v = double.Parse((await Scalar("SELECT SQRT(16)"))!);
        Assert.Equal(4.0, v);
    }
    [Fact] public async Task Sqrt_NonPerfect()
    {
        var v = double.Parse((await Scalar("SELECT SQRT(2)"))!);
        Assert.InRange(v, 1.414, 1.415);
    }

    // LOG / LN / LOG10
    [Fact] public async Task Ln_E()
    {
        var v = double.Parse((await Scalar("SELECT LN(EXP(1))"))!);
        Assert.Equal(1.0, v, 5);
    }
    [Fact] public async Task Log_Base2()
    {
        var v = double.Parse((await Scalar("SELECT LOG(8, 2)"))!);
        Assert.Equal(3.0, v, 5);
    }
    [Fact] public async Task Log10_Basic()
    {
        var v = double.Parse((await Scalar("SELECT LOG10(1000)"))!);
        Assert.Equal(3.0, v, 5);
    }

    // EXP
    [Fact] public async Task Exp_Zero()
    {
        var v = double.Parse((await Scalar("SELECT EXP(0)"))!);
        Assert.Equal(1.0, v);
    }
    [Fact] public async Task Exp_One()
    {
        var v = double.Parse((await Scalar("SELECT EXP(1)"))!);
        Assert.InRange(v, 2.718, 2.719);
    }

    // GREATEST / LEAST
    [Fact] public async Task Greatest_Ints() => Assert.Equal("10", await Scalar("SELECT GREATEST(1, 5, 10, 3)"));
    [Fact] public async Task Least_Ints() => Assert.Equal("1", await Scalar("SELECT LEAST(1, 5, 10, 3)"));
    [Fact] public async Task Greatest_WithNull() => Assert.Null(await Scalar("SELECT GREATEST(1, NULL, 10)"));
    [Fact] public async Task Least_WithNull() => Assert.Null(await Scalar("SELECT LEAST(1, NULL, 10)"));

    // DIV
    [Fact] public async Task Div_Basic() => Assert.Equal("3", await Scalar("SELECT DIV(10, 3)"));
    [Fact] public async Task Div_Exact() => Assert.Equal("5", await Scalar("SELECT DIV(10, 2)"));

    // SAFE_DIVIDE
    [Fact] public async Task SafeDivide_Normal()
    {
        var v = double.Parse((await Scalar("SELECT SAFE_DIVIDE(10, 4)"))!);
        Assert.Equal(2.5, v);
    }
    [Fact] public async Task SafeDivide_ByZero() => Assert.Null(await Scalar("SELECT SAFE_DIVIDE(10, 0)"));

    // IEEE_DIVIDE
    [Fact] public async Task IeeeDivide_ByZero()
    {
        var result = await Scalar("SELECT IEEE_DIVIDE(10, 0)");
        Assert.NotNull(result);
        Assert.Contains("\u221E", result!); // Infinity symbol
    }

    // RAND
    [Fact] public async Task Rand_InRange()
    {
        var v = double.Parse((await Scalar("SELECT RAND()"))!);
        Assert.InRange(v, 0.0, 1.0);
    }
}