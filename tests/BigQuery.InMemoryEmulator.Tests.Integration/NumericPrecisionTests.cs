using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class NumericPrecisionTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public NumericPrecisionTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_num_{Guid.NewGuid():N}"[..30];
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

    // Integer arithmetic
    [Fact] public async Task Int_Add() => Assert.Equal("30", await Scalar("SELECT 10 + 20"));
    [Fact] public async Task Int_Sub() => Assert.Equal("-5", await Scalar("SELECT 10 - 15"));
    [Fact] public async Task Int_Mul() => Assert.Equal("200", await Scalar("SELECT 10 * 20"));
    [Fact] public async Task Int_Div() => Assert.Equal("3", await Scalar("SELECT 10 / 3"));
    [Fact] public async Task Int_Mod() => Assert.Equal("1", await Scalar("SELECT MOD(10, 3)"));
    [Fact] public async Task Int_Negative() => Assert.Equal("-42", await Scalar("SELECT -42"));
    [Fact] public async Task Int_Large() => Assert.Equal("9999999999", await Scalar("SELECT 9999999999"));

    // Float arithmetic
    [Fact] public async Task Float_Add()
    {
        var v = double.Parse((await Scalar("SELECT 1.1 + 2.2"))!);
        Assert.True(Math.Abs(v - 3.3) < 0.0001);
    }
    [Fact] public async Task Float_Mul()
    {
        var v = double.Parse((await Scalar("SELECT 2.5 * 4.0"))!);
        Assert.Equal(10.0, v);
    }
    [Fact] public async Task Float_Div()
    {
        var v = double.Parse((await Scalar("SELECT 10.0 / 3.0"))!);
        Assert.True(Math.Abs(v - 3.3333) < 0.001);
    }

    // IEEE special values
    [Fact] public async Task IEEE_Infinity()
    {
        var result = await Scalar("SELECT CAST('inf' AS FLOAT64)");
        Assert.NotNull(result);
        Assert.True(result == "Infinity" || result == "\u221E" || result!.Contains("Inf"));
    }
    [Fact] public async Task IEEE_NegInfinity()
    {
        var result = await Scalar("SELECT CAST('-inf' AS FLOAT64)");
        Assert.NotNull(result);
        Assert.True(result == "-Infinity" || result == "-\u221E" || result!.Contains("Inf"));
    }
    [Fact] public async Task IEEE_NaN()
    {
        var result = await Scalar("SELECT CAST('nan' AS FLOAT64)");
        Assert.True(result!.Contains("NaN") || result == "NaN" || double.IsNaN(double.Parse(result!)));
    }
    [Fact] public async Task IEEE_Divide_ByZero()
    {
        var result = await Scalar("SELECT IEEE_DIVIDE(1.0, 0.0)");
        Assert.NotNull(result);
        Assert.True(result == "Infinity" || result == "\u221E" || result!.Contains("Inf"));
    }
    [Fact] public async Task IEEE_Divide_ZeroByZero()
    {
        var result = await Scalar("SELECT IEEE_DIVIDE(0.0, 0.0)");
        Assert.True(result!.Contains("NaN") || result == "NaN" || double.IsNaN(double.Parse(result!)));
    }

    // SAFE_DIVIDE
    [Fact] public async Task SafeDivide_Normal()
    {
        var v = double.Parse((await Scalar("SELECT SAFE_DIVIDE(10.0, 2.0)"))!);
        Assert.Equal(5.0, v);
    }
    [Fact] public async Task SafeDivide_ByZero() => Assert.Null(await Scalar("SELECT SAFE_DIVIDE(10, 0)"));

    // NUMERIC type
    [Fact] public async Task Numeric_Basic()
    {
        var result = await Scalar("SELECT CAST('123.456789' AS NUMERIC)");
        Assert.Contains("123.456789", result!);
    }

    // Math with NULL
    [Fact] public async Task Add_Null() => Assert.Null(await Scalar("SELECT 5 + CAST(NULL AS INT64)"));
    [Fact] public async Task Mul_Null() => Assert.Null(await Scalar("SELECT 5 * CAST(NULL AS INT64)"));
    [Fact] public async Task Div_Null() => Assert.Null(await Scalar("SELECT CAST(NULL AS INT64) / 2"));
    [Fact] public async Task Mod_Null() => Assert.Null(await Scalar("SELECT MOD(NULL, 3)"));

    // Order of operations
    [Fact] public async Task Precedence_MulBeforeAdd() => Assert.Equal("14", await Scalar("SELECT 2 + 3 * 4"));
    [Fact] public async Task Precedence_Parens() => Assert.Equal("20", await Scalar("SELECT (2 + 3) * 4"));
    [Fact] public async Task Precedence_NestedParens() => Assert.Equal("36", await Scalar("SELECT (2 + (3 + 4)) * 4"));

    // Unary minus
    [Fact] public async Task UnaryMinus_Int() => Assert.Equal("-5", await Scalar("SELECT -(3 + 2)"));
    [Fact] public async Task UnaryMinus_Float()
    {
        var v = double.Parse((await Scalar("SELECT -(2.5)"))!);
        Assert.Equal(-2.5, v);
    }

    // ROUND / TRUNC / CEIL / FLOOR
    [Fact] public async Task Round_Basic()
    {
        var v = double.Parse((await Scalar("SELECT ROUND(2.567, 2)"))!);
        Assert.Equal(2.57, v);
    }
    [Fact] public async Task Trunc_Basic() => Assert.Equal("2", await Scalar("SELECT CAST(TRUNC(2.9) AS INT64)"));
    [Fact] public async Task Ceil_Basic() => Assert.Equal("3", await Scalar("SELECT CAST(CEIL(2.1) AS INT64)"));
    [Fact] public async Task Floor_Basic() => Assert.Equal("2", await Scalar("SELECT CAST(FLOOR(2.9) AS INT64)"));

    // ABS / SIGN
    [Fact] public async Task Abs_Positive() => Assert.Equal("5", await Scalar("SELECT ABS(5)"));
    [Fact] public async Task Abs_Negative() => Assert.Equal("5", await Scalar("SELECT ABS(-5)"));
    [Fact] public async Task Sign_Positive() => Assert.Equal("1", await Scalar("SELECT SIGN(42)"));
    [Fact] public async Task Sign_Negative() => Assert.Equal("-1", await Scalar("SELECT SIGN(-42)"));
    [Fact] public async Task Sign_Zero() => Assert.Equal("0", await Scalar("SELECT SIGN(0)"));

    // POW / SQRT / LOG
    [Fact] public async Task Pow_Basic()
    {
        var v = double.Parse((await Scalar("SELECT POW(2, 10)"))!);
        Assert.Equal(1024.0, v);
    }
    [Fact] public async Task Sqrt_Basic()
    {
        var v = double.Parse((await Scalar("SELECT SQRT(144)"))!);
        Assert.Equal(12.0, v);
    }
    [Fact] public async Task Log_Basic()
    {
        var v = double.Parse((await Scalar("SELECT LN(1)"))!);
        Assert.Equal(0.0, v);
    }
}