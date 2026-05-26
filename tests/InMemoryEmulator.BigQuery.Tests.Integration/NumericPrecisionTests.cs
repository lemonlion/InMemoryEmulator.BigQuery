using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

[Collection(IntegrationCollection.Name)]
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
    [Fact] public async Task Int_Div() => Assert.Equal("3", await Scalar("SELECT DIV(10, 3)"));  // / returns FLOAT64; use DIV for integer division
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
    // GO emulator bug: cannot parse inf/nan string literals to FLOAT64.
    [Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
    [Fact] public async Task Float_Div()
    {
        var v = double.Parse((await Scalar("SELECT 10.0 / 3.0"))!);
        Assert.True(Math.Abs(v - 3.3333) < 0.001);
    }

    // IEEE special values
    // GO emulator bug: cannot parse inf/nan string literals to FLOAT64.
    [Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
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
    // GO emulator bug: returns '+Inf'/'-Inf' format instead of standard 'inf'/'-inf'.
    [Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
    [Fact] public async Task IEEE_NaN()
    {
        var result = await Scalar("SELECT CAST('nan' AS FLOAT64)");
        Assert.True(result!.Contains("NaN") || result == "NaN" || double.IsNaN(double.Parse(result!)));
    }
    // GO emulator bug: returns invalid result causing NullReferenceException.
    [Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
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
    // GO emulator bug: unary minus operator not implemented (missing zetasqlite_unary_minus).
    [Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
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

    // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric_type
    //   NUMERIC division rounds half away from zero to 9 decimal places.
    [Fact]
    public async Task Numeric_Division_RoundsHalfAwayFromZero()
    {
        // 2/3 = 0.666666666... → 10th digit is 6 (≥5), so rounds up to 0.666666667
        var result = await Scalar("SELECT CAST(2 AS NUMERIC) / CAST(3 AS NUMERIC)");
        Assert.Equal("0.666666667", result);
    }

    [Fact]
    public async Task Numeric_SafeDivide_RoundsHalfAwayFromZero()
    {
        var result = await Scalar("SELECT SAFE_DIVIDE(CAST(2 AS NUMERIC), CAST(3 AS NUMERIC))");
        Assert.Equal("0.666666667", result);
    }

    [Fact]
    public async Task Numeric_Division_TruncatesWhenDigitBelow5()
    {
        // 1/3 = 0.333333333... → 10th digit is 3 (<5), so truncates to 0.333333333
        var result = await Scalar("SELECT CAST(1 AS NUMERIC) / CAST(3 AS NUMERIC)");
        Assert.Equal("0.333333333", result);
    }

    [Fact]
    public async Task Numeric_SafeDivide_NegativeRoundsAwayFromZero()
    {
        // -2/3 = -0.666666666... → rounds away from zero to -0.666666667
        var result = await Scalar("SELECT SAFE_DIVIDE(CAST(-2 AS NUMERIC), CAST(3 AS NUMERIC))");
        Assert.Equal("-0.666666667", result);
    }

    [Fact]
    public async Task Numeric_SafeDivide_NestedPrecision()
    {
        // Nested SAFE_DIVIDE matching the production pattern
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($"CREATE TABLE `{_datasetId}.num_t` (a NUMERIC, b NUMERIC)", parameters: null);
        await client.ExecuteQueryAsync($"INSERT INTO `{_datasetId}.num_t` (a, b) VALUES (12125.48, 492), (25506.26, 1108)", parameters: null);

        var sql = $@"
            WITH agg AS (
                SELECT
                    SUM(IF(a > 20000, a, NULL)) AS cmp_takings,
                    SUM(IF(a > 20000, b, NULL)) AS cmp_visits,
                    SUM(IF(a < 20000, a, NULL)) AS cur_takings,
                    SUM(IF(a < 20000, b, NULL)) AS cur_visits
                FROM `{_datasetId}.num_t`
            )
            SELECT
                SAFE_DIVIDE(cur_takings, cur_visits) AS cur_avg,
                SAFE_DIVIDE(cmp_takings, cmp_visits) AS cmp_avg,
                SAFE_DIVIDE(
                    SAFE_DIVIDE(cur_takings, cur_visits) - SAFE_DIVIDE(cmp_takings, cmp_visits),
                    SAFE_DIVIDE(cmp_takings, cmp_visits)
                ) AS change
            FROM agg";

        var result = await client.ExecuteQueryAsync(sql, parameters: null);
        var rows = result.ToList();
        Assert.Single(rows);

        var changeField = result.Schema.Fields.First(f => f.Name == "change");
        Assert.NotEqual("STRING", changeField.Type);
        Assert.NotNull(rows[0]["change"]);
    }
}
