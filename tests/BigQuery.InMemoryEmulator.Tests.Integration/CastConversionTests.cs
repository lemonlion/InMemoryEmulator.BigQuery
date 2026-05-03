using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class CastConversionTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public CastConversionTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_cst_{Guid.NewGuid():N}"[..30];
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

    // CAST to INT64
    [Fact] public async Task Cast_StringToInt() => Assert.Equal("42", await Scalar("SELECT CAST('42' AS INT64)"));
    [Fact] public async Task Cast_FloatToInt() => Assert.Equal("3", await Scalar("SELECT CAST(3.7 AS INT64)"));
    [Fact] public async Task Cast_BoolToInt_True() => Assert.Equal("1", await Scalar("SELECT CAST(TRUE AS INT64)"));
    [Fact] public async Task Cast_BoolToInt_False() => Assert.Equal("0", await Scalar("SELECT CAST(FALSE AS INT64)"));

    // CAST to FLOAT64
    [Fact] public async Task Cast_StringToFloat()
    {
        var v = double.Parse((await Scalar("SELECT CAST('3.14' AS FLOAT64)"))!);
        Assert.InRange(v, 3.13, 3.15);
    }
    [Fact] public async Task Cast_IntToFloat()
    {
        var v = double.Parse((await Scalar("SELECT CAST(42 AS FLOAT64)"))!);
        Assert.Equal(42.0, v);
    }

    // CAST to STRING
    [Fact] public async Task Cast_IntToString() => Assert.Equal("42", await Scalar("SELECT CAST(42 AS STRING)"));
    [Fact] public async Task Cast_FloatToString() => Assert.Contains("3.14", (await Scalar("SELECT CAST(3.14 AS STRING)"))!);
    [Fact] public async Task Cast_BoolToString_True() => Assert.Equal("true", await Scalar("SELECT CAST(TRUE AS STRING)"));
    [Fact] public async Task Cast_BoolToString_False() => Assert.Equal("false", await Scalar("SELECT CAST(FALSE AS STRING)"));
    [Fact] public async Task Cast_DateToString() => Assert.Equal("2024-01-15", await Scalar("SELECT CAST(DATE '2024-01-15' AS STRING)"));

    // CAST to BOOL
    [Fact] public async Task Cast_IntToBool_Nonzero() => Assert.Equal("True", await Scalar("SELECT CAST(1 AS BOOL)"));
    [Fact] public async Task Cast_IntToBool_Zero() => Assert.Equal("False", await Scalar("SELECT CAST(0 AS BOOL)"));

    // CAST to DATE
    [Fact] public async Task Cast_StringToDate()
    {
        var result = await Scalar("SELECT CAST('2024-01-15' AS DATE)");
        Assert.NotNull(result);
    }

    // CAST to TIMESTAMP
[Fact] public async Task Cast_StringToTimestamp()
    {
        var result = await Scalar("SELECT CAST('2024-01-15 10:30:00' AS TIMESTAMP)");
        Assert.NotNull(result);
    }

    // SAFE_CAST
    [Fact] public async Task SafeCast_Valid() => Assert.Equal("42", await Scalar("SELECT SAFE_CAST('42' AS INT64)"));
    [Fact] public async Task SafeCast_Invalid() => Assert.Null(await Scalar("SELECT SAFE_CAST('abc' AS INT64)"));
    [Fact] public async Task SafeCast_FloatInvalid() => Assert.Null(await Scalar("SELECT SAFE_CAST('xyz' AS FLOAT64)"));
    [Fact] public async Task SafeCast_DateInvalid() => Assert.Null(await Scalar("SELECT SAFE_CAST('not-a-date' AS DATE)"));
    [Fact] public async Task SafeCast_Null() => Assert.Null(await Scalar("SELECT SAFE_CAST(NULL AS INT64)"));

    // Implicit conversions via operations
    [Fact] public async Task Implicit_IntFloat()
    {
        var v = double.Parse((await Scalar("SELECT 1 + 1.5"))!);
        Assert.Equal(2.5, v);
    }

    // CAST NULL
    [Fact] public async Task Cast_NullToInt() => Assert.Null(await Scalar("SELECT CAST(NULL AS INT64)"));
    [Fact] public async Task Cast_NullToString() => Assert.Null(await Scalar("SELECT CAST(NULL AS STRING)"));
    [Fact] public async Task Cast_NullToFloat() => Assert.Null(await Scalar("SELECT CAST(NULL AS FLOAT64)"));
    [Fact] public async Task Cast_NullToBool() => Assert.Null(await Scalar("SELECT CAST(NULL AS BOOL)"));
    [Fact] public async Task Cast_NullToDate() => Assert.Null(await Scalar("SELECT CAST(NULL AS DATE)"));
}