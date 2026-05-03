using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class GenerateArrayDateTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public GenerateArrayDateTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_gen_{Guid.NewGuid():N}"[..30];
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

    private async Task<List<BigQueryRow>> Query(string sql)
    {
        var client = await _fixture.GetClientAsync();
        var result = await client.ExecuteQueryAsync(sql, parameters: null);
        return result.ToList();
    }

    // GENERATE_ARRAY
    [Fact] public async Task GenerateArray_1to5() => Assert.Equal("5", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(1, 5))"));
    [Fact] public async Task GenerateArray_Step2() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(1, 5, 2))")); // 1,3,5
    [Fact] public async Task GenerateArray_Step3() => Assert.Equal("2", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(0, 5, 3))")); // 0,3
    [Fact] public async Task GenerateArray_Single() => Assert.Equal("1", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(5, 5))"));
    [Fact] public async Task GenerateArray_Empty() => Assert.Equal("0", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(5, 1))"));
    [Fact] public async Task GenerateArray_Negative() => Assert.Equal("5", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(-2, 2))"));
    [Fact] public async Task GenerateArray_Unnest()
    {
        var rows = await Query("SELECT n FROM UNNEST(GENERATE_ARRAY(1, 5)) as n ORDER BY n");
        Assert.Equal(5, rows.Count);
        Assert.Equal("1", rows[0]["n"]?.ToString());
        Assert.Equal("5", rows[4]["n"]?.ToString());
    }

    // GENERATE_DATE_ARRAY
    [Fact] public async Task GenerateDateArray_Days()
    {
        var result = await Scalar("SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-05'))");
        Assert.Equal("5", result);
    }
    [Fact] public async Task GenerateDateArray_Months()
    {
        var result = await Scalar("SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-06-01', INTERVAL 1 MONTH))");
        Assert.Equal("6", result);
    }
    [Fact] public async Task GenerateDateArray_Weeks()
    {
        var result = await Scalar("SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-29', INTERVAL 1 WEEK))");
        Assert.Equal("5", result); // Jan 1, 8, 15, 22, 29
    }

    // GENERATE_TIMESTAMP_ARRAY
    [Fact] public async Task GenerateTimestampArray_Hours()
    {
        var result = await Scalar("SELECT ARRAY_LENGTH(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP '2024-01-01 00:00:00 UTC', TIMESTAMP '2024-01-01 05:00:00 UTC', INTERVAL 1 HOUR))");
        Assert.Equal("6", result);
    }

    // Using GENERATE_ARRAY with computation
    [Fact] public async Task GenerateArray_WithComputation()
    {
        var result = await Scalar("SELECT SUM(n) FROM UNNEST(GENERATE_ARRAY(1, 10)) as n");
        Assert.Equal("55", result);
    }
    [Fact] public async Task GenerateArray_WithFilter()
    {
        var result = await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 20)) as n WHERE MOD(n, 2) = 0");
        Assert.Equal("10", result);
    }
    [Fact] public async Task GenerateArray_Squares()
    {
        var rows = await Query("SELECT n, n * n as sq FROM UNNEST(GENERATE_ARRAY(1, 5)) as n ORDER BY n");
        Assert.Equal(5, rows.Count);
        Assert.Equal("1", rows[0]["sq"]?.ToString());
        Assert.Equal("25", rows[4]["sq"]?.ToString());
    }
}