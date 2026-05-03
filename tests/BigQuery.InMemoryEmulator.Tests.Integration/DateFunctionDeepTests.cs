using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DateFunctionDeepTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public DateFunctionDeepTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_dt2_{Guid.NewGuid():N}"[..30];
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

    // CURRENT_DATE
    [Fact] public async Task CurrentDate_NotNull()
    {
        var result = await Scalar("SELECT CURRENT_DATE()");
        Assert.NotNull(result);
    }

    // DATE
    [Fact] public async Task Date_FromParts() => Assert.NotNull(await Scalar("SELECT DATE(2024, 1, 15)"));
    [Fact] public async Task Date_FromTimestamp()
    {
        var result = await Scalar("SELECT DATE(TIMESTAMP '2024-01-15 10:30:00 UTC')");
        Assert.NotNull(result);
    }

    // DATE_ADD
    [Fact] public async Task DateAdd_Day() => Assert.NotNull(await Scalar("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 1 DAY)"));
    [Fact] public async Task DateAdd_Month() => Assert.NotNull(await Scalar("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 1 MONTH)"));
    [Fact] public async Task DateAdd_Year() => Assert.NotNull(await Scalar("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 1 YEAR)"));
    [Fact] public async Task DateAdd_Null() => Assert.Null(await Scalar("SELECT DATE_ADD(NULL, INTERVAL 1 DAY)"));

    // DATE_SUB
    [Fact] public async Task DateSub_Day() => Assert.NotNull(await Scalar("SELECT DATE_SUB(DATE '2024-01-15', INTERVAL 1 DAY)"));
    [Fact] public async Task DateSub_Month() => Assert.NotNull(await Scalar("SELECT DATE_SUB(DATE '2024-01-15', INTERVAL 1 MONTH)"));

    // DATE_DIFF
    [Fact] public async Task DateDiff_Day() => Assert.Equal("10", await Scalar("SELECT DATE_DIFF(DATE '2024-01-15', DATE '2024-01-05', DAY)"));
    [Fact] public async Task DateDiff_Month() => Assert.Equal("2", await Scalar("SELECT DATE_DIFF(DATE '2024-03-15', DATE '2024-01-15', MONTH)"));
    [Fact] public async Task DateDiff_Year() => Assert.Equal("1", await Scalar("SELECT DATE_DIFF(DATE '2025-01-15', DATE '2024-01-15', YEAR)"));
    [Fact] public async Task DateDiff_Negative() => Assert.Equal("-10", await Scalar("SELECT DATE_DIFF(DATE '2024-01-05', DATE '2024-01-15', DAY)"));

    // DATE_TRUNC
    [Fact] public async Task DateTrunc_Month() => Assert.NotNull(await Scalar("SELECT DATE_TRUNC(DATE '2024-01-15', MONTH)"));
    [Fact] public async Task DateTrunc_Year() => Assert.NotNull(await Scalar("SELECT DATE_TRUNC(DATE '2024-06-15', YEAR)"));

    // EXTRACT
    [Fact] public async Task Extract_Year() => Assert.Equal("2024", await Scalar("SELECT EXTRACT(YEAR FROM DATE '2024-06-15')"));
    [Fact] public async Task Extract_Month() => Assert.Equal("6", await Scalar("SELECT EXTRACT(MONTH FROM DATE '2024-06-15')"));
    [Fact] public async Task Extract_Day() => Assert.Equal("15", await Scalar("SELECT EXTRACT(DAY FROM DATE '2024-06-15')"));
    [Fact] public async Task Extract_DayOfWeek() 
    {
        var result = await Scalar("SELECT EXTRACT(DAYOFWEEK FROM DATE '2024-01-01')"); // Monday
        Assert.NotNull(result);
        int.Parse(result!);
    }

    // FORMAT_DATE
    [Fact] public async Task FormatDate_Basic() => Assert.Equal("2024-01-15", await Scalar("SELECT FORMAT_DATE('%Y-%m-%d', DATE '2024-01-15')"));
    [Fact] public async Task FormatDate_Custom() => Assert.Equal("15/01/2024", await Scalar("SELECT FORMAT_DATE('%d/%m/%Y', DATE '2024-01-15')"));

    // PARSE_DATE
    [Fact] public async Task ParseDate_Basic() => Assert.NotNull(await Scalar("SELECT PARSE_DATE('%Y-%m-%d', '2024-01-15')"));

    // LAST_DAY
    [Fact] public async Task LastDay_Jan() => Assert.NotNull(await Scalar("SELECT LAST_DAY(DATE '2024-01-15')"));
    [Fact] public async Task LastDay_Feb_Leap() => Assert.NotNull(await Scalar("SELECT LAST_DAY(DATE '2024-02-15')"));
    [Fact] public async Task LastDay_Feb_NonLeap() => Assert.NotNull(await Scalar("SELECT LAST_DAY(DATE '2023-02-15')"));

    // TIMESTAMP functions
    [Fact] public async Task CurrentTimestamp_NotNull()
    {
        var result = await Scalar("SELECT CURRENT_TIMESTAMP()");
        Assert.NotNull(result);
    }
    [Fact] public async Task TimestampAdd_Hour()
    {
        var result = await Scalar("SELECT TIMESTAMP_ADD(TIMESTAMP '2024-01-15 10:00:00 UTC', INTERVAL 2 HOUR)");
        Assert.NotNull(result);
    }
    [Fact] public async Task TimestampSub_Day()
    {
        var result = await Scalar("SELECT TIMESTAMP_SUB(TIMESTAMP '2024-01-15 10:00:00 UTC', INTERVAL 1 DAY)");
        Assert.NotNull(result);
    }
    [Fact] public async Task TimestampDiff_Second() => Assert.Equal("3600", await Scalar("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-15 11:00:00 UTC', TIMESTAMP '2024-01-15 10:00:00 UTC', SECOND)"));
    [Fact] public async Task TimestampDiff_Minute() => Assert.Equal("60", await Scalar("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-15 11:00:00 UTC', TIMESTAMP '2024-01-15 10:00:00 UTC', MINUTE)"));
    [Fact] public async Task TimestampTrunc_Day()
    {
        var result = await Scalar("SELECT TIMESTAMP_TRUNC(TIMESTAMP '2024-01-15 10:30:00 UTC', DAY)");
        Assert.NotNull(result);
    }

    // UNIX_SECONDS / TIMESTAMP_SECONDS
    [Fact] public async Task UnixSeconds()
    {
        var result = await Scalar("SELECT UNIX_SECONDS(TIMESTAMP '1970-01-01 00:00:01 UTC')");
        Assert.Equal("1", result);
    }
    [Fact] public async Task TimestampSeconds()
    {
        var result = await Scalar("SELECT TIMESTAMP_SECONDS(0)");
        Assert.NotNull(result);
    }
}