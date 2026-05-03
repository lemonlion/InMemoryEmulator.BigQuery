using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DateTimeFunctionExtendedTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public DateTimeFunctionExtendedTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_dtx_{Guid.NewGuid():N}"[..30];
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
        var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
        var rows = result.ToList();
        return rows.Count > 0 ? rows[0][0]?.ToString() : null;
    }

    // EXTRACT from DATE
    [Fact] public async Task Extract_Year() => Assert.Equal("2024", await Scalar("SELECT EXTRACT(YEAR FROM DATE '2024-03-15')"));
    [Fact] public async Task Extract_Month() => Assert.Equal("3", await Scalar("SELECT EXTRACT(MONTH FROM DATE '2024-03-15')"));
    [Fact] public async Task Extract_Day() => Assert.Equal("15", await Scalar("SELECT EXTRACT(DAY FROM DATE '2024-03-15')"));
    [Fact] public async Task Extract_DayOfWeek() => Assert.NotNull(await Scalar("SELECT EXTRACT(DAYOFWEEK FROM DATE '2024-03-15')"));
    [Fact] public async Task Extract_DayOfYear() => Assert.Equal("75", await Scalar("SELECT EXTRACT(DAYOFYEAR FROM DATE '2024-03-15')"));
    [Fact] public async Task Extract_Quarter() => Assert.Equal("1", await Scalar("SELECT EXTRACT(QUARTER FROM DATE '2024-03-15')"));
    [Fact] public async Task Extract_Week() => Assert.NotNull(await Scalar("SELECT EXTRACT(WEEK FROM DATE '2024-03-15')"));

    // EXTRACT from TIMESTAMP
    [Fact] public async Task Extract_Hour() => Assert.Equal("14", await Scalar("SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-03-15 14:30:45 UTC')"));
    [Fact] public async Task Extract_Minute() => Assert.Equal("30", await Scalar("SELECT EXTRACT(MINUTE FROM TIMESTAMP '2024-03-15 14:30:45 UTC')"));
    [Fact] public async Task Extract_Second() => Assert.Equal("45", await Scalar("SELECT EXTRACT(SECOND FROM TIMESTAMP '2024-03-15 14:30:45 UTC')"));

    // DATE_ADD
    [Fact] public async Task DateAdd_Day()
    {
        var result = await Scalar("SELECT CAST(DATE_ADD(DATE '2024-01-15', INTERVAL 10 DAY) AS STRING)");
        Assert.Contains("2024-01-25", result!);
    }
    [Fact] public async Task DateAdd_Month()
    {
        var result = await Scalar("SELECT CAST(DATE_ADD(DATE '2024-01-15', INTERVAL 2 MONTH) AS STRING)");
        Assert.Contains("2024-03-15", result!);
    }
    [Fact] public async Task DateAdd_Year()
    {
        var result = await Scalar("SELECT CAST(DATE_ADD(DATE '2024-01-15', INTERVAL 1 YEAR) AS STRING)");
        Assert.Contains("2025-01-15", result!);
    }

    // DATE_SUB
    [Fact] public async Task DateSub_Day()
    {
        var result = await Scalar("SELECT CAST(DATE_SUB(DATE '2024-01-15', INTERVAL 5 DAY) AS STRING)");
        Assert.Contains("2024-01-10", result!);
    }
    [Fact] public async Task DateSub_Month()
    {
        var result = await Scalar("SELECT CAST(DATE_SUB(DATE '2024-03-15', INTERVAL 1 MONTH) AS STRING)");
        Assert.Contains("2024-02-15", result!);
    }

    // DATE_DIFF
    [Fact] public async Task DateDiff_Days() => Assert.Equal("10", await Scalar("SELECT DATE_DIFF(DATE '2024-01-25', DATE '2024-01-15', DAY)"));
    [Fact] public async Task DateDiff_Months() => Assert.Equal("3", await Scalar("SELECT DATE_DIFF(DATE '2024-04-15', DATE '2024-01-15', MONTH)"));
    [Fact] public async Task DateDiff_Years() => Assert.Equal("2", await Scalar("SELECT DATE_DIFF(DATE '2026-01-15', DATE '2024-01-15', YEAR)"));
    [Fact] public async Task DateDiff_Negative() => Assert.Equal("-10", await Scalar("SELECT DATE_DIFF(DATE '2024-01-15', DATE '2024-01-25', DAY)"));

    // DATE_TRUNC
    [Fact] public async Task DateTrunc_Month()
    {
        var result = await Scalar("SELECT CAST(DATE_TRUNC(DATE '2024-03-15', MONTH) AS STRING)");
        Assert.Contains("2024-03-01", result!);
    }
    [Fact] public async Task DateTrunc_Year()
    {
        var result = await Scalar("SELECT CAST(DATE_TRUNC(DATE '2024-06-15', YEAR) AS STRING)");
        Assert.Contains("2024-01-01", result!);
    }

    // TIMESTAMP_ADD
    [Fact] public async Task TimestampAdd_Hour()
    {
        var result = await Scalar("SELECT CAST(TIMESTAMP_ADD(TIMESTAMP '2024-01-15 10:00:00 UTC', INTERVAL 3 HOUR) AS STRING)");
        Assert.Contains("13:00:00", result!);
    }
    [Fact] public async Task TimestampAdd_Minute()
    {
        var result = await Scalar("SELECT CAST(TIMESTAMP_ADD(TIMESTAMP '2024-01-15 10:00:00 UTC', INTERVAL 45 MINUTE) AS STRING)");
        Assert.Contains("10:45:00", result!);
    }

    // TIMESTAMP_DIFF
    [Fact] public async Task TimestampDiff_Hours() => Assert.Equal("3", await Scalar("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-15 13:00:00 UTC', TIMESTAMP '2024-01-15 10:00:00 UTC', HOUR)"));
    [Fact] public async Task TimestampDiff_Minutes() => Assert.Equal("90", await Scalar("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-15 11:30:00 UTC', TIMESTAMP '2024-01-15 10:00:00 UTC', MINUTE)"));
    [Fact] public async Task TimestampDiff_Seconds() => Assert.Equal("3600", await Scalar("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-15 11:00:00 UTC', TIMESTAMP '2024-01-15 10:00:00 UTC', SECOND)"));

    // TIMESTAMP_TRUNC
    [Fact] public async Task TimestampTrunc_Hour()
    {
        var result = await Scalar("SELECT CAST(TIMESTAMP_TRUNC(TIMESTAMP '2024-01-15 14:35:42 UTC', HOUR) AS STRING)");
        Assert.Contains("14:00:00", result!);
    }
    [Fact] public async Task TimestampTrunc_Day()
    {
        var result = await Scalar("SELECT CAST(TIMESTAMP_TRUNC(TIMESTAMP '2024-01-15 14:35:42 UTC', DAY) AS STRING)");
        Assert.Contains("2024-01-15", result!);
    }

    // DATE from parts
    [Fact] public async Task Date_FromParts()
    {
        var result = await Scalar("SELECT CAST(DATE(2024, 3, 15) AS STRING)");
        Assert.Contains("2024-03-15", result!);
    }
    [Fact] public async Task Date_FromTimestamp()
    {
        var result = await Scalar("SELECT CAST(DATE(TIMESTAMP '2024-03-15 10:00:00 UTC') AS STRING)");
        Assert.Contains("2024-03-15", result!);
    }

    // LAST_DAY
    [Fact] public async Task LastDay_January()
    {
        var result = await Scalar("SELECT CAST(LAST_DAY(DATE '2024-01-15', MONTH) AS STRING)");
        Assert.Contains("2024-01-31", result!);
    }
    [Fact] public async Task LastDay_February_Leap()
    {
        var result = await Scalar("SELECT CAST(LAST_DAY(DATE '2024-02-01', MONTH) AS STRING)");
        Assert.Contains("2024-02-29", result!);
    }
    [Fact] public async Task LastDay_February_NonLeap()
    {
        var result = await Scalar("SELECT CAST(LAST_DAY(DATE '2023-02-01', MONTH) AS STRING)");
        Assert.Contains("2023-02-28", result!);
    }

    // UNIX_SECONDS / TIMESTAMP_SECONDS
    [Fact] public async Task UnixSeconds_Epoch() => Assert.Equal("0", await Scalar("SELECT UNIX_SECONDS(TIMESTAMP '1970-01-01 00:00:00 UTC')"));
    [Fact] public async Task TimestampSeconds_Epoch()
    {
        var result = await Scalar("SELECT CAST(TIMESTAMP_SECONDS(0) AS STRING)");
        Assert.Contains("1970-01-01", result!);
    }
    [Fact] public async Task UnixMillis_Epoch() => Assert.Equal("0", await Scalar("SELECT UNIX_MILLIS(TIMESTAMP '1970-01-01 00:00:00 UTC')"));

    // CURRENT functions
    [Fact] public async Task CurrentDate_NotNull() => Assert.NotNull(await Scalar("SELECT CAST(CURRENT_DATE() AS STRING)"));
    [Fact] public async Task CurrentTimestamp_NotNull() => Assert.NotNull(await Scalar("SELECT CAST(CURRENT_TIMESTAMP() AS STRING)"));

    // NULL propagation
    [Fact] public async Task Extract_Null() => Assert.Null(await Scalar("SELECT EXTRACT(YEAR FROM CAST(NULL AS DATE))"));
    [Fact] public async Task DateAdd_NullDate() => Assert.Null(await Scalar("SELECT DATE_ADD(CAST(NULL AS DATE), INTERVAL 1 DAY)"));
    [Fact] public async Task DateDiff_NullArg() => Assert.Null(await Scalar("SELECT DATE_DIFF(CAST(NULL AS DATE), DATE '2024-01-01', DAY)"));
    [Fact] public async Task TimestampDiff_Null() => Assert.Null(await Scalar("SELECT TIMESTAMP_DIFF(CAST(NULL AS TIMESTAMP), TIMESTAMP '2024-01-15 10:00:00 UTC', HOUR)"));

    // Date arithmetic in expressions
    [Fact] public async Task DateAdd_InSelect()
    {
        var result = await Scalar("SELECT CAST(DATE_ADD(DATE '2024-12-25', INTERVAL 7 DAY) AS STRING)");
        Assert.Contains("2025-01-01", result!);
    }
    [Fact] public async Task DateSub_CrossYear()
    {
        var result = await Scalar("SELECT CAST(DATE_SUB(DATE '2024-01-05', INTERVAL 10 DAY) AS STRING)");
        Assert.Contains("2023-12-26", result!);
    }
}