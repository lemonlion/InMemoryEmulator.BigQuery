using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for advanced date/time arithmetic and interval handling.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DateTimeArithmeticAdvancedTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public DateTimeArithmeticAdvancedTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_dta_{Guid.NewGuid():N}"[..30];
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

	// DATE_ADD
	[Fact] public async Task DateAdd_Days() => Assert.Equal("2024-01-20", await Scalar("SELECT CAST(DATE_ADD(DATE '2024-01-15', INTERVAL 5 DAY) AS STRING)"));
	[Fact] public async Task DateAdd_Weeks() => Assert.Equal("2024-01-29", await Scalar("SELECT CAST(DATE_ADD(DATE '2024-01-15', INTERVAL 2 WEEK) AS STRING)"));
	[Fact] public async Task DateAdd_Months() => Assert.Equal("2024-04-15", await Scalar("SELECT CAST(DATE_ADD(DATE '2024-01-15', INTERVAL 3 MONTH) AS STRING)"));
	[Fact] public async Task DateAdd_Years() => Assert.Equal("2026-01-15", await Scalar("SELECT CAST(DATE_ADD(DATE '2024-01-15', INTERVAL 2 YEAR) AS STRING)"));
	[Fact] public async Task DateAdd_NegativeDays() => Assert.Equal("2024-01-10", await Scalar("SELECT CAST(DATE_ADD(DATE '2024-01-15', INTERVAL -5 DAY) AS STRING)"));
	[Fact] public async Task DateAdd_MonthEndRollover() => Assert.Equal("2024-02-29", await Scalar("SELECT CAST(DATE_ADD(DATE '2024-01-31', INTERVAL 1 MONTH) AS STRING)"));

	// DATE_SUB
	[Fact] public async Task DateSub_Days() => Assert.Equal("2024-01-10", await Scalar("SELECT CAST(DATE_SUB(DATE '2024-01-15', INTERVAL 5 DAY) AS STRING)"));
	[Fact] public async Task DateSub_Months() => Assert.Equal("2023-10-15", await Scalar("SELECT CAST(DATE_SUB(DATE '2024-01-15', INTERVAL 3 MONTH) AS STRING)"));
	[Fact] public async Task DateSub_Years() => Assert.Equal("2022-01-15", await Scalar("SELECT CAST(DATE_SUB(DATE '2024-01-15', INTERVAL 2 YEAR) AS STRING)"));

	// DATE_DIFF
	[Fact] public async Task DateDiff_Days() => Assert.Equal("10", await Scalar("SELECT DATE_DIFF(DATE '2024-01-25', DATE '2024-01-15', DAY)"));
	[Fact] public async Task DateDiff_Weeks() => Assert.Equal("4", await Scalar("SELECT DATE_DIFF(DATE '2024-02-15', DATE '2024-01-15', WEEK)"));
	[Fact] public async Task DateDiff_Months() => Assert.Equal("3", await Scalar("SELECT DATE_DIFF(DATE '2024-04-15', DATE '2024-01-15', MONTH)"));
	[Fact] public async Task DateDiff_Years() => Assert.Equal("2", await Scalar("SELECT DATE_DIFF(DATE '2026-01-15', DATE '2024-01-15', YEAR)"));
	[Fact] public async Task DateDiff_Negative() => Assert.Equal("-10", await Scalar("SELECT DATE_DIFF(DATE '2024-01-05', DATE '2024-01-15', DAY)"));

	// DATE_TRUNC
	[Fact] public async Task DateTrunc_Day() => Assert.Equal("2024-01-15", await Scalar("SELECT CAST(DATE_TRUNC(DATE '2024-01-15', DAY) AS STRING)"));
	[Fact] public async Task DateTrunc_Month() => Assert.Equal("2024-01-01", await Scalar("SELECT CAST(DATE_TRUNC(DATE '2024-01-15', MONTH) AS STRING)"));
	[Fact] public async Task DateTrunc_Year() => Assert.Equal("2024-01-01", await Scalar("SELECT CAST(DATE_TRUNC(DATE '2024-03-15', YEAR) AS STRING)"));
	[Fact] public async Task DateTrunc_Week() => Assert.Contains("2024-01-1", await Scalar("SELECT CAST(DATE_TRUNC(DATE '2024-01-17', WEEK) AS STRING)") ?? "");

	// TIMESTAMP_ADD
	[Fact] public async Task TimestampAdd_Seconds() => Assert.Contains("10:30:30", await Scalar("SELECT CAST(TIMESTAMP_ADD(TIMESTAMP '2024-01-15 10:30:00 UTC', INTERVAL 30 SECOND) AS STRING)") ?? "");
	[Fact] public async Task TimestampAdd_Minutes() => Assert.Contains("11:00:00", await Scalar("SELECT CAST(TIMESTAMP_ADD(TIMESTAMP '2024-01-15 10:30:00 UTC', INTERVAL 30 MINUTE) AS STRING)") ?? "");
	[Fact] public async Task TimestampAdd_Hours() => Assert.Contains("12:30:00", await Scalar("SELECT CAST(TIMESTAMP_ADD(TIMESTAMP '2024-01-15 10:30:00 UTC', INTERVAL 2 HOUR) AS STRING)") ?? "");

	// TIMESTAMP_DIFF
	[Fact] public async Task TimestampDiff_Seconds() => Assert.Equal("3600", await Scalar("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-15 11:30:00 UTC', TIMESTAMP '2024-01-15 10:30:00 UTC', SECOND)"));
	[Fact] public async Task TimestampDiff_Minutes() => Assert.Equal("60", await Scalar("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-15 11:30:00 UTC', TIMESTAMP '2024-01-15 10:30:00 UTC', MINUTE)"));
	[Fact] public async Task TimestampDiff_Hours() => Assert.Equal("24", await Scalar("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-16 10:30:00 UTC', TIMESTAMP '2024-01-15 10:30:00 UTC', HOUR)"));
	[Fact] public async Task TimestampDiff_Days() => Assert.Equal("7", await Scalar("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-22 10:30:00 UTC', TIMESTAMP '2024-01-15 10:30:00 UTC', DAY)"));

	// TIMESTAMP_TRUNC
	[Fact] public async Task TimestampTrunc_Hour() => Assert.Contains("10:00:00", await Scalar("SELECT CAST(TIMESTAMP_TRUNC(TIMESTAMP '2024-01-15 10:30:45 UTC', HOUR) AS STRING)") ?? "");
	[Fact] public async Task TimestampTrunc_Day() => Assert.Contains("2024-01-15", await Scalar("SELECT CAST(TIMESTAMP_TRUNC(TIMESTAMP '2024-01-15 10:30:45 UTC', DAY) AS STRING)") ?? "");

	// DATETIME_ADD
	[Fact] public async Task DatetimeAdd_Hours() => Assert.Contains("12:30:00", await Scalar("SELECT CAST(DATETIME_ADD(DATETIME '2024-01-15 10:30:00', INTERVAL 2 HOUR) AS STRING)") ?? "");
	[Fact] public async Task DatetimeAdd_Days() => Assert.Contains("2024-01-17", await Scalar("SELECT CAST(DATETIME_ADD(DATETIME '2024-01-15 10:30:00', INTERVAL 2 DAY) AS STRING)") ?? "");
	[Fact] public async Task DatetimeAdd_Months() => Assert.Contains("2024-04-15", await Scalar("SELECT CAST(DATETIME_ADD(DATETIME '2024-01-15 10:30:00', INTERVAL 3 MONTH) AS STRING)") ?? "");

	// DATETIME_DIFF
	[Fact] public async Task DatetimeDiff_Days() => Assert.Equal("10", await Scalar("SELECT DATETIME_DIFF(DATETIME '2024-01-25 10:00:00', DATETIME '2024-01-15 10:00:00', DAY)"));
	[Fact] public async Task DatetimeDiff_Hours() => Assert.Equal("48", await Scalar("SELECT DATETIME_DIFF(DATETIME '2024-01-17 10:00:00', DATETIME '2024-01-15 10:00:00', HOUR)"));

	// TIME_ADD / TIME_SUB
	[Fact] public async Task TimeAdd_Minutes() => Assert.Contains("11:00:00", await Scalar("SELECT CAST(TIME_ADD(TIME '10:30:00', INTERVAL 30 MINUTE) AS STRING)") ?? "");
	[Fact] public async Task TimeAdd_Hours() => Assert.Contains("12:30:00", await Scalar("SELECT CAST(TIME_ADD(TIME '10:30:00', INTERVAL 2 HOUR) AS STRING)") ?? "");
	[Fact] public async Task TimeSub_Minutes() => Assert.Contains("10:00:00", await Scalar("SELECT CAST(TIME_SUB(TIME '10:30:00', INTERVAL 30 MINUTE) AS STRING)") ?? "");

	// TIME_DIFF
	[Fact] public async Task TimeDiff_Seconds() => Assert.Equal("3600", await Scalar("SELECT TIME_DIFF(TIME '11:30:00', TIME '10:30:00', SECOND)"));
	[Fact] public async Task TimeDiff_Minutes() => Assert.Equal("60", await Scalar("SELECT TIME_DIFF(TIME '11:30:00', TIME '10:30:00', MINUTE)"));
	[Fact] public async Task TimeDiff_Hours() => Assert.Equal("1", await Scalar("SELECT TIME_DIFF(TIME '11:30:00', TIME '10:30:00', HOUR)"));

	// EXTRACT
	[Fact] public async Task Extract_YearFromDate() => Assert.Equal("2024", await Scalar("SELECT EXTRACT(YEAR FROM DATE '2024-03-15')"));
	[Fact] public async Task Extract_MonthFromDate() => Assert.Equal("3", await Scalar("SELECT EXTRACT(MONTH FROM DATE '2024-03-15')"));
	[Fact] public async Task Extract_DayFromDate() => Assert.Equal("15", await Scalar("SELECT EXTRACT(DAY FROM DATE '2024-03-15')"));
	[Fact] public async Task Extract_DayOfWeekFromDate() => Assert.NotNull(await Scalar("SELECT EXTRACT(DAYOFWEEK FROM DATE '2024-03-15')"));
	[Fact] public async Task Extract_DayOfYearFromDate() => Assert.Equal("75", await Scalar("SELECT EXTRACT(DAYOFYEAR FROM DATE '2024-03-15')"));
	[Fact] public async Task Extract_HourFromTimestamp() => Assert.Equal("10", await Scalar("SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-01-15 10:30:45 UTC')"));
	[Fact] public async Task Extract_MinuteFromTimestamp() => Assert.Equal("30", await Scalar("SELECT EXTRACT(MINUTE FROM TIMESTAMP '2024-01-15 10:30:45 UTC')"));
	[Fact] public async Task Extract_SecondFromTimestamp() => Assert.Equal("45", await Scalar("SELECT EXTRACT(SECOND FROM TIMESTAMP '2024-01-15 10:30:45 UTC')"));

	// CURRENT_DATE, CURRENT_TIMESTAMP, CURRENT_DATETIME, CURRENT_TIME
	[Fact] public async Task CurrentDate_NotNull() => Assert.NotNull(await Scalar("SELECT CAST(CURRENT_DATE() AS STRING)"));
	[Fact] public async Task CurrentTimestamp_NotNull() => Assert.NotNull(await Scalar("SELECT CAST(CURRENT_TIMESTAMP() AS STRING)"));
	[Fact] public async Task CurrentDatetime_NotNull() => Assert.NotNull(await Scalar("SELECT CAST(CURRENT_DATETIME() AS STRING)"));
	[Fact] public async Task CurrentTime_NotNull() => Assert.NotNull(await Scalar("SELECT CAST(CURRENT_TIME() AS STRING)"));

	// LAST_DAY
	[Fact] public async Task LastDay_January() => Assert.Equal("2024-01-31", await Scalar("SELECT CAST(LAST_DAY(DATE '2024-01-15') AS STRING)"));
	[Fact] public async Task LastDay_February_Leap() => Assert.Equal("2024-02-29", await Scalar("SELECT CAST(LAST_DAY(DATE '2024-02-01') AS STRING)"));
	[Fact] public async Task LastDay_February_NonLeap() => Assert.Equal("2023-02-28", await Scalar("SELECT CAST(LAST_DAY(DATE '2023-02-15') AS STRING)"));

	// NULL handling
	[Fact] public async Task DateAdd_NullDate() => Assert.Null(await Scalar("SELECT DATE_ADD(NULL, INTERVAL 5 DAY)"));
	[Fact] public async Task DateDiff_NullFirst() => Assert.Null(await Scalar("SELECT DATE_DIFF(NULL, DATE '2024-01-15', DAY)"));
	[Fact] public async Task DateDiff_NullSecond() => Assert.Null(await Scalar("SELECT DATE_DIFF(DATE '2024-01-15', NULL, DAY)"));
}
