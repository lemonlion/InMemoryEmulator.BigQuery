using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for advanced date/time functions: LAST_DAY, TIME_TRUNC, FORMAT_TIME,
/// PARSE_TIME, PARSE_DATETIME, DATETIME_ADD/SUB/DIFF, DATE_FROM_UNIX_DATE, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DateTimeFunctionAdvancedTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public DateTimeFunctionAdvancedTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_dt_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- LAST_DAY ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#last_day
	[Fact] public async Task LastDay_January() => Assert.Equal("2024-01-31", await S("SELECT CAST(LAST_DAY(DATE '2024-01-15') AS STRING)"));
	[Fact] public async Task LastDay_February_Leap() => Assert.Equal("2024-02-29", await S("SELECT CAST(LAST_DAY(DATE '2024-02-01') AS STRING)"));
	[Fact] public async Task LastDay_February_NonLeap() => Assert.Equal("2023-02-28", await S("SELECT CAST(LAST_DAY(DATE '2023-02-01') AS STRING)"));
	[Fact] public async Task LastDay_April() => Assert.Equal("2024-04-30", await S("SELECT CAST(LAST_DAY(DATE '2024-04-15') AS STRING)"));
	[Fact] public async Task LastDay_December() => Assert.Equal("2024-12-31", await S("SELECT CAST(LAST_DAY(DATE '2024-12-01') AS STRING)"));
	[Fact] public async Task LastDay_Null() => Assert.Null(await S("SELECT LAST_DAY(CAST(NULL AS DATE))"));

	// ---- DATE_FROM_UNIX_DATE ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_from_unix_date
	[Fact] public async Task DateFromUnixDate_Epoch() => Assert.Equal("1970-01-01", await S("SELECT CAST(DATE_FROM_UNIX_DATE(0) AS STRING)"));
	[Fact] public async Task DateFromUnixDate_Day1() => Assert.Equal("1970-01-02", await S("SELECT CAST(DATE_FROM_UNIX_DATE(1) AS STRING)"));
	[Fact] public async Task DateFromUnixDate_Modern() => Assert.Equal("2024-01-01", await S("SELECT CAST(DATE_FROM_UNIX_DATE(19723) AS STRING)"));

	// ---- UNIX_DATE ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#unix_date
	[Fact] public async Task UnixDate_Epoch() => Assert.Equal("0", await S("SELECT UNIX_DATE(DATE '1970-01-01')"));
	[Fact] public async Task UnixDate_Day1() => Assert.Equal("1", await S("SELECT UNIX_DATE(DATE '1970-01-02')"));
	[Fact] public async Task UnixDate_Modern() => Assert.Equal("19723", await S("SELECT UNIX_DATE(DATE '2024-01-01')"));

	// ---- UNIX_SECONDS / TIMESTAMP_SECONDS ----
	[Fact] public async Task UnixSeconds_Epoch() => Assert.Equal("0", await S("SELECT UNIX_SECONDS(TIMESTAMP '1970-01-01 00:00:00 UTC')"));
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_seconds
	[Fact] public async Task TimestampSeconds_Epoch()
	{
		var v = await S("SELECT CAST(TIMESTAMP_SECONDS(0) AS STRING)");
		Assert.NotNull(v);
		Assert.Contains("1970-01-01", v);
	}
	[Fact] public async Task UnixSeconds_Roundtrip()
	{
		var v = await S("SELECT UNIX_SECONDS(TIMESTAMP_SECONDS(1704063600))");
		Assert.Equal("1704063600", v);
	}

	// ---- UNIX_MILLIS / TIMESTAMP_MILLIS ----
	[Fact] public async Task UnixMillis_Epoch() => Assert.Equal("0", await S("SELECT UNIX_MILLIS(TIMESTAMP '1970-01-01 00:00:00 UTC')"));
	[Fact] public async Task TimestampMillis_Roundtrip()
	{
		var v = await S("SELECT UNIX_MILLIS(TIMESTAMP_MILLIS(1704063600000))");
		Assert.Equal("1704063600000", v);
	}

	// ---- UNIX_MICROS / TIMESTAMP_MICROS ----
	[Fact] public async Task UnixMicros_Epoch() => Assert.Equal("0", await S("SELECT UNIX_MICROS(TIMESTAMP '1970-01-01 00:00:00 UTC')"));
	[Fact] public async Task TimestampMicros_Roundtrip()
	{
		var v = await S("SELECT UNIX_MICROS(TIMESTAMP_MICROS(1704063600000000))");
		Assert.Equal("1704063600000000", v);
	}

	// ---- DATE_ADD / DATE_SUB ----
	[Fact] public async Task DateAdd_Day() => Assert.Equal("2024-01-16", await S("SELECT CAST(DATE_ADD(DATE '2024-01-15', INTERVAL 1 DAY) AS STRING)"));
	[Fact] public async Task DateAdd_Month() => Assert.Equal("2024-02-15", await S("SELECT CAST(DATE_ADD(DATE '2024-01-15', INTERVAL 1 MONTH) AS STRING)"));
	[Fact] public async Task DateAdd_Year() => Assert.Equal("2025-01-15", await S("SELECT CAST(DATE_ADD(DATE '2024-01-15', INTERVAL 1 YEAR) AS STRING)"));
	[Fact] public async Task DateSub_Day() => Assert.Equal("2024-01-14", await S("SELECT CAST(DATE_SUB(DATE '2024-01-15', INTERVAL 1 DAY) AS STRING)"));
	[Fact] public async Task DateAdd_Week() => Assert.Equal("2024-01-22", await S("SELECT CAST(DATE_ADD(DATE '2024-01-15', INTERVAL 1 WEEK) AS STRING)"));
	[Fact] public async Task DateAdd_MonthEndOfMonth() => Assert.Equal("2024-02-29", await S("SELECT CAST(DATE_ADD(DATE '2024-01-31', INTERVAL 1 MONTH) AS STRING)"));
	[Fact] public async Task DateSub_Month() => Assert.Equal("2023-12-15", await S("SELECT CAST(DATE_SUB(DATE '2024-01-15', INTERVAL 1 MONTH) AS STRING)"));

	// ---- DATE_DIFF ----
	[Fact] public async Task DateDiff_Days() => Assert.Equal("10", await S("SELECT DATE_DIFF(DATE '2024-01-15', DATE '2024-01-05', DAY)"));
	[Fact] public async Task DateDiff_Months() => Assert.Equal("2", await S("SELECT DATE_DIFF(DATE '2024-03-15', DATE '2024-01-15', MONTH)"));
	[Fact] public async Task DateDiff_Years() => Assert.Equal("1", await S("SELECT DATE_DIFF(DATE '2025-01-01', DATE '2024-01-01', YEAR)"));
	[Fact] public async Task DateDiff_Negative() => Assert.Equal("-10", await S("SELECT DATE_DIFF(DATE '2024-01-05', DATE '2024-01-15', DAY)"));

	// ---- DATE_TRUNC ----
	[Fact] public async Task DateTrunc_Month() => Assert.Equal("2024-01-01", await S("SELECT CAST(DATE_TRUNC(DATE '2024-01-15', MONTH) AS STRING)"));
	[Fact] public async Task DateTrunc_Year() => Assert.Equal("2024-01-01", await S("SELECT CAST(DATE_TRUNC(DATE '2024-06-15', YEAR) AS STRING)"));
	[Fact] public async Task DateTrunc_Week() => Assert.Equal("2024-01-14", await S("SELECT CAST(DATE_TRUNC(DATE '2024-01-17', WEEK) AS STRING)"));

	// ---- DATETIME_ADD / DATETIME_SUB ----
	[Fact] public async Task DatetimeAdd_Hour() => Assert.Contains("11:00:00", await S("SELECT CAST(DATETIME_ADD(DATETIME '2024-01-15 10:00:00', INTERVAL 1 HOUR) AS STRING)") ?? "");
	[Fact] public async Task DatetimeAdd_Minute() => Assert.Contains("10:30:00", await S("SELECT CAST(DATETIME_ADD(DATETIME '2024-01-15 10:00:00', INTERVAL 30 MINUTE) AS STRING)") ?? "");
	[Fact] public async Task DatetimeSub_Day() => Assert.Contains("2024-01-14", await S("SELECT CAST(DATETIME_SUB(DATETIME '2024-01-15 10:00:00', INTERVAL 1 DAY) AS STRING)") ?? "");

	// ---- DATETIME_DIFF ----
	[Fact] public async Task DatetimeDiff_Hours()
	{
		var v = await S("SELECT DATETIME_DIFF(DATETIME '2024-01-15 12:00:00', DATETIME '2024-01-15 10:00:00', HOUR)");
		Assert.Equal("2", v);
	}
	[Fact] public async Task DatetimeDiff_Days()
	{
		var v = await S("SELECT DATETIME_DIFF(DATETIME '2024-01-20 00:00:00', DATETIME '2024-01-15 00:00:00', DAY)");
		Assert.Equal("5", v);
	}

	// ---- DATETIME_TRUNC ----
	[Fact] public async Task DatetimeTrunc_Hour() => Assert.Contains("10:00:00", await S("SELECT CAST(DATETIME_TRUNC(DATETIME '2024-01-15 10:30:45', HOUR) AS STRING)") ?? "");
	[Fact] public async Task DatetimeTrunc_Day() => Assert.Contains("2024-01-15", await S("SELECT CAST(DATETIME_TRUNC(DATETIME '2024-01-15 10:30:45', DAY) AS STRING)") ?? "");

	// ---- TIMESTAMP_ADD / TIMESTAMP_SUB ----
	[Fact] public async Task TimestampAdd_Second()
	{
		var v = await S("SELECT CAST(TIMESTAMP_ADD(TIMESTAMP '2024-01-15 10:00:00 UTC', INTERVAL 30 SECOND) AS STRING)");
		Assert.NotNull(v);
		Assert.Contains("10:00:30", v);
	}
	[Fact] public async Task TimestampSub_Hour()
	{
		var v = await S("SELECT CAST(TIMESTAMP_SUB(TIMESTAMP '2024-01-15 10:00:00 UTC', INTERVAL 2 HOUR) AS STRING)");
		Assert.NotNull(v);
		Assert.Contains("08:00:00", v);
	}

	// ---- TIMESTAMP_DIFF ----
	[Fact] public async Task TimestampDiff_Seconds()
	{
		var v = await S("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-15 10:00:30 UTC', TIMESTAMP '2024-01-15 10:00:00 UTC', SECOND)");
		Assert.Equal("30", v);
	}

	// ---- TIMESTAMP_TRUNC ----
	[Fact] public async Task TimestampTrunc_Day()
	{
		var v = await S("SELECT CAST(TIMESTAMP_TRUNC(TIMESTAMP '2024-01-15 10:30:45 UTC', DAY) AS STRING)");
		Assert.NotNull(v);
		Assert.Contains("2024-01-15", v);
	}

	// ---- TIME_ADD / TIME_SUB / TIME_DIFF ----
	[Fact] public async Task TimeAdd_Hour()
	{
		var v = await S("SELECT CAST(TIME_ADD(TIME '10:00:00', INTERVAL 2 HOUR) AS STRING)");
		Assert.NotNull(v);
		Assert.Contains("12:00:00", v);
	}
	[Fact] public async Task TimeSub_Minute()
	{
		var v = await S("SELECT CAST(TIME_SUB(TIME '10:30:00', INTERVAL 30 MINUTE) AS STRING)");
		Assert.NotNull(v);
		Assert.Contains("10:00:00", v);
	}
	[Fact] public async Task TimeDiff_Hours()
	{
		var v = await S("SELECT TIME_DIFF(TIME '12:00:00', TIME '10:00:00', HOUR)");
		Assert.Equal("2", v);
	}

	// ---- TIME_TRUNC ----
	[Fact] public async Task TimeTrunc_Hour()
	{
		var v = await S("SELECT CAST(TIME_TRUNC(TIME '10:30:45', HOUR) AS STRING)");
		Assert.NotNull(v);
		Assert.Contains("10:00:00", v);
	}

	// ---- FORMAT_DATE ----
	[Fact] public async Task FormatDate_YMD() => Assert.Equal("2024-01-15", await S("SELECT FORMAT_DATE('%Y-%m-%d', DATE '2024-01-15')"));
	[Fact] public async Task FormatDate_MonthName() => Assert.Contains("Jan", await S("SELECT FORMAT_DATE('%b %d, %Y', DATE '2024-01-15')") ?? "");
	[Fact] public async Task FormatDate_Null() => Assert.Null(await S("SELECT FORMAT_DATE('%Y', CAST(NULL AS DATE))"));

	// ---- PARSE_DATE ----
	[Fact] public async Task ParseDate_YMD() => Assert.Equal("2024-01-15", await S("SELECT CAST(PARSE_DATE('%Y-%m-%d', '2024-01-15') AS STRING)"));
	[Fact] public async Task ParseDate_MDY() => Assert.Equal("2024-01-15", await S("SELECT CAST(PARSE_DATE('%m/%d/%Y', '01/15/2024') AS STRING)"));

	// ---- FORMAT_TIMESTAMP ----
	[Fact] public async Task FormatTimestamp_Basic()
	{
		var v = await S("SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP '2024-01-15 10:30:00 UTC')");
		Assert.NotNull(v);
		Assert.Contains("2024-01-15", v);
	}

	// ---- FORMAT_DATETIME ----
	[Fact] public async Task FormatDatetime_Basic()
	{
		var v = await S("SELECT FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', DATETIME '2024-01-15 10:30:00')");
		Assert.NotNull(v);
		Assert.Contains("2024-01-15", v);
	}

	// ---- FORMAT_TIME ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#format_time
	[Fact] public async Task FormatTime_Basic()
	{
		var v = await S("SELECT FORMAT_TIME('%H:%M', TIME '10:30:00')");
		Assert.Equal("10:30", v);
	}
	[Fact] public async Task FormatTime_Full()
	{
		var v = await S("SELECT FORMAT_TIME('%H:%M:%S', TIME '10:30:45')");
		Assert.Equal("10:30:45", v);
	}

	// ---- PARSE_TIMESTAMP ----
	[Fact] public async Task ParseTimestamp_Basic()
	{
		var v = await S("SELECT CAST(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', '2024-01-15 10:30:00') AS STRING)");
		Assert.NotNull(v);
		Assert.Contains("2024-01-15", v);
	}

	// ---- PARSE_DATETIME ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#parse_datetime
	[Fact] public async Task ParseDatetime_Basic()
	{
		var v = await S("SELECT CAST(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', '2024-01-15 10:30:00') AS STRING)");
		Assert.NotNull(v);
		Assert.Contains("2024-01-15", v);
	}

	// ---- PARSE_TIME ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#parse_time
	[Fact] public async Task ParseTime_Basic()
	{
		var v = await S("SELECT CAST(PARSE_TIME('%H:%M:%S', '10:30:45') AS STRING)");
		Assert.NotNull(v);
		Assert.Contains("10:30:45", v);
	}

	// ---- EXTRACT ----
	[Fact] public async Task Extract_Year() => Assert.Equal("2024", await S("SELECT EXTRACT(YEAR FROM DATE '2024-06-15')"));
	[Fact] public async Task Extract_Month() => Assert.Equal("6", await S("SELECT EXTRACT(MONTH FROM DATE '2024-06-15')"));
	[Fact] public async Task Extract_Day() => Assert.Equal("15", await S("SELECT EXTRACT(DAY FROM DATE '2024-06-15')"));
	[Fact] public async Task Extract_Quarter() => Assert.Equal("2", await S("SELECT EXTRACT(QUARTER FROM DATE '2024-06-15')"));
	[Fact] public async Task Extract_DayOfWeek() => Assert.NotNull(await S("SELECT EXTRACT(DAYOFWEEK FROM DATE '2024-01-15')"));
	[Fact] public async Task Extract_DayOfYear() => Assert.Equal("15", await S("SELECT EXTRACT(DAYOFYEAR FROM DATE '2024-01-15')"));
	[Fact] public async Task Extract_Week() => Assert.NotNull(await S("SELECT EXTRACT(WEEK FROM DATE '2024-01-15')"));
	[Fact] public async Task Extract_HourFromDatetime() => Assert.Equal("10", await S("SELECT EXTRACT(HOUR FROM DATETIME '2024-01-15 10:30:00')"));
	[Fact] public async Task Extract_MinuteFromDatetime() => Assert.Equal("30", await S("SELECT EXTRACT(MINUTE FROM DATETIME '2024-01-15 10:30:00')"));
	[Fact] public async Task Extract_SecondFromDatetime() => Assert.Equal("45", await S("SELECT EXTRACT(SECOND FROM DATETIME '2024-01-15 10:30:45')"));
	[Fact] public async Task Extract_HourFromTimestamp() => Assert.NotNull(await S("SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-01-15 10:30:00 UTC')"));
	[Fact] public async Task Extract_HourFromTime() => Assert.Equal("10", await S("SELECT EXTRACT(HOUR FROM TIME '10:30:00')"));

	// ---- DATE constructor ----
	[Fact] public async Task Date_FromParts() => Assert.Equal("2024-06-15", await S("SELECT CAST(DATE(2024, 6, 15) AS STRING)"));

	// ---- DATETIME constructor ----
	[Fact] public async Task Datetime_FromDateAndTime()
	{
		var v = await S("SELECT CAST(DATETIME(DATE '2024-01-15', TIME '10:30:00') AS STRING)");
		Assert.NotNull(v);
		Assert.Contains("2024-01-15", v);
		Assert.Contains("10:30:00", v);
	}

	// ---- TIMESTAMP constructor ----
	[Fact] public async Task Timestamp_FromString()
	{
		var v = await S("SELECT CAST(TIMESTAMP('2024-01-15 10:00:00+00') AS STRING)");
		Assert.NotNull(v);
		Assert.Contains("2024-01-15", v);
	}

	// ---- CURRENT_* ----
	[Fact] public async Task CurrentDate_NotNull() => Assert.NotNull(await S("SELECT CAST(CURRENT_DATE() AS STRING)"));
	[Fact] public async Task CurrentDatetime_NotNull() => Assert.NotNull(await S("SELECT CAST(CURRENT_DATETIME() AS STRING)"));
	[Fact] public async Task CurrentTimestamp_NotNull() => Assert.NotNull(await S("SELECT CAST(CURRENT_TIMESTAMP() AS STRING)"));
	[Fact] public async Task CurrentTime_NotNull() => Assert.NotNull(await S("SELECT CAST(CURRENT_TIME() AS STRING)"));
}
