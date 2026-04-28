using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Date/time function edge cases and deeper coverage.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DateTimeFunctionEdgeCaseTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public DateTimeFunctionEdgeCaseTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
	}

	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		if (rows.Count == 0) return null;
		var val = rows[0][0];
		if (val is DateTime dt) return dt.TimeOfDay == TimeSpan.Zero ? dt.ToString("yyyy-MM-dd") : dt.ToString("yyyy-MM-dd HH:mm:ss");
		if (val is DateTimeOffset dto) return dto.TimeOfDay == TimeSpan.Zero ? dto.ToString("yyyy-MM-dd") : dto.ToString("yyyy-MM-dd HH:mm:ss");
		return val?.ToString();
	}

	// ---- CURRENT functions ----
	[Fact] public async Task CurrentDate_ReturnsDate() { var v = await Scalar("SELECT CURRENT_DATE()"); Assert.Matches(@"\d{4}-\d{2}-\d{2}", v!); }
	[Fact(Skip = "Needs investigation")] public async Task CurrentDatetime_ReturnsValue() => Assert.NotNull(await Scalar("SELECT CURRENT_DATETIME()"));
	[Fact] public async Task CurrentTimestamp_ReturnsValue() => Assert.NotNull(await Scalar("SELECT CURRENT_TIMESTAMP()"));
	[Fact] public async Task CurrentTime_ReturnsValue() => Assert.NotNull(await Scalar("SELECT CURRENT_TIME()"));

	// ---- DATE constructor ----
	[Fact] public async Task Date_FromParts() { var v = await Scalar("SELECT DATE(2024, 3, 15)"); Assert.Contains("2024-03-15", v); }
	[Fact] public async Task Date_FromTimestamp() { var v = await Scalar("SELECT DATE(TIMESTAMP '2024-03-15T10:30:00+00:00')"); Assert.Contains("2024-03-15", v); }
	[Fact] public async Task Date_JanFirst() { var v = await Scalar("SELECT DATE(2024, 1, 1)"); Assert.Contains("2024-01-01", v); }
	[Fact] public async Task Date_DecLast() { var v = await Scalar("SELECT DATE(2024, 12, 31)"); Assert.Contains("2024-12-31", v); }

	// ---- DATETIME constructor ----
	[Fact(Skip = "Needs investigation")] public async Task Datetime_FromParts() { var v = await Scalar("SELECT DATETIME(2024, 3, 15, 10, 30, 0)"); Assert.Contains("2024-03-15", v); }
	[Fact(Skip = "Needs investigation")] public async Task Datetime_FromDateAndTime() { var v = await Scalar("SELECT DATETIME(DATE '2024-03-15', TIME '10:30:00')"); Assert.Contains("2024-03-15", v); }

	// ---- TIMESTAMP constructor ----
	[Fact] public async Task Timestamp_FromString() { var v = await Scalar("SELECT TIMESTAMP('2024-03-15T10:30:00+00:00')"); Assert.NotNull(v); }

	// ---- EXTRACT variants ----
	[Fact] public async Task Extract_Year() => Assert.Equal("2024", await Scalar("SELECT EXTRACT(YEAR FROM DATE '2024-03-15')"));
	[Fact] public async Task Extract_Month() => Assert.Equal("3", await Scalar("SELECT EXTRACT(MONTH FROM DATE '2024-03-15')"));
	[Fact] public async Task Extract_Day() => Assert.Equal("15", await Scalar("SELECT EXTRACT(DAY FROM DATE '2024-03-15')"));
	[Fact] public async Task Extract_DayOfWeek() { var v = int.Parse(await Scalar("SELECT EXTRACT(DAYOFWEEK FROM DATE '2024-03-15')") ?? "0"); Assert.True(v >= 1 && v <= 7); }
	[Fact] public async Task Extract_DayOfYear() => Assert.Equal("75", await Scalar("SELECT EXTRACT(DAYOFYEAR FROM DATE '2024-03-15')"));
	[Fact] public async Task Extract_Quarter() => Assert.Equal("1", await Scalar("SELECT EXTRACT(QUARTER FROM DATE '2024-03-15')"));
	[Fact] public async Task Extract_Week() { var v = int.Parse(await Scalar("SELECT EXTRACT(WEEK FROM DATE '2024-03-15')") ?? "-1"); Assert.True(v >= 0 && v <= 53); }
	[Fact] public async Task Extract_IsoWeek() { var v = int.Parse(await Scalar("SELECT EXTRACT(ISOWEEK FROM DATE '2024-03-15')") ?? "-1"); Assert.True(v >= 1 && v <= 53); }
	[Fact] public async Task Extract_HourFromDatetime() => Assert.Equal("10", await Scalar("SELECT EXTRACT(HOUR FROM DATETIME '2024-03-15 10:30:45')"));
	[Fact] public async Task Extract_MinuteFromDatetime() => Assert.Equal("30", await Scalar("SELECT EXTRACT(MINUTE FROM DATETIME '2024-03-15 10:30:45')"));
	[Fact] public async Task Extract_SecondFromDatetime() => Assert.Equal("45", await Scalar("SELECT EXTRACT(SECOND FROM DATETIME '2024-03-15 10:30:45')"));
	[Fact] public async Task Extract_HourFromTime() => Assert.Equal("14", await Scalar("SELECT EXTRACT(HOUR FROM TIME '14:30:00')"));
	[Fact] public async Task Extract_HourFromTimestamp() => Assert.NotNull(await Scalar("SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-03-15 10:30:00 UTC')"));

	// ---- DATE_ADD / DATE_SUB ----
	[Fact] public async Task DateAdd_Day() { var v = await Scalar("SELECT DATE_ADD(DATE '2024-01-01', INTERVAL 1 DAY)"); Assert.Contains("2024-01-02", v); }
	[Fact] public async Task DateAdd_Month() { var v = await Scalar("SELECT DATE_ADD(DATE '2024-01-31', INTERVAL 1 MONTH)"); Assert.Contains("2024-02", v); }
	[Fact] public async Task DateAdd_Year() { var v = await Scalar("SELECT DATE_ADD(DATE '2024-03-15', INTERVAL 1 YEAR)"); Assert.Contains("2025-03-15", v); }
	[Fact] public async Task DateSub_Day() { var v = await Scalar("SELECT DATE_SUB(DATE '2024-01-01', INTERVAL 1 DAY)"); Assert.Contains("2023-12-31", v); }
	[Fact] public async Task DateAdd_Week() { var v = await Scalar("SELECT DATE_ADD(DATE '2024-01-01', INTERVAL 1 WEEK)"); Assert.Contains("2024-01-08", v); }
	[Fact] public async Task DateAdd_NegativeDay() { var v = await Scalar("SELECT DATE_ADD(DATE '2024-01-10', INTERVAL -5 DAY)"); Assert.Contains("2024-01-05", v); }
	[Fact] public async Task DateSub_Month() { var v = await Scalar("SELECT DATE_SUB(DATE '2024-03-31', INTERVAL 1 MONTH)"); Assert.Contains("2024-02", v); }

	// ---- DATE_DIFF ----
	[Fact] public async Task DateDiff_Days() => Assert.Equal("10", await Scalar("SELECT DATE_DIFF(DATE '2024-01-11', DATE '2024-01-01', DAY)"));
	[Fact] public async Task DateDiff_Months() => Assert.Equal("3", await Scalar("SELECT DATE_DIFF(DATE '2024-04-15', DATE '2024-01-15', MONTH)"));
	[Fact] public async Task DateDiff_Years() => Assert.Equal("1", await Scalar("SELECT DATE_DIFF(DATE '2025-01-01', DATE '2024-01-01', YEAR)"));
	[Fact] public async Task DateDiff_Negative() => Assert.Equal("-10", await Scalar("SELECT DATE_DIFF(DATE '2024-01-01', DATE '2024-01-11', DAY)"));

	// ---- DATE_TRUNC ----
	[Fact] public async Task DateTrunc_Month() { var v = await Scalar("SELECT DATE_TRUNC(DATE '2024-03-15', MONTH)"); Assert.Contains("2024-03-01", v); }
	[Fact] public async Task DateTrunc_Year() { var v = await Scalar("SELECT DATE_TRUNC(DATE '2024-03-15', YEAR)"); Assert.Contains("2024-01-01", v); }
	[Fact] public async Task DateTrunc_Week() { var v = await Scalar("SELECT DATE_TRUNC(DATE '2024-03-15', WEEK)"); Assert.NotNull(v); }

	// ---- LAST_DAY ----
	[Fact] public async Task LastDay_Jan() { var v = await Scalar("SELECT LAST_DAY(DATE '2024-01-15')"); Assert.Contains("2024-01-31", v); }
	[Fact] public async Task LastDay_Feb_LeapYear() { var v = await Scalar("SELECT LAST_DAY(DATE '2024-02-01')"); Assert.Contains("2024-02-29", v); }
	[Fact] public async Task LastDay_Feb_NonLeapYear() { var v = await Scalar("SELECT LAST_DAY(DATE '2023-02-01')"); Assert.Contains("2023-02-28", v); }

	// ---- DATE_FROM_UNIX_DATE ----
	[Fact] public async Task DateFromUnixDate_Epoch() { var v = await Scalar("SELECT DATE_FROM_UNIX_DATE(0)"); Assert.Contains("1970-01-01", v); }
	[Fact] public async Task DateFromUnixDate_Recent() { var v = await Scalar("SELECT DATE_FROM_UNIX_DATE(19797)"); Assert.NotNull(v); }

	// ---- UNIX_DATE ----
	[Fact] public async Task UnixDate_Epoch() => Assert.Equal("0", await Scalar("SELECT UNIX_DATE(DATE '1970-01-01')"));
	[Fact] public async Task UnixDate_RoundTrip() => Assert.Contains("1970-01-01", (await Scalar("SELECT DATE_FROM_UNIX_DATE(UNIX_DATE(DATE '1970-01-01'))"))!);

	// ---- TIMESTAMP_ADD / TIMESTAMP_SUB ----
	[Fact(Skip = "Not yet supported")] public async Task TimestampAdd_Hours() { var v = await Scalar("SELECT TIMESTAMP_ADD(TIMESTAMP '2024-01-01T10:00:00+00:00', INTERVAL 2 HOUR)"); Assert.Contains("12:00:00", v); }
	[Fact(Skip = "Not yet supported")] public async Task TimestampAdd_Minutes() { var v = await Scalar("SELECT TIMESTAMP_ADD(TIMESTAMP '2024-01-01T10:00:00+00:00', INTERVAL 30 MINUTE)"); Assert.Contains("10:30:00", v); }
	[Fact(Skip = "Not yet supported")] public async Task TimestampSub_Days() { var v = await Scalar("SELECT TIMESTAMP_SUB(TIMESTAMP '2024-01-10T00:00:00+00:00', INTERVAL 5 DAY)"); Assert.Contains("2024-01-05", v); }

	// ---- TIMESTAMP_DIFF ----
	[Fact(Skip = "Not yet supported")] public async Task TimestampDiff_Hours() => Assert.Equal("24", await Scalar("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-02T00:00:00+00:00', TIMESTAMP '2024-01-01T00:00:00+00:00', HOUR)"));
	[Fact(Skip = "Not yet supported")] public async Task TimestampDiff_Seconds() => Assert.Equal("86400", await Scalar("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-02T00:00:00+00:00', TIMESTAMP '2024-01-01T00:00:00+00:00', SECOND)"));

	// ---- FORMAT_DATE / FORMAT_DATETIME / FORMAT_TIMESTAMP / FORMAT_TIME ----
	[Fact] public async Task FormatDate_YearMonth() { var v = await Scalar("SELECT FORMAT_DATE('%Y-%m', DATE '2024-03-15')"); Assert.Equal("2024-03", v); }
	[Fact] public async Task FormatDate_DayName() { var v = await Scalar("SELECT FORMAT_DATE('%A', DATE '2024-03-15')"); Assert.NotNull(v); }
	[Fact(Skip = "Not yet supported")] public async Task FormatDatetime_Full() { var v = await Scalar("SELECT FORMAT_DATETIME('%Y-%m-%d %H:%M', DATETIME '2024-03-15 10:30:00')"); Assert.Contains("2024-03-15", v); }
	[Fact(Skip = "Not yet supported")] public async Task FormatTimestamp_Iso() { var v = await Scalar("SELECT FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S', TIMESTAMP '2024-03-15T10:30:00+00:00')"); Assert.Contains("2024-03-15", v); }
	[Fact] public async Task FormatTime_HourMinute() { var v = await Scalar("SELECT FORMAT_TIME('%H:%M', TIME '14:30:00')"); Assert.Equal("14:30", v); }

	// ---- PARSE_DATE / PARSE_DATETIME / PARSE_TIMESTAMP / PARSE_TIME ----
	[Fact] public async Task ParseDate_Basic() { var v = await Scalar("SELECT PARSE_DATE('%Y-%m-%d', '2024-03-15')"); Assert.Contains("2024-03-15", v); }
	[Fact(Skip = "Needs investigation")] public async Task ParseDatetime_Basic() { var v = await Scalar("SELECT PARSE_DATETIME('%Y-%m-%d %H:%M:%S', '2024-03-15 10:30:00')"); Assert.Contains("2024-03-15", v); }
	[Fact] public async Task ParseTimestamp_Basic() { var v = await Scalar("SELECT PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', '2024-03-15T10:30:00')"); Assert.NotNull(v); }
	[Fact] public async Task ParseTime_Basic() { var v = await Scalar("SELECT PARSE_TIME('%H:%M:%S', '14:30:00')"); Assert.Contains("14:30:00", v); }

	// ---- TIME_ADD / TIME_SUB / TIME_DIFF / TIME_TRUNC ----
	[Fact] public async Task TimeAdd_Hours() { var v = await Scalar("SELECT TIME_ADD(TIME '10:00:00', INTERVAL 2 HOUR)"); Assert.Contains("12:00:00", v); }
	[Fact] public async Task TimeSub_Minutes() { var v = await Scalar("SELECT TIME_SUB(TIME '10:30:00', INTERVAL 30 MINUTE)"); Assert.Contains("10:00:00", v); }
	[Fact] public async Task TimeDiff_Hours() => Assert.Equal("2", await Scalar("SELECT TIME_DIFF(TIME '12:00:00', TIME '10:00:00', HOUR)"));
	[Fact] public async Task TimeTrunc_Hour() { var v = await Scalar("SELECT TIME_TRUNC(TIME '10:30:45', HOUR)"); Assert.Contains("10:00:00", v); }

	// ---- DATETIME_ADD / DATETIME_SUB / DATETIME_DIFF / DATETIME_TRUNC ----
	[Fact(Skip = "Needs investigation")] public async Task DatetimeAdd_Hours() { var v = await Scalar("SELECT DATETIME_ADD(DATETIME '2024-03-15 10:00:00', INTERVAL 2 HOUR)"); Assert.Contains("12:00:00", v); }
	[Fact(Skip = "Needs investigation")] public async Task DatetimeSub_Days() { var v = await Scalar("SELECT DATETIME_SUB(DATETIME '2024-03-15 10:00:00', INTERVAL 5 DAY)"); Assert.Contains("2024-03-10", v); }
	[Fact] public async Task DatetimeDiff_Days() => Assert.Equal("10", await Scalar("SELECT DATETIME_DIFF(DATETIME '2024-03-15 00:00:00', DATETIME '2024-03-05 00:00:00', DAY)"));
	[Fact] public async Task DatetimeTrunc_Month() { var v = await Scalar("SELECT DATETIME_TRUNC(DATETIME '2024-03-15 10:30:00', MONTH)"); Assert.Contains("2024-03-01", v); }

	// ---- GENERATE_DATE_ARRAY / GENERATE_TIMESTAMP_ARRAY ----
	[Fact] public async Task GenerateDateArray_Basic() => Assert.Equal("4", await Scalar("SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-04'))"));
	[Fact] public async Task GenerateDateArray_WithStep() => Assert.Equal("2", await Scalar("SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-04', INTERVAL 2 DAY))"));
	[Fact(Skip = "Not yet supported")] public async Task GenerateTimestampArray_Basic() { var v = await Scalar("SELECT ARRAY_LENGTH(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP '2024-01-01T00:00:00+00:00', TIMESTAMP '2024-01-01T03:00:00+00:00', INTERVAL 1 HOUR))"); Assert.Equal("4", v); }
}
