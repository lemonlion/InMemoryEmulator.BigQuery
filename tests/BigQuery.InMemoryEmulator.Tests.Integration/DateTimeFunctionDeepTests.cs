using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for DATE functions and TIMESTAMP functions.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DateTimeFunctionDeepTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public DateTimeFunctionDeepTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
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

	// ---- DATE construction ----
	[Fact] public async Task Date_Literal() => Assert.Equal("2024-01-15", await Scalar("SELECT DATE '2024-01-15'"));
	[Fact] public async Task Date_Function() => Assert.Equal("2024-01-15", await Scalar("SELECT DATE(2024, 1, 15)"));
	[Fact] public async Task Date_Jan1() => Assert.Equal("2024-01-01", await Scalar("SELECT DATE(2024, 1, 1)"));
	[Fact] public async Task Date_Dec31() => Assert.Equal("2024-12-31", await Scalar("SELECT DATE(2024, 12, 31)"));
	[Fact] public async Task Date_LeapYear() => Assert.Equal("2024-02-29", await Scalar("SELECT DATE(2024, 2, 29)"));

	// ---- DATE_ADD ----
	[Fact] public async Task DateAdd_Day() => Assert.Equal("2024-01-16", await Scalar("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 1 DAY)"));
	[Fact] public async Task DateAdd_7Days() => Assert.Equal("2024-01-22", await Scalar("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 7 DAY)"));
	[Fact] public async Task DateAdd_Month() => Assert.Equal("2024-02-15", await Scalar("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 1 MONTH)"));
	[Fact] public async Task DateAdd_Year() => Assert.Equal("2025-01-15", await Scalar("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 1 YEAR)"));
	[Fact(Skip = "Emulator limitation")] public async Task DateAdd_NegDay() => Assert.Equal("2024-01-14", await Scalar("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL -1 DAY)"));
	[Fact] public async Task DateAdd_MonthEnd() => Assert.Equal("2024-02-29", await Scalar("SELECT DATE_ADD(DATE '2024-01-31', INTERVAL 1 MONTH)"));
	[Fact] public async Task DateAdd_CrossYear() => Assert.Equal("2025-01-01", await Scalar("SELECT DATE_ADD(DATE '2024-12-31', INTERVAL 1 DAY)"));

	// ---- DATE_SUB ----
	[Fact] public async Task DateSub_Day() => Assert.Equal("2024-01-14", await Scalar("SELECT DATE_SUB(DATE '2024-01-15', INTERVAL 1 DAY)"));
	[Fact] public async Task DateSub_7Days() => Assert.Equal("2024-01-08", await Scalar("SELECT DATE_SUB(DATE '2024-01-15', INTERVAL 7 DAY)"));
	[Fact] public async Task DateSub_Month() => Assert.Equal("2023-12-15", await Scalar("SELECT DATE_SUB(DATE '2024-01-15', INTERVAL 1 MONTH)"));
	[Fact] public async Task DateSub_Year() => Assert.Equal("2023-01-15", await Scalar("SELECT DATE_SUB(DATE '2024-01-15', INTERVAL 1 YEAR)"));
	[Fact] public async Task DateSub_CrossYear() => Assert.Equal("2023-12-31", await Scalar("SELECT DATE_SUB(DATE '2024-01-01', INTERVAL 1 DAY)"));

	// ---- DATE_DIFF ----
	[Fact] public async Task DateDiff_SameDay() => Assert.Equal("0", await Scalar("SELECT DATE_DIFF(DATE '2024-01-15', DATE '2024-01-15', DAY)"));
	[Fact] public async Task DateDiff_OneDay() => Assert.Equal("1", await Scalar("SELECT DATE_DIFF(DATE '2024-01-16', DATE '2024-01-15', DAY)"));
	[Fact] public async Task DateDiff_Week() => Assert.Equal("7", await Scalar("SELECT DATE_DIFF(DATE '2024-01-22', DATE '2024-01-15', DAY)"));
	[Fact] public async Task DateDiff_NegDay() => Assert.Equal("-1", await Scalar("SELECT DATE_DIFF(DATE '2024-01-15', DATE '2024-01-16', DAY)"));
	[Fact] public async Task DateDiff_Months() => Assert.Equal("1", await Scalar("SELECT DATE_DIFF(DATE '2024-02-15', DATE '2024-01-15', MONTH)"));
	[Fact] public async Task DateDiff_Years() => Assert.Equal("1", await Scalar("SELECT DATE_DIFF(DATE '2025-01-15', DATE '2024-01-15', YEAR)"));
	[Fact] public async Task DateDiff_30Days() => Assert.Equal("30", await Scalar("SELECT DATE_DIFF(DATE '2024-02-14', DATE '2024-01-15', DAY)"));

	// ---- DATE_TRUNC ----
	[Fact] public async Task DateTrunc_Month() => Assert.Equal("2024-01-01", await Scalar("SELECT DATE_TRUNC(DATE '2024-01-15', MONTH)"));
	[Fact] public async Task DateTrunc_Year() => Assert.Equal("2024-01-01", await Scalar("SELECT DATE_TRUNC(DATE '2024-06-15', YEAR)"));
	[Fact] public async Task DateTrunc_Day() => Assert.Equal("2024-01-15", await Scalar("SELECT DATE_TRUNC(DATE '2024-01-15', DAY)"));

	// ---- EXTRACT ----
	[Fact] public async Task Extract_Year() => Assert.Equal("2024", await Scalar("SELECT EXTRACT(YEAR FROM DATE '2024-01-15')"));
	[Fact] public async Task Extract_Month() => Assert.Equal("1", await Scalar("SELECT EXTRACT(MONTH FROM DATE '2024-01-15')"));
	[Fact] public async Task Extract_Day() => Assert.Equal("15", await Scalar("SELECT EXTRACT(DAY FROM DATE '2024-01-15')"));
	[Fact] public async Task Extract_DayOfWeek() { var v = await Scalar("SELECT EXTRACT(DAYOFWEEK FROM DATE '2024-01-15')"); Assert.NotNull(v); int.Parse(v!); }
	[Fact] public async Task Extract_DayOfYear() => Assert.Equal("15", await Scalar("SELECT EXTRACT(DAYOFYEAR FROM DATE '2024-01-15')"));
	[Fact] public async Task Extract_Quarter() => Assert.Equal("1", await Scalar("SELECT EXTRACT(QUARTER FROM DATE '2024-01-15')"));
	[Fact] public async Task Extract_Quarter2() => Assert.Equal("2", await Scalar("SELECT EXTRACT(QUARTER FROM DATE '2024-04-15')"));
	[Fact] public async Task Extract_Quarter3() => Assert.Equal("3", await Scalar("SELECT EXTRACT(QUARTER FROM DATE '2024-07-15')"));
	[Fact] public async Task Extract_Quarter4() => Assert.Equal("4", await Scalar("SELECT EXTRACT(QUARTER FROM DATE '2024-10-15')"));
	[Fact] public async Task Extract_Week() { var v = await Scalar("SELECT EXTRACT(WEEK FROM DATE '2024-01-15')"); Assert.NotNull(v); }
	[Fact] public async Task Extract_Month_June() => Assert.Equal("6", await Scalar("SELECT EXTRACT(MONTH FROM DATE '2024-06-15')"));
	[Fact] public async Task Extract_Month_Dec() => Assert.Equal("12", await Scalar("SELECT EXTRACT(MONTH FROM DATE '2024-12-25')"));
	[Fact] public async Task Extract_YearFromTs() => Assert.Equal("2024", await Scalar("SELECT EXTRACT(YEAR FROM TIMESTAMP '2024-01-15T10:30:00+00:00')"));
	[Fact] public async Task Extract_MonthFromTs() => Assert.Equal("1", await Scalar("SELECT EXTRACT(MONTH FROM TIMESTAMP '2024-01-15T10:30:00+00:00')"));

	// ---- TIMESTAMP functions ----
	[Fact] public async Task Timestamp_Add_Second()
	{
		var v = await Scalar("SELECT CAST(TIMESTAMP_ADD(TIMESTAMP '2024-01-15T10:00:00+00:00', INTERVAL 30 SECOND) AS STRING)");
		Assert.NotNull(v);
		Assert.Contains("10:00:30", v!);
	}

	[Fact] public async Task Timestamp_Add_Minute()
	{
		var v = await Scalar("SELECT CAST(TIMESTAMP_ADD(TIMESTAMP '2024-01-15T10:00:00+00:00', INTERVAL 5 MINUTE) AS STRING)");
		Assert.NotNull(v);
		Assert.Contains("10:05:00", v!);
	}

	[Fact] public async Task Timestamp_Add_Hour()
	{
		var v = await Scalar("SELECT CAST(TIMESTAMP_ADD(TIMESTAMP '2024-01-15T10:00:00+00:00', INTERVAL 3 HOUR) AS STRING)");
		Assert.NotNull(v);
		Assert.Contains("13:00:00", v!);
	}

	[Fact] public async Task Timestamp_Diff_Seconds() => Assert.Equal("60", await Scalar("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-15T10:01:00+00:00', TIMESTAMP '2024-01-15T10:00:00+00:00', SECOND)"));
	[Fact] public async Task Timestamp_Diff_Minutes() => Assert.Equal("5", await Scalar("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-15T10:05:00+00:00', TIMESTAMP '2024-01-15T10:00:00+00:00', MINUTE)"));
	[Fact] public async Task Timestamp_Diff_Hours() => Assert.Equal("3", await Scalar("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-15T13:00:00+00:00', TIMESTAMP '2024-01-15T10:00:00+00:00', HOUR)"));
	[Fact] public async Task Timestamp_Diff_Days() => Assert.Equal("1", await Scalar("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-16T10:00:00+00:00', TIMESTAMP '2024-01-15T10:00:00+00:00', DAY)"));

	// ---- CURRENT_DATE / CURRENT_TIMESTAMP ----
	[Fact] public async Task CurrentDate_NotNull() { var v = await Scalar("SELECT CURRENT_DATE()"); Assert.NotNull(v); }
	[Fact] public async Task CurrentTimestamp_NotNull() { var v = await Scalar("SELECT CAST(CURRENT_TIMESTAMP() AS STRING)"); Assert.NotNull(v); }

	// ---- FORMAT_DATE / FORMAT_TIMESTAMP ----
	[Fact] public async Task FormatDate_Basic() => Assert.Equal("2024-01-15", await Scalar("SELECT FORMAT_DATE('%Y-%m-%d', DATE '2024-01-15')"));
	[Fact] public async Task FormatDate_Slash() => Assert.Equal("01/15/2024", await Scalar("SELECT FORMAT_DATE('%m/%d/%Y', DATE '2024-01-15')"));
	[Fact] public async Task FormatDate_YearMonth() => Assert.Equal("2024-01", await Scalar("SELECT FORMAT_DATE('%Y-%m', DATE '2024-01-15')"));
	[Fact] public async Task FormatDate_YearOnly() => Assert.Equal("2024", await Scalar("SELECT FORMAT_DATE('%Y', DATE '2024-01-15')"));

	// ---- PARSE_DATE ----
	[Fact] public async Task ParseDate_Basic() => Assert.Equal("2024-01-15", await Scalar("SELECT PARSE_DATE('%Y-%m-%d', '2024-01-15')"));
	[Fact] public async Task ParseDate_Slash() => Assert.Equal("2024-01-15", await Scalar("SELECT PARSE_DATE('%m/%d/%Y', '01/15/2024')"));

	// ---- DATE comparisons ----
	[Fact] public async Task Date_Equal() => Assert.Equal("True", await Scalar("SELECT DATE '2024-01-15' = DATE '2024-01-15'"));
	[Fact] public async Task Date_NotEqual() => Assert.Equal("True", await Scalar("SELECT DATE '2024-01-15' != DATE '2024-01-16'"));
	[Fact] public async Task Date_Less() => Assert.Equal("True", await Scalar("SELECT DATE '2024-01-15' < DATE '2024-01-16'"));
	[Fact] public async Task Date_Greater() => Assert.Equal("True", await Scalar("SELECT DATE '2024-01-16' > DATE '2024-01-15'"));
	[Fact] public async Task Date_LessEqual() => Assert.Equal("True", await Scalar("SELECT DATE '2024-01-15' <= DATE '2024-01-15'"));
	[Fact] public async Task Date_GreaterEqual() => Assert.Equal("True", await Scalar("SELECT DATE '2024-01-15' >= DATE '2024-01-15'"));
	[Fact] public async Task Date_Between() => Assert.Equal("True", await Scalar("SELECT DATE '2024-01-15' BETWEEN DATE '2024-01-01' AND DATE '2024-01-31'"));

	// ---- LAST_DAY ----
	[Fact] public async Task LastDay_Jan() => Assert.Equal("2024-01-31", await Scalar("SELECT LAST_DAY(DATE '2024-01-15')"));
	[Fact] public async Task LastDay_Feb() => Assert.Equal("2024-02-29", await Scalar("SELECT LAST_DAY(DATE '2024-02-15')"));
	[Fact] public async Task LastDay_FebNonLeap() => Assert.Equal("2023-02-28", await Scalar("SELECT LAST_DAY(DATE '2023-02-15')"));
	[Fact] public async Task LastDay_Apr() => Assert.Equal("2024-04-30", await Scalar("SELECT LAST_DAY(DATE '2024-04-10')"));
	[Fact] public async Task LastDay_Dec() => Assert.Equal("2024-12-31", await Scalar("SELECT LAST_DAY(DATE '2024-12-01')"));

	// ---- TIMESTAMP_TRUNC ----
	[Fact] public async Task TimestampTrunc_Day()
	{
		var v = await Scalar("SELECT CAST(TIMESTAMP_TRUNC(TIMESTAMP '2024-01-15T10:30:45+00:00', DAY) AS STRING)");
		Assert.NotNull(v);
		Assert.Contains("2024-01-15", v!);
	}

	[Fact] public async Task TimestampTrunc_Hour()
	{
		var v = await Scalar("SELECT CAST(TIMESTAMP_TRUNC(TIMESTAMP '2024-01-15T10:30:45+00:00', HOUR) AS STRING)");
		Assert.NotNull(v);
		Assert.Contains("10:00:00", v!);
	}

	// ---- UNIX_SECONDS / TIMESTAMP_SECONDS ----
	[Fact] public async Task UnixSeconds_Epoch() => Assert.Equal("0", await Scalar("SELECT UNIX_SECONDS(TIMESTAMP '1970-01-01T00:00:00+00:00')"));
	[Fact] public async Task UnixSeconds_Positive() { var v = await Scalar("SELECT UNIX_SECONDS(TIMESTAMP '2024-01-15T00:00:00+00:00')"); Assert.NotNull(v); Assert.True(long.Parse(v!) > 0); }

	// ---- DATE arithmetic with expressions ----
	[Fact]
	public async Task DateArith_ComputedInterval() => Assert.Equal("2024-01-20", await Scalar("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 5 DAY)"));

	[Fact]
	public async Task DateArith_SubtractMonths() => Assert.Equal("2023-10-15", await Scalar("SELECT DATE_SUB(DATE '2024-01-15', INTERVAL 3 MONTH)"));
}
