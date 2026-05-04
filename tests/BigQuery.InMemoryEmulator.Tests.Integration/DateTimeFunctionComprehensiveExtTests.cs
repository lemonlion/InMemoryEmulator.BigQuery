using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for date/time functions: CURRENT_DATE, DATE_ADD, DATE_SUB, DATE_DIFF, DATE_TRUNC, EXTRACT, FORMAT_DATE, PARSE_DATE.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DateTimeFunctionComprehensiveExtTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public DateTimeFunctionComprehensiveExtTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> S(string sql)
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync(sql, parameters: null);
		var rows = r.ToList();
		if (rows.Count == 0) return null;
		var val = rows[0][0];
		if (val is DateTime dt) return dt.TimeOfDay == TimeSpan.Zero ? dt.ToString("yyyy-MM-dd") : dt.ToString("yyyy-MM-dd HH:mm:ss");
		if (val is DateTimeOffset dto) return dto.TimeOfDay == TimeSpan.Zero ? dto.ToString("yyyy-MM-dd") : dto.ToString("yyyy-MM-dd HH:mm:ss");
		return val?.ToString();
	}

	// ---- DATE literals ----
	[Fact] public async Task DateLiteral() => Assert.Equal("2024-01-15", await S("SELECT DATE '2024-01-15'"));
	[Fact] public async Task DateLiteral_LeapYear() => Assert.Equal("2024-02-29", await S("SELECT DATE '2024-02-29'"));

	// ---- CURRENT_DATE ----
	[Fact] public async Task CurrentDate_NotNull() => Assert.NotNull(await S("SELECT CURRENT_DATE()"));
	[Fact] public async Task CurrentDate_Format()
	{
		var v = await S("SELECT CURRENT_DATE()");
		Assert.Matches(@"^\d{4}-\d{2}-\d{2}$", v!);
	}

	// ---- DATE_ADD ----
	[Fact] public async Task DateAdd_Days() => Assert.Equal("2024-01-20", await S("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 5 DAY)"));
	[Fact] public async Task DateAdd_Months() => Assert.Equal("2024-04-15", await S("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 3 MONTH)"));
	[Fact] public async Task DateAdd_Years() => Assert.Equal("2025-01-15", await S("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 1 YEAR)"));
	[Fact] public async Task DateAdd_Weeks() => Assert.Equal("2024-01-29", await S("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 2 WEEK)"));
	[Fact] public async Task DateAdd_NegativeDays() => Assert.Equal("2024-01-10", await S("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL -5 DAY)"));
	[Fact] public async Task DateAdd_CrossMonth() => Assert.Equal("2024-02-04", await S("SELECT DATE_ADD(DATE '2024-01-30', INTERVAL 5 DAY)"));

	// ---- DATE_SUB ----
	[Fact] public async Task DateSub_Days() => Assert.Equal("2024-01-10", await S("SELECT DATE_SUB(DATE '2024-01-15', INTERVAL 5 DAY)"));
	[Fact] public async Task DateSub_Months() => Assert.Equal("2023-10-15", await S("SELECT DATE_SUB(DATE '2024-01-15', INTERVAL 3 MONTH)"));
	[Fact] public async Task DateSub_Years() => Assert.Equal("2023-01-15", await S("SELECT DATE_SUB(DATE '2024-01-15', INTERVAL 1 YEAR)"));

	// ---- DATE_DIFF ----
	[Fact] public async Task DateDiff_Days() => Assert.Equal("10", await S("SELECT DATE_DIFF(DATE '2024-01-25', DATE '2024-01-15', DAY)"));
	[Fact] public async Task DateDiff_Months() => Assert.Equal("3", await S("SELECT DATE_DIFF(DATE '2024-04-15', DATE '2024-01-15', MONTH)"));
	[Fact] public async Task DateDiff_Years() => Assert.Equal("1", await S("SELECT DATE_DIFF(DATE '2025-01-15', DATE '2024-01-15', YEAR)"));
	[Fact] public async Task DateDiff_Negative() => Assert.Equal("-10", await S("SELECT DATE_DIFF(DATE '2024-01-15', DATE '2024-01-25', DAY)"));

	// ---- DATE_TRUNC ----
	[Fact] public async Task DateTrunc_Month() => Assert.Equal("2024-01-01", await S("SELECT DATE_TRUNC(DATE '2024-01-15', MONTH)"));
	[Fact] public async Task DateTrunc_Year() => Assert.Equal("2024-01-01", await S("SELECT DATE_TRUNC(DATE '2024-06-15', YEAR)"));
	[Fact] public async Task DateTrunc_Week() => Assert.NotNull(await S("SELECT DATE_TRUNC(DATE '2024-01-15', WEEK)"));

	// ---- EXTRACT ----
	[Fact] public async Task Extract_Year() => Assert.Equal("2024", await S("SELECT EXTRACT(YEAR FROM DATE '2024-01-15')"));
	[Fact] public async Task Extract_Month() => Assert.Equal("1", await S("SELECT EXTRACT(MONTH FROM DATE '2024-01-15')"));
	[Fact] public async Task Extract_Day() => Assert.Equal("15", await S("SELECT EXTRACT(DAY FROM DATE '2024-01-15')"));
	[Fact] public async Task Extract_DayOfWeek() => Assert.NotNull(await S("SELECT EXTRACT(DAYOFWEEK FROM DATE '2024-01-15')"));
	[Fact] public async Task Extract_DayOfYear() => Assert.Equal("15", await S("SELECT EXTRACT(DAYOFYEAR FROM DATE '2024-01-15')"));
	[Fact] public async Task Extract_Quarter() => Assert.Equal("1", await S("SELECT EXTRACT(QUARTER FROM DATE '2024-03-15')"));
	[Fact] public async Task Extract_Week() => Assert.NotNull(await S("SELECT EXTRACT(WEEK FROM DATE '2024-01-15')"));

	// ---- TIMESTAMP functions ----
	[Fact] public async Task TimestampLiteral() => Assert.NotNull(await S("SELECT TIMESTAMP '2024-01-15 10:30:00 UTC'"));
	[Fact] public async Task CurrentTimestamp() => Assert.NotNull(await S("SELECT CURRENT_TIMESTAMP()"));
	[Fact] public async Task TimestampAdd_Hours()
	{
		var v = await S("SELECT TIMESTAMP_ADD(TIMESTAMP '2024-01-15 10:00:00 UTC', INTERVAL 5 HOUR)");
		Assert.NotNull(v);
		Assert.Contains("15", v);
	}
	[Fact] public async Task TimestampDiff_Hours() => Assert.NotNull(await S("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-15 15:00:00 UTC', TIMESTAMP '2024-01-15 10:00:00 UTC', HOUR)"));
	[Fact] public async Task TimestampTrunc_Day() => Assert.NotNull(await S("SELECT TIMESTAMP_TRUNC(TIMESTAMP '2024-01-15 10:30:00 UTC', DAY)"));

	// ---- EXTRACT from TIMESTAMP ----
	[Fact] public async Task Extract_Hour() => Assert.Equal("10", await S("SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-01-15 10:30:45 UTC')"));
	[Fact] public async Task Extract_Minute() => Assert.Equal("30", await S("SELECT EXTRACT(MINUTE FROM TIMESTAMP '2024-01-15 10:30:45 UTC')"));
	[Fact] public async Task Extract_Second() => Assert.Equal("45", await S("SELECT EXTRACT(SECOND FROM TIMESTAMP '2024-01-15 10:30:45 UTC')"));

	// ---- FORMAT_DATE ----
	[Fact] public async Task FormatDate_Full() => Assert.Equal("2024-01-15", await S("SELECT FORMAT_DATE('%Y-%m-%d', DATE '2024-01-15')"));
	[Fact] public async Task FormatDate_MonthDay() => Assert.Equal("01/15", await S("SELECT FORMAT_DATE('%m/%d', DATE '2024-01-15')"));
	[Fact] public async Task FormatDate_YearOnly() => Assert.Equal("2024", await S("SELECT FORMAT_DATE('%Y', DATE '2024-01-15')"));

	// ---- PARSE_DATE ----
	[Fact] public async Task ParseDate_Basic() => Assert.Equal("2024-01-15", await S("SELECT PARSE_DATE('%Y-%m-%d', '2024-01-15')"));
	[Fact] public async Task ParseDate_SlashFormat() => Assert.Equal("2024-01-15", await S("SELECT PARSE_DATE('%m/%d/%Y', '01/15/2024')"));

	// ---- DATE from parts ----
	[Fact] public async Task Date_FromParts() => Assert.Equal("2024-01-15", await S("SELECT DATE(2024, 1, 15)"));

	// ---- LAST_DAY ----
	[Fact] public async Task LastDay_January() => Assert.Equal("2024-01-31", await S("SELECT LAST_DAY(DATE '2024-01-15')"));
	[Fact] public async Task LastDay_February() => Assert.Equal("2024-02-29", await S("SELECT LAST_DAY(DATE '2024-02-15')"));
	[Fact] public async Task LastDay_FebNonLeap() => Assert.Equal("2023-02-28", await S("SELECT LAST_DAY(DATE '2023-02-15')"));

	// ---- Date arithmetic with columns ----
	[Fact] public async Task DateArith_AddExtract()
	{
		var v = await S("SELECT EXTRACT(MONTH FROM DATE_ADD(DATE '2024-01-15', INTERVAL 2 MONTH))");
		Assert.Equal("3", v);
	}
	[Fact] public async Task DateArith_DiffDaysBetween()
	{
		var v = await S("SELECT DATE_DIFF(DATE '2024-03-01', DATE '2024-02-01', DAY)");
		Assert.Equal("29", v); // 2024 is a leap year
	}

	// ---- NULL handling ----
	[Fact] public async Task NullDate_Add() => Assert.Null(await S("SELECT DATE_ADD(CAST(NULL AS DATE), INTERVAL 1 DAY)"));
	[Fact] public async Task NullDate_Diff() => Assert.Null(await S("SELECT DATE_DIFF(NULL, DATE '2024-01-01', DAY)"));
	[Fact] public async Task NullDate_Extract() => Assert.Null(await S("SELECT EXTRACT(YEAR FROM CAST(NULL AS DATE))"));
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_trunc
	//   "Returns NULL if date_expression is NULL."
	[Fact] public async Task NullDate_Trunc() => Assert.Null(await S("SELECT DATE_TRUNC(CAST(NULL AS DATE), MONTH)"));

	// ---- TIME functions ----
	[Fact] public async Task TimeLiteral() => Assert.NotNull(await S("SELECT TIME '10:30:45'"));
	[Fact] public async Task CurrentTime() => Assert.NotNull(await S("SELECT CURRENT_TIME()"));

	// ---- DATETIME functions ----
	[Fact] public async Task DatetimeLiteral() => Assert.NotNull(await S("SELECT DATETIME '2024-01-15 10:30:45'"));
	[Fact] public async Task CurrentDatetime() => Assert.NotNull(await S("SELECT CURRENT_DATETIME()"));
}
