using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for date arithmetic, date extraction, and date formatting patterns.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DateArithmeticTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public DateArithmeticTests(BigQuerySession session) => _session = session;
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

	// ---- DATE_ADD with various intervals ----
	[Fact] public async Task DateAdd_2Days() => Assert.Equal("2024-01-17", await Scalar("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 2 DAY)"));
	[Fact] public async Task DateAdd_10Days() => Assert.Equal("2024-01-25", await Scalar("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 10 DAY)"));
	[Fact] public async Task DateAdd_30Days() => Assert.Equal("2024-02-14", await Scalar("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 30 DAY)"));
	[Fact] public async Task DateAdd_60Days() => Assert.Equal("2024-03-15", await Scalar("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 60 DAY)"));
	[Fact] public async Task DateAdd_365Days() => Assert.Equal("2025-01-14", await Scalar("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 365 DAY)"));
	[Fact] public async Task DateAdd_2Months() => Assert.Equal("2024-03-15", await Scalar("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 2 MONTH)"));
	[Fact] public async Task DateAdd_6Months() => Assert.Equal("2024-07-15", await Scalar("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 6 MONTH)"));
	[Fact] public async Task DateAdd_12Months() => Assert.Equal("2025-01-15", await Scalar("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 12 MONTH)"));
	[Fact] public async Task DateAdd_2Years() => Assert.Equal("2026-01-15", await Scalar("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 2 YEAR)"));
	[Fact] public async Task DateAdd_10Years() => Assert.Equal("2034-01-15", await Scalar("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 10 YEAR)"));

	// ---- DATE_SUB with various intervals ----
	[Fact] public async Task DateSub_2Days() => Assert.Equal("2024-01-13", await Scalar("SELECT DATE_SUB(DATE '2024-01-15', INTERVAL 2 DAY)"));
	[Fact] public async Task DateSub_10Days() => Assert.Equal("2024-01-05", await Scalar("SELECT DATE_SUB(DATE '2024-01-15', INTERVAL 10 DAY)"));
	[Fact] public async Task DateSub_30Days() => Assert.Equal("2023-12-16", await Scalar("SELECT DATE_SUB(DATE '2024-01-15', INTERVAL 30 DAY)"));
	[Fact] public async Task DateSub_2Months() => Assert.Equal("2023-11-15", await Scalar("SELECT DATE_SUB(DATE '2024-01-15', INTERVAL 2 MONTH)"));
	[Fact] public async Task DateSub_6Months() => Assert.Equal("2023-07-15", await Scalar("SELECT DATE_SUB(DATE '2024-01-15', INTERVAL 6 MONTH)"));
	[Fact] public async Task DateSub_2Years() => Assert.Equal("2022-01-15", await Scalar("SELECT DATE_SUB(DATE '2024-01-15', INTERVAL 2 YEAR)"));

	// ---- DATE_DIFF combinations ----
	[Fact] public async Task DateDiff_90Days() => Assert.Equal("90", await Scalar("SELECT DATE_DIFF(DATE '2024-04-14', DATE '2024-01-15', DAY)"));
	[Fact] public async Task DateDiff_WeekUnit() => Assert.Equal("4", await Scalar("SELECT DATE_DIFF(DATE '2024-02-15', DATE '2024-01-15', WEEK)"));
	[Fact] public async Task DateDiff_3Months() => Assert.Equal("3", await Scalar("SELECT DATE_DIFF(DATE '2024-04-15', DATE '2024-01-15', MONTH)"));
	[Fact] public async Task DateDiff_12Months() => Assert.Equal("12", await Scalar("SELECT DATE_DIFF(DATE '2025-01-15', DATE '2024-01-15', MONTH)"));
	[Fact] public async Task DateDiff_2Years() => Assert.Equal("2", await Scalar("SELECT DATE_DIFF(DATE '2026-01-15', DATE '2024-01-15', YEAR)"));
	[Fact] public async Task DateDiff_SameMonth() => Assert.Equal("0", await Scalar("SELECT DATE_DIFF(DATE '2024-01-20', DATE '2024-01-01', MONTH)"));

	// ---- EXTRACT patterns ----
	[Fact] public async Task Extract_Jan() => Assert.Equal("1", await Scalar("SELECT EXTRACT(MONTH FROM DATE '2024-01-15')"));
	[Fact] public async Task Extract_Feb() => Assert.Equal("2", await Scalar("SELECT EXTRACT(MONTH FROM DATE '2024-02-15')"));
	[Fact] public async Task Extract_Mar() => Assert.Equal("3", await Scalar("SELECT EXTRACT(MONTH FROM DATE '2024-03-15')"));
	[Fact] public async Task Extract_Apr() => Assert.Equal("4", await Scalar("SELECT EXTRACT(MONTH FROM DATE '2024-04-15')"));
	[Fact] public async Task Extract_May() => Assert.Equal("5", await Scalar("SELECT EXTRACT(MONTH FROM DATE '2024-05-15')"));
	[Fact] public async Task Extract_Jun() => Assert.Equal("6", await Scalar("SELECT EXTRACT(MONTH FROM DATE '2024-06-15')"));
	[Fact] public async Task Extract_Jul() => Assert.Equal("7", await Scalar("SELECT EXTRACT(MONTH FROM DATE '2024-07-15')"));
	[Fact] public async Task Extract_Aug() => Assert.Equal("8", await Scalar("SELECT EXTRACT(MONTH FROM DATE '2024-08-15')"));
	[Fact] public async Task Extract_Sep() => Assert.Equal("9", await Scalar("SELECT EXTRACT(MONTH FROM DATE '2024-09-15')"));
	[Fact] public async Task Extract_Oct() => Assert.Equal("10", await Scalar("SELECT EXTRACT(MONTH FROM DATE '2024-10-15')"));
	[Fact] public async Task Extract_Nov() => Assert.Equal("11", await Scalar("SELECT EXTRACT(MONTH FROM DATE '2024-11-15')"));
	[Fact] public async Task Extract_Dec() => Assert.Equal("12", await Scalar("SELECT EXTRACT(MONTH FROM DATE '2024-12-15')"));
	[Fact] public async Task Extract_Year2020() => Assert.Equal("2020", await Scalar("SELECT EXTRACT(YEAR FROM DATE '2020-06-15')"));
	[Fact] public async Task Extract_Year2000() => Assert.Equal("2000", await Scalar("SELECT EXTRACT(YEAR FROM DATE '2000-01-01')"));
	[Fact] public async Task Extract_Day1() => Assert.Equal("1", await Scalar("SELECT EXTRACT(DAY FROM DATE '2024-01-01')"));
	[Fact] public async Task Extract_Day28() => Assert.Equal("28", await Scalar("SELECT EXTRACT(DAY FROM DATE '2024-02-28')"));
	[Fact] public async Task Extract_Day29() => Assert.Equal("29", await Scalar("SELECT EXTRACT(DAY FROM DATE '2024-02-29')"));
	[Fact] public async Task Extract_Day30() => Assert.Equal("30", await Scalar("SELECT EXTRACT(DAY FROM DATE '2024-04-30')"));
	[Fact] public async Task Extract_Day31() => Assert.Equal("31", await Scalar("SELECT EXTRACT(DAY FROM DATE '2024-01-31')"));

	// ---- DATE_TRUNC patterns ----
	[Fact] public async Task DateTrunc_Jan() => Assert.Equal("2024-01-01", await Scalar("SELECT DATE_TRUNC(DATE '2024-01-25', MONTH)"));
	[Fact] public async Task DateTrunc_Feb() => Assert.Equal("2024-02-01", await Scalar("SELECT DATE_TRUNC(DATE '2024-02-15', MONTH)"));
	[Fact] public async Task DateTrunc_Dec() => Assert.Equal("2024-12-01", await Scalar("SELECT DATE_TRUNC(DATE '2024-12-25', MONTH)"));
	[Fact] public async Task DateTrunc_Q1() => Assert.Equal("2024-01-01", await Scalar("SELECT DATE_TRUNC(DATE '2024-03-15', QUARTER)"));
	[Fact] public async Task DateTrunc_Q2() => Assert.Equal("2024-04-01", await Scalar("SELECT DATE_TRUNC(DATE '2024-05-15', QUARTER)"));
	[Fact] public async Task DateTrunc_Q3() => Assert.Equal("2024-07-01", await Scalar("SELECT DATE_TRUNC(DATE '2024-08-15', QUARTER)"));
	[Fact] public async Task DateTrunc_Q4() => Assert.Equal("2024-10-01", await Scalar("SELECT DATE_TRUNC(DATE '2024-11-15', QUARTER)"));
	[Fact] public async Task DateTrunc_Year() => Assert.Equal("2024-01-01", await Scalar("SELECT DATE_TRUNC(DATE '2024-06-15', YEAR)"));

	// ---- LAST_DAY patterns ----
	[Fact] public async Task LastDay_March() => Assert.Equal("2024-03-31", await Scalar("SELECT LAST_DAY(DATE '2024-03-15')"));
	[Fact] public async Task LastDay_June() => Assert.Equal("2024-06-30", await Scalar("SELECT LAST_DAY(DATE '2024-06-15')"));
	[Fact] public async Task LastDay_Sept() => Assert.Equal("2024-09-30", await Scalar("SELECT LAST_DAY(DATE '2024-09-01')"));
	[Fact] public async Task LastDay_Nov() => Assert.Equal("2024-11-30", await Scalar("SELECT LAST_DAY(DATE '2024-11-15')"));
	[Fact] public async Task LastDay_Feb_Leap() => Assert.Equal("2024-02-29", await Scalar("SELECT LAST_DAY(DATE '2024-02-01')"));
	[Fact] public async Task LastDay_Feb_NoLeap() => Assert.Equal("2023-02-28", await Scalar("SELECT LAST_DAY(DATE '2023-02-01')"));

	// ---- FORMAT_DATE patterns ----
	[Fact] public async Task FormatDate_ISO() => Assert.Equal("2024-01-15", await Scalar("SELECT FORMAT_DATE('%Y-%m-%d', DATE '2024-01-15')"));
	[Fact] public async Task FormatDate_US() => Assert.Equal("01/15/2024", await Scalar("SELECT FORMAT_DATE('%m/%d/%Y', DATE '2024-01-15')"));
	[Fact] public async Task FormatDate_Short() => Assert.Equal("24-01-15", await Scalar("SELECT FORMAT_DATE('%y-%m-%d', DATE '2024-01-15')"));
	[Fact] public async Task FormatDate_YM() => Assert.Equal("2024-01", await Scalar("SELECT FORMAT_DATE('%Y-%m', DATE '2024-01-15')"));
	[Fact] public async Task FormatDate_Y() => Assert.Equal("2024", await Scalar("SELECT FORMAT_DATE('%Y', DATE '2024-01-15')"));
	[Fact] public async Task FormatDate_m() => Assert.Equal("01", await Scalar("SELECT FORMAT_DATE('%m', DATE '2024-01-15')"));
	[Fact] public async Task FormatDate_d() => Assert.Equal("15", await Scalar("SELECT FORMAT_DATE('%d', DATE '2024-01-15')"));

	// ---- PARSE_DATE patterns ----
	[Fact] public async Task ParseDate_ISO() => Assert.Equal("2024-03-20", await Scalar("SELECT PARSE_DATE('%Y-%m-%d', '2024-03-20')"));
	[Fact] public async Task ParseDate_US() => Assert.Equal("2024-03-20", await Scalar("SELECT PARSE_DATE('%m/%d/%Y', '03/20/2024')"));
	[Fact] public async Task ParseDate_YM() => Assert.Equal("2024-06-01", await Scalar("SELECT PARSE_DATE('%Y-%m-%d', '2024-06-01')"));

	// ---- Date comparisons in WHERE ----
	[Fact]
	public async Task DateCompare_InWhere()
	{
		var v = await Scalar(@"
SELECT COUNT(*) FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-31')) AS d
WHERE d > DATE '2024-01-15'");
		Assert.Equal("16", v);
	}

	[Fact]
	public async Task DateCompare_Between()
	{
		var v = await Scalar(@"
SELECT COUNT(*) FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-31')) AS d
WHERE d BETWEEN DATE '2024-01-10' AND DATE '2024-01-20'");
		Assert.Equal("11", v);
	}

	// ---- TIMESTAMP_DIFF patterns ----
	[Fact] public async Task TsDiff_120Seconds() => Assert.Equal("120", await Scalar("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-15T10:02:00+00:00', TIMESTAMP '2024-01-15T10:00:00+00:00', SECOND)"));
	[Fact] public async Task TsDiff_24Hours() => Assert.Equal("24", await Scalar("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-16T10:00:00+00:00', TIMESTAMP '2024-01-15T10:00:00+00:00', HOUR)"));
	[Fact] public async Task TsDiff_7Days() => Assert.Equal("7", await Scalar("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-22T10:00:00+00:00', TIMESTAMP '2024-01-15T10:00:00+00:00', DAY)"));

	// ---- UNIX_SECONDS ----
	[Fact] public async Task UnixSeconds_KnownValue() { var v = await Scalar("SELECT UNIX_SECONDS(TIMESTAMP '2024-01-01T00:00:00+00:00')"); Assert.True(long.Parse(v!) > 0); }
	[Fact] public async Task UnixSeconds_Epoch() => Assert.Equal("0", await Scalar("SELECT UNIX_SECONDS(TIMESTAMP '1970-01-01T00:00:00+00:00')"));
	[Fact] public async Task UnixSeconds_Positive()  { var v = long.Parse((await Scalar("SELECT UNIX_SECONDS(TIMESTAMP '2000-01-01T00:00:00+00:00')"))!); Assert.True(v > 946684800 - 1 && v < 946684800 + 1); }
}
