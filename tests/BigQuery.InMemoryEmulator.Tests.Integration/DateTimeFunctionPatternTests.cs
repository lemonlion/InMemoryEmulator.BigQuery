using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Date/Time function patterns: DATE, TIMESTAMP, DATETIME, EXTRACT, DATE_ADD, DATE_SUB, DATE_DIFF, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DateTimeFunctionPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public DateTimeFunctionPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_dtfp_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql)
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null);
		var rows = r.ToList();
		if (rows.Count == 0) return null;
		var val = rows[0][0];
		if (val is DateTime dt) return dt.TimeOfDay == TimeSpan.Zero ? dt.ToString("yyyy-MM-dd") : dt.ToString("yyyy-MM-dd HH:mm:ss");
		if (val is DateTimeOffset dto) return dto.TimeOfDay == TimeSpan.Zero ? dto.ToString("yyyy-MM-dd") : dto.ToString("yyyy-MM-dd HH:mm:ss");
		return val?.ToString();
	}

	// ---- DATE construction ----
	[Fact] public async Task Date_Literal() { var v = await S("SELECT DATE '2024-06-15'"); Assert.Equal("2024-06-15", v); }
	[Fact] public async Task Date_FromParts() { var v = await S("SELECT DATE(2024, 6, 15)"); Assert.Equal("2024-06-15", v); }

	// ---- EXTRACT ----
	[Fact] public async Task Extract_Year() => Assert.Equal("2024", await S("SELECT EXTRACT(YEAR FROM DATE '2024-06-15')"));
	[Fact] public async Task Extract_Month() => Assert.Equal("6", await S("SELECT EXTRACT(MONTH FROM DATE '2024-06-15')"));
	[Fact] public async Task Extract_Day() => Assert.Equal("15", await S("SELECT EXTRACT(DAY FROM DATE '2024-06-15')"));
	[Fact] public async Task Extract_DayOfWeek() => Assert.NotNull(await S("SELECT EXTRACT(DAYOFWEEK FROM DATE '2024-06-15')"));
	[Fact] public async Task Extract_DayOfYear() => Assert.NotNull(await S("SELECT EXTRACT(DAYOFYEAR FROM DATE '2024-06-15')"));
	[Fact] public async Task Extract_Quarter() => Assert.Equal("2", await S("SELECT EXTRACT(QUARTER FROM DATE '2024-06-15')"));
	[Fact] public async Task Extract_Week() => Assert.NotNull(await S("SELECT EXTRACT(WEEK FROM DATE '2024-06-15')"));

	// ---- EXTRACT from TIMESTAMP ----
	[Fact] public async Task Extract_Hour() => Assert.Equal("10", await S("SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-06-15 10:30:45')"));
	[Fact] public async Task Extract_Minute() => Assert.Equal("30", await S("SELECT EXTRACT(MINUTE FROM TIMESTAMP '2024-06-15 10:30:45')"));
	[Fact] public async Task Extract_Second() => Assert.Equal("45", await S("SELECT EXTRACT(SECOND FROM TIMESTAMP '2024-06-15 10:30:45')"));

	// ---- DATE_ADD ----
	[Fact] public async Task DateAdd_Day() { var v = await S("SELECT DATE_ADD(DATE '2024-06-15', INTERVAL 10 DAY)"); Assert.Equal("2024-06-25", v); }
	[Fact] public async Task DateAdd_Month() { var v = await S("SELECT DATE_ADD(DATE '2024-01-31', INTERVAL 1 MONTH)"); Assert.NotNull(v); Assert.Contains("2024-02", v); }
	[Fact] public async Task DateAdd_Year() { var v = await S("SELECT DATE_ADD(DATE '2024-06-15', INTERVAL 1 YEAR)"); Assert.Equal("2025-06-15", v); }

	// ---- DATE_SUB ----
	[Fact] public async Task DateSub_Day() { var v = await S("SELECT DATE_SUB(DATE '2024-06-15', INTERVAL 5 DAY)"); Assert.Equal("2024-06-10", v); }
	[Fact] public async Task DateSub_Month() { var v = await S("SELECT DATE_SUB(DATE '2024-06-15', INTERVAL 1 MONTH)"); Assert.Equal("2024-05-15", v); }

	// ---- DATE_DIFF ----
	[Fact] public async Task DateDiff_Day() => Assert.Equal("10", await S("SELECT DATE_DIFF(DATE '2024-06-25', DATE '2024-06-15', DAY)"));
	[Fact] public async Task DateDiff_Month() => Assert.Equal("3", await S("SELECT DATE_DIFF(DATE '2024-06-15', DATE '2024-03-15', MONTH)"));
	[Fact] public async Task DateDiff_Year() => Assert.Equal("2", await S("SELECT DATE_DIFF(DATE '2024-06-15', DATE '2022-06-15', YEAR)"));

	// ---- DATE_TRUNC ----
	[Fact] public async Task DateTrunc_Month() { var v = await S("SELECT DATE_TRUNC(DATE '2024-06-15', MONTH)"); Assert.Equal("2024-06-01", v); }
	[Fact] public async Task DateTrunc_Year() { var v = await S("SELECT DATE_TRUNC(DATE '2024-06-15', YEAR)"); Assert.Equal("2024-01-01", v); }
	[Fact] public async Task DateTrunc_Quarter() { var v = await S("SELECT DATE_TRUNC(DATE '2024-06-15', QUARTER)"); Assert.Equal("2024-04-01", v); }

	// ---- TIMESTAMP functions ----
	[Fact] public async Task Timestamp_Add_Hour()
	{
		var v = await S("SELECT TIMESTAMP_ADD(TIMESTAMP '2024-06-15 10:00:00', INTERVAL 2 HOUR)");
		Assert.NotNull(v);
		Assert.Contains("12:00:00", v);
	}
	[Fact] public async Task Timestamp_Sub_Minute()
	{
		var v = await S("SELECT TIMESTAMP_SUB(TIMESTAMP '2024-06-15 10:30:00', INTERVAL 30 MINUTE)");
		Assert.NotNull(v);
		Assert.Contains("10:00:00", v);
	}
	[Fact] public async Task Timestamp_Diff_Second()
	{
		var v = await S("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-06-15 10:00:30', TIMESTAMP '2024-06-15 10:00:00', SECOND)");
		Assert.Equal("30", v);
	}
	[Fact] public async Task Timestamp_Diff_Hour()
	{
		var v = await S("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-06-15 12:00:00', TIMESTAMP '2024-06-15 10:00:00', HOUR)");
		Assert.Equal("2", v);
	}
	[Fact] public async Task Timestamp_Trunc_Hour()
	{
		var v = await S("SELECT TIMESTAMP_TRUNC(TIMESTAMP '2024-06-15 10:35:45', HOUR)");
		Assert.NotNull(v);
		Assert.Contains("10:00:00", v);
	}

	// ---- CURRENT_DATE / CURRENT_TIMESTAMP ----
	[Fact] public async Task CurrentDate()
	{
		var v = await S("SELECT CURRENT_DATE()");
		Assert.NotNull(v);
		Assert.Contains(DateTime.UtcNow.Year.ToString(), v);
	}
	[Fact] public async Task CurrentTimestamp()
	{
		var v = await S("SELECT CURRENT_TIMESTAMP()");
		Assert.NotNull(v);
		Assert.Contains(DateTime.UtcNow.Year.ToString(), v);
	}

	// ---- FORMAT_DATE ----
	[Fact] public async Task FormatDate_YMD()
	{
		var v = await S("SELECT FORMAT_DATE('%Y-%m-%d', DATE '2024-06-15')");
		Assert.Equal("2024-06-15", v);
	}
	[Fact] public async Task FormatDate_MonthName()
	{
		var v = await S("SELECT FORMAT_DATE('%B', DATE '2024-06-15')");
		Assert.NotNull(v);
		Assert.Contains("Jun", v);
	}
	[Fact] public async Task FormatDate_DayName()
	{
		var v = await S("SELECT FORMAT_DATE('%A', DATE '2024-06-15')");
		Assert.NotNull(v);
		// 2024-06-15 is Saturday
	}

	// ---- PARSE_DATE ----
	[Fact] public async Task ParseDate_YMD()
	{
		var v = await S("SELECT PARSE_DATE('%Y-%m-%d', '2024-06-15')");
		Assert.NotNull(v);
		Assert.Contains("2024", v);
	}

	// ---- Date in table context ----
	[Fact] public async Task Date_InQuery()
	{
		await Exec("CREATE TABLE `{ds}.events` (id INT64, event_date DATE)");
		await Exec("INSERT INTO `{ds}.events` VALUES (1, DATE '2024-01-15'),(2, DATE '2024-06-20'),(3, DATE '2024-12-25')");
		Assert.Equal("2", await S("SELECT COUNT(*) FROM `{ds}.events` WHERE event_date > DATE '2024-03-01'"));
	}
	[Fact] public async Task Date_OrderBy()
	{
		await Exec("CREATE TABLE `{ds}.ev2` (id INT64, d DATE)");
		await Exec("INSERT INTO `{ds}.ev2` VALUES (1, DATE '2024-12-25'),(2, DATE '2024-01-15'),(3, DATE '2024-06-20')");
		var rows = await Q("SELECT id FROM `{ds}.ev2` ORDER BY d");
		Assert.Equal("2", rows[0]["id"]?.ToString());
		Assert.Equal("3", rows[1]["id"]?.ToString());
		Assert.Equal("1", rows[2]["id"]?.ToString());
	}
	[Fact] public async Task DateDiff_InQuery()
	{
		await Exec("CREATE TABLE `{ds}.ev3` (id INT64, start_date DATE, end_date DATE)");
		await Exec("INSERT INTO `{ds}.ev3` VALUES (1, DATE '2024-01-01', DATE '2024-01-31'),(2, DATE '2024-06-01', DATE '2024-06-15')");
		var v = await S("SELECT SUM(DATE_DIFF(end_date, start_date, DAY)) FROM `{ds}.ev3`");
		Assert.Equal("44", v); // 30 + 14
	}

	private async Task Exec(string sql) { var c = await _fixture.GetClientAsync(); await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }
}
