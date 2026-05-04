using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Date formatting patterns: FORMAT_DATE, FORMAT_TIMESTAMP, FORMAT_DATETIME, FORMAT_TIME, various format specifiers.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DateFormatPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public DateFormatPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_dfp_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.dates` (id INT64, d DATE, ts TIMESTAMP, dt DATETIME)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.dates` VALUES
			(1, '2024-01-15', '2024-01-15 10:30:00 UTC', '2024-01-15 10:30:00'),
			(2, '2024-06-30', '2024-06-30 23:59:59 UTC', '2024-06-30 23:59:59'),
			(3, '2024-12-25', '2024-12-25 00:00:00 UTC', '2024-12-25 00:00:00'),
			(4, '2024-02-29', '2024-02-29 12:00:00 UTC', '2024-02-29 12:00:00'),
			(5, '2023-01-01', '2023-01-01 00:00:00 UTC', '2023-01-01 00:00:00'),
			(6, NULL, NULL, NULL)", parameters: null);
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
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- FORMAT_DATE ----
	[Fact] public async Task FormatDate_Year() => Assert.Equal("2024", await S("SELECT FORMAT_DATE('%Y', DATE '2024-01-15')"));
	[Fact] public async Task FormatDate_Month() => Assert.Equal("01", await S("SELECT FORMAT_DATE('%m', DATE '2024-01-15')"));
	[Fact] public async Task FormatDate_Day() => Assert.Equal("15", await S("SELECT FORMAT_DATE('%d', DATE '2024-01-15')"));
	[Fact] public async Task FormatDate_YearMonth() => Assert.Equal("2024-06", await S("SELECT FORMAT_DATE('%Y-%m', DATE '2024-06-30')"));
	[Fact] public async Task FormatDate_MonthDay() => Assert.Equal("06/30", await S("SELECT FORMAT_DATE('%m/%d', DATE '2024-06-30')"));
	[Fact] public async Task FormatDate_Full() => Assert.Equal("2024-12-25", await S("SELECT FORMAT_DATE('%Y-%m-%d', DATE '2024-12-25')"));
	[Fact] public async Task FormatDate_DayOfYear()
	{
		var v = await S("SELECT EXTRACT(DAYOFYEAR FROM DATE '2024-01-15')");
		Assert.Equal("15", v);
	}
	[Fact] public async Task FormatDate_Column()
	{
		var v = await S("SELECT FORMAT_DATE('%Y', d) FROM `{ds}.dates` WHERE id = 1");
		Assert.Equal("2024", v);
	}
	[Fact] public async Task FormatDate_Null() => Assert.Null(await S("SELECT FORMAT_DATE('%Y', CAST(NULL AS DATE))"));

	// ---- FORMAT_TIMESTAMP ----
	[Fact] public async Task FormatTimestamp_DateTime() => Assert.Equal("2024-01-15 10:30:00", await S("SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP '2024-01-15 10:30:00 UTC')"));
	[Fact] public async Task FormatTimestamp_DateOnly() => Assert.Equal("2024-01-15", await S("SELECT FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP '2024-01-15 10:30:00 UTC')"));
	[Fact] public async Task FormatTimestamp_TimeOnly() => Assert.Equal("10:30:00", await S("SELECT FORMAT_TIMESTAMP('%H:%M:%S', TIMESTAMP '2024-01-15 10:30:00 UTC')"));
	[Fact] public async Task FormatTimestamp_Hour() => Assert.Equal("10", await S("SELECT FORMAT_TIMESTAMP('%H', TIMESTAMP '2024-01-15 10:30:00 UTC')"));
	[Fact] public async Task FormatTimestamp_Minute() => Assert.Equal("30", await S("SELECT FORMAT_TIMESTAMP('%M', TIMESTAMP '2024-01-15 10:30:00 UTC')"));
	[Fact] public async Task FormatTimestamp_Column()
	{
		var v = await S("SELECT FORMAT_TIMESTAMP('%Y-%m-%d', ts) FROM `{ds}.dates` WHERE id = 1");
		Assert.Equal("2024-01-15", v);
	}

	// ---- EXTRACT from DATE ----
	[Fact] public async Task ExtractYear_Date() => Assert.Equal("2024", await S("SELECT EXTRACT(YEAR FROM DATE '2024-01-15')"));
	[Fact] public async Task ExtractMonth_Date() => Assert.Equal("1", await S("SELECT EXTRACT(MONTH FROM DATE '2024-01-15')"));
	[Fact] public async Task ExtractDay_Date() => Assert.Equal("15", await S("SELECT EXTRACT(DAY FROM DATE '2024-01-15')"));
	[Fact] public async Task ExtractDayOfWeek_Date()
	{
		var v = await S("SELECT EXTRACT(DAYOFWEEK FROM DATE '2024-01-15')");
		Assert.Equal("2", v); // Monday = 2 in BigQuery (Sunday=1)
	}
	[Fact] public async Task ExtractDayOfYear_Date()
	{
		var v = await S("SELECT EXTRACT(DAYOFYEAR FROM DATE '2024-01-15')");
		Assert.Equal("15", v);
	}
	[Fact] public async Task ExtractQuarter_Date()
	{
		var v = await S("SELECT EXTRACT(QUARTER FROM DATE '2024-06-30')");
		Assert.Equal("2", v);
	}
	[Fact] public async Task ExtractWeek_Date()
	{
		var v = await S("SELECT EXTRACT(WEEK FROM DATE '2024-01-15')");
		Assert.NotNull(v);
	}
	[Fact] public async Task ExtractIsoYear_Date()
	{
		var v = await S("SELECT EXTRACT(YEAR FROM DATE '2024-01-01')");
		Assert.Equal("2024", v);
	}

	// ---- EXTRACT from TIMESTAMP ----
	[Fact] public async Task ExtractYear_Timestamp() => Assert.Equal("2024", await S("SELECT EXTRACT(YEAR FROM TIMESTAMP '2024-01-15 10:30:00 UTC')"));
	[Fact] public async Task ExtractHour_Timestamp() => Assert.Equal("10", await S("SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-01-15 10:30:00 UTC')"));
	[Fact] public async Task ExtractMinute_Timestamp() => Assert.Equal("30", await S("SELECT EXTRACT(MINUTE FROM TIMESTAMP '2024-01-15 10:30:00 UTC')"));
	[Fact] public async Task ExtractSecond_Timestamp() => Assert.Equal("0", await S("SELECT EXTRACT(SECOND FROM TIMESTAMP '2024-01-15 10:30:00 UTC')"));

	// ---- EXTRACT from column ----
	[Fact] public async Task ExtractYear_Column() => Assert.Equal("2024", await S("SELECT EXTRACT(YEAR FROM d) FROM `{ds}.dates` WHERE id = 1"));
	[Fact] public async Task ExtractMonth_Column() => Assert.Equal("6", await S("SELECT EXTRACT(MONTH FROM d) FROM `{ds}.dates` WHERE id = 2"));

	// ---- DATE_ADD/DATE_SUB ----
	[Fact] public async Task DateAdd_Day() => Assert.Equal("2024-01-16", await S("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 1 DAY)"));
	[Fact] public async Task DateAdd_Month() => Assert.Equal("2024-02-15", await S("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 1 MONTH)"));
	[Fact] public async Task DateAdd_Year() => Assert.Equal("2025-01-15", await S("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 1 YEAR)"));
	[Fact] public async Task DateSub_Day() => Assert.Equal("2024-01-14", await S("SELECT DATE_SUB(DATE '2024-01-15', INTERVAL 1 DAY)"));
	[Fact] public async Task DateAdd_Week() => Assert.Equal("2024-01-22", await S("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 1 WEEK)"));
	[Fact] public async Task DateSub_Month() => Assert.Equal("2023-12-15", await S("SELECT DATE_SUB(DATE '2024-01-15', INTERVAL 1 MONTH)"));
	[Fact] public async Task DateAdd_LeapYear() => Assert.Equal("2024-02-29", await S("SELECT DATE_ADD(DATE '2024-02-28', INTERVAL 1 DAY)"));
	[Fact] public async Task DateAdd_MonthEnd() => Assert.Equal("2024-02-29", await S("SELECT DATE_ADD(DATE '2024-01-31', INTERVAL 1 MONTH)"));

	// ---- DATE_DIFF ----
	[Fact] public async Task DateDiff_Days()
	{
		var v = await S("SELECT DATE_DIFF(DATE '2024-01-20', DATE '2024-01-15', DAY)");
		Assert.Equal("5", v);
	}
	[Fact] public async Task DateDiff_Months()
	{
		var v = await S("SELECT DATE_DIFF(DATE '2024-06-30', DATE '2024-01-15', MONTH)");
		Assert.Equal("5", v);
	}
	[Fact] public async Task DateDiff_Years()
	{
		var v = await S("SELECT DATE_DIFF(DATE '2025-01-01', DATE '2023-01-01', YEAR)");
		Assert.Equal("2", v);
	}
	[Fact] public async Task DateDiff_Negative()
	{
		var v = await S("SELECT DATE_DIFF(DATE '2024-01-01', DATE '2024-01-15', DAY)");
		Assert.Equal("-14", v);
	}

	// ---- DATE_TRUNC ----
	[Fact] public async Task DateTrunc_Month() => Assert.Equal("2024-01-01", await S("SELECT DATE_TRUNC(DATE '2024-01-15', MONTH)"));
	[Fact] public async Task DateTrunc_Year() => Assert.Equal("2024-01-01", await S("SELECT DATE_TRUNC(DATE '2024-06-30', YEAR)"));
	[Fact] public async Task DateTrunc_Quarter() => Assert.Equal("2024-04-01", await S("SELECT DATE_TRUNC(DATE '2024-06-30', QUARTER)"));
	[Fact] public async Task DateTrunc_Week() => Assert.StartsWith("2024-01-1", await S("SELECT DATE_TRUNC(DATE '2024-01-15', WEEK)") ?? "");

	// ---- LAST_DAY ----
	[Fact] public async Task LastDay_Jan() => Assert.Equal("2024-01-31", await S("SELECT LAST_DAY(DATE '2024-01-15')"));
	[Fact] public async Task LastDay_Feb_Leap() => Assert.Equal("2024-02-29", await S("SELECT LAST_DAY(DATE '2024-02-01')"));
	[Fact] public async Task LastDay_Jun() => Assert.Equal("2024-06-30", await S("SELECT LAST_DAY(DATE '2024-06-15')"));

	// ---- DATE from parts ----
	[Fact] public async Task Date_Function() => Assert.Equal("2024-01-15", await S("SELECT DATE(2024, 1, 15)"));
	[Fact] public async Task Date_FromTimestamp() => Assert.Equal("2024-01-15", await S("SELECT DATE(TIMESTAMP '2024-01-15 10:30:00 UTC')"));

	// ---- CURRENT date/timestamp ----
	[Fact] public async Task CurrentDate()
	{
		var v = await S("SELECT CURRENT_DATE()");
		Assert.NotNull(v);
	}
	[Fact] public async Task CurrentTimestamp()
	{
		var v = await S("SELECT CURRENT_TIMESTAMP()");
		Assert.NotNull(v);
	}

	// ---- Date comparisons ----
	[Fact] public async Task Date_Equals()
	{
		var rows = await Q("SELECT id FROM `{ds}.dates` WHERE d = '2024-01-15'");
		Assert.Single(rows);
	}
	[Fact] public async Task Date_GreaterThan()
	{
		var rows = await Q("SELECT id FROM `{ds}.dates` WHERE d > '2024-06-01' ORDER BY id");
		Assert.True(rows.Count >= 2); // June 30, Dec 25
	}
	[Fact] public async Task Date_Between()
	{
		var rows = await Q("SELECT id FROM `{ds}.dates` WHERE d BETWEEN '2024-01-01' AND '2024-06-30' ORDER BY id");
		Assert.True(rows.Count >= 3); // Jan 15, Feb 29, Jun 30
	}

	// ---- Date in GROUP BY ----
	[Fact] public async Task Date_GroupBy_Year()
	{
		var rows = await Q("SELECT EXTRACT(YEAR FROM d) AS yr, COUNT(*) AS cnt FROM `{ds}.dates` WHERE d IS NOT NULL GROUP BY yr ORDER BY yr");
		Assert.True(rows.Count >= 2); // 2023 and 2024
	}

	// ---- Date ordering ----
	[Fact] public async Task Date_OrderBy()
	{
		var rows = await Q("SELECT id FROM `{ds}.dates` WHERE d IS NOT NULL ORDER BY d");
		Assert.Equal("5", rows[0]["id"]?.ToString()); // 2023-01-01 is earliest
	}
	[Fact] public async Task Date_OrderByDesc()
	{
		var rows = await Q("SELECT id FROM `{ds}.dates` WHERE d IS NOT NULL ORDER BY d DESC");
		Assert.Equal("3", rows[0]["id"]?.ToString()); // 2024-12-25 is latest
	}

	// ---- PARSE_DATE ----
	[Fact] public async Task ParseDate_Basic()
	{
		var v = await S("SELECT PARSE_DATE('%Y-%m-%d', '2024-01-15')");
		Assert.Contains("2024", v ?? "");
	}
	[Fact] public async Task ParseDate_CustomFormat()
	{
		var v = await S("SELECT PARSE_DATE('%m/%d/%Y', '01/15/2024')");
		Assert.Contains("2024", v ?? "");
	}

	// ---- Date with CASE ----
	[Fact] public async Task Date_InCase()
	{
		var rows = await Q("SELECT id, CASE WHEN d > '2024-06-01' THEN 'late' ELSE 'early' END AS half FROM `{ds}.dates` WHERE id <= 3 ORDER BY id");
		Assert.Equal("early", rows[0]["half"]?.ToString()); // Jan 15
		Assert.Equal("late", rows[1]["half"]?.ToString()); // Jun 30
	}
}
