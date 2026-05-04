using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Date function comprehensive coverage: DATE, DATE_ADD, DATE_SUB, DATE_DIFF, DATE_TRUNC, FORMAT_DATE, PARSE_DATE.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DateFunctionCoverageTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public DateFunctionCoverageTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_dfc_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.events` (id INT64, event_date DATE, label STRING)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.events` VALUES
			(1, DATE '2024-01-15', 'new_year'),
			(2, DATE '2024-03-20', 'spring'),
			(3, DATE '2024-06-21', 'summer'),
			(4, DATE '2024-09-22', 'autumn'),
			(5, DATE '2024-12-25', 'christmas')", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); if (rows.Count == 0) return null; var val = rows[0][0]; if (val is DateTime dt) return dt.ToString("yyyy-MM-dd"); return val?.ToString(); }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }
	private string Dt(object? val) { if (val is DateTime dt) return dt.ToString("yyyy-MM-dd"); return val?.ToString() ?? ""; }

	// ---- CURRENT_DATE ----
	[Fact] public async Task CurrentDate_NotNull()
	{
		var v = await S("SELECT CURRENT_DATE()");
		Assert.NotNull(v);
		Assert.Contains("-", v);
	}

	// ---- DATE literals ----
	[Fact] public async Task Date_Literal() => Assert.Equal("2024-01-01", await S("SELECT DATE '2024-01-01'"));

	// ---- DATE_ADD ----
	[Fact] public async Task DateAdd_Day() => Assert.Equal("2024-01-20", await S("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 5 DAY)"));
	[Fact] public async Task DateAdd_Month() => Assert.Equal("2024-02-15", await S("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 1 MONTH)"));
	[Fact] public async Task DateAdd_Year() => Assert.Equal("2025-01-15", await S("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 1 YEAR)"));
	[Fact] public async Task DateAdd_Week() => Assert.Equal("2024-01-22", await S("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 1 WEEK)"));
	[Fact] public async Task DateAdd_Negative() => Assert.Equal("2024-01-10", await S("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL -5 DAY)"));

	// ---- DATE_SUB ----
	[Fact] public async Task DateSub_Day() => Assert.Equal("2024-01-10", await S("SELECT DATE_SUB(DATE '2024-01-15', INTERVAL 5 DAY)"));
	[Fact] public async Task DateSub_Month() => Assert.Equal("2023-12-15", await S("SELECT DATE_SUB(DATE '2024-01-15', INTERVAL 1 MONTH)"));
	[Fact] public async Task DateSub_Year() => Assert.Equal("2023-01-15", await S("SELECT DATE_SUB(DATE '2024-01-15', INTERVAL 1 YEAR)"));

	// ---- DATE_DIFF ----
	[Fact] public async Task DateDiff_Day()
	{
		var v = await S("SELECT DATE_DIFF(DATE '2024-01-20', DATE '2024-01-15', DAY)");
		Assert.Equal("5", v);
	}
	[Fact] public async Task DateDiff_Month()
	{
		var v = await S("SELECT DATE_DIFF(DATE '2024-06-15', DATE '2024-01-15', MONTH)");
		Assert.Equal("5", v);
	}
	[Fact] public async Task DateDiff_Year()
	{
		var v = await S("SELECT DATE_DIFF(DATE '2025-01-15', DATE '2024-01-15', YEAR)");
		Assert.Equal("1", v);
	}
	[Fact] public async Task DateDiff_Negative()
	{
		var v = await S("SELECT DATE_DIFF(DATE '2024-01-10', DATE '2024-01-15', DAY)");
		Assert.Equal("-5", v);
	}

	// ---- DATE_TRUNC ----
	[Fact] public async Task DateTrunc_Month() => Assert.Equal("2024-03-01", await S("SELECT DATE_TRUNC(DATE '2024-03-20', MONTH)"));
	[Fact] public async Task DateTrunc_Year() => Assert.Equal("2024-01-01", await S("SELECT DATE_TRUNC(DATE '2024-06-15', YEAR)"));
	[Fact] public async Task DateTrunc_Week() => Assert.Equal("2024-01-14", await S("SELECT DATE_TRUNC(DATE '2024-01-15', WEEK)"));

	// ---- EXTRACT from DATE ----
	[Fact] public async Task Extract_Year()
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync($"SELECT EXTRACT(YEAR FROM event_date) FROM `{_ds}.events` WHERE id = 1", parameters: null);
		Assert.Equal("2024", r.ToList()[0][0]?.ToString());
	}
	[Fact] public async Task Extract_Month()
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync($"SELECT EXTRACT(MONTH FROM event_date) FROM `{_ds}.events` WHERE id = 2", parameters: null);
		Assert.Equal("3", r.ToList()[0][0]?.ToString());
	}
	[Fact] public async Task Extract_Day()
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync($"SELECT EXTRACT(DAY FROM event_date) FROM `{_ds}.events` WHERE id = 3", parameters: null);
		Assert.Equal("21", r.ToList()[0][0]?.ToString());
	}
	[Fact] public async Task Extract_DayOfWeek()
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync($"SELECT EXTRACT(DAYOFWEEK FROM event_date) FROM `{_ds}.events` WHERE id = 1", parameters: null);
		// Jan 15, 2024 is Monday → DAYOFWEEK 2 (Sunday=1)
		Assert.Equal("2", r.ToList()[0][0]?.ToString());
	}

	// ---- FORMAT_DATE ----
	[Fact] public async Task FormatDate_YMD()
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync($"SELECT FORMAT_DATE('%Y-%m-%d', event_date) FROM `{_ds}.events` WHERE id = 1", parameters: null);
		Assert.Equal("2024-01-15", r.ToList()[0][0]?.ToString());
	}
	[Fact] public async Task FormatDate_Slash()
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync($"SELECT FORMAT_DATE('%m/%d/%Y', event_date) FROM `{_ds}.events` WHERE id = 1", parameters: null);
		Assert.Equal("01/15/2024", r.ToList()[0][0]?.ToString());
	}
	[Fact] public async Task FormatDate_MonthName()
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync($"SELECT FORMAT_DATE('%B', event_date) FROM `{_ds}.events` WHERE id = 3", parameters: null);
		Assert.Equal("June", r.ToList()[0][0]?.ToString());
	}

	// ---- PARSE_DATE ----
	[Fact] public async Task ParseDate_Basic() => Assert.Equal("2024-01-15", await S("SELECT PARSE_DATE('%Y-%m-%d', '2024-01-15')"));
	[Fact] public async Task ParseDate_Slash() => Assert.Equal("2024-03-20", await S("SELECT PARSE_DATE('%m/%d/%Y', '03/20/2024')"));

	// ---- Table queries ----
	[Fact] public async Task Table_DateFilter()
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync($"SELECT COUNT(*) FROM `{_ds}.events` WHERE event_date > DATE '2024-06-01'", parameters: null);
		Assert.Equal("3", r.ToList()[0][0]?.ToString()); // summer, autumn, christmas
	}
	[Fact] public async Task Table_DateDiffBetweenRows()
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync($"SELECT DATE_DIFF(MAX(event_date), MIN(event_date), DAY) FROM `{_ds}.events`", parameters: null);
		Assert.Equal("345", r.ToList()[0][0]?.ToString()); // Jan 15 to Dec 25 = 345 days
	}
	[Fact] public async Task Table_OrderByDate()
	{
		var rows = await Q($"SELECT label FROM `{_ds}.events` ORDER BY event_date DESC LIMIT 1");
		Assert.Equal("christmas", rows[0]["label"]?.ToString());
	}
	[Fact] public async Task Table_GroupByMonth()
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync($"SELECT EXTRACT(MONTH FROM event_date) AS m, COUNT(*) AS cnt FROM `{_ds}.events` GROUP BY m ORDER BY m", parameters: null);
		var rows = r.ToList();
		Assert.Equal(5, rows.Count);
	}
}
