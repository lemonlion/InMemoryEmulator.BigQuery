using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Timestamp and timezone handling: TIMESTAMP, CURRENT_TIMESTAMP, TIMESTAMP_ADD, TIMESTAMP_DIFF,
/// TIMESTAMP_TRUNC, FORMAT_TIMESTAMP, PARSE_TIMESTAMP.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class TimestampFunctionCoverageTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public TimestampFunctionCoverageTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_tfc_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.events` (id INT64, ts TIMESTAMP, label STRING)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.events` VALUES
			(1, TIMESTAMP '2024-01-15 10:30:00 UTC', 'event_a'),
			(2, TIMESTAMP '2024-03-20 14:45:00 UTC', 'event_b'),
			(3, TIMESTAMP '2024-06-01 08:00:00 UTC', 'event_c'),
			(4, TIMESTAMP '2024-06-01 22:15:30 UTC', 'event_d'),
			(5, TIMESTAMP '2024-12-31 23:59:59 UTC', 'event_e')", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); if (rows.Count == 0) return null; var val = rows[0][0]; if (val is DateTime dt) return dt.ToString("yyyy-MM-dd HH:mm:ss"); return val?.ToString(); }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- CURRENT_TIMESTAMP ----
	[Fact] public async Task CurrentTimestamp_NotNull()
	{
		var v = await S("SELECT CURRENT_TIMESTAMP()");
		Assert.NotNull(v);
	}

	// ---- TIMESTAMP literals ----
	[Fact] public async Task Timestamp_Literal()
	{
		var v = await S("SELECT TIMESTAMP '2024-01-01 00:00:00 UTC'");
		Assert.NotNull(v);
		Assert.Contains("2024-01-01", v);
	}

	// ---- TIMESTAMP_ADD ----
	[Fact] public async Task TimestampAdd_Day()
	{
		var v = await S("SELECT TIMESTAMP_ADD(TIMESTAMP '2024-01-15 10:30:00 UTC', INTERVAL 5 DAY)");
		Assert.NotNull(v);
		Assert.Contains("2024-01-20", v);
	}
	[Fact] public async Task TimestampAdd_Hour()
	{
		var v = await S("SELECT TIMESTAMP_ADD(TIMESTAMP '2024-01-15 10:30:00 UTC', INTERVAL 3 HOUR)");
		Assert.NotNull(v);
		Assert.Contains("13:30:00", v);
	}
	[Fact] public async Task TimestampAdd_Minute()
	{
		var v = await S("SELECT TIMESTAMP_ADD(TIMESTAMP '2024-01-15 10:30:00 UTC', INTERVAL 45 MINUTE)");
		Assert.NotNull(v);
		Assert.Contains("11:15:00", v);
	}
	[Fact] public async Task TimestampAdd_Second()
	{
		var v = await S("SELECT TIMESTAMP_ADD(TIMESTAMP '2024-01-15 10:30:00 UTC', INTERVAL 90 SECOND)");
		Assert.NotNull(v);
		Assert.Contains("10:31:30", v);
	}

	// ---- TIMESTAMP_SUB ----
	[Fact] public async Task TimestampSub_Day()
	{
		var v = await S("SELECT TIMESTAMP_SUB(TIMESTAMP '2024-01-15 10:30:00 UTC', INTERVAL 5 DAY)");
		Assert.NotNull(v);
		Assert.Contains("2024-01-10", v);
	}
	[Fact] public async Task TimestampSub_Hour()
	{
		var v = await S("SELECT TIMESTAMP_SUB(TIMESTAMP '2024-01-15 10:30:00 UTC', INTERVAL 2 HOUR)");
		Assert.NotNull(v);
		Assert.Contains("08:30:00", v);
	}

	// ---- TIMESTAMP_DIFF ----
	[Fact] public async Task TimestampDiff_Day()
	{
		var v = await S("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-20 00:00:00 UTC', TIMESTAMP '2024-01-15 00:00:00 UTC', DAY)");
		Assert.Equal("5", v);
	}
	[Fact] public async Task TimestampDiff_Hour()
	{
		var v = await S("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-15 15:00:00 UTC', TIMESTAMP '2024-01-15 10:00:00 UTC', HOUR)");
		Assert.Equal("5", v);
	}
	[Fact] public async Task TimestampDiff_Second()
	{
		var v = await S("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-15 10:01:30 UTC', TIMESTAMP '2024-01-15 10:00:00 UTC', SECOND)");
		Assert.Equal("90", v);
	}

	// ---- TIMESTAMP_TRUNC ----
	[Fact] public async Task TimestampTrunc_Day()
	{
		var v = await S("SELECT TIMESTAMP_TRUNC(TIMESTAMP '2024-01-15 10:30:45 UTC', DAY)");
		Assert.NotNull(v);
		Assert.Contains("2024-01-15", v);
		Assert.Contains("00:00:00", v);
	}
	[Fact] public async Task TimestampTrunc_Hour()
	{
		var v = await S("SELECT TIMESTAMP_TRUNC(TIMESTAMP '2024-01-15 10:30:45 UTC', HOUR)");
		Assert.NotNull(v);
		Assert.Contains("10:00:00", v);
	}
	[Fact] public async Task TimestampTrunc_Month()
	{
		var v = await S("SELECT TIMESTAMP_TRUNC(TIMESTAMP '2024-03-15 10:30:00 UTC', MONTH)");
		Assert.NotNull(v);
		Assert.Contains("2024-03-01", v);
	}

	// ---- EXTRACT from TIMESTAMP ----
	[Fact] public async Task Extract_Year()
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync($"SELECT EXTRACT(YEAR FROM ts) FROM `{_ds}.events` WHERE id = 1", parameters: null);
		var rows = r.ToList();
		Assert.Equal("2024", rows[0][0]?.ToString());
	}
	[Fact] public async Task Extract_Month()
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync($"SELECT EXTRACT(MONTH FROM ts) FROM `{_ds}.events` WHERE id = 2", parameters: null);
		var rows = r.ToList();
		Assert.Equal("3", rows[0][0]?.ToString());
	}
	[Fact] public async Task Extract_Day()
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync($"SELECT EXTRACT(DAY FROM ts) FROM `{_ds}.events` WHERE id = 1", parameters: null);
		var rows = r.ToList();
		Assert.Equal("15", rows[0][0]?.ToString());
	}
	[Fact] public async Task Extract_Hour()
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync($"SELECT EXTRACT(HOUR FROM ts) FROM `{_ds}.events` WHERE id = 1", parameters: null);
		var rows = r.ToList();
		Assert.Equal("10", rows[0][0]?.ToString());
	}
	[Fact] public async Task Extract_DayOfWeek()
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync($"SELECT EXTRACT(DAYOFWEEK FROM ts) FROM `{_ds}.events` WHERE id = 1", parameters: null);
		var rows = r.ToList();
		// Jan 15, 2024 is Monday → DAYOFWEEK 2 (Sunday=1)
		Assert.Equal("2", rows[0][0]?.ToString());
	}

	// ---- FORMAT_TIMESTAMP ----
	[Fact] public async Task FormatTimestamp_Full()
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync($"SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', ts) FROM `{_ds}.events` WHERE id = 1", parameters: null);
		var rows = r.ToList();
		Assert.Equal("2024-01-15 10:30:00", rows[0][0]?.ToString());
	}
	[Fact] public async Task FormatTimestamp_DateOnly()
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync($"SELECT FORMAT_TIMESTAMP('%Y-%m-%d', ts) FROM `{_ds}.events` WHERE id = 2", parameters: null);
		var rows = r.ToList();
		Assert.Equal("2024-03-20", rows[0][0]?.ToString());
	}

	// ---- Table queries ----
	[Fact] public async Task Where_TimestampCompare()
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync($"SELECT COUNT(*) FROM `{_ds}.events` WHERE ts > TIMESTAMP '2024-06-01 00:00:00 UTC'", parameters: null);
		var rows = r.ToList();
		Assert.Equal("3", rows[0][0]?.ToString()); // event_c, event_d, event_e
	}
	[Fact] public async Task OrderBy_Timestamp()
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync($"SELECT label FROM `{_ds}.events` ORDER BY ts DESC LIMIT 1", parameters: null);
		var rows = r.ToList();
		Assert.Equal("event_e", rows[0]["label"]?.ToString());
	}
	[Fact] public async Task TimestampDiff_BetweenRows()
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync($"SELECT TIMESTAMP_DIFF(MAX(ts), MIN(ts), DAY) FROM `{_ds}.events`", parameters: null);
		var rows = r.ToList();
		Assert.Equal("351", rows[0][0]?.ToString()); // Jan 15 to Dec 31 = 351 days
	}

	// ---- Null handling ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_add
	//   "Returns NULL if any argument is NULL."
	[Fact] public async Task TimestampAdd_Null() => Assert.Null(await S("SELECT TIMESTAMP_ADD(NULL, INTERVAL 1 DAY)"));
	[Fact] public async Task TimestampDiff_Null()
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync("SELECT TIMESTAMP_DIFF(NULL, TIMESTAMP '2024-01-01 00:00:00 UTC', DAY)", parameters: null);
		var rows = r.ToList();
		Assert.Null(rows[0][0]);
	}
}
