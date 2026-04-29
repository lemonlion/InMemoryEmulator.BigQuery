using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration.Phase25;

/// <summary>
/// Integration tests for Phase 25: missing date/time/datetime/timestamp functions,
/// UNPIVOT operator, and TABLESAMPLE operator.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class DateTimeFunctionIntegrationTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public DateTimeFunctionIntegrationTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_p25_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();

		// Scores table for UNPIVOT / TABLESAMPLE tests
		var scoresSchema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "student", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "math", Type = "INTEGER", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "english", Type = "INTEGER", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "science", Type = "INTEGER", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "scores", scoresSchema);
		await client.InsertRowsAsync(_datasetId, "scores", new[]
		{
			new BigQueryInsertRow("r1") { ["student"] = "Alice", ["math"] = 90, ["english"] = 85, ["science"] = 92 },
			new BigQueryInsertRow("r2") { ["student"] = "Bob", ["math"] = 80, ["english"] = 88, ["science"] = 75 },
		});
	}

	public async ValueTask DisposeAsync()
	{
		try
		{
			var client = await _fixture.GetClientAsync();
			await client.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true });
		}
		catch { }
		await _fixture.DisposeAsync();
	}

	#region DATE Functions

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_from_unix_date
	[Fact]
	public async Task DateFromUnixDate_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT DATE_FROM_UNIX_DATE(14238) AS d", parameters: null);
		var rows = results.ToList();
		Assert.Equal(new DateTime(2008, 12, 25), (DateTime)rows[0]["d"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#unix_date
	[Fact]
	public async Task UnixDate_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT UNIX_DATE(DATE '2008-12-25') AS d", parameters: null);
		Assert.Equal(14238L, Convert.ToInt64(results.ToList()[0]["d"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#last_day
	[Fact]
	public async Task LastDay_Month()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT LAST_DAY(DATE '2008-11-10', MONTH) AS d", parameters: null);
		Assert.Equal(new DateTime(2008, 11, 30), (DateTime)results.ToList()[0]["d"]);
	}

	[Fact]
	public async Task LastDay_Year()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT LAST_DAY(DATE '2008-11-10', YEAR) AS d", parameters: null);
		Assert.Equal(new DateTime(2008, 12, 31), (DateTime)results.ToList()[0]["d"]);
	}

	#endregion

	#region DATETIME Functions

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_add
	[Fact]
	public async Task DatetimeAdd_Days()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT DATETIME_ADD(DATETIME '2008-12-25 15:30:00', INTERVAL 10 DAY) AS result",
			parameters: null);
		var dt = Assert.IsType<DateTime>(results.ToList()[0]["result"]);
		Assert.Equal(2009, dt.Year);
		Assert.Equal(1, dt.Month);
		Assert.Equal(4, dt.Day);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_diff
	[Fact]
	public async Task DatetimeDiff_Day()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT DATETIME_DIFF(DATETIME '2010-07-07 10:20:00', DATETIME '2008-12-25 15:30:00', DAY) AS d",
			parameters: null);
		Assert.Equal(559L, Convert.ToInt64(results.ToList()[0]["d"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_trunc
	[Fact]
	public async Task DatetimeTrunc_Hour()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT DATETIME_TRUNC(DATETIME '2008-12-25 15:30:45', HOUR) AS result",
			parameters: null);
		var dt = Assert.IsType<DateTime>(results.ToList()[0]["result"]);
		Assert.Equal(15, dt.Hour);
		Assert.Equal(0, dt.Minute);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#format_datetime
	[Fact]
	public async Task FormatDatetime_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', DATETIME '2008-12-25 15:30:00') AS result",
			parameters: null);
		Assert.Equal("2008-12-25 15:30:00", results.ToList()[0]["result"]?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#parse_datetime
	[Fact]
	public async Task ParseDatetime_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT PARSE_DATETIME('%Y-%m-%d %H:%M:%S', '2008-12-25 15:30:00') AS result",
			parameters: null);
		var dt = Assert.IsType<DateTime>(results.ToList()[0]["result"]);
		Assert.Equal(2008, dt.Year);
		Assert.Equal(12, dt.Month);
		Assert.Equal(25, dt.Day);
	}

	#endregion

	#region TIME Functions

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time
	[Fact]
	public async Task Time_FromComponents()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT TIME(15, 30, 0) AS t", parameters: null);
		Assert.Equal("15:30:00", results.ToList()[0]["t"]?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time_add
	[Fact]
	public async Task TimeAdd_Minutes()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT TIME_ADD(TIME '15:30:00', INTERVAL 10 MINUTE) AS t", parameters: null);
		Assert.Equal("15:40:00", results.ToList()[0]["t"]?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time_diff
	[Fact]
	public async Task TimeDiff_Minutes()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT TIME_DIFF(TIME '16:25:00', TIME '15:30:00', MINUTE) AS d", parameters: null);
		Assert.Equal(55L, Convert.ToInt64(results.ToList()[0]["d"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time_trunc
	[Fact]
	public async Task TimeTrunc_Hour()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT TIME_TRUNC(TIME '15:30:45', HOUR) AS t", parameters: null);
		Assert.Equal("15:00:00", results.ToList()[0]["t"]?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#format_time
	[Fact]
	public async Task FormatTime_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT FORMAT_TIME('%T', TIME '15:30:45') AS result", parameters: null);
		Assert.Equal("15:30:45", results.ToList()[0]["result"]?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#parse_time
	[Fact]
	public async Task ParseTime_Full()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT PARSE_TIME('%H:%M:%S', '15:30:45') AS t", parameters: null);
		Assert.Equal("15:30:45", results.ToList()[0]["t"]?.ToString());
	}

	#endregion

	#region TIMESTAMP Functions

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#unix_micros
	[Fact]
	public async Task UnixMicros_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT UNIX_MICROS(TIMESTAMP '1970-01-01 00:00:03.210000 UTC') AS us", parameters: null);
		Assert.Equal(3210000L, Convert.ToInt64(results.ToList()[0]["us"]));
	}

	#endregion

	#region UNPIVOT

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unpivot_operator
	[Fact]
	public async Task Unpivot_BasicColumns()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT * FROM `{_datasetId}.scores` UNPIVOT(score FOR subject IN (math, english, science))",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(6, rows.Count); // 2 students × 3 subjects
	}

	#endregion

	#region TABLESAMPLE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#tablesample_operator
	[Fact]
	public async Task Tablesample_100Percent_ReturnsAll()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT * FROM `{_datasetId}.scores` TABLESAMPLE SYSTEM (100 PERCENT)",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(2, rows.Count);
	}

	#endregion
}
