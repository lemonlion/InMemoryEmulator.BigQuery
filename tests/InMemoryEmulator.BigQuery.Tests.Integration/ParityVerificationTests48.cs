using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 48: NULL handling in TIME_ADD/SUB/TRUNC,
/// DATETIME_TRUNC, FORMAT_DATE, DATE_FROM_UNIX_DATE, UNIX_DATE,
/// GENERATE_TIMESTAMP_ARRAY, DATE/DATETIME constructor UTC.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests48 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public ParityVerificationTests48(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
	}

	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string> ScalarAsync(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var row = result.First();
		return row[0]?.ToString() ?? "NULL";
	}

	// ============================================================
	// TIME_ADD / TIME_SUB NULL handling
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time_add
	//   Returns NULL if any argument is NULL.
	[Fact]
	public async Task TimeAdd_NullTime_ReturnsNull()
	{
		var result = await ScalarAsync("SELECT TIME_ADD(CAST(NULL AS TIME), INTERVAL 1 HOUR)");
		Assert.Equal("NULL", result);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time_sub
	//   Returns NULL if any argument is NULL.
	[Fact]
	public async Task TimeSub_NullTime_ReturnsNull()
	{
		var result = await ScalarAsync("SELECT TIME_SUB(CAST(NULL AS TIME), INTERVAL 1 HOUR)");
		Assert.Equal("NULL", result);
	}

	// ============================================================
	// TIME_TRUNC NULL handling
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time_trunc
	//   Returns NULL if time_expression is NULL.
	[Fact]
	public async Task TimeTrunc_NullTime_ReturnsNull()
	{
		var result = await ScalarAsync("SELECT TIME_TRUNC(CAST(NULL AS TIME), HOUR)");
		Assert.Equal("NULL", result);
	}

	// ============================================================
	// DATETIME_TRUNC NULL handling
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_trunc
	//   Returns NULL if datetime_expression is NULL.
	[Fact]
	public async Task DatetimeTrunc_NullDatetime_ReturnsNull()
	{
		var result = await ScalarAsync(
			"SELECT DATETIME_TRUNC(CAST(NULL AS DATETIME), DAY)");
		Assert.Equal("NULL", result);
	}

	// ============================================================
	// FORMAT_DATE NULL format
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#format_date
	//   "Returns NULL if any argument is NULL."
	[Fact]
	public async Task FormatDate_NullFormat_ReturnsNull()
	{
		var result = await ScalarAsync(
			"SELECT FORMAT_DATE(CAST(NULL AS STRING), DATE '2024-01-01')");
		Assert.Equal("NULL", result);
	}

	// ============================================================
	// DATE_FROM_UNIX_DATE NULL handling
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_from_unix_date
	//   Returns NULL if int64_expression is NULL.
	[Fact]
	public async Task DateFromUnixDate_Null_ReturnsNull()
	{
		var result = await ScalarAsync("SELECT DATE_FROM_UNIX_DATE(CAST(NULL AS INT64))");
		Assert.Equal("NULL", result);
	}

	// ============================================================
	// UNIX_DATE NULL handling
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#unix_date
	//   Returns NULL if date_expression is NULL.
	[Fact]
	public async Task UnixDate_Null_ReturnsNull()
	{
		var result = await ScalarAsync("SELECT UNIX_DATE(CAST(NULL AS DATE))");
		Assert.Equal("NULL", result);
	}

	// ============================================================
	// GENERATE_TIMESTAMP_ARRAY NULL step
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_timestamp_array
	//   BigQuery: "Invalid cast from ARRAY<TIMESTAMP> to STRING"
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task GenerateTimestampArray_NullStep_ReturnsNull()
	{
		var result = await ScalarAsync(
			"SELECT CAST(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP '2024-01-01', TIMESTAMP '2024-01-02', INTERVAL CAST(NULL AS INT64) HOUR) AS STRING)");
		Assert.Equal("NULL", result);
	}

	// ============================================================
	// DATE() and DATETIME() constructor UTC behavior
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date
	//   "Extracts the DATE from a TIMESTAMP expression ... using UTC."
	[Fact]
	public async Task DateConstructor_FromTimestamp_UsesUtc()
	{
		var result = await ScalarAsync(
			"SELECT CAST(DATE(TIMESTAMP '2024-01-01 23:00:00-05:00') AS STRING)");
		Assert.Equal("2024-01-02", result);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime
	//   "If TIMESTAMP, the datetime is extracted at UTC."
	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task DatetimeConstructor_FromTimestamp_UsesUtc()
	{
		var result = await ScalarAsync(
			"SELECT CAST(DATETIME(TIMESTAMP '2024-01-01 23:00:00-05:00') AS STRING)");
		Assert.Equal("2024-01-02 04:00:00", result);
	}
}
