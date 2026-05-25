using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 42: FIRST_VALUE/LAST_VALUE IGNORE NULLS,
/// TIMESTAMP_TRUNC with timezone, TIMESTAMP_DIFF NANOSECOND.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests42 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public ParityVerificationTests42(BigQuerySession session) => _session = session;

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

	private async Task<List<string>> ColumnAsync(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		return result.Select(r => r[0]?.ToString() ?? "NULL").ToList();
	}

	// ============================================================
	// FIRST_VALUE with IGNORE NULLS
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#first_value
	//   "If ignore_nulls is true, FIRST_VALUE excludes NULL values from the calculation."
	[Fact]
	public async Task FirstValue_IgnoreNulls_SkipsNull()
	{
		var sql = @"
			SELECT FIRST_VALUE(x IGNORE NULLS) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
			FROM UNNEST([STRUCT(1 AS id, CAST(NULL AS INT64) AS x), STRUCT(2 AS id, 10 AS x), STRUCT(3 AS id, 20 AS x)])";
		var results = await ColumnAsync(sql);
		// First non-null across entire partition is 10 for all rows
		Assert.All(results, r => Assert.Equal("10", r));
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task FirstValue_RespectNulls_Default()
	{
		var sql = @"
			SELECT FIRST_VALUE(x) OVER (ORDER BY id)
			FROM UNNEST([STRUCT(1 AS id, CAST(NULL AS INT64) AS x), STRUCT(2 AS id, 10 AS x), STRUCT(3 AS id, 20 AS x)])";
		var results = await ColumnAsync(sql);
		// Default behavior: first value is NULL
		Assert.All(results, r => Assert.Equal("NULL", r));
	}

	// ============================================================
	// LAST_VALUE with IGNORE NULLS
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#last_value
	//   "If ignore_nulls is true, LAST_VALUE excludes NULL values from the calculation."
	[Fact]
	public async Task LastValue_IgnoreNulls_SkipsNull()
	{
		var sql = @"
			SELECT LAST_VALUE(x IGNORE NULLS) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
			FROM UNNEST([STRUCT(1 AS id, 10 AS x), STRUCT(2 AS id, 20 AS x), STRUCT(3 AS id, CAST(NULL AS INT64) AS x)])";
		var results = await ColumnAsync(sql);
		// Last non-null is 20 for all rows
		Assert.All(results, r => Assert.Equal("20", r));
	}

	[Fact]
	public async Task LastValue_AllNulls_ReturnsNull()
	{
		var sql = @"
			SELECT LAST_VALUE(x IGNORE NULLS) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
			FROM UNNEST([STRUCT(1 AS id, CAST(NULL AS INT64) AS x), STRUCT(2 AS id, CAST(NULL AS INT64) AS x)])";
		var results = await ColumnAsync(sql);
		Assert.All(results, r => Assert.Equal("NULL", r));
	}

	// ============================================================
	// TIMESTAMP_TRUNC with timezone
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_trunc
	//   "TIMESTAMP_TRUNC(timestamp_expression, date_time_part[, timezone])"
	//   "If time_zone is specified, the truncation is performed with respect to that time zone."
	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task TimestampTrunc_Month_WithTimezone()
	{
		// 2024-03-15 06:00:00 UTC = 2024-03-14 23:00:00 America/Los_Angeles (PDT starts March 10)
		// Truncating to MONTH in LA timezone: 2024-03-01 00:00:00 PDT = 2024-03-01 08:00:00 UTC
		var result = await ScalarAsync(
			"SELECT CAST(TIMESTAMP_TRUNC(TIMESTAMP '2024-03-15 06:00:00 UTC', MONTH, 'America/Los_Angeles') AS STRING)");
		Assert.Equal("2024-03-01 08:00:00+00", result);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task TimestampTrunc_Day_WithTimezone()
	{
		// 2024-01-15 03:00:00 UTC = 2024-01-14 19:00:00 America/Los_Angeles (PST, -8)
		// Truncating to DAY in LA timezone: 2024-01-14 00:00:00 PST = 2024-01-14 08:00:00 UTC
		var result = await ScalarAsync(
			"SELECT CAST(TIMESTAMP_TRUNC(TIMESTAMP '2024-01-15 03:00:00 UTC', DAY, 'America/Los_Angeles') AS STRING)");
		Assert.Equal("2024-01-14 08:00:00+00", result);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task TimestampTrunc_Hour_WithTimezone_SameAsWithout()
	{
		// Truncating to HOUR should be the same regardless of timezone since offsets don't affect hours
		var result = await ScalarAsync(
			"SELECT CAST(TIMESTAMP_TRUNC(TIMESTAMP '2024-06-15 14:30:00 UTC', HOUR, 'America/Los_Angeles') AS STRING)");
		Assert.Equal("2024-06-15 14:00:00+00", result);
	}

	// ============================================================
	// TIMESTAMP_DIFF with NANOSECOND
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_diff
	//   BigQuery: "TIMESTAMP_DIFF does not support the NANOSECOND date part"
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task TimestampDiff_Nanosecond()
	{
		// 1 second = 1,000,000,000 nanoseconds
		var result = await ScalarAsync(
			"SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-01 00:00:01 UTC', TIMESTAMP '2024-01-01 00:00:00 UTC', NANOSECOND)");
		Assert.Equal("1000000000", result);
	}

	[Fact]
	public async Task TimestampDiff_Microsecond()
	{
		// 1 second = 1,000,000 microseconds
		var result = await ScalarAsync(
			"SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-01 00:00:01 UTC', TIMESTAMP '2024-01-01 00:00:00 UTC', MICROSECOND)");
		Assert.Equal("1000000", result);
	}

	[Fact]
	public async Task TimestampDiff_Millisecond()
	{
		// 1 second = 1,000 milliseconds
		var result = await ScalarAsync(
			"SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-01 00:00:01 UTC', TIMESTAMP '2024-01-01 00:00:00 UTC', MILLISECOND)");
		Assert.Equal("1000", result);
	}
}
