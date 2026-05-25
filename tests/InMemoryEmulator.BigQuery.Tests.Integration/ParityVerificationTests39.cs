using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 39: EXTRACT(WEEK) returning 0-53,
/// EXTRACT(ISOWEEK) correct ISO semantics, PARSE_TIMESTAMP with %E*S,
/// DATETIME_TRUNC with ISOWEEK/ISOYEAR, FormatValue overflow protection.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests39 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public ParityVerificationTests39(BigQuerySession session) => _session = session;

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

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#extract
	//   "WEEK: Returns the week number of the date in the range [0, 53].
	//    Weeks begin with Sunday, and dates prior to the first Sunday of the year are in week 0."
	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Extract_Week_Jan1Friday_ReturnsZero()
	{
		// 2016-01-01 is a Friday — before the first Sunday (Jan 3), so week = 0
		var result = await ScalarAsync("SELECT EXTRACT(WEEK FROM DATE '2016-01-01')");
		Assert.Equal("0", result);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Extract_Week_Jan3Sunday_ReturnsOne()
	{
		// 2016-01-03 is the first Sunday → week 1
		var result = await ScalarAsync("SELECT EXTRACT(WEEK FROM DATE '2016-01-03')");
		Assert.Equal("1", result);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Extract_Week_Jan1Sunday_ReturnsOne()
	{
		// 2023-01-01 is a Sunday → week 1
		var result = await ScalarAsync("SELECT EXTRACT(WEEK FROM DATE '2023-01-01')");
		Assert.Equal("1", result);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Extract_Week_Dec31_2016()
	{
		// 2016-12-31 is Saturday, the last day before Sunday Jan 1 2017.
		// It should be in week 52 (52 Sundays have passed in 2016)
		var result = await ScalarAsync("SELECT EXTRACT(WEEK FROM DATE '2016-12-31')");
		Assert.Equal("52", result);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#extract
	//   "ISOWEEK: Returns the ISO 8601 week number."
	//   "The first Thursday of the calendar year always falls within the first ISO week."
	[Fact]
	public async Task Extract_IsoWeek_Jan1_2016()
	{
		// 2016-01-01 (Friday) is ISO week 53 of ISO year 2015
		var result = await ScalarAsync("SELECT EXTRACT(ISOWEEK FROM DATE '2016-01-01')");
		Assert.Equal("53", result);
	}

	[Fact]
	public async Task Extract_IsoWeek_Jan4_2016()
	{
		// 2016-01-04 (Monday) is ISO week 1 of 2016
		var result = await ScalarAsync("SELECT EXTRACT(ISOWEEK FROM DATE '2016-01-04')");
		Assert.Equal("1", result);
	}

	[Fact]
	public async Task Extract_IsoWeek_Dec31_2020()
	{
		// 2020-12-31 (Thursday) is ISO week 53 of 2020
		var result = await ScalarAsync("SELECT EXTRACT(ISOWEEK FROM DATE '2020-12-31')");
		Assert.Equal("53", result);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#parse_timestamp
	//   "%E*S: Seconds with full fractional precision (a literal '*')."
	[Fact]
	public async Task ParseTimestamp_E6S()
	{
		var result = await ScalarAsync(
			"SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S', PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E6S', '2024-01-15 10:30:45.123456'))");
		Assert.Equal("2024-01-15 10:30:45.123456", result);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task ParseTimestamp_E_Star_S()
	{
		var result = await ScalarAsync(
			"SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', '2024-01-15 10:30:45.123'))");
		Assert.Equal("2024-01-15 10:30:45.123", result);
	}

	[Fact]
	public async Task ParseTimestamp_E3S()
	{
		var result = await ScalarAsync(
			"SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E3S', PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3S', '2024-01-15 10:30:45.123'))");
		Assert.Equal("2024-01-15 10:30:45.123", result);
	}

	// DATETIME_TRUNC ISOWEEK / ISOYEAR
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_trunc
	//   "ISOWEEK: Truncates datetime_expression to the preceding Monday."
	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task DatetimeTrunc_IsoWeek()
	{
		// 2024-01-10 is Wednesday → truncates to Monday Jan 8
		var result = await ScalarAsync(
			"SELECT CAST(DATETIME_TRUNC(DATETIME '2024-01-10 15:30:00', ISOWEEK) AS STRING)");
		Assert.Equal("2024-01-08 00:00:00", result);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task DatetimeTrunc_IsoYear()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_trunc
		//   "ISOYEAR: Truncates datetime_expression to the preceding ISO 8601 week-numbering year boundary."
		// ISO year 2024 starts on Monday 2024-01-01
		var result = await ScalarAsync(
			"SELECT CAST(DATETIME_TRUNC(DATETIME '2024-03-15 12:00:00', ISOYEAR) AS STRING)");
		Assert.Equal("2024-01-01 00:00:00", result);
	}

	// FormatValue double overflow: very large double values should not crash
	[Fact]
	public async Task LargeFloat64_DoesNotCrash()
	{
		var result = await ScalarAsync("SELECT 1.0e18 * 1000");
		// Should be 1e21 — a valid double, should not crash even though > long.MaxValue
		Assert.NotNull(result);
		Assert.NotEqual("NULL", result);
	}

	// ParseTimestamp with %A (day name) and %a (abbreviated day name)
	[Fact]
	public async Task ParseTimestamp_DayName()
	{
		var result = await ScalarAsync(
			"SELECT FORMAT_TIMESTAMP('%Y-%m-%d', PARSE_TIMESTAMP('%A, %B %d, %Y', 'Thursday, December 25, 2008'))");
		Assert.Equal("2008-12-25", result);
	}
}
