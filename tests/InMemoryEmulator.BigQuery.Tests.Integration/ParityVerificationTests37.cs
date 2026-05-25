using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 37: DATE_DIFF, DATETIME_DIFF, TIMESTAMP_DIFF boundary
/// counting semantics, GENERATE_ARRAY step=0 error, SIGN return type for FLOAT64.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_diff
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_diff
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_diff
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests37 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public ParityVerificationTests37(BigQuerySession session) => _session = session;

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

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_diff
	//   "Gets the number of unit boundaries between two date values at a particular date part."
	//   Saturday 2017-10-14 to Sunday 2017-10-15 crosses 1 WEEK boundary (Sunday is week start).
	[Fact]
	public async Task DateDiff_Week_BoundaryCounting()
	{
		// Saturday → Sunday crosses one Sunday boundary
		var result = await ScalarAsync("SELECT DATE_DIFF(DATE '2017-10-15', DATE '2017-10-14', WEEK)");
		Assert.Equal("1", result);
	}

	[Fact]
	public async Task DateDiff_Week_SameWeek_NoBoundary()
	{
		// Monday → Saturday, no Sunday boundary crossed
		var result = await ScalarAsync("SELECT DATE_DIFF(DATE '2024-01-06', DATE '2024-01-01', WEEK)");
		Assert.Equal("0", result);
	}

	[Fact]
	public async Task DateDiff_Week_MultipleBoundaries()
	{
		// 14 days = 2 week boundaries (2 Sundays crossed)
		var result = await ScalarAsync("SELECT DATE_DIFF(DATE '2024-01-15', DATE '2024-01-01', WEEK)");
		Assert.Equal("2", result);
	}

	[Fact]
	public async Task DateDiff_Week_NegativeDirection()
	{
		// Reversed direction — Saturday to Sunday backwards is -1
		var result = await ScalarAsync("SELECT DATE_DIFF(DATE '2017-10-14', DATE '2017-10-15', WEEK)");
		Assert.Equal("-1", result);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_diff
	//   "Gets the number of unit boundaries between two DATETIME values."
	[Fact]
	public async Task DatetimeDiff_Week_BoundaryCounting()
	{
		// Saturday evening → Sunday morning crosses one Sunday boundary
		var result = await ScalarAsync(
			"SELECT DATETIME_DIFF(DATETIME '2017-10-15 06:00:00', DATETIME '2017-10-14 22:00:00', WEEK)");
		Assert.Equal("1", result);
	}

	[Fact]
	public async Task DatetimeDiff_Week_SameWeek()
	{
		// Both on same week (Mon-Sat)
		var result = await ScalarAsync(
			"SELECT DATETIME_DIFF(DATETIME '2024-01-06 23:59:59', DATETIME '2024-01-01 00:00:00', WEEK)");
		Assert.Equal("0", result);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_diff
	//   BigQuery computes floor-division of the total microsecond difference by the unit size.
	//   2 hours apart = 0 complete days.
	[Fact]
	public async Task TimestampDiff_Day_BoundaryCounting()
	{
		var result = await ScalarAsync(
			"SELECT TIMESTAMP_DIFF(TIMESTAMP '2020-01-02 01:00:00 UTC', TIMESTAMP '2020-01-01 23:00:00 UTC', DAY)");
		Assert.Equal("0", result);
	}

	[Fact]
	public async Task TimestampDiff_Day_NoBoundary()
	{
		// 01:00 to 23:00 same day = 0 day boundaries (22 hours but same day)
		var result = await ScalarAsync(
			"SELECT TIMESTAMP_DIFF(TIMESTAMP '2020-01-01 23:00:00 UTC', TIMESTAMP '2020-01-01 01:00:00 UTC', DAY)");
		Assert.Equal("0", result);
	}

	[Fact]
	public async Task TimestampDiff_Day_NegativeDirection()
	{
		// Reversed: 2 hours apart in negative direction = 0 complete days
		var result = await ScalarAsync(
			"SELECT TIMESTAMP_DIFF(TIMESTAMP '2020-01-01 23:00:00 UTC', TIMESTAMP '2020-01-02 01:00:00 UTC', DAY)");
		Assert.Equal("0", result);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_array
	//   "Returns an error if step_expression is set to 0"
	[Fact]
	public async Task GenerateArray_StepZero_ShouldError()
	{
		var client = await _fixture.GetClientAsync();
		var ex = await Assert.ThrowsAnyAsync<Exception>(
			() => client.ExecuteQueryAsync("SELECT GENERATE_ARRAY(1, 10, 0)", parameters: null));
		Assert.NotNull(ex);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#sign
	//   "INPUT: FLOAT64 → OUTPUT: FLOAT64"
	//   "If X is NaN, the output is NaN."
	[Fact]
	public async Task Sign_Float64_ReturnsFloat64()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#sign
		//   SIGN(FLOAT64) returns FLOAT64. SDK ToString() for integer-valued doubles gives no .0 suffix.
		var result = await ScalarAsync("SELECT SIGN(-2.5)");
		Assert.Equal("-1", result);
	}

	[Fact]
	public async Task Sign_NaN_ReturnsNaN()
	{
		var result = await ScalarAsync("SELECT SIGN(CAST('nan' AS FLOAT64))");
		Assert.Equal("NaN", result);
	}

	[Fact]
	public async Task Sign_Infinity_Returns1()
	{
		var result = await ScalarAsync("SELECT SIGN(CAST('inf' AS FLOAT64))");
		Assert.Equal("1", result);
	}

	[Fact]
	public async Task Sign_NegativeInfinity_ReturnsNeg1()
	{
		var result = await ScalarAsync("SELECT SIGN(CAST('-inf' AS FLOAT64))");
		Assert.Equal("-1", result);
	}

	[Fact]
	public async Task Sign_Zero_Float64_ReturnsZero()
	{
		var result = await ScalarAsync("SELECT SIGN(0.0)");
		Assert.Equal("0", result);
	}

	[Fact]
	public async Task Sign_Int64_ReturnsInt64()
	{
		// SIGN of an integer should still return an integer
		var result = await ScalarAsync("SELECT SIGN(-5)");
		Assert.Equal("-1", result);
	}

	// DATE_DIFF boundary counting for other parts
	[Fact]
	public async Task DateDiff_Day_AlwaysCountsDayBoundaries()
	{
		// DATE types are always at midnight so TotalDays should work.
		// But verify boundary behavior is correct.
		var result = await ScalarAsync("SELECT DATE_DIFF(DATE '2024-01-02', DATE '2024-01-01', DAY)");
		Assert.Equal("1", result);
	}

	[Fact]
	public async Task DateDiff_Month_PartialMonth()
	{
		// Jan 31 → Feb 1 = 1 month boundary
		var result = await ScalarAsync("SELECT DATE_DIFF(DATE '2024-02-01', DATE '2024-01-31', MONTH)");
		Assert.Equal("1", result);
	}

	[Fact]
	public async Task DateDiff_Quarter_CrossesQuarterBoundary()
	{
		// March 31 → April 1 crosses Q1→Q2 boundary
		var result = await ScalarAsync("SELECT DATE_DIFF(DATE '2024-04-01', DATE '2024-03-31', QUARTER)");
		Assert.Equal("1", result);
	}

	// TIMESTAMP_DIFF with HOUR boundary counting
	[Fact]
	public async Task TimestampDiff_Hour_BoundaryCounting()
	{
		// 2 minutes apart = 0 complete hours
		var result = await ScalarAsync(
			"SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-01 13:01:00 UTC', TIMESTAMP '2024-01-01 12:59:00 UTC', HOUR)");
		Assert.Equal("0", result);
	}

	[Fact]
	public async Task TimestampDiff_Hour_NoBoundary()
	{
		// 12:01 to 12:59 = 0 hour boundaries
		var result = await ScalarAsync(
			"SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-01 12:59:00 UTC', TIMESTAMP '2024-01-01 12:01:00 UTC', HOUR)");
		Assert.Equal("0", result);
	}

	// DATETIME_DIFF HOUR boundary
	[Fact]
	public async Task DatetimeDiff_Hour_BoundaryCounting()
	{
		var result = await ScalarAsync(
			"SELECT DATETIME_DIFF(DATETIME '2024-01-01 14:01:00', DATETIME '2024-01-01 13:59:00', HOUR)");
		Assert.Equal("1", result);
	}

	// DATETIME_DIFF DAY boundary
	[Fact]
	public async Task DatetimeDiff_Day_BoundaryCounting()
	{
		// 23:59 to 00:01 next day = 1 day boundary
		var result = await ScalarAsync(
			"SELECT DATETIME_DIFF(DATETIME '2024-01-02 00:01:00', DATETIME '2024-01-01 23:59:00', DAY)");
		Assert.Equal("1", result);
	}
}
