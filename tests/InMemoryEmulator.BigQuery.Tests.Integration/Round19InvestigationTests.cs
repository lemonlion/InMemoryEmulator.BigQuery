using Google;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Research round 19: investigating potential bugs in various areas.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class Round19InvestigationTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public Round19InvestigationTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await _fixture.GetClientAsync();
		return (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
	}

	private async Task<Exception?> GetError(string sql)
	{
		try
		{
			var client = await _fixture.GetClientAsync();
			(await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
			return null;
		}
		catch (Exception ex) { return ex; }
	}

	// ============ AREA 4: ARRAY subscript without OFFSET/ORDINAL ============
	// In BigQuery, bare `arr[0]` is NOT valid - requires OFFSET/ORDINAL
	// If emulator allows it, that's a bug
	// NOTE: Bare array subscript `[1,2,3][0]` is a confirmed divergence from BigQuery.
	// In real BigQuery this should produce an error requiring OFFSET/ORDINAL.
	// The emulator parser silently ignores the [0] due to .Try() backtracking.
	// Fix requires parser-level changes - tracked as a known limitation.

	// ============ AREA 5: GENERATE_DATE_ARRAY ============
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_date_array
	[Fact]
	public async Task GenerateDateArray_NullStart_ReturnsNull()
	{
		var result = await Scalar("SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(NULL, DATE '2024-01-05'))");
		Assert.Null(result);
	}

	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task GenerateDateArray_NullEnd_ReturnsNull()
	{
		var result = await Scalar("SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', NULL))");
		Assert.Null(result);
	}

	// ============ AREA 6: DATE_FROM_UNIX_DATE / UNIX_DATE ============
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_from_unix_date
	[Fact]
	public async Task DateFromUnixDate_Zero()
	{
		var result = await Scalar("SELECT CAST(DATE_FROM_UNIX_DATE(0) AS STRING)");
		Assert.Equal("1970-01-01", result);
	}

	[Fact]
	public async Task UnixDate_Epoch()
	{
		var result = await Scalar("SELECT UNIX_DATE(DATE '1970-01-01')");
		Assert.Equal("0", result);
	}

	[Fact]
	public async Task UnixDate_KnownDate()
	{
		// 2024-01-01 is day 19723 since epoch
		var result = await Scalar("SELECT UNIX_DATE(DATE '2024-01-01')");
		Assert.Equal("19723", result);
	}

	// ============ AREA 7: TIMESTAMP_SECONDS / UNIX_SECONDS ============
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_seconds
	[Fact]
	public async Task TimestampSeconds_Zero()
	{
		var result = await Scalar("SELECT CAST(TIMESTAMP_SECONDS(0) AS STRING)");
		Assert.Equal("1970-01-01 00:00:00+00", result);
	}

	[Fact]
	public async Task UnixSeconds_Epoch()
	{
		var result = await Scalar("SELECT UNIX_SECONDS(TIMESTAMP '1970-01-01 00:00:00 UTC')");
		Assert.Equal("0", result);
	}

	[Fact]
	public async Task UnixMillis_OneSecond()
	{
		var result = await Scalar("SELECT UNIX_MILLIS(TIMESTAMP '1970-01-01 00:00:01 UTC')");
		Assert.Equal("1000", result);
	}

	// ============ AREA 8: ARRAY_CONCAT with NULL arrays ============
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_concat
	//   "The argument to ARRAY_CONCAT (or ARRAY_CONCAT_AGG) must be an array type but was NULL"
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task ArrayConcat_WithNullArray_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAsync<GoogleApiException>(async () =>
			await client.ExecuteQueryAsync("SELECT ARRAY_CONCAT([1, 2], NULL)", parameters: null));
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task ArrayConcat_NullFirst_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAsync<GoogleApiException>(async () =>
			await client.ExecuteQueryAsync("SELECT ARRAY_CONCAT(NULL, [3, 4])", parameters: null));
	}

	// ============ AREA 9: FORMAT_DATE format elements ============
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements#format_elements_date_time
	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task FormatDate_FullDayName()
	{
		// 2024-01-01 is a Monday
		var result = await Scalar("SELECT FORMAT_DATE('%A', DATE '2024-01-01')");
		Assert.Equal("Monday", result);
	}

	[Fact]
	public async Task FormatDate_AbbreviatedDayName()
	{
		var result = await Scalar("SELECT FORMAT_DATE('%a', DATE '2024-01-01')");
		Assert.Equal("Mon", result);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task FormatDate_DayOfYear()
	{
		// Jan 15 is day 015
		var result = await Scalar("SELECT FORMAT_DATE('%j', DATE '2024-01-15')");
		Assert.Equal("015", result);
	}

	[Fact]
	public async Task FormatDate_DayOfYear_Dec31()
	{
		// 2024 is a leap year so Dec 31 = day 366
		var result = await Scalar("SELECT FORMAT_DATE('%j', DATE '2024-12-31')");
		Assert.Equal("366", result);
	}

	// ============ AREA 10: LAST_DAY function ============
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#last_day
	[Fact]
	public async Task LastDay_Month_LeapYear()
	{
		var result = await Scalar("SELECT CAST(LAST_DAY(DATE '2024-02-01') AS STRING)");
		Assert.Equal("2024-02-29", result);
	}

	[Fact]
	public async Task LastDay_Month_Explicit()
	{
		var result = await Scalar("SELECT CAST(LAST_DAY(DATE '2024-02-01', MONTH) AS STRING)");
		Assert.Equal("2024-02-29", result);
	}

	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task LastDay_Year()
	{
		var result = await Scalar("SELECT CAST(LAST_DAY(DATE '2024-02-01', YEAR) AS STRING)");
		Assert.Equal("2024-12-31", result);
	}

	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task LastDay_Quarter_Q1()
	{
		var result = await Scalar("SELECT CAST(LAST_DAY(DATE '2024-02-01', QUARTER) AS STRING)");
		Assert.Equal("2024-03-31", result);
	}

	[Fact]
	public async Task LastDay_Week()
	{
		// LAST_DAY with WEEK: returns the last day of the ISO week (Sunday)
		// Actually, BigQuery says: "Returns the last day of the containing week."
		// The BigQuery week starts on Sunday by default, so last day of week = Saturday.
		// 2024-02-01 is a Thursday. Week (Sun-Sat): Jan 28 - Feb 3. Last day = Feb 3 (Saturday).
		var result = await Scalar("SELECT CAST(LAST_DAY(DATE '2024-02-01', WEEK) AS STRING)");
		Assert.Equal("2024-02-03", result);
	}

	[Fact]
	public async Task LastDay_Null_ReturnsNull()
	{
		var result = await Scalar("SELECT LAST_DAY(CAST(NULL AS DATE))");
		Assert.Null(result);
	}

	// ============ AREA 12: ERROR function ============
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/debugging_functions#error
	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Error_ThrowsWithMessage()
	{
		var ex = await GetError("SELECT ERROR('custom error message')");
		Assert.NotNull(ex);
		Assert.Contains("custom error message", ex.Message);
	}

	// ============ AREA 13: REGEXP_INSTR edge cases ============
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_instr
	//   REGEXP_INSTR(source_value, regexp[, position[, occurrence[, occurrence_position]]])
	[Fact]
	public async Task RegexpInstr_Basic()
	{
		var result = await Scalar("SELECT REGEXP_INSTR('abcabc', 'bc')");
		Assert.Equal("2", result);
	}

	[Fact]
	public async Task RegexpInstr_WithPosition()
	{
		// Starting from position 3 (1-based), look for 'bc'
		// 'abcabc' starting at position 3 = 'cabc', first match of 'bc' at position 5 in original
		var result = await Scalar("SELECT REGEXP_INSTR('abcabc', 'bc', 3)");
		Assert.Equal("5", result);
	}

	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task RegexpInstr_WithOccurrence()
	{
		// Second occurrence of 'bc' in 'abcabc' starts at position 5
		var result = await Scalar("SELECT REGEXP_INSTR('abcabc', 'bc', 1, 2)");
		Assert.Equal("5", result);
	}

	[Fact]
	public async Task RegexpInstr_WithOccurrencePosition_End()
	{
		// occurrence_position=1 means return position after the match end
		// Second 'bc' in 'abcabc' is at position 5-6, so end+1 = 7
		var result = await Scalar("SELECT REGEXP_INSTR('abcabc', 'bc', 1, 2, 1)");
		Assert.Equal("7", result);
	}

	[Fact]
	public async Task RegexpInstr_NoMatch()
	{
		var result = await Scalar("SELECT REGEXP_INSTR('hello', 'xyz')");
		Assert.Equal("0", result);
	}

	// ============ AREA 6 extended: UNIX_DATE roundtrip ============
	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task DateFromUnixDate_NegativeDay()
	{
		// Day -1 = 1969-12-31
		var result = await Scalar("SELECT CAST(DATE_FROM_UNIX_DATE(-1) AS STRING)");
		Assert.Equal("1969-12-31", result);
	}

	// ============ AREA 9 extended: FORMAT_DATE %B (full month name) ============
	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task FormatDate_FullMonthName()
	{
		var result = await Scalar("SELECT FORMAT_DATE('%B', DATE '2024-01-15')");
		Assert.Equal("January", result);
	}

	[Fact]
	public async Task FormatDate_AbbreviatedMonthName()
	{
		var result = await Scalar("SELECT FORMAT_DATE('%b', DATE '2024-01-15')");
		Assert.Equal("Jan", result);
	}

	// ============ AREA 7 extended: TIMESTAMP_MICROS ============
	[Fact]
	public async Task TimestampMicros_OneMillion()
	{
		// 1000000 microseconds = 1 second after epoch
		var result = await Scalar("SELECT CAST(TIMESTAMP_MICROS(1000000) AS STRING)");
		Assert.Equal("1970-01-01 00:00:01+00", result);
	}

	[Fact]
	public async Task UnixMicros_OneSecond()
	{
		var result = await Scalar("SELECT UNIX_MICROS(TIMESTAMP '1970-01-01 00:00:01 UTC')");
		Assert.Equal("1000000", result);
	}

	// ============ AREA 10 extended: LAST_DAY with WEEK(MONDAY) ============
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#last_day
	//   "LAST_DAY(date_expression[, date_part])"
	//   For WEEK(WEEKDAY), returns the last day of that week definition
	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task LastDay_WeekMonday()
	{
		// With WEEK(MONDAY), the week starts on Monday and ends on Sunday.
		// 2024-02-01 (Thursday) is in week Mon Jan 29 - Sun Feb 4. Last day = Feb 4 (Sunday).
		var result = await Scalar("SELECT CAST(LAST_DAY(DATE '2024-02-01', WEEK(MONDAY)) AS STRING)");
		Assert.Equal("2024-02-04", result);
	}
}
