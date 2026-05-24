using Google;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Integration tests for bugs fixed in research round 24:
/// - || (concat) operator uses .ToString() instead of ConvertToString()
/// - FORMAT_TIME drops fractional seconds
/// </summary>
[Collection(IntegrationCollection.Name)]
public class Round24BugFixTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public Round24BugFixTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_r24_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// =====================================================
	// BUG 1: || (concat) operator uses .ToString() instead of ConvertToString()
	// Same bug as CONCAT function had (fixed in round 23) but the binary operator
	// was missed.
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#concatenation_operator
	//   "The || operator produces the same result as CONCAT."
	// =====================================================

	[Fact]
	public async Task ConcatOperator_BoolTrue_ProducesLowercase()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#concatenation_operator
		//   || implicitly casts to STRING, same as CONCAT
		var result = await S("SELECT TRUE || ' hello'");
		Assert.Equal("true hello", result);
	}

	[Fact]
	public async Task ConcatOperator_BoolFalse_ProducesLowercase()
	{
		var result = await S("SELECT FALSE || ' world'");
		Assert.Equal("false world", result);
	}

	[Fact]
	public async Task ConcatOperator_WholeFloat_ShowsDecimalPoint()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_string
		//   BigQuery CAST(FLOAT64 AS STRING) for whole numbers omits ".0" suffix
		var result = await S("SELECT CAST(1 AS FLOAT64) || ' hello'");
		Assert.Equal("1 hello", result);
	}

	[Fact]
	public async Task ConcatOperator_FractionalFloat_ShowsCorrectly()
	{
		var result = await S("SELECT CAST(1.5 AS FLOAT64) || ' hello'");
		Assert.Equal("1.5 hello", result);
	}

	[Fact]
	public async Task ConcatOperator_Date_ProducesIsoFormat()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#concatenation_operator
		//   || implicitly casts DATE to yyyy-MM-dd
		var result = await S("SELECT DATE '2024-01-01' || ' is today'");
		Assert.Equal("2024-01-01 is today", result);
	}

	[Fact]
	public async Task ConcatOperator_Datetime_ProducesCorrectFormat()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_string
		//   CAST(DATETIME AS STRING) → "YYYY-MM-DD HH:MM:SS"
		var result = await S("SELECT DATETIME '2024-01-01 12:30:00' || ' ok'");
		Assert.Equal("2024-01-01 12:30:00 ok", result);
	}

	[Fact]
	public async Task ConcatOperator_Timestamp_ProducesCorrectFormat()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_string
		//   CAST(TIMESTAMP AS STRING) → "YYYY-MM-DD HH:MM:SS+00"
		var result = await S("SELECT TIMESTAMP '2024-01-01 12:30:00 UTC' || ' ok'");
		Assert.Equal("2024-01-01 12:30:00+00 ok", result);
	}

	[Fact]
	public async Task ConcatOperator_Null_ReturnsNull()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators
		//   "Operands of || cannot be literal NULL"
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAsync<GoogleApiException>(
			() => client.ExecuteQueryAsync("SELECT NULL || 'hello'", parameters: null));
	}

	// =====================================================
	// BUG 2: FORMAT_TIME drops fractional seconds
	// EvaluateFormatTime creates DateTimeOffset without subsecond precision
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#format_time
	//   "Formats a TIME value according to the specified format string."
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements#format_elements_date_time
	//   "%E*S: Seconds with full fractional precision."
	// =====================================================

	[Fact]
	public async Task FormatTime_FractionalSeconds_EStar()
	{
		// TIME '12:30:45.123456' with %E*S should show "45.123456"
		var result = await S("SELECT FORMAT_TIME('%E*S', TIME '12:30:45.123456')");
		Assert.Equal("45.123456", result);
	}

	[Fact]
	public async Task FormatTime_FractionalSeconds_E3S()
	{
		// TIME '12:30:45.123456' with %E3S should show "45.123"
		var result = await S("SELECT FORMAT_TIME('%E3S', TIME '12:30:45.123456')");
		Assert.Equal("45.123", result);
	}

	[Fact]
	public async Task FormatTime_FractionalSeconds_Full()
	{
		// Full format with fractional seconds
		var result = await S("SELECT FORMAT_TIME('%H:%M:%E*S', TIME '12:30:45.123456')");
		Assert.Equal("12:30:45.123456", result);
	}

	[Fact]
	public async Task FormatTime_NoFraction_EStar_JustSeconds()
	{
		// Without fractions, %E*S should show just seconds
		var result = await S("SELECT FORMAT_TIME('%E*S', TIME '12:30:45')");
		Assert.Equal("45", result);
	}

	// =====================================================
	// BUG 3: TRIM/LTRIM/RTRIM with NULL characters returns string instead of NULL
	// The ?? "" pattern hides the NULL, making it trim nothing instead of returning NULL
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#trim
	//   "Returns NULL if value_to_trim or set_of_characters is NULL."
	// =====================================================

	[Fact]
	public async Task Trim_NullCharacters_ReturnsNull()
	{
		var result = await S("SELECT TRIM('hello', CAST(NULL AS STRING))");
		Assert.Null(result);
	}

	[Fact]
	public async Task Ltrim_NullCharacters_ReturnsNull()
	{
		var result = await S("SELECT LTRIM('hello', CAST(NULL AS STRING))");
		Assert.Null(result);
	}

	[Fact]
	public async Task Rtrim_NullCharacters_ReturnsNull()
	{
		var result = await S("SELECT RTRIM('hello', CAST(NULL AS STRING))");
		Assert.Null(result);
	}

	[Fact]
	public async Task Trim_NullValue_ReturnsNull()
	{
		// value is NULL
		var result = await S("SELECT TRIM(CAST(NULL AS STRING), 'x')");
		Assert.Null(result);
	}

	[Fact]
	public async Task Trim_BothArgs_Works()
	{
		// Normal case: trim specific chars
		var result = await S("SELECT TRIM('xxhelloxx', 'x')");
		Assert.Equal("hello", result);
	}

	// =====================================================
	// BUG 4: REGEXP_REPLACE with NULL replacement returns result instead of NULL
	// The ?? "" pattern converts NULL replacement to empty string
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_replace
	//   "Returns NULL if any argument is NULL."
	// =====================================================

	[Fact]
	public async Task RegexpReplace_NullReplacement_ReturnsNull()
	{
		var result = await S("SELECT REGEXP_REPLACE('hello', 'l', CAST(NULL AS STRING))");
		Assert.Null(result);
	}

	[Fact]
	public async Task RegexpReplace_NullStr_ReturnsNull()
	{
		var result = await S("SELECT REGEXP_REPLACE(CAST(NULL AS STRING), 'l', 'x')");
		Assert.Null(result);
	}

	[Fact]
	public async Task RegexpReplace_NullPattern_ReturnsNull()
	{
		var result = await S("SELECT REGEXP_REPLACE('hello', CAST(NULL AS STRING), 'x')");
		Assert.Null(result);
	}

	[Fact]
	public async Task RegexpReplace_AllNonNull_Works()
	{
		var result = await S("SELECT REGEXP_REPLACE('hello', 'l', 'x')");
		Assert.Equal("hexxo", result);
	}

	// =====================================================
	// BUG 5: EXTRACT(TIME FROM ...) returns string instead of TimeSpan
	// This causes wrong formatting (7 fractional digits instead of 6 max)
	// and breaks downstream TIME operations.
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#extract
	//   "EXTRACT returns an INT64 value except for DATE and TIME parts."
	// =====================================================

	[Fact]
	public async Task ExtractTime_FromDatetime_CorrectFormat()
	{
		// CAST(EXTRACT(TIME FROM ...) AS STRING) should format as HH:mm:ss
		var result = await S("SELECT CAST(EXTRACT(TIME FROM DATETIME '2024-01-01 12:30:45') AS STRING)");
		Assert.Equal("12:30:45", result);
	}

	[Fact]
	public async Task ExtractTime_FromTimestamp_CorrectFormat()
	{
		var result = await S("SELECT CAST(EXTRACT(TIME FROM TIMESTAMP '2024-01-01 12:30:45 UTC') AS STRING)");
		Assert.Equal("12:30:45", result);
	}

	[Fact]
	public async Task ExtractTime_FormatTime_Works()
	{
		// EXTRACT(TIME) should produce a value compatible with FORMAT_TIME
		var result = await S("SELECT FORMAT_TIME('%H:%M:%S', EXTRACT(TIME FROM DATETIME '2024-01-01 14:30:00'))");
		Assert.Equal("14:30:00", result);
	}

	// =====================================================
	// BUG 6: CAST(TIME AS STRING) drops fractional seconds
	// ConvertToString uses @"hh\:mm\:ss" format which has no fractional part
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_string
	//   "Casting from a time type to a string is of the form HH:MM:SS[.FFFFFF]."
	// =====================================================

	[Fact]
	public async Task CastTimeAsString_WithFractionalSeconds()
	{
		// TIME with fractional seconds should be preserved in CAST AS STRING
		var result = await S("SELECT CAST(TIME '12:30:45.123456' AS STRING)");
		Assert.Equal("12:30:45.123456", result);
	}

	[Fact]
	public async Task CastTimeAsString_NoFractionalSeconds()
	{
		// TIME without fractional seconds should not show decimal point
		var result = await S("SELECT CAST(TIME '12:30:45' AS STRING)");
		Assert.Equal("12:30:45", result);
	}

	[Fact]
	public async Task CastTimeAsString_PartialFractionalSeconds()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_string
		//   Trailing zeros in fractional seconds are trimmed.
		var result = await S("SELECT CAST(TIME '12:30:45.100000' AS STRING)");
		Assert.Equal("12:30:45.100", result);
	}
}
