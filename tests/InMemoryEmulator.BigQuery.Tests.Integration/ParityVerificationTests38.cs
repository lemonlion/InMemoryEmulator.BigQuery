using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 38: FORMAT_TIMESTAMP %E*S specifiers,
/// LPAD/RPAD edge cases, SPLIT NULL delimiter, REGEXP_EXTRACT with position/occurrence,
/// and additional boundary/edge case coverage.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests38 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public ParityVerificationTests38(BigQuerySession session) => _session = session;

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

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#format_timestamp
	//   "%E*S" — "Seconds with full fractional precision (a literal '*')"
	//   e.g. "00.123456"
	[Fact]
	public async Task FormatTimestamp_E_Star_S_FullPrecision()
	{
		var result = await ScalarAsync(
			"SELECT FORMAT_TIMESTAMP('%E*S', TIMESTAMP '2024-01-15 10:30:45.123456 UTC')");
		Assert.Equal("45.123456", result);
	}

	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task FormatTimestamp_E3S_ThreeDigits()
	{
		// %E3S = seconds with 3 fractional digits
		var result = await ScalarAsync(
			"SELECT FORMAT_TIMESTAMP('%E3S', TIMESTAMP '2024-01-15 10:30:45.123456 UTC')");
		Assert.Equal("45.123", result);
	}

	[Fact]
	public async Task FormatTimestamp_E6S_SixDigits()
	{
		// %E6S = seconds with 6 fractional digits
		var result = await ScalarAsync(
			"SELECT FORMAT_TIMESTAMP('%E6S', TIMESTAMP '2024-01-15 10:30:45.123456 UTC')");
		Assert.Equal("45.123456", result);
	}

	[Fact]
	public async Task FormatTimestamp_FullFormat_WithEStarS()
	{
		var result = await ScalarAsync(
			"SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', TIMESTAMP '2024-01-15 10:30:45.123456 UTC')");
		Assert.Equal("2024-01-15 10:30:45.123456", result);
	}

	[Fact]
	public async Task FormatTimestamp_E_Star_S_ZeroFraction()
	{
		// When fractional seconds are 0, still shows the decimal
		var result = await ScalarAsync(
			"SELECT FORMAT_TIMESTAMP('%E*S', TIMESTAMP '2024-01-15 10:30:45 UTC')");
		// BigQuery returns "45" or "45.000000" — depends on interpretation.
		// The docs say "full fractional precision". With 0 microseconds, BQ shows "45".
		Assert.Equal("45", result);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#lpad
	//   "If return_length is less than or equal to the original_value length, this function
	//    truncates the original_value to the return_length value."
	//   "This function returns an error if: return_length is negative; pattern is empty."
	[Fact]
	public async Task Lpad_Truncation()
	{
		// When return_length < string length, truncate
		var result = await ScalarAsync("SELECT LPAD('hello', 3, 'x')");
		Assert.Equal("hel", result);
	}

	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Rpad_Truncation()
	{
		var result = await ScalarAsync("SELECT RPAD('hello', 3, 'x')");
		Assert.Equal("hel", result);
	}

	[Fact]
	public async Task Lpad_EmptyPattern_ShouldError()
	{
		var client = await _fixture.GetClientAsync();
		var ex = await Assert.ThrowsAnyAsync<Exception>(
			() => client.ExecuteQueryAsync("SELECT LPAD('hello', 10, '')", parameters: null));
		Assert.NotNull(ex);
	}

	[Fact]
	public async Task Rpad_EmptyPattern_ShouldError()
	{
		var client = await _fixture.GetClientAsync();
		var ex = await Assert.ThrowsAnyAsync<Exception>(
			() => client.ExecuteQueryAsync("SELECT RPAD('hello', 10, '')", parameters: null));
		Assert.NotNull(ex);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#split
	//   BigQuery SDK represents NULL array as System.String[] in BigQueryRow.
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Split_NullDelimiter_ReturnsNull()
	{
		var result = await ScalarAsync("SELECT SPLIT('a,b,c', CAST(NULL AS STRING))");
		Assert.Equal("NULL", result);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_extract
	//   REGEXP_EXTRACT(value, regexp[, position[, occurrence]])
	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task RegexpExtract_WithPosition()
	{
		// Start search from position 4 (1-indexed)
		var result = await ScalarAsync("SELECT REGEXP_EXTRACT('abcabc', 'a(b)c', 1, 2)");
		Assert.Equal("b", result);
	}

	[Fact]
	public async Task RegexpExtract_Position_NoMatch()
	{
		// Position past where the pattern can match
		var result = await ScalarAsync("SELECT REGEXP_EXTRACT('abc', 'a', 4)");
		Assert.Equal("NULL", result);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#safe_divide
	//   "Equivalent to the division operator (X / Y), but returns NULL if an error occurs"
	[Fact]
	public async Task SafeDivide_ByZero_ReturnsNull()
	{
		var result = await ScalarAsync("SELECT SAFE_DIVIDE(10, 0)");
		Assert.Equal("NULL", result);
	}

	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task SafeDivide_Normal()
	{
		var result = await ScalarAsync("SELECT SAFE_DIVIDE(10, 4)");
		Assert.Equal("2.5", result);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#ieee_divide
	//   "Division of X by Y, never raises an error."
	//   "Returns FLOAT64. X / 0 = inf or -inf or NaN"
	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task IeeeDivide_ByZero_ReturnsInfinity()
	{
		var result = await ScalarAsync("SELECT CAST(IEEE_DIVIDE(1.0, 0.0) AS STRING)");
		Assert.Equal("inf", result);
	}

	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task IeeeDivide_NegativeByZero_ReturnsNegInf()
	{
		var result = await ScalarAsync("SELECT CAST(IEEE_DIVIDE(-1.0, 0.0) AS STRING)");
		Assert.Equal("-inf", result);
	}

	[Fact]
	public async Task IeeeDivide_ZeroByZero_ReturnsNaN()
	{
		var result = await ScalarAsync("SELECT IEEE_DIVIDE(0.0, 0.0)");
		Assert.Equal("NaN", result);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#is_nan
	// GO emulator bug: cannot parse inf/nan string literals to FLOAT64.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task IsNan_True()
	{
		var result = await ScalarAsync("SELECT IS_NAN(CAST('nan' AS FLOAT64))");
		Assert.Equal("True", result);
	}

	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task IsNan_False()
	{
		var result = await ScalarAsync("SELECT IS_NAN(1.5)");
		Assert.Equal("False", result);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#is_inf
	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task IsInf_PositiveInfinity()
	{
		var result = await ScalarAsync("SELECT IS_INF(CAST('inf' AS FLOAT64))");
		Assert.Equal("True", result);
	}

	// GO emulator bug: cannot parse inf/nan string literals to FLOAT64.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task IsInf_NegativeInfinity()
	{
		var result = await ScalarAsync("SELECT IS_INF(CAST('-inf' AS FLOAT64))");
		Assert.Equal("True", result);
	}

	[Fact]
	public async Task IsInf_Normal()
	{
		var result = await ScalarAsync("SELECT IS_INF(1.5)");
		Assert.Equal("False", result);
	}

	// GENERATE_ARRAY with negative step
	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task GenerateArray_NegativeStep()
	{
		var result = await ScalarAsync("SELECT ARRAY_TO_STRING(ARRAY(SELECT CAST(x AS STRING) FROM UNNEST(GENERATE_ARRAY(5, 1, -1)) AS x), ',')");
		Assert.Equal("5,4,3,2,1", result);
	}

	// GENERATE_ARRAY float step=0 should also error
	[Fact]
	public async Task GenerateArray_FloatStepZero_ShouldError()
	{
		var client = await _fixture.GetClientAsync();
		var ex = await Assert.ThrowsAnyAsync<Exception>(
			() => client.ExecuteQueryAsync("SELECT GENERATE_ARRAY(1.0, 10.0, 0.0)", parameters: null));
		Assert.NotNull(ex);
	}
}
