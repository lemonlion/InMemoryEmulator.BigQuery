using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 46: REGEXP_EXTRACT group, CAST DATETIME,
/// CAST hex INT64, PARSE_TIMESTAMP %E*S, GENERATE_DATE_ARRAY NULL step,
/// REGEXP_REPLACE literal $.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests46 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public ParityVerificationTests46(BigQuerySession session) => _session = session;

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
	// REGEXP_EXTRACT: non-participating capturing group → NULL
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_extract
	//   If the optional capturing group does not participate in the match, returns NULL.
	[Fact]
	public async Task RegexpExtract_NonParticipatingGroup_ReturnsNull()
	{
		var result = await ScalarAsync("SELECT REGEXP_EXTRACT('ab', '(z)?b')");
		Assert.Equal("NULL", result);
	}

	[Fact]
	public async Task RegexpExtract_ParticipatingGroup_ReturnsGroup()
	{
		var result = await ScalarAsync("SELECT REGEXP_EXTRACT('zb', '(z)?b')");
		Assert.Equal("z", result);
	}

	// ============================================================
	// CAST(DATETIME AS STRING): space separator, not 'T'
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_string
	//   "Casting from a datetime type to a string ... is of the form YYYY-MM-DD HH:MM:SS"
	[Fact]
	public async Task CastDatetimeAsString_UsesSpaceSeparator()
	{
		var result = await ScalarAsync("SELECT CAST(DATETIME '2023-01-15 12:30:00' AS STRING)");
		Assert.Equal("2023-01-15 12:30:00", result);
	}

	[Fact]
	public async Task CastDatetimeAsString_WithFractionalSeconds()
	{
		var result = await ScalarAsync("SELECT CAST(DATETIME '2023-01-15 12:30:45.123456' AS STRING)");
		Assert.Equal("2023-01-15 12:30:45.123456", result);
	}

	// ============================================================
	// CAST(STRING AS INT64): hex strings
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_int64
	//   "A hex string can be cast to an integer. For example, 0x123 to 291 or -0x123 to -291."
	[Fact]
	public async Task CastStringAsInt64_HexPositive()
	{
		var result = await ScalarAsync("SELECT CAST('0x1F' AS INT64)");
		Assert.Equal("31", result);
	}

	[Fact]
	public async Task CastStringAsInt64_HexNegative()
	{
		var result = await ScalarAsync("SELECT CAST('-0x1F' AS INT64)");
		Assert.Equal("-31", result);
	}

	// ============================================================
	// PARSE_TIMESTAMP with %E*S (fractional seconds)
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#parse_timestamp
	//   "%E*S represents seconds with full fractional precision"
	[Fact]
	public async Task ParseTimestamp_FractionalSeconds_EStar()
	{
		var result = await ScalarAsync(
			"SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', '2023-01-15 12:30:45.123456'))");
		Assert.Equal("2023-01-15 12:30:45.123456", result);
	}

	[Fact]
	public async Task ParseTimestamp_FractionalSeconds_E3S()
	{
		var result = await ScalarAsync(
			"SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E3S', PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3S', '2023-01-15 12:30:45.123'))");
		Assert.Equal("2023-01-15 12:30:45.123", result);
	}

	// ============================================================
	// GENERATE_DATE_ARRAY: NULL step returns NULL
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_date_array
	//   BigQuery: "Invalid cast from ARRAY<DATE> to STRING"
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task GenerateDateArray_NullStep_ReturnsNull()
	{
		var result = await ScalarAsync(
			"SELECT CAST(GENERATE_DATE_ARRAY('2023-01-01', '2023-01-10', INTERVAL CAST(NULL AS INT64) DAY) AS STRING)");
		Assert.Equal("NULL", result);
	}

	// ============================================================
	// REGEXP_REPLACE: literal $ in replacement is NOT a backreference
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_replace
	//   BigQuery uses \1 for backrefs, not $1. Literal $ should stay literal.
	[Fact]
	public async Task RegexpReplace_LiteralDollarSign_NotBackRef()
	{
		var result = await ScalarAsync(@"SELECT REGEXP_REPLACE('abc', '(b)', '$1')");
		Assert.Equal("a$1c", result);
	}

	[Fact]
	public async Task RegexpReplace_BackslashBackRef_Works()
	{
		var result = await ScalarAsync(@"SELECT REGEXP_REPLACE('abc', '(b)', '\\1x')");
		Assert.Equal("abxc", result);
	}
}
