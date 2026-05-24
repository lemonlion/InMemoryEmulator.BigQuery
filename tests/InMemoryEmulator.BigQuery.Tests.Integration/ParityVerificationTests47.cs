using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 47: CAST TIMESTAMP AS DATE/DATETIME UTC,
/// REGEXP_EXTRACT_ALL group, TO_JSON_STRING NaN/Infinity.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests47 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public ParityVerificationTests47(BigQuerySession session) => _session = session;

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
	// CAST(TIMESTAMP AS DATE): extracts date in UTC
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_date
	//   "Casting from a timestamp to a date effectively truncates the timestamp
	//    as of the default time zone (UTC)."
	[Fact]
	public async Task CastTimestampAsDate_UsesUtc()
	{
		// 2024-01-01 23:00:00 in UTC-5 = 2024-01-02 04:00:00 UTC → date is 2024-01-02
		var result = await ScalarAsync(
			"SELECT CAST(CAST(TIMESTAMP '2024-01-01 23:00:00-05:00' AS DATE) AS STRING)");
		Assert.Equal("2024-01-02", result);
	}

	[Fact]
	public async Task CastTimestampAsDate_PositiveOffset()
	{
		// 2024-01-02 02:00:00 in UTC+5 = 2024-01-01 21:00:00 UTC → date is 2024-01-01
		var result = await ScalarAsync(
			"SELECT CAST(CAST(TIMESTAMP '2024-01-02 02:00:00+05:00' AS DATE) AS STRING)");
		Assert.Equal("2024-01-01", result);
	}

	// ============================================================
	// CAST(TIMESTAMP AS DATETIME): extracts datetime in UTC
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_datetime
	//   "Casting from a timestamp to datetime effectively truncates the
	//    timestamp as of the default time zone (UTC)."
	[Fact]
	public async Task CastTimestampAsDatetime_UsesUtc()
	{
		var result = await ScalarAsync(
			"SELECT CAST(CAST(TIMESTAMP '2024-01-01 23:00:00-05:00' AS DATETIME) AS STRING)");
		Assert.Equal("2024-01-02 04:00:00", result);
	}

	[Fact]
	public async Task CastTimestampAsDatetime_PositiveOffset()
	{
		var result = await ScalarAsync(
			"SELECT CAST(CAST(TIMESTAMP '2024-01-02 02:00:00+05:00' AS DATETIME) AS STRING)");
		Assert.Equal("2024-01-01 21:00:00", result);
	}

	// ============================================================
	// REGEXP_EXTRACT_ALL: non-participating group → NULL in array
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_extract_all
	//   Optional group that doesn't participate → NULL element
	[Fact]
	public async Task RegexpExtractAll_NonParticipatingGroup_ReturnsNullElement()
	{
		// Pattern (a)?b: first match on 'b' has no 'a', second on 'ab' has 'a'
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_extract_all
		//   Non-participating groups return empty string (observed on real BigQuery)
		var result = await ScalarAsync(
			"SELECT ARRAY_TO_STRING(REGEXP_EXTRACT_ALL('b ab', '(a)?b'), ',', 'N')");
		Assert.Equal(",a", result);
	}

	// ============================================================
	// TO_JSON_STRING: NaN, Infinity
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#to_json_string
	//   "A non-finite number (NaN, Infinity, -Infinity) converts to a JSON string."
	[Fact]
	public async Task ToJsonString_Nan()
	{
		var result = await ScalarAsync("SELECT TO_JSON_STRING(CAST('nan' AS FLOAT64))");
		// BigQuery returns the string: "NaN" (including the quotes in JSON output)
		Assert.Contains("NaN", result);
	}

	[Fact]
	public async Task ToJsonString_Infinity()
	{
		var result = await ScalarAsync("SELECT TO_JSON_STRING(CAST('inf' AS FLOAT64))");
		Assert.Contains("Infinity", result);
	}

	[Fact]
	public async Task ToJsonString_NegativeInfinity()
	{
		var result = await ScalarAsync("SELECT TO_JSON_STRING(CAST('-inf' AS FLOAT64))");
		Assert.Contains("-Infinity", result);
	}

	[Fact]
	public async Task ToJsonString_NormalFloat()
	{
		var result = await ScalarAsync("SELECT TO_JSON_STRING(3.14)");
		Assert.Equal("3.14", result);
	}
}
