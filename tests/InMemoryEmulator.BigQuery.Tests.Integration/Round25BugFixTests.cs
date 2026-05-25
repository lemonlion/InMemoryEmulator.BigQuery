using Google;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Integration tests for bugs fixed in research round 25:
/// - STRING_AGG with NULL delimiter should return NULL
/// - FORMAT with NULL non-%t/%T args should return NULL (entire result)
/// - CONTAINS_SUBSTR with NULL expression should return NULL
/// </summary>
[Collection(IntegrationCollection.Name)]
public class Round25BugFixTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public Round25BugFixTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_r25_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
	}

	/// <summary>
	/// STRING_AGG with NULL delimiter should throw an error.
	/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#string_agg
	///   "Argument 2 to STRING_AGG must be non-NULL"
	/// </summary>
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task StringAgg_NullDelimiter_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAsync<GoogleApiException>(async () =>
			await client.ExecuteQueryAsync(
				"SELECT STRING_AGG(x, CAST(NULL AS STRING)) AS agg FROM UNNEST(['a', 'b', 'c']) AS x",
				parameters: null));
	}

	/// <summary>
	/// FORMAT('%d', NULL) should return NULL per docs:
	/// "The function generally produces a NULL value if a NULL argument is present."
	/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#format_string
	/// </summary>
	[Fact]
	public async Task Format_NullArg_NonT_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT FORMAT('%d', CAST(NULL AS INT64)) AS fmt",
			parameters: null);
		var row = result.Single();
		Assert.Null(row["fmt"]);
	}

	/// <summary>
	/// FORMAT('%s', NULL) should return NULL per docs.
	/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#format_string
	/// </summary>
	[Fact]
	public async Task Format_NullStringArg_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT FORMAT('%s', CAST(NULL AS STRING)) AS fmt",
			parameters: null);
		var row = result.Single();
		Assert.Null(row["fmt"]);
	}

	/// <summary>
	/// FORMAT('%t', NULL) should return literal 'NULL' text per docs:
	/// "if the format specifier is %t or %T, a NULL value produces 'NULL' (without quotes)"
	/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#format_string
	/// </summary>
	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Format_NullArg_T_ReturnsLiteralNull()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT FORMAT('%t', CAST(NULL AS INT64)) AS fmt",
			parameters: null);
		var row = result.Single();
		Assert.Equal("NULL", (string)row["fmt"]);
	}

	/// <summary>
	/// FORMAT with mix of %t and non-%t: if a non-%t arg is NULL, entire result is NULL.
	/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#format_string
	/// </summary>
	[Fact]
	public async Task Format_MixedArgs_NullNonT_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT FORMAT('%d %t', CAST(NULL AS INT64), 42) AS fmt",
			parameters: null);
		var row = result.Single();
		Assert.Null(row["fmt"]);
	}

	/// <summary>
	/// CONTAINS_SUBSTR with NULL first arg (expression) should return NULL.
	/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#contains_substr
	///   "If the expression is NULL, the return value is NULL."
	/// </summary>
	// GO emulator limitation: CONTAINS_SUBSTR function not implemented in goccy/bigquery-emulator.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task ContainsSubstr_NullExpression_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT CONTAINS_SUBSTR(CAST(NULL AS STRING), 'abc') AS result",
			parameters: null);
		var row = result.Single();
		Assert.Null(row["result"]);
	}

	/// <summary>
	/// String comparison operators with NULL should return NULL, not FALSE.
	/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#comparison_operators
	///   "Returns NULL when a or b is NULL."
	/// </summary>
	[Fact]
	public async Task StringComparison_WithNull_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT ('abc' < CAST(NULL AS STRING)) AS r1, (CAST(NULL AS STRING) < CAST(NULL AS STRING)) AS r2, (CAST(NULL AS STRING) = CAST(NULL AS STRING)) AS r3",
			parameters: null);
		var row = result.Single();
		Assert.Null(row["r1"]);
		Assert.Null(row["r2"]);
		Assert.Null(row["r3"]);
	}

	/// <summary>
	/// CAST(DATETIME AS STRING) should trim trailing zeros in fractional seconds.
	/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_string
	///   Format: "YYYY-MM-DD HH:MM:SS[.DDDDDD]" with trailing zeros trimmed.
	/// </summary>
	[Fact]
	public async Task CastDatetimeAsString_TrimsTrailingZeros()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT CAST(DATETIME '2024-01-01 00:00:00.123000' AS STRING) AS dt",
			parameters: null);
		var row = result.Single();
		Assert.Equal("2024-01-01 00:00:00.123", (string)row["dt"]);
	}

	/// <summary>
	/// CAST(DATETIME AS STRING) with all 6 significant fractional digits.
	/// </summary>
	[Fact]
	public async Task CastDatetimeAsString_FullFraction()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT CAST(DATETIME '2024-01-01 12:34:56.123456' AS STRING) AS dt",
			parameters: null);
		var row = result.Single();
		Assert.Equal("2024-01-01 12:34:56.123456", (string)row["dt"]);
	}

	/// <summary>
	/// CAST(TIME AS STRING) should trim trailing zeros in fractional seconds.
	/// </summary>
	[Fact]
	public async Task CastTimeAsString_TrimsTrailingZeros()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT CAST(TIME '12:34:56.100000' AS STRING) AS t",
			parameters: null);
		var row = result.Single();
		Assert.Equal("12:34:56.100", (string)row["t"]);
	}
}
