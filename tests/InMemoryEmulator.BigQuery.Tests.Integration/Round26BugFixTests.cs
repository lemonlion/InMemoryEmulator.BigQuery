using Google;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Integration tests for bugs fixed in research round 26:
/// - CONTAINS_SUBSTR with NULL search string should return NULL (not True)
/// </summary>
[Collection(IntegrationCollection.Name)]
public class Round26BugFixTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public Round26BugFixTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_r26_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
	}

	/// <summary>
	/// CONTAINS_SUBSTR with NULL second arg (search string) should error.
	/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#contains_substr
	///   "The second argument of CONTAINS_SUBSTR() must not be null."
	/// </summary>
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task ContainsSubstr_NullSearchString_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAsync<GoogleApiException>(async () =>
			await client.ExecuteQueryAsync(
				"SELECT CONTAINS_SUBSTR('hello world', CAST(NULL AS STRING)) AS result",
				parameters: null));
	}

	/// <summary>
	/// CONTAINS_SUBSTR with both args NULL should error (second arg is NULL).
	/// </summary>
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task ContainsSubstr_BothNull_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAsync<GoogleApiException>(async () =>
			await client.ExecuteQueryAsync(
				"SELECT CONTAINS_SUBSTR(CAST(NULL AS STRING), CAST(NULL AS STRING)) AS result",
				parameters: null));
	}

	/// <summary>
	/// JSON_SET with NULL path should ignore the path operation and return original JSON.
	/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_set
	///   "If json_path is SQL NULL, the json_path_value_pair operation is ignored."
	/// BigQuery rejects NULL path: "Argument 1: Unable to coerce type STRING to expected type JSON"
	/// </summary>
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task JsonSet_NullPath_ReturnsOriginalJson()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT JSON_SET(JSON '{\"a\": 1}', CAST(NULL AS STRING), 99) AS result",
			parameters: null);
		var row = result.Single();
		// The operation is ignored, so original JSON is returned
		Assert.Contains("\"a\"", row["result"]?.ToString());
	}

	/// <summary>
	/// DATE(NULL) should return NULL, not throw.
	/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date
	///   "Returns NULL if any input is NULL."
	/// </summary>
	[Fact]
	public async Task DateConstructor_NullArg_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT DATE(CAST(NULL AS TIMESTAMP)) AS result",
			parameters: null);
		var row = result.Single();
		Assert.Null(row["result"]);
	}

	/// <summary>
	/// DATETIME(NULL) should return NULL, not throw.
	/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime
	///   "Returns NULL if any input is NULL."
	/// </summary>
	[Fact]
	public async Task DateTimeConstructor_NullArg_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT DATETIME(CAST(NULL AS TIMESTAMP)) AS result",
			parameters: null);
		var row = result.Single();
		Assert.Null(row["result"]);
	}

	/// <summary>
	/// DATETIME(date, NULL) should return NULL when time is NULL.
	/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime
	///   "Returns NULL if any input is NULL."
	/// </summary>
	[Fact]
	public async Task DateTimeConstructor_NullTime_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT DATETIME(DATE '2024-01-01', CAST(NULL AS TIME)) AS result",
			parameters: null);
		var row = result.Single();
		Assert.Null(row["result"]);
	}

	/// <summary>
	/// TIMESTAMP(NULL) should return NULL, not throw.
	/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp
	///   "Returns NULL if any input is NULL."
	/// </summary>
	[Fact]
	public async Task TimestampConstructor_NullArg_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT TIMESTAMP(CAST(NULL AS STRING)) AS result",
			parameters: null);
		var row = result.Single();
		Assert.Null(row["result"]);
	}

	/// <summary>
	/// TIME(NULL) should return NULL, not default to 00:00:00.
	/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time
	///   "Returns NULL if any input is NULL."
	/// </summary>
	[Fact]
	public async Task TimeConstructor_NullArg_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT TIME(CAST(NULL AS TIMESTAMP)) AS result",
			parameters: null);
		var row = result.Single();
		Assert.Null(row["result"]);
	}
}
