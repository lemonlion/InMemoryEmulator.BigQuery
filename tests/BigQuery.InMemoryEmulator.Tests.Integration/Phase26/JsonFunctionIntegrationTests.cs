using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration.Phase26;

/// <summary>
/// Integration tests for Phase 26: JSON functions.
/// PARSE_JSON, TO_JSON, JSON_ARRAY, JSON_OBJECT, JSON_REMOVE.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class JsonFunctionIntegrationTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public JsonFunctionIntegrationTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
	}

	public async ValueTask DisposeAsync()
	{
		await _fixture.DisposeAsync();
	}

	#region PARSE_JSON

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#parse_json
	//   "Takes a JSON-formatted string and returns a JSON value."
	[Fact]
	public async Task ParseJson_Object()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT PARSE_JSON('{\"a\": 1, \"b\": \"hello\"}') AS result", parameters: null);
		var result = results.ToList()[0]["result"]?.ToString();
		Assert.Contains("a", result);
		Assert.Contains("1", result);
	}

	[Fact]
	public async Task ParseJson_Array()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT PARSE_JSON('[1, 2, 3]') AS result", parameters: null);
		Assert.NotNull(results.ToList()[0]["result"]);
	}

	#endregion

	#region TO_JSON

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#to_json
	//   "Takes a SQL value and returns a JSON value."
	[Fact]
	public async Task ToJson_Integer()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT TO_JSON(42) AS result", parameters: null);
		Assert.Equal("42", results.ToList()[0]["result"]?.ToString());
	}

	[Fact]
	public async Task ToJson_Null()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT TO_JSON(NULL) AS result", parameters: null);
		var result = results.ToList()[0]["result"]?.ToString();
		Assert.Equal("null", result);
	}

	#endregion

	#region JSON_ARRAY

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_array
	//   "Creates a JSON array."
	[Fact]
	public async Task JsonArray_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT JSON_ARRAY(1, 2, 3) AS result", parameters: null);
		Assert.Equal("[1,2,3]", results.ToList()[0]["result"]?.ToString());
	}

	[Fact]
	public async Task JsonArray_Empty()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT JSON_ARRAY() AS result", parameters: null);
		Assert.Equal("[]", results.ToList()[0]["result"]?.ToString());
	}

	#endregion

	#region JSON_OBJECT

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_object
	//   "Creates a JSON object."
	[Fact]
	public async Task JsonObject_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT JSON_OBJECT('a', 1, 'b', 2) AS result", parameters: null);
		var result = results.ToList()[0]["result"]?.ToString();
		Assert.Contains("\"a\"", result);
		Assert.Contains("1", result);
	}

	[Fact]
	public async Task JsonObject_Empty()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT JSON_OBJECT() AS result", parameters: null);
		Assert.Equal("{}", results.ToList()[0]["result"]?.ToString());
	}

	#endregion

	#region JSON_REMOVE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_remove
	//   "Removes a JSON element at a path."
	[Fact]
	public async Task JsonRemove_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT JSON_REMOVE('{\"a\": 1, \"b\": 2}', '$.a') AS result", parameters: null);
		var result = results.ToList()[0]["result"]?.ToString();
		Assert.DoesNotContain("\"a\"", result);
		Assert.Contains("\"b\"", result);
	}

	#endregion
}
