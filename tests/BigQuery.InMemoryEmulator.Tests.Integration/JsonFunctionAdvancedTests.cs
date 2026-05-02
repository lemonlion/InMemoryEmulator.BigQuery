using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for JSON functions: JSON_KEYS, JSON_SET, JSON_STRIP_NULLS,
/// JSON_TYPE, JSON_OBJECT, JSON_ARRAY, LAX conversions, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class JsonFunctionAdvancedTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public JsonFunctionAdvancedTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_json_{Guid.NewGuid():N}"[..30];
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

	// ---- JSON_KEYS ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_keys
	[Fact] public async Task JsonKeys_TopLevel()
	{
		var v = await S("SELECT ARRAY_LENGTH(JSON_KEYS(JSON '{\"a\":1,\"b\":2,\"c\":3}'))");
		Assert.Equal("3", v);
	}
	[Fact] public async Task JsonKeys_Nested()
	{
		// JSON_KEYS returns the top-level keys of the JSON object
		var v = await S("SELECT ARRAY_LENGTH(JSON_KEYS(JSON '{\"a\":{\"x\":1},\"b\":2}'))");
		Assert.Equal("2", v);
	}
	[Fact] public async Task JsonKeys_Null() => Assert.Null(await S("SELECT JSON_KEYS(NULL)"));
	[Fact] public async Task JsonKeys_EmptyObject()
	{
		var v = await S("SELECT ARRAY_LENGTH(JSON_KEYS(JSON '{}'))");
		Assert.Equal("0", v);
	}

	// ---- JSON_TYPE ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_type
	[Fact] public async Task JsonType_Object() => Assert.Equal("object", await S("SELECT JSON_TYPE(JSON '{\"a\":1}')"));
	[Fact] public async Task JsonType_Array() => Assert.Equal("array", await S("SELECT JSON_TYPE(JSON '[1,2,3]')"));
	[Fact] public async Task JsonType_String() => Assert.Equal("string", await S("SELECT JSON_TYPE(JSON '\"hello\"')"));
	[Fact] public async Task JsonType_Number() => Assert.Equal("number", await S("SELECT JSON_TYPE(JSON '42')"));
	[Fact] public async Task JsonType_Boolean() => Assert.Equal("boolean", await S("SELECT JSON_TYPE(JSON 'true')"));
	[Fact] public async Task JsonType_Null() => Assert.Equal("null", await S("SELECT JSON_TYPE(JSON 'null')"));

	// ---- JSON_SET ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_set
	[Fact] public async Task JsonSet_NewKey()
	{
		var v = await S("SELECT JSON_VALUE(JSON_SET(JSON '{\"a\":1}', '$.b', JSON '2'), '$.b')");
		Assert.Equal("2", v);
	}
	[Fact] public async Task JsonSet_ReplaceExisting()
	{
		var v = await S("SELECT JSON_VALUE(JSON_SET(JSON '{\"a\":1}', '$.a', JSON '99'), '$.a')");
		Assert.Equal("99", v);
	}

	// ---- JSON_STRIP_NULLS ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_strip_nulls
	[Fact] public async Task JsonStripNulls_Basic()
	{
		var v = await S("SELECT JSON_VALUE(JSON_STRIP_NULLS(JSON '{\"a\":1,\"b\":null}'), '$.a')");
		Assert.Equal("1", v);
	}
	[Fact] public async Task JsonStripNulls_AllNull()
	{
		var v = await S("SELECT TO_JSON_STRING(JSON_STRIP_NULLS(JSON '{\"a\":null,\"b\":null}'))");
		Assert.Equal("{}", v);
	}

	// ---- JSON_REMOVE ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_remove
	[Fact] public async Task JsonRemove_Key()
	{
		var v = await S("SELECT JSON_VALUE(JSON_REMOVE(JSON '{\"a\":1,\"b\":2}', '$.a'), '$.b')");
		Assert.Equal("2", v);
	}

	// ---- JSON_OBJECT ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_object
	[Fact] public async Task JsonObject_Basic()
	{
		var v = await S("SELECT JSON_VALUE(JSON_OBJECT('a', 1, 'b', 2), '$.a')");
		Assert.Equal("1", v);
	}
	[Fact] public async Task JsonObject_Empty()
	{
		var v = await S("SELECT TO_JSON_STRING(JSON_OBJECT())");
		Assert.Equal("{}", v);
	}

	// ---- JSON_ARRAY ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_array
	[Fact] public async Task JsonArray_Basic()
	{
		var v = await S("SELECT JSON_VALUE(JSON_ARRAY(1, 2, 3), '$[0]')");
		Assert.Equal("1", v);
	}
	[Fact] public async Task JsonArray_Empty()
	{
		var v = await S("SELECT TO_JSON_STRING(JSON_ARRAY())");
		Assert.Equal("[]", v);
	}

	// ---- LAX conversions ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#lax_bool
	[Fact] public async Task LaxBool_FromTrue() => Assert.Equal("True", await S("SELECT LAX_BOOL(JSON 'true')"));
	[Fact] public async Task LaxBool_FromFalse() => Assert.Equal("False", await S("SELECT LAX_BOOL(JSON 'false')"));
	[Fact] public async Task LaxBool_FromNull() => Assert.Null(await S("SELECT LAX_BOOL(JSON 'null')"));

	[Fact] public async Task LaxInt64_FromNumber() => Assert.Equal("42", await S("SELECT LAX_INT64(JSON '42')"));
	[Fact] public async Task LaxInt64_FromString() => Assert.Equal("42", await S("SELECT LAX_INT64(JSON '\"42\"')"));
	[Fact] public async Task LaxInt64_FromNull() => Assert.Null(await S("SELECT LAX_INT64(JSON 'null')"));

	[Fact] public async Task LaxFloat64_FromNumber()
	{
		var v = await S("SELECT LAX_FLOAT64(JSON '3.14')");
		Assert.NotNull(v);
		Assert.StartsWith("3.14", v);
	}
	[Fact] public async Task LaxFloat64_FromNull() => Assert.Null(await S("SELECT LAX_FLOAT64(JSON 'null')"));

	[Fact] public async Task LaxString_FromString() => Assert.Equal("hello", await S("SELECT LAX_STRING(JSON '\"hello\"')"));
	[Fact] public async Task LaxString_FromNumber() => Assert.Equal("42", await S("SELECT LAX_STRING(JSON '42')"));
	[Fact] public async Task LaxString_FromNull() => Assert.Null(await S("SELECT LAX_STRING(JSON 'null')"));

	// ---- TO_JSON_STRING edge cases ----
	[Fact] public async Task ToJsonString_Int() => Assert.Equal("1", await S("SELECT TO_JSON_STRING(1)"));
	[Fact] public async Task ToJsonString_Bool() => Assert.Equal("true", await S("SELECT TO_JSON_STRING(true)"));
	[Fact] public async Task ToJsonString_Null() => Assert.Equal("null", await S("SELECT TO_JSON_STRING(NULL)"));
	[Fact] public async Task ToJsonString_String() => Assert.Equal("\"hello\"", await S("SELECT TO_JSON_STRING('hello')"));
	[Fact] public async Task ToJsonString_Float() => Assert.Equal("3.14", await S("SELECT TO_JSON_STRING(3.14)"));

	// ---- PARSE_JSON ----
	[Fact] public async Task ParseJson_Object()
	{
		var v = await S("SELECT JSON_VALUE(PARSE_JSON('{\"a\":1}'), '$.a')");
		Assert.Equal("1", v);
	}
	[Fact] public async Task ParseJson_Array()
	{
		var v = await S("SELECT JSON_VALUE(PARSE_JSON('[1,2,3]'), '$[0]')");
		Assert.Equal("1", v);
	}

	// ---- JSON_EXTRACT / JSON_VALUE edge cases ----
	[Fact] public async Task JsonValue_NestedPath()
	{
		var v = await S("SELECT JSON_VALUE('{\"a\":{\"b\":{\"c\":\"deep\"}}}', '$.a.b.c')");
		Assert.Equal("deep", v);
	}
	[Fact] public async Task JsonValue_ArrayIndex()
	{
		var v = await S("SELECT JSON_VALUE('[10,20,30]', '$[1]')");
		Assert.Equal("20", v);
	}
	[Fact] public async Task JsonValue_Missing() => Assert.Null(await S("SELECT JSON_VALUE('{\"a\":1}', '$.b')"));

	[Fact] public async Task JsonExtract_Object()
	{
		var v = await S("SELECT JSON_EXTRACT('{\"a\":{\"b\":1}}', '$.a')");
		Assert.NotNull(v);
		Assert.Contains("b", v);
	}

	[Fact] public async Task JsonExtractScalar_Boolean()
	{
		var v = await S("SELECT JSON_EXTRACT_SCALAR('{\"a\":true}', '$.a')");
		Assert.Equal("true", v);
	}

	// ---- JSON_EXTRACT_ARRAY ----
	[Fact] public async Task JsonExtractArray_Basic()
	{
		var v = await S("SELECT ARRAY_LENGTH(JSON_EXTRACT_ARRAY('{\"a\":[1,2,3]}', '$.a'))");
		Assert.Equal("3", v);
	}
	[Fact] public async Task JsonExtractArray_TopLevel()
	{
		var v = await S("SELECT ARRAY_LENGTH(JSON_EXTRACT_ARRAY('[1,2,3]'))");
		Assert.Equal("3", v);
	}

	// ---- JSON_EXTRACT_STRING_ARRAY ----
	[Fact] public async Task JsonExtractStringArray_Basic()
	{
		var v = await S("SELECT ARRAY_LENGTH(JSON_EXTRACT_STRING_ARRAY('[\"a\",\"b\",\"c\"]'))");
		Assert.Equal("3", v);
	}

	// ---- TO_JSON ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#to_json
	[Fact] public async Task ToJson_Integer()
	{
		// TO_JSON(42) returns JSON value; TO_JSON_STRING wraps it
		var v = await S("SELECT CAST(TO_JSON(42) AS STRING)");
		Assert.NotNull(v);
		Assert.Contains("42", v);
	}
	[Fact] public async Task ToJson_String()
	{
		var v = await S("SELECT TO_JSON_STRING(TO_JSON('hello'))");
		Assert.Equal("\"hello\"", v);
	}
}
