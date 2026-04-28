using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Comprehensive integration tests for JSON functions.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class JsonFunctionComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public JsonFunctionComprehensiveTests(BigQuerySession session) => _session = session;

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

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- JSON_EXTRACT / JSON_QUERY ----
	[Fact] public async Task JsonExtract_String() => Assert.Equal("\"bar\"", await Scalar("SELECT JSON_EXTRACT('{\"foo\": \"bar\"}', '$.foo')"));
	[Fact] public async Task JsonExtract_Number() => Assert.Equal("42", await Scalar("SELECT JSON_EXTRACT('{\"x\": 42}', '$.x')"));
	[Fact] public async Task JsonExtract_Nested() => Assert.Equal("\"deep\"", await Scalar("SELECT JSON_EXTRACT('{\"a\": {\"b\": \"deep\"}}', '$.a.b')"));
	[Fact] public async Task JsonExtract_Missing() => Assert.Null(await Scalar("SELECT JSON_EXTRACT('{\"x\": 1}', '$.y')"));
	[Fact] public async Task JsonExtract_Null() => Assert.Null(await Scalar("SELECT JSON_EXTRACT(NULL, '$.x')"));
	[Fact] public async Task JsonQuery_Same() => Assert.Equal("\"bar\"", await Scalar("SELECT JSON_QUERY('{\"foo\": \"bar\"}', '$.foo')"));
	[Fact] public async Task JsonQuery_Array() => Assert.Equal("[1,2,3]", await Scalar("SELECT JSON_QUERY('{\"a\": [1,2,3]}', '$.a')"));
	[Fact] public async Task JsonQuery_Object() { var v = await Scalar("SELECT JSON_QUERY('{\"a\": {\"b\": 1}}', '$.a')"); Assert.Contains("\"b\"", v); }

	// ---- JSON_EXTRACT_SCALAR / JSON_VALUE ----
	[Fact] public async Task JsonExtractScalar_String() => Assert.Equal("bar", await Scalar("SELECT JSON_EXTRACT_SCALAR('{\"foo\": \"bar\"}', '$.foo')"));
	[Fact] public async Task JsonExtractScalar_Number() => Assert.Equal("42", await Scalar("SELECT JSON_EXTRACT_SCALAR('{\"x\": 42}', '$.x')"));
	[Fact] public async Task JsonExtractScalar_Null() => Assert.Null(await Scalar("SELECT JSON_EXTRACT_SCALAR(NULL, '$.x')"));
	[Fact] public async Task JsonValue_Same() => Assert.Equal("bar", await Scalar("SELECT JSON_VALUE('{\"foo\": \"bar\"}', '$.foo')"));
	[Fact] public async Task JsonValue_Nested() => Assert.Equal("deep", await Scalar("SELECT JSON_VALUE('{\"a\": {\"b\": \"deep\"}}', '$.a.b')"));
	[Fact] public async Task JsonValue_Missing() => Assert.Null(await Scalar("SELECT JSON_VALUE('{\"x\": 1}', '$.y')"));

	// ---- JSON_EXTRACT_ARRAY / JSON_QUERY_ARRAY / JSON_EXTRACT_STRING_ARRAY / JSON_VALUE_ARRAY ----
	[Fact] public async Task JsonExtractArray_Basic() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(JSON_EXTRACT_ARRAY('{\"a\": [1,2,3]}', '$.a'))"));
	[Fact] public async Task JsonExtractArray_Root() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(JSON_EXTRACT_ARRAY('[1,2,3]'))"));
	[Fact] public async Task JsonQueryArray_Basic() => Assert.Equal("2", await Scalar("SELECT ARRAY_LENGTH(JSON_QUERY_ARRAY('{\"a\": [10,20]}', '$.a'))"));
	[Fact] public async Task JsonExtractStringArray_Basic() => Assert.Equal("2", await Scalar("SELECT ARRAY_LENGTH(JSON_EXTRACT_STRING_ARRAY('{\"a\": [\"x\",\"y\"]}', '$.a'))"));
	[Fact] public async Task JsonValueArray_Basic() => Assert.Equal("2", await Scalar("SELECT ARRAY_LENGTH(JSON_VALUE_ARRAY('{\"a\": [\"x\",\"y\"]}', '$.a'))"));

	// ---- JSON_SET ----
	[Fact] public async Task JsonSet_AddField() { var v = await Scalar("SELECT JSON_SET('{\"a\": 1}', '$.b', 2)"); Assert.Contains("\"b\"", v); }
	[Fact] public async Task JsonSet_UpdateField() { var v = await Scalar("SELECT JSON_SET('{\"a\": 1}', '$.a', 99)"); Assert.Contains("99", v); }

	// ---- JSON_REMOVE ----
	[Fact] public async Task JsonRemove_Field() { var v = await Scalar("SELECT JSON_REMOVE('{\"a\": 1, \"b\": 2}', '$.a')"); Assert.DoesNotContain("\"a\"", v); Assert.Contains("\"b\"", v); }

	// ---- JSON_STRIP_NULLS ----
	[Fact] public async Task JsonStripNulls_Basic() { var v = await Scalar("SELECT JSON_STRIP_NULLS('{\"a\": 1, \"b\": null}')"); Assert.DoesNotContain("null", v?.ToLower()); Assert.Contains("\"a\"", v); }

	// ---- JSON_KEYS ----
	[Fact] public async Task JsonKeys_Basic() => Assert.Equal("2", await Scalar("SELECT ARRAY_LENGTH(JSON_KEYS('{\"a\": 1, \"b\": 2}'))"));
	[Fact] public async Task JsonKeys_Nested() => Assert.Equal("2", await Scalar("SELECT ARRAY_LENGTH(JSON_KEYS('{\"a\": {\"c\": 1}, \"b\": 2}'))"));

	// ---- JSON_TYPE ----
	[Fact] public async Task JsonType_Object() => Assert.Equal("object", await Scalar("SELECT JSON_TYPE(PARSE_JSON('{\"a\": 1}'))"));
	[Fact] public async Task JsonType_Array() => Assert.Equal("array", await Scalar("SELECT JSON_TYPE(PARSE_JSON('[1,2]'))"));
	[Fact] public async Task JsonType_String() => Assert.Equal("string", await Scalar("SELECT JSON_TYPE(PARSE_JSON('\"hello\"'))"));
	[Fact] public async Task JsonType_Number() => Assert.Equal("number", await Scalar("SELECT JSON_TYPE(PARSE_JSON('42'))"));
	[Fact] public async Task JsonType_Boolean() => Assert.Equal("boolean", await Scalar("SELECT JSON_TYPE(PARSE_JSON('true'))"));
	[Fact] public async Task JsonType_Null() => Assert.Equal("null", await Scalar("SELECT JSON_TYPE(PARSE_JSON('null'))"));

	// ---- PARSE_JSON / TO_JSON / TO_JSON_STRING ----
	[Fact(Skip = "Needs investigation")] public async Task ParseJson_Object() { var v = await Scalar("SELECT TO_JSON_STRING(PARSE_JSON('{\"a\": 1}'))"); Assert.Contains("\"a\"", v); }
	[Fact] public async Task ToJson_Int() { var v = await Scalar("SELECT TO_JSON_STRING(42)"); Assert.Equal("42", v); }
	[Fact] public async Task ToJson_String() { var v = await Scalar("SELECT TO_JSON_STRING('hello')"); Assert.Equal("\"hello\"", v); }
	[Fact] public async Task ToJsonString_Bool() { var v = await Scalar("SELECT TO_JSON_STRING(TRUE)"); Assert.Equal("true", v); }
	[Fact] public async Task ToJsonString_Null() { var v = await Scalar("SELECT TO_JSON_STRING(NULL)"); Assert.Equal("null", v); }
	[Fact] public async Task ToJsonString_Array() { var v = await Scalar("SELECT TO_JSON_STRING([1, 2, 3])"); Assert.Equal("[1,2,3]", v); }

	// ---- JSON_ARRAY / JSON_OBJECT ----
	[Fact] public async Task JsonArray_Basic() { var v = await Scalar("SELECT TO_JSON_STRING(JSON_ARRAY(1, 'two', TRUE))"); Assert.Contains("1", v); Assert.Contains("two", v); }
	[Fact(Skip = "Needs investigation")] public async Task JsonObject_Basic() { var v = await Scalar("SELECT TO_JSON_STRING(JSON_OBJECT('a', 1, 'b', 'hello'))"); Assert.Contains("\"a\"", v); Assert.Contains("hello", v); }

	// ---- LAX type accessors ----
	[Fact(Skip = "Not yet supported")] public async Task LaxBool_True() => Assert.Equal("true", await Scalar("SELECT LAX_BOOL(PARSE_JSON('true'))"));
	[Fact(Skip = "Not yet supported")] public async Task LaxBool_StringTrue() => Assert.Equal("true", await Scalar("SELECT LAX_BOOL(PARSE_JSON('\"true\"'))"));
	[Fact(Skip = "Not yet supported")] public async Task LaxInt64_Number() => Assert.Equal("42", await Scalar("SELECT LAX_INT64(PARSE_JSON('42'))"));
	[Fact(Skip = "Not yet supported")] public async Task LaxInt64_String() => Assert.Equal("42", await Scalar("SELECT LAX_INT64(PARSE_JSON('\"42\"'))"));
	[Fact(Skip = "Not yet supported")] public async Task LaxFloat64_Number() { var v = await Scalar("SELECT LAX_FLOAT64(PARSE_JSON('3.14'))"); Assert.StartsWith("3.14", v); }
	[Fact(Skip = "Not yet supported")] public async Task LaxString_String() => Assert.Equal("hello", await Scalar("SELECT LAX_STRING(PARSE_JSON('\"hello\"'))"));
	[Fact(Skip = "Not yet supported")] public async Task LaxString_Number() => Assert.Equal("42", await Scalar("SELECT LAX_STRING(PARSE_JSON('42'))"));

	// ---- Strict type accessors ----
	[Fact(Skip = "Not yet supported")] public async Task JsonBool_True() => Assert.Equal("true", await Scalar("SELECT BOOL(PARSE_JSON('true'))"));
	[Fact(Skip = "Not yet supported")] public async Task JsonInt64_Number() => Assert.Equal("42", await Scalar("SELECT INT64(PARSE_JSON('42'))"));
	[Fact(Skip = "Not yet supported")] public async Task JsonFloat64_Number() { var v = await Scalar("SELECT FLOAT64(PARSE_JSON('3.14'))"); Assert.StartsWith("3.14", v); }
	[Fact(Skip = "Not yet supported")] public async Task JsonString_String() => Assert.Equal("hello", await Scalar("SELECT STRING(PARSE_JSON('\"hello\"'))"));

	// ---- JSON_ARRAY_APPEND / JSON_ARRAY_INSERT ----
	[Fact(Skip = "Not yet supported")] public async Task JsonArrayAppend_Basic() { var v = await Scalar("SELECT JSON_ARRAY_APPEND('[1,2]', '$', 3)"); Assert.Contains("3", v); }
	[Fact(Skip = "Not yet supported")] public async Task JsonArrayInsert_Basic() { var v = await Scalar("SELECT JSON_ARRAY_INSERT('[1,3]', '$[1]', 2)"); Assert.Contains("2", v); }

	// ---- JSON_CONTAINS ----
	[Fact(Skip = "Not yet supported")] public async Task JsonContains_Found() => Assert.Equal("true", await Scalar("SELECT JSON_CONTAINS('[1,2,3]', '2')"));
	[Fact(Skip = "Not yet supported")] public async Task JsonContains_NotFound() => Assert.Equal("false", await Scalar("SELECT JSON_CONTAINS('[1,2,3]', '5')"));
}
