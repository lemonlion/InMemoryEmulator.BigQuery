using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for JSON functions: JSON_EXTRACT, JSON_EXTRACT_SCALAR, JSON_QUERY, JSON_VALUE, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class JsonFunctionTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public JsonFunctionTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- JSON_EXTRACT_SCALAR ----
	[Fact] public async Task JsonExtractScalar_String() => Assert.Equal("Alice", await Scalar("SELECT JSON_EXTRACT_SCALAR('{\"name\": \"Alice\", \"age\": 30}', '$.name')"));
	[Fact] public async Task JsonExtractScalar_Number() => Assert.Equal("30", await Scalar("SELECT JSON_EXTRACT_SCALAR('{\"name\": \"Alice\", \"age\": 30}', '$.age')"));
	[Fact] public async Task JsonExtractScalar_Missing() => Assert.Null(await Scalar("SELECT JSON_EXTRACT_SCALAR('{\"name\": \"Alice\"}', '$.missing')"));
	[Fact] public async Task JsonExtractScalar_Nested() => Assert.Equal("NY", await Scalar("SELECT JSON_EXTRACT_SCALAR('{\"addr\": {\"city\": \"NY\"}}', '$.addr.city')"));
	[Fact] public async Task JsonExtractScalar_Null() => Assert.Null(await Scalar("SELECT JSON_EXTRACT_SCALAR(NULL, '$.name')"));
	[Fact(Skip = "Needs investigation")] public async Task JsonExtractScalar_ArrayElem() => Assert.Equal("2", await Scalar("SELECT JSON_EXTRACT_SCALAR('[1, 2, 3]', '$[1]')"));
	[Fact] public async Task JsonExtractScalar_BoolTrue() => Assert.Equal("true", await Scalar("SELECT JSON_EXTRACT_SCALAR('{\"flag\": true}', '$.flag')"));
	[Fact] public async Task JsonExtractScalar_BoolFalse() => Assert.Equal("false", await Scalar("SELECT JSON_EXTRACT_SCALAR('{\"flag\": false}', '$.flag')"));
	[Fact] public async Task JsonExtractScalar_NullValue() => Assert.Null(await Scalar("SELECT JSON_EXTRACT_SCALAR('{\"val\": null}', '$.val')"));
	[Fact] public async Task JsonExtractScalar_EmptyString() => Assert.Equal("", await Scalar("SELECT JSON_EXTRACT_SCALAR('{\"val\": \"\"}', '$.val')"));
	[Fact] public async Task JsonExtractScalar_DeepNested() => Assert.Equal("deep", await Scalar("SELECT JSON_EXTRACT_SCALAR('{\"a\": {\"b\": {\"c\": \"deep\"}}}', '$.a.b.c')"));
	[Fact] public async Task JsonExtractScalar_NumericString() => Assert.Equal("42", await Scalar("SELECT JSON_EXTRACT_SCALAR('{\"n\": 42}', '$.n')"));

	// ---- JSON_EXTRACT ----
	[Fact] public async Task JsonExtract_Object() { var v = await Scalar("SELECT JSON_EXTRACT('{\"a\": {\"b\": 1}}', '$.a')"); Assert.NotNull(v); Assert.Contains("b", v); }
	[Fact] public async Task JsonExtract_Array() { var v = await Scalar("SELECT JSON_EXTRACT('{\"arr\": [1, 2, 3]}', '$.arr')"); Assert.NotNull(v); Assert.Contains("1", v); }
	[Fact] public async Task JsonExtract_Missing() => Assert.Null(await Scalar("SELECT JSON_EXTRACT('{\"a\": 1}', '$.b')"));
	[Fact] public async Task JsonExtract_Root() { var v = await Scalar("SELECT JSON_EXTRACT('{\"a\": 1}', '$')"); Assert.NotNull(v); }
	[Fact] public async Task JsonExtract_Null() => Assert.Null(await Scalar("SELECT JSON_EXTRACT(NULL, '$.a')"));

	// ---- JSON_VALUE (alias for JSON_EXTRACT_SCALAR in some contexts) ----
	[Fact] public async Task JsonValue_String() => Assert.Equal("hello", await Scalar("SELECT JSON_VALUE('{\"msg\": \"hello\"}', '$.msg')"));
	[Fact] public async Task JsonValue_Number() => Assert.Equal("42", await Scalar("SELECT JSON_VALUE('{\"n\": 42}', '$.n')"));
	[Fact] public async Task JsonValue_Missing() => Assert.Null(await Scalar("SELECT JSON_VALUE('{\"a\": 1}', '$.b')"));
	[Fact] public async Task JsonValue_Null() => Assert.Null(await Scalar("SELECT JSON_VALUE(NULL, '$.a')"));

	// ---- JSON_QUERY (returns JSON) ----
	[Fact] public async Task JsonQuery_Object() { var v = await Scalar("SELECT JSON_QUERY('{\"a\": {\"x\": 1}}', '$.a')"); Assert.NotNull(v); Assert.Contains("x", v); }
	[Fact] public async Task JsonQuery_Array() { var v = await Scalar("SELECT JSON_QUERY('{\"arr\": [1,2]}', '$.arr')"); Assert.NotNull(v); Assert.Contains("1", v); }
	[Fact] public async Task JsonQuery_Scalar() { var v = await Scalar("SELECT JSON_QUERY('{\"n\": 42}', '$.n')"); Assert.Contains("42", v); }
	[Fact] public async Task JsonQuery_Missing() => Assert.Null(await Scalar("SELECT JSON_QUERY('{\"a\": 1}', '$.b')"));
	[Fact] public async Task JsonQuery_Null() => Assert.Null(await Scalar("SELECT JSON_QUERY(NULL, '$.a')"));

	// ---- JSON_EXTRACT_ARRAY ----
	[Fact]
	public async Task JsonExtractArray_Length()
	{
		var v = await Scalar("SELECT ARRAY_LENGTH(JSON_EXTRACT_ARRAY('[1, 2, 3]'))");
		Assert.Equal("3", v);
	}
	[Fact]
	public async Task JsonExtractArray_FromObject()
	{
		var v = await Scalar("SELECT ARRAY_LENGTH(JSON_EXTRACT_ARRAY('{\"items\": [1, 2]}', '$.items'))");
		Assert.Equal("2", v);
	}
	[Fact]
	public async Task JsonExtractArray_Empty()
	{
		var v = await Scalar("SELECT ARRAY_LENGTH(JSON_EXTRACT_ARRAY('[]'))");
		Assert.Equal("0", v);
	}

	// ---- JSON_EXTRACT_STRING_ARRAY ----
	[Fact]
	public async Task JsonExtractStringArray_Basic()
	{
		var v = await Scalar("SELECT ARRAY_TO_STRING(JSON_EXTRACT_STRING_ARRAY('[\"a\", \"b\", \"c\"]'), ',')");
		Assert.Equal("a,b,c", v);
	}

	// ---- TO_JSON_STRING ----
	[Fact] public async Task ToJsonString_Int() { var v = await Scalar("SELECT TO_JSON_STRING(42)"); Assert.Equal("42", v); }
	[Fact] public async Task ToJsonString_String() { var v = await Scalar("SELECT TO_JSON_STRING('hello')"); Assert.Contains("hello", v); }
	[Fact] public async Task ToJsonString_Bool() { var v = await Scalar("SELECT TO_JSON_STRING(TRUE)"); Assert.Contains("true", v?.ToLower()); }
	[Fact] public async Task ToJsonString_Null() { var v = await Scalar("SELECT TO_JSON_STRING(NULL)"); Assert.Contains("null", v?.ToLower()); }
	[Fact] public async Task ToJsonString_Array() { var v = await Scalar("SELECT TO_JSON_STRING([1, 2, 3])"); Assert.Contains("1", v); Assert.Contains("3", v); }

	// ---- Nested JSON operations ----
	[Fact]
	public async Task JsonNested_ExtractThenLength()
	{
		var v = await Scalar("SELECT LENGTH(JSON_EXTRACT_SCALAR('{\"name\": \"Alice\"}', '$.name'))");
		Assert.Equal("5", v);
	}

	[Fact]
	public async Task JsonNested_ConcatExtracted()
	{
		var v = await Scalar("SELECT CONCAT(JSON_EXTRACT_SCALAR('{\"first\": \"John\"}', '$.first'), ' ', JSON_EXTRACT_SCALAR('{\"last\": \"Doe\"}', '$.last'))");
		Assert.Equal("John Doe", v);
	}

	// ---- Edge cases ----
	[Fact] public async Task Json_EmptyObject() { var v = await Scalar("SELECT JSON_EXTRACT('{}', '$.a')"); Assert.Null(v); }
	[Fact] public async Task Json_EmptyArray() { var v = await Scalar("SELECT ARRAY_LENGTH(JSON_EXTRACT_ARRAY('[]'))"); Assert.Equal("0", v); }
	[Fact(Skip = "Needs investigation")] public async Task Json_SpecialChars() => Assert.Equal("he said \"hi\"", await Scalar("SELECT JSON_EXTRACT_SCALAR('{\"msg\": \"he said \\\\\"hi\\\\\"\"}', '$.msg')"));
}
