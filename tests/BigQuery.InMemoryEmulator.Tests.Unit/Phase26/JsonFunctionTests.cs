using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase26;

/// <summary>
/// Phase 26: JSON functions - PARSE_JSON, TO_JSON, JSON_ARRAY, JSON_OBJECT, JSON_REMOVE.
/// </summary>
public class JsonFunctionTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		return new QueryExecutor(store);
	}

	#region PARSE_JSON

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#parse_json
	//   "Takes a JSON-formatted string and returns a JSON value."
	[Fact]
	public void ParseJson_Object()
	{
		var sql = @"SELECT PARSE_JSON('{""a"": 1, ""b"": ""hello""}') AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		var result = rows[0].F[0].V?.ToString();
		Assert.Contains("a", result);
		Assert.Contains("1", result);
	}

	[Fact]
	public void ParseJson_Array()
	{
		var sql = @"SELECT PARSE_JSON('[1, 2, 3]') AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		var result = rows[0].F[0].V?.ToString();
		Assert.Contains("1", result);
		Assert.Contains("2", result);
		Assert.Contains("3", result);
	}

	[Fact]
	public void ParseJson_Null_ReturnsNull()
	{
		var sql = "SELECT PARSE_JSON(NULL) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region TO_JSON

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#to_json
	//   "Takes a SQL value and returns a JSON value."
	[Fact]
	public void ToJson_Integer()
	{
		var sql = "SELECT TO_JSON(42) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("42", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void ToJson_String()
	{
		var sql = "SELECT TO_JSON('hello') AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		var result = rows[0].F[0].V?.ToString();
		Assert.Contains("hello", result);
	}

	[Fact]
	public void ToJson_Null()
	{
		var sql = "SELECT TO_JSON(NULL) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		// TO_JSON(NULL) returns JSON null
		var result = rows[0].F[0].V?.ToString();
		Assert.Equal("null", result);
	}

	#endregion

	#region JSON_ARRAY

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_array
	//   "Creates a JSON array."
	[Fact]
	public void JsonArray_Basic()
	{
		var sql = "SELECT JSON_ARRAY(1, 2, 3) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("[1,2,3]", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void JsonArray_Mixed()
	{
		var sql = "SELECT JSON_ARRAY(1, 'hello', TRUE) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		var result = rows[0].F[0].V?.ToString();
		Assert.Contains("1", result);
		Assert.Contains("hello", result);
	}

	[Fact]
	public void JsonArray_Empty()
	{
		var sql = "SELECT JSON_ARRAY() AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("[]", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region JSON_OBJECT

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_object
	//   "Creates a JSON object."
	[Fact]
	public void JsonObject_Basic()
	{
		var sql = "SELECT JSON_OBJECT('a', 1, 'b', 2) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		var result = rows[0].F[0].V?.ToString();
		Assert.Contains("\"a\"", result);
		Assert.Contains("1", result);
		Assert.Contains("\"b\"", result);
		Assert.Contains("2", result);
	}

	[Fact]
	public void JsonObject_Empty()
	{
		var sql = "SELECT JSON_OBJECT() AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("{}", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region JSON_REMOVE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_remove
	//   "Removes a JSON element at a path."
	[Fact]
	public void JsonRemove_Basic()
	{
		var sql = @"SELECT JSON_REMOVE('{""a"": 1, ""b"": 2}', '$.a') AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		var result = rows[0].F[0].V?.ToString();
		Assert.DoesNotContain("\"a\"", result);
		Assert.Contains("\"b\"", result);
	}

	[Fact]
	public void JsonRemove_Null_ReturnsNull()
	{
		var sql = "SELECT JSON_REMOVE(NULL, '$.a') AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion
}
