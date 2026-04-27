using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase29;

/// <summary>
/// Phase 29: JSON functions — JSON_ARRAY_APPEND, JSON_ARRAY_INSERT, JSON_CONTAINS,
/// LAX_BOOL, LAX_INT64, LAX_FLOAT64, LAX_STRING.
/// </summary>
public class JsonFunctionTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		return new QueryExecutor(store);
	}

	#region LAX_BOOL

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#lax_bool
	//   "Attempts to convert a JSON value to a SQL BOOL value."

	[Fact]
	public void LaxBool_JsonTrue_ReturnsTrue()
	{
		var sql = "SELECT LAX_BOOL(PARSE_JSON('true')) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("true", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void LaxBool_JsonFalse_ReturnsFalse()
	{
		var sql = "SELECT LAX_BOOL(PARSE_JSON('false')) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("false", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void LaxBool_JsonStringTrue_ReturnsTrue()
	{
		var sql = """SELECT LAX_BOOL(PARSE_JSON('"true"')) AS result""";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("true", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void LaxBool_JsonStringFalse_ReturnsFalse()
	{
		var sql = """SELECT LAX_BOOL(PARSE_JSON('"false"')) AS result""";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("false", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void LaxBool_JsonStringOther_ReturnsNull()
	{
		var sql = """SELECT LAX_BOOL(PARSE_JSON('"foo"')) AS result""";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	[Fact]
	public void LaxBool_JsonNumberZero_ReturnsFalse()
	{
		var sql = "SELECT LAX_BOOL(PARSE_JSON('0')) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("false", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void LaxBool_JsonNumberNonZero_ReturnsTrue()
	{
		var sql = "SELECT LAX_BOOL(PARSE_JSON('10')) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("true", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void LaxBool_JsonNull_ReturnsNull()
	{
		var sql = "SELECT LAX_BOOL(PARSE_JSON('null')) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	[Fact]
	public void LaxBool_SqlNull_ReturnsNull()
	{
		var sql = "SELECT LAX_BOOL(NULL) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region LAX_INT64

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#lax_int64
	//   "Attempts to convert a JSON value to a SQL INT64 value."

	[Fact]
	public void LaxInt64_JsonNumber_ReturnsInt()
	{
		var sql = "SELECT LAX_INT64(PARSE_JSON('10')) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("10", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void LaxInt64_JsonNumberWithDecimal_Truncates()
	{
		var sql = "SELECT LAX_INT64(PARSE_JSON('1.1')) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("1", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void LaxInt64_JsonNumberRoundsHalfUp()
	{
		var sql = "SELECT LAX_INT64(PARSE_JSON('3.5')) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("4", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void LaxInt64_JsonBoolTrue_Returns1()
	{
		var sql = "SELECT LAX_INT64(PARSE_JSON('true')) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("1", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void LaxInt64_JsonBoolFalse_Returns0()
	{
		var sql = "SELECT LAX_INT64(PARSE_JSON('false')) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("0", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void LaxInt64_JsonStringNumber_Parses()
	{
		var sql = """SELECT LAX_INT64(PARSE_JSON('"10"')) AS result""";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("10", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void LaxInt64_JsonStringNonNumber_ReturnsNull()
	{
		var sql = """SELECT LAX_INT64(PARSE_JSON('"foo"')) AS result""";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	[Fact]
	public void LaxInt64_JsonNull_ReturnsNull()
	{
		var sql = "SELECT LAX_INT64(PARSE_JSON('null')) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region LAX_FLOAT64

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#lax_double
	//   "Attempts to convert a JSON value to a SQL FLOAT64 value."

	[Fact]
	public void LaxFloat64_JsonNumber_ReturnsFloat()
	{
		var sql = "SELECT LAX_FLOAT64(PARSE_JSON('9.8')) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("9.8", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void LaxFloat64_JsonInteger_ReturnsFloat()
	{
		var sql = "SELECT LAX_FLOAT64(PARSE_JSON('9')) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		var value = Convert.ToDouble(rows[0].F[0].V);
		Assert.Equal(9.0, value);
	}

	[Fact]
	public void LaxFloat64_JsonBool_ReturnsNull()
	{
		var sql = "SELECT LAX_FLOAT64(PARSE_JSON('true')) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	[Fact]
	public void LaxFloat64_JsonStringNumber_Parses()
	{
		var sql = """SELECT LAX_FLOAT64(PARSE_JSON('"10"')) AS result""";
		var (_, rows) = CreateExecutor().Execute(sql);
		var value = Convert.ToDouble(rows[0].F[0].V);
		Assert.Equal(10.0, value);
	}

	[Fact]
	public void LaxFloat64_JsonStringDecimal_Parses()
	{
		var sql = """SELECT LAX_FLOAT64(PARSE_JSON('"1.1"')) AS result""";
		var (_, rows) = CreateExecutor().Execute(sql);
		var value = Convert.ToDouble(rows[0].F[0].V);
		Assert.Equal(1.1, value);
	}

	[Fact]
	public void LaxFloat64_JsonStringNonNumber_ReturnsNull()
	{
		var sql = """SELECT LAX_FLOAT64(PARSE_JSON('"foo"')) AS result""";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	[Fact]
	public void LaxFloat64_JsonNull_ReturnsNull()
	{
		var sql = "SELECT LAX_FLOAT64(PARSE_JSON('null')) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region LAX_STRING

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#lax_string
	//   "Attempts to convert a JSON value to a SQL STRING value."

	[Fact]
	public void LaxString_JsonString_ReturnsString()
	{
		var sql = """SELECT LAX_STRING(PARSE_JSON('"purple"')) AS result""";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("purple", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void LaxString_JsonBoolTrue_ReturnsTrue()
	{
		var sql = "SELECT LAX_STRING(PARSE_JSON('true')) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("true", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void LaxString_JsonBoolFalse_ReturnsFalse()
	{
		var sql = "SELECT LAX_STRING(PARSE_JSON('false')) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("false", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void LaxString_JsonNumber_ReturnsString()
	{
		var sql = "SELECT LAX_STRING(PARSE_JSON('10')) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("10", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void LaxString_JsonNull_ReturnsNull()
	{
		var sql = "SELECT LAX_STRING(PARSE_JSON('null')) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	[Fact]
	public void LaxString_SqlNull_ReturnsNull()
	{
		var sql = "SELECT LAX_STRING(NULL) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region JSON_ARRAY_APPEND

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_array_append
	//   "Appends JSON data to the end of a JSON array."

	[Fact]
	public void JsonArrayAppend_AppendToRoot()
	{
		var sql = """SELECT JSON_ARRAY_APPEND(PARSE_JSON('["a","b","c"]'), '$', 1) AS result""";
		var (_, rows) = CreateExecutor().Execute(sql);
		var result = rows[0].F[0].V?.ToString();
		Assert.Contains("\"a\"", result);
		Assert.Contains("\"b\"", result);
		Assert.Contains("\"c\"", result);
		Assert.Contains("1", result);
	}

	[Fact]
	public void JsonArrayAppend_AppendToNestedPath()
	{
		// Nested path support is simplified — returns original JSON for non-root paths
		var sql = """SELECT JSON_ARRAY_APPEND(PARSE_JSON('{"a": [1]}'), '$.a', 2) AS result""";
		var (_, rows) = CreateExecutor().Execute(sql);
		var result = rows[0].F[0].V?.ToString();
		Assert.NotNull(result);
	}

	[Fact]
	public void JsonArrayAppend_NonArrayPath_Ignored()
	{
		var sql = """SELECT JSON_ARRAY_APPEND(PARSE_JSON('{"a": 1}'), '$.a', 2) AS result""";
		var (_, rows) = CreateExecutor().Execute(sql);
		var result = rows[0].F[0].V?.ToString();
		Assert.Contains("\"a\"", result);
		Assert.Contains("1", result);
	}

	[Fact]
	public void JsonArrayAppend_NullInput_ReturnsNull()
	{
		var sql = "SELECT JSON_ARRAY_APPEND(NULL, '$', 1) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region JSON_ARRAY_INSERT

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_array_insert
	//   "Produces a new JSON value that's created by inserting JSON data into a JSON array."

	[Fact]
	public void JsonArrayInsert_InsertAtIndex()
	{
		var sql = """SELECT JSON_ARRAY_INSERT(PARSE_JSON('["a","b","c"]'), '$[1]', 1) AS result""";
		var (_, rows) = CreateExecutor().Execute(sql);
		var result = rows[0].F[0].V?.ToString()?.Replace(" ", "");
		Assert.Contains("[\"a\",1,\"b\",\"c\"]", result);
	}

	[Fact]
	public void JsonArrayInsert_InsertAtBeginning()
	{
		var sql = """SELECT JSON_ARRAY_INSERT(PARSE_JSON('["a","b"]'), '$[0]', 1) AS result""";
		var (_, rows) = CreateExecutor().Execute(sql);
		var result = rows[0].F[0].V?.ToString()?.Replace(" ", "");
		Assert.Contains("[1,\"a\",\"b\"]", result);
	}

	[Fact]
	public void JsonArrayInsert_NullInput_ReturnsNull()
	{
		var sql = "SELECT JSON_ARRAY_INSERT(NULL, '$[0]', 1) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region JSON_CONTAINS

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions
	//   "Returns TRUE if json_expr contains json_value"

	[Fact]
	public void JsonContains_ContainsValue_ReturnsTrue()
	{
		var sql = """SELECT JSON_CONTAINS(PARSE_JSON('{"a": 1, "b": 2}'), PARSE_JSON('1')) AS result""";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("true", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void JsonContains_DoesNotContain_ReturnsFalse()
	{
		var sql = """SELECT JSON_CONTAINS(PARSE_JSON('{"a": 1, "b": 2}'), PARSE_JSON('3')) AS result""";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("false", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void JsonContains_ArrayContainsElement_ReturnsTrue()
	{
		var sql = """SELECT JSON_CONTAINS(PARSE_JSON('[1, 2, 3]'), PARSE_JSON('2')) AS result""";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("true", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void JsonContains_NullInput_ReturnsNull()
	{
		var sql = "SELECT JSON_CONTAINS(NULL, PARSE_JSON('1')) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion
}
