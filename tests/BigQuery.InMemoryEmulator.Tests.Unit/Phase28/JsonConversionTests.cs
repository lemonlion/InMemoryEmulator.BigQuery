using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase28;

/// <summary>
/// Phase 28: JSON type conversion functions — BOOL(), INT64(), FLOAT64(), STRING()
/// when used to extract typed values from JSON.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions
/// </summary>
public class JsonConversionTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		store.Datasets["ds"] = new InMemoryDataset("ds");
		return new QueryExecutor(store, "ds");
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#bool_for_json
	//   "Converts a JSON boolean to a SQL BOOL value."
	[Fact]
	public void Bool_ExtractsJsonBoolean_True()
	{
		var exec = CreateExecutor();
		var result = exec.Execute("SELECT BOOL(JSON 'true') AS val");
		Assert.Single(result.Rows);
		Assert.Equal("true", result.Rows[0].F[0].V);
	}

	[Fact]
	public void Bool_ExtractsJsonBoolean_False()
	{
		var exec = CreateExecutor();
		var result = exec.Execute("SELECT BOOL(JSON 'false') AS val");
		Assert.Single(result.Rows);
		Assert.Equal("false", result.Rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#int64_for_json
	//   "Converts a JSON number to a SQL INT64 value."
	[Fact]
	public void Int64_ExtractsJsonNumber()
	{
		var exec = CreateExecutor();
		var result = exec.Execute("SELECT INT64(JSON '42') AS val");
		Assert.Single(result.Rows);
		Assert.Equal("42", result.Rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#float64_for_json
	//   "Converts a JSON number to a SQL FLOAT64 value."
	[Fact]
	public void Float64_ExtractsJsonNumber()
	{
		var exec = CreateExecutor();
		var result = exec.Execute("SELECT FLOAT64(JSON '3.14') AS val");
		Assert.Single(result.Rows);
		Assert.Equal("3.14", result.Rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#string_for_json
	//   "Converts a JSON string to a SQL STRING value."
	[Fact]
	public void String_ExtractsJsonString()
	{
		var exec = CreateExecutor();
		var result = exec.Execute("SELECT STRING(JSON '\"hello\"') AS val");
		Assert.Single(result.Rows);
		Assert.Equal("hello", result.Rows[0].F[0].V);
	}

	[Fact]
	public void Bool_NullInput_ReturnsNull()
	{
		var exec = CreateExecutor();
		var result = exec.Execute("SELECT BOOL(NULL) AS val");
		Assert.Single(result.Rows);
		Assert.Null(result.Rows[0].F[0].V);
	}

	[Fact]
	public void Int64_FromJsonExtract()
	{
		var exec = CreateExecutor();
		var result = exec.Execute("SELECT INT64(JSON_QUERY('{\"a\": 99}', '$.a')) AS val");
		Assert.Single(result.Rows);
		Assert.Equal("99", result.Rows[0].F[0].V);
	}
}
