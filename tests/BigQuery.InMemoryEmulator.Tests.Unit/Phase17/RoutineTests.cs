using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;
using BigQuery.InMemoryEmulator;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase17;

/// <summary>
/// Tests for SQL UDF routines (CREATE FUNCTION, DROP FUNCTION, function invocation).
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions
/// </summary>
public class RoutineTests
{
	private InMemoryBigQueryClient CreateClient()
	{
		var store = new InMemoryDataStore("test-project");
		store.Datasets["test_dataset"] = new InMemoryDataset("test_dataset");
		return new InMemoryBigQueryClient(store, "test-project", "test_dataset");
	}

	[Fact]
	public void CreateFunction_And_Invoke()
	{
		var client = CreateClient();
		client.ExecuteQuery(@"
			CREATE FUNCTION add_one(x INT64) RETURNS INT64 AS (x + 1);
			SELECT add_one(5) AS result;
		", parameters: null);
		// The multi-statement query goes through ProceduralExecutor
		// We need to test via the procedural path
	}

	[Fact]
	public void CreateTempFunction_And_Invoke()
	{
		var client = CreateClient();
		var result = client.ExecuteQuery(@"
			CREATE TEMP FUNCTION double_val(x INT64) RETURNS INT64 AS (x * 2);
			SELECT double_val(21) AS result;
		", parameters: null);
		var rows = result.ToList();
		Assert.Single(rows);
		Assert.Equal("42", rows[0]["result"]?.ToString());
	}

	[Fact]
	public void CreateFunction_OrReplace()
	{
		var client = CreateClient();
		var result = client.ExecuteQuery(@"
			CREATE FUNCTION my_fn(x INT64) RETURNS INT64 AS (x + 1);
			CREATE OR REPLACE FUNCTION my_fn(x INT64) RETURNS INT64 AS (x + 100);
			SELECT my_fn(1) AS result;
		", parameters: null);
		var rows = result.ToList();
		Assert.Single(rows);
		Assert.Equal("101", rows[0]["result"]?.ToString());
	}

	[Fact]
	public void CreateFunction_AlreadyExists_Throws()
	{
		var client = CreateClient();
		var ex = Assert.Throws<InvalidOperationException>(() => client.ExecuteQuery(@"
			CREATE FUNCTION dup_fn(x INT64) RETURNS INT64 AS (x);
			CREATE FUNCTION dup_fn(x INT64) RETURNS INT64 AS (x);
		", parameters: null));
		Assert.Contains("Already Exists", ex.Message);
	}

	[Fact]
	public void DropFunction()
	{
		var client = CreateClient();
		var result = client.ExecuteQuery(@"
			CREATE TEMP FUNCTION to_drop(x INT64) RETURNS INT64 AS (x);
			DROP FUNCTION to_drop;
			SELECT 1 AS done;
		", parameters: null);
		var rows = result.ToList();
		Assert.Single(rows);
	}

	[Fact]
	public void DropFunction_IfExists_NoError()
	{
		var client = CreateClient();
		var result = client.ExecuteQuery(@"
			DROP FUNCTION IF EXISTS nonexistent_fn;
			SELECT 1 AS done;
		", parameters: null);
		var rows = result.ToList();
		Assert.Single(rows);
	}

	[Fact]
	public void Function_MultipleParams()
	{
		var client = CreateClient();
		var result = client.ExecuteQuery(@"
			CREATE TEMP FUNCTION add_vals(a INT64, b INT64) RETURNS INT64 AS (a + b);
			SELECT add_vals(10, 32) AS result;
		", parameters: null);
		var rows = result.ToList();
		Assert.Single(rows);
		Assert.Equal("42", rows[0]["result"]?.ToString());
	}

	[Fact]
	public void Function_StringParam()
	{
		var client = CreateClient();
		var result = client.ExecuteQuery(@"
			CREATE TEMP FUNCTION greet(name STRING) RETURNS STRING AS (CONCAT('Hello, ', name));
			SELECT greet('World') AS result;
		", parameters: null);
		var rows = result.ToList();
		Assert.Single(rows);
		Assert.Equal("Hello, World", rows[0]["result"]?.ToString());
	}
}
