using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase17;

/// <summary>
/// Tests for SQL UDF routines (CREATE FUNCTION, DROP FUNCTION, function invocation).
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions
/// </summary>
public class RoutineTests
{
private static (InMemoryDataStore Store, ProceduralExecutor Executor) CreateExecutor()
{
var store = new InMemoryDataStore("test-project");
store.Datasets["test_dataset"] = new InMemoryDataset("test_dataset");
var executor = new ProceduralExecutor(store, "test_dataset");
return (store, executor);
}

[Fact]
public void CreateTempFunction_And_Invoke()
{
var (_, executor) = CreateExecutor();
var (schema, rows) = executor.Execute(@"
CREATE TEMP FUNCTION double_val(x INT64) RETURNS INT64 AS (x * 2);
SELECT double_val(21) AS result;
");
Assert.Single(rows);
Assert.Equal("42", rows[0].F?[0].V?.ToString());
}

[Fact]
public void CreateFunction_OrReplace()
{
var (_, executor) = CreateExecutor();
var (schema, rows) = executor.Execute(@"
CREATE FUNCTION my_fn(x INT64) RETURNS INT64 AS (x + 1);
CREATE OR REPLACE FUNCTION my_fn(x INT64) RETURNS INT64 AS (x + 100);
SELECT my_fn(1) AS result;
");
Assert.Single(rows);
Assert.Equal("101", rows[0].F?[0].V?.ToString());
}

[Fact]
public void CreateFunction_AlreadyExists_Throws()
{
var (_, executor) = CreateExecutor();
var ex = Assert.Throws<InvalidOperationException>(() => executor.Execute(@"
CREATE FUNCTION dup_fn(x INT64) RETURNS INT64 AS (x);
CREATE FUNCTION dup_fn(x INT64) RETURNS INT64 AS (x);
"));
Assert.Contains("already exists", ex.Message, StringComparison.OrdinalIgnoreCase);
}

[Fact]
public void DropFunction()
{
var (_, executor) = CreateExecutor();
var (schema, rows) = executor.Execute(@"
CREATE TEMP FUNCTION to_drop(x INT64) RETURNS INT64 AS (x);
DROP FUNCTION to_drop;
SELECT 1 AS done;
");
Assert.Single(rows);
}

[Fact]
public void DropFunction_IfExists_NoError()
{
var (_, executor) = CreateExecutor();
var (schema, rows) = executor.Execute(@"
DROP FUNCTION IF EXISTS nonexistent_fn;
SELECT 1 AS done;
");
Assert.Single(rows);
}

[Fact]
public void Function_MultipleParams()
{
var (_, executor) = CreateExecutor();
var (schema, rows) = executor.Execute(@"
CREATE TEMP FUNCTION add_vals(a INT64, b INT64) RETURNS INT64 AS (a + b);
SELECT add_vals(10, 32) AS result;
");
Assert.Single(rows);
Assert.Equal("42", rows[0].F?[0].V?.ToString());
}

[Fact]
public void Function_StringParam()
{
var (_, executor) = CreateExecutor();
var (schema, rows) = executor.Execute(@"
CREATE TEMP FUNCTION greet(name STRING) RETURNS STRING AS (CONCAT('Hello, ', name));
SELECT greet('World') AS result;
");
Assert.Single(rows);
Assert.Equal("Hello, World", rows[0].F?[0].V?.ToString());
}
}