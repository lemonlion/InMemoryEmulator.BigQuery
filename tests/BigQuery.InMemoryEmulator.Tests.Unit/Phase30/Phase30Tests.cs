using BigQuery.InMemoryEmulator.SqlEngine;
using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase30;

/// <summary>
/// Phase 30: Vector/distance functions (DOT_PRODUCT, APPROX_*), DCL stubs (GRANT/REVOKE),
/// statement stubs (EXPORT DATA, LOAD DATA), BEGIN...EXCEPTION...END, templated UDF args (ANY TYPE).
/// </summary>
public class Phase30Tests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
			]
		};
		var table = new InMemoryTable("test_ds", "items", schema);
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 1L, ["name"] = "alpha" }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 2L, ["name"] = "beta" }));
		ds.Tables["items"] = table;

		return new QueryExecutor(store, "test_ds");
	}

	private static void AssertNoOpResult(TableSchema schema, List<TableRow> rows)
	{
		// No-op DDL stubs return an EmptyResult: single row with affected_rows = 0.
		Assert.Single(rows);
		Assert.Equal("0", rows[0].F[0].V?.ToString());
	}

	#region DOT_PRODUCT

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions
	//   DOT_PRODUCT computes the inner product of two vectors: sum(v1[i] * v2[i]).
	[Fact]
	public void DotProduct_KnownValues()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT DOT_PRODUCT([1.0, 2.0, 3.0], [4.0, 5.0, 6.0]) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		// 1*4 + 2*5 + 3*6 = 4+10+18 = 32
		Assert.Equal(32.0, val, 10);
	}

	// Ref: DOT_PRODUCT with identical vectors => sum of squares
	[Fact]
	public void DotProduct_IdenticalVectors()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT DOT_PRODUCT([3.0, 4.0], [3.0, 4.0]) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		// 3*3 + 4*4 = 9+16 = 25
		Assert.Equal(25.0, val, 10);
	}

	// Ref: DOT_PRODUCT with orthogonal vectors => 0
	[Fact]
	public void DotProduct_OrthogonalVectors_ReturnsZero()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT DOT_PRODUCT([1.0, 0.0], [0.0, 1.0]) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		Assert.Equal(0.0, val, 10);
	}

	// Ref: DOT_PRODUCT with NULL vector => NULL
	[Fact]
	public void DotProduct_NullVector_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT DOT_PRODUCT(NULL, [1.0, 2.0]) AS d");
		Assert.Null(rows[0].F[0].V);
	}

	// Ref: DOT_PRODUCT with mismatched dimensions => NULL (lenient)
	[Fact]
	public void DotProduct_MismatchedDimensions_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT DOT_PRODUCT([1.0, 2.0], [3.0, 4.0, 5.0]) AS d");
		Assert.Null(rows[0].F[0].V);
	}

	// Ref: DOT_PRODUCT with negative values
	[Fact]
	public void DotProduct_NegativeValues()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT DOT_PRODUCT([-1.0, 2.0], [3.0, -4.0]) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		// -1*3 + 2*(-4) = -3 + -8 = -11
		Assert.Equal(-11.0, val, 10);
	}

	// Ref: DOT_PRODUCT with single element
	[Fact]
	public void DotProduct_SingleElement()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT DOT_PRODUCT([5.0], [3.0]) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		Assert.Equal(15.0, val, 10);
	}

	#endregion

	#region APPROX_COSINE_DISTANCE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/search_functions
	//   APPROX_COSINE_DISTANCE is an approximate version of COSINE_DISTANCE.
	//   In the in-memory emulator, approximate = exact.
	[Fact]
	public void ApproxCosineDistance_KnownValues()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT APPROX_COSINE_DISTANCE([1.0, 2.0], [3.0, 4.0]) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		Assert.InRange(val, 0.016, 0.017);
	}

	[Fact]
	public void ApproxCosineDistance_IdenticalVectors_ReturnsZero()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT APPROX_COSINE_DISTANCE([1.0, 2.0], [1.0, 2.0]) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		Assert.Equal(0.0, val, 10);
	}

	[Fact]
	public void ApproxCosineDistance_NullVector_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT APPROX_COSINE_DISTANCE(NULL, [1.0, 2.0]) AS d");
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region APPROX_EUCLIDEAN_DISTANCE

	// Ref: APPROX_EUCLIDEAN_DISTANCE is an approximate version of EUCLIDEAN_DISTANCE.
	//   In the in-memory emulator, approximate = exact.
	[Fact]
	public void ApproxEuclideanDistance_KnownValues()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT APPROX_EUCLIDEAN_DISTANCE([1.0, 2.0], [3.0, 4.0]) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		// sqrt(4+4) = 2.8284...
		Assert.InRange(val, 2.828, 2.829);
	}

	[Fact]
	public void ApproxEuclideanDistance_345Triangle()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT APPROX_EUCLIDEAN_DISTANCE([0.0, 0.0], [3.0, 4.0]) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		Assert.Equal(5.0, val, 10);
	}

	[Fact]
	public void ApproxEuclideanDistance_NullVector_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT APPROX_EUCLIDEAN_DISTANCE(NULL, [1.0, 2.0]) AS d");
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region APPROX_DOT_PRODUCT

	// Ref: APPROX_DOT_PRODUCT is an approximate version of DOT_PRODUCT.
	//   In the in-memory emulator, approximate = exact.
	[Fact]
	public void ApproxDotProduct_KnownValues()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT APPROX_DOT_PRODUCT([1.0, 2.0, 3.0], [4.0, 5.0, 6.0]) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		Assert.Equal(32.0, val, 10);
	}

	[Fact]
	public void ApproxDotProduct_NullVector_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT APPROX_DOT_PRODUCT(NULL, [1.0, 2.0]) AS d");
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region DCL — GRANT / REVOKE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-control-language
	//   GRANT and REVOKE manage access to datasets, tables, views, and routines.
	//   In the emulator, these are accepted as no-ops.
	[Fact]
	public void Grant_AcceptedAsNoOp()
	{
		var (schema, rows) = CreateExecutor().Execute(
			"GRANT `roles/bigquery.dataViewer` ON TABLE `test_ds.items` TO 'user:test@example.com'");
		AssertNoOpResult(schema, rows);
	}

	[Fact]
	public void Revoke_AcceptedAsNoOp()
	{
		var (schema, rows) = CreateExecutor().Execute(
			"REVOKE `roles/bigquery.dataViewer` ON TABLE `test_ds.items` FROM 'user:test@example.com'");
		AssertNoOpResult(schema, rows);
	}

	[Fact]
	public void Grant_OnSchema_AcceptedAsNoOp()
	{
		var (schema, rows) = CreateExecutor().Execute(
			"GRANT `roles/bigquery.dataEditor` ON SCHEMA `test_ds` TO 'group:team@example.com'");
		AssertNoOpResult(schema, rows);
	}

	[Fact]
	public void Revoke_OnSchema_AcceptedAsNoOp()
	{
		var (schema, rows) = CreateExecutor().Execute(
			"REVOKE `roles/bigquery.dataEditor` ON SCHEMA `test_ds` FROM 'group:team@example.com'");
		AssertNoOpResult(schema, rows);
	}

	#endregion

	#region EXPORT DATA / LOAD DATA

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/other-statements#export_data_statement
	//   EXPORT DATA exports query results to external storage.
	//   In the emulator, this is accepted as a no-op.
	[Fact]
	public void ExportData_AcceptedAsNoOp()
	{
		var (schema, rows) = CreateExecutor().Execute(
			"EXPORT DATA OPTIONS(uri='gs://bucket/path/*', format='CSV') AS SELECT * FROM `items`");
		AssertNoOpResult(schema, rows);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/other-statements#load_data_statement
	//   LOAD DATA loads data from external sources into BigQuery tables.
	//   In the emulator, this is accepted as a no-op.
	[Fact]
	public void LoadData_AcceptedAsNoOp()
	{
		var (schema, rows) = CreateExecutor().Execute(
			"LOAD DATA INTO `test_ds.items` FROM FILES(format='CSV', uris=['gs://bucket/file.csv'])");
		AssertNoOpResult(schema, rows);
	}

	[Fact]
	public void LoadData_Overwrite_AcceptedAsNoOp()
	{
		var (schema, rows) = CreateExecutor().Execute(
			"LOAD DATA OVERWRITE `test_ds.items` FROM FILES(format='PARQUET', uris=['gs://bucket/*.parquet'])");
		AssertNoOpResult(schema, rows);
	}

	#endregion

	#region BEGIN ... EXCEPTION ... END

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#begin
	//   "If an exception occurs in script_statement_list, and you have an
	//   EXCEPTION clause, control transfers to that exception handler."
	[Fact]
	public void BeginExceptionEnd_CatchesError()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;
		var executor = new ProceduralExecutor(store, "test_ds");

		var result = executor.Execute(@"
DECLARE x INT64 DEFAULT 0;
BEGIN
  SET x = 1/0;
EXCEPTION WHEN ERROR THEN
  SET x = -1;
END;
SELECT x;
");
		var val = Convert.ToInt64(result.Rows[0].F[0].V);
		Assert.Equal(-1L, val);
	}

	[Fact]
	public void BeginExceptionEnd_ErrorMessage_Available()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;
		var executor = new ProceduralExecutor(store, "test_ds");

		var result = executor.Execute(@"
DECLARE msg STRING DEFAULT '';
BEGIN
  SELECT ERROR('custom error');
EXCEPTION WHEN ERROR THEN
  SET msg = @@error.message;
END;
SELECT msg;
");
		var msg = result.Rows[0].F[0].V?.ToString();
		Assert.Contains("custom error", msg!);
	}

	[Fact]
	public void BeginExceptionEnd_NoError_ExecutesNormally()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;
		var executor = new ProceduralExecutor(store, "test_ds");

		var result = executor.Execute(@"
DECLARE x INT64 DEFAULT 0;
BEGIN
  SET x = 42;
EXCEPTION WHEN ERROR THEN
  SET x = -1;
END;
SELECT x;
");
		var val = Convert.ToInt64(result.Rows[0].F[0].V);
		Assert.Equal(42L, val);
	}

	#endregion

	#region Templated UDF (ANY TYPE)

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions#templated-sql-udf-parameters
	//   "A templated parameter can match more than one argument type at function call time.
	//    If a function signature includes a templated parameter, BigQuery allows
	//    function calls to pass one of several argument types to the function."
	[Fact]
	public void AnyType_SqlUdf_WithInt64()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;
		var executor = new ProceduralExecutor(store, "test_ds");

		executor.Execute("CREATE TEMP FUNCTION identity(x ANY TYPE) AS (x)");

		var queryExec = new QueryExecutor(store, "test_ds");
		var (_, rows) = queryExec.Execute("SELECT identity(42) AS result");
		var val = Convert.ToInt64(rows[0].F[0].V);
		Assert.Equal(42L, val);
	}

	[Fact]
	public void AnyType_SqlUdf_WithString()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;
		var executor = new ProceduralExecutor(store, "test_ds");

		executor.Execute("CREATE TEMP FUNCTION identity(x ANY TYPE) AS (x)");

		var queryExec = new QueryExecutor(store, "test_ds");
		var (_, rows) = queryExec.Execute("SELECT identity('hello') AS result");
		var val = rows[0].F[0].V?.ToString();
		Assert.Equal("hello", val);
	}

	[Fact]
	public void AnyType_SqlUdf_MultipleParams()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;
		var executor = new ProceduralExecutor(store, "test_ds");

		executor.Execute("CREATE TEMP FUNCTION add_things(a ANY TYPE, b ANY TYPE) AS (a + b)");

		var queryExec = new QueryExecutor(store, "test_ds");
		var (_, rows) = queryExec.Execute("SELECT add_things(10, 20) AS result");
		var val = Convert.ToInt64(rows[0].F[0].V);
		Assert.Equal(30L, val);
	}

	[Fact]
	public void AnyType_SqlUdf_MixedWithTypedParam()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;
		var executor = new ProceduralExecutor(store, "test_ds");

		executor.Execute("CREATE TEMP FUNCTION fmt(prefix STRING, val ANY TYPE) AS (CONCAT(prefix, CAST(val AS STRING)))");

		var queryExec = new QueryExecutor(store, "test_ds");
		var (_, rows) = queryExec.Execute("SELECT fmt('num:', 42) AS result");
		var val = rows[0].F[0].V?.ToString();
		Assert.Equal("num:42", val);
	}

	#endregion
}
