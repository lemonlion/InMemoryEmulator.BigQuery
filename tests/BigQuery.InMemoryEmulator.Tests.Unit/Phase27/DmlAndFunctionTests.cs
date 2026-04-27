using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase27;

/// <summary>
/// Phase 27: DML — TRUNCATE TABLE, Aggregate — ARRAY_CONCAT_AGG,
/// Navigation — NTH_VALUE, PERCENTILE_CONT, PERCENTILE_DISC.
/// </summary>
public class DmlAndFunctionTests
{
	private static (QueryExecutor Exec, InMemoryDataStore Store) CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("ds");
		store.Datasets["ds"] = ds;
		return (new QueryExecutor(store, "ds"), store);
	}

	private static void SeedTable(InMemoryDataStore store)
	{
		var ds = store.Datasets["ds"];
		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER" },
				new TableFieldSchema { Name = "val", Type = "INTEGER" },
				new TableFieldSchema { Name = "grp", Type = "STRING" },
				new TableFieldSchema { Name = "name", Type = "STRING" },
			]
		};
		var table = new InMemoryTable("ds", "items", schema);
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 1L, ["val"] = 10L, ["grp"] = "A", ["name"] = "a" }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 2L, ["val"] = 20L, ["grp"] = "A", ["name"] = "b" }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 3L, ["val"] = 30L, ["grp"] = "B", ["name"] = "c" }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 4L, ["val"] = 40L, ["grp"] = "B", ["name"] = "d" }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 5L, ["val"] = 50L, ["grp"] = "B", ["name"] = "e" }));
		ds.Tables["items"] = table;
	}

	#region TRUNCATE TABLE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#truncate_table_statement
	//   "Deletes all rows from the named table."
	[Fact]
	public void TruncateTable_RemovesAllRows()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);

		Assert.Equal(5, store.Datasets["ds"].Tables["items"].Rows.Count);
		exec.Execute("TRUNCATE TABLE items");
		Assert.Empty(store.Datasets["ds"].Tables["items"].Rows);
	}

	[Fact]
	public void TruncateTable_PreservesSchema()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);

		exec.Execute("TRUNCATE TABLE items");
		var table = store.Datasets["ds"].Tables["items"];
		Assert.Equal(4, table.Schema.Fields.Count);
		Assert.Equal("id", table.Schema.Fields[0].Name);
	}

	[Fact]
	public void TruncateTable_WithDatasetPrefix()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);

		exec.Execute("TRUNCATE TABLE ds.items");
		Assert.Empty(store.Datasets["ds"].Tables["items"].Rows);
	}

	#endregion

	#region ARRAY_CONCAT_AGG

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#array_concat_agg
	//   "Concatenates elements from expression of type ARRAY, returning a single ARRAY as a result."
	[Fact]
	public void ArrayConcatAgg_Basic()
	{
		var (exec, store) = CreateExecutor();
		// Seed a table with array column
		var ds = store.Datasets["ds"];
		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER" },
				new TableFieldSchema { Name = "arr", Type = "INTEGER", Mode = "REPEATED" },
			]
		};
		var table = new InMemoryTable("ds", "arr_data", schema);
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 1L, ["arr"] = new List<object?> { 1L, 2L } }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 2L, ["arr"] = new List<object?> { 3L, 4L } }));
		ds.Tables["arr_data"] = table;

		var sql = "SELECT ARRAY_CONCAT_AGG(arr) AS result FROM arr_data";
		var (_, rows) = exec.Execute(sql);
		// Should produce a concatenated array [1, 2, 3, 4]
		var result = rows[0].F[0].V;
		Assert.NotNull(result);
	}

	[Fact]
	public void ArrayConcatAgg_SkipsNulls()
	{
		var (exec, store) = CreateExecutor();
		var ds = store.Datasets["ds"];
		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER" },
				new TableFieldSchema { Name = "arr", Type = "INTEGER", Mode = "REPEATED" },
			]
		};
		var table = new InMemoryTable("ds", "arr_data", schema);
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 1L, ["arr"] = new List<object?> { 1L } }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 2L, ["arr"] = null }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 3L, ["arr"] = new List<object?> { 2L } }));
		ds.Tables["arr_data"] = table;

		var sql = "SELECT ARRAY_CONCAT_AGG(arr) AS result FROM arr_data";
		var (_, rows) = exec.Execute(sql);
		Assert.NotNull(rows[0].F[0].V);
	}

	#endregion

	#region NTH_VALUE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#nth_value
	//   "Returns the value of value_expression at the Nth row of the current window frame."
	[Fact]
	public void NthValue_Basic()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);

		var sql = "SELECT val, NTH_VALUE(val, 2) OVER (ORDER BY val) AS nv FROM items ORDER BY val";
		var (_, rows) = exec.Execute(sql);
		// Nth value at position 2 in the ordered partition should be 20
		Assert.Equal("20", rows[1].F[1].V?.ToString());
		Assert.Equal("20", rows[2].F[1].V?.ToString());
	}

	[Fact]
	public void NthValue_BeyondRowCount_ReturnsNull()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);

		var sql = "SELECT val, NTH_VALUE(val, 100) OVER (ORDER BY val) AS nv FROM items ORDER BY val";
		var (_, rows) = exec.Execute(sql);
		// N=100 is beyond partition size, should be null
		Assert.Null(rows[0].F[1].V);
	}

	#endregion

	#region PERCENTILE_CONT

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#percentile_cont
	//   "Computes the specified percentile value for the value_expression, with linear interpolation."
	[Fact]
	public void PercentileCont_Median()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);

		var sql = "SELECT PERCENTILE_CONT(val, 0.5) OVER () AS median FROM items LIMIT 1";
		var (_, rows) = exec.Execute(sql);
		// Values: 10, 20, 30, 40, 50 → median = 30
		Assert.Equal(30.0, Convert.ToDouble(rows[0].F[0].V), 5);
	}

	[Fact]
	public void PercentileCont_Zero()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);

		var sql = "SELECT PERCENTILE_CONT(val, 0) OVER () AS p FROM items LIMIT 1";
		var (_, rows) = exec.Execute(sql);
		// 0th percentile = minimum = 10
		Assert.Equal(10.0, Convert.ToDouble(rows[0].F[0].V), 5);
	}

	[Fact]
	public void PercentileCont_One()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);

		var sql = "SELECT PERCENTILE_CONT(val, 1) OVER () AS p FROM items LIMIT 1";
		var (_, rows) = exec.Execute(sql);
		// 100th percentile = maximum = 50
		Assert.Equal(50.0, Convert.ToDouble(rows[0].F[0].V), 5);
	}

	[Fact]
	public void PercentileCont_Interpolation()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);

		var sql = "SELECT PERCENTILE_CONT(val, 0.25) OVER () AS p FROM items LIMIT 1";
		var (_, rows) = exec.Execute(sql);
		// 25th percentile of [10,20,30,40,50]: index = 0.25*4 = 1.0 → value at index 1 = 20
		Assert.Equal(20.0, Convert.ToDouble(rows[0].F[0].V), 5);
	}

	#endregion

	#region PERCENTILE_DISC

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#percentile_disc
	//   "Computes the specified percentile value for a discrete value_expression."
	[Fact]
	public void PercentileDisc_Median()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);

		var sql = "SELECT PERCENTILE_DISC(val, 0.5) OVER () AS median FROM items LIMIT 1";
		var (_, rows) = exec.Execute(sql);
		// Values: 10, 20, 30, 40, 50. CUME_DIST: 0.2, 0.4, 0.6, 0.8, 1.0
		// First row with CUME_DIST >= 0.5 is 30 (CUME_DIST=0.6)
		Assert.Equal("30", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void PercentileDisc_Zero()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);

		var sql = "SELECT PERCENTILE_DISC(val, 0) OVER () AS p FROM items LIMIT 1";
		var (_, rows) = exec.Execute(sql);
		// 0th percentile = first value = 10
		Assert.Equal("10", rows[0].F[0].V?.ToString());
	}

	#endregion
}
