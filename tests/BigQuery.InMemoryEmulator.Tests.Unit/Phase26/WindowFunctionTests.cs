using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase26;

/// <summary>
/// Phase 26: Window/numbering/navigation functions.
/// </summary>
public class WindowFunctionTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("ds");
		store.Datasets["ds"] = ds;

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "x", Type = "INTEGER" },
				new TableFieldSchema { Name = "grp", Type = "STRING" },
				new TableFieldSchema { Name = "val", Type = "INTEGER" },
				new TableFieldSchema { Name = "name", Type = "STRING" },
			]
		};
		var table = new InMemoryTable("ds", "win", schema);
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["x"] = 1L, ["grp"] = "A", ["val"] = 10L, ["name"] = "a" }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["x"] = 2L, ["grp"] = "A", ["val"] = 20L, ["name"] = "b" }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["x"] = 2L, ["grp"] = "A", ["val"] = 30L, ["name"] = "c" }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["x"] = 3L, ["grp"] = "B", ["val"] = 40L, ["name"] = "d" }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["x"] = 4L, ["grp"] = "B", ["val"] = 50L, ["name"] = "e" }));
		ds.Tables["win"] = table;

		return new QueryExecutor(store, "ds");
	}

	#region NTILE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions#ntile
	//   "Divides the rows into constant_integer_expression buckets based on row ordering
	//    and returns the 1-based bucket number that is assigned to each row."
	[Fact]
	public void Ntile_Basic()
	{
		var sql = "SELECT x, NTILE(2) OVER (ORDER BY val) AS bucket FROM win ORDER BY val";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal(5, rows.Count);
		// 5 rows / 2 buckets → sizes 3, 2 (remainder 1 goes to bucket 1)
		Assert.Equal("1", rows[0].F[1].V?.ToString()); // val=10, bucket 1
		Assert.Equal("1", rows[1].F[1].V?.ToString()); // val=20, bucket 1
		Assert.Equal("1", rows[2].F[1].V?.ToString()); // val=30, bucket 1
		Assert.Equal("2", rows[3].F[1].V?.ToString()); // val=40, bucket 2
		Assert.Equal("2", rows[4].F[1].V?.ToString()); // val=50, bucket 2
	}

	[Fact]
	public void Ntile_UnevenDistribution()
	{
		// 5 rows into 3 buckets: sizes 2, 2, 1
		var sql = "SELECT val, NTILE(3) OVER (ORDER BY val) AS bucket FROM win ORDER BY val";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("1", rows[0].F[1].V?.ToString()); // val=10, bucket 1
		Assert.Equal("1", rows[1].F[1].V?.ToString()); // val=20, bucket 1
		Assert.Equal("2", rows[2].F[1].V?.ToString()); // val=30, bucket 2
		Assert.Equal("2", rows[3].F[1].V?.ToString()); // val=40, bucket 2
		Assert.Equal("3", rows[4].F[1].V?.ToString()); // val=50, bucket 3
	}

	#endregion

	#region PERCENT_RANK

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions#percent_rank
	//   "Return the percentile rank of a row defined as (RK-1)/(NR-1)."
	[Fact]
	public void PercentRank_Basic()
	{
		var sql = "SELECT x, PERCENT_RANK() OVER (ORDER BY x) AS pr FROM win ORDER BY x, val";
		var (_, rows) = CreateExecutor().Execute(sql);
		// x values: 1, 2, 2, 3, 4. NR=5.
		// RANK for x=1 is 1 → (1-1)/4=0
		// RANK for x=2 is 2 → (2-1)/4=0.25
		// RANK for x=2 is 2 → (2-1)/4=0.25
		// RANK for x=3 is 4 → (4-1)/4=0.75
		// RANK for x=4 is 5 → (5-1)/4=1.0
		Assert.Equal(0.0, Convert.ToDouble(rows[0].F[1].V), 5);
		Assert.Equal(0.25, Convert.ToDouble(rows[1].F[1].V), 5);
		Assert.Equal(0.25, Convert.ToDouble(rows[2].F[1].V), 5);
		Assert.Equal(0.75, Convert.ToDouble(rows[3].F[1].V), 5);
		Assert.Equal(1.0, Convert.ToDouble(rows[4].F[1].V), 5);
	}

	[Fact]
	public void PercentRank_SingleRow_ReturnsZero()
	{
		var sql = "SELECT PERCENT_RANK() OVER (ORDER BY 1) AS pr FROM (SELECT 1 AS x)";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal(0.0, Convert.ToDouble(rows[0].F[0].V));
	}

	#endregion

	#region CUME_DIST

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions#cume_dist
	//   "Return the relative rank of a row defined as NP/NR."
	[Fact]
	public void CumeDist_Basic()
	{
		var sql = "SELECT x, CUME_DIST() OVER (ORDER BY x) AS cd FROM win ORDER BY x, val";
		var (_, rows) = CreateExecutor().Execute(sql);
		// x: 1,2,2,3,4. NR=5.
		// x=1: NP=1, 1/5=0.2
		// x=2: NP=3, 3/5=0.6
		// x=2: NP=3, 3/5=0.6
		// x=3: NP=4, 4/5=0.8
		// x=4: NP=5, 5/5=1.0
		Assert.Equal(0.2, Convert.ToDouble(rows[0].F[1].V), 5);
		Assert.Equal(0.6, Convert.ToDouble(rows[1].F[1].V), 5);
		Assert.Equal(0.6, Convert.ToDouble(rows[2].F[1].V), 5);
		Assert.Equal(0.8, Convert.ToDouble(rows[3].F[1].V), 5);
		Assert.Equal(1.0, Convert.ToDouble(rows[4].F[1].V), 5);
	}

	#endregion

	#region FIRST_VALUE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#first_value
	//   "Returns the value of the value_expression for the first row in the current window frame."
	[Fact]
	public void FirstValue_Basic()
	{
		var sql = "SELECT name, FIRST_VALUE(name) OVER (ORDER BY val) AS fv FROM win ORDER BY val";
		var (_, rows) = CreateExecutor().Execute(sql);
		// All rows should get the first name in order by val = 'a'
		Assert.Equal("a", rows[0].F[1].V?.ToString());
		Assert.Equal("a", rows[1].F[1].V?.ToString());
		Assert.Equal("a", rows[2].F[1].V?.ToString());
	}

	[Fact]
	public void FirstValue_WithPartition()
	{
		var sql = "SELECT grp, val, FIRST_VALUE(val) OVER (PARTITION BY grp ORDER BY val) AS fv FROM win ORDER BY grp, val";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("10", rows[0].F[2].V?.ToString()); // A partition, first=10
		Assert.Equal("10", rows[1].F[2].V?.ToString());
		Assert.Equal("10", rows[2].F[2].V?.ToString());
		Assert.Equal("40", rows[3].F[2].V?.ToString()); // B partition, first=40
		Assert.Equal("40", rows[4].F[2].V?.ToString());
	}

	#endregion

	#region LAST_VALUE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#last_value
	//   "Returns the value of the value_expression for the last row in the current window frame."
	[Fact]
	public void LastValue_Basic()
	{
		var sql = "SELECT name, LAST_VALUE(name) OVER (ORDER BY val) AS lv FROM win ORDER BY val";
		var (_, rows) = CreateExecutor().Execute(sql);
		// Without explicit frame, our emulator uses the whole partition
		Assert.Equal("e", rows[4].F[1].V?.ToString());
	}

	#endregion

	#region LAG

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#lag
	//   "Returns the value of the value_expression on a preceding row."
	[Fact]
	public void Lag_Default()
	{
		var sql = "SELECT val, LAG(val) OVER (ORDER BY val) AS prev FROM win ORDER BY val";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[1].V); // no previous for first row
		Assert.Equal("10", rows[1].F[1].V?.ToString());
		Assert.Equal("20", rows[2].F[1].V?.ToString());
	}

	[Fact]
	public void Lag_WithOffset()
	{
		var sql = "SELECT val, LAG(val, 2) OVER (ORDER BY val) AS prev FROM win ORDER BY val";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[1].V);
		Assert.Null(rows[1].F[1].V);
		Assert.Equal("10", rows[2].F[1].V?.ToString());
		Assert.Equal("20", rows[3].F[1].V?.ToString());
	}

	[Fact]
	public void Lag_WithDefault()
	{
		var sql = "SELECT val, LAG(val, 1, -1) OVER (ORDER BY val) AS prev FROM win ORDER BY val";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("-1", rows[0].F[1].V?.ToString());
		Assert.Equal("10", rows[1].F[1].V?.ToString());
		Assert.Equal("20", rows[2].F[1].V?.ToString());
	}

	#endregion

	#region LEAD

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#lead
	//   "Returns the value of the value_expression on a subsequent row."
	[Fact]
	public void Lead_Default()
	{
		var sql = "SELECT val, LEAD(val) OVER (ORDER BY val) AS nxt FROM win ORDER BY val";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("20", rows[0].F[1].V?.ToString());
		Assert.Equal("30", rows[1].F[1].V?.ToString());
		Assert.Null(rows[4].F[1].V);
	}

	[Fact]
	public void Lead_WithOffset()
	{
		var sql = "SELECT val, LEAD(val, 2) OVER (ORDER BY val) AS nxt FROM win ORDER BY val";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("30", rows[0].F[1].V?.ToString());
		Assert.Equal("40", rows[1].F[1].V?.ToString());
		Assert.Null(rows[3].F[1].V);
		Assert.Null(rows[4].F[1].V);
	}

	[Fact]
	public void Lead_WithDefault()
	{
		var sql = "SELECT val, LEAD(val, 1, 99) OVER (ORDER BY val) AS nxt FROM win ORDER BY val";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("20", rows[0].F[1].V?.ToString());
		Assert.Equal("30", rows[1].F[1].V?.ToString());
		Assert.Equal("99", rows[4].F[1].V?.ToString());
	}

	#endregion
}
