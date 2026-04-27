using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase26;

/// <summary>
/// Phase 26: Aggregate functions - BIT_AND/OR/XOR, CORR, COVAR_POP, COVAR_SAMP,
/// APPROX_QUANTILES, APPROX_TOP_COUNT, APPROX_TOP_SUM.
/// </summary>
public class AggregateFunctionTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		return new QueryExecutor(store);
	}

	private static QueryExecutor CreateExecutorWithPairs(IEnumerable<(long x, long y)> pairs)
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("ds");
		store.Datasets["ds"] = ds;

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "x", Type = "INTEGER" },
				new TableFieldSchema { Name = "y", Type = "INTEGER" },
			]
		};
		var table = new InMemoryTable("ds", "pairs", schema);
		foreach (var p in pairs)
			table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["x"] = p.x, ["y"] = p.y }));
		ds.Tables["pairs"] = table;

		return new QueryExecutor(store, "ds");
	}

	private static QueryExecutor CreateExecutorWithLabelWeight(IEnumerable<(string label, long w)> items)
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("ds");
		store.Datasets["ds"] = ds;

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "label", Type = "STRING" },
				new TableFieldSchema { Name = "w", Type = "INTEGER" },
			]
		};
		var table = new InMemoryTable("ds", "data", schema);
		foreach (var i in items)
			table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["label"] = i.label, ["w"] = i.w }));
		ds.Tables["data"] = table;

		return new QueryExecutor(store, "ds");
	}

	#region BIT_AND

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#bit_and
	//   "Performs a bitwise AND operation on expression and returns the result."
	[Fact]
	public void BitAnd_Basic()
	{
		// 61441 = 0xF001, 161 = 0x00A1, AND = 1
		var sql = "SELECT BIT_AND(x) AS result FROM UNNEST([61441, 161]) AS x";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("1", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void BitAnd_AllBitsSet()
	{
		var sql = "SELECT BIT_AND(x) AS result FROM UNNEST([7, 5, 3]) AS x";
		var (_, rows) = CreateExecutor().Execute(sql);
		// 7=111, 5=101, 3=011 → AND=001=1
		Assert.Equal("1", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region BIT_OR

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#bit_or
	//   "Performs a bitwise OR operation on expression and returns the result."
	[Fact]
	public void BitOr_Basic()
	{
		// 61441 = 0xF001, 161 = 0x00A1, OR = 0xF0A1 = 61601
		var sql = "SELECT BIT_OR(x) AS result FROM UNNEST([61441, 161]) AS x";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("61601", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region BIT_XOR

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#bit_xor
	//   "Performs a bitwise XOR operation on expression and returns the result."
	[Fact]
	public void BitXor_Basic()
	{
		var sql = "SELECT BIT_XOR(x) AS result FROM UNNEST([5678, 1234]) AS x";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("4860", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void BitXor_Triple()
	{
		var sql = "SELECT BIT_XOR(x) AS result FROM UNNEST([1234, 5678, 1234]) AS x";
		var (_, rows) = CreateExecutor().Execute(sql);
		// 1234 ^ 5678 = 4860, 4860 ^ 1234 = 5678
		Assert.Equal("5678", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region CORR

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#corr
	//   "Returns the Pearson coefficient of correlation of a set of number pairs."
	[Fact]
	public void Corr_PerfectPositive()
	{
		var executor = CreateExecutorWithPairs([(1, 2), (2, 4), (3, 6)]);
		var sql = "SELECT CORR(x, y) AS result FROM pairs";
		var (_, rows) = executor.Execute(sql);
		Assert.Equal(1.0, Convert.ToDouble(rows[0].F[0].V), 5);
	}

	[Fact]
	public void Corr_PerfectNegative()
	{
		var executor = CreateExecutorWithPairs([(1, 6), (2, 4), (3, 2)]);
		var sql = "SELECT CORR(x, y) AS result FROM pairs";
		var (_, rows) = executor.Execute(sql);
		Assert.Equal(-1.0, Convert.ToDouble(rows[0].F[0].V), 5);
	}

	#endregion

	#region COVAR_POP

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#covar_pop
	//   "Returns the population covariance of a set of number pairs."
	[Fact]
	public void CovarPop_Basic()
	{
		var executor = CreateExecutorWithPairs([(1, 2), (2, 4), (3, 6)]);
		var sql = "SELECT COVAR_POP(x, y) AS result FROM pairs";
		var (_, rows) = executor.Execute(sql);
		// mean_x=2, mean_y=4
		// cov_pop = ((1-2)(2-4) + (2-2)(4-4) + (3-2)(6-4)) / 3 = (2+0+2)/3 = 4/3
		var expected = 4.0 / 3;
		Assert.Equal(expected, Convert.ToDouble(rows[0].F[0].V), 5);
	}

	#endregion

	#region COVAR_SAMP

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#covar_samp
	//   "Returns the sample covariance of a set of number pairs."
	[Fact]
	public void CovarSamp_Basic()
	{
		var executor = CreateExecutorWithPairs([(1, 2), (2, 4), (3, 6)]);
		var sql = "SELECT COVAR_SAMP(x, y) AS result FROM pairs";
		var (_, rows) = executor.Execute(sql);
		// mean_x=2, mean_y=4
		// cov_samp = ((1-2)(2-4) + (2-2)(4-4) + (3-2)(6-4)) / (3-1) = 4/2 = 2
		Assert.Equal(2.0, Convert.ToDouble(rows[0].F[0].V), 5);
	}

	[Fact]
	public void CovarSamp_SingleRow_ReturnsNull()
	{
		var sql = "SELECT COVAR_SAMP(x, y) AS result FROM (SELECT 1 AS x, 2 AS y)";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region APPROX_QUANTILES

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/approximate_aggregate_functions#approx_quantiles
	//   "Returns the approximate boundaries for a group of expression values, where number
	//    represents the number of quantiles to create."
	[Fact]
	public void ApproxQuantiles_Basic()
	{
		var sql = "SELECT APPROX_QUANTILES(x, 2) AS result FROM UNNEST([1, 2, 3, 4, 5]) AS x";
		var (_, rows) = CreateExecutor().Execute(sql);
		// 2 quantiles → 3 boundaries: [min, median, max] = [1, 3, 5]
		var result = rows[0].F[0].V?.ToString();
		Assert.Contains("1", result);
		Assert.Contains("3", result);
		Assert.Contains("5", result);
	}

	#endregion

	#region APPROX_TOP_COUNT

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/approximate_aggregate_functions#approx_top_count
	//   "Returns the approximate top elements of expression as an array of STRUCTs."
	[Fact]
	public void ApproxTopCount_Basic()
	{
		var sql = "SELECT APPROX_TOP_COUNT(x, 2) AS result FROM UNNEST(['apple', 'apple', 'pear', 'pear', 'pear', 'banana']) AS x";
		var (_, rows) = CreateExecutor().Execute(sql);
		// Top 2: pear(3), apple(2)
		Assert.NotNull(rows[0].F[0].V);
	}

	#endregion

	#region APPROX_TOP_SUM

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/approximate_aggregate_functions#approx_top_sum
	//   "Returns the approximate top elements of expression, based on the sum of an assigned weight."
	[Fact]
	public void ApproxTopSum_Basic()
	{
		var executor = CreateExecutorWithLabelWeight([("a", 10), ("b", 20), ("a", 30), ("c", 5)]);
		var sql = "SELECT APPROX_TOP_SUM(label, w, 2) AS result FROM data";
		var (_, rows) = executor.Execute(sql);
		// a:40, b:20, c:5 → top 2: a(40), b(20)
		Assert.NotNull(rows[0].F[0].V);
	}

	#endregion
}
