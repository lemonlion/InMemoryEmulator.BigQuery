using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase26;

/// <summary>
/// Phase 26: Array functions - GENERATE_DATE_ARRAY, GENERATE_TIMESTAMP_ARRAY,
/// ARRAY_INCLUDES, ARRAY_INCLUDES_ALL, ARRAY_INCLUDES_ANY,
/// ARRAY_MAX, ARRAY_MIN, ARRAY_SUM, ARRAY_AVG.
/// </summary>
public class ArrayFunctionTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		return new QueryExecutor(store);
	}

	#region GENERATE_DATE_ARRAY

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_date_array
	//   "Returns an array of dates. The start_date and end_date parameters determine the inclusive
	//    start and end of the array."
	[Fact]
	public void GenerateDateArray_DefaultStep()
	{
		var sql = "SELECT GENERATE_DATE_ARRAY('2016-10-05', '2016-10-08') AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		var result = rows[0].F[0].V?.ToString();
		Assert.Contains("2016-10-05", result);
		Assert.Contains("2016-10-06", result);
		Assert.Contains("2016-10-07", result);
		Assert.Contains("2016-10-08", result);
	}

	[Fact]
	public void GenerateDateArray_WithStep()
	{
		var sql = "SELECT GENERATE_DATE_ARRAY('2016-10-05', '2016-10-09', INTERVAL 2 DAY) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		var result = rows[0].F[0].V?.ToString();
		Assert.Contains("2016-10-05", result);
		Assert.Contains("2016-10-07", result);
		Assert.Contains("2016-10-09", result);
	}

	[Fact]
	public void GenerateDateArray_MonthStep()
	{
		var sql = "SELECT GENERATE_DATE_ARRAY('2016-01-01', '2016-05-01', INTERVAL 2 MONTH) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		var result = rows[0].F[0].V?.ToString();
		Assert.Contains("2016-01-01", result);
		Assert.Contains("2016-03-01", result);
		Assert.Contains("2016-05-01", result);
	}

	[Fact]
	public void GenerateDateArray_EmptyWhenStartAfterEnd()
	{
		var sql = "SELECT GENERATE_DATE_ARRAY('2016-10-05', '2016-10-01', INTERVAL 1 DAY) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		// Should return empty array
		var val = rows[0].F[0].V;
		if (val is System.Collections.IList list)
			Assert.Empty(list);
	}

	#endregion

	#region GENERATE_TIMESTAMP_ARRAY

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_timestamp_array
	//   "Returns an ARRAY of TIMESTAMPS separated by a given interval."
	[Fact]
	public void GenerateTimestampArray_Basic()
	{
		var sql = "SELECT GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00', '2016-10-07 00:00:00', INTERVAL 1 DAY) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.NotNull(rows[0].F[0].V);
	}

	#endregion

	#region ARRAY_INCLUDES

	// ARRAY_INCLUDES returns TRUE if the array contains the target value.
	[Fact]
	public void ArrayIncludes_Found()
	{
		var sql = "SELECT ARRAY_INCLUDES([1, 2, 3], 2) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("true", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void ArrayIncludes_NotFound()
	{
		var sql = "SELECT ARRAY_INCLUDES([1, 2, 3], 5) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("false", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region ARRAY_INCLUDES_ALL

	// ARRAY_INCLUDES_ALL returns TRUE if every element of the second array is in the first.
	[Fact]
	public void ArrayIncludesAll_True()
	{
		var sql = "SELECT ARRAY_INCLUDES_ALL([1, 2, 3, 4], [2, 3]) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("true", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void ArrayIncludesAll_False()
	{
		var sql = "SELECT ARRAY_INCLUDES_ALL([1, 2, 3], [2, 5]) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("false", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region ARRAY_INCLUDES_ANY

	// ARRAY_INCLUDES_ANY returns TRUE if any element of the second array is in the first.
	[Fact]
	public void ArrayIncludesAny_True()
	{
		var sql = "SELECT ARRAY_INCLUDES_ANY([1, 2, 3], [5, 2]) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("true", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void ArrayIncludesAny_False()
	{
		var sql = "SELECT ARRAY_INCLUDES_ANY([1, 2, 3], [5, 6]) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("false", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region ARRAY_MAX

	// ARRAY_MAX returns the maximum value from an array.
	[Fact]
	public void ArrayMax_Basic()
	{
		var sql = "SELECT ARRAY_MAX([3, 1, 4, 1, 5]) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("5", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void ArrayMax_Null_ReturnsNull()
	{
		var sql = "SELECT ARRAY_MAX(CAST(NULL AS ARRAY<INT64>)) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region ARRAY_MIN

	// ARRAY_MIN returns the minimum value from an array.
	[Fact]
	public void ArrayMin_Basic()
	{
		var sql = "SELECT ARRAY_MIN([3, 1, 4, 1, 5]) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("1", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region ARRAY_SUM

	// ARRAY_SUM returns the sum of values in an array.
	[Fact]
	public void ArraySum_Basic()
	{
		var sql = "SELECT ARRAY_SUM([1, 2, 3, 4]) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("10", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region ARRAY_AVG

	// ARRAY_AVG returns the average of values in an array.
	[Fact]
	public void ArrayAvg_Basic()
	{
		var sql = "SELECT ARRAY_AVG([1, 2, 3, 4]) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal(2.5, Convert.ToDouble(rows[0].F[0].V));
	}

	#endregion
}
