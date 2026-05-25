using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Unit.Phase26;

/// <summary>
/// Phase 26: Array functions - GENERATE_DATE_ARRAY, GENERATE_TIMESTAMP_ARRAY.
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

}
