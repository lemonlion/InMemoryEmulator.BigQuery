using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase29;

/// <summary>
/// Phase 29: Array functions — ARRAY_FILTER, ARRAY_IS_DISTINCT, ARRAY_TRANSFORM.
/// </summary>
public class ArrayFunctionTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		return new QueryExecutor(store);
	}

	#region ARRAY_IS_DISTINCT

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_is_distinct
	//   "Returns true if the array contains no repeated elements."

	[Fact]
	public void ArrayIsDistinct_AllDistinct_ReturnsTrue()
	{
		var sql = "SELECT ARRAY_IS_DISTINCT([1, 2, 3]) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("true", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void ArrayIsDistinct_WithDuplicates_ReturnsFalse()
	{
		var sql = "SELECT ARRAY_IS_DISTINCT([1, 2, 1]) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("false", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void ArrayIsDistinct_EmptyArray_ReturnsTrue()
	{
		var sql = "SELECT ARRAY_IS_DISTINCT(GENERATE_ARRAY(1, 0)) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("true", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void ArrayIsDistinct_StringValues()
	{
		var sql = "SELECT ARRAY_IS_DISTINCT(['a', 'b', 'c']) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("true", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void ArrayIsDistinct_StringDuplicates()
	{
		var sql = "SELECT ARRAY_IS_DISTINCT(['a', 'b', 'a']) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("false", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void ArrayIsDistinct_NullInput_ReturnsNull()
	{
		var sql = "SELECT ARRAY_IS_DISTINCT(NULL) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region ARRAY_FILTER

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_filter
	//   "Takes an array, filters out unwanted elements using a boolean lambda expression."

	[Fact]
	public void ArrayFilter_BasicFilter()
	{
		var sql = "SELECT ARRAY_FILTER([1, 2, 3, 4, 5], e -> e > 3) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		var result = rows[0].F[0].V?.ToString();
		Assert.Contains("4", result);
		Assert.Contains("5", result);
		Assert.DoesNotContain("1", result);
	}

	[Fact]
	public void ArrayFilter_NoMatch_ReturnsEmpty()
	{
		var sql = "SELECT ARRAY_LENGTH(ARRAY_FILTER([1, 2, 3], e -> e > 10)) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("0", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void ArrayFilter_AllMatch()
	{
		var sql = "SELECT ARRAY_LENGTH(ARRAY_FILTER([1, 2, 3], e -> e > 0)) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("3", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void ArrayFilter_NullInput_ReturnsNull()
	{
		var sql = "SELECT ARRAY_FILTER(NULL, e -> e > 0) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region ARRAY_TRANSFORM

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_transform
	//   "Takes an array, transforms each element using a lambda expression, and returns a new array."

	[Fact]
	public void ArrayTransform_MultiplyByTwo()
	{
		var sql = "SELECT ARRAY_TRANSFORM([1, 2, 3], e -> e * 2) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		var result = rows[0].F[0].V?.ToString();
		Assert.Contains("2", result);
		Assert.Contains("4", result);
		Assert.Contains("6", result);
	}

	[Fact]
	public void ArrayTransform_AddOne()
	{
		var sql = "SELECT ARRAY_TRANSFORM([10, 20, 30], e -> e + 1) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		var result = rows[0].F[0].V?.ToString();
		Assert.Contains("11", result);
		Assert.Contains("21", result);
		Assert.Contains("31", result);
	}

	[Fact]
	public void ArrayTransform_NullInput_ReturnsNull()
	{
		var sql = "SELECT ARRAY_TRANSFORM(NULL, e -> e * 2) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	[Fact]
	public void ArrayTransform_EmptyArray()
	{
		var sql = "SELECT ARRAY_LENGTH(ARRAY_TRANSFORM(GENERATE_ARRAY(1, 0), e -> e * 2)) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("0", rows[0].F[0].V?.ToString());
	}

	#endregion
}
