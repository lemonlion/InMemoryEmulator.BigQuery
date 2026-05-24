using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Unit.Phase32;

/// <summary>
/// Verification tests for bugs found in research round 15.
/// </summary>
public class BugVerificationRound15Tests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		return new QueryExecutor(store);
	}

	// =====================================================
	// BUG 1: DATE_DIFF with WEEK(MONDAY) falls through to default case
	// Expected: Count week boundaries starting from Monday
	// Actual: Falls through to default which returns day count
	// =====================================================
	[Fact]
	public void DateDiff_WeekMonday_ShouldCountMondayBoundaries()
	{
		// DATE_DIFF('2024-01-08', '2024-01-01', WEEK(MONDAY))
		// Jan 1 (Mon) to Jan 8 (Mon) = 1 Monday boundary crossed
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_diff
		var executor = CreateExecutor();
		var (_, rows) = executor.Execute("SELECT DATE_DIFF(DATE '2024-01-08', DATE '2024-01-01', WEEK(MONDAY))");
		Assert.Equal("1", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void DateDiff_WeekMonday_SameMondayWeek_ShouldReturnZero()
	{
		// Jan 2 (Tue) and Jan 5 (Fri) are in the same Monday-week (Jan 1 Mon)
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_diff
		var executor = CreateExecutor();
		var (_, rows) = executor.Execute("SELECT DATE_DIFF(DATE '2024-01-05', DATE '2024-01-02', WEEK(MONDAY))");
		Assert.Equal("0", rows[0].F[0].V?.ToString());
	}

	// =====================================================
	// BUG 2: POW(0, -1) should throw error, not return Infinity
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#pow
	//   "Generates an error if X is 0 and Y is a finite value less than 0."
	// =====================================================
	[Fact]
	public void Pow_ZeroNegativeExponent_ShouldThrow()
	{
		var executor = CreateExecutor();
		Assert.ThrowsAny<Exception>(() => executor.Execute("SELECT POW(0, -1)"));
	}

	[Fact]
	public void Pow_ZeroNegativeTwo_ShouldThrow()
	{
		var executor = CreateExecutor();
		Assert.ThrowsAny<Exception>(() => executor.Execute("SELECT POW(0, -2)"));
	}

	// =====================================================
	// BUG 3: JSON_EXTRACT with consecutive array indices fails
	// JSON_EXTRACT('[[[1]]]', '$[0][0][0]') should return '1'
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_extract
	// =====================================================
	[Fact]
	public void JsonExtract_ConsecutiveArrayIndices()
	{
		var executor = CreateExecutor();
		var (_, rows) = executor.Execute("SELECT JSON_EXTRACT('[[[1]]]', '$[0][0][0]')");
		Assert.Equal("1", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void JsonExtract_NestedArrayIndex()
	{
		var executor = CreateExecutor();
		var (_, rows) = executor.Execute("SELECT JSON_EXTRACT('{\"a\":[[10,20],[30,40]]}', '$.a[1][0]')");
		Assert.Equal("30", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void JsonExtractScalar_ConsecutiveArrayIndices()
	{
		var executor = CreateExecutor();
		var (_, rows) = executor.Execute("SELECT JSON_EXTRACT_SCALAR('[[[42]]]', '$[0][0][0]')");
		Assert.Equal("42", rows[0].F[0].V?.ToString());
	}

	// =====================================================
	// BUG 4: DATETIME_DIFF with WEEK(MONDAY) falls through to default case
	// Same issue as DATE_DIFF
	// =====================================================
	[Fact]
	public void DatetimeDiff_WeekMonday_ShouldCountMondayBoundaries()
	{
		var executor = CreateExecutor();
		var (_, rows) = executor.Execute("SELECT DATETIME_DIFF(DATETIME '2024-01-08 10:00:00', DATETIME '2024-01-01 10:00:00', WEEK(MONDAY))");
		Assert.Equal("1", rows[0].F[0].V?.ToString());
	}

	// =====================================================
	// BUG 5: TIMESTAMP_DIFF with WEEK is not supported for TIMESTAMP type
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_diff
	//   "TIMESTAMP_DIFF does not support the WEEK date part when the argument is TIMESTAMP type"
	// =====================================================
	[Fact]
	public void TimestampDiff_Week_ShouldThrowError()
	{
		var executor = CreateExecutor();
		var ex = Assert.Throws<InvalidOperationException>(() =>
			executor.Execute("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-14 00:00:00 UTC', TIMESTAMP '2024-01-01 00:00:00 UTC', WEEK)"));
		Assert.Contains("TIMESTAMP_DIFF does not support the WEEK date part", ex.Message);
	}

	// =====================================================
	// BUG 6: POW with negative base and non-integer exponent should error
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#pow
	//   "Generates an error if X is a finite value less than 0 and Y is a noninteger."
	// =====================================================
	[Fact]
	public void Pow_NegativeBase_NonIntegerExponent_ShouldThrow()
	{
		var executor = CreateExecutor();
		Assert.ThrowsAny<Exception>(() => executor.Execute("SELECT POW(-1, 0.5)"));
	}

	// =====================================================
	// BUG 7: EXTRACT(WEEK(MONDAY) FROM date) - parser ordering issue
	// The EXTRACT rewrite must run AFTER WEEK(DOW) rewrite
	// =====================================================
	[Fact]
	public void Extract_WeekMonday_FromDate()
	{
		// EXTRACT(WEEK(MONDAY) FROM DATE '2024-01-08')
		// Jan 8 2024 is a Monday.
		// Weeks starting Monday: Jan 1 is the first Monday.
		// Jan 8 is the start of week 2 (Jan 1=week1, Jan 8=week2)
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#extract
		var executor = CreateExecutor();
		var (_, rows) = executor.Execute("SELECT EXTRACT(WEEK(MONDAY) FROM DATE '2024-01-08')");
		Assert.Equal("2", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Extract_WeekSunday_FromDate()
	{
		// Jan 7 2024 is a Sunday.
		// First Sunday in 2024 = Jan 7. Prior days are week 0.
		// Jan 7 = start of week 1
		var executor = CreateExecutor();
		var (_, rows) = executor.Execute("SELECT EXTRACT(WEEK(SUNDAY) FROM DATE '2024-01-07')");
		Assert.Equal("1", rows[0].F[0].V?.ToString());
	}

	// =====================================================
	// Regression check: POW(-2, 2) should work (integer exponent)
	// =====================================================
	[Fact]
	public void Pow_NegativeBase_IntegerExponent_ShouldWork()
	{
		var executor = CreateExecutor();
		var (_, rows) = executor.Execute("SELECT POW(-2, 2)");
		Assert.Equal("4", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Pow_NegativeBase_NegativeIntegerExponent_ShouldWork()
	{
		var executor = CreateExecutor();
		var (_, rows) = executor.Execute("SELECT POW(-2, -2)");
		Assert.Equal("0.25", rows[0].F[0].V?.ToString());
	}
}
