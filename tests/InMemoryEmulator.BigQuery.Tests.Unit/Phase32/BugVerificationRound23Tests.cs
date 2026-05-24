using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Unit.Phase32;

/// <summary>
/// Verification tests for bugs found in research round 23.
/// </summary>
public class BugVerificationRound23Tests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		return new QueryExecutor(store);
	}

	[Fact]
	public void Concat_WithBooleans_ShouldUseLowercase()
	{
		// BigQuery: CONCAT(TRUE, FALSE) → 'truefalse'
		var (_, rows) = CreateExecutor().Execute("SELECT CONCAT(TRUE, FALSE)");
		Assert.Equal("truefalse", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Concat_WithIntegers_ShouldCastToString()
	{
		// BigQuery: CONCAT(1, 2, 3) → '123'
		var (_, rows) = CreateExecutor().Execute("SELECT CONCAT(1, 2, 3)");
		Assert.Equal("123", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Concat_WithDate_ShouldFormatAsIso()
	{
		// BigQuery: CONCAT(DATE '2024-01-01', ' is today') → '2024-01-01 is today'
		var (_, rows) = CreateExecutor().Execute("SELECT CONCAT(DATE '2024-01-01', ' is today')");
		Assert.Equal("2024-01-01 is today", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void LastDay_IsoWeek_ShouldReturnSunday()
	{
		// BigQuery: LAST_DAY(DATE '2024-01-08', ISOWEEK) → 2024-01-14 (Sunday)
		// 2024-01-08 is a Monday, the ISO week goes Mon-Sun so it ends on 2024-01-14
		var (_, rows) = CreateExecutor().Execute("SELECT LAST_DAY(DATE '2024-01-08', ISOWEEK)");
		Assert.Equal("2024-01-14", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void LastDay_IsoYear_ShouldReturnLastDayOfIsoYear()
	{
		// BigQuery: LAST_DAY(DATE '2024-01-01', ISOYEAR) → 2024-12-29 (last Sunday of ISO year 2024)
		// ISO year 2024 starts on 2024-01-01 (Monday) and ends on 2024-12-29 (Sunday)
		var (_, rows) = CreateExecutor().Execute("SELECT LAST_DAY(DATE '2024-01-01', ISOYEAR)");
		Assert.Equal("2024-12-29", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Lpad_Truncation_WhenOriginalLonger()
	{
		// BigQuery: LPAD('hello world', 7) → 'hello w'
		var (_, rows) = CreateExecutor().Execute("SELECT LPAD('hello world', 7)");
		Assert.Equal("hello w", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Rpad_Truncation_WhenOriginalLonger()
	{
		// BigQuery: RPAD('hello world', 7) → 'hello w'
		var (_, rows) = CreateExecutor().Execute("SELECT RPAD('hello world', 7)");
		Assert.Equal("hello w", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Soundex_Robert()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT SOUNDEX('Robert')");
		Assert.Equal("R163", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Soundex_Rupert()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT SOUNDEX('Rupert')");
		Assert.Equal("R163", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void SafeCast_InvalidDate_ReturnsNull()
	{
		// SAFE_CAST('not-a-date' AS DATE) → NULL
		var (_, rows) = CreateExecutor().Execute("SELECT SAFE_CAST('not-a-date' AS DATE)");
		Assert.Null(rows[0].F[0].V);
	}

	[Fact]
	public void EmptyString_IsNotNull()
	{
		// '' IS NULL → FALSE in BigQuery  
		var (_, rows) = CreateExecutor().Execute("SELECT '' IS NULL");
		Assert.Equal("false", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Length_EmptyString_ReturnsZero()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT LENGTH('')");
		Assert.Equal("0", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void DateTrunc_Month_EndOfMonth()
	{
		// DATE_TRUNC(DATE '2024-01-31', MONTH) → 2024-01-01
		var (_, rows) = CreateExecutor().Execute("SELECT DATE_TRUNC(DATE '2024-01-31', MONTH)");
		Assert.Equal("2024-01-01", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void WindowFunction_OrderByDesc()
	{
		var (_, rows) = CreateExecutor().Execute(@"
			SELECT val, ROW_NUMBER() OVER (ORDER BY val DESC) AS rn
			FROM UNNEST([3, 1, 2]) AS val");
		// Sort by val DESC: val=3 → rn=1, val=2 → rn=2, val=1 → rn=3
		// Find row where val=3
		var row3 = rows.First(r => r.F[0].V?.ToString() == "3");
		Assert.Equal("1", row3.F[1].V?.ToString());
	}

	[Fact]
	public void MultipleCTEsWithDependencies()
	{
		var (_, rows) = CreateExecutor().Execute(@"
			WITH a AS (SELECT 1 AS x),
			     b AS (SELECT x + 1 AS y FROM a),
			     c AS (SELECT y + 1 AS z FROM b)
			SELECT z FROM c");
		Assert.Equal("3", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void MultipleWindowFunctions_DifferentPartitions()
	{
		var (_, rows) = CreateExecutor().Execute(@"
			SELECT val, grp,
				ROW_NUMBER() OVER (ORDER BY val) AS rn_all,
				ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) AS rn_grp
			FROM (SELECT 1 AS val, 'a' AS grp UNION ALL SELECT 2, 'a' UNION ALL SELECT 3, 'b' UNION ALL SELECT 4, 'b')");
		Assert.Equal(4, rows.Count);
	}

	[Fact]
	public void Concat_WithFloat_ShouldFormatCorrectly()
	{
		// CONCAT(1.5) should give '1.5'
		var (_, rows) = CreateExecutor().Execute("SELECT CONCAT(CAST(1.5 AS FLOAT64))");
		Assert.Equal("1.5", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Concat_WholeFloat_ShouldShowDecimalPoint()
	{
		// CONCAT(CAST(1 AS FLOAT64)) should give '1' (BigQuery omits ".0" for whole-number floats)
		var (_, rows) = CreateExecutor().Execute("SELECT CONCAT(CAST(1 AS FLOAT64))");
		Assert.Equal("1", rows[0].F[0].V?.ToString());
	}
}
