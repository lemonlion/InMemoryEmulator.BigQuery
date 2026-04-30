using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for complex expressions combining multiple functions and operators.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ComplexExpressionTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public ComplexExpressionTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- String + Math combinations ----
	[Fact] public async Task LengthTimesTwo() => Assert.Equal("10", await Scalar("SELECT LENGTH('hello') * 2"));
	[Fact] public async Task ConcatAndLength() => Assert.Equal("10", await Scalar("SELECT LENGTH(CONCAT('hello', 'world'))"));
	[Fact] public async Task UpperOfSubstr() => Assert.Equal("HEL", await Scalar("SELECT UPPER(SUBSTR('hello', 1, 3))"));
	[Fact] public async Task ReverseOfUpper() => Assert.Equal("OLLEH", await Scalar("SELECT REVERSE(UPPER('hello'))"));
	[Fact] public async Task RepeatAndLength() => Assert.Equal("15", await Scalar("SELECT LENGTH(REPEAT('hello', 3))"));
	[Fact] public async Task StrposOfConcat() => Assert.Equal("6", await Scalar("SELECT STRPOS(CONCAT('hello', 'world'), 'world')"));
	[Fact] public async Task ConcatOfCast() => Assert.Equal("value: 42", await Scalar("SELECT CONCAT('value: ', CAST(42 AS STRING))"));
	[Fact] public async Task LengthOfReplace() => Assert.Equal("5", await Scalar("SELECT LENGTH(REPLACE('hello', 'l', 'L'))"));
	[Fact] public async Task TrimAndUpper() => Assert.Equal("HELLO", await Scalar("SELECT UPPER(TRIM('  hello  '))"));
	[Fact] public async Task SubstrAndReverse() => Assert.Equal("leh", await Scalar("SELECT REVERSE(SUBSTR('hello', 1, 3))"));

	// ---- Math nesting ----
	[Fact] public async Task AbsSqrt() => Assert.Equal("5", await Scalar("SELECT CAST(SQRT(ABS(-25)) AS INT64)"));
	[Fact] public async Task RoundDivide() => Assert.Equal("3", await Scalar("SELECT ROUND(10.0 / 3.0)"));
	[Fact] public async Task CeilMod() => Assert.Equal("3", await Scalar("SELECT CAST(CEIL(MOD(5.5, 3.0)) AS INT64)"));
	[Fact] public async Task FloorPow() => Assert.Equal("9", await Scalar("SELECT CAST(FLOOR(POW(2.1, 3)) AS INT64)"));
	[Fact] public async Task SignAbs() => Assert.Equal("1", await Scalar("SELECT SIGN(ABS(-42))"));
	[Fact] public async Task ModDiv() => Assert.Equal("1", await Scalar("SELECT MOD(DIV(10, 3), 2)"));
	[Fact] public async Task GreatestLeast() => Assert.Equal("3", await Scalar("SELECT GREATEST(1, 2, 3) - LEAST(1, 2, 3) + 1"));
	[Fact] public async Task PowMod() => Assert.Equal("1", await Scalar("SELECT CAST(MOD(CAST(POW(2, 10) AS INT64), 3) AS INT64)"));

	// ---- Conditional + String ----
	[Fact] public async Task CaseUpperLower() => Assert.Equal("HELLO", await Scalar("SELECT CASE WHEN TRUE THEN UPPER('hello') ELSE LOWER('WORLD') END"));
	[Fact] public async Task CoalesceConcat() => Assert.Equal("hello", await Scalar("SELECT COALESCE(CAST(NULL AS STRING), CONCAT('hel', 'lo'))"));
	[Fact] public async Task IfnullSubstr() => Assert.Equal("hel", await Scalar("SELECT IFNULL(CAST(NULL AS STRING), SUBSTR('hello', 1, 3))"));
	[Fact] public async Task CaseLength() => Assert.Equal("long", await Scalar("SELECT CASE WHEN LENGTH('hello world') > 5 THEN 'long' ELSE 'short' END"));

	// ---- Date + Math ----
	[Fact] public async Task ExtractTimesTwo() => Assert.Equal("4048", await Scalar("SELECT EXTRACT(YEAR FROM DATE '2024-01-15') * 2"));
	[Fact] public async Task DateDiffAbs() => Assert.Equal("5", await Scalar("SELECT ABS(DATE_DIFF(DATE '2024-01-10', DATE '2024-01-15', DAY))"));
	[Fact] public async Task ExtractMod() => Assert.Equal("1", await Scalar("SELECT MOD(EXTRACT(MONTH FROM DATE '2024-01-15'), 2)"));

	// ---- Aggregate + Expression ----
	[Fact] public async Task SumTimesTwo() => Assert.Equal("30", await Scalar("SELECT SUM(x) * 2 FROM UNNEST([1,2,3,4,5]) AS x"));
	[Fact] public async Task CountPlusOne() => Assert.Equal("6", await Scalar("SELECT COUNT(*) + 1 FROM UNNEST([1,2,3,4,5]) AS x"));
	[Fact] public async Task MaxMinusMin() => Assert.Equal("4", await Scalar("SELECT MAX(x) - MIN(x) FROM UNNEST([1,2,3,4,5]) AS x"));
	[Fact] public async Task AvgRound() => Assert.Equal("3", await Scalar("SELECT CAST(ROUND(AVG(x)) AS INT64) FROM UNNEST([1.0,2.0,3.0,4.0,5.0]) AS x"));
	[Fact] public async Task SumOfAbs() => Assert.Equal("6", await Scalar("SELECT SUM(ABS(x)) FROM UNNEST([-1,-2,-3]) AS x"));
	[Fact] public async Task MaxOfLength() => Assert.Equal("6", await Scalar("SELECT MAX(LENGTH(x)) FROM UNNEST(['a','bb','ccc','dddddd']) AS x"));

	// ---- Complex boolean expressions ----
	[Fact] public async Task BoolComplex1() => Assert.Equal("True", await Scalar("SELECT (1 > 0) AND (2 < 3) AND ('a' = 'a')"));
	[Fact] public async Task BoolComplex2() => Assert.Equal("True", await Scalar("SELECT (1 > 10) OR (2 < 3)"));
	[Fact] public async Task BoolComplex3() => Assert.Equal("True", await Scalar("SELECT NOT (1 > 10) AND (2 < 3)"));
	[Fact] public async Task BoolComplex4() => Assert.Equal("True", await Scalar("SELECT (5 BETWEEN 1 AND 10) AND (3 IN (1, 2, 3))"));
	[Fact] public async Task BoolComplex5() => Assert.Equal("True", await Scalar("SELECT ('hello' LIKE 'hel%') AND (LENGTH('hello') = 5)"));

	// ---- Complex CASE with multiple functions ----
	[Fact]
	public async Task ComplexCase()
	{
		var v = await Scalar(@"
SELECT CASE
  WHEN LENGTH('hello') > 10 THEN UPPER('short')
  WHEN LENGTH('hello') > 3 THEN CONCAT(UPPER(SUBSTR('hello', 1, 1)), LOWER(SUBSTR('hello', 2)))
  ELSE 'tiny'
END");
		Assert.Equal("Hello", v);
	}

	// ---- Arithmetic with CASE ----
	[Fact]
	public async Task ArithWithCase() => Assert.Equal("24", await Scalar(@"
SELECT SUM(CASE WHEN x > 3 THEN x * 2 ELSE x END) FROM UNNEST([1,2,3,4,5]) AS x"));

	// ---- Nested function composition ----
	[Fact] public async Task DeepNest1() => Assert.Equal("3", await Scalar("SELECT LENGTH(UPPER(TRIM(REVERSE('  abc  '))))"));
	[Fact] public async Task DeepNest2() => Assert.Equal("5", await Scalar("SELECT CAST(SQRT(ABS(SIGN(-1) * -25)) AS INT64)"));
	[Fact] public async Task DeepNest3() => Assert.Equal("2", await Scalar("SELECT ABS(MOD(CAST(CEIL(-7.5) AS INT64), 5))"));
	[Fact] public async Task DeepNest4() => Assert.Equal("ABC", await Scalar("SELECT UPPER(SUBSTR(REPEAT('abc', 3), 1, 3))"));
	[Fact] public async Task DeepNest5() => Assert.Equal("42", await Scalar("SELECT CAST(ROUND(SQRT(POW(42, 2))) AS INT64)"));

	// ---- Multi-column expressions ----
	[Fact]
	public async Task MultiColExpr()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
SELECT
  x,
  x * x AS squared,
  CAST(SQRT(x) AS INT64) AS sqrt_floor,
  MOD(x, 3) AS mod3,
  CASE WHEN x > 5 THEN 'big' ELSE 'small' END AS size
FROM UNNEST([1,4,9,16,25]) AS x
ORDER BY x", parameters: null);
		var rows = result.ToList();
		Assert.Equal(5, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("1", rows[0][1]?.ToString()); // 1*1
		Assert.Equal("25", rows[4][0]?.ToString());
		Assert.Equal("625", rows[4][1]?.ToString()); // 25*25
		Assert.Equal("5", rows[4][2]?.ToString()); // sqrt(25)
		Assert.Equal("big", rows[4][4]?.ToString());
	}

	// ---- String building from parts ----
	[Fact]
	public async Task StringBuilder()
	{
		var v = await Scalar(@"
SELECT CONCAT(
  UPPER(SUBSTR('john doe', 1, 1)),
  LOWER(SUBSTR('john doe', 2, 3)),
  ' ',
  UPPER(SUBSTR('john doe', 6, 1)),
  LOWER(SUBSTR('john doe', 7))
)");
		Assert.Equal("John Doe", v);
	}

	// ---- Math formulas ----
	[Fact] public async Task Quadratic_Discriminant() => Assert.Equal("0", await Scalar("SELECT CAST(POW(2, 2) - 4 * 1 * 1 AS INT64)")); // b^2 - 4ac with a=1, b=2, c=1
	[Fact] public async Task Distance2D() => Assert.Equal("5", await Scalar("SELECT CAST(SQRT(POW(3, 2) + POW(4, 2)) AS INT64)")); // 3-4-5 triangle
	[Fact] public async Task CircleArea() => Assert.Equal("3", await Scalar("SELECT CAST(TRUNC(3.14159 * POW(1, 2)) AS INT64)")); // pi * r^2, r=1
}
