using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for array functions: ARRAY_LENGTH, ARRAY_REVERSE, ARRAY_TO_STRING,
/// ARRAY_CONCAT, GENERATE_ARRAY, GENERATE_DATE_ARRAY, UNNEST patterns.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ArrayFunctionDeepTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public ArrayFunctionDeepTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	private async Task<List<string?>> Column(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		return result.Select(r => r[0]?.ToString()).ToList();
	}

	// ---- ARRAY_LENGTH ----
	[Fact] public async Task ArrayLength_Empty() => Assert.Equal("0", await Scalar("SELECT ARRAY_LENGTH([])"));
	[Fact] public async Task ArrayLength_One() => Assert.Equal("1", await Scalar("SELECT ARRAY_LENGTH([1])"));
	[Fact] public async Task ArrayLength_Three() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH([1,2,3])"));
	[Fact] public async Task ArrayLength_Five() => Assert.Equal("5", await Scalar("SELECT ARRAY_LENGTH([1,2,3,4,5])"));
	[Fact] public async Task ArrayLength_Strings() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(['a','b','c'])"));
	[Fact] public async Task ArrayLength_Ten() => Assert.Equal("10", await Scalar("SELECT ARRAY_LENGTH([1,2,3,4,5,6,7,8,9,10])"));

	// ---- ARRAY_TO_STRING ----
	[Fact] public async Task ArrayToString_Comma() => Assert.Equal("a,b,c", await Scalar("SELECT ARRAY_TO_STRING(['a','b','c'], ',')"));
	[Fact] public async Task ArrayToString_Space() => Assert.Equal("hello world", await Scalar("SELECT ARRAY_TO_STRING(['hello','world'], ' ')"));
	[Fact] public async Task ArrayToString_Pipe() => Assert.Equal("x|y|z", await Scalar("SELECT ARRAY_TO_STRING(['x','y','z'], '|')"));
	[Fact(Skip = "Empty array literal CAST([] AS ARRAY<type>) not supported")] public async Task ArrayToString_Empty() => Assert.Equal("", await Scalar("SELECT ARRAY_TO_STRING(CAST([] AS ARRAY<STRING>), ',')"));
	[Fact] public async Task ArrayToString_Single() => Assert.Equal("one", await Scalar("SELECT ARRAY_TO_STRING(['one'], ',')"));
	[Fact] public async Task ArrayToString_Dash() => Assert.Equal("2024-01-15", await Scalar("SELECT ARRAY_TO_STRING(['2024','01','15'], '-')"));
	[Fact] public async Task ArrayToString_NoSep() => Assert.Equal("abc", await Scalar("SELECT ARRAY_TO_STRING(['a','b','c'], '')"));

	// ---- ARRAY_REVERSE ----
	[Fact] public async Task ArrayReverse_Basic()
	{
		var rows = await Column("SELECT x FROM UNNEST(ARRAY_REVERSE([1,2,3])) AS x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("3", rows[0]);
		Assert.Equal("2", rows[1]);
		Assert.Equal("1", rows[2]);
	}

	[Fact] public async Task ArrayReverse_Strings()
	{
		var rows = await Column("SELECT x FROM UNNEST(ARRAY_REVERSE(['a','b','c'])) AS x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("c", rows[0]);
		Assert.Equal("a", rows[2]);
	}

	[Fact] public async Task ArrayReverse_Single()
	{
		var rows = await Column("SELECT x FROM UNNEST(ARRAY_REVERSE([42])) AS x");
		Assert.Single(rows);
		Assert.Equal("42", rows[0]);
	}

	[Fact] public async Task ArrayReverse_Length() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(ARRAY_REVERSE([1,2,3]))"));

	// ---- ARRAY_CONCAT ----
	[Fact] public async Task ArrayConcat_TwoArrays() => Assert.Equal("4", await Scalar("SELECT ARRAY_LENGTH(ARRAY_CONCAT([1,2], [3,4]))"));
	[Fact] public async Task ArrayConcat_ThreeArrays() => Assert.Equal("6", await Scalar("SELECT ARRAY_LENGTH(ARRAY_CONCAT([1,2], [3,4], [5,6]))"));
	[Fact] public async Task ArrayConcat_Empty() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(ARRAY_CONCAT([], [1,2,3]))"));
	[Fact(Skip = "Empty array literal CAST([] AS ARRAY<type>) not supported")] public async Task ArrayConcat_BothEmpty() => Assert.Equal("0", await Scalar("SELECT ARRAY_LENGTH(ARRAY_CONCAT(CAST([] AS ARRAY<INT64>), CAST([] AS ARRAY<INT64>)))"));
	[Fact] public async Task ArrayConcat_Elements()
	{
		var rows = await Column("SELECT x FROM UNNEST(ARRAY_CONCAT([1,2], [3,4])) AS x");
		Assert.Equal(4, rows.Count);
		Assert.Equal("1", rows[0]);
		Assert.Equal("4", rows[3]);
	}

	// ---- GENERATE_ARRAY ----
	[Fact] public async Task GenerateArray_Basic() => Assert.Equal("5", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(1, 5))"));
	[Fact] public async Task GenerateArray_Step2() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(1, 5, 2))"));
	[Fact] public async Task GenerateArray_OneElem() => Assert.Equal("1", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(1, 1))"));
	[Fact] public async Task GenerateArray_Negative() => Assert.Equal("5", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(-2, 2))"));
	[Fact] public async Task GenerateArray_Elements()
	{
		var rows = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x");
		Assert.Equal(5, rows.Count);
		Assert.Equal("1", rows[0]);
		Assert.Equal("5", rows[4]);
	}
	[Fact] public async Task GenerateArray_Step3()
	{
		var rows = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(0, 12, 3)) AS x");
		Assert.Equal(5, rows.Count);
		Assert.Equal("0", rows[0]);
		Assert.Equal("3", rows[1]);
		Assert.Equal("12", rows[4]);
	}
	[Fact] public async Task GenerateArray_Large() => Assert.Equal("101", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(0, 100))"));

	// ---- UNNEST ----
	[Fact]
	public async Task Unnest_Ints()
	{
		var rows = await Column("SELECT x FROM UNNEST([10,20,30]) AS x ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("10", rows[0]);
		Assert.Equal("30", rows[2]);
	}

	[Fact]
	public async Task Unnest_Strings()
	{
		var rows = await Column("SELECT x FROM UNNEST(['foo','bar','baz']) AS x ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("bar", rows[0]);
		Assert.Equal("foo", rows[2]);
	}

	[Fact]
	public async Task Unnest_WithFilter()
	{
		var rows = await Column("SELECT x FROM UNNEST([1,2,3,4,5]) AS x WHERE x > 3 ORDER BY x");
		Assert.Equal(2, rows.Count);
		Assert.Equal("4", rows[0]);
		Assert.Equal("5", rows[1]);
	}

	[Fact(Skip = "UNNEST WITH OFFSET not supported")]
	public async Task Unnest_WithOffset()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT x, off FROM UNNEST(['a','b','c']) AS x WITH OFFSET AS off ORDER BY off",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(3, rows.Count);
		Assert.Equal("a", rows[0][0]?.ToString());
		Assert.Equal("0", rows[0][1]?.ToString());
		Assert.Equal("c", rows[2][0]?.ToString());
		Assert.Equal("2", rows[2][1]?.ToString());
	}

	// ---- ARRAY in subquery ----
	[Fact(Skip = "ARRAY subquery not supported")] public async Task Array_Subquery() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST([1,2,3]) AS x))"));
	[Fact(Skip = "ARRAY subquery not supported")] public async Task Array_SubqueryFiltered() => Assert.Equal("2", await Scalar("SELECT ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST([1,2,3,4,5]) AS x WHERE x > 3))"));

	// ---- ARRAY_AGG with UNNEST ----
	[Fact]
	public async Task ArrayAgg_FromUnnest() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(ARRAY_AGG(x)) FROM UNNEST([10,20,30]) AS x"));

	// ---- Nested array operations ----
	[Fact] public async Task Nested_ConcatReverse()
	{
		var rows = await Column("SELECT x FROM UNNEST(ARRAY_REVERSE(ARRAY_CONCAT([1,2], [3,4]))) AS x");
		Assert.Equal(4, rows.Count);
		Assert.Equal("4", rows[0]);
		Assert.Equal("1", rows[3]);
	}

	[Fact] public async Task Nested_LengthOfConcat() => Assert.Equal("6", await Scalar("SELECT ARRAY_LENGTH(ARRAY_CONCAT([1,2,3], [4,5,6]))"));

	// ---- GENERATE_DATE_ARRAY ----
	[Fact] public async Task GenerateDateArray_Days() => Assert.Equal("8", await Scalar("SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-08'))"));
	[Fact] public async Task GenerateDateArray_Week() => Assert.Equal("5", await Scalar("SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-29', INTERVAL 7 DAY))"));
	[Fact] public async Task GenerateDateArray_Month() => Assert.Equal("12", await Scalar("SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-12-01', INTERVAL 1 MONTH))"));
}
