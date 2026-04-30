using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Deep tests for ARRAY functions and operations.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ArrayFunctionBoundaryTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public ArrayFunctionBoundaryTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- ARRAY_LENGTH ----
	[Fact] public async Task ArrayLength_Three() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH([1, 2, 3])"));
	[Fact] public async Task ArrayLength_One() => Assert.Equal("1", await Scalar("SELECT ARRAY_LENGTH([42])"));
	[Fact] public async Task ArrayLength_Empty() => Assert.Equal("0", await Scalar("SELECT ARRAY_LENGTH([])"));
	[Fact] public async Task ArrayLength_Strings() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(['a', 'b', 'c'])"));
	[Fact] public async Task ArrayLength_Null() => Assert.Null(await Scalar("SELECT ARRAY_LENGTH(CAST(NULL AS ARRAY<INT64>))"));
	[Fact] public async Task ArrayLength_Five() => Assert.Equal("5", await Scalar("SELECT ARRAY_LENGTH([1,2,3,4,5])"));

	// ---- ARRAY_TO_STRING ----
	[Fact] public async Task ArrayToString_Comma() => Assert.Equal("1,2,3", await Scalar("SELECT ARRAY_TO_STRING([1, 2, 3], ',')"));
	[Fact] public async Task ArrayToString_Dash() => Assert.Equal("a-b-c", await Scalar("SELECT ARRAY_TO_STRING(['a', 'b', 'c'], '-')"));
	[Fact] public async Task ArrayToString_Space() => Assert.Equal("hello world", await Scalar("SELECT ARRAY_TO_STRING(['hello', 'world'], ' ')"));
	[Fact] public async Task ArrayToString_Empty() => Assert.Equal("", await Scalar("SELECT ARRAY_TO_STRING(CAST([] AS ARRAY<STRING>), ',')"));
	[Fact] public async Task ArrayToString_Single() => Assert.Equal("one", await Scalar("SELECT ARRAY_TO_STRING(['one'], ',')"));
	[Fact] public async Task ArrayToString_Pipe() => Assert.Equal("x|y|z", await Scalar("SELECT ARRAY_TO_STRING(['x', 'y', 'z'], '|')"));

	// ---- ARRAY_REVERSE ----
	[Fact] public async Task ArrayReverse_Ints() { var v = await Scalar("SELECT ARRAY_TO_STRING(ARRAY_REVERSE([1, 2, 3]), ',')"); Assert.Equal("3,2,1", v); }
	[Fact] public async Task ArrayReverse_Strings() { var v = await Scalar("SELECT ARRAY_TO_STRING(ARRAY_REVERSE(['a', 'b', 'c']), ',')"); Assert.Equal("c,b,a", v); }
	[Fact] public async Task ArrayReverse_Single() { var v = await Scalar("SELECT ARRAY_TO_STRING(ARRAY_REVERSE([42]), ',')"); Assert.Equal("42", v); }
	[Fact] public async Task ArrayReverse_Empty() { var v = await Scalar("SELECT ARRAY_TO_STRING(ARRAY_REVERSE(CAST([] AS ARRAY<INT64>)), ',')"); Assert.Equal("", v); }

	// ---- ARRAY_CONCAT ----
	[Fact] public async Task ArrayConcat_TwoArrays() { var v = await Scalar("SELECT ARRAY_TO_STRING(ARRAY_CONCAT([1, 2], [3, 4]), ',')"); Assert.Equal("1,2,3,4", v); }
	[Fact] public async Task ArrayConcat_ThreeArrays() { var v = await Scalar("SELECT ARRAY_TO_STRING(ARRAY_CONCAT([1], [2], [3]), ',')"); Assert.Equal("1,2,3", v); }
	[Fact] public async Task ArrayConcat_EmptyFirst() { var v = await Scalar("SELECT ARRAY_TO_STRING(ARRAY_CONCAT(CAST([] AS ARRAY<INT64>), [1, 2]), ',')"); Assert.Equal("1,2", v); }
	[Fact] public async Task ArrayConcat_Strings() { var v = await Scalar("SELECT ARRAY_TO_STRING(ARRAY_CONCAT(['a', 'b'], ['c']), ',')"); Assert.Equal("a,b,c", v); }

	// ---- GENERATE_ARRAY ----
	[Fact] public async Task GenerateArray_Basic() { var v = await Scalar("SELECT ARRAY_TO_STRING(GENERATE_ARRAY(1, 5), ',')"); Assert.Equal("1,2,3,4,5", v); }
	[Fact] public async Task GenerateArray_Step2() { var v = await Scalar("SELECT ARRAY_TO_STRING(GENERATE_ARRAY(1, 10, 2), ',')"); Assert.Equal("1,3,5,7,9", v); }
	[Fact] public async Task GenerateArray_Step3() { var v = await Scalar("SELECT ARRAY_TO_STRING(GENERATE_ARRAY(0, 9, 3), ',')"); Assert.Equal("0,3,6,9", v); }
	[Fact] public async Task GenerateArray_Single() { var v = await Scalar("SELECT ARRAY_TO_STRING(GENERATE_ARRAY(5, 5), ',')"); Assert.Equal("5", v); }
	[Fact] public async Task GenerateArray_Negative() { var v = await Scalar("SELECT ARRAY_TO_STRING(GENERATE_ARRAY(-3, 3), ',')"); Assert.Equal("-3,-2,-1,0,1,2,3", v); }
	[Fact] public async Task GenerateArray_Large() { var v = await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(1, 100))"); Assert.Equal("100", v); }

	// ---- GENERATE_DATE_ARRAY ----
	[Fact] public async Task GenerateDateArray_Days() { var v = await Scalar("SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-10'))"); Assert.Equal("10", v); }
	[Fact] public async Task GenerateDateArray_Months() { var v = await Scalar("SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-06-01', INTERVAL 1 MONTH))"); Assert.Equal("6", v); }
	[Fact] public async Task GenerateDateArray_Weeks() { var v = await Scalar("SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-02-01', INTERVAL 1 WEEK))"); Assert.NotNull(v); var n = int.Parse(v!); Assert.True(n > 0); }

	// ---- Array subscript operator ----
	[Fact] public async Task ArraySubscript_First() => Assert.Equal("10", await Scalar("SELECT [10, 20, 30][OFFSET(0)]"));
	[Fact] public async Task ArraySubscript_Last() => Assert.Equal("30", await Scalar("SELECT [10, 20, 30][OFFSET(2)]"));
	[Fact] public async Task ArraySubscript_Ordinal1() => Assert.Equal("10", await Scalar("SELECT [10, 20, 30][ORDINAL(1)]"));
	[Fact] public async Task ArraySubscript_Ordinal3() => Assert.Equal("30", await Scalar("SELECT [10, 20, 30][ORDINAL(3)]"));
	[Fact] public async Task ArraySubscript_SafeOffset() => Assert.Null(await Scalar("SELECT [10, 20, 30][SAFE_OFFSET(5)]"));
	[Fact] public async Task ArraySubscript_SafeOrdinal() => Assert.Null(await Scalar("SELECT [10, 20, 30][SAFE_ORDINAL(5)]"));

	// ---- Array in expressions ----
	[Fact] public async Task Array_InSelect() { var v = await Scalar("SELECT ARRAY_LENGTH([1, 2, 3]) + ARRAY_LENGTH([4, 5])"); Assert.Equal("5", v); }
	[Fact] public async Task Array_NestedConcat() { var v = await Scalar("SELECT ARRAY_LENGTH(ARRAY_CONCAT([1], [2], [3, 4]))"); Assert.Equal("4", v); }
	[Fact] public async Task Array_ConcatReverse() { var v = await Scalar("SELECT ARRAY_TO_STRING(ARRAY_REVERSE(ARRAY_CONCAT([1, 2], [3, 4])), ',')"); Assert.Equal("4,3,2,1", v); }
	[Fact] public async Task Array_GenerateLength() { var v = await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(1, 50))"); Assert.Equal("50", v); }
	[Fact] public async Task Array_LiteralStrings() { var v = await Scalar("SELECT ARRAY_TO_STRING(['hello', 'world'], ' ')"); Assert.Equal("hello world", v); }

	// ---- ARRAY with subquery (inline) ----
	[Fact] public async Task Array_SubqueryGenerate() { var v = await Scalar("SELECT ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST(GENERATE_ARRAY(1,10)) AS x))"); Assert.Equal("10", v); }
	[Fact] public async Task Array_SubqueryFiltered() { var v = await Scalar("SELECT ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST([1,2,3,4,5]) AS x WHERE x > 2))"); Assert.Equal("3", v); }
}
