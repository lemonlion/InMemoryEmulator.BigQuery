using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for array functions: ARRAY_FIRST, ARRAY_LAST, ARRAY_FILTER,
/// ARRAY_TRANSFORM, ARRAY_INCLUDES, ARRAY_IS_DISTINCT, ARRAY_MAX/MIN/SUM/AVG, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ArrayFunctionAdvancedTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public ArrayFunctionAdvancedTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_arr_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	private async Task<List<BigQueryRow>> Q(string sql)
	{
		var client = await _fixture.GetClientAsync();
		return (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
	}

	// ---- ARRAY_FIRST ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_first
	[Fact] public async Task ArrayFirst_Integers() => Assert.Equal("10", await S("SELECT ARRAY_FIRST([10, 20, 30])"));
	[Fact] public async Task ArrayFirst_Strings() => Assert.Equal("a", await S("SELECT ARRAY_FIRST(['a', 'b', 'c'])"));
	[Fact] public async Task ArrayFirst_Single() => Assert.Equal("42", await S("SELECT ARRAY_FIRST([42])"));

	// ---- ARRAY_LAST ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_last
	[Fact] public async Task ArrayLast_Integers() => Assert.Equal("30", await S("SELECT ARRAY_LAST([10, 20, 30])"));
	[Fact] public async Task ArrayLast_Strings() => Assert.Equal("c", await S("SELECT ARRAY_LAST(['a', 'b', 'c'])"));
	[Fact] public async Task ArrayLast_Single() => Assert.Equal("42", await S("SELECT ARRAY_LAST([42])"));

	// ---- ARRAY_INCLUDES ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_includes
	[Fact] public async Task ArrayIncludes_Found() => Assert.Equal("True", await S("SELECT ARRAY_INCLUDES([1, 2, 3], 2)"));
	[Fact] public async Task ArrayIncludes_NotFound() => Assert.Equal("False", await S("SELECT ARRAY_INCLUDES([1, 2, 3], 4)"));
	[Fact] public async Task ArrayIncludes_String() => Assert.Equal("True", await S("SELECT ARRAY_INCLUDES(['a','b','c'], 'b')"));

	// ---- ARRAY_INCLUDES_ALL ----
	[Fact] public async Task ArrayIncludesAll_True() => Assert.Equal("True", await S("SELECT ARRAY_INCLUDES_ALL([1,2,3,4], [1,3])"));
	[Fact] public async Task ArrayIncludesAll_False() => Assert.Equal("False", await S("SELECT ARRAY_INCLUDES_ALL([1,2,3], [1,4])"));

	// ---- ARRAY_INCLUDES_ANY ----
	[Fact] public async Task ArrayIncludesAny_True() => Assert.Equal("True", await S("SELECT ARRAY_INCLUDES_ANY([1,2,3], [3,4,5])"));
	[Fact] public async Task ArrayIncludesAny_False() => Assert.Equal("False", await S("SELECT ARRAY_INCLUDES_ANY([1,2,3], [4,5,6])"));

	// ---- ARRAY_MAX ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_max
	[Fact] public async Task ArrayMax_Integers() => Assert.Equal("30", await S("SELECT ARRAY_MAX([10, 30, 20])"));
	[Fact] public async Task ArrayMax_WithNulls() => Assert.Equal("30", await S("SELECT ARRAY_MAX([10, NULL, 30, 20])"));
	[Fact] public async Task ArrayMax_Single() => Assert.Equal("5", await S("SELECT ARRAY_MAX([5])"));

	// ---- ARRAY_MIN ----
	[Fact] public async Task ArrayMin_Integers() => Assert.Equal("10", await S("SELECT ARRAY_MIN([10, 30, 20])"));
	[Fact] public async Task ArrayMin_WithNulls() => Assert.Equal("10", await S("SELECT ARRAY_MIN([10, NULL, 30, 20])"));

	// ---- ARRAY_SUM ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_sum
	[Fact] public async Task ArraySum_Integers() => Assert.Equal("60", await S("SELECT ARRAY_SUM([10, 20, 30])"));
	[Fact] public async Task ArraySum_WithNulls() => Assert.Equal("60", await S("SELECT ARRAY_SUM([10, NULL, 20, 30])"));
	[Fact] public async Task ArraySum_Empty() => Assert.Null(await S("SELECT ARRAY_SUM(CAST([] AS ARRAY<INT64>))"));

	// ---- ARRAY_AVG ----
	[Fact] public async Task ArrayAvg_Integers()
	{
		var v = await S("SELECT ARRAY_AVG([10, 20, 30])");
		Assert.NotNull(v);
		Assert.Equal("20", v?.Split('.')[0]); // should be 20.0
	}
	[Fact] public async Task ArrayAvg_WithNulls()
	{
		var v = await S("SELECT ARRAY_AVG([10, NULL, 20, 30])");
		Assert.NotNull(v);
		Assert.Equal("20", v?.Split('.')[0]);
	}

	// ---- ARRAY_IS_DISTINCT ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_is_distinct
	[Fact] public async Task ArrayIsDistinct_True() => Assert.Equal("True", await S("SELECT ARRAY_IS_DISTINCT([1, 2, 3])"));
	[Fact] public async Task ArrayIsDistinct_False()
	{
		var v = await S("SELECT ARRAY_IS_DISTINCT([1, 2, 2, 3])");
		Assert.Equal("False", v);
	}

	// ---- ARRAY_FILTER (lambda) ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_filter
	[Fact] public async Task ArrayFilter_GreaterThan()
	{
		var v = await S("SELECT ARRAY_LENGTH(ARRAY_FILTER([1,2,3,4,5], e -> e > 3))");
		Assert.Equal("2", v);
	}
	[Fact] public async Task ArrayFilter_EvenNumbers()
	{
		var v = await S("SELECT ARRAY_LENGTH(ARRAY_FILTER([1,2,3,4,5,6], e -> MOD(e, 2) = 0))");
		Assert.Equal("3", v);
	}
	[Fact] public async Task ArrayFilter_None()
	{
		var v = await S("SELECT ARRAY_LENGTH(ARRAY_FILTER([1,2,3], e -> e > 10))");
		Assert.Equal("0", v);
	}

	// ---- ARRAY_TRANSFORM (lambda) ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_transform
	[Fact] public async Task ArrayTransform_Double()
	{
		var v = await S("SELECT ARRAY_TO_STRING(ARRAY_TRANSFORM([1,2,3], e -> e * 2), ',')");
		Assert.Equal("2,4,6", v);
	}
	[Fact] public async Task ArrayTransform_ToString()
	{
		var v = await S("SELECT ARRAY_TO_STRING(ARRAY_TRANSFORM([1,2,3], e -> CAST(e AS STRING)), ',')");
		Assert.Equal("1,2,3", v);
	}

	// ---- ARRAY_TO_STRING ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_to_string
	[Fact] public async Task ArrayToString_Basic() => Assert.Equal("a,b,c", await S("SELECT ARRAY_TO_STRING(['a', 'b', 'c'], ',')"));
	[Fact] public async Task ArrayToString_SpaceDelim() => Assert.Equal("a b c", await S("SELECT ARRAY_TO_STRING(['a', 'b', 'c'], ' ')"));
	[Fact] public async Task ArrayToString_Empty() => Assert.Equal("", await S("SELECT ARRAY_TO_STRING(CAST([] AS ARRAY<STRING>), ',')"));
	[Fact] public async Task ArrayToString_Single() => Assert.Equal("hello", await S("SELECT ARRAY_TO_STRING(['hello'], ',')"));
	[Fact] public async Task ArrayToString_WithNull_NoReplace() => Assert.Equal("a,c", await S("SELECT ARRAY_TO_STRING(['a', NULL, 'c'], ',')"));
	[Fact] public async Task ArrayToString_WithNull_Replace() => Assert.Equal("a,NULL,c", await S("SELECT ARRAY_TO_STRING(['a', NULL, 'c'], ',', 'NULL')"));

	// ---- ARRAY_CONCAT ----
	[Fact] public async Task ArrayConcat_Two()
	{
		var v = await S("SELECT ARRAY_TO_STRING(ARRAY_CONCAT([1,2], [3,4]), ',')");
		Assert.Equal("1,2,3,4", v);
	}
	[Fact] public async Task ArrayConcat_Three()
	{
		var v = await S("SELECT ARRAY_TO_STRING(ARRAY_CONCAT([1], [2], [3]), ',')");
		Assert.Equal("1,2,3", v);
	}

	// ---- ARRAY_REVERSE ----
	[Fact] public async Task ArrayReverse_Basic()
	{
		var v = await S("SELECT ARRAY_TO_STRING(ARRAY_REVERSE([1,2,3]), ',')");
		Assert.Equal("3,2,1", v);
	}
	[Fact] public async Task ArrayReverse_Single()
	{
		var v = await S("SELECT ARRAY_TO_STRING(ARRAY_REVERSE([42]), ',')");
		Assert.Equal("42", v);
	}

	// ---- ARRAY_SLICE ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_slice
	[Fact] public async Task ArraySlice_Basic()
	{
		var v = await S("SELECT ARRAY_TO_STRING(ARRAY_SLICE([1,2,3,4,5], 1, 3), ',')");
		Assert.Equal("2,3,4", v);
	}
	[Fact] public async Task ArraySlice_FromStart()
	{
		var v = await S("SELECT ARRAY_TO_STRING(ARRAY_SLICE([1,2,3,4,5], 0, 2), ',')");
		Assert.Equal("1,2,3", v);
	}

	// ---- ARRAY_LENGTH ----
	[Fact] public async Task ArrayLength_Basic() => Assert.Equal("3", await S("SELECT ARRAY_LENGTH([1,2,3])"));
	[Fact] public async Task ArrayLength_Empty() => Assert.Equal("0", await S("SELECT ARRAY_LENGTH(CAST([] AS ARRAY<INT64>))"));
	[Fact] public async Task ArrayLength_Null() => Assert.Null(await S("SELECT ARRAY_LENGTH(NULL)"));

	// ---- GENERATE_ARRAY ----
	[Fact] public async Task GenerateArray_Step2()
	{
		var v = await S("SELECT ARRAY_TO_STRING(GENERATE_ARRAY(1, 10, 2), ',')");
		Assert.Equal("1,3,5,7,9", v);
	}
	[Fact] public async Task GenerateArray_Reverse()
	{
		var v = await S("SELECT ARRAY_TO_STRING(GENERATE_ARRAY(5, 1, -1), ',')");
		Assert.Equal("5,4,3,2,1", v);
	}
	[Fact] public async Task GenerateArray_Single()
	{
		var v = await S("SELECT ARRAY_TO_STRING(GENERATE_ARRAY(1, 1), ',')");
		Assert.Equal("1", v);
	}

	// ---- UNNEST patterns ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unnest_operator
	[Fact] public async Task Unnest_WithOrdinality()
	{
		var rows = await Q("SELECT val, off FROM UNNEST(['a','b','c']) AS val WITH OFFSET AS off ORDER BY off");
		Assert.Equal(3, rows.Count);
		Assert.Equal("a", rows[0]["val"]?.ToString());
		Assert.Equal("0", rows[0]["off"]?.ToString());
	}

	[Fact] public async Task Unnest_Integers()
	{
		var rows = await Q("SELECT x FROM UNNEST([10, 20, 30]) AS x ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("10", rows[0]["x"]?.ToString());
	}

	[Fact] public async Task Unnest_InSelect()
	{
		var v = await S("SELECT SUM(x) FROM UNNEST([1,2,3,4,5]) AS x");
		Assert.Equal("15", v);
	}

	[Fact] public async Task Unnest_CrossJoin_WithTable()
	{
		var rows = await Q("SELECT * FROM UNNEST([1, 2]) AS a CROSS JOIN UNNEST(['x', 'y']) AS b ORDER BY a, b");
		Assert.Equal(4, rows.Count);
	}

	// ---- Array in WHERE with IN ----
	[Fact] public async Task Array_InClause()
	{
		var rows = await Q("SELECT x FROM UNNEST([1,2,3,4,5]) AS x WHERE x IN (2, 4) ORDER BY x");
		Assert.Equal(2, rows.Count);
	}

	// ---- SAFE_OFFSET/SAFE_ORDINAL ----
	[Fact] public async Task SafeOffset_Valid()
	{
		var v = await S("SELECT [10,20,30][SAFE_OFFSET(1)]");
		Assert.Equal("20", v);
	}
	[Fact] public async Task SafeOffset_OutOfBounds()
	{
		var v = await S("SELECT [10,20,30][SAFE_OFFSET(5)]");
		Assert.Null(v);
	}
}
