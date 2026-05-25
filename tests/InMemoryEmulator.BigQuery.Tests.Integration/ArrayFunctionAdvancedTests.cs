using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Integration tests for array functions and patterns: ARRAY_FIRST, ARRAY_LAST,
/// ARRAY_IS_DISTINCT, ARRAY_LENGTH, ARRAY_CONCAT, ARRAY_REVERSE, ARRAY_TO_STRING,
/// GENERATE_ARRAY, UNNEST, and subquery equivalents for filtering/transforming/aggregating arrays.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
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
	// GO emulator limitation: ARRAY_FIRST function not implemented in goccy/bigquery-emulator.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact] public async Task ArrayFirst_Integers() => Assert.Equal("10", await S("SELECT ARRAY_FIRST([10, 20, 30])"));
	// GO emulator limitation: ARRAY_FIRST function not implemented in goccy/bigquery-emulator.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact] public async Task ArrayFirst_Strings() => Assert.Equal("a", await S("SELECT ARRAY_FIRST(['a', 'b', 'c'])"));
	// GO emulator limitation: ARRAY_LAST function not implemented in goccy/bigquery-emulator.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact] public async Task ArrayFirst_Single() => Assert.Equal("42", await S("SELECT ARRAY_FIRST([42])"));

	// ---- ARRAY_LAST ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_last
	[Fact] public async Task ArrayLast_Integers() => Assert.Equal("30", await S("SELECT ARRAY_LAST([10, 20, 30])"));
	// GO emulator limitation: ARRAY_LAST function not implemented in goccy/bigquery-emulator.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact] public async Task ArrayLast_Strings() => Assert.Equal("c", await S("SELECT ARRAY_LAST(['a', 'b', 'c'])"));
	[Fact] public async Task ArrayLast_Single() => Assert.Equal("42", await S("SELECT ARRAY_LAST([42])"));

	// ---- ARRAY_INCLUDES equivalent (val IN UNNEST(arr)) ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#in_operators
	[Fact] [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] public async Task ArrayIncludes_Found() => Assert.Equal("True", await S("SELECT 2 IN UNNEST([1, 2, 3])"));
	[Fact] [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] public async Task ArrayIncludes_NotFound() => Assert.Equal("False", await S("SELECT 4 IN UNNEST([1, 2, 3])"));
	[Fact] [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] public async Task ArrayIncludes_String() => Assert.Equal("True", await S("SELECT 'b' IN UNNEST(['a','b','c'])"));

	// ---- ARRAY_INCLUDES_ALL equivalent (LOGICAL_AND subquery) ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#in_operators
	[Fact] [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] public async Task ArrayIncludesAll_True() => Assert.Equal("True", await S("SELECT (SELECT LOGICAL_AND(x IN UNNEST([1,2,3,4])) FROM UNNEST([1,3]) AS x)"));
	[Fact] [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] public async Task ArrayIncludesAll_False() => Assert.Equal("False", await S("SELECT (SELECT LOGICAL_AND(x IN UNNEST([1,2,3])) FROM UNNEST([1,4]) AS x)"));

	// ---- ARRAY_INCLUDES_ANY equivalent (LOGICAL_OR subquery) ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#in_operators
	[Fact] [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] public async Task ArrayIncludesAny_True() => Assert.Equal("True", await S("SELECT (SELECT LOGICAL_OR(x IN UNNEST([1,2,3])) FROM UNNEST([3,4,5]) AS x)"));
	[Fact] [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] public async Task ArrayIncludesAny_False() => Assert.Equal("False", await S("SELECT (SELECT LOGICAL_OR(x IN UNNEST([1,2,3])) FROM UNNEST([4,5,6]) AS x)"));

	// ---- ARRAY_MAX equivalent (subquery with MAX) ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#max
	[Fact] [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] public async Task ArrayMax_Integers() => Assert.Equal("30", await S("SELECT (SELECT MAX(x) FROM UNNEST([10, 30, 20]) AS x)"));
	[Fact] [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] public async Task ArrayMax_WithNulls() => Assert.Equal("30", await S("SELECT (SELECT MAX(x) FROM UNNEST([10, NULL, 30, 20]) AS x)"));
	[Fact] [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] public async Task ArrayMax_Single() => Assert.Equal("5", await S("SELECT (SELECT MAX(x) FROM UNNEST([5]) AS x)"));

	// ---- ARRAY_MIN equivalent (subquery with MIN) ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#min
	[Fact] [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] public async Task ArrayMin_Integers() => Assert.Equal("10", await S("SELECT (SELECT MIN(x) FROM UNNEST([10, 30, 20]) AS x)"));
	[Fact] [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] public async Task ArrayMin_WithNulls() => Assert.Equal("10", await S("SELECT (SELECT MIN(x) FROM UNNEST([10, NULL, 30, 20]) AS x)"));

	// ---- ARRAY_SUM equivalent (subquery with SUM) ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#sum
	[Fact] [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] public async Task ArraySum_Integers() => Assert.Equal("60", await S("SELECT (SELECT SUM(x) FROM UNNEST([10, 20, 30]) AS x)"));
	[Fact] [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] public async Task ArraySum_WithNulls() => Assert.Equal("60", await S("SELECT (SELECT SUM(x) FROM UNNEST([10, NULL, 20, 30]) AS x)"));
	[Fact] [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] public async Task ArraySum_Empty() => Assert.Null(await S("SELECT (SELECT SUM(x) FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x)"));

	// ---- ARRAY_AVG equivalent (subquery with AVG) ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#avg
	[Fact] [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] public async Task ArrayAvg_Integers()
	{
		var v = await S("SELECT (SELECT AVG(x) FROM UNNEST([10, 20, 30]) AS x)");
		Assert.NotNull(v);
		Assert.Equal("20", v?.Split('.')[0]); // should be 20.0
	}
	[Fact] [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] public async Task ArrayAvg_WithNulls()
	{
		var v = await S("SELECT (SELECT AVG(x) FROM UNNEST([10, NULL, 20, 30]) AS x)");
		Assert.NotNull(v);
		Assert.Equal("20", v?.Split('.')[0]);
	}

	// ---- ARRAY_IS_DISTINCT ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_is_distinct
	// GO emulator limitation: function unimplemented in goccy/bigquery-emulator.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact] public async Task ArrayIsDistinct_True() => Assert.Equal("True", await S("SELECT ARRAY_IS_DISTINCT([1, 2, 3])"));
	[Fact] public async Task ArrayIsDistinct_False()
	{
		var v = await S("SELECT ARRAY_IS_DISTINCT([1, 2, 2, 3])");
		Assert.Equal("False", v);
	}

	// ---- ARRAY_FILTER equivalent (subquery with ARRAY_AGG + WHERE) ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#array_agg
	[Fact] [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] public async Task ArrayFilter_GreaterThan()
	{
		var v = await S("SELECT ARRAY_LENGTH((SELECT ARRAY_AGG(e) FROM UNNEST([1,2,3,4,5]) AS e WHERE e > 3))");
		Assert.Equal("2", v);
	}
	[Fact] [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] public async Task ArrayFilter_EvenNumbers()
	{
		var v = await S("SELECT ARRAY_LENGTH((SELECT ARRAY_AGG(e) FROM UNNEST([1,2,3,4,5,6]) AS e WHERE MOD(e, 2) = 0))");
		Assert.Equal("3", v);
	}
	[Fact] [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] public async Task ArrayFilter_None()
	{
		var v = await S("SELECT COALESCE(ARRAY_LENGTH((SELECT ARRAY_AGG(e) FROM UNNEST([1,2,3]) AS e WHERE e > 10)), 0)");
		Assert.Equal("0", v);
	}

	// ---- ARRAY_TRANSFORM equivalent (subquery with ARRAY_AGG) ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#array_agg
	[Fact] [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] public async Task ArrayTransform_Double()
	{
		var v = await S("SELECT ARRAY_TO_STRING((SELECT ARRAY_AGG(CAST(e * 2 AS STRING)) FROM UNNEST([1,2,3]) AS e), ',')");
		Assert.Equal("2,4,6", v);
	}
	[Fact] [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] public async Task ArrayTransform_ToString()
	{
		var v = await S("SELECT ARRAY_TO_STRING((SELECT ARRAY_AGG(CAST(e AS STRING)) FROM UNNEST([1,2,3]) AS e), ',')");
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
		var v = await S("SELECT ARRAY_TO_STRING(ARRAY(SELECT CAST(x AS STRING) FROM UNNEST(ARRAY_CONCAT([1,2], [3,4])) AS x), ',')");
		Assert.Equal("1,2,3,4", v);
	}
	[Fact] public async Task ArrayConcat_Three()
	{
		var v = await S("SELECT ARRAY_TO_STRING(ARRAY(SELECT CAST(x AS STRING) FROM UNNEST(ARRAY_CONCAT([1], [2], [3])) AS x), ',')");
		Assert.Equal("1,2,3", v);
	}

	// ---- ARRAY_REVERSE ----
	[Fact] public async Task ArrayReverse_Basic()
	{
		var v = await S("SELECT ARRAY_TO_STRING(ARRAY(SELECT CAST(x AS STRING) FROM UNNEST(ARRAY_REVERSE([1,2,3])) AS x), ',')");
		Assert.Equal("3,2,1", v);
	}
	[Fact] public async Task ArrayReverse_Single()
	{
		var v = await S("SELECT ARRAY_TO_STRING(ARRAY(SELECT CAST(x AS STRING) FROM UNNEST(ARRAY_REVERSE([42])) AS x), ',')");
		Assert.Equal("42", v);
	}

	// ---- ARRAY_SLICE equivalent (subquery with OFFSET) ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_slice
	[Fact] public async Task ArraySlice_Basic()
	{
		var v = await S("SELECT ARRAY_TO_STRING((SELECT ARRAY_AGG(CAST(elem AS STRING) ORDER BY off) FROM UNNEST([1,2,3,4,5]) AS elem WITH OFFSET AS off WHERE off >= 1 AND off <= 3), ',')");
		Assert.Equal("2,3,4", v);
	}
	[Fact] public async Task ArraySlice_FromStart()
	{
		var v = await S("SELECT ARRAY_TO_STRING((SELECT ARRAY_AGG(CAST(elem AS STRING) ORDER BY off) FROM UNNEST([1,2,3,4,5]) AS elem WITH OFFSET AS off WHERE off >= 0 AND off <= 2), ',')");
		Assert.Equal("1,2,3", v);
	}

	// ---- ARRAY_LENGTH ----
	[Fact] public async Task ArrayLength_Basic() => Assert.Equal("3", await S("SELECT ARRAY_LENGTH([1,2,3])"));
	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact] public async Task ArrayLength_Empty() => Assert.Equal("0", await S("SELECT ARRAY_LENGTH(CAST([] AS ARRAY<INT64>))"));
	[Fact] public async Task ArrayLength_Null() => Assert.Null(await S("SELECT ARRAY_LENGTH(NULL)"));

	// ---- GENERATE_ARRAY ----
	[Fact] public async Task GenerateArray_Step2()
	{
		var v = await S("SELECT ARRAY_TO_STRING(ARRAY(SELECT CAST(x AS STRING) FROM UNNEST(GENERATE_ARRAY(1, 10, 2)) AS x), ',')");
		Assert.Equal("1,3,5,7,9", v);
	}
	[Fact] public async Task GenerateArray_Reverse()
	{
		var v = await S("SELECT ARRAY_TO_STRING(ARRAY(SELECT CAST(x AS STRING) FROM UNNEST(GENERATE_ARRAY(5, 1, -1)) AS x), ',')");
		Assert.Equal("5,4,3,2,1", v);
	}
	[Fact] public async Task GenerateArray_Single()
	{
		var v = await S("SELECT ARRAY_TO_STRING(ARRAY(SELECT CAST(x AS STRING) FROM UNNEST(GENERATE_ARRAY(1, 1)) AS x), ',')");
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
