using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for array functions: ARRAY_LENGTH, ARRAY_REVERSE, ARRAY_TO_STRING, GENERATE_ARRAY, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ArrayFunctionComprehensiveExtTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public ArrayFunctionComprehensiveExtTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql, parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql, parameters: null); return r.ToList(); }

	// ---- ARRAY_LENGTH ----
	[Fact] public async Task ArrayLength_Basic() => Assert.Equal("3", await S("SELECT ARRAY_LENGTH([1,2,3])"));
	[Fact] public async Task ArrayLength_Empty() => Assert.Equal("0", await S("SELECT ARRAY_LENGTH([])"));
	[Fact] public async Task ArrayLength_One() => Assert.Equal("1", await S("SELECT ARRAY_LENGTH([42])"));
	[Fact] public async Task ArrayLength_Strings() => Assert.Equal("3", await S("SELECT ARRAY_LENGTH(['a','b','c'])"));
	[Fact] public async Task ArrayLength_Null() => Assert.Null(await S("SELECT ARRAY_LENGTH(CAST(NULL AS ARRAY<INT64>))"));

	// ---- ARRAY_REVERSE ----
	[Fact] public async Task ArrayReverse_Basic()
	{
		var rows = await Q("SELECT x FROM UNNEST(ARRAY_REVERSE([1,2,3])) AS x");
		Assert.Equal("3", rows[0]["x"]?.ToString());
		Assert.Equal("1", rows[2]["x"]?.ToString());
	}
	[Fact] public async Task ArrayReverse_Single()
	{
		var rows = await Q("SELECT x FROM UNNEST(ARRAY_REVERSE([42])) AS x");
		Assert.Single(rows);
		Assert.Equal("42", rows[0]["x"]?.ToString());
	}

	// ---- ARRAY_TO_STRING ----
	[Fact] public async Task ArrayToString_Basic() => Assert.Equal("a,b,c", await S("SELECT ARRAY_TO_STRING(['a','b','c'], ',')"));
	[Fact] public async Task ArrayToString_Space() => Assert.Equal("1 2 3", await S("SELECT ARRAY_TO_STRING(['1','2','3'], ' ')"));
	[Fact] public async Task ArrayToString_Empty() => Assert.Equal("", await S("SELECT ARRAY_TO_STRING(CAST([] AS ARRAY<STRING>), ',')"));
	[Fact] public async Task ArrayToString_WithNull() => Assert.Equal("a,c", await S("SELECT ARRAY_TO_STRING(['a', NULL, 'c'], ',')"));
	[Fact] public async Task ArrayToString_NullReplace() => Assert.Equal("a,N/A,c", await S("SELECT ARRAY_TO_STRING(['a', NULL, 'c'], ',', 'N/A')"));

	// ---- GENERATE_ARRAY ----
	[Fact] public async Task GenerateArray_Basic() => Assert.Equal("5", await S("SELECT ARRAY_LENGTH(GENERATE_ARRAY(1, 5))"));
	[Fact] public async Task GenerateArray_Step()
	{
		var rows = await Q("SELECT x FROM UNNEST(GENERATE_ARRAY(0, 10, 2)) AS x ORDER BY x");
		Assert.Equal(6, rows.Count); // 0,2,4,6,8,10
		Assert.Equal("0", rows[0]["x"]?.ToString());
		Assert.Equal("10", rows[5]["x"]?.ToString());
	}
	[Fact] public async Task GenerateArray_Negative()
	{
		var rows = await Q("SELECT x FROM UNNEST(GENERATE_ARRAY(5, 1, -1)) AS x ORDER BY x");
		Assert.Equal(5, rows.Count);
	}
	[Fact] public async Task GenerateArray_Single() => Assert.Equal("1", await S("SELECT ARRAY_LENGTH(GENERATE_ARRAY(5, 5))"));

	// ---- ARRAY with UNNEST ----
	[Fact] public async Task ArrayUnnest_Basic()
	{
		var rows = await Q("SELECT x FROM UNNEST([10, 20, 30]) AS x ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("10", rows[0]["x"]?.ToString());
	}
	[Fact] public async Task ArrayUnnest_WithOffset()
	{
		var rows = await Q("SELECT x, off FROM UNNEST([10, 20, 30]) AS x WITH OFFSET AS off ORDER BY off");
		Assert.Equal(3, rows.Count);
		Assert.Equal("0", rows[0]["off"]?.ToString());
	}
	[Fact] public async Task ArrayUnnest_Strings()
	{
		var rows = await Q("SELECT x FROM UNNEST(['hello', 'world']) AS x ORDER BY x");
		Assert.Equal(2, rows.Count);
		Assert.Equal("hello", rows[0]["x"]?.ToString());
	}

	// ---- ARRAY_CONCAT ----
	[Fact] public async Task ArrayConcat_Basic() => Assert.Equal("5", await S("SELECT ARRAY_LENGTH(ARRAY_CONCAT([1,2], [3,4,5]))"));
	[Fact] public async Task ArrayConcat_Three() => Assert.Equal("6", await S("SELECT ARRAY_LENGTH(ARRAY_CONCAT([1], [2,3], [4,5,6]))"));
	[Fact] public async Task ArrayConcat_Empty() => Assert.Equal("3", await S("SELECT ARRAY_LENGTH(ARRAY_CONCAT([1,2,3], []))"));

	// ---- Array in WITH ----
	[Fact] public async Task ArrayInCte()
	{
		// Test CTE providing array data that gets queried
		var v = await S("WITH arr AS (SELECT ARRAY_LENGTH([1,2,3,4,5]) AS cnt) SELECT cnt FROM arr");
		Assert.Equal("5", v);
	}

	// ---- Array with WHERE ----
	[Fact] public async Task ArrayUnnest_WithWhere()
	{
		var rows = await Q("SELECT x FROM UNNEST([1,2,3,4,5]) AS x WHERE x > 3 ORDER BY x");
		Assert.Equal(2, rows.Count);
	}

	// ---- Array with aggregation ----
	[Fact] public async Task ArrayUnnest_WithAgg()
	{
		var v = await S("SELECT SUM(x) FROM UNNEST([10, 20, 30]) AS x");
		Assert.Equal("60", v);
	}
	[Fact] public async Task ArrayUnnest_WithCount()
	{
		var v = await S("SELECT COUNT(*) FROM UNNEST([1,2,3,4,5]) AS x WHERE x > 2");
		Assert.Equal("3", v);
	}

	// ---- ARRAY constructors ----
	[Fact] public async Task ArrayConstructor_Literal()
	{
		var v = await S("SELECT ARRAY_LENGTH([1, 2, 3])");
		Assert.Equal("3", v);
	}
	[Fact] public async Task ArrayConstructor_Mixed()
	{
		var v = await S("SELECT ARRAY_LENGTH([1, 2+3, 4*5])");
		Assert.Equal("3", v);
	}

	// ---- Nested arrays (array of arrays via STRUCT) ----
	[Fact] public async Task NestedArray_ViaStruct()
	{
		var rows = await Q(@"
			SELECT s.name, x
			FROM UNNEST([STRUCT('A' AS name, [1,2,3] AS vals), STRUCT('B', [4,5])]) AS s,
			UNNEST(s.vals) AS x
			ORDER BY s.name, x");
		Assert.Equal(5, rows.Count);
	}

	// ---- ARRAY_AGG basic ----
	[Fact] public async Task ArrayAgg_FromUnnest()
	{
		var v = await S("SELECT ARRAY_LENGTH(ARRAY_AGG(x)) FROM UNNEST([1,2,3,4,5]) AS x WHERE x > 2");
		Assert.Equal("3", v);
	}

	// ---- GENERATE_DATE_ARRAY ----
	[Fact] public async Task GenerateDateArray_Basic()
	{
		var v = await S("SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-05'))");
		Assert.Equal("5", v);
	}
	[Fact] public async Task GenerateDateArray_Monthly()
	{
		var v = await S("SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-06-01', INTERVAL 1 MONTH))");
		Assert.Equal("6", v);
	}

	// ---- Empty array operations ----
	[Fact] public async Task EmptyArray_Unnest()
	{
		var rows = await Q("SELECT x FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x");
		Assert.Empty(rows);
	}
	[Fact] public async Task EmptyArray_Concat()
	{
		var v = await S("SELECT ARRAY_LENGTH(ARRAY_CONCAT([], [1,2,3]))");
		Assert.Equal("3", v);
	}
}
