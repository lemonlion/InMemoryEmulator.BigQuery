using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for ORDER BY, LIMIT, OFFSET, and result ordering.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#order_by_clause
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class OrderByLimitTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public OrderByLimitTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		return result.ToList();
	}

	private async Task<string?> Scalar(string sql)
	{
		var rows = await Query(sql);
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- ORDER BY ASC ----
	[Fact]
	public async Task OrderBy_Asc_Default()
	{
		var rows = await Query("SELECT x FROM UNNEST([3,1,4,1,5]) AS x ORDER BY x");
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("1", rows[1][0]?.ToString());
		Assert.Equal("3", rows[2][0]?.ToString());
		Assert.Equal("4", rows[3][0]?.ToString());
		Assert.Equal("5", rows[4][0]?.ToString());
	}

	[Fact]
	public async Task OrderBy_Asc_Explicit()
	{
		var rows = await Query("SELECT x FROM UNNEST([3,1,2]) AS x ORDER BY x ASC");
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("2", rows[1][0]?.ToString());
		Assert.Equal("3", rows[2][0]?.ToString());
	}

	// ---- ORDER BY DESC ----
	[Fact]
	public async Task OrderBy_Desc()
	{
		var rows = await Query("SELECT x FROM UNNEST([3,1,4,1,5]) AS x ORDER BY x DESC");
		Assert.Equal("5", rows[0][0]?.ToString());
		Assert.Equal("4", rows[1][0]?.ToString());
		Assert.Equal("3", rows[2][0]?.ToString());
	}

	// ---- ORDER BY strings ----
	[Fact]
	public async Task OrderBy_Strings_Asc()
	{
		var rows = await Query("SELECT x FROM UNNEST(['cherry','apple','banana']) AS x ORDER BY x");
		Assert.Equal("apple", rows[0][0]?.ToString());
		Assert.Equal("banana", rows[1][0]?.ToString());
		Assert.Equal("cherry", rows[2][0]?.ToString());
	}

	[Fact]
	public async Task OrderBy_Strings_Desc()
	{
		var rows = await Query("SELECT x FROM UNNEST(['cherry','apple','banana']) AS x ORDER BY x DESC");
		Assert.Equal("cherry", rows[0][0]?.ToString());
		Assert.Equal("banana", rows[1][0]?.ToString());
		Assert.Equal("apple", rows[2][0]?.ToString());
	}

	// ---- ORDER BY with expression ----
	[Fact]
	public async Task OrderBy_Expression()
	{
		var rows = await Query("SELECT x FROM UNNEST([-3, 2, -1, 4]) AS x ORDER BY ABS(x)");
		Assert.Equal("-1", rows[0][0]?.ToString());
		Assert.Equal("2", rows[1][0]?.ToString());
	}

	// ---- ORDER BY by ordinal ----
	[Fact]
	public async Task OrderBy_Ordinal()
	{
		var rows = await Query("SELECT x, x * 2 AS doubled FROM UNNEST([3,1,2]) AS x ORDER BY 2");
		Assert.Equal("2", rows[0]["doubled"]?.ToString());
		Assert.Equal("4", rows[1]["doubled"]?.ToString());
		Assert.Equal("6", rows[2]["doubled"]?.ToString());
	}

	// ---- ORDER BY by alias ----
	[Fact]
	public async Task OrderBy_Alias()
	{
		var rows = await Query("SELECT x AS val FROM UNNEST([3,1,2]) AS x ORDER BY val");
		Assert.Equal("1", rows[0]["val"]?.ToString());
		Assert.Equal("2", rows[1]["val"]?.ToString());
		Assert.Equal("3", rows[2]["val"]?.ToString());
	}

	// ---- LIMIT ----
	[Fact]
	public async Task Limit_Basic()
	{
		var rows = await Query("SELECT x FROM UNNEST([1,2,3,4,5]) AS x ORDER BY x LIMIT 3");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("3", rows[2][0]?.ToString());
	}

	[Fact]
	public async Task Limit_One()
	{
		var rows = await Query("SELECT x FROM UNNEST([5,3,1]) AS x ORDER BY x LIMIT 1");
		Assert.Single(rows);
		Assert.Equal("1", rows[0][0]?.ToString());
	}

	[Fact]
	public async Task Limit_Zero()
	{
		var rows = await Query("SELECT x FROM UNNEST([1,2,3]) AS x LIMIT 0");
		Assert.Empty(rows);
	}

	[Fact]
	public async Task Limit_GreaterThanRows()
	{
		var rows = await Query("SELECT x FROM UNNEST([1,2,3]) AS x ORDER BY x LIMIT 100");
		Assert.Equal(3, rows.Count);
	}

	// ---- OFFSET ----
	[Fact]
	public async Task Offset_Basic()
	{
		var rows = await Query("SELECT x FROM UNNEST([1,2,3,4,5]) AS x ORDER BY x LIMIT 2 OFFSET 2");
		Assert.Equal(2, rows.Count);
		Assert.Equal("3", rows[0][0]?.ToString());
		Assert.Equal("4", rows[1][0]?.ToString());
	}

	[Fact]
	public async Task Offset_SkipAll()
	{
		var rows = await Query("SELECT x FROM UNNEST([1,2,3]) AS x ORDER BY x LIMIT 10 OFFSET 10");
		Assert.Empty(rows);
	}

	[Fact]
	public async Task Offset_Zero()
	{
		var rows = await Query("SELECT x FROM UNNEST([1,2,3]) AS x ORDER BY x LIMIT 3 OFFSET 0");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
	}

	[Fact]
	public async Task Offset_PartialPage()
	{
		var rows = await Query("SELECT x FROM UNNEST([1,2,3,4,5]) AS x ORDER BY x LIMIT 3 OFFSET 3");
		Assert.Equal(2, rows.Count);
		Assert.Equal("4", rows[0][0]?.ToString());
		Assert.Equal("5", rows[1][0]?.ToString());
	}

	// ---- Pagination pattern ----
	[Fact]
	public async Task Pagination_Page1()
	{
		var rows = await Query("SELECT x FROM UNNEST(GENERATE_ARRAY(1,20)) AS x ORDER BY x LIMIT 5 OFFSET 0");
		Assert.Equal(5, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("5", rows[4][0]?.ToString());
	}

	[Fact]
	public async Task Pagination_Page2()
	{
		var rows = await Query("SELECT x FROM UNNEST(GENERATE_ARRAY(1,20)) AS x ORDER BY x LIMIT 5 OFFSET 5");
		Assert.Equal(5, rows.Count);
		Assert.Equal("6", rows[0][0]?.ToString());
		Assert.Equal("10", rows[4][0]?.ToString());
	}

	// ---- Multiple ORDER BY columns ----
	[Fact]
	public async Task OrderBy_TwoColumns()
	{
		var rows = await Query(@"
			SELECT grp, val FROM UNNEST([
				STRUCT('a' AS grp, 2 AS val), STRUCT('b', 1), STRUCT('a', 1), STRUCT('b', 2)
			]) AS t ORDER BY grp, val");
		Assert.Equal("a", rows[0]["grp"]?.ToString());
		Assert.Equal("1", rows[0]["val"]?.ToString());
		Assert.Equal("a", rows[1]["grp"]?.ToString());
		Assert.Equal("2", rows[1]["val"]?.ToString());
		Assert.Equal("b", rows[2]["grp"]?.ToString());
		Assert.Equal("1", rows[2]["val"]?.ToString());
	}

	[Fact]
	public async Task OrderBy_MixedDirection()
	{
		var rows = await Query(@"
			SELECT grp, val FROM UNNEST([
				STRUCT('a' AS grp, 2 AS val), STRUCT('b', 1), STRUCT('a', 1), STRUCT('b', 2)
			]) AS t ORDER BY grp ASC, val DESC");
		Assert.Equal("a", rows[0]["grp"]?.ToString());
		Assert.Equal("2", rows[0]["val"]?.ToString());
		Assert.Equal("a", rows[1]["grp"]?.ToString());
		Assert.Equal("1", rows[1]["val"]?.ToString());
	}

	// ---- ORDER BY with NULLS ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#order_by_clause
	//   "NULLs appear first when the sort order is ascending"
	[Fact]
	public async Task OrderBy_NullsFirst_Asc()
	{
		var rows = await Query(@"
			SELECT x FROM UNNEST([3, CAST(NULL AS INT64), 1, 2]) AS x ORDER BY x ASC");
		Assert.Equal(4, rows.Count);
		Assert.Null(rows[0][0]);
		Assert.Equal("1", rows[1][0]?.ToString());
	}

	// ---- DISTINCT ----
	[Fact]
	public async Task Distinct_Basic()
	{
		var rows = await Query("SELECT DISTINCT x FROM UNNEST([1,2,2,3,3,3]) AS x ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("2", rows[1][0]?.ToString());
		Assert.Equal("3", rows[2][0]?.ToString());
	}

	[Fact]
	public async Task Distinct_Strings()
	{
		var rows = await Query("SELECT DISTINCT x FROM UNNEST(['a','b','a','c','b']) AS x ORDER BY x");
		Assert.Equal(3, rows.Count);
	}

	[Fact]
	public async Task Distinct_WithLimit()
	{
		var rows = await Query("SELECT DISTINCT x FROM UNNEST([1,2,2,3,3,3]) AS x ORDER BY x LIMIT 2");
		Assert.Equal(2, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("2", rows[1][0]?.ToString());
	}

	// ---- TOP N pattern ----
	[Fact]
	public async Task TopN_Max3()
	{
		var rows = await Query("SELECT x FROM UNNEST([10,50,30,20,40]) AS x ORDER BY x DESC LIMIT 3");
		Assert.Equal(3, rows.Count);
		Assert.Equal("50", rows[0][0]?.ToString());
		Assert.Equal("40", rows[1][0]?.ToString());
		Assert.Equal("30", rows[2][0]?.ToString());
	}

	[Fact]
	public async Task TopN_Min2()
	{
		var rows = await Query("SELECT x FROM UNNEST([10,50,30,20,40]) AS x ORDER BY x ASC LIMIT 2");
		Assert.Equal(2, rows.Count);
		Assert.Equal("10", rows[0][0]?.ToString());
		Assert.Equal("20", rows[1][0]?.ToString());
	}
}
