using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for ORDER BY, LIMIT, OFFSET, and result ordering patterns.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#order_by_clause
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class OrderByLimitPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public OrderByLimitPatternTests(BigQuerySession session) => _session = session;
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

	// ---- ORDER BY ASC ----
	[Fact]
	public async Task OrderBy_AscDefault()
	{
		var v = await Column("SELECT x FROM UNNEST([3, 1, 4, 1, 5]) AS x ORDER BY x");
		Assert.Equal(new[] { "1", "1", "3", "4", "5" }, v);
	}

	[Fact]
	public async Task OrderBy_AscExplicit()
	{
		var v = await Column("SELECT x FROM UNNEST([5, 3, 1, 4, 2]) AS x ORDER BY x ASC");
		Assert.Equal(new[] { "1", "2", "3", "4", "5" }, v);
	}

	// ---- ORDER BY DESC ----
	[Fact]
	public async Task OrderBy_Desc()
	{
		var v = await Column("SELECT x FROM UNNEST([3, 1, 4, 1, 5]) AS x ORDER BY x DESC");
		Assert.Equal(new[] { "5", "4", "3", "1", "1" }, v);
	}

	[Fact]
	public async Task OrderBy_DescRange()
	{
		var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x ORDER BY x DESC");
		Assert.Equal(new[] { "5", "4", "3", "2", "1" }, v);
	}

	// ---- ORDER BY string ----
	[Fact]
	public async Task OrderBy_StringAsc()
	{
		var v = await Column("SELECT x FROM UNNEST(['cherry', 'apple', 'banana', 'date']) AS x ORDER BY x");
		Assert.Equal(new[] { "apple", "banana", "cherry", "date" }, v);
	}

	[Fact]
	public async Task OrderBy_StringDesc()
	{
		var v = await Column("SELECT x FROM UNNEST(['cherry', 'apple', 'banana', 'date']) AS x ORDER BY x DESC");
		Assert.Equal(new[] { "date", "cherry", "banana", "apple" }, v);
	}

	// ---- ORDER BY expression ----
	[Fact]
	public async Task OrderBy_Expression()
	{
		var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x ORDER BY MOD(x, 3), x");
		Assert.Equal(new[] { "3", "1", "4", "2", "5" }, v);
	}

	[Fact]
	public async Task OrderBy_LengthStr()
	{
		var v = await Column("SELECT x FROM UNNEST(['a', 'bbb', 'cc', 'dddd', 'e']) AS x ORDER BY LENGTH(x), x");
		Assert.Equal(new[] { "a", "e", "cc", "bbb", "dddd" }, v);
	}

	// ---- LIMIT ----
	[Fact(Skip = "Emulator limitation")]
	public async Task Limit_3()
	{
		var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x ORDER BY x LIMIT 3");
		Assert.Equal(new[] { "1", "2", "3" }, v);
	}

	[Fact]
	public async Task Limit_1()
	{
		var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x ORDER BY x LIMIT 1");
		Assert.Equal(new[] { "1" }, v);
	}

	[Fact]
	public async Task Limit_0()
	{
		var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x ORDER BY x LIMIT 0");
		Assert.Empty(v);
	}

	[Fact]
	public async Task Limit_GTCount()
	{
		var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 3)) AS x ORDER BY x LIMIT 100");
		Assert.Equal(new[] { "1", "2", "3" }, v);
	}

	// ---- OFFSET ----
	[Fact(Skip = "Emulator limitation")]
	public async Task Offset_2()
	{
		var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x ORDER BY x LIMIT 3 OFFSET 2");
		Assert.Equal(new[] { "3", "4", "5" }, v);
	}

	[Fact(Skip = "Emulator limitation")]
	public async Task Offset_0()
	{
		var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x ORDER BY x LIMIT 3 OFFSET 0");
		Assert.Equal(new[] { "1", "2", "3" }, v);
	}

	[Fact(Skip = "Emulator limitation")]
	public async Task Offset_7()
	{
		var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x ORDER BY x LIMIT 5 OFFSET 7");
		Assert.Equal(new[] { "8", "9", "10" }, v);
	}

	[Fact]
	public async Task Offset_PastEnd()
	{
		var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x ORDER BY x LIMIT 5 OFFSET 100");
		Assert.Empty(v);
	}

	// ---- Pagination patterns ----
	[Fact(Skip = "Emulator limitation")]
	public async Task Pagination_Page1()
	{
		var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 20)) AS x ORDER BY x LIMIT 5 OFFSET 0");
		Assert.Equal(new[] { "1", "2", "3", "4", "5" }, v);
	}

	[Fact(Skip = "Emulator limitation")]
	public async Task Pagination_Page2()
	{
		var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 20)) AS x ORDER BY x LIMIT 5 OFFSET 5");
		Assert.Equal(new[] { "6", "7", "8", "9", "10" }, v);
	}

	[Fact(Skip = "Emulator limitation")]
	public async Task Pagination_Page3()
	{
		var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 20)) AS x ORDER BY x LIMIT 5 OFFSET 10");
		Assert.Equal(new[] { "11", "12", "13", "14", "15" }, v);
	}

	[Fact(Skip = "Emulator limitation")]
	public async Task Pagination_LastPage()
	{
		var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 20)) AS x ORDER BY x LIMIT 5 OFFSET 15");
		Assert.Equal(new[] { "16", "17", "18", "19", "20" }, v);
	}

	// ---- TOP-N patterns ----
	[Fact(Skip = "Emulator limitation")]
	public async Task TopN_Largest3()
	{
		var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x ORDER BY x DESC LIMIT 3");
		Assert.Equal(new[] { "10", "9", "8" }, v);
	}

	[Fact(Skip = "Emulator limitation")]
	public async Task TopN_Smallest3()
	{
		var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x ORDER BY x ASC LIMIT 3");
		Assert.Equal(new[] { "1", "2", "3" }, v);
	}

	// ---- ORDER BY with CASE ----
	[Fact]
	public async Task OrderBy_CaseExpr()
	{
		var v = await Column(@"
SELECT x FROM UNNEST([1, 2, 3, 4, 5]) AS x
ORDER BY CASE WHEN x = 3 THEN 0 ELSE 1 END, x");
		Assert.Equal("3", v[0]);
	}

	// ---- ORDER BY with NULLS ----
	[Fact]
	public async Task OrderBy_NullsPresent()
	{
		var v = await Column("SELECT x FROM UNNEST([3, NULL, 1, NULL, 2]) AS x ORDER BY x");
		Assert.Equal("1", v.First(x => x != null));
	}

	// ---- Combining ORDER BY + WHERE + LIMIT ----
	[Fact(Skip = "Emulator limitation")]
	public async Task Combined_FilterSortLimit()
	{
		var v = await Column(@"
SELECT x FROM UNNEST(GENERATE_ARRAY(1, 20)) AS x
WHERE MOD(x, 2) = 0
ORDER BY x DESC
LIMIT 3");
		Assert.Equal(new[] { "20", "18", "16" }, v);
	}

	[Fact(Skip = "Emulator limitation")]
	public async Task Combined_FilterSortOffset()
	{
		var v = await Column(@"
SELECT x FROM UNNEST(GENERATE_ARRAY(1, 20)) AS x
WHERE MOD(x, 3) = 0
ORDER BY x
LIMIT 3 OFFSET 2");
		Assert.Equal(new[] { "9", "12", "15" }, v);
	}

	// ---- ORDER BY ordinal position ----
	[Fact(Skip = "Emulator limitation")]
	public async Task OrderBy_Ordinal()
	{
		var v = await Column("SELECT x * 2 AS doubled FROM UNNEST([5, 3, 1, 4, 2]) AS x ORDER BY 1");
		Assert.Equal(new[] { "2", "4", "6", "8", "10" }, v);
	}

	// ---- ORDER BY alias ----
	[Fact(Skip = "Emulator limitation")]
	public async Task OrderBy_Alias()
	{
		var v = await Column("SELECT x * 2 AS doubled FROM UNNEST([5, 3, 1, 4, 2]) AS x ORDER BY doubled");
		Assert.Equal(new[] { "2", "4", "6", "8", "10" }, v);
	}
}
