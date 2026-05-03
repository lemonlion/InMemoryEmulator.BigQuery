using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for UNION ALL, UNION DISTINCT, INTERSECT, EXCEPT set operations.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class SetOperationTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public SetOperationTests(BigQuerySession session) => _session = session;
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

	// ---- UNION ALL ----
	[Fact]
	public async Task UnionAll_Basic()
	{
		var rows = await Query("SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3");
		Assert.Equal(3, rows.Count);
	}

	[Fact]
	public async Task UnionAll_Duplicates()
	{
		var rows = await Query("SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 1");
		Assert.Equal(3, rows.Count);
	}

	[Fact]
	public async Task UnionAll_Strings()
	{
		var rows = await Query("SELECT 'a' AS x UNION ALL SELECT 'b' UNION ALL SELECT 'c'");
		Assert.Equal(3, rows.Count);
	}

	[Fact]
	public async Task UnionAll_WithUnnest()
	{
		var rows = await Query(@"
			SELECT x FROM UNNEST([1,2]) AS x
			UNION ALL
			SELECT x FROM UNNEST([3,4]) AS x
			ORDER BY x");
		Assert.Equal(4, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("4", rows[3][0]?.ToString());
	}

	[Fact]
	public async Task UnionAll_MultipleColumns()
	{
		var rows = await Query(@"
			SELECT 1 AS a, 'x' AS b
			UNION ALL
			SELECT 2, 'y'
			UNION ALL
			SELECT 3, 'z'
			ORDER BY a");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]["a"]?.ToString());
		Assert.Equal("x", rows[0]["b"]?.ToString());
	}

	// ---- UNION DISTINCT ----
	[Fact]
	public async Task UnionDistinct_RemovesDupes()
	{
		var rows = await Query("SELECT 1 AS x UNION DISTINCT SELECT 1 UNION DISTINCT SELECT 2");
		Assert.Equal(2, rows.Count);
	}

	[Fact]
	public async Task UnionDistinct_NoDupes()
	{
		var rows = await Query("SELECT 1 AS x UNION DISTINCT SELECT 2 UNION DISTINCT SELECT 3");
		Assert.Equal(3, rows.Count);
	}

	[Fact]
	public async Task UnionDistinct_AllSame()
	{
		var rows = await Query("SELECT 1 AS x UNION DISTINCT SELECT 1 UNION DISTINCT SELECT 1");
		Assert.Single(rows);
	}

	[Fact]
	public async Task UnionDistinct_Strings()
	{
		var rows = await Query("SELECT 'a' AS x UNION DISTINCT SELECT 'a' UNION DISTINCT SELECT 'b'");
		Assert.Equal(2, rows.Count);
	}

	// ---- INTERSECT DISTINCT ----
	[Fact]
	public async Task IntersectDistinct_Overlap()
	{
		var rows = await Query(@"
			SELECT x FROM UNNEST([1,2,3]) AS x
			INTERSECT DISTINCT
			SELECT x FROM UNNEST([2,3,4]) AS x
			ORDER BY x");
		Assert.Equal(2, rows.Count);
		Assert.Equal("2", rows[0][0]?.ToString());
		Assert.Equal("3", rows[1][0]?.ToString());
	}

	[Fact]
	public async Task IntersectDistinct_NoOverlap()
	{
		var rows = await Query(@"
			SELECT x FROM UNNEST([1,2]) AS x
			INTERSECT DISTINCT
			SELECT x FROM UNNEST([3,4]) AS x");
		Assert.Empty(rows);
	}

	[Fact]
	public async Task IntersectDistinct_FullOverlap()
	{
		var rows = await Query(@"
			SELECT x FROM UNNEST([1,2,3]) AS x
			INTERSECT DISTINCT
			SELECT x FROM UNNEST([1,2,3]) AS x
			ORDER BY x");
		Assert.Equal(3, rows.Count);
	}

	// ---- EXCEPT DISTINCT ----
	[Fact]
	public async Task ExceptDistinct_Basic()
	{
		var rows = await Query(@"
			SELECT x FROM UNNEST([1,2,3,4]) AS x
			EXCEPT DISTINCT
			SELECT x FROM UNNEST([2,4]) AS x
			ORDER BY x");
		Assert.Equal(2, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("3", rows[1][0]?.ToString());
	}

	[Fact]
	public async Task ExceptDistinct_RemoveAll()
	{
		var rows = await Query(@"
			SELECT x FROM UNNEST([1,2,3]) AS x
			EXCEPT DISTINCT
			SELECT x FROM UNNEST([1,2,3]) AS x");
		Assert.Empty(rows);
	}

	[Fact]
	public async Task ExceptDistinct_RemoveNone()
	{
		var rows = await Query(@"
			SELECT x FROM UNNEST([1,2,3]) AS x
			EXCEPT DISTINCT
			SELECT x FROM UNNEST([4,5]) AS x
			ORDER BY x");
		Assert.Equal(3, rows.Count);
	}

	// ---- Combined set operations ----
	[Fact]
	public async Task Combined_UnionIntersect()
	{
		var v = await Scalar(@"
			SELECT COUNT(*) FROM (
				SELECT x FROM UNNEST([1,2]) AS x
				UNION ALL
				SELECT x FROM UNNEST([3,4]) AS x
			) AS t");
		Assert.Equal("4", v);
	}

	// ---- UNION ALL with aggregates ----
	[Fact]
	public async Task UnionAll_WithAggregate()
	{
		var v = await Scalar(@"
			SELECT SUM(x) FROM (
				SELECT 10 AS x
				UNION ALL
				SELECT 20
				UNION ALL
				SELECT 30
			) AS t");
		Assert.Equal("60", v);
	}

	// ---- UNION ALL with CTE ----
	[Fact]
	public async Task UnionAll_WithCTE()
	{
		var rows = await Query(@"
			WITH a AS (SELECT x FROM UNNEST([1,2]) AS x),
			     b AS (SELECT x FROM UNNEST([3,4]) AS x)
			SELECT x FROM a UNION ALL SELECT x FROM b ORDER BY x");
		Assert.Equal(4, rows.Count);
	}

	// ---- Empty set operations ----
	[Fact]
	public async Task UnionAll_EmptyFirst()
	{
		var rows = await Query(@"
			SELECT x FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x
			UNION ALL
			SELECT x FROM UNNEST([1,2]) AS x
			ORDER BY x");
		Assert.Equal(2, rows.Count);
	}

	[Fact]
	public async Task UnionAll_EmptySecond()
	{
		var rows = await Query(@"
			SELECT x FROM UNNEST([1,2]) AS x
			UNION ALL
			SELECT x FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x
			ORDER BY x");
		Assert.Equal(2, rows.Count);
	}
}
