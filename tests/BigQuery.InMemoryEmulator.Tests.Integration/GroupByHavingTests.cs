using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for GROUP BY, HAVING, and aggregate combinations.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#group_by_clause
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class GroupByHavingTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public GroupByHavingTests(BigQuerySession session) => _session = session;
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

	// ---- Basic GROUP BY ----
	[Fact]
	public async Task GroupBy_CountPerGroup()
	{
		var rows = await Query("SELECT x, COUNT(*) as cnt FROM UNNEST([1,1,2,2,2,3]) AS x GROUP BY x ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]["x"]?.ToString());
		Assert.Equal("2", rows[0]["cnt"]?.ToString());
		Assert.Equal("2", rows[1]["x"]?.ToString());
		Assert.Equal("3", rows[1]["cnt"]?.ToString());
		Assert.Equal("3", rows[2]["x"]?.ToString());
		Assert.Equal("1", rows[2]["cnt"]?.ToString());
	}

	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task GroupBy_SumPerGroup()
	{
		var rows = await Query(@"
			SELECT grp, SUM(val) as total
			FROM UNNEST([STRUCT(1 AS grp, 10 AS val), STRUCT(1, 20), STRUCT(2, 30), STRUCT(2, 40)]) AS t
			GROUP BY grp ORDER BY grp");
		Assert.Equal(2, rows.Count);
		Assert.Equal("30", rows[0]["total"]?.ToString());
		Assert.Equal("70", rows[1]["total"]?.ToString());
	}

	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task GroupBy_MinMaxPerGroup()
	{
		var rows = await Query(@"
			SELECT grp, MIN(val) as mn, MAX(val) as mx
			FROM UNNEST([STRUCT('a' AS grp, 1 AS val), STRUCT('a', 5), STRUCT('b', 2), STRUCT('b', 8)]) AS t
			GROUP BY grp ORDER BY grp");
		Assert.Equal(2, rows.Count);
		Assert.Equal("1", rows[0]["mn"]?.ToString());
		Assert.Equal("5", rows[0]["mx"]?.ToString());
		Assert.Equal("2", rows[1]["mn"]?.ToString());
		Assert.Equal("8", rows[1]["mx"]?.ToString());
	}

	[Fact]
	public async Task GroupBy_CountStar()
	{
		var rows = await Query("SELECT x, COUNT(*) as cnt FROM UNNEST(['a','b','a','c','b','a']) AS x GROUP BY x ORDER BY cnt DESC");
		Assert.Equal(3, rows.Count);
		Assert.Equal("a", rows[0]["x"]?.ToString());
		Assert.Equal("3", rows[0]["cnt"]?.ToString());
	}

	// ---- HAVING clause ----
	[Fact]
	public async Task Having_FilterGroups()
	{
		var rows = await Query("SELECT x, COUNT(*) as cnt FROM UNNEST([1,1,2,2,2,3]) AS x GROUP BY x HAVING COUNT(*) > 1 ORDER BY x");
		Assert.Equal(2, rows.Count);
		Assert.Equal("1", rows[0]["x"]?.ToString());
		Assert.Equal("2", rows[1]["x"]?.ToString());
	}

	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task Having_SumFilter()
	{
		var rows = await Query(@"
			SELECT grp, SUM(val) as total
			FROM UNNEST([STRUCT('a' AS grp, 10 AS val), STRUCT('a', 20), STRUCT('b', 5), STRUCT('b', 3)]) AS t
			GROUP BY grp HAVING SUM(val) > 10 ORDER BY grp");
		Assert.Single(rows);
		Assert.Equal("a", rows[0]["grp"]?.ToString());
		Assert.Equal("30", rows[0]["total"]?.ToString());
	}

	[Fact]
	public async Task Having_NoResults()
	{
		var rows = await Query("SELECT x, COUNT(*) as cnt FROM UNNEST([1,2,3]) AS x GROUP BY x HAVING COUNT(*) > 5");
		Assert.Empty(rows);
	}

	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task Having_AvgFilter()
	{
		var rows = await Query(@"
			SELECT grp, AVG(val) as avg_val
			FROM UNNEST([STRUCT('a' AS grp, 10.0 AS val), STRUCT('a', 20.0), STRUCT('b', 1.0), STRUCT('b', 2.0)]) AS t
			GROUP BY grp HAVING AVG(val) > 5.0 ORDER BY grp");
		Assert.Single(rows);
		Assert.Equal("a", rows[0]["grp"]?.ToString());
	}

	// ---- GROUP BY with multiple columns ----
	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task GroupBy_TwoColumns()
	{
		var rows = await Query(@"
			SELECT c1, c2, COUNT(*) as cnt
			FROM UNNEST([STRUCT('a' AS c1, 1 AS c2), STRUCT('a', 1), STRUCT('a', 2), STRUCT('b', 1)]) AS t
			GROUP BY c1, c2 ORDER BY c1, c2");
		Assert.Equal(3, rows.Count);
		Assert.Equal("2", rows[0]["cnt"]?.ToString()); // a,1 -> 2
		Assert.Equal("1", rows[1]["cnt"]?.ToString()); // a,2 -> 1
		Assert.Equal("1", rows[2]["cnt"]?.ToString()); // b,1 -> 1
	}

	// ---- GROUP BY with expressions ----
	[Fact]
	public async Task GroupBy_Expression()
	{
		var rows = await Query(@"
			SELECT x > 2 AS gt2, COUNT(*) as cnt
			FROM UNNEST([1,2,3,4,5]) AS x
			GROUP BY gt2 ORDER BY gt2");
		Assert.Equal(2, rows.Count);
	}

	// ---- GROUP BY with ORDER BY aggregate ----
	[Fact]
	public async Task GroupBy_OrderByCount()
	{
		var rows = await Query("SELECT x, COUNT(*) as cnt FROM UNNEST([1,1,1,2,2,3]) AS x GROUP BY x ORDER BY cnt DESC");
		Assert.Equal("1", rows[0]["x"]?.ToString());
		Assert.Equal("3", rows[0]["cnt"]?.ToString());
	}

	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task GroupBy_OrderBySum()
	{
		var rows = await Query(@"
			SELECT grp, SUM(val) as total
			FROM UNNEST([STRUCT('a' AS grp, 100 AS val), STRUCT('b', 50), STRUCT('c', 200)]) AS t
			GROUP BY grp ORDER BY total DESC");
		Assert.Equal("c", rows[0]["grp"]?.ToString());
		Assert.Equal("200", rows[0]["total"]?.ToString());
	}

	// ---- GROUP BY with LIMIT ----
	[Fact]
	public async Task GroupBy_Limit()
	{
		var rows = await Query("SELECT x, COUNT(*) as cnt FROM UNNEST([1,1,2,2,3,3,4]) AS x GROUP BY x ORDER BY cnt DESC LIMIT 2");
		Assert.Equal(2, rows.Count);
	}

	// ---- Aggregate of aggregates (not allowed, but subquery is) ----
	[Fact]
	public async Task SubqueryAggregate()
	{
		var v = await Scalar(@"
			SELECT MAX(cnt) FROM (
				SELECT x, COUNT(*) as cnt FROM UNNEST([1,1,2,2,2,3]) AS x GROUP BY x
			) AS t");
		Assert.Equal("3", v);
	}

	// ---- GROUP BY with NULL values ----
	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task GroupBy_NullGroup()
	{
		var rows = await Query(@"
			SELECT grp, COUNT(*) as cnt
			FROM UNNEST([STRUCT('a' AS grp), STRUCT('a'), STRUCT(CAST(NULL AS STRING)), STRUCT(CAST(NULL AS STRING))]) AS t
			GROUP BY grp ORDER BY grp");
		Assert.True(rows.Count >= 2); // at least group 'a' and NULL group
	}

	// ---- HAVING with functions ----
	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task Having_MinFilter()
	{
		var rows = await Query(@"
			SELECT grp, MIN(val) as min_val
			FROM UNNEST([STRUCT('a' AS grp, 1 AS val), STRUCT('a', 5), STRUCT('b', 10), STRUCT('b', 20)]) AS t
			GROUP BY grp HAVING MIN(val) >= 5 ORDER BY grp");
		Assert.Single(rows);
		Assert.Equal("b", rows[0]["grp"]?.ToString());
	}

	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task Having_MaxFilter()
	{
		var rows = await Query(@"
			SELECT grp, MAX(val) as max_val
			FROM UNNEST([STRUCT('a' AS grp, 10 AS val), STRUCT('a', 50), STRUCT('b', 3), STRUCT('b', 7)]) AS t
			GROUP BY grp HAVING MAX(val) > 10 ORDER BY grp");
		Assert.Single(rows);
		Assert.Equal("a", rows[0]["grp"]?.ToString());
	}
}
