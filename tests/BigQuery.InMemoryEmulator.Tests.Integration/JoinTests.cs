using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for JOIN operations: CROSS JOIN, INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#join_types
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class JoinTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public JoinTests(BigQuerySession session) => _session = session;
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

	// ---- CROSS JOIN ----
	[Fact]
	public async Task CrossJoin_Count()
	{
		var v = await Scalar(@"
			SELECT COUNT(*) FROM UNNEST([1,2,3]) AS a CROSS JOIN UNNEST([4,5]) AS b");
		Assert.Equal("6", v);
	}

	[Fact]
	public async Task CrossJoin_Values()
	{
		var rows = await Query(@"
			SELECT a, b FROM UNNEST([1,2]) AS a CROSS JOIN UNNEST([10,20]) AS b ORDER BY a, b");
		Assert.Equal(4, rows.Count);
		Assert.Equal("1", rows[0]["a"]?.ToString());
		Assert.Equal("10", rows[0]["b"]?.ToString());
	}

	[Fact]
	public async Task CrossJoin_Strings()
	{
		var rows = await Query(@"
			SELECT a, b FROM UNNEST(['x','y']) AS a CROSS JOIN UNNEST(['1','2']) AS b ORDER BY a, b");
		Assert.Equal(4, rows.Count);
	}

	// ---- INNER JOIN ----
	[Fact]
	public async Task InnerJoin_Basic()
	{
		var rows = await Query(@"
			SELECT l.id, l.name, r.score
			FROM UNNEST([STRUCT(1 AS id, 'Alice' AS name), STRUCT(2, 'Bob'), STRUCT(3, 'Charlie')]) AS l
			INNER JOIN UNNEST([STRUCT(1 AS id, 90 AS score), STRUCT(2, 80)]) AS r
			ON l.id = r.id
			ORDER BY l.id");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("90", rows[0]["score"]?.ToString());
		Assert.Equal("Bob", rows[1]["name"]?.ToString());
		Assert.Equal("80", rows[1]["score"]?.ToString());
	}

	[Fact]
	public async Task InnerJoin_NoMatch()
	{
		var rows = await Query(@"
			SELECT l.id FROM UNNEST([STRUCT(1 AS id)]) AS l
			INNER JOIN UNNEST([STRUCT(2 AS id)]) AS r ON l.id = r.id");
		Assert.Empty(rows);
	}

	[Fact]
	public async Task InnerJoin_MultipleMatches()
	{
		var rows = await Query(@"
			SELECT l.id, r.val FROM UNNEST([STRUCT(1 AS id)]) AS l
			INNER JOIN UNNEST([STRUCT(1 AS id, 'a' AS val), STRUCT(1, 'b')]) AS r ON l.id = r.id
			ORDER BY r.val");
		Assert.Equal(2, rows.Count);
	}

	// ---- LEFT JOIN ----
	[Fact]
	public async Task LeftJoin_AllMatch()
	{
		var rows = await Query(@"
			SELECT l.id, r.val FROM UNNEST([STRUCT(1 AS id), STRUCT(2)]) AS l
			LEFT JOIN UNNEST([STRUCT(1 AS id, 'a' AS val), STRUCT(2, 'b')]) AS r ON l.id = r.id
			ORDER BY l.id");
		Assert.Equal(2, rows.Count);
		Assert.Equal("a", rows[0]["val"]?.ToString());
		Assert.Equal("b", rows[1]["val"]?.ToString());
	}

	[Fact]
	public async Task LeftJoin_PartialMatch()
	{
		var rows = await Query(@"
			SELECT l.id, r.val FROM UNNEST([STRUCT(1 AS id), STRUCT(2), STRUCT(3)]) AS l
			LEFT JOIN UNNEST([STRUCT(1 AS id, 'a' AS val)]) AS r ON l.id = r.id
			ORDER BY l.id");
		Assert.Equal(3, rows.Count);
		Assert.Equal("a", rows[0]["val"]?.ToString());
		Assert.Null(rows[1]["val"]);
		Assert.Null(rows[2]["val"]);
	}

	[Fact]
	public async Task LeftJoin_NoMatch()
	{
		var rows = await Query(@"
			SELECT l.id, r.val FROM UNNEST([STRUCT(1 AS id)]) AS l
			LEFT JOIN UNNEST([STRUCT(2 AS id, 'x' AS val)]) AS r ON l.id = r.id");
		Assert.Single(rows);
		Assert.Null(rows[0]["val"]);
	}

	// ---- RIGHT JOIN ----
	[Fact]
	public async Task RightJoin_AllMatch()
	{
		var rows = await Query(@"
			SELECT l.val, r.id FROM UNNEST([STRUCT(1 AS id, 'a' AS val), STRUCT(2, 'b')]) AS l
			RIGHT JOIN UNNEST([STRUCT(1 AS id), STRUCT(2)]) AS r ON l.id = r.id
			ORDER BY r.id");
		Assert.Equal(2, rows.Count);
	}

	[Fact]
	public async Task RightJoin_PartialMatch()
	{
		var rows = await Query(@"
			SELECT l.val, r.id FROM UNNEST([STRUCT(1 AS id, 'a' AS val)]) AS l
			RIGHT JOIN UNNEST([STRUCT(1 AS id), STRUCT(2), STRUCT(3)]) AS r ON l.id = r.id
			ORDER BY r.id");
		Assert.Equal(3, rows.Count);
		Assert.Equal("a", rows[0]["val"]?.ToString());
		Assert.Null(rows[1]["val"]);
		Assert.Null(rows[2]["val"]);
	}

	// ---- FULL OUTER JOIN ----
	[Fact]
	public async Task FullJoin_AllMatch()
	{
		var rows = await Query(@"
			SELECT l.id AS lid, r.id AS rid
			FROM UNNEST([STRUCT(1 AS id), STRUCT(2)]) AS l
			FULL OUTER JOIN UNNEST([STRUCT(1 AS id), STRUCT(2)]) AS r ON l.id = r.id
			ORDER BY COALESCE(l.id, r.id)");
		Assert.Equal(2, rows.Count);
	}

	[Fact]
	public async Task FullJoin_NoOverlap()
	{
		var rows = await Query(@"
			SELECT l.id AS lid, r.id AS rid
			FROM UNNEST([STRUCT(1 AS id), STRUCT(2)]) AS l
			FULL OUTER JOIN UNNEST([STRUCT(3 AS id), STRUCT(4)]) AS r ON l.id = r.id
			ORDER BY COALESCE(l.id, r.id)");
		Assert.Equal(4, rows.Count);
	}

	[Fact]
	public async Task FullJoin_Partial()
	{
		var rows = await Query(@"
			SELECT l.id AS lid, r.id AS rid
			FROM UNNEST([STRUCT(1 AS id), STRUCT(2)]) AS l
			FULL OUTER JOIN UNNEST([STRUCT(2 AS id), STRUCT(3)]) AS r ON l.id = r.id");
		Assert.Equal(3, rows.Count); // 1(left-only), 2(matched), 3(right-only)
	}

	// ---- Self-join ----
	[Fact]
	public async Task SelfJoin_Cross()
	{
		var v = await Scalar(@"
			WITH nums AS (SELECT x FROM UNNEST([1,2,3]) AS x)
			SELECT COUNT(*) FROM nums a CROSS JOIN nums b");
		Assert.Equal("9", v);
	}

	// ---- JOIN with aggregate ----
	[Fact]
	public async Task JoinAggregate()
	{
		var v = await Scalar(@"
			SELECT SUM(l.val * r.mul) FROM
			UNNEST([STRUCT(1 AS id, 10 AS val), STRUCT(2, 20)]) AS l
			INNER JOIN UNNEST([STRUCT(1 AS id, 2 AS mul), STRUCT(2, 3)]) AS r ON l.id = r.id");
		Assert.Equal("80", v); // 10*2 + 20*3 = 80
	}

	// ---- JOIN with filter ----
	[Fact]
	public async Task JoinWithWhere()
	{
		var rows = await Query(@"
			SELECT l.id, r.val FROM UNNEST([STRUCT(1 AS id), STRUCT(2), STRUCT(3)]) AS l
			INNER JOIN UNNEST([STRUCT(1 AS id, 'a' AS val), STRUCT(2, 'b'), STRUCT(3, 'c')]) AS r ON l.id = r.id
			WHERE l.id > 1 ORDER BY l.id");
		Assert.Equal(2, rows.Count);
		Assert.Equal("b", rows[0]["val"]?.ToString());
		Assert.Equal("c", rows[1]["val"]?.ToString());
	}

	// ---- USING clause ----
	[Fact]
	public async Task Join_Using()
	{
		var rows = await Query(@"
			SELECT id, name, score
			FROM UNNEST([STRUCT(1 AS id, 'Alice' AS name), STRUCT(2, 'Bob')]) AS l
			INNER JOIN UNNEST([STRUCT(1 AS id, 90 AS score), STRUCT(2, 80)]) AS r
			USING (id) ORDER BY id");
		Assert.Equal(2, rows.Count);
	}
}
