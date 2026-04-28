using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for JOIN types: CROSS JOIN, INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL OUTER JOIN,
/// JOIN with CTEs, multi-table joins, self joins.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#join_types
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class JoinDeepTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public JoinDeepTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- CROSS JOIN ----
	[Fact]
	public async Task CrossJoin_Count()
	{
		var v = await Scalar(@"
WITH a AS (SELECT 1 AS x UNION ALL SELECT 2),
     b AS (SELECT 10 AS y UNION ALL SELECT 20)
SELECT COUNT(*) FROM a CROSS JOIN b");
		Assert.Equal("4", v);
	}

	[Fact]
	public async Task CrossJoin_3x3()
	{
		var v = await Scalar(@"
WITH a AS (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3),
     b AS (SELECT 10 AS y UNION ALL SELECT 20 UNION ALL SELECT 30)
SELECT COUNT(*) FROM a CROSS JOIN b");
		Assert.Equal("9", v);
	}

	[Fact]
	public async Task CrossJoin_EmptyLeft()
	{
		var v = await Scalar(@"
WITH a AS (SELECT 1 AS x WHERE FALSE),
     b AS (SELECT 10 AS y UNION ALL SELECT 20)
SELECT COUNT(*) FROM a CROSS JOIN b");
		Assert.Equal("0", v);
	}

	// ---- INNER JOIN ----
	[Fact]
	public async Task InnerJoin_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH a AS (SELECT 1 AS id, 'alice' AS name UNION ALL SELECT 2, 'bob' UNION ALL SELECT 3, 'charlie'),
     b AS (SELECT 1 AS id, 100 AS score UNION ALL SELECT 2, 200)
SELECT a.name, b.score FROM a INNER JOIN b ON a.id = b.id ORDER BY a.name", parameters: null);
		var rows = result.ToList();
		Assert.Equal(2, rows.Count);
		Assert.Equal("alice", rows[0][0]?.ToString());
		Assert.Equal("100", rows[0][1]?.ToString());
		Assert.Equal("bob", rows[1][0]?.ToString());
		Assert.Equal("200", rows[1][1]?.ToString());
	}

	[Fact]
	public async Task InnerJoin_NoMatch()
	{
		var v = await Scalar(@"
WITH a AS (SELECT 1 AS id, 'alice' AS name),
     b AS (SELECT 2 AS id, 100 AS score)
SELECT COUNT(*) FROM a INNER JOIN b ON a.id = b.id");
		Assert.Equal("0", v);
	}

	[Fact]
	public async Task InnerJoin_MultiMatch()
	{
		var v = await Scalar(@"
WITH a AS (SELECT 1 AS id, 'x' AS name UNION ALL SELECT 1, 'y'),
     b AS (SELECT 1 AS id, 100 AS score UNION ALL SELECT 1, 200)
SELECT COUNT(*) FROM a INNER JOIN b ON a.id = b.id");
		Assert.Equal("4", v);
	}

	// ---- LEFT JOIN ----
	[Fact]
	public async Task LeftJoin_AllMatch()
	{
		var v = await Scalar(@"
WITH a AS (SELECT 1 AS id UNION ALL SELECT 2),
     b AS (SELECT 1 AS id, 'x' AS val UNION ALL SELECT 2, 'y')
SELECT COUNT(*) FROM a LEFT JOIN b ON a.id = b.id");
		Assert.Equal("2", v);
	}

	[Fact]
	public async Task LeftJoin_SomeNull()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH a AS (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3),
     b AS (SELECT 1 AS id, 'x' AS val UNION ALL SELECT 2, 'y')
SELECT a.id, b.val FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id", parameters: null);
		var rows = result.ToList();
		Assert.Equal(3, rows.Count);
		Assert.Equal("x", rows[0][1]?.ToString());
		Assert.Equal("y", rows[1][1]?.ToString());
		Assert.Null(rows[2][1]);
	}

	[Fact]
	public async Task LeftJoin_NoMatch()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH a AS (SELECT 1 AS id UNION ALL SELECT 2),
     b AS (SELECT 3 AS id, 'x' AS val)
SELECT a.id, b.val FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id", parameters: null);
		var rows = result.ToList();
		Assert.Equal(2, rows.Count);
		Assert.Null(rows[0][1]);
		Assert.Null(rows[1][1]);
	}

	// ---- RIGHT JOIN ----
	[Fact]
	public async Task RightJoin_SomeNull()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH a AS (SELECT 1 AS id, 'x' AS val UNION ALL SELECT 2, 'y'),
     b AS (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3)
SELECT b.id, a.val FROM a RIGHT JOIN b ON a.id = b.id ORDER BY b.id", parameters: null);
		var rows = result.ToList();
		Assert.Equal(3, rows.Count);
		Assert.Equal("x", rows[0][1]?.ToString());
		Assert.Equal("y", rows[1][1]?.ToString());
		Assert.Null(rows[2][1]);
	}

	// ---- FULL OUTER JOIN ----
	[Fact]
	public async Task FullJoin_All()
	{
		var v = await Scalar(@"
WITH a AS (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3),
     b AS (SELECT 2 AS id UNION ALL SELECT 3 UNION ALL SELECT 4)
SELECT COUNT(*) FROM a FULL OUTER JOIN b ON a.id = b.id");
		Assert.Equal("4", v);
	}

	[Fact]
	public async Task FullJoin_NoOverlap()
	{
		var v = await Scalar(@"
WITH a AS (SELECT 1 AS id UNION ALL SELECT 2),
     b AS (SELECT 3 AS id UNION ALL SELECT 4)
SELECT COUNT(*) FROM a FULL OUTER JOIN b ON a.id = b.id");
		Assert.Equal("4", v);
	}

	// ---- Self JOIN ----
	[Fact]
	public async Task SelfJoin_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH emp AS (
  SELECT 1 AS id, 'alice' AS name, CAST(NULL AS INT64) AS mgr_id UNION ALL
  SELECT 2, 'bob', 1 UNION ALL
  SELECT 3, 'charlie', 1
)
SELECT e.name, m.name AS manager
FROM emp e LEFT JOIN emp m ON e.mgr_id = m.id
ORDER BY e.id", parameters: null);
		var rows = result.ToList();
		Assert.Equal(3, rows.Count);
		Assert.Null(rows[0][1]); // alice has no manager
		Assert.Equal("alice", rows[1][1]?.ToString()); // bob's manager is alice
		Assert.Equal("alice", rows[2][1]?.ToString()); // charlie's manager is alice
	}

	// ---- Multi-table JOIN ----
	[Fact]
	public async Task ThreeTableJoin()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH users AS (SELECT 1 AS id, 'alice' AS name UNION ALL SELECT 2, 'bob'),
     orders AS (SELECT 1 AS user_id, 101 AS order_id UNION ALL SELECT 1, 102 UNION ALL SELECT 2, 103),
     items AS (SELECT 101 AS order_id, 'book' AS item UNION ALL SELECT 102, 'pen' UNION ALL SELECT 103, 'cup')
SELECT u.name, o.order_id, i.item
FROM users u
JOIN orders o ON u.id = o.user_id
JOIN items i ON o.order_id = i.order_id
ORDER BY o.order_id", parameters: null);
		var rows = result.ToList();
		Assert.Equal(3, rows.Count);
		Assert.Equal("alice", rows[0][0]?.ToString());
		Assert.Equal("book", rows[0][2]?.ToString());
		Assert.Equal("bob", rows[2][0]?.ToString());
		Assert.Equal("cup", rows[2][2]?.ToString());
	}

	// ---- JOIN with aggregate ----
	[Fact]
	public async Task JoinWithAggregate()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH users AS (SELECT 1 AS id, 'alice' AS name UNION ALL SELECT 2, 'bob'),
     orders AS (SELECT 1 AS user_id UNION ALL SELECT 1 UNION ALL SELECT 2)
SELECT u.name, COUNT(*) AS order_count
FROM users u JOIN orders o ON u.id = o.user_id
GROUP BY u.name
ORDER BY u.name", parameters: null);
		var rows = result.ToList();
		Assert.Equal(2, rows.Count);
		Assert.Equal("alice", rows[0][0]?.ToString());
		Assert.Equal("2", rows[0][1]?.ToString());
		Assert.Equal("bob", rows[1][0]?.ToString());
		Assert.Equal("1", rows[1][1]?.ToString());
	}

	// ---- JOIN with WHERE ----
	[Fact]
	public async Task JoinWithWhere()
	{
		var v = await Scalar(@"
WITH a AS (SELECT 1 AS id, 10 AS val UNION ALL SELECT 2, 20 UNION ALL SELECT 3, 30),
     b AS (SELECT 1 AS id, 'x' AS letter UNION ALL SELECT 2, 'y' UNION ALL SELECT 3, 'z')
SELECT COUNT(*) FROM a JOIN b ON a.id = b.id WHERE a.val > 10");
		Assert.Equal("2", v);
	}

	// ---- Comma JOIN (implicit cross join) ----
	[Fact]
	public async Task CommaJoin()
	{
		var v = await Scalar(@"
WITH a AS (SELECT 1 AS x UNION ALL SELECT 2),
     b AS (SELECT 10 AS y UNION ALL SELECT 20)
SELECT COUNT(*) FROM a, b");
		Assert.Equal("4", v);
	}

	// ---- JOIN with USING ----
	[Fact]
	public async Task JoinUsing()
	{
		var v = await Scalar(@"
WITH a AS (SELECT 1 AS id, 'alice' AS name UNION ALL SELECT 2, 'bob'),
     b AS (SELECT 1 AS id, 100 AS score UNION ALL SELECT 2, 200)
SELECT COUNT(*) FROM a JOIN b USING (id)");
		Assert.Equal("2", v);
	}
}
