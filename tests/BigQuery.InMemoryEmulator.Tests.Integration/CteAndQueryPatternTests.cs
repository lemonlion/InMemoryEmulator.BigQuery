using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for CTE (Common Table Expression) patterns and complex query structures.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_clause
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class CteAndQueryPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public CteAndQueryPatternTests(BigQuerySession session) => _session = session;
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

	// ---- Basic CTE ----
	[Fact]
	public async Task Cte_Simple() => Assert.Equal("1", await Scalar("WITH t AS (SELECT 1 AS x) SELECT x FROM t"));

	[Fact(Skip = "Emulator limitation")]
	public async Task Cte_MultiRow() => Assert.Equal("3", await Scalar(@"
WITH t AS (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3)
SELECT COUNT(*) FROM t"));

	[Fact]
	public async Task Cte_WithAlias()
	{
		var rows = await Column(@"
WITH nums AS (SELECT x FROM UNNEST([1,2,3]) AS x)
SELECT x FROM nums ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]);
		Assert.Equal("3", rows[2]);
	}

	// ---- Multiple CTEs ----
	[Fact(Skip = "Emulator limitation")]
	public async Task Cte_TwoCtes() => Assert.Equal("3", await Scalar(@"
WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y)
SELECT a.x + b.y FROM a, b"));

	[Fact]
	public async Task Cte_ChainedCtes() => Assert.Equal("2", await Scalar(@"
WITH a AS (SELECT 1 AS x), b AS (SELECT x + 1 AS y FROM a)
SELECT y FROM b"));

	[Fact(Skip = "Emulator limitation")]
	public async Task Cte_ThreeCtes() => Assert.Equal("6", await Scalar(@"
WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS x), c AS (SELECT 3 AS x)
SELECT a.x + b.x + c.x FROM a, b, c"));

	[Fact]
	public async Task Cte_DependentChain() => Assert.Equal("4", await Scalar(@"
WITH a AS (SELECT 1 AS v), b AS (SELECT v + 1 AS v FROM a), c AS (SELECT v + 1 AS v FROM b), d AS (SELECT v + 1 AS v FROM c)
SELECT v FROM d"));

	// ---- CTE with aggregation ----
	[Fact]
	public async Task Cte_Aggregate() => Assert.Equal("15", await Scalar(@"
WITH nums AS (SELECT x FROM UNNEST([1,2,3,4,5]) AS x)
SELECT SUM(x) FROM nums"));

	[Fact(Skip = "Emulator limitation")]
	public async Task Cte_GroupBy() => Assert.Equal("2", await Scalar(@"
WITH data AS (
  SELECT 'a' AS grp, 1 AS val UNION ALL
  SELECT 'a', 2 UNION ALL
  SELECT 'b', 3
)
SELECT COUNT(DISTINCT grp) FROM data"));

	[Fact(Skip = "Emulator limitation")]
	public async Task Cte_AggregateJoin()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH data AS (
  SELECT 'a' AS grp, 10 AS val UNION ALL
  SELECT 'a', 20 UNION ALL
  SELECT 'b', 30
),
sums AS (
  SELECT grp, SUM(val) AS total FROM data GROUP BY grp
)
SELECT grp, total FROM sums ORDER BY grp", parameters: null);
		var rows = result.ToList();
		Assert.Equal(2, rows.Count);
		Assert.Equal("a", rows[0][0]?.ToString());
		Assert.Equal("30", rows[0][1]?.ToString());
		Assert.Equal("b", rows[1][0]?.ToString());
		Assert.Equal("30", rows[1][1]?.ToString());
	}

	// ---- CTE with filtering ----
	[Fact]
	public async Task Cte_Where() => Assert.Equal("2", await Scalar(@"
WITH nums AS (SELECT x FROM UNNEST([1,2,3,4,5]) AS x)
SELECT COUNT(*) FROM nums WHERE x > 3"));

	[Fact(Skip = "Emulator limitation")]
	public async Task Cte_WhereString()
	{
		var rows = await Column(@"
WITH names AS (
  SELECT 'alice' AS name UNION ALL SELECT 'bob' UNION ALL SELECT 'anna'
)
SELECT name FROM names WHERE name LIKE 'a%' ORDER BY name");
		Assert.Equal(2, rows.Count);
		Assert.Equal("alice", rows[0]);
		Assert.Equal("anna", rows[1]);
	}

	// ---- CTE with ORDER BY / LIMIT ----
	[Fact]
	public async Task Cte_OrderLimit()
	{
		var rows = await Column(@"
WITH nums AS (SELECT x FROM UNNEST([5,3,1,4,2]) AS x)
SELECT x FROM nums ORDER BY x LIMIT 3");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]);
		Assert.Equal("2", rows[1]);
		Assert.Equal("3", rows[2]);
	}

	[Fact]
	public async Task Cte_OrderDesc()
	{
		var rows = await Column(@"
WITH nums AS (SELECT x FROM UNNEST([5,3,1,4,2]) AS x)
SELECT x FROM nums ORDER BY x DESC LIMIT 3");
		Assert.Equal(3, rows.Count);
		Assert.Equal("5", rows[0]);
		Assert.Equal("4", rows[1]);
		Assert.Equal("3", rows[2]);
	}

	[Fact]
	public async Task Cte_Offset() => Assert.Equal("3", await Scalar(@"
WITH nums AS (SELECT x FROM UNNEST([1,2,3,4,5]) AS x)
SELECT x FROM nums ORDER BY x LIMIT 1 OFFSET 2"));

	// ---- Subqueries in SELECT ----
	[Fact]
	public async Task Subquery_Scalar() => Assert.Equal("3", await Scalar("SELECT (SELECT MAX(x) FROM UNNEST([1,2,3]) AS x)"));

	[Fact(Skip = "Emulator limitation")]
	public async Task Subquery_InWhere()
	{
		var rows = await Column(@"
WITH nums AS (SELECT x FROM UNNEST([1,2,3,4,5]) AS x)
SELECT x FROM nums WHERE x > (SELECT AVG(x) FROM nums) ORDER BY x");
		Assert.Equal(2, rows.Count);
		Assert.Equal("4", rows[0]);
		Assert.Equal("5", rows[1]);
	}

	[Fact]
	public async Task Subquery_InSelect()
	{
		var val = await Scalar(@"
SELECT (SELECT SUM(x) FROM UNNEST([1,2,3]) AS x) + (SELECT SUM(x) FROM UNNEST([4,5,6]) AS x)");
		Assert.Equal("21", val);
	}

	// ---- DISTINCT ----
	[Fact]
	public async Task Distinct_Basic()
	{
		var rows = await Column("SELECT DISTINCT x FROM UNNEST([1,1,2,2,3,3]) AS x ORDER BY x");
		Assert.Equal(3, rows.Count);
	}

	[Fact]
	public async Task Distinct_Strings()
	{
		var rows = await Column("SELECT DISTINCT x FROM UNNEST(['a','a','b','b','c']) AS x ORDER BY x");
		Assert.Equal(3, rows.Count);
	}

	// ---- Complex WHERE ----
	[Fact]
	public async Task Where_And()
	{
		var rows = await Column("SELECT x FROM UNNEST([1,2,3,4,5,6,7,8,9,10]) AS x WHERE x > 3 AND x < 8 ORDER BY x");
		Assert.Equal(4, rows.Count);
		Assert.Equal("4", rows[0]);
		Assert.Equal("7", rows[3]);
	}

	[Fact]
	public async Task Where_Or()
	{
		var rows = await Column("SELECT x FROM UNNEST([1,2,3,4,5]) AS x WHERE x = 1 OR x = 5 ORDER BY x");
		Assert.Equal(2, rows.Count);
		Assert.Equal("1", rows[0]);
		Assert.Equal("5", rows[1]);
	}

	[Fact]
	public async Task Where_NotEqual()
	{
		var rows = await Column("SELECT x FROM UNNEST([1,2,3]) AS x WHERE x != 2 ORDER BY x");
		Assert.Equal(2, rows.Count);
		Assert.Equal("1", rows[0]);
		Assert.Equal("3", rows[1]);
	}

	// ---- UNION ALL ----
	[Fact(Skip = "Emulator limitation")]
	public async Task UnionAll_Basic()
	{
		var rows = await Column("SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY x");
		Assert.Equal(3, rows.Count);
	}

	[Fact(Skip = "Emulator limitation")]
	public async Task UnionAll_WithCte()
	{
		var rows = await Column(@"
WITH a AS (SELECT 1 AS x UNION ALL SELECT 2),
     b AS (SELECT 3 AS x UNION ALL SELECT 4)
SELECT x FROM a UNION ALL SELECT x FROM b ORDER BY x");
		Assert.Equal(4, rows.Count);
	}

	// ---- ORDER BY multiple columns ----
	[Fact(Skip = "Emulator limitation")]
	public async Task OrderBy_TwoColumns()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH data AS (
  SELECT 'a' AS grp, 2 AS val UNION ALL
  SELECT 'a', 1 UNION ALL
  SELECT 'b', 1 UNION ALL
  SELECT 'b', 2
)
SELECT grp, val FROM data ORDER BY grp, val", parameters: null);
		var rows = result.ToList();
		Assert.Equal(4, rows.Count);
		Assert.Equal("a", rows[0][0]?.ToString());
		Assert.Equal("1", rows[0][1]?.ToString());
		Assert.Equal("b", rows[3][0]?.ToString());
		Assert.Equal("2", rows[3][1]?.ToString());
	}

	[Fact(Skip = "Emulator limitation")]
	public async Task OrderBy_MixedAscDesc()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH data AS (
  SELECT 'a' AS grp, 2 AS val UNION ALL
  SELECT 'a', 1 UNION ALL
  SELECT 'b', 1 UNION ALL
  SELECT 'b', 2
)
SELECT grp, val FROM data ORDER BY grp ASC, val DESC", parameters: null);
		var rows = result.ToList();
		Assert.Equal(4, rows.Count);
		Assert.Equal("a", rows[0][0]?.ToString());
		Assert.Equal("2", rows[0][1]?.ToString());
		Assert.Equal("b", rows[3][0]?.ToString());
		Assert.Equal("1", rows[3][1]?.ToString());
	}

	// ---- LIMIT / OFFSET combinations ----
	[Fact] public async Task Limit_Zero() { var rows = await Column("SELECT x FROM UNNEST([1,2,3]) AS x ORDER BY x LIMIT 0"); Assert.Empty(rows); }
	[Fact] public async Task Limit_One() { var rows = await Column("SELECT x FROM UNNEST([1,2,3]) AS x ORDER BY x LIMIT 1"); Assert.Single(rows); Assert.Equal("1", rows[0]); }
	[Fact] public async Task Limit_Many() { var rows = await Column("SELECT x FROM UNNEST([1,2,3]) AS x ORDER BY x LIMIT 100"); Assert.Equal(3, rows.Count); }
	[Fact] public async Task Offset_Skip() { var rows = await Column("SELECT x FROM UNNEST([1,2,3,4,5]) AS x ORDER BY x LIMIT 2 OFFSET 2"); Assert.Equal(2, rows.Count); Assert.Equal("3", rows[0]); }

	// ---- Aliases ----
	[Fact] public async Task Alias_Column() => Assert.Equal("42", await Scalar("SELECT 42 AS my_value"));
	[Fact] public async Task Alias_Expression() => Assert.Equal("6", await Scalar("SELECT 2 * 3 AS result"));
	[Fact] public async Task Alias_Table() => Assert.Equal("3", await Scalar("SELECT COUNT(*) FROM UNNEST([1,2,3]) AS t"));

	// ---- Nested subquery patterns ----
	[Fact(Skip = "Emulator limitation")]
	public async Task FromSubquery()
	{
		var rows = await Column("SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) AS t ORDER BY x");
		Assert.Equal(3, rows.Count);
	}

	[Fact(Skip = "Emulator limitation")]
	public async Task FromSubquery_WithWhere()
	{
		var rows = await Column("SELECT x FROM (SELECT x FROM UNNEST([1,2,3,4,5]) AS x) AS t WHERE x > 3 ORDER BY x");
		Assert.Equal(2, rows.Count);
	}

	// ---- SELECT expressions ----
	[Fact] public async Task SelectLiteral_Int() => Assert.Equal("42", await Scalar("SELECT 42"));
	[Fact] public async Task SelectLiteral_Str() => Assert.Equal("hello", await Scalar("SELECT 'hello'"));
	[Fact] public async Task SelectLiteral_Bool() => Assert.Equal("True", await Scalar("SELECT TRUE"));
	[Fact] public async Task SelectLiteral_Float() => Assert.Equal("3.14", await Scalar("SELECT 3.14"));
	[Fact] public async Task SelectLiteral_Null() => Assert.Null(await Scalar("SELECT NULL"));
	[Fact] public async Task SelectExpression() => Assert.Equal("15", await Scalar("SELECT 5 + 10"));
	[Fact] public async Task SelectMultiExpr() { var client = await _fixture.GetClientAsync(); var r = await client.ExecuteQueryAsync("SELECT 1, 2, 3", parameters: null); var row = r.Single(); Assert.Equal("1", row[0]?.ToString()); Assert.Equal("3", row[2]?.ToString()); }
}
