using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for CTE (WITH clause) features: simple, chained, recursive, with DML.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_clause
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class CteComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public CteComprehensiveTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql, parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Query(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql, parameters: null); return r.ToList(); }

	// ---- Simple CTE ----
	[Fact] public async Task Cte_Simple()
	{
		var v = await Scalar("WITH t AS (SELECT 42 AS x) SELECT x FROM t");
		Assert.Equal("42", v);
	}

	// ---- CTE with multiple columns ----
	[Fact] public async Task Cte_MultiColumn()
	{
		var rows = await Query("WITH t AS (SELECT 1 AS a, 'hello' AS b) SELECT a, b FROM t");
		Assert.Equal("1", rows[0]["a"]?.ToString());
		Assert.Equal("hello", rows[0]["b"]?.ToString());
	}

	// ---- CTE with WHERE ----
	[Fact] public async Task Cte_WithWhere()
	{
		var rows = await Query("WITH nums AS (SELECT x FROM UNNEST([1,2,3,4,5]) AS x) SELECT x FROM nums WHERE x > 3 ORDER BY x");
		Assert.Equal(2, rows.Count);
		Assert.Equal("4", rows[0][0]?.ToString());
	}

	// ---- CTE with aggregation ----
	[Fact] public async Task Cte_WithAggregation()
	{
		var v = await Scalar("WITH nums AS (SELECT x FROM UNNEST([1,2,3,4,5]) AS x) SELECT SUM(x) FROM nums");
		Assert.Equal("15", v);
	}

	// ---- Multiple CTEs ----
	[Fact] public async Task Cte_Multiple()
	{
		var rows = await Query(@"
			WITH a AS (SELECT 1 AS x UNION ALL SELECT 2),
			     b AS (SELECT 10 AS y UNION ALL SELECT 20)
			SELECT a.x, b.y FROM a CROSS JOIN b ORDER BY a.x, b.y");
		Assert.Equal(4, rows.Count);
	}

	// ---- CTE referencing another CTE ----
	[Fact] public async Task Cte_ChainedReferences()
	{
		var v = await Scalar(@"
			WITH step1 AS (SELECT 5 AS v),
			     step2 AS (SELECT v * 2 AS v FROM step1)
			SELECT v FROM step2");
		Assert.Equal("10", v);
	}

	// ---- CTE with UNION ALL ----
	[Fact] public async Task Cte_WithUnionAll()
	{
		var rows = await Query(@"
			WITH combined AS (
				SELECT 'a' AS name UNION ALL SELECT 'b' UNION ALL SELECT 'c'
			)
			SELECT name FROM combined ORDER BY name");
		Assert.Equal(3, rows.Count);
	}

	// ---- CTE used multiple times ----
	[Fact] public async Task Cte_UsedMultipleTimes()
	{
		var rows = await Query(@"
			WITH nums AS (SELECT x FROM UNNEST([1,2,3]) AS x)
			SELECT a.x AS ax, b.x AS bx FROM nums a CROSS JOIN nums b WHERE a.x < b.x ORDER BY a.x, b.x");
		Assert.Equal(3, rows.Count); // (1,2), (1,3), (2,3)
	}

	// ---- CTE with nested subquery ----
	[Fact] public async Task Cte_WithSubquery()
	{
		var v = await Scalar(@"
			WITH t AS (SELECT x FROM UNNEST([1,2,3,4,5]) AS x WHERE x > 2)
			SELECT COUNT(*) FROM t");
		Assert.Equal("3", v);
	}

	// ---- CTE with ORDER BY and LIMIT ----
	[Fact] public async Task Cte_WithOrderByLimit()
	{
		var rows = await Query(@"
			WITH nums AS (SELECT x FROM UNNEST([5,3,1,4,2]) AS x)
			SELECT x FROM nums ORDER BY x LIMIT 3");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
	}

	// ---- CTE with GROUP BY ----
	[Fact] public async Task Cte_WithGroupBy()
	{
		var rows = await Query(@"
			WITH data AS (
				SELECT 'A' AS cat, 10 AS val UNION ALL
				SELECT 'B', 20 UNION ALL
				SELECT 'A', 30
			)
			SELECT cat, SUM(val) AS total FROM data GROUP BY cat ORDER BY cat");
		Assert.Equal(2, rows.Count);
		Assert.Equal("40", rows[0]["total"]?.ToString());
	}

	// ---- CTE with window function ----
	[Fact] public async Task Cte_WithWindowFunction()
	{
		var rows = await Query(@"
			WITH data AS (SELECT x FROM UNNEST([10,20,30]) AS x)
			SELECT x, ROW_NUMBER() OVER (ORDER BY x) AS rn FROM data ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]["rn"]?.ToString());
	}

	// ---- CTE with DISTINCT ----
	[Fact] public async Task Cte_WithDistinct()
	{
		var rows = await Query(@"
			WITH data AS (SELECT x FROM UNNEST([1,1,2,2,3]) AS x)
			SELECT DISTINCT x FROM data ORDER BY x");
		Assert.Equal(3, rows.Count);
	}

	// ---- CTE with JOIN ----
	[Fact] public async Task Cte_WithJoin()
	{
		var rows = await Query(@"
			WITH left_t AS (SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b'),
			     right_t AS (SELECT 1 AS id, 100 AS val UNION ALL SELECT 2, 200)
			SELECT l.name, r.val FROM left_t l JOIN right_t r ON l.id = r.id ORDER BY l.name");
		Assert.Equal(2, rows.Count);
		Assert.Equal("a", rows[0]["name"]?.ToString());
		Assert.Equal("100", rows[0]["val"]?.ToString());
	}

	// ---- CTE with CASE ----
	[Fact] public async Task Cte_WithCase()
	{
		var rows = await Query(@"
			WITH data AS (SELECT x FROM UNNEST([1,2,3]) AS x)
			SELECT CASE WHEN x > 2 THEN 'big' ELSE 'small' END AS label FROM data ORDER BY x");
		Assert.Equal("small", rows[0]["label"]?.ToString());
		Assert.Equal("big", rows[2]["label"]?.ToString());
	}

	// ---- CTE with computed columns ----
	[Fact] public async Task Cte_ComputedColumns()
	{
		var v = await Scalar(@"
			WITH t AS (SELECT 10 AS a, 20 AS b)
			SELECT a + b AS total FROM t");
		Assert.Equal("30", v);
	}

	// ---- CTE three levels deep ----
	[Fact] public async Task Cte_ThreeLevels()
	{
		var v = await Scalar(@"
			WITH l1 AS (SELECT 2 AS x),
			     l2 AS (SELECT x * 3 AS x FROM l1),
			     l3 AS (SELECT x + 4 AS x FROM l2)
			SELECT x FROM l3");
		Assert.Equal("10", v);
	}

	// ---- CTE with HAVING ----
	[Fact] public async Task Cte_WithHaving()
	{
		var rows = await Query(@"
			WITH data AS (
				SELECT 'A' AS cat, 1 AS v UNION ALL SELECT 'A', 2 UNION ALL SELECT 'B', 3
			)
			SELECT cat, COUNT(*) as cnt FROM data GROUP BY cat HAVING COUNT(*) > 1");
		Assert.Single(rows);
		Assert.Equal("A", rows[0]["cat"]?.ToString());
	}

	// ---- CTE with EXISTS ----
	[Fact] public async Task Cte_WithExists()
	{
		var v = await Scalar(@"
			WITH data AS (SELECT x FROM UNNEST([1,2,3]) AS x)
			SELECT EXISTS(SELECT 1 FROM data WHERE x > 2)");
		Assert.Equal("True", v);
	}

	// ---- Recursive CTE: simple counter ----
	[Fact] public async Task Cte_Recursive_Counter()
	{
		var rows = await Query(@"
			WITH RECURSIVE counter AS (
				SELECT 1 AS n
				UNION ALL
				SELECT n + 1 FROM counter WHERE n < 5
			)
			SELECT n FROM counter ORDER BY n");
		Assert.Equal(5, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("5", rows[4][0]?.ToString());
	}

	// ---- Recursive CTE: factorial ----
	[Fact] public async Task Cte_Recursive_Factorial()
	{
		var v = await Scalar(@"
			WITH RECURSIVE fact AS (
				SELECT 1 AS n, 1 AS f
				UNION ALL
				SELECT n + 1, f * (n + 1) FROM fact WHERE n < 5
			)
			SELECT f FROM fact WHERE n = 5");
		Assert.Equal("120", v);
	}

	// ---- Recursive CTE: fibonacci ----
	[Fact] public async Task Cte_Recursive_Fibonacci()
	{
		var rows = await Query(@"
			WITH RECURSIVE fib AS (
				SELECT 1 AS n, 1 AS a, 1 AS b
				UNION ALL
				SELECT n + 1, b, a + b FROM fib WHERE n < 8
			)
			SELECT a FROM fib ORDER BY n");
		Assert.Equal(8, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("21", rows[7][0]?.ToString());
	}

	// ---- Recursive CTE: generate series ----
	[Fact] public async Task Cte_Recursive_Series()
	{
		var v = await Scalar(@"
			WITH RECURSIVE series AS (
				SELECT 0 AS x
				UNION ALL
				SELECT x + 10 FROM series WHERE x < 50
			)
			SELECT COUNT(*) FROM series");
		Assert.Equal("6", v); // 0,10,20,30,40,50
	}

	// ---- CTE with string operations ----
	[Fact] public async Task Cte_StringOps()
	{
		var v = await Scalar(@"
			WITH t AS (SELECT 'hello world' AS s)
			SELECT UPPER(s) FROM t");
		Assert.Equal("HELLO WORLD", v);
	}
}
