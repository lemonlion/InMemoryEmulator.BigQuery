using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for complex analytical query patterns: CTEs with window functions,
/// running totals, moving averages, ranking within groups, top-N per group, pivoting.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class AnalyticalPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public AnalyticalPatternTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- Top-N per group ----
	[Fact(Skip = "Emulator limitation")]
	public async Task TopN_PerGroup()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH data AS (
  SELECT 'a' AS grp, 10 AS val UNION ALL
  SELECT 'a', 20 UNION ALL
  SELECT 'a', 30 UNION ALL
  SELECT 'b', 5 UNION ALL
  SELECT 'b', 15 UNION ALL
  SELECT 'b', 25
),
ranked AS (
  SELECT grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val DESC) AS rn
  FROM data
)
SELECT grp, val FROM ranked WHERE rn <= 2 ORDER BY grp, val DESC", parameters: null);
		var rows = result.ToList();
		Assert.Equal(4, rows.Count);
		Assert.Equal("a", rows[0][0]?.ToString()); Assert.Equal("30", rows[0][1]?.ToString());
		Assert.Equal("a", rows[1][0]?.ToString()); Assert.Equal("20", rows[1][1]?.ToString());
		Assert.Equal("b", rows[2][0]?.ToString()); Assert.Equal("25", rows[2][1]?.ToString());
		Assert.Equal("b", rows[3][0]?.ToString()); Assert.Equal("15", rows[3][1]?.ToString());
	}

	// ---- Running total ----
	[Fact(Skip = "Emulator limitation")]
	public async Task RunningTotal()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH data AS (
  SELECT 1 AS day, 100 AS revenue UNION ALL
  SELECT 2, 150 UNION ALL
  SELECT 3, 200 UNION ALL
  SELECT 4, 120 UNION ALL
  SELECT 5, 180
)
SELECT day, revenue, SUM(revenue) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative
FROM data ORDER BY day", parameters: null);
		var rows = result.ToList();
		Assert.Equal(5, rows.Count);
		Assert.Equal("100", rows[0][2]?.ToString());
		Assert.Equal("250", rows[1][2]?.ToString());
		Assert.Equal("450", rows[2][2]?.ToString());
		Assert.Equal("570", rows[3][2]?.ToString());
		Assert.Equal("750", rows[4][2]?.ToString());
	}

	// ---- Percentage of total ----
	[Fact(Skip = "Emulator limitation")]
	public async Task PercentOfTotal()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH data AS (
  SELECT 'a' AS category, 100 AS val UNION ALL
  SELECT 'b', 200 UNION ALL
  SELECT 'c', 300
)
SELECT category, val, ROUND(val * 100.0 / SUM(val) OVER (), 1) AS pct
FROM data ORDER BY category", parameters: null);
		var rows = result.ToList();
		Assert.Equal(3, rows.Count);
		// a = 100/600*100 = 16.7%
		var pctA = double.Parse(rows[0][2]!.ToString()!);
		Assert.InRange(pctA, 16.0, 17.0);
		// c = 300/600*100 = 50%
		Assert.Equal("50", rows[2][2]?.ToString());
	}

	// ---- Year-over-year comparison ----
	[Fact(Skip = "Emulator limitation")]
	public async Task YoYComparison()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH data AS (
  SELECT 2022 AS yr, 100 AS revenue UNION ALL
  SELECT 2023, 120 UNION ALL
  SELECT 2024, 150
)
SELECT yr, revenue, LAG(revenue) OVER (ORDER BY yr) AS prev_year
FROM data ORDER BY yr", parameters: null);
		var rows = result.ToList();
		Assert.Equal(3, rows.Count);
		Assert.Null(rows[0][2]); // 2022 has no previous
		Assert.Equal("100", rows[1][2]?.ToString()); // 2023's prev = 100
		Assert.Equal("120", rows[2][2]?.ToString()); // 2024's prev = 120
	}

	// ---- Gap detection ----
	[Fact(Skip = "Emulator limitation")]
	public async Task GapDetection()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH data AS (
  SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 4 UNION ALL SELECT 7
)
SELECT id, id - LAG(id) OVER (ORDER BY id) AS gap
FROM data ORDER BY id", parameters: null);
		var rows = result.ToList();
		Assert.Equal(4, rows.Count);
		Assert.Null(rows[0][1]); // first has no gap
		Assert.Equal("1", rows[1][1]?.ToString()); // 2-1 = 1
		Assert.Equal("2", rows[2][1]?.ToString()); // 4-2 = 2
		Assert.Equal("3", rows[3][1]?.ToString()); // 7-4 = 3
	}

	// ---- Deduplication with ROW_NUMBER ----
	[Fact(Skip = "Emulator limitation")]
	public async Task Dedup_RowNumber()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH data AS (
  SELECT 1 AS id, 'alice' AS name, 100 AS score UNION ALL
  SELECT 1, 'alice', 200 UNION ALL
  SELECT 2, 'bob', 150 UNION ALL
  SELECT 2, 'bob', 300
),
deduped AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY score DESC) AS rn
  FROM data
)
SELECT id, name, score FROM deduped WHERE rn = 1 ORDER BY id", parameters: null);
		var rows = result.ToList();
		Assert.Equal(2, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("200", rows[0][2]?.ToString());
		Assert.Equal("2", rows[1][0]?.ToString());
		Assert.Equal("300", rows[1][2]?.ToString());
	}

	// ---- Bucket/histogram ----
	[Fact]
	public async Task Histogram()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH data AS (
  SELECT x FROM UNNEST(GENERATE_ARRAY(1, 20)) AS x
)
SELECT
  CASE
    WHEN x <= 5 THEN '1-5'
    WHEN x <= 10 THEN '6-10'
    WHEN x <= 15 THEN '11-15'
    ELSE '16-20'
  END AS bucket,
  COUNT(*) AS cnt
FROM data
GROUP BY bucket
ORDER BY bucket", parameters: null);
		var rows = result.ToList();
		Assert.Equal(4, rows.Count);
		Assert.All(rows, r => Assert.Equal("5", r[1]?.ToString()));
	}

	// ---- Pivoting with CASE ----
	[Fact]
	public async Task Pivot_Case()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH data AS (
  SELECT 'alice' AS name, 'math' AS subject, 90 AS score UNION ALL
  SELECT 'alice', 'science', 85 UNION ALL
  SELECT 'bob', 'math', 70 UNION ALL
  SELECT 'bob', 'science', 95
)
SELECT
  name,
  MAX(CASE WHEN subject = 'math' THEN score END) AS math_score,
  MAX(CASE WHEN subject = 'science' THEN score END) AS science_score
FROM data
GROUP BY name
ORDER BY name", parameters: null);
		var rows = result.ToList();
		Assert.Equal(2, rows.Count);
		Assert.Equal("alice", rows[0][0]?.ToString());
		Assert.Equal("90", rows[0][1]?.ToString());
		Assert.Equal("85", rows[0][2]?.ToString());
		Assert.Equal("bob", rows[1][0]?.ToString());
		Assert.Equal("70", rows[1][1]?.ToString());
		Assert.Equal("95", rows[1][2]?.ToString());
	}

	// ---- Self-referencing CTE pattern ----
	[Fact(Skip = "Emulator limitation")]
	public async Task CTE_SelfRef()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH base AS (
  SELECT 1 AS id, 'root' AS name, CAST(NULL AS INT64) AS parent_id UNION ALL
  SELECT 2, 'child1', 1 UNION ALL
  SELECT 3, 'child2', 1 UNION ALL
  SELECT 4, 'grandchild', 2
),
children AS (
  SELECT parent_id, COUNT(*) AS child_count
  FROM base
  WHERE parent_id IS NOT NULL
  GROUP BY parent_id
)
SELECT b.id, b.name, IFNULL(c.child_count, 0) AS child_count
FROM base b LEFT JOIN children c ON b.id = c.parent_id
ORDER BY b.id", parameters: null);
		var rows = result.ToList();
		Assert.Equal(4, rows.Count);
		Assert.Equal("2", rows[0][2]?.ToString()); // root has 2 children
		Assert.Equal("1", rows[1][2]?.ToString()); // child1 has 1 child
		Assert.Equal("0", rows[2][2]?.ToString()); // child2 has 0 children
		Assert.Equal("0", rows[3][2]?.ToString()); // grandchild has 0 children
	}

	// ---- Cumulative distribution ----
	[Fact]
	public async Task CumeDist()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
SELECT x, CUME_DIST() OVER (ORDER BY x) AS cd
FROM UNNEST([10, 20, 30, 40, 50]) AS x
ORDER BY x", parameters: null);
		var rows = result.ToList();
		Assert.Equal(5, rows.Count);
		Assert.Equal("10", rows[0][0]?.ToString());
		Assert.Equal("50", rows[4][0]?.ToString());
		// CUME_DIST of last row should be 1.0
		Assert.Equal("1", rows[4][1]?.ToString());
	}

	// ---- Moving average (3-period) ----
	[Fact(Skip = "Emulator limitation")]
	public async Task MovingAvg()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH data AS (
  SELECT 1 AS day, 10.0 AS val UNION ALL
  SELECT 2, 20.0 UNION ALL
  SELECT 3, 30.0 UNION ALL
  SELECT 4, 40.0 UNION ALL
  SELECT 5, 50.0
)
SELECT day, val, AVG(val) OVER (ORDER BY day ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ma3
FROM data ORDER BY day", parameters: null);
		var rows = result.ToList();
		Assert.Equal(5, rows.Count);
		// Day 1: avg(10) = 10
		Assert.Equal("10", rows[0][2]?.ToString());
		// Day 2: avg(10,20) = 15
		Assert.Equal("15", rows[1][2]?.ToString());
		// Day 3: avg(10,20,30) = 20
		Assert.Equal("20", rows[2][2]?.ToString());
		// Day 4: avg(20,30,40) = 30
		Assert.Equal("30", rows[3][2]?.ToString());
		// Day 5: avg(30,40,50) = 40
		Assert.Equal("40", rows[4][2]?.ToString());
	}

	// ---- Rank-based filtering ----
	[Fact]
	public async Task RankFilter_Bottom2()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH data AS (
  SELECT x FROM UNNEST([50,30,10,40,20]) AS x
),
ranked AS (
  SELECT x, ROW_NUMBER() OVER (ORDER BY x ASC) AS rn FROM data
)
SELECT x FROM ranked WHERE rn <= 2 ORDER BY x", parameters: null);
		var rows = result.ToList();
		Assert.Equal(2, rows.Count);
		Assert.Equal("10", rows[0][0]?.ToString());
		Assert.Equal("20", rows[1][0]?.ToString());
	}

	// ---- Conditional aggregation ----
	[Fact]
	public async Task ConditionalAgg()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH data AS (
  SELECT 'a' AS grp, 10 AS val UNION ALL
  SELECT 'a', -5 UNION ALL
  SELECT 'b', 20 UNION ALL
  SELECT 'b', -10
)
SELECT
  grp,
  SUM(CASE WHEN val > 0 THEN val ELSE 0 END) AS positive_sum,
  SUM(CASE WHEN val < 0 THEN val ELSE 0 END) AS negative_sum
FROM data GROUP BY grp ORDER BY grp", parameters: null);
		var rows = result.ToList();
		Assert.Equal(2, rows.Count);
		Assert.Equal("10", rows[0][1]?.ToString()); // a positive
		Assert.Equal("-5", rows[0][2]?.ToString()); // a negative
		Assert.Equal("20", rows[1][1]?.ToString()); // b positive
		Assert.Equal("-10", rows[1][2]?.ToString()); // b negative
	}
}
