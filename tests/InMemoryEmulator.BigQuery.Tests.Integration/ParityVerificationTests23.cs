using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 23: Advanced scenarios testing rarely-tested paths.
/// Complex nested expressions, multi-level CTEs, window functions with PARTITION BY + ORDER BY,
/// QUALIFY with aliases, string escaping, large IN lists, complex CASE, type coercion,
/// aggregate of aggregates pattern, DISTINCT ON equivalents.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests23 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests23(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv23_{Guid.NewGuid():N}"[..28];
		await _fixture.CreateDatasetAsync(_ds);
	}

	public async ValueTask DisposeAsync()
	{
		try
		{
			var c = await _fixture.GetClientAsync();
			await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true });
		}
		catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null);
		var rows = result.ToList();
		return rows.Count == 0 ? null : rows[0][0]?.ToString();
	}

	private async Task<List<BigQueryRow>> Q(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null);
		return result.ToList();
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Complex nested subquery patterns
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task SubqueryInWhere()
	{
		var rows = await Q(@"
			SELECT x FROM UNNEST([1, 2, 3, 4, 5]) AS x
			WHERE x > (SELECT AVG(y) FROM UNNEST([1, 2, 3, 4, 5]) AS y)
			ORDER BY x");
		// AVG = 3.0, so x > 3 → [4, 5]
		Assert.Equal(2, rows.Count);
		Assert.Equal("4", rows[0][0]?.ToString());
		Assert.Equal("5", rows[1][0]?.ToString());
	}

	[Fact] public async Task SubqueryInFrom_Derived()
	{
		var rows = await Q(@"
			SELECT doubled FROM (
				SELECT x * 2 AS doubled FROM UNNEST([1, 2, 3]) AS x
			) WHERE doubled > 3
			ORDER BY doubled");
		Assert.Equal(2, rows.Count);
		Assert.Equal("4", rows[0][0]?.ToString());
		Assert.Equal("6", rows[1][0]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Multi-level CTEs (3+ levels)
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task CTE_ThreeLevels()
	{
		var result = await S(@"
			WITH 
				l1 AS (SELECT x FROM UNNEST([1, 2, 3, 4, 5]) AS x),
				l2 AS (SELECT x, x*x AS sq FROM l1 WHERE x > 2),
				l3 AS (SELECT SUM(sq) AS total FROM l2)
			SELECT total FROM l3");
		// x > 2: [3,4,5], squares: [9,16,25], sum = 50
		Assert.Equal("50", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Window with PARTITION BY and aggregate filter
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Window_PartitionBy_MultipleAggregates()
	{
		var rows = await Q(@"
			SELECT grp, val,
				SUM(val) OVER (PARTITION BY grp) AS grp_sum,
				COUNT(*) OVER (PARTITION BY grp) AS grp_cnt,
				SUM(val) OVER () AS total_sum
			FROM (
				SELECT 'A' AS grp, 10 AS val UNION ALL
				SELECT 'A', 20 UNION ALL
				SELECT 'B', 5 UNION ALL
				SELECT 'B', 15 UNION ALL
				SELECT 'B', 25
			)
			ORDER BY grp, val");
		// Group A: sum=30, cnt=2
		Assert.Equal("A", rows[0][0]?.ToString());
		Assert.Equal("10", rows[0][1]?.ToString());
		Assert.Equal("30", rows[0][2]?.ToString());
		Assert.Equal("2", rows[0][3]?.ToString());
		Assert.Equal("75", rows[0][4]?.ToString()); // total: 10+20+5+15+25=75
		// Group B: sum=45, cnt=3
		Assert.Equal("B", rows[2][0]?.ToString());
		Assert.Equal("45", rows[2][2]?.ToString());
		Assert.Equal("3", rows[2][3]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Complex CASE with multiple WHEN branches
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Case_MultipleWhenBranches()
	{
		var rows = await Q(@"
			SELECT x,
				CASE
					WHEN x < 2 THEN 'low'
					WHEN x < 4 THEN 'medium'
					WHEN x < 6 THEN 'high'
					ELSE 'very high'
				END AS category
			FROM UNNEST([1, 3, 5, 7]) AS x
			ORDER BY x");
		Assert.Equal("low", rows[0][1]?.ToString());
		Assert.Equal("medium", rows[1][1]?.ToString());
		Assert.Equal("high", rows[2][1]?.ToString());
		Assert.Equal("very high", rows[3][1]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Type coercion in UNION ALL
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#union
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task UnionAll_TypeCoercion()
	{
		var rows = await Q(@"
			SELECT 1 AS x
			UNION ALL SELECT 2
			UNION ALL SELECT 3
			ORDER BY x");
		Assert.Equal(3, rows.Count);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// String escape sequences
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task String_WithNewline()
	{
		var result = await S("SELECT LENGTH('line1\\nline2')");
		Assert.Equal("11", result); // \n is one character
	}

	[Fact] public async Task String_WithTab()
	{
		var result = await S("SELECT LENGTH('a\\tb')");
		Assert.Equal("3", result); // \t is one character
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Complex aggregate expressions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Agg_AverageOfSquares()
	{
		var result = await S(@"
			SELECT AVG(x * x) FROM UNNEST([1, 2, 3, 4, 5]) AS x");
		// avg of [1,4,9,16,25] = 55/5 = 11
		Assert.Equal("11", result);
	}

	[Fact] public async Task Agg_SumMinusMin()
	{
		var result = await S(@"
			SELECT SUM(x) - MIN(x) FROM UNNEST([1, 2, 3, 4, 5]) AS x");
		// 15 - 1 = 14
		Assert.Equal("14", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// DISTINCT with ordering
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task SelectDistinct_WithOrderBy()
	{
		var rows = await Q(@"
			SELECT DISTINCT x FROM UNNEST([3, 1, 2, 1, 3, 2]) AS x ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("2", rows[1][0]?.ToString());
		Assert.Equal("3", rows[2][0]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// TOP N per group pattern (common BigQuery analytics)
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task TopNPerGroup()
	{
		var rows = await Q(@"
			WITH data AS (
				SELECT 'A' AS grp, 10 AS val UNION ALL
				SELECT 'A', 20 UNION ALL
				SELECT 'A', 30 UNION ALL
				SELECT 'B', 5 UNION ALL
				SELECT 'B', 15 UNION ALL
				SELECT 'B', 25
			),
			ranked AS (
				SELECT grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val DESC) AS rn
				FROM data
			)
			SELECT grp, val FROM ranked WHERE rn <= 2 ORDER BY grp, val DESC");
		// Top 2 per group
		Assert.Equal(4, rows.Count);
		Assert.Equal("A", rows[0][0]?.ToString());
		Assert.Equal("30", rows[0][1]?.ToString());
		Assert.Equal("A", rows[1][0]?.ToString());
		Assert.Equal("20", rows[1][1]?.ToString());
		Assert.Equal("B", rows[2][0]?.ToString());
		Assert.Equal("25", rows[2][1]?.ToString());
		Assert.Equal("B", rows[3][0]?.ToString());
		Assert.Equal("15", rows[3][1]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// NOT IN with NULLs
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#in_operators
	// ───────────────────────────────────────────────────────────────────────────

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact] public async Task NotIn_Basic()
	{
		var result = await S("SELECT 5 NOT IN (1, 2, 3)");
		Assert.Equal("True", result);
	}

	[Fact] public async Task NotIn_WithNull()
	{
		// 5 NOT IN (1, NULL, 3) → NULL (because NULL comparison unknown)
		var result = await S("SELECT 5 NOT IN (1, NULL, 3)");
		Assert.Null(result);
	}

	[Fact] public async Task NotIn_FoundDespiteNull()
	{
		// 1 NOT IN (1, NULL, 3) → FALSE (found match)
		var result = await S("SELECT 1 NOT IN (1, NULL, 3)");
		Assert.Equal("False", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// SAFE_CAST vs CAST behavior
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task SafeCast_InvalidTimestamp()
	{
		var result = await S("SELECT SAFE_CAST('badvalue' AS TIMESTAMP)");
		Assert.Null(result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Complex JOIN patterns
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task LeftJoin_NoMatch()
	{
		var rows = await Q(@"
			WITH a AS (SELECT x FROM UNNEST([1, 2, 3]) AS x),
			     b AS (SELECT y FROM UNNEST([10, 20]) AS y)
			SELECT a.x, b.y FROM a LEFT JOIN b ON a.x = b.y
			ORDER BY a.x");
		// No matches, so b.y is always NULL
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Null(rows[0][1]);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Mathematical edge cases
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Power_ZeroToZero()
	{
		// POW(0, 0) = 1 in BigQuery
		var result = await S("SELECT POW(0, 0)");
		Assert.Equal("1", result);
	}

	[Fact] public async Task Sqrt_Zero()
	{
		var result = await S("SELECT SQRT(0)");
		Assert.Equal("0", result);
	}

	[Fact] public async Task Abs_Negative()
	{
		var result = await S("SELECT ABS(-42)");
		Assert.Equal("42", result);
	}

	[Fact] public async Task Sign_Negative()
	{
		var result = await S("SELECT SIGN(-5)");
		Assert.Equal("-1", result);
	}

	[Fact] public async Task Sign_Zero()
	{
		var result = await S("SELECT SIGN(0)");
		Assert.Equal("0", result);
	}

	[Fact] public async Task Sign_Positive()
	{
		var result = await S("SELECT SIGN(5)");
		Assert.Equal("1", result);
	}
}
