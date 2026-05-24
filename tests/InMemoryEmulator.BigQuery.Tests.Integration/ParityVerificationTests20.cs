using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 20: Advanced aggregation, window frame semantics,
/// complex CTE patterns, EXCEPT/INTERSECT with multiple columns, TABLESAMPLE-style patterns,
/// set ops, analytical functions, error handling edge cases.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests20 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests20(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv20_{Guid.NewGuid():N}"[..28];
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
	// EXCEPT DISTINCT / INTERSECT DISTINCT
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ExceptDistinct_Basic()
	{
		var rows = await Q(@"
			SELECT x FROM UNNEST([1, 2, 3, 4, 5]) AS x
			EXCEPT DISTINCT
			SELECT x FROM UNNEST([3, 4, 5, 6, 7]) AS x
			ORDER BY x");
		Assert.Equal(2, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("2", rows[1][0]?.ToString());
	}

	[Fact] public async Task IntersectDistinct_Basic()
	{
		var rows = await Q(@"
			SELECT x FROM UNNEST([1, 2, 3, 4, 5]) AS x
			INTERSECT DISTINCT
			SELECT x FROM UNNEST([3, 4, 5, 6, 7]) AS x
			ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("3", rows[0][0]?.ToString());
		Assert.Equal("4", rows[1][0]?.ToString());
		Assert.Equal("5", rows[2][0]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Window ROWS frame with PRECEDING/FOLLOWING
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls#def_window_frame
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Window_RunningSumRows()
	{
		var rows = await Q(@"
			SELECT x, SUM(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running
			FROM UNNEST([1, 2, 3, 4, 5]) AS x
			ORDER BY x");
		Assert.Equal("1", rows[0][1]?.ToString());
		Assert.Equal("3", rows[1][1]?.ToString());  // 1+2
		Assert.Equal("6", rows[2][1]?.ToString());  // 1+2+3
		Assert.Equal("10", rows[3][1]?.ToString()); // 1+2+3+4
		Assert.Equal("15", rows[4][1]?.ToString()); // 1+2+3+4+5
	}

	[Fact] public async Task Window_MovingAvg3()
	{
		var rows = await Q(@"
			SELECT x, AVG(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS avg3
			FROM UNNEST([10, 20, 30, 40, 50]) AS x
			ORDER BY x");
		Assert.Equal("15", rows[0][1]?.ToString());  // avg(10,20) = 15
		Assert.Equal("20", rows[1][1]?.ToString()); // avg(10,20,30) = 20
		Assert.Equal("30", rows[2][1]?.ToString()); // avg(20,30,40) = 30
		Assert.Equal("40", rows[3][1]?.ToString()); // avg(30,40,50) = 40
		Assert.Equal("45", rows[4][1]?.ToString()); // avg(40,50) = 45
	}

	[Fact] public async Task Window_LeadLag()
	{
		var rows = await Q(@"
			SELECT x,
				LAG(x, 1) OVER (ORDER BY x) AS prev_val,
				LEAD(x, 1) OVER (ORDER BY x) AS next_val
			FROM UNNEST([10, 20, 30]) AS x
			ORDER BY x");
		Assert.Null(rows[0][1]);
		Assert.Equal("20", rows[0][2]?.ToString());
		Assert.Equal("10", rows[1][1]?.ToString());
		Assert.Equal("30", rows[1][2]?.ToString());
		Assert.Equal("20", rows[2][1]?.ToString());
		Assert.Null(rows[2][2]);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// COUNTIF / SUM with CASE
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task CountIf_Basic()
	{
		var result = await S("SELECT COUNTIF(x > 3) FROM UNNEST([1, 2, 3, 4, 5]) AS x");
		Assert.Equal("2", result);
	}

	[Fact] public async Task SumCase_ConditionalAgg()
	{
		var result = await S(@"
			SELECT SUM(CASE WHEN x > 3 THEN x ELSE 0 END) 
			FROM UNNEST([1, 2, 3, 4, 5]) AS x");
		Assert.Equal("9", result); // 4+5
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Multiple UNION ALL
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task UnionAll_Three()
	{
		var rows = await Q(@"
			SELECT 1 AS x
			UNION ALL SELECT 2
			UNION ALL SELECT 3
			ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("2", rows[1][0]?.ToString());
		Assert.Equal("3", rows[2][0]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// CTE with multiple references
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task CTE_MultipleReferences()
	{
		var result = await S(@"
			WITH vals AS (SELECT x FROM UNNEST([1, 2, 3]) AS x)
			SELECT SUM(a.x + b.x) FROM vals a CROSS JOIN vals b");
		Assert.Equal("36", result); // Each pair sums: (1+1)+(1+2)+(1+3)+(2+1)+...+(3+3) = 36
	}

	[Fact] public async Task CTE_ChainedReferences()
	{
		var result = await S(@"
			WITH 
				step1 AS (SELECT x FROM UNNEST([1, 2, 3]) AS x),
				step2 AS (SELECT x * 2 AS doubled FROM step1)
			SELECT SUM(doubled) FROM step2");
		Assert.Equal("12", result); // 2+4+6=12
	}

	// ───────────────────────────────────────────────────────────────────────────
	// HAVING with aggregate functions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Having_MultipleConditions()
	{
		var rows = await Q(@"
			SELECT grp, COUNT(*) AS cnt, SUM(val) AS total
			FROM (
				SELECT 'A' AS grp, 10 AS val UNION ALL
				SELECT 'A', 20 UNION ALL
				SELECT 'A', 30 UNION ALL
				SELECT 'B', 5 UNION ALL
				SELECT 'B', 15
			)
			GROUP BY grp
			HAVING COUNT(*) >= 3 AND SUM(val) > 50
			ORDER BY grp");
		Assert.Single(rows);
		Assert.Equal("A", rows[0][0]?.ToString());
		Assert.Equal("3", rows[0][1]?.ToString());
		Assert.Equal("60", rows[0][2]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Nested subquery in SELECT
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ScalarSubquery_InSelect()
	{
		var result = await S(@"
			SELECT (SELECT MAX(x) FROM UNNEST([1, 2, 3, 4, 5]) AS x)");
		Assert.Equal("5", result);
	}

	[Fact] public async Task ScalarSubquery_Correlated()
	{
		var rows = await Q(@"
			SELECT x, (SELECT SUM(y) FROM UNNEST([1, 2, 3]) AS y WHERE y <= x) AS sum_up_to
			FROM UNNEST([1, 2, 3]) AS x
			ORDER BY x");
		Assert.Equal("1", rows[0][1]?.ToString());  // sum where y<=1: 1
		Assert.Equal("3", rows[1][1]?.ToString());  // sum where y<=2: 1+2=3
		Assert.Equal("6", rows[2][1]?.ToString());  // sum where y<=3: 1+2+3=6
	}

	// ───────────────────────────────────────────────────────────────────────────
	// ARRAY_AGG with STRUCT
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ArrayAgg_OrderByDesc()
	{
		var result = await S(@"
			SELECT ARRAY_TO_STRING(ARRAY_AGG(CAST(x AS STRING) ORDER BY x DESC), ',')
			FROM UNNEST([3, 1, 4, 1, 5]) AS x");
		Assert.Equal("5,4,3,1,1", result);
	}

	[Fact] public async Task ArrayAgg_Distinct()
	{
		var result = await S(@"
			SELECT ARRAY_LENGTH(ARRAY_AGG(DISTINCT x))
			FROM UNNEST([1, 2, 2, 3, 3, 3]) AS x");
		Assert.Equal("3", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// GENERATE_DATE_ARRAY
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_date_array
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task GenerateDateArray_Daily()
	{
		var result = await S(@"
			SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-07', INTERVAL 1 DAY))");
		Assert.Equal("7", result);
	}

	[Fact] public async Task GenerateDateArray_Monthly()
	{
		var result = await S(@"
			SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-06-01', INTERVAL 1 MONTH))");
		Assert.Equal("6", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Arithmetic overflow: INT64 boundaries
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Cast_LargeInt()
	{
		var result = await S("SELECT CAST(9223372036854775807 AS INT64)");
		Assert.Equal("9223372036854775807", result); // INT64 max
	}

	// ───────────────────────────────────────────────────────────────────────────
	// BOOL expressions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Bool_AndOr_Precedence()
	{
		// AND has higher precedence than OR
		var result = await S("SELECT TRUE OR FALSE AND FALSE");
		Assert.Equal("True", result); // TRUE OR (FALSE AND FALSE) = TRUE OR FALSE = TRUE
	}

	[Fact] public async Task Bool_Not()
	{
		var result = await S("SELECT NOT FALSE");
		Assert.Equal("True", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Nested CASE expressions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task NestedCase()
	{
		var result = await S(@"
			SELECT CASE 
				WHEN 5 > 3 THEN 
					CASE WHEN 5 > 4 THEN 'big' ELSE 'medium' END
				ELSE 'small'
			END");
		Assert.Equal("big", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// GROUP BY with expression
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task GroupBy_Expression()
	{
		var rows = await Q(@"
			SELECT x > 2 AS big, COUNT(*) AS cnt
			FROM UNNEST([1, 2, 3, 4, 5]) AS x
			GROUP BY x > 2
			ORDER BY big");
		Assert.Equal(2, rows.Count);
		Assert.Equal("False", rows[0][0]?.ToString());
		Assert.Equal("2", rows[0][1]?.ToString());
		Assert.Equal("True", rows[1][0]?.ToString());
		Assert.Equal("3", rows[1][1]?.ToString());
	}
}
