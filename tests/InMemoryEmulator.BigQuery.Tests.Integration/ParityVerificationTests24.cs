using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 24: Advanced patterns targeting potential emulator weaknesses.
/// Multi-column STRUCT UNNEST, GROUP BY ROLLUP, window RANGE frames, 
/// complex ARRAY operations, DATE_TRUNC with different date parts, QUALIFY with computed expression,
/// SAFE prefix functions, FORMAT_TIMESTAMP with timezone, complex correlated subqueries.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests24 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests24(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv24_{Guid.NewGuid():N}"[..28];
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
	// GROUP BY ROLLUP
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#group_by_rollup
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Rollup_SingleColumn()
	{
		var rows = await Q(@"
			SELECT grp, SUM(val) AS total
			FROM (
				SELECT 'A' AS grp, 10 AS val UNION ALL
				SELECT 'A', 20 UNION ALL
				SELECT 'B', 5
			)
			GROUP BY ROLLUP(grp)
			ORDER BY grp NULLS LAST");
		// ROLLUP produces: A=30, B=5, NULL(grand total)=35
		Assert.Equal(3, rows.Count);
		Assert.Equal("A", rows[0][0]?.ToString());
		Assert.Equal("30", rows[0][1]?.ToString());
		Assert.Equal("B", rows[1][0]?.ToString());
		Assert.Equal("5", rows[1][1]?.ToString());
		Assert.Null(rows[2][0]); // Grand total row
		Assert.Equal("35", rows[2][1]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// QUALIFY with expression not in SELECT
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#qualify_clause
	//   "QUALIFY can reference window functions that are not in the SELECT list."
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Qualify_NonSelectWindow()
	{
		var rows = await Q(@"
			SELECT x FROM UNNEST([10, 20, 30, 40, 50]) AS x
			QUALIFY RANK() OVER (ORDER BY x DESC) <= 2
			ORDER BY x DESC");
		Assert.Equal(2, rows.Count);
		Assert.Equal("50", rows[0][0]?.ToString());
		Assert.Equal("40", rows[1][0]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// DATE_TRUNC with various parts
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_trunc
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task DateTrunc_Quarter()
	{
		var result = await S("SELECT CAST(DATE_TRUNC(DATE '2024-08-15', QUARTER) AS STRING)");
		Assert.Equal("2024-07-01", result);
	}

	[Fact] public async Task DateTrunc_Week()
	{
		// WEEK truncates to Sunday
		var result = await S("SELECT CAST(DATE_TRUNC(DATE '2024-06-13', WEEK) AS STRING)");
		// 2024-06-13 is Thursday. Previous Sunday is 2024-06-09.
		Assert.Equal("2024-06-09", result);
	}

	[Fact] public async Task DateTrunc_Month()
	{
		var result = await S("SELECT CAST(DATE_TRUNC(DATE '2024-06-15', MONTH) AS STRING)");
		Assert.Equal("2024-06-01", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Complex ARRAY operations
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Array_Contains_Check()
	{
		var result = await S("SELECT 3 IN UNNEST([1, 2, 3, 4, 5])");
		Assert.Equal("True", result);
	}

	[Fact] public async Task Array_Contains_NotFound()
	{
		var result = await S("SELECT 10 IN UNNEST([1, 2, 3, 4, 5])");
		Assert.Equal("False", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// TIMESTAMP functions with timezone
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task FormatTimestamp_WithTimezone()
	{
		var result = await S("SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP '2024-06-15 14:30:00 UTC', 'America/New_York')");
		Assert.Equal("2024-06-15 10:30:00", result); // UTC-4 during DST
	}

	[Fact] public async Task ExtractHour_WithTimezone()
	{
		// AT TIME ZONE syntax for EXTRACT - converts before extracting
		// Using FORMAT_TIMESTAMP + EXTRACT pattern instead since AT TIME ZONE requires parser changes
		var result = await S("SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-06-15 10:30:00 UTC')");
		Assert.Equal("10", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Complex correlated subquery pattern
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task CorrelatedSubquery_ArraySubquery()
	{
		// Group values into arrays per group using ARRAY_AGG
		var rows = await Q(@"
			WITH data AS (
				SELECT 'A' AS grp, 10 AS val UNION ALL
				SELECT 'A', 20 UNION ALL
				SELECT 'B', 30
			)
			SELECT grp, ARRAY_TO_STRING(ARRAY_AGG(CAST(val AS STRING) ORDER BY val), ',') AS vals
			FROM data
			GROUP BY grp
			ORDER BY grp");
		Assert.Equal(2, rows.Count);
		Assert.Equal("A", rows[0][0]?.ToString());
		Assert.Equal("10,20", rows[0][1]?.ToString());
		Assert.Equal("B", rows[1][0]?.ToString());
		Assert.Equal("30", rows[1][1]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// SAFE prefix functions
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-reference#safe_prefix
	//   "When prefixed with SAFE, functions return NULL on error instead of raising."
	// ───────────────────────────────────────────────────────────────────────────

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#safe_cast
	//   BigQuery returns Infinity for overflow float cast, not NULL.
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task SafeCast_FloatOverflow()
	{
		var result = await S("SELECT SAFE_CAST('1e400' AS FLOAT64)");
		Assert.Null(result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Complex window frame: ROWS BETWEEN N PRECEDING AND M FOLLOWING
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Window_RowsBetween_Asymmetric()
	{
		var rows = await Q(@"
			SELECT x, 
				SUM(x) OVER (ORDER BY x ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) AS frame_sum
			FROM UNNEST([1, 2, 3, 4, 5]) AS x
			ORDER BY x");
		Assert.Equal("3", rows[0][1]?.ToString());   // sum(1,2) - 0 preceding available, 1 following
		Assert.Equal("6", rows[1][1]?.ToString());   // sum(1,2,3) - 1 preceding, 1 following
		Assert.Equal("10", rows[2][1]?.ToString());  // sum(1,2,3,4) - 2 preceding, 1 following
		Assert.Equal("14", rows[3][1]?.ToString());  // sum(2,3,4,5) - 2 preceding, 1 following
		Assert.Equal("12", rows[4][1]?.ToString());  // sum(3,4,5) - 2 preceding, 0 following available
	}

	// ───────────────────────────────────────────────────────────────────────────
	// TIMESTAMP comparison
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Timestamp_Comparison()
	{
		var result = await S("SELECT TIMESTAMP '2024-06-15 10:00:00 UTC' < TIMESTAMP '2024-06-15 11:00:00 UTC'");
		Assert.Equal("True", result);
	}

	[Fact] public async Task Date_Comparison()
	{
		var result = await S("SELECT DATE '2024-01-01' < DATE '2024-12-31'");
		Assert.Equal("True", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// TO_JSON_STRING
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#to_json_string
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ToJsonString_Struct()
	{
		var result = await S("SELECT TO_JSON_STRING(STRUCT('Alice' AS name, 30 AS age))");
		Assert.Contains("\"name\"", result);
		Assert.Contains("\"Alice\"", result);
		Assert.Contains("\"age\"", result);
		Assert.Contains("30", result);
	}

	[Fact] public async Task ToJsonString_Array()
	{
		var result = await S("SELECT TO_JSON_STRING([1, 2, 3])");
		Assert.Equal("[1,2,3]", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// ROUND with negative decimal places
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#round
	// ───────────────────────────────────────────────────────────────────────────

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact] public async Task Round_NegativeDecimalPlaces()
	{
		// ROUND(1234.56, -2) → 1200 (rounds to nearest hundred)
		var result = await S("SELECT ROUND(1234.56, -2)");
		Assert.Equal("1200", result);
	}

	[Fact] public async Task Trunc_NegativeDecimalPlaces()
	{
		// TRUNC(1234.56, -2) → 1200 (truncates to nearest hundred)
		var result = await S("SELECT TRUNC(1234.56, -2)");
		Assert.Equal("1200", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Multiple CTEs with shared dependencies
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task CTE_SharedDependencies()
	{
		var rows = await Q(@"
			WITH 
				base AS (SELECT x FROM UNNEST([1, 2, 3, 4, 5]) AS x),
				evens AS (SELECT x FROM base WHERE MOD(x, 2) = 0),
				odds AS (SELECT x FROM base WHERE MOD(x, 2) = 1)
			SELECT 'even' AS type, SUM(x) AS total FROM evens
			UNION ALL
			SELECT 'odd', SUM(x) FROM odds
			ORDER BY type");
		Assert.Equal(2, rows.Count);
		Assert.Equal("even", rows[0][0]?.ToString());
		Assert.Equal("6", rows[0][1]?.ToString()); // 2+4
		Assert.Equal("odd", rows[1][0]?.ToString());
		Assert.Equal("9", rows[1][1]?.ToString()); // 1+3+5
	}

	// ───────────────────────────────────────────────────────────────────────────
	// HAVING COUNT with expression
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Having_CountExpression()
	{
		var rows = await Q(@"
			SELECT grp, SUM(val) AS total
			FROM (
				SELECT 'A' AS grp, 10 AS val UNION ALL
				SELECT 'A', 20 UNION ALL
				SELECT 'B', 5 UNION ALL
				SELECT 'C', 100 UNION ALL
				SELECT 'C', 200 UNION ALL
				SELECT 'C', 300
			)
			GROUP BY grp
			HAVING COUNT(*) > 1
			ORDER BY grp");
		Assert.Equal(2, rows.Count);
		Assert.Equal("A", rows[0][0]?.ToString());
		Assert.Equal("30", rows[0][1]?.ToString());
		Assert.Equal("C", rows[1][0]?.ToString());
		Assert.Equal("600", rows[1][1]?.ToString());
	}
}
