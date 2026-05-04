using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Parity verification tests batch 12: TIMESTAMP_TRUNC ISOYEAR, window function edge cases,
/// complex CASE expressions, ARRAY functions, STRING_AGG with ORDER BY and LIMIT,
/// DATE_ADD/SUB edge cases, EXTRACT from timestamps, complex nested queries.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests12 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests12(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv12_{Guid.NewGuid():N}"[..28];
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
	// TIMESTAMP_TRUNC ISOYEAR
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_trunc
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task TimestampTrunc_Isoyear()
	{
		// ISO year 2024 starts Mon 2024-01-01
		var result = await S("SELECT CAST(TIMESTAMP_TRUNC(TIMESTAMP '2024-06-15 10:00:00 UTC', ISOYEAR) AS STRING)");
		Assert.Equal("2024-01-01 00:00:00+00", result);
	}

	[Fact] public async Task TimestampTrunc_Isoyear_2023()
	{
		// ISO year 2023 starts Mon 2023-01-02
		var result = await S("SELECT CAST(TIMESTAMP_TRUNC(TIMESTAMP '2023-06-15 10:00:00 UTC', ISOYEAR) AS STRING)");
		Assert.Equal("2023-01-02 00:00:00+00", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Window functions: running aggregates
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task RunningSum_WithFrame()
	{
		var rows = await Q(@"
			SELECT val, SUM(val) OVER (ORDER BY val ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS running
			FROM UNNEST([10, 20, 30, 40]) AS val
			ORDER BY val");
		// row0: only 10 (no preceding) = 10; row1: 10+20=30; row2: 20+30=50; row3: 30+40=70
		Assert.Equal("10", rows[0]["running"]?.ToString());
		Assert.Equal("30", rows[1]["running"]?.ToString());
		Assert.Equal("50", rows[2]["running"]?.ToString());
		Assert.Equal("70", rows[3]["running"]?.ToString());
	}

	[Fact] public async Task WindowAvg_BetweenPrecedingAndFollowing()
	{
		var rows = await Q(@"
			SELECT val, CAST(AVG(val) OVER (ORDER BY val ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS STRING) AS avg_val
			FROM UNNEST([10, 20, 30, 40, 50]) AS val
			ORDER BY val");
		// row0: (10+20)/2=15; row1: (10+20+30)/3=20; row2: (20+30+40)/3=30; row3: (30+40+50)/3=40; row4: (40+50)/2=45
		Assert.Equal("15.0", rows[0]["avg_val"]?.ToString());
		Assert.Equal("20.0", rows[1]["avg_val"]?.ToString());
		Assert.Equal("30.0", rows[2]["avg_val"]?.ToString());
		Assert.Equal("40.0", rows[3]["avg_val"]?.ToString());
		Assert.Equal("45.0", rows[4]["avg_val"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// EXTRACT from TIMESTAMP
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#extract
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Extract_DayOfWeek()
	{
		// BigQuery: DAYOFWEEK returns 1 (Sunday) through 7 (Saturday)
		// 2024-01-15 is Monday
		var result = await S("SELECT EXTRACT(DAYOFWEEK FROM TIMESTAMP '2024-01-15 10:00:00 UTC')");
		Assert.Equal("2", result); // Monday = 2
	}

	[Fact] public async Task Extract_DayOfYear()
	{
		var result = await S("SELECT EXTRACT(DAYOFYEAR FROM TIMESTAMP '2024-02-01 10:00:00 UTC')");
		Assert.Equal("32", result); // Jan has 31 days, Feb 1 = day 32
	}

	[Fact] public async Task Extract_IsoYear()
	{
		// 2023-01-01 is Sunday, belongs to ISO year 2022
		var result = await S("SELECT EXTRACT(ISOYEAR FROM DATE '2023-01-01')");
		Assert.Equal("2022", result);
	}

	[Fact] public async Task Extract_Isoweek()
	{
		// 2024-01-01 is Monday, ISO week 1
		var result = await S("SELECT EXTRACT(ISOWEEK FROM DATE '2024-01-01')");
		Assert.Equal("1", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// ARRAY functions
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ArrayLength_Empty()
	{
		var result = await S("SELECT ARRAY_LENGTH([])");
		Assert.Equal("0", result);
	}

	[Fact] public async Task ArrayReverse()
	{
		var result = await S("SELECT ARRAY_TO_STRING(ARRAY_REVERSE([1, 2, 3]), ',')");
		Assert.Equal("3,2,1", result);
	}

	[Fact] public async Task ArrayConcat()
	{
		var result = await S("SELECT ARRAY_TO_STRING(ARRAY_CONCAT([1, 2], [3, 4]), ',')");
		Assert.Equal("1,2,3,4", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// STRING_AGG with ORDER BY
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#string_agg
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task StringAgg_OrderBy()
	{
		var result = await S(@"
			SELECT STRING_AGG(x, ',' ORDER BY x)
			FROM UNNEST(['cherry', 'apple', 'banana']) AS x");
		Assert.Equal("apple,banana,cherry", result);
	}

	[Fact] public async Task StringAgg_OrderByDesc()
	{
		var result = await S(@"
			SELECT STRING_AGG(x, '-' ORDER BY x DESC)
			FROM UNNEST(['cherry', 'apple', 'banana']) AS x");
		Assert.Equal("cherry-banana-apple", result);
	}

	[Fact] public async Task StringAgg_Distinct()
	{
		var result = await S(@"
			SELECT STRING_AGG(DISTINCT x, ',' ORDER BY x)
			FROM UNNEST(['a', 'b', 'a', 'c', 'b']) AS x");
		Assert.Equal("a,b,c", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// DATE_ADD / DATE_SUB with different parts
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_add
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task DateAdd_Quarter()
	{
		var result = await S("SELECT CAST(DATE_ADD(DATE '2024-01-15', INTERVAL 1 QUARTER) AS STRING)");
		Assert.Equal("2024-04-15", result);
	}

	[Fact] public async Task DateSub_Year()
	{
		var result = await S("SELECT CAST(DATE_SUB(DATE '2024-03-15', INTERVAL 1 YEAR) AS STRING)");
		Assert.Equal("2023-03-15", result);
	}

	[Fact] public async Task DateAdd_Month_EndOfMonth()
	{
		// Adding 1 month to Jan 31 → Feb 29 in leap year 2024
		var result = await S("SELECT CAST(DATE_ADD(DATE '2024-01-31', INTERVAL 1 MONTH) AS STRING)");
		Assert.Equal("2024-02-29", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Complex CASE with multiple WHEN and aggregates
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Case_InAggregate()
	{
		var result = await S(@"
			SELECT SUM(CASE WHEN x > 3 THEN x ELSE 0 END)
			FROM UNNEST([1, 2, 3, 4, 5]) AS x");
		Assert.Equal("9", result); // 4+5
	}

	[Fact] public async Task Case_WithNull()
	{
		var result = await S(@"
			SELECT CASE WHEN NULL THEN 'yes' ELSE 'no' END");
		Assert.Equal("no", result); // NULL is not TRUE
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Subquery in FROM
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task SubqueryInFrom()
	{
		var result = await S(@"
			SELECT SUM(doubled) FROM (
				SELECT x * 2 AS doubled FROM UNNEST([1, 2, 3]) AS x
			)");
		Assert.Equal("12", result); // 2+4+6
	}

	[Fact] public async Task SubqueryInFrom_WithAlias()
	{
		var result = await S(@"
			SELECT sub.total FROM (
				SELECT SUM(x) AS total FROM UNNEST([10, 20, 30]) AS x
			) AS sub");
		Assert.Equal("60", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Multiple CTEs referencing each other
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Cte_ChainedReferences()
	{
		var result = await S(@"
			WITH 
				base AS (SELECT x FROM UNNEST([1, 2, 3, 4, 5]) AS x),
				doubled AS (SELECT x * 2 AS val FROM base),
				summed AS (SELECT SUM(val) AS total FROM doubled)
			SELECT total FROM summed");
		Assert.Equal("30", result); // (1+2+3+4+5)*2 = 30
	}

	// ───────────────────────────────────────────────────────────────────────────
	// GENERATE_DATE_ARRAY
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_date_array
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task GenerateDateArray_Month()
	{
		var result = await S(@"
			SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-06-01', INTERVAL 1 MONTH))");
		Assert.Equal("6", result); // Jan, Feb, Mar, Apr, May, Jun
	}

	[Fact] public async Task GenerateDateArray_Week()
	{
		var result = await S(@"
			SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-22', INTERVAL 1 WEEK))");
		Assert.Equal("4", result); // Jan 1, 8, 15, 22
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Nested aggregates with GROUP BY
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task GroupBy_WithMultipleAggregates()
	{
		var rows = await Q(@"
			SELECT 
				grp,
				COUNT(*) AS cnt,
				SUM(val) AS total,
				MIN(val) AS min_val,
				MAX(val) AS max_val
			FROM UNNEST([
				STRUCT('A' AS grp, 10 AS val),
				STRUCT('A' AS grp, 20 AS val),
				STRUCT('B' AS grp, 30 AS val),
				STRUCT('B' AS grp, 40 AS val),
				STRUCT('B' AS grp, 50 AS val)
			])
			GROUP BY grp
			ORDER BY grp");
		Assert.Equal("A", rows[0]["grp"]?.ToString());
		Assert.Equal("2", rows[0]["cnt"]?.ToString());
		Assert.Equal("30", rows[0]["total"]?.ToString());
		Assert.Equal("10", rows[0]["min_val"]?.ToString());
		Assert.Equal("20", rows[0]["max_val"]?.ToString());
		Assert.Equal("B", rows[1]["grp"]?.ToString());
		Assert.Equal("3", rows[1]["cnt"]?.ToString());
		Assert.Equal("120", rows[1]["total"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// LEAST / GREATEST with multiple args
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Least_MultipleArgs()
	{
		var result = await S("SELECT LEAST(5, 3, 8, 1, 7)");
		Assert.Equal("1", result);
	}

	[Fact] public async Task Greatest_MultipleArgs()
	{
		var result = await S("SELECT GREATEST(5, 3, 8, 1, 7)");
		Assert.Equal("8", result);
	}

	[Fact] public async Task Least_WithNull()
	{
		var result = await S("SELECT LEAST(5, NULL, 3)");
		Assert.Null(result); // NULL propagates
	}

	// ───────────────────────────────────────────────────────────────────────────
	// TO_JSON_STRING
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#to_json_string
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ToJsonString_Struct()
	{
		var result = await S("SELECT TO_JSON_STRING(STRUCT(1 AS x, 'hello' AS y))");
		Assert.Equal("{\"x\":1,\"y\":\"hello\"}", result);
	}

	[Fact] public async Task ToJsonString_Array()
	{
		var result = await S("SELECT TO_JSON_STRING([1, 2, 3])");
		Assert.Equal("[1,2,3]", result);
	}
}
