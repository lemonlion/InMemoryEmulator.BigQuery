using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Parity verification tests batch 10: TIMESTAMP_TRUNC/DATE_TRUNC missing parts,
/// advanced window functions (NTILE, PERCENT_RANK, CUME_DIST, LAST_VALUE),
/// REGEXP_REPLACE, JSON functions, SAFE functions, HAVING with expressions.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests10 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests10(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv10_{Guid.NewGuid():N}"[..28];
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
	// TIMESTAMP_TRUNC missing parts
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_trunc
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task TimestampTrunc_Quarter()
	{
		// 2024-05-15 is in Q2 (April 1)
		var result = await S("SELECT CAST(TIMESTAMP_TRUNC(TIMESTAMP '2024-05-15 10:30:00 UTC', QUARTER) AS STRING)");
		Assert.Equal("2024-04-01 00:00:00+00", result);
	}

	[Fact] public async Task TimestampTrunc_Week()
	{
		// 2024-01-17 is Wednesday. WEEK truncates to preceding Sunday = Jan 14
		var result = await S("SELECT CAST(TIMESTAMP_TRUNC(TIMESTAMP '2024-01-17 10:30:00 UTC', WEEK) AS STRING)");
		Assert.Equal("2024-01-14 00:00:00+00", result);
	}

	[Fact] public async Task TimestampTrunc_Millisecond()
	{
		var result = await S("SELECT CAST(TIMESTAMP_TRUNC(TIMESTAMP '2024-01-15 10:30:45.123456 UTC', MILLISECOND) AS STRING)");
		Assert.Equal("2024-01-15 10:30:45.123+00", result);
	}

	[Fact] public async Task TimestampTrunc_Microsecond()
	{
		var result = await S("SELECT CAST(TIMESTAMP_TRUNC(TIMESTAMP '2024-01-15 10:30:45.123456 UTC', MICROSECOND) AS STRING)");
		Assert.Equal("2024-01-15 10:30:45.123456+00", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// DATE_TRUNC missing ISOWEEK
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_trunc
	//   "ISOWEEK: Truncates date_expression to the preceding Monday."
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task DateTrunc_Isoweek()
	{
		// 2024-01-17 is Wednesday. ISOWEEK truncates to Monday = Jan 15
		var result = await S("SELECT CAST(DATE_TRUNC(DATE '2024-01-17', ISOWEEK) AS STRING)");
		Assert.Equal("2024-01-15", result);
	}

	[Fact] public async Task DateTrunc_Isoweek_Sunday()
	{
		// 2024-01-14 is Sunday. ISOWEEK truncates to Monday of PREVIOUS week = Jan 8
		var result = await S("SELECT CAST(DATE_TRUNC(DATE '2024-01-14', ISOWEEK) AS STRING)");
		Assert.Equal("2024-01-08", result);
	}

	[Fact] public async Task DateTrunc_Isoweek_Monday()
	{
		// Monday stays on Monday
		var result = await S("SELECT CAST(DATE_TRUNC(DATE '2024-01-15', ISOWEEK) AS STRING)");
		Assert.Equal("2024-01-15", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// TIMESTAMP_TRUNC ISOWEEK
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task TimestampTrunc_Isoweek()
	{
		// 2024-01-17 Wed → preceding Monday = Jan 15
		var result = await S("SELECT CAST(TIMESTAMP_TRUNC(TIMESTAMP '2024-01-17 10:30:00 UTC', ISOWEEK) AS STRING)");
		Assert.Equal("2024-01-15 00:00:00+00", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Advanced window functions
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Ntile_Basic()
	{
		var rows = await Q(@"
			SELECT val, NTILE(3) OVER (ORDER BY val) AS bucket
			FROM UNNEST([10, 20, 30, 40, 50, 60, 70, 80, 90]) AS val
			ORDER BY val");
		// 9 values into 3 buckets: [10,20,30]=1, [40,50,60]=2, [70,80,90]=3
		Assert.Equal("1", rows[0]["bucket"]?.ToString());
		Assert.Equal("1", rows[2]["bucket"]?.ToString());
		Assert.Equal("2", rows[3]["bucket"]?.ToString());
		Assert.Equal("3", rows[8]["bucket"]?.ToString());
	}

	[Fact] public async Task PercentRank_Basic()
	{
		var rows = await Q(@"
			SELECT val, CAST(PERCENT_RANK() OVER (ORDER BY val) AS STRING) AS pct
			FROM UNNEST([10, 20, 30, 40, 50]) AS val
			ORDER BY val");
		// PERCENT_RANK = (rank - 1) / (total_rows - 1)
		// val=10: (1-1)/(5-1) = 0.0
		// val=50: (5-1)/(5-1) = 1.0
		Assert.Equal("0.0", rows[0]["pct"]?.ToString());
		Assert.Equal("1.0", rows[4]["pct"]?.ToString());
	}

	[Fact] public async Task CumeDist_Basic()
	{
		var rows = await Q(@"
			SELECT val, CAST(CUME_DIST() OVER (ORDER BY val) AS STRING) AS cd
			FROM UNNEST([10, 20, 30, 40, 50]) AS val
			ORDER BY val");
		// CUME_DIST = number_of_rows_with_value_lte / total_rows
		// val=10: 1/5 = 0.2
		// val=50: 5/5 = 1.0
		Assert.Equal("0.2", rows[0]["cd"]?.ToString());
		Assert.Equal("1.0", rows[4]["cd"]?.ToString());
	}

	[Fact] public async Task LastValue_WithFrame()
	{
		var rows = await Q(@"
			SELECT val, LAST_VALUE(val) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv
			FROM UNNEST([10, 20, 30]) AS val
			ORDER BY val");
		// LAST_VALUE with full frame should always be 30
		Assert.Equal("30", rows[0]["lv"]?.ToString());
		Assert.Equal("30", rows[2]["lv"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// REGEXP_REPLACE
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_replace
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task RegexpReplace_Basic()
	{
		var result = await S(@"SELECT REGEXP_REPLACE('hello world', r'(\w+)', 'X')");
		Assert.Equal("X X", result);
	}

	[Fact] public async Task RegexpReplace_Backreference()
	{
		var result = await S(@"SELECT REGEXP_REPLACE('abc123def', r'([a-z]+)(\d+)', r'\1-\2')");
		Assert.Equal("abc-123def", result);
	}

	[Fact] public async Task RegexpReplace_GlobalReplace()
	{
		var result = await S(@"SELECT REGEXP_REPLACE('aaa', r'a', 'b')");
		Assert.Equal("bbb", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// SAFE functions
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-reference#safe_prefix
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task SafeCast_InvalidString()
	{
		var result = await S("SELECT SAFE_CAST('not_a_number' AS INT64)");
		Assert.Null(result);
	}

	[Fact] public async Task SafeDivide_ZeroByZero()
	{
		var result = await S("SELECT SAFE_DIVIDE(0, 0)");
		Assert.Null(result);
	}

	[Fact] public async Task SafeDivide_NonZeroByZero()
	{
		var result = await S("SELECT SAFE_DIVIDE(10, 0)");
		Assert.Null(result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// JSON functions
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task JsonExtractScalar_String()
	{
		var result = await S(@"SELECT JSON_EXTRACT_SCALAR('{""name"": ""Alice"", ""age"": 30}', '$.name')");
		Assert.Equal("Alice", result);
	}

	[Fact] public async Task JsonExtractScalar_Number()
	{
		var result = await S(@"SELECT JSON_EXTRACT_SCALAR('{""name"": ""Alice"", ""age"": 30}', '$.age')");
		Assert.Equal("30", result);
	}

	[Fact] public async Task JsonExtractScalar_Nested()
	{
		var result = await S(@"SELECT JSON_EXTRACT_SCALAR('{""a"": {""b"": ""deep""}}', '$.a.b')");
		Assert.Equal("deep", result);
	}

	[Fact] public async Task JsonExtractScalar_Null()
	{
		var result = await S(@"SELECT JSON_EXTRACT_SCALAR('{""name"": ""Alice""}', '$.missing')");
		Assert.Null(result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// GENERATE_TIMESTAMP_ARRAY
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_timestamp_array
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task GenerateTimestampArray_Hour()
	{
		var result = await S(@"
			SELECT ARRAY_LENGTH(GENERATE_TIMESTAMP_ARRAY(
				TIMESTAMP '2024-01-01 00:00:00 UTC', 
				TIMESTAMP '2024-01-01 05:00:00 UTC', 
				INTERVAL 1 HOUR))");
		Assert.Equal("6", result); // 00, 01, 02, 03, 04, 05
	}

	[Fact] public async Task GenerateTimestampArray_Day()
	{
		var result = await S(@"
			SELECT ARRAY_LENGTH(GENERATE_TIMESTAMP_ARRAY(
				TIMESTAMP '2024-01-01 00:00:00 UTC', 
				TIMESTAMP '2024-01-03 00:00:00 UTC', 
				INTERVAL 1 DAY))");
		Assert.Equal("3", result); // Jan 1, 2, 3
	}

	// ───────────────────────────────────────────────────────────────────────────
	// NTH_VALUE window function
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#nth_value
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task NthValue_Basic()
	{
		var rows = await Q(@"
			SELECT val, NTH_VALUE(val, 2) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_val
			FROM UNNEST([10, 20, 30, 40]) AS val
			ORDER BY val");
		Assert.Equal("20", rows[0]["second_val"]?.ToString());
		Assert.Equal("20", rows[3]["second_val"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// HAVING with aggregate condition
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Having_WithAggregate()
	{
		var rows = await Q(@"
			SELECT dept, COUNT(*) AS cnt
			FROM UNNEST([
				STRUCT('Eng' AS dept), STRUCT('Eng' AS dept), STRUCT('Eng' AS dept),
				STRUCT('Sales' AS dept), STRUCT('HR' AS dept)
			])
			GROUP BY dept
			HAVING COUNT(*) >= 2
			ORDER BY dept");
		Assert.Single(rows);
		Assert.Equal("Eng", rows[0]["dept"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// CAST edge cases
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Cast_BoolToString()
	{
		var result = await S("SELECT CAST(TRUE AS STRING)");
		Assert.Equal("true", result);
	}

	[Fact] public async Task Cast_StringToBool_True()
	{
		var result = await S("SELECT CAST('true' AS BOOL)");
		Assert.Equal("True", result);
	}

	[Fact] public async Task Cast_IntToFloat()
	{
		var result = await S("SELECT CAST(CAST(42 AS FLOAT64) AS STRING)");
		Assert.Equal("42.0", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Edge cases in expressions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Division_IntegerDivision()
	{
		// BigQuery: integer / integer = FLOAT64
		var result = await S("SELECT CAST(7 / 2 AS STRING)");
		Assert.Equal("3.5", result);
	}

	[Fact] public async Task Modulo()
	{
		var result = await S("SELECT MOD(17, 5)");
		Assert.Equal("2", result);
	}

	[Fact] public async Task Power()
	{
		var result = await S("SELECT CAST(POWER(2, 10) AS INT64)");
		Assert.Equal("1024", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// String edge cases
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Repeat_String()
	{
		var result = await S("SELECT REPEAT('ab', 3)");
		Assert.Equal("ababab", result);
	}

	[Fact] public async Task Lpad_Rpad()
	{
		var r1 = await S("SELECT LPAD('hi', 5, '0')");
		var r2 = await S("SELECT RPAD('hi', 5, '0')");
		Assert.Equal("000hi", r1);
		Assert.Equal("hi000", r2);
	}

	[Fact] public async Task Starts_Ends_With()
	{
		var r1 = await S("SELECT STARTS_WITH('hello world', 'hello')");
		var r2 = await S("SELECT ENDS_WITH('hello world', 'world')");
		Assert.Equal("True", r1);
		Assert.Equal("True", r2);
	}
}
