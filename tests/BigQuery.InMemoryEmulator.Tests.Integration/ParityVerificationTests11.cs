using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Parity verification tests batch 11: ARRAY_AGG LIMIT, TIMESTAMP_TRUNC timezone preservation,
/// DATE_TRUNC/TIMESTAMP_TRUNC ISOYEAR, FIRST_VALUE/LAST_VALUE frame semantics,
/// REGEXP_EXTRACT with capture groups, FORMAT() specifiers, edge case aggregates.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests11 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests11(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv11_{Guid.NewGuid():N}"[..28];
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
	// ARRAY_AGG LIMIT
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#array_agg
	//   "LIMIT n: Limits the number of elements in the resulting array to n."
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ArrayAgg_Limit()
	{
		var result = await S(@"
			SELECT ARRAY_LENGTH(ARRAY_AGG(x LIMIT 3))
			FROM UNNEST([1, 2, 3, 4, 5]) AS x");
		Assert.Equal("3", result);
	}

	[Fact] public async Task ArrayAgg_OrderByLimit()
	{
		var result = await S(@"
			SELECT ARRAY_TO_STRING(ARRAY_AGG(x ORDER BY x LIMIT 3), ',')
			FROM UNNEST([5, 3, 1, 4, 2]) AS x");
		Assert.Equal("1,2,3", result);
	}

	[Fact] public async Task ArrayAgg_DistinctLimit()
	{
		var result = await S(@"
			SELECT ARRAY_LENGTH(ARRAY_AGG(DISTINCT x LIMIT 2))
			FROM UNNEST([1, 1, 2, 2, 3, 3]) AS x");
		Assert.Equal("2", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// TIMESTAMP_TRUNC timezone preservation
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_trunc
	//   "The result is always expressed in UTC, regardless of input timezone."
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task TimestampTrunc_PreservesUtc()
	{
		// Timestamps internally are always UTC in BigQuery
		var result = await S("SELECT CAST(TIMESTAMP_TRUNC(TIMESTAMP '2024-03-15 14:30:00 UTC', HOUR) AS STRING)");
		Assert.Equal("2024-03-15 14:00:00+00", result);
	}

	[Fact] public async Task TimestampTrunc_YearUtc()
	{
		var result = await S("SELECT CAST(TIMESTAMP_TRUNC(TIMESTAMP '2024-06-15 14:30:00 UTC', YEAR) AS STRING)");
		Assert.Equal("2024-01-01 00:00:00+00", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// DATE_TRUNC ISOYEAR
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_trunc
	//   "ISOYEAR: Returns the first Monday of the ISO year."
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task DateTrunc_Isoyear()
	{
		// ISO year 2024 starts on Monday 2024-01-01 (Jan 1 2024 is a Monday)
		var result = await S("SELECT CAST(DATE_TRUNC(DATE '2024-06-15', ISOYEAR) AS STRING)");
		Assert.Equal("2024-01-01", result);
	}

	[Fact] public async Task DateTrunc_Isoyear_2023()
	{
		// ISO year 2023 starts on Monday 2023-01-02 (Jan 1 2023 is Sunday, so Iso year starts Jan 2)
		var result = await S("SELECT CAST(DATE_TRUNC(DATE '2023-06-15', ISOYEAR) AS STRING)");
		Assert.Equal("2023-01-02", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// REGEXP_EXTRACT with capture groups
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_extract
	//   "If the pattern includes a capture group, REGEXP_EXTRACT returns the substring matched by that group."
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task RegexpExtract_CaptureGroup()
	{
		var result = await S(@"SELECT REGEXP_EXTRACT('foo@bar.com', r'@(.+)')");
		Assert.Equal("bar.com", result);
	}

	[Fact] public async Task RegexpExtract_NoCaptureGroup()
	{
		var result = await S(@"SELECT REGEXP_EXTRACT('hello123world', r'\d+')");
		Assert.Equal("123", result);
	}

	[Fact] public async Task RegexpExtract_NoMatch()
	{
		var result = await S(@"SELECT REGEXP_EXTRACT('hello', r'\d+')");
		Assert.Null(result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// FIRST_VALUE / LAST_VALUE window frame semantics
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#first_value
	//   Default frame for FIRST_VALUE/LAST_VALUE is ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task FirstValue_DefaultFrame()
	{
		var rows = await Q(@"
			SELECT val, FIRST_VALUE(val) OVER (ORDER BY val) AS fv
			FROM UNNEST([30, 10, 20]) AS val
			ORDER BY val");
		// Default frame is UNBOUNDED PRECEDING to CURRENT ROW, first value is always 10
		Assert.Equal("10", rows[0]["fv"]?.ToString());
		Assert.Equal("10", rows[2]["fv"]?.ToString());
	}

	[Fact] public async Task LastValue_DefaultFrame()
	{
		var rows = await Q(@"
			SELECT val, LAST_VALUE(val) OVER (ORDER BY val) AS lv
			FROM UNNEST([30, 10, 20]) AS val
			ORDER BY val");
		// Default frame: UNBOUNDED PRECEDING to CURRENT ROW → last value = current row
		Assert.Equal("10", rows[0]["lv"]?.ToString());
		Assert.Equal("20", rows[1]["lv"]?.ToString());
		Assert.Equal("30", rows[2]["lv"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// FORMAT() specifiers
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#format_string
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Format_Float()
	{
		var result = await S("SELECT FORMAT('%.2f', 3.14159)");
		Assert.Equal("3.14", result);
	}

	[Fact] public async Task Format_Integer()
	{
		var result = await S("SELECT FORMAT('%05d', 42)");
		Assert.Equal("00042", result);
	}

	[Fact] public async Task Format_Hex()
	{
		var result = await S("SELECT FORMAT('%x', 255)");
		Assert.Equal("ff", result);
	}

	[Fact] public async Task Format_UpperHex()
	{
		var result = await S("SELECT FORMAT('%X', 255)");
		Assert.Equal("FF", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Edge case aggregates
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Count_AllNull()
	{
		var result = await S("SELECT COUNT(x) FROM UNNEST([CAST(NULL AS INT64), NULL, NULL]) AS x");
		Assert.Equal("0", result);
	}

	[Fact] public async Task Sum_AllNull()
	{
		var result = await S("SELECT SUM(x) FROM UNNEST([CAST(NULL AS INT64), NULL, NULL]) AS x");
		Assert.Null(result);
	}

	[Fact] public async Task Min_AllNull()
	{
		var result = await S("SELECT MIN(x) FROM UNNEST([CAST(NULL AS INT64), NULL, NULL]) AS x");
		Assert.Null(result);
	}

	[Fact] public async Task ArrayAgg_IgnoreNulls()
	{
		var result = await S(@"
			SELECT ARRAY_TO_STRING(ARRAY_AGG(x IGNORE NULLS ORDER BY x), ',')
			FROM UNNEST([CAST(3 AS INT64), NULL, 1, NULL, 2]) AS x");
		Assert.Equal("1,2,3", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Mathematical edge cases
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Div_NegativeNumbers()
	{
		// BigQuery DIV truncates toward zero
		var result = await S("SELECT DIV(-7, 2)");
		Assert.Equal("-3", result);
	}

	[Fact] public async Task Mod_NegativeNumbers()
	{
		// BigQuery MOD: result has same sign as dividend
		var result = await S("SELECT MOD(-7, 3)");
		Assert.Equal("-1", result);
	}

	[Fact] public async Task Ieee_Divide_Infinity()
	{
		var result = await S("SELECT CAST(IEEE_DIVIDE(1, 0) AS STRING)");
		Assert.Equal("inf", result);
	}

	[Fact] public async Task Ieee_Divide_NaN()
	{
		var result = await S("SELECT CAST(IEEE_DIVIDE(0, 0) AS STRING)");
		Assert.Equal("nan", result);
	}
}
