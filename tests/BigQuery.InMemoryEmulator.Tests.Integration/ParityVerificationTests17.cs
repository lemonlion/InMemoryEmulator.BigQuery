using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Parity verification tests batch 17: LIKE patterns, complex date math,
/// TIMESTAMP casting/formatting, PARSE_DATE/PARSE_TIMESTAMP, multi-byte strings,
/// FORMAT_DATE/FORMAT_TIMESTAMP, DATE/TIME literal parsing, CURRENT_TIMESTAMP.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests17 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests17(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv17_{Guid.NewGuid():N}"[..28];
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
	// LIKE patterns
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#like_operator
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Like_StartsWith()
	{
		var result = await S("SELECT 'hello world' LIKE 'hello%'");
		Assert.Equal("True", result);
	}

	[Fact] public async Task Like_EndsWith()
	{
		var result = await S("SELECT 'hello world' LIKE '%world'");
		Assert.Equal("True", result);
	}

	[Fact] public async Task Like_Contains()
	{
		var result = await S("SELECT 'hello world' LIKE '%lo wo%'");
		Assert.Equal("True", result);
	}

	[Fact] public async Task Like_SingleChar()
	{
		var result = await S("SELECT 'cat' LIKE 'c_t'");
		Assert.Equal("True", result);
	}

	[Fact] public async Task Like_NoMatch()
	{
		var result = await S("SELECT 'hello' LIKE 'world%'");
		Assert.Equal("False", result);
	}

	[Fact] public async Task Not_Like()
	{
		var result = await S("SELECT 'hello' NOT LIKE 'world%'");
		Assert.Equal("True", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// FORMAT_DATE
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#format_date
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task FormatDate_YearMonth()
	{
		var result = await S("SELECT FORMAT_DATE('%Y-%m', DATE '2024-06-15')");
		Assert.Equal("2024-06", result);
	}

	[Fact] public async Task FormatDate_DayName()
	{
		// 2024-01-15 is Monday
		var result = await S("SELECT FORMAT_DATE('%A', DATE '2024-01-15')");
		Assert.Equal("Monday", result);
	}

	[Fact] public async Task FormatDate_ShortDayName()
	{
		var result = await S("SELECT FORMAT_DATE('%a', DATE '2024-01-15')");
		Assert.Equal("Mon", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// FORMAT_TIMESTAMP
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#format_timestamp
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task FormatTimestamp_Full()
	{
		var result = await S("SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP '2024-06-15 14:30:45 UTC')");
		Assert.Equal("2024-06-15 14:30:45", result);
	}

	[Fact] public async Task FormatTimestamp_DateOnly()
	{
		var result = await S("SELECT FORMAT_TIMESTAMP('%F', TIMESTAMP '2024-06-15 14:30:45 UTC')");
		Assert.Equal("2024-06-15", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// PARSE_DATE
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#parse_date
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ParseDate_Standard()
	{
		var result = await S("SELECT CAST(PARSE_DATE('%Y-%m-%d', '2024-06-15') AS STRING)");
		Assert.Equal("2024-06-15", result);
	}

	[Fact] public async Task ParseDate_AmericanFormat()
	{
		var result = await S("SELECT CAST(PARSE_DATE('%m/%d/%Y', '06/15/2024') AS STRING)");
		Assert.Equal("2024-06-15", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// PARSE_TIMESTAMP
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#parse_timestamp
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ParseTimestamp_Standard()
	{
		var result = await S("SELECT CAST(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', '2024-06-15 14:30:45') AS STRING)");
		Assert.Equal("2024-06-15 14:30:45+00", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Date arithmetic chain
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task DateArithmetic_Chain()
	{
		var result = await S(@"
			SELECT CAST(
				DATE_ADD(DATE_ADD(DATE '2024-01-15', INTERVAL 1 MONTH), INTERVAL 10 DAY) 
			AS STRING)");
		Assert.Equal("2024-02-25", result); // Jan 15 + 1 month = Feb 15 + 10 days = Feb 25
	}

	[Fact] public async Task DateDiff_SameDate()
	{
		var result = await S("SELECT DATE_DIFF(DATE '2024-01-15', DATE '2024-01-15', DAY)");
		Assert.Equal("0", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Multi-byte / Unicode strings
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Length_Unicode()
	{
		// LENGTH returns number of characters, not bytes
		var result = await S("SELECT LENGTH('héllo')");
		Assert.Equal("5", result);
	}

	[Fact] public async Task Upper_Unicode()
	{
		var result = await S("SELECT UPPER('café')");
		Assert.Equal("CAFÉ", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// TIMESTAMP from DATE
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Timestamp_FromDate()
	{
		var result = await S("SELECT CAST(TIMESTAMP(DATE '2024-06-15') AS STRING)");
		Assert.Equal("2024-06-15 00:00:00+00", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// CAST between date types
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Cast_TimestampToDate()
	{
		var result = await S("SELECT CAST(CAST(TIMESTAMP '2024-06-15 14:30:00 UTC' AS DATE) AS STRING)");
		Assert.Equal("2024-06-15", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Aggregate with empty input
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Sum_EmptyInput()
	{
		var result = await S("SELECT SUM(x) FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x");
		Assert.Null(result);
	}

	[Fact] public async Task Count_EmptyInput()
	{
		var result = await S("SELECT COUNT(x) FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x");
		Assert.Equal("0", result);
	}

	[Fact] public async Task CountStar_EmptyInput()
	{
		var result = await S("SELECT COUNT(*) FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x");
		Assert.Equal("0", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// IS DISTINCT FROM
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#is_distinct_from
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task IsDistinctFrom_NullNull()
	{
		// NULL IS NOT DISTINCT FROM NULL → TRUE (same as IS NOT DISTINCT FROM)
		var result = await S("SELECT NULL IS NOT DISTINCT FROM NULL");
		Assert.Equal("True", result);
	}

	[Fact] public async Task IsDistinctFrom_NullValue()
	{
		var result = await S("SELECT NULL IS DISTINCT FROM 1");
		Assert.Equal("True", result);
	}

	[Fact] public async Task IsDistinctFrom_SameValue()
	{
		var result = await S("SELECT 1 IS DISTINCT FROM 1");
		Assert.Equal("False", result);
	}
}
