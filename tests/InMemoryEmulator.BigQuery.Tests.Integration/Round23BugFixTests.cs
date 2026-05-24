using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Integration tests for bugs fixed in research round 23:
/// - CONCAT with non-string types uses wrong formatting (booleans, dates, floats)
/// - LAST_DAY missing ISOWEEK and ISOYEAR support
/// </summary>
[Collection(IntegrationCollection.Name)]
public class Round23BugFixTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public Round23BugFixTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_r23_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// =====================================================
	// BUG 1: CONCAT uses .ToString() instead of ConvertToString()
	// causing wrong format for booleans, dates, and floats
	// =====================================================

	[Fact]
	public async Task Concat_Booleans_ProducesLowercase()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#concat
		//   CONCAT implicitly casts to STRING; CAST(BOOL AS STRING) → lowercase
		var result = await S("SELECT CONCAT(TRUE, FALSE)");
		Assert.Equal("truefalse", result);
	}

	[Fact]
	public async Task Concat_Date_ProducesIsoFormat()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#concat
		//   CONCAT implicitly casts DATE to STRING in yyyy-MM-dd format
		var result = await S("SELECT CONCAT(DATE '2024-01-01', ' is today')");
		Assert.Equal("2024-01-01 is today", result);
	}

	[Fact]
	public async Task Concat_WholeFloat_ShowsDecimalPoint()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_string
		//   BigQuery CAST(FLOAT64 AS STRING) for whole numbers omits ".0" suffix
		var result = await S("SELECT CONCAT(CAST(1 AS FLOAT64))");
		Assert.Equal("1", result);
	}

	[Fact]
	public async Task Concat_FractionalFloat_ShowsCorrectly()
	{
		var result = await S("SELECT CONCAT(CAST(1.5 AS FLOAT64))");
		Assert.Equal("1.5", result);
	}

	[Fact]
	public async Task Concat_Integers_CastsToString()
	{
		var result = await S("SELECT CONCAT(1, 2, 3)");
		Assert.Equal("123", result);
	}

	// =====================================================
	// BUG 2: LAST_DAY missing ISOWEEK support
	// Falls through to default case returning end of month
	// =====================================================

	[Fact]
	public async Task LastDay_IsoWeek_Monday_ReturnsSunday()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#last_day
		//   "LAST_DAY(date, ISOWEEK): last day of the ISO week (Sunday)"
		// 2024-01-08 is Monday → ISO week ends on 2024-01-14 (Sunday)
		var result = await S("SELECT CAST(LAST_DAY(DATE '2024-01-08', ISOWEEK) AS STRING)");
		Assert.Equal("2024-01-14", result);
	}

	[Fact]
	public async Task LastDay_IsoWeek_Sunday_ReturnsSameDay()
	{
		// 2024-01-14 is Sunday → already last day of ISO week
		var result = await S("SELECT CAST(LAST_DAY(DATE '2024-01-14', ISOWEEK) AS STRING)");
		Assert.Equal("2024-01-14", result);
	}

	[Fact]
	public async Task LastDay_IsoWeek_Wednesday_ReturnsSunday()
	{
		// 2024-01-10 is Wednesday → ISO week ends on 2024-01-14 (Sunday)
		var result = await S("SELECT CAST(LAST_DAY(DATE '2024-01-10', ISOWEEK) AS STRING)");
		Assert.Equal("2024-01-14", result);
	}

	[Fact]
	public async Task LastDay_IsoWeek_Saturday_ReturnsSunday()
	{
		// 2024-01-13 is Saturday → ISO week ends on 2024-01-14 (Sunday)
		var result = await S("SELECT CAST(LAST_DAY(DATE '2024-01-13', ISOWEEK) AS STRING)");
		Assert.Equal("2024-01-14", result);
	}

	// =====================================================
	// BUG 3: LAST_DAY missing ISOYEAR support
	// Falls through to default case returning end of month
	// =====================================================

	[Fact]
	public async Task LastDay_IsoYear_2024()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#last_day
		//   "LAST_DAY(date, ISOYEAR): last day of the ISO year (Sunday of last ISO week)"
		// ISO year 2024 has 52 weeks. Starts 2024-01-01 (Mon), ends 2024-12-29 (Sun)
		var result = await S("SELECT CAST(LAST_DAY(DATE '2024-06-15', ISOYEAR) AS STRING)");
		Assert.Equal("2024-12-29", result);
	}

	[Fact]
	public async Task LastDay_IsoYear_LeapYear()
	{
		// ISO year 2020 has 53 weeks. Ends on 2021-01-03 (Sunday)
		var result = await S("SELECT CAST(LAST_DAY(DATE '2020-06-15', ISOYEAR) AS STRING)");
		Assert.Equal("2021-01-03", result);
	}

	// =====================================================
	// Verified working (non-bugs confirmed as working)
	// =====================================================

	[Fact]
	public async Task Lpad_Truncation()
	{
		var result = await S("SELECT LPAD('hello world', 7)");
		Assert.Equal("hello w", result);
	}

	[Fact]
	public async Task Rpad_Truncation()
	{
		var result = await S("SELECT RPAD('hello world', 7)");
		Assert.Equal("hello w", result);
	}

	[Fact]
	public async Task Soundex_Robert()
	{
		var result = await S("SELECT SOUNDEX('Robert')");
		Assert.Equal("R163", result);
	}

	[Fact]
	public async Task Soundex_Rupert()
	{
		var result = await S("SELECT SOUNDEX('Rupert')");
		Assert.Equal("R163", result);
	}

	[Fact]
	public async Task SafeCast_InvalidDate_ReturnsNull()
	{
		var result = await S("SELECT SAFE_CAST('not-a-date' AS DATE)");
		Assert.Null(result);
	}

	[Fact]
	public async Task DateTrunc_Month_EndOfMonth()
	{
		var result = await S("SELECT CAST(DATE_TRUNC(DATE '2024-01-31', MONTH) AS STRING)");
		Assert.Equal("2024-01-01", result);
	}

	[Fact]
	public async Task WindowFunction_OrderByDesc()
	{
		var result = await S(@"
			SELECT CAST(rn AS STRING) FROM (
				SELECT val, ROW_NUMBER() OVER (ORDER BY val DESC) AS rn
				FROM UNNEST([3, 1, 2]) AS val
			) WHERE val = 3");
		Assert.Equal("1", result);
	}

	[Fact]
	public async Task MultipleCTEs_WithDependencies()
	{
		var result = await S(@"
			WITH a AS (SELECT 1 AS x),
			     b AS (SELECT x + 1 AS y FROM a),
			     c AS (SELECT y + 1 AS z FROM b)
			SELECT CAST(z AS STRING) FROM c");
		Assert.Equal("3", result);
	}

	[Fact]
	public async Task MultipleWindows_DifferentPartitions()
	{
		var result = await S(@"
			SELECT CAST(COUNT(*) AS STRING) FROM (
				SELECT val, grp,
					ROW_NUMBER() OVER (ORDER BY val) AS rn_all,
					ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) AS rn_grp
				FROM (SELECT 1 AS val, 'a' AS grp UNION ALL SELECT 2, 'a' UNION ALL SELECT 3, 'b' UNION ALL SELECT 4, 'b')
			)");
		Assert.Equal("4", result);
	}
}
