using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 21: JSON functions, REGEXP complex patterns,
/// DATE/TIMESTAMP arithmetic with intervals, complex UNNEST patterns,
/// GROUP BY ROLLUP expressions, complex window partitioning,
/// array subqueries, SAFE functions, FORMAT specifiers.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests21 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests21(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv21_{Guid.NewGuid():N}"[..28];
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
	// JSON functions
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task JsonExtractScalar_SimpleKey()
	{
		var result = await S(@"SELECT JSON_EXTRACT_SCALAR('{""name"":""Alice"",""age"":30}', '$.name')");
		Assert.Equal("Alice", result);
	}

	[Fact] public async Task JsonExtractScalar_NumericValue()
	{
		var result = await S(@"SELECT JSON_EXTRACT_SCALAR('{""name"":""Alice"",""age"":30}', '$.age')");
		Assert.Equal("30", result);
	}

	[Fact] public async Task JsonExtractScalar_NestedKey()
	{
		var result = await S(@"SELECT JSON_EXTRACT_SCALAR('{""a"":{""b"":""deep""}}', '$.a.b')");
		Assert.Equal("deep", result);
	}

	[Fact] public async Task JsonExtractScalar_MissingKey()
	{
		var result = await S(@"SELECT JSON_EXTRACT_SCALAR('{""name"":""Alice""}', '$.missing')");
		Assert.Null(result);
	}

	[Fact] public async Task JsonExtractScalar_ArrayElement()
	{
		var result = await S(@"SELECT JSON_EXTRACT_SCALAR('{""items"":[""a"",""b"",""c""]}', '$.items[1]')");
		Assert.Equal("b", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// REGEXP functions
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_extract
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task RegexpExtract_NoGroup()
	{
		var result = await S("SELECT REGEXP_EXTRACT('hello123world', r'[0-9]+')");
		Assert.Equal("123", result);
	}

	[Fact] public async Task RegexpExtract_WithGroup()
	{
		var result = await S("SELECT REGEXP_EXTRACT('email@example.com', r'@(.+)')");
		Assert.Equal("example.com", result);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact] public async Task RegexpExtract_NoMatch()
	{
		var result = await S("SELECT REGEXP_EXTRACT('hello', r'[0-9]+')");
		Assert.Null(result);
	}

	[Fact] public async Task RegexpReplace_Backreference()
	{
		var result = await S(@"SELECT REGEXP_REPLACE('abc-def', r'(\w+)-(\w+)', r'\2-\1')");
		Assert.Equal("def-abc", result);
	}

	[Fact] public async Task RegexpContains_True()
	{
		var result = await S("SELECT REGEXP_CONTAINS('hello world', r'\\bworld\\b')");
		Assert.Equal("True", result);
	}

	[Fact] public async Task RegexpContains_False()
	{
		var result = await S("SELECT REGEXP_CONTAINS('hello', r'\\d+')");
		Assert.Equal("False", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// DATE arithmetic
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task DateAdd_Year()
	{
		var result = await S("SELECT CAST(DATE_ADD(DATE '2024-02-29', INTERVAL 1 YEAR) AS STRING)");
		// 2024-02-29 + 1 year → 2025-02-28 (leap year adjustment)
		Assert.Equal("2025-02-28", result);
	}

	[Fact] public async Task DateSub_Month()
	{
		var result = await S("SELECT CAST(DATE_SUB(DATE '2024-03-31', INTERVAL 1 MONTH) AS STRING)");
		// 2024-03-31 - 1 month → 2024-02-29 (end of Feb in leap year)
		Assert.Equal("2024-02-29", result);
	}

	[Fact] public async Task DateDiff_Months()
	{
		var result = await S("SELECT DATE_DIFF(DATE '2024-06-15', DATE '2024-01-15', MONTH)");
		Assert.Equal("5", result);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact] public async Task DateDiff_Years()
	{
		var result = await S("SELECT DATE_DIFF(DATE '2024-06-15', DATE '2020-06-15', YEAR)");
		Assert.Equal("4", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// TIMESTAMP arithmetic
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task TimestampAdd_Hour()
	{
		var result = await S("SELECT CAST(TIMESTAMP_ADD(TIMESTAMP '2024-01-01 23:00:00 UTC', INTERVAL 2 HOUR) AS STRING)");
		Assert.Equal("2024-01-02 01:00:00+00", result);
	}

	[Fact] public async Task TimestampDiff_Minutes()
	{
		var result = await S("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-01 01:30:00 UTC', TIMESTAMP '2024-01-01 00:00:00 UTC', MINUTE)");
		Assert.Equal("90", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Complex UNNEST with offset
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unnest_operator
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Unnest_WithOffset()
	{
		var rows = await Q(@"
			SELECT element, off FROM UNNEST(['a', 'b', 'c']) AS element WITH OFFSET AS off
			ORDER BY off");
		Assert.Equal(3, rows.Count);
		Assert.Equal("a", rows[0][0]?.ToString());
		Assert.Equal("0", rows[0][1]?.ToString());
		Assert.Equal("c", rows[2][0]?.ToString());
		Assert.Equal("2", rows[2][1]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Window: FIRST_VALUE / LAST_VALUE with frame
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task FirstValue_Default()
	{
		var rows = await Q(@"
			SELECT x, FIRST_VALUE(x) OVER (ORDER BY x) AS first_v
			FROM UNNEST([3, 1, 4, 1, 5]) AS x
			ORDER BY x");
		// FIRST_VALUE over ordered window → always 1
		Assert.Equal("1", rows[0][1]?.ToString());
		Assert.Equal("1", rows[4][1]?.ToString());
	}

	[Fact] public async Task LastValue_FullFrame()
	{
		var rows = await Q(@"
			SELECT x, LAST_VALUE(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_v
			FROM UNNEST([3, 1, 4, 1, 5]) AS x
			ORDER BY x");
		Assert.Equal("5", rows[0][1]?.ToString());
		Assert.Equal("5", rows[4][1]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// SAFE functions
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-reference#safe_prefix
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task SafeDivide_ByZero()
	{
		var result = await S("SELECT SAFE_DIVIDE(10, 0)");
		Assert.Null(result);
	}

	[Fact] public async Task SafeDivide_Normal()
	{
		var result = await S("SELECT SAFE_DIVIDE(10, 2)");
		Assert.Equal("5", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// IFNULL / COALESCE
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task IfNull_NullValue()
	{
		var result = await S("SELECT IFNULL(NULL, 'default')");
		Assert.Equal("default", result);
	}

	[Fact] public async Task IfNull_NonNull()
	{
		var result = await S("SELECT IFNULL('value', 'default')");
		Assert.Equal("value", result);
	}

	[Fact] public async Task Coalesce_SkipsNulls()
	{
		var result = await S("SELECT COALESCE(NULL, NULL, 'third', 'fourth')");
		Assert.Equal("third", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// APPROX_COUNT_DISTINCT
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/approximate_aggregate_functions#approx_count_distinct
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ApproxCountDistinct()
	{
		var result = await S("SELECT APPROX_COUNT_DISTINCT(x) FROM UNNEST([1, 2, 2, 3, 3, 3]) AS x");
		Assert.Equal("3", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// FORMAT function
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#format_string
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Format_Integer()
	{
		var result = await S("SELECT FORMAT('%d', 42)");
		Assert.Equal("42", result);
	}

	[Fact] public async Task Format_Float()
	{
		var result = await S("SELECT FORMAT('%.2f', 3.14159)");
		Assert.Equal("3.14", result);
	}

	[Fact] public async Task Format_String()
	{
		var result = await S("SELECT FORMAT('Hello %s!', 'World')");
		Assert.Equal("Hello World!", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// EXTRACT from TIMESTAMP
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#extract
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Extract_HourFromTimestamp()
	{
		var result = await S("SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-06-15 14:30:00 UTC')");
		Assert.Equal("14", result);
	}

	[Fact] public async Task Extract_DayOfWeekFromDate()
	{
		// BigQuery: 1=Sunday, 7=Saturday. 2024-06-15 is a Saturday
		var result = await S("SELECT EXTRACT(DAYOFWEEK FROM DATE '2024-06-15')");
		Assert.Equal("7", result);
	}

	[Fact] public async Task Extract_IsoYear()
	{
		var result = await S("SELECT EXTRACT(ISOYEAR FROM DATE '2024-12-30')");
		Assert.Equal("2025", result); // Last days of 2024 belong to ISO year 2025
	}

	// ───────────────────────────────────────────────────────────────────────────
	// BIT operations
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#bitwise_operators
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task BitAnd()
	{
		var result = await S("SELECT 12 & 10");
		Assert.Equal("8", result); // 1100 & 1010 = 1000
	}

	[Fact] public async Task BitOr()
	{
		var result = await S("SELECT 12 | 10");
		Assert.Equal("14", result); // 1100 | 1010 = 1110
	}

	[Fact] public async Task BitXor()
	{
		var result = await S("SELECT 12 ^ 10");
		Assert.Equal("6", result); // 1100 ^ 1010 = 0110
	}

	[Fact] public async Task BitNot()
	{
		var result = await S("SELECT ~0");
		Assert.Equal("-1", result); // All bits flipped
	}

	[Fact] public async Task BitShiftLeft()
	{
		var result = await S("SELECT 1 << 3");
		Assert.Equal("8", result);
	}

	[Fact] public async Task BitShiftRight()
	{
		var result = await S("SELECT 8 >> 2");
		Assert.Equal("2", result);
	}
}
