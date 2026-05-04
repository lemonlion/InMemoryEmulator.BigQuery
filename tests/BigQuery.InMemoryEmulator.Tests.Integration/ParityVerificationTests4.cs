using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Fourth round of parity verification tests — JSON functions, timestamp conversion,
/// math functions, aggregate functions (VARIANCE, STDDEV, CORR, BIT_AND/OR/XOR),
/// PIVOT/UNPIVOT, IN subquery, MERGE, and string functions.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ParityVerificationTests4 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests4(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_pv4_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }
	private async Task Exec(string sql) { var c = await _fixture.GetClientAsync(); await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); }

	// ========================================================================
	// JSON FUNCTIONS
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions
	// ========================================================================

	[Fact]
	public async Task JsonExtractScalar_String()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_extract_scalar
		var r = await S("SELECT JSON_EXTRACT_SCALAR('{\"name\": \"Alice\", \"age\": 30}', '$.name')");
		Assert.Equal("Alice", r);
	}

	[Fact]
	public async Task JsonExtractScalar_Number()
	{
		var r = await S("SELECT JSON_EXTRACT_SCALAR('{\"x\": 42}', '$.x')");
		Assert.Equal("42", r);
	}

	[Fact]
	public async Task JsonExtractScalar_Nested()
	{
		var r = await S("SELECT JSON_EXTRACT_SCALAR('{\"a\": {\"b\": \"deep\"}}', '$.a.b')");
		Assert.Equal("deep", r);
	}

	[Fact]
	public async Task JsonExtractScalar_MissingKey_ReturnsNull()
	{
		var r = await S("SELECT JSON_EXTRACT_SCALAR('{\"x\": 1}', '$.y')");
		Assert.Null(r);
	}

	[Fact]
	public async Task JsonValue_Basic()
	{
		// JSON_VALUE is synonym for JSON_EXTRACT_SCALAR
		var r = await S("SELECT JSON_VALUE('{\"key\": \"val\"}', '$.key')");
		Assert.Equal("val", r);
	}

	[Fact]
	public async Task JsonExtract_ReturnsJson()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_extract
		var r = await S("SELECT JSON_EXTRACT('{\"a\": [1,2,3]}', '$.a')");
		Assert.Equal("[1,2,3]", r);
	}

	[Fact]
	public async Task JsonExtract_ArrayElement()
	{
		var r = await S("SELECT JSON_EXTRACT_SCALAR('{\"items\": [10, 20, 30]}', '$.items[1]')");
		Assert.Equal("20", r);
	}

	[Fact]
	public async Task ToJsonString_Integer()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#to_json_string
		var r = await S("SELECT TO_JSON_STRING(42)");
		Assert.Equal("42", r);
	}

	[Fact]
	public async Task ToJsonString_String()
	{
		var r = await S("SELECT TO_JSON_STRING('hello')");
		Assert.Equal("\"hello\"", r);
	}

	[Fact]
	public async Task ToJsonString_Null()
	{
		var r = await S("SELECT TO_JSON_STRING(NULL)");
		Assert.Equal("null", r);
	}

	[Fact]
	public async Task ToJsonString_Boolean()
	{
		var r = await S("SELECT TO_JSON_STRING(TRUE)");
		Assert.Equal("true", r);
	}

	// ========================================================================
	// TIMESTAMP CONVERSION FUNCTIONS
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions
	// ========================================================================

	[Fact]
	public async Task UnixSeconds_Epoch()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#unix_seconds
		var r = await S("SELECT UNIX_SECONDS(TIMESTAMP '2020-01-01 00:00:00 UTC')");
		Assert.Equal("1577836800", r);
	}

	[Fact]
	public async Task UnixMillis_Epoch()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#unix_millis
		var r = await S("SELECT UNIX_MILLIS(TIMESTAMP '2020-01-01 00:00:00 UTC')");
		Assert.Equal("1577836800000", r);
	}

	[Fact]
	public async Task UnixMicros_Epoch()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#unix_micros
		var r = await S("SELECT UNIX_MICROS(TIMESTAMP '2020-01-01 00:00:00 UTC')");
		Assert.Equal("1577836800000000", r);
	}

	[Fact]
	public async Task TimestampSeconds_FromUnix()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_seconds
		var r = await S("SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_SECONDS(1577836800))");
		Assert.Equal("2020-01-01 00:00:00", r);
	}

	[Fact]
	public async Task TimestampMillis_FromUnix()
	{
		var r = await S("SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MILLIS(1577836800000))");
		Assert.Equal("2020-01-01 00:00:00", r);
	}

	[Fact]
	public async Task TimestampMicros_FromUnix()
	{
		var r = await S("SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MICROS(1577836800000000))");
		Assert.Equal("2020-01-01 00:00:00", r);
	}

	[Fact]
	public async Task TimestampSub_Hours()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_sub
		var r = await S("SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_SUB(TIMESTAMP '2020-01-01 12:00:00 UTC', INTERVAL 3 HOUR))");
		Assert.Equal("2020-01-01 09:00:00", r);
	}

	[Fact]
	public async Task FormatTimestamp_Basic()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#format_timestamp
		var r = await S("SELECT FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP '2023-06-15 10:30:00 UTC')");
		Assert.Equal("2023-06-15", r);
	}

	[Fact]
	public async Task ParseTimestamp_Basic()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#parse_timestamp
		var r = await S("SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', '2023-06-15 10:30:00'))");
		Assert.Equal("2023-06-15 10:30:00", r);
	}

	// ========================================================================
	// MATH FUNCTIONS
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions
	// ========================================================================

	[Fact]
	public async Task Abs_Negative() => Assert.Equal("5", await S("SELECT ABS(-5)"));

	[Fact]
	public async Task Abs_Positive() => Assert.Equal("3", await S("SELECT ABS(3)"));

	[Fact]
	public async Task Round_Basic() => Assert.Equal("3", await S("SELECT CAST(ROUND(2.5) AS INT64)"));

	[Fact]
	public async Task Round_DecimalPlaces() => Assert.Equal("3.14", await S("SELECT ROUND(3.14159, 2)"));

	[Fact]
	public async Task Trunc_Positive() => Assert.Equal("3", await S("SELECT CAST(TRUNC(3.7) AS INT64)"));

	[Fact]
	public async Task Trunc_Negative() => Assert.Equal("-3", await S("SELECT CAST(TRUNC(-3.7) AS INT64)"));

	[Fact]
	public async Task Ceil_Positive() => Assert.Equal("4", await S("SELECT CAST(CEIL(3.1) AS INT64)"));

	[Fact]
	public async Task Floor_Positive() => Assert.Equal("3", await S("SELECT CAST(FLOOR(3.9) AS INT64)"));

	[Fact]
	public async Task Pow_Basic() => Assert.Equal("8", await S("SELECT CAST(POW(2, 3) AS INT64)"));

	[Fact]
	public async Task Sqrt_Basic() => Assert.Equal("3", await S("SELECT CAST(SQRT(9) AS INT64)"));

	[Fact]
	public async Task Log_Base10() => Assert.Equal("2", await S("SELECT CAST(LOG(100, 10) AS INT64)"));

	[Fact]
	public async Task Ln_E() => Assert.Equal("1", await S("SELECT CAST(LN(EXP(1)) AS INT64)"));

	[Fact]
	public async Task IsInf_True() => Assert.Equal("True", await S("SELECT IS_INF(CAST('inf' AS FLOAT64))"));

	[Fact]
	public async Task IsNan_True() => Assert.Equal("True", await S("SELECT IS_NAN(CAST('nan' AS FLOAT64))"));

	[Fact]
	public async Task SafeAdd_Overflow_ReturnsNull()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#safe_add
		var r = await S("SELECT SAFE_ADD(9223372036854775807, 1)");
		Assert.Null(r);
	}

	[Fact]
	public async Task SafeSubtract_Overflow_ReturnsNull()
	{
		var r = await S("SELECT SAFE_SUBTRACT(CAST('-9223372036854775808' AS INT64), 1)");
		Assert.Null(r);
	}

	[Fact]
	public async Task SafeMultiply_Overflow_ReturnsNull()
	{
		var r = await S("SELECT SAFE_MULTIPLY(9223372036854775807, 2)");
		Assert.Null(r);
	}

	[Fact]
	public async Task SafeNegate_OverflowMin_ReturnsNull()
	{
		// Use CAST to ensure INT64 type (literal 9223372036854775808 exceeds long range and parses as double)
		var r = await S("SELECT SAFE_NEGATE(CAST('-9223372036854775808' AS INT64))");
		Assert.Null(r);
	}

	// ========================================================================
	// STRING FUNCTIONS
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
	// ========================================================================

	[Fact]
	public async Task Strpos_Found()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#strpos
		var r = await S("SELECT STRPOS('hello world', 'world')");
		Assert.Equal("7", r);
	}

	[Fact]
	public async Task Strpos_NotFound()
	{
		var r = await S("SELECT STRPOS('hello', 'xyz')");
		Assert.Equal("0", r);
	}

	[Fact]
	public async Task Instr_Basic()
	{
		var r = await S("SELECT INSTR('banana', 'an')");
		Assert.Equal("2", r);
	}

	[Fact]
	public async Task Lpad_Shorter()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#lpad
		var r = await S("SELECT LPAD('hi', 5, '0')");
		Assert.Equal("000hi", r);
	}

	[Fact]
	public async Task Rpad_Shorter()
	{
		var r = await S("SELECT RPAD('hi', 5, '0')");
		Assert.Equal("hi000", r);
	}

	[Fact]
	public async Task Left_Basic()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#left
		var r = await S("SELECT LEFT('hello', 3)");
		Assert.Equal("hel", r);
	}

	[Fact]
	public async Task Right_Basic()
	{
		var r = await S("SELECT RIGHT('hello', 3)");
		Assert.Equal("llo", r);
	}

	[Fact]
	public async Task Initcap_Basic()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#initcap
		var r = await S("SELECT INITCAP('hello world')");
		Assert.Equal("Hello World", r);
	}

	[Fact]
	public async Task CharLength_Basic()
	{
		var r = await S("SELECT CHAR_LENGTH('hello')");
		Assert.Equal("5", r);
	}

	[Fact]
	public async Task ByteLength_Ascii()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#byte_length
		var r = await S("SELECT BYTE_LENGTH('hello')");
		Assert.Equal("5", r);
	}

	[Fact]
	public async Task Ascii_Basic()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#ascii
		var r = await S("SELECT ASCII('A')");
		Assert.Equal("65", r);
	}

	[Fact]
	public async Task Chr_Basic()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#chr
		var r = await S("SELECT CHR(65)");
		Assert.Equal("A", r);
	}

	[Fact]
	public async Task Unicode_Basic()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#unicode
		var r = await S("SELECT UNICODE('A')");
		Assert.Equal("65", r);
	}

	[Fact]
	public async Task ContainsSubstr_Found()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#contains_substr
		var r = await S("SELECT CONTAINS_SUBSTR('hello world', 'WORLD')");
		Assert.Equal("True", r);
	}

	[Fact]
	public async Task ContainsSubstr_NotFound()
	{
		var r = await S("SELECT CONTAINS_SUBSTR('hello', 'xyz')");
		Assert.Equal("False", r);
	}

	[Fact]
	public async Task RegexpExtractAll_Basic()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_extract_all
		var r = await S("SELECT ARRAY_LENGTH(REGEXP_EXTRACT_ALL('abc123def456', r'\\d+'))");
		Assert.Equal("2", r);
	}

	// ========================================================================
	// AGGREGATE FUNCTIONS
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions
	// ========================================================================

	[Fact]
	public async Task AnyValue_ReturnsNonNull()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#any_value
		await Exec("CREATE TABLE `{ds}.av` (id INT64, val STRING)");
		await Exec("INSERT INTO `{ds}.av` VALUES (1, 'a'), (2, NULL), (3, 'c')");
		var r = await S("SELECT ANY_VALUE(val) FROM `{ds}.av`");
		// ANY_VALUE returns any non-null value, so it should not be null
		Assert.NotNull(r);
	}

	[Fact]
	public async Task ArrayAgg_RespectNulls()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#array_agg
		// "By default, ARRAY_AGG includes NULLs."
		var r = await S("SELECT ARRAY_LENGTH(ARRAY_AGG(x)) FROM UNNEST([1, NULL, 3]) AS x");
		Assert.Equal("3", r);
	}

	[Fact]
	public async Task ArrayConcatAgg_Basic()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#array_concat_agg
		await Exec("CREATE TABLE `{ds}.aca` (id INT64, arr ARRAY<INT64>)");
		await Exec("INSERT INTO `{ds}.aca` VALUES (1, [1, 2]), (2, [3, 4])");
		var r = await S("SELECT ARRAY_LENGTH(ARRAY_CONCAT_AGG(arr)) FROM `{ds}.aca`");
		Assert.Equal("4", r);
	}

	[Fact]
	public async Task BitAnd_Basic()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#bit_and
		var r = await S("SELECT BIT_AND(x) FROM UNNEST([7, 5, 3]) AS x");
		// 7=111, 5=101, 3=011 → AND = 001 = 1
		Assert.Equal("1", r);
	}

	[Fact]
	public async Task BitOr_Basic()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#bit_or
		var r = await S("SELECT BIT_OR(x) FROM UNNEST([1, 2, 4]) AS x");
		// 001 | 010 | 100 = 111 = 7
		Assert.Equal("7", r);
	}

	[Fact]
	public async Task BitXor_Basic()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#bit_xor
		var r = await S("SELECT BIT_XOR(x) FROM UNNEST([5, 3]) AS x");
		// 101 XOR 011 = 110 = 6
		Assert.Equal("6", r);
	}

	[Fact]
	public async Task VarSamp_Basic()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#var_samp
		// VAR_SAMP of [2, 4, 4, 4, 5, 5, 7, 9] = 4.571...
		var r = await S("SELECT ROUND(VAR_SAMP(x), 2) FROM UNNEST([2, 4, 4, 4, 5, 5, 7, 9]) AS x");
		Assert.Equal("4.57", r);
	}

	[Fact]
	public async Task StddevSamp_Basic()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#stddev_samp
		// STDDEV = sqrt(VAR_SAMP)
		var r = await S("SELECT ROUND(STDDEV_SAMP(x), 2) FROM UNNEST([2, 4, 4, 4, 5, 5, 7, 9]) AS x");
		Assert.Equal("2.14", r);
	}

	[Fact]
	public async Task VarPop_Basic()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#var_pop
		var r = await S("SELECT ROUND(VAR_POP(x), 1) FROM UNNEST([2, 4, 6]) AS x");
		// mean=4, var_pop = ((2-4)^2 + (4-4)^2 + (6-4)^2) / 3 = 8/3 = 2.666... → 2.7
		Assert.Equal("2.7", r);
	}

	[Fact]
	public async Task Corr_Perfect()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#corr
		// Perfect positive correlation
		await Exec("CREATE TABLE `{ds}.corr_t` (x FLOAT64, y FLOAT64)");
		await Exec("INSERT INTO `{ds}.corr_t` VALUES (1.0, 2.0), (2.0, 4.0), (3.0, 6.0)");
		var r = await S("SELECT ROUND(CORR(x, y), 1) FROM `{ds}.corr_t`");
		// CORR returns 1.0 (FLOAT64) which FormatValue renders as "1" (integer form for whole floats)
		Assert.Equal("1", r);
	}

	[Fact]
	public async Task ApproxQuantiles_Median()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/approximate_aggregate_functions#approx_quantiles
		// APPROX_QUANTILES(x, 2) returns [min, median, max]
		var r = await S("SELECT (APPROX_QUANTILES(x, 2))[OFFSET(1)] FROM UNNEST([1, 2, 3, 4, 5]) AS x");
		Assert.Equal("3", r);
	}

	// ========================================================================
	// IN SUBQUERY
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#in_operators
	// ========================================================================

	[Fact]
	public async Task InSubquery_Found()
	{
		await Exec("CREATE TABLE `{ds}.ins1` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.ins1` VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");
		var rows = await Q("SELECT name FROM `{ds}.ins1` WHERE id IN (SELECT x FROM UNNEST([1, 3]) AS x) ORDER BY name");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("Charlie", rows[1]["name"]?.ToString());
	}

	[Fact]
	public async Task InSubquery_NotIn()
	{
		await Exec("CREATE TABLE `{ds}.ins2` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.ins2` VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");
		var rows = await Q("SELECT name FROM `{ds}.ins2` WHERE id NOT IN (SELECT x FROM UNNEST([1, 3]) AS x) ORDER BY name");
		Assert.Single(rows);
		Assert.Equal("Bob", rows[0]["name"]?.ToString());
	}

	// ========================================================================
	// PIVOT
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#pivot_operator
	// ========================================================================

	[Fact]
	public async Task Pivot_Basic()
	{
		await Exec("CREATE TABLE `{ds}.pvt` (product STRING, quarter STRING, revenue INT64)");
		await Exec("INSERT INTO `{ds}.pvt` VALUES ('A','Q1',100), ('A','Q2',200), ('B','Q1',150), ('B','Q2',250)");
		var rows = await Q(@"
			SELECT * FROM `{ds}.pvt`
			PIVOT(SUM(revenue) FOR quarter IN ('Q1', 'Q2'))
			ORDER BY product");
		Assert.Equal(2, rows.Count);
		Assert.Equal("100", rows[0]["Q1"]?.ToString());
		Assert.Equal("200", rows[0]["Q2"]?.ToString());
	}

	// ========================================================================
	// MERGE
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement
	// ========================================================================

	[Fact]
	public async Task Merge_InsertAndUpdate()
	{
		await Exec("CREATE TABLE `{ds}.mt` (id INT64, val STRING)");
		await Exec("INSERT INTO `{ds}.mt` VALUES (1, 'old')");
		await Exec(@"
			MERGE `{ds}.mt` AS t
			USING (SELECT 1 AS id, 'new' AS val UNION ALL SELECT 2, 'added') AS s
			ON t.id = s.id
			WHEN MATCHED THEN UPDATE SET val = s.val
			WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)");
		var rows = await Q("SELECT id, val FROM `{ds}.mt` ORDER BY id");
		Assert.Equal(2, rows.Count);
		Assert.Equal("new", rows[0]["val"]?.ToString());
		Assert.Equal("added", rows[1]["val"]?.ToString());
	}

	[Fact]
	public async Task Merge_Delete()
	{
		await Exec("CREATE TABLE `{ds}.md` (id INT64, active BOOL)");
		await Exec("INSERT INTO `{ds}.md` VALUES (1, TRUE), (2, FALSE), (3, TRUE)");
		await Exec(@"
			MERGE `{ds}.md` AS t
			USING (SELECT id FROM `{ds}.md` WHERE active = FALSE) AS s
			ON t.id = s.id
			WHEN MATCHED THEN DELETE");
		var rows = await Q("SELECT id FROM `{ds}.md` ORDER BY id");
		Assert.Equal(2, rows.Count);
		Assert.Equal("1", rows[0]["id"]?.ToString());
		Assert.Equal("3", rows[1]["id"]?.ToString());
	}

	// ========================================================================
	// DATETIME / TIME FUNCTIONS
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions
	// ========================================================================

	[Fact]
	public async Task DatetimeAdd_Days()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_add
		var r = await S("SELECT CAST(DATETIME_ADD(DATETIME '2023-01-15 10:00:00', INTERVAL 5 DAY) AS STRING)");
		Assert.Equal("2023-01-20T10:00:00", r);
	}

	[Fact]
	public async Task DatetimeSub_Hours()
	{
		var r = await S("SELECT CAST(DATETIME_SUB(DATETIME '2023-01-15 10:00:00', INTERVAL 3 HOUR) AS STRING)");
		Assert.Equal("2023-01-15T07:00:00", r);
	}

	[Fact]
	public async Task DatetimeDiff_Days()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_diff
		var r = await S("SELECT DATETIME_DIFF(DATETIME '2023-01-20', DATETIME '2023-01-15', DAY)");
		Assert.Equal("5", r);
	}

	[Fact]
	public async Task DatetimeTrunc_Month()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_trunc
		var r = await S("SELECT CAST(DATETIME_TRUNC(DATETIME '2023-06-15 10:30:00', MONTH) AS STRING)");
		Assert.Equal("2023-06-01T00:00:00", r);
	}

	[Fact]
	public async Task TimeDiff_Seconds()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time_diff
		var r = await S("SELECT TIME_DIFF(TIME '10:30:00', TIME '10:00:00', SECOND)");
		Assert.Equal("1800", r);
	}

	[Fact]
	public async Task TimeAdd_Minutes()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time_add
		var r = await S("SELECT CAST(TIME_ADD(TIME '10:00:00', INTERVAL 30 MINUTE) AS STRING)");
		Assert.Equal("10:30:00", r);
	}

	[Fact]
	public async Task TimeTrunc_Hour()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time_trunc
		var r = await S("SELECT CAST(TIME_TRUNC(TIME '10:45:30', HOUR) AS STRING)");
		Assert.Equal("10:00:00", r);
	}

	// ========================================================================
	// HASH / ENCODING FUNCTIONS
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions
	// ========================================================================

	[Fact]
	public async Task Md5_Known()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#md5
		var r = await S("SELECT TO_HEX(MD5('hello'))");
		Assert.Equal("5d41402abc4b2a76b9719d911017c592", r);
	}

	[Fact]
	public async Task Sha256_Known()
	{
		var r = await S("SELECT TO_HEX(SHA256('hello'))");
		Assert.Equal("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", r);
	}

	[Fact]
	public async Task ToBase64_Basic()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#to_base64
		var r = await S("SELECT TO_BASE64(b'hello')");
		Assert.Equal("aGVsbG8=", r);
	}

	[Fact]
	public async Task FromBase64_Basic()
	{
		var r = await S("SELECT CAST(FROM_BASE64('aGVsbG8=') AS STRING)");
		Assert.Equal("hello", r);
	}

	[Fact]
	public async Task ToHex_Basic()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#to_hex
		var r = await S("SELECT TO_HEX(b'Hello')");
		Assert.Equal("48656c6c6f", r);
	}

	// ========================================================================
	// WINDOW FUNCTIONS - PERCENTILE
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions
	// ========================================================================

	[Fact]
	public async Task PercentileCont_Median()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#percentile_cont
		var r = await S(@"
			SELECT PERCENTILE_CONT(x, 0.5) OVER() AS median
			FROM UNNEST([1, 2, 3, 4, 5]) AS x
			LIMIT 1");
		Assert.Equal("3", r);
	}

	[Fact]
	public async Task PercentileDisc_Median()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#percentile_disc
		var r = await S(@"
			SELECT PERCENTILE_DISC(x, 0.5) OVER() AS median
			FROM UNNEST([1, 2, 3, 4, 5]) AS x
			LIMIT 1");
		Assert.Equal("3", r);
	}

	// ========================================================================
	// UNPIVOT
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unpivot_operator
	// ========================================================================

	[Fact]
	public async Task Unpivot_Basic()
	{
		await Exec("CREATE TABLE `{ds}.upvt` (id INT64, q1 INT64, q2 INT64)");
		await Exec("INSERT INTO `{ds}.upvt` VALUES (1, 100, 200), (2, 150, 250)");
		var rows = await Q(@"
			SELECT * FROM `{ds}.upvt`
			UNPIVOT(revenue FOR quarter IN (q1, q2))
			ORDER BY id, quarter");
		Assert.Equal(4, rows.Count);
		Assert.Equal("100", rows[0]["revenue"]?.ToString());
		Assert.Equal("q1", rows[0]["quarter"]?.ToString());
	}

	// ========================================================================
	// SCALAR SUBQUERY EDGE CASES
	// ========================================================================

	[Fact]
	public async Task ScalarSubquery_InSelect()
	{
		await Exec("CREATE TABLE `{ds}.ss1` (id INT64, dept STRING)");
		await Exec("CREATE TABLE `{ds}.ss2` (dept STRING, budget INT64)");
		await Exec("INSERT INTO `{ds}.ss1` VALUES (1, 'eng'), (2, 'sales')");
		await Exec("INSERT INTO `{ds}.ss2` VALUES ('eng', 1000), ('sales', 500)");
		var rows = await Q(@"
			SELECT id, (SELECT budget FROM `{ds}.ss2` WHERE dept = t.dept) AS budget
			FROM `{ds}.ss1` AS t ORDER BY id");
		Assert.Equal(2, rows.Count);
		Assert.Equal("1000", rows[0]["budget"]?.ToString());
		Assert.Equal("500", rows[1]["budget"]?.ToString());
	}

	// ========================================================================
	// MISCELLANEOUS
	// ========================================================================

	[Fact]
	public async Task GenerateArray_IntStep()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_array
		var r = await S("SELECT ARRAY_LENGTH(GENERATE_ARRAY(1, 10, 2))");
		Assert.Equal("5", r);
	}

	[Fact]
	public async Task ArrayConcat_TwoArrays()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_concat
		var r = await S("SELECT ARRAY_LENGTH(ARRAY_CONCAT([1, 2], [3, 4, 5]))");
		Assert.Equal("5", r);
	}

	[Fact]
	public async Task GenerateUuid_NotNull()
	{
		var r = await S("SELECT GENERATE_UUID()");
		Assert.NotNull(r);
		Assert.Equal(36, r!.Length); // UUID format: 8-4-4-4-12
	}

	[Fact]
	public async Task RangeBucket_Basic()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#range_bucket
		// RANGE_BUCKET(point, array) returns 0-based bucket index
		var r = await S("SELECT RANGE_BUCKET(25, [0, 10, 20, 30, 40])");
		Assert.Equal("3", r);
	}

	[Fact]
	public async Task Soundex_Words()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#soundex
		var r = await S("SELECT SOUNDEX('Robert')");
		Assert.Equal("R163", r);
	}
}
