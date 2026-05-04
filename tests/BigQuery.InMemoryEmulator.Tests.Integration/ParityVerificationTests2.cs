using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Second round of parity verification tests — targeting edge cases in date/time functions,
/// string aggregation, complex joins, window functions, and other known gaps.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ParityVerificationTests2 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests2(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_pv2_{Guid.NewGuid():N}"[..30];
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

	// ===== EXTRACT(ISOYEAR) =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#extract
	// "ISOYEAR: Returns the ISO 8601 week-numbering year."

	[Fact]
	public async Task Extract_IsoYear()
	{
		// Jan 1, 2022 is a Saturday, so ISO year is still 2021 (ISO week 52 of 2021)
		var result = await S("SELECT EXTRACT(ISOYEAR FROM DATE '2022-01-01')");
		Assert.Equal("2021", result);
	}

	[Fact]
	public async Task Extract_IsoWeek()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#extract
		// "ISOWEEK: Returns the ISO 8601 week number of the date_expression."
		var result = await S("SELECT EXTRACT(ISOWEEK FROM DATE '2022-01-01')");
		Assert.Equal("52", result);
	}

	[Fact]
	public async Task Extract_DayOfWeek()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#extract
		// "DAYOFWEEK: Returns values in the range [1,7] with Sunday as the first day of the week."
		// 2024-01-01 is Monday → 2
		var result = await S("SELECT EXTRACT(DAYOFWEEK FROM DATE '2024-01-01')");
		Assert.Equal("2", result);
	}

	[Fact]
	public async Task Extract_DayOfYear()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#extract
		// "DAYOFYEAR: Returns values in the range [1, 366]."
		var result = await S("SELECT EXTRACT(DAYOFYEAR FROM DATE '2024-03-01')");
		Assert.Equal("61", result);
	}

	// ===== FORMAT_DATE edge cases =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#format_date
	
	[Fact]
	public async Task FormatDate_DayOfYear()
	{
		// %j = Day of year as a decimal number (001-366)
		var result = await S("SELECT FORMAT_DATE('%j', DATE '2024-03-01')");
		Assert.Equal("061", result);
	}

	[Fact]
	public async Task FormatDate_WeekNumber()
	{
		// %V = ISO 8601 week number (01-53)
		var result = await S("SELECT FORMAT_DATE('%V', DATE '2022-01-01')");
		Assert.Equal("52", result);
	}

	[Fact]
	public async Task FormatDate_AbbrMonth()
	{
		// %b = Abbreviated month name
		var result = await S("SELECT FORMAT_DATE('%b', DATE '2024-03-15')");
		Assert.Equal("Mar", result);
	}

	// ===== STRING_AGG with ORDER BY =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#string_agg
	// "STRING_AGG([DISTINCT] expression [, delimiter] [ORDER BY key [{ASC|DESC}]])"

	[Fact]
	public async Task StringAgg_WithOrderBy()
	{
		await Exec("CREATE TABLE `{ds}.sa1` (grp STRING, val STRING)");
		await Exec("INSERT INTO `{ds}.sa1` VALUES ('X','c'),('X','a'),('X','b')");
		var result = await S("SELECT STRING_AGG(val, ',' ORDER BY val ASC) FROM `{ds}.sa1`");
		Assert.Equal("a,b,c", result);
	}

	[Fact]
	public async Task StringAgg_WithDistinct()
	{
		await Exec("CREATE TABLE `{ds}.sa2` (val STRING)");
		await Exec("INSERT INTO `{ds}.sa2` VALUES ('a'),('b'),('a'),('c'),('b')");
		var result = await S("SELECT STRING_AGG(DISTINCT val, ',')  FROM `{ds}.sa2`");
		// DISTINCT removes duplicates - result should have exactly 3 unique values
		var parts = result!.Split(',');
		Assert.Equal(3, parts.Length);
		Assert.Contains("a", parts);
		Assert.Contains("b", parts);
		Assert.Contains("c", parts);
	}

	// ===== Integer division =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#division_operator
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#div
	// "DIV(X, Y) - Returns the result of integer division of X by Y."
	// Note: The / operator always returns FLOAT64 in BigQuery.

	[Fact]
	public async Task IntegerDivision_Truncates()
	{
		var result = await S("SELECT DIV(7, 2)");
		Assert.Equal("3", result);
	}

	[Fact]
	public async Task IntegerDivision_Negative_TruncatesTowardZero()
	{
		var result = await S("SELECT DIV(-7, 2)");
		Assert.Equal("-3", result);
	}

	// ===== LAST_DAY =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#last_day

	[Fact]
	public async Task LastDay_Default()
	{
		var result = await S("SELECT CAST(LAST_DAY(DATE '2024-02-15') AS STRING)");
		Assert.Equal("2024-02-29", result);
	}

	[Fact]
	public async Task LastDay_February_NonLeap()
	{
		var result = await S("SELECT CAST(LAST_DAY(DATE '2023-02-15') AS STRING)");
		Assert.Equal("2023-02-28", result);
	}

	// ===== DATE_DIFF =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_diff

	[Fact]
	public async Task DateDiff_Days()
	{
		var result = await S("SELECT DATE_DIFF(DATE '2024-03-01', DATE '2024-02-01', DAY)");
		Assert.Equal("29", result);
	}

	[Fact]
	public async Task DateDiff_Months()
	{
		var result = await S("SELECT DATE_DIFF(DATE '2024-03-15', DATE '2024-01-20', MONTH)");
		Assert.Equal("2", result);
	}

	// ===== ARRAY_AGG with ORDER BY =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#array_agg

	[Fact]
	public async Task ArrayAgg_OrderBy()
	{
		await Exec("CREATE TABLE `{ds}.aa1` (val INT64)");
		await Exec("INSERT INTO `{ds}.aa1` VALUES (3),(1),(2)");
		var rows = await Q("SELECT v FROM `{ds}.aa1`, UNNEST(ARRAY(SELECT val FROM `{ds}.aa1` ORDER BY val)) AS v");
		// Should return values in order: 1, 2, 3 (repeated 3 times since cross join)
		Assert.True(rows.Count > 0);
	}

	// ===== Window frame ROWS BETWEEN =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls#def_of_window_frame

	[Fact]
	public async Task Window_RowsBetween_Preceding()
	{
		await Exec("CREATE TABLE `{ds}.wf1` (val INT64)");
		await Exec("INSERT INTO `{ds}.wf1` VALUES (1),(2),(3),(4),(5)");
		var rows = await Q("SELECT val, SUM(val) OVER (ORDER BY val ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS running FROM `{ds}.wf1` ORDER BY val");
		Assert.Equal(5, rows.Count);
		Assert.Equal("1", rows[0]["running"]?.ToString()); // SUM(1)
		Assert.Equal("3", rows[1]["running"]?.ToString()); // SUM(1,2)
		Assert.Equal("5", rows[2]["running"]?.ToString()); // SUM(2,3)
		Assert.Equal("7", rows[3]["running"]?.ToString()); // SUM(3,4)
		Assert.Equal("9", rows[4]["running"]?.ToString()); // SUM(4,5)
	}

	[Fact]
	public async Task Window_RowsBetween_Following()
	{
		await Exec("CREATE TABLE `{ds}.wf2` (val INT64)");
		await Exec("INSERT INTO `{ds}.wf2` VALUES (1),(2),(3),(4),(5)");
		var rows = await Q("SELECT val, SUM(val) OVER (ORDER BY val ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS s FROM `{ds}.wf2` ORDER BY val");
		Assert.Equal(5, rows.Count);
		Assert.Equal("3", rows[0]["s"]?.ToString()); // SUM(1,2)
		Assert.Equal("5", rows[1]["s"]?.ToString()); // SUM(2,3)
		Assert.Equal("7", rows[2]["s"]?.ToString()); // SUM(3,4)
		Assert.Equal("9", rows[3]["s"]?.ToString()); // SUM(4,5)
		Assert.Equal("5", rows[4]["s"]?.ToString()); // SUM(5)
	}

	// ===== GENERATE_TIMESTAMP_ARRAY =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_timestamp_array

	[Fact]
	public async Task GenerateTimestampArray()
	{
		var rows = await Q("SELECT ts FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 03:00:00', INTERVAL 1 HOUR)) AS ts ORDER BY ts");
		Assert.Equal(4, rows.Count);
	}

	// ===== STRUCT comparison =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type

	[Fact]
	public async Task Struct_Equality()
	{
		var result = await S("SELECT STRUCT(1 AS a, 'x' AS b) = STRUCT(1 AS a, 'x' AS b)");
		Assert.Equal("True", result);
	}

	[Fact]
	public async Task Struct_Inequality()
	{
		var result = await S("SELECT STRUCT(1 AS a, 'x' AS b) = STRUCT(1 AS a, 'y' AS b)");
		Assert.Equal("False", result);
	}

	// ===== TIMESTAMP_DIFF =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_diff

	[Fact]
	public async Task TimestampDiff_Hours()
	{
		var result = await S("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-02 12:00:00', TIMESTAMP '2024-01-01 00:00:00', HOUR)");
		Assert.Equal("36", result);
	}

	// ===== REGEXP_EXTRACT =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_extract

	[Fact]
	public async Task RegexpExtract_Basic()
	{
		var result = await S(@"SELECT REGEXP_EXTRACT('hello world 123', r'\d+')");
		Assert.Equal("123", result);
	}

	[Fact]
	public async Task RegexpExtract_WithCapturingGroup()
	{
		var result = await S(@"SELECT REGEXP_EXTRACT('foo-bar-baz', r'foo-(\w+)-baz')");
		Assert.Equal("bar", result);
	}

	// ===== REGEXP_REPLACE =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_replace

	[Fact]
	public async Task RegexpReplace()
	{
		var result = await S(@"SELECT REGEXP_REPLACE('abc 123 def 456', r'\d+', 'NUM')");
		Assert.Equal("abc NUM def NUM", result);
	}

	// ===== REGEXP_CONTAINS =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_contains

	[Fact]
	public async Task RegexpContains()
	{
		var result = await S(@"SELECT REGEXP_CONTAINS('hello world', r'wor\w+')");
		Assert.Equal("True", result);
	}

	// ===== SPLIT =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#split

	[Fact]
	public async Task Split_Basic()
	{
		var rows = await Q("SELECT part FROM UNNEST(SPLIT('a,b,c', ',')) AS part ORDER BY part");
		Assert.Equal(3, rows.Count);
		Assert.Equal("a", rows[0]["part"]?.ToString());
		Assert.Equal("b", rows[1]["part"]?.ToString());
		Assert.Equal("c", rows[2]["part"]?.ToString());
	}

	// ===== ARRAY_TO_STRING =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_to_string

	[Fact]
	public async Task ArrayToString()
	{
		var result = await S("SELECT ARRAY_TO_STRING(['a','b','c'], '-')");
		Assert.Equal("a-b-c", result);
	}

	// ===== SAFE_DIVIDE =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#safe_divide

	[Fact]
	public async Task SafeDivide_NullOnZero()
	{
		var result = await S("SELECT SAFE_DIVIDE(10, 0)");
		Assert.Null(result);
	}

	// ===== GREATEST / LEAST with mixed types =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#greatest

	[Fact]
	public async Task Greatest_MixedNumeric()
	{
		var result = await S("SELECT GREATEST(1, 2.5, 3)");
		Assert.Equal("3", result);
	}

	[Fact]
	public async Task Least_MixedNumeric()
	{
		var result = await S("SELECT LEAST(1, 2.5, 0.5)");
		Assert.Equal("0.5", result);
	}

	// ===== LATERAL subquery / correlated subquery in SELECT =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries#correlated_subqueries

	[Fact]
	public async Task CorrelatedSubquery_InSelect()
	{
		await Exec("CREATE TABLE `{ds}.cs1` (id INT64, val INT64)");
		await Exec("INSERT INTO `{ds}.cs1` VALUES (1,10),(1,20),(2,30)");
		var rows = await Q("SELECT DISTINCT id, (SELECT SUM(val) FROM `{ds}.cs1` t2 WHERE t2.id = t1.id) AS total FROM `{ds}.cs1` t1 ORDER BY id");
		Assert.Equal(2, rows.Count);
		Assert.Equal("30", rows[0]["total"]?.ToString());
		Assert.Equal("30", rows[1]["total"]?.ToString());
	}

	// ===== IFNULL with expressions =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions#ifnull

	[Fact]
	public async Task IfNull_WithExpression()
	{
		var result = await S("SELECT IFNULL(NULL, 'default')");
		Assert.Equal("default", result);
	}

	// ===== MULTIPLE CTEs chained =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_clause

	[Fact]
	public async Task Cte_Multiple_Chained()
	{
		var rows = await Q(@"
			WITH 
				cte1 AS (SELECT 1 AS a),
				cte2 AS (SELECT a + 1 AS b FROM cte1),
				cte3 AS (SELECT b + 1 AS c FROM cte2)
			SELECT c FROM cte3");
		Assert.Single(rows);
		Assert.Equal("3", rows[0]["c"]?.ToString());
	}

	// ===== CROSS JOIN with UNNEST =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#cross_join

	[Fact]
	public async Task CrossJoin_Unnest()
	{
		await Exec("CREATE TABLE `{ds}.cj1` (id INT64, tags ARRAY<STRING>)");
		await Exec("INSERT INTO `{ds}.cj1` VALUES (1, ['a','b']),(2, ['c'])");
		var rows = await Q("SELECT id, tag FROM `{ds}.cj1` CROSS JOIN UNNEST(tags) AS tag ORDER BY id, tag");
		Assert.Equal(3, rows.Count);
		Assert.Equal("a", rows[0]["tag"]?.ToString());
		Assert.Equal("b", rows[1]["tag"]?.ToString());
		Assert.Equal("c", rows[2]["tag"]?.ToString());
	}

	// ===== CURRENT_DATE / CURRENT_TIMESTAMP =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#current_date

	[Fact]
	public async Task CurrentDate_NotNull()
	{
		var result = await S("SELECT CAST(CURRENT_DATE() AS STRING)");
		Assert.NotNull(result);
		Assert.Matches(@"^\d{4}-\d{2}-\d{2}$", result);
	}

	[Fact]
	public async Task CurrentTimestamp_NotNull()
	{
		var result = await S("SELECT CAST(CURRENT_TIMESTAMP() AS STRING)");
		Assert.NotNull(result);
	}

	// ===== LAG / LEAD window functions =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#lag

	[Fact]
	public async Task Lag_Basic()
	{
		await Exec("CREATE TABLE `{ds}.lag1` (val INT64)");
		await Exec("INSERT INTO `{ds}.lag1` VALUES (10),(20),(30)");
		var rows = await Q("SELECT val, LAG(val) OVER (ORDER BY val) AS prev FROM `{ds}.lag1` ORDER BY val");
		Assert.Equal(3, rows.Count);
		Assert.Null(rows[0]["prev"]);
		Assert.Equal("10", rows[1]["prev"]?.ToString());
		Assert.Equal("20", rows[2]["prev"]?.ToString());
	}

	[Fact]
	public async Task Lead_Basic()
	{
		await Exec("CREATE TABLE `{ds}.lead1` (val INT64)");
		await Exec("INSERT INTO `{ds}.lead1` VALUES (10),(20),(30)");
		var rows = await Q("SELECT val, LEAD(val) OVER (ORDER BY val) AS nxt FROM `{ds}.lead1` ORDER BY val");
		Assert.Equal(3, rows.Count);
		Assert.Equal("20", rows[0]["nxt"]?.ToString());
		Assert.Equal("30", rows[1]["nxt"]?.ToString());
		Assert.Null(rows[2]["nxt"]);
	}

	// ===== NTILE =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions#ntile

	[Fact]
	public async Task Ntile_Basic()
	{
		await Exec("CREATE TABLE `{ds}.nt1` (val INT64)");
		await Exec("INSERT INTO `{ds}.nt1` VALUES (1),(2),(3),(4),(5),(6)");
		var rows = await Q("SELECT val, NTILE(3) OVER (ORDER BY val) AS bucket FROM `{ds}.nt1` ORDER BY val");
		Assert.Equal(6, rows.Count);
		// NTILE(3) over 6 rows: 2 rows each in buckets 1, 2, 3
		Assert.Equal("1", rows[0]["bucket"]?.ToString());
		Assert.Equal("1", rows[1]["bucket"]?.ToString());
		Assert.Equal("2", rows[2]["bucket"]?.ToString());
		Assert.Equal("2", rows[3]["bucket"]?.ToString());
		Assert.Equal("3", rows[4]["bucket"]?.ToString());
		Assert.Equal("3", rows[5]["bucket"]?.ToString());
	}

	// ===== FIRST_VALUE / LAST_VALUE =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#first_value

	[Fact]
	public async Task FirstValue_Basic()
	{
		await Exec("CREATE TABLE `{ds}.fv1` (val INT64)");
		await Exec("INSERT INTO `{ds}.fv1` VALUES (10),(20),(30)");
		var rows = await Q("SELECT val, FIRST_VALUE(val) OVER (ORDER BY val) AS fv FROM `{ds}.fv1` ORDER BY val");
		Assert.Equal(3, rows.Count);
		Assert.Equal("10", rows[0]["fv"]?.ToString());
		Assert.Equal("10", rows[1]["fv"]?.ToString());
		Assert.Equal("10", rows[2]["fv"]?.ToString());
	}

	[Fact]
	public async Task LastValue_WithFrameUnbounded()
	{
		await Exec("CREATE TABLE `{ds}.lv1` (val INT64)");
		await Exec("INSERT INTO `{ds}.lv1` VALUES (10),(20),(30)");
		var rows = await Q("SELECT val, LAST_VALUE(val) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv FROM `{ds}.lv1` ORDER BY val");
		Assert.Equal(3, rows.Count);
		Assert.Equal("30", rows[0]["lv"]?.ToString());
		Assert.Equal("30", rows[1]["lv"]?.ToString());
		Assert.Equal("30", rows[2]["lv"]?.ToString());
	}

	// ===== IF() expression =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions#if

	[Fact]
	public async Task If_WithNullCondition()
	{
		// IF(NULL, ...) should return the false branch
		var result = await S("SELECT IF(NULL, 'yes', 'no')");
		Assert.Equal("no", result);
	}

	// ===== FARM_FINGERPRINT =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#farm_fingerprint

	[Fact]
	public async Task FarmFingerprint_Consistent()
	{
		var r1 = await S("SELECT FARM_FINGERPRINT('hello')");
		var r2 = await S("SELECT FARM_FINGERPRINT('hello')");
		Assert.Equal(r1, r2);
		Assert.NotNull(r1);
	}

	// ===== PARSE_DATE =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#parse_date

	[Fact]
	public async Task ParseDate_Basic()
	{
		var result = await S("SELECT CAST(PARSE_DATE('%Y-%m-%d', '2024-03-15') AS STRING)");
		Assert.Equal("2024-03-15", result);
	}

	// ===== DATE_ADD / DATE_SUB =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_add

	[Fact]
	public async Task DateAdd_Days()
	{
		var result = await S("SELECT CAST(DATE_ADD(DATE '2024-02-28', INTERVAL 1 DAY) AS STRING)");
		Assert.Equal("2024-02-29", result);
	}

	[Fact]
	public async Task DateSub_Months()
	{
		var result = await S("SELECT CAST(DATE_SUB(DATE '2024-03-31', INTERVAL 1 MONTH) AS STRING)");
		Assert.Equal("2024-02-29", result);
	}

	// ===== STARTS_WITH / ENDS_WITH =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#starts_with

	[Fact]
	public async Task StartsWith_Basic()
	{
		var result = await S("SELECT STARTS_WITH('hello world', 'hello')");
		Assert.Equal("True", result);
	}

	[Fact]
	public async Task EndsWith_Basic()
	{
		var result = await S("SELECT ENDS_WITH('hello world', 'world')");
		Assert.Equal("True", result);
	}

	// ===== DENSE_RANK =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions#dense_rank

	[Fact]
	public async Task DenseRank_WithTies()
	{
		await Exec("CREATE TABLE `{ds}.dr1` (val INT64)");
		await Exec("INSERT INTO `{ds}.dr1` VALUES (10),(20),(20),(30)");
		var rows = await Q("SELECT val, DENSE_RANK() OVER (ORDER BY val) AS dr FROM `{ds}.dr1` ORDER BY val");
		Assert.Equal(4, rows.Count);
		Assert.Equal("1", rows[0]["dr"]?.ToString());
		Assert.Equal("2", rows[1]["dr"]?.ToString());
		Assert.Equal("2", rows[2]["dr"]?.ToString());
		Assert.Equal("3", rows[3]["dr"]?.ToString());
	}

	// ===== RANK with gaps =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions#rank

	[Fact]
	public async Task Rank_WithGaps()
	{
		await Exec("CREATE TABLE `{ds}.rk1` (val INT64)");
		await Exec("INSERT INTO `{ds}.rk1` VALUES (10),(20),(20),(30)");
		var rows = await Q("SELECT val, RANK() OVER (ORDER BY val) AS rk FROM `{ds}.rk1` ORDER BY val");
		Assert.Equal(4, rows.Count);
		Assert.Equal("1", rows[0]["rk"]?.ToString());
		Assert.Equal("2", rows[1]["rk"]?.ToString());
		Assert.Equal("2", rows[2]["rk"]?.ToString());
		Assert.Equal("4", rows[3]["rk"]?.ToString()); // Gap: rank 3 is skipped
	}

	// ===== TIMESTAMP arithmetic =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_add

	[Fact]
	public async Task TimestampAdd_Minutes()
	{
		var result = await S("SELECT CAST(TIMESTAMP_ADD(TIMESTAMP '2024-01-01 12:00:00', INTERVAL 30 MINUTE) AS STRING)");
		Assert.Contains("12:30:00", result!);
	}

	// ===== LOGICAL_AND / LOGICAL_OR =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#logical_and

	[Fact]
	public async Task LogicalAnd_AllTrue()
	{
		var result = await S("SELECT LOGICAL_AND(val > 0) FROM UNNEST([1,2,3]) AS val");
		Assert.Equal("True", result);
	}

	[Fact]
	public async Task LogicalAnd_SomeFalse()
	{
		var result = await S("SELECT LOGICAL_AND(val > 1) FROM UNNEST([1,2,3]) AS val");
		Assert.Equal("False", result);
	}

	// ===== MOD function =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#mod

	[Fact]
	public async Task Mod_Function()
	{
		var result = await S("SELECT MOD(10, 3)");
		Assert.Equal("1", result);
	}

	// ===== REPEAT =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#repeat

	[Fact]
	public async Task Repeat_String()
	{
		var result = await S("SELECT REPEAT('ab', 3)");
		Assert.Equal("ababab", result);
	}

	// ===== REVERSE =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#reverse

	[Fact]
	public async Task Reverse_String()
	{
		var result = await S("SELECT REVERSE('hello')");
		Assert.Equal("olleh", result);
	}

	// ===== SIGN =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#sign

	[Fact]
	public async Task Sign_Positive()
	{
		var result = await S("SELECT SIGN(42)");
		Assert.Equal("1", result);
	}

	[Fact]
	public async Task Sign_Negative()
	{
		var result = await S("SELECT SIGN(-5)");
		Assert.Equal("-1", result);
	}

	[Fact]
	public async Task Sign_Zero()
	{
		var result = await S("SELECT SIGN(0)");
		Assert.Equal("0", result);
	}
}
