using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Third round of parity verification tests — targeting STRING_AGG with DISTINCT+ORDER BY,
/// complex subqueries, JOIN USING, window functions over aggregates, and advanced date/time.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ParityVerificationTests3 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests3(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_pv3_{Guid.NewGuid():N}"[..30];
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

	// ===== STRING_AGG with DISTINCT and ORDER BY =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#string_agg
	// "STRING_AGG([DISTINCT] expression [, delimiter] [ORDER BY key [{ASC|DESC}]])"

	[Fact]
	public async Task StringAgg_DistinctAndOrderBy()
	{
		await Exec("CREATE TABLE `{ds}.sado1` (val STRING)");
		await Exec("INSERT INTO `{ds}.sado1` VALUES ('c'),('a'),('b'),('a'),('c')");
		var result = await S("SELECT STRING_AGG(DISTINCT val, ',' ORDER BY val ASC) FROM `{ds}.sado1`");
		Assert.Equal("a,b,c", result);
	}

	// ===== Window function over grouped aggregate =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls
	// "Window functions can reference aggregate columns in the SELECT."

	[Fact]
	public async Task Window_OverGroupedAggregate()
	{
		await Exec("CREATE TABLE `{ds}.wga1` (grp STRING, val INT64)");
		await Exec("INSERT INTO `{ds}.wga1` VALUES ('A',10),('A',20),('B',30),('C',40)");
		var rows = await Q("SELECT grp, SUM(val) AS total, RANK() OVER (ORDER BY SUM(val) DESC) AS rnk FROM `{ds}.wga1` GROUP BY grp ORDER BY rnk");
		Assert.Equal(3, rows.Count);
		Assert.Equal("C", rows[0]["grp"]?.ToString());
		Assert.Equal("1", rows[0]["rnk"]?.ToString());
	}

	// ===== CTE with UNNEST cross join =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_clause

	[Fact]
	public async Task Cte_Unnest_CrossJoin()
	{
		var rows = await Q(@"
			WITH data AS (
				SELECT 1 AS id, ['a','b'] AS tags
				UNION ALL
				SELECT 2, ['c']
			)
			SELECT id, tag FROM data CROSS JOIN UNNEST(tags) AS tag ORDER BY id, tag");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]["id"]?.ToString());
		Assert.Equal("a", rows[0]["tag"]?.ToString());
	}

	// ===== JOIN USING =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#join_types

	[Fact]
	public async Task Join_Using_Basic()
	{
		await Exec("CREATE TABLE `{ds}.ju1` (id INT64, name STRING)");
		await Exec("CREATE TABLE `{ds}.ju2` (id INT64, score INT64)");
		await Exec("INSERT INTO `{ds}.ju1` VALUES (1,'Alice'),(2,'Bob')");
		await Exec("INSERT INTO `{ds}.ju2` VALUES (1,90),(2,85)");
		var rows = await Q("SELECT id, name, score FROM `{ds}.ju1` JOIN `{ds}.ju2` USING (id) ORDER BY id");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("90", rows[0]["score"]?.ToString());
	}

	// ===== LEFT JOIN with NULL =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#left_join

	[Fact]
	public async Task LeftJoin_NullForMissing()
	{
		await Exec("CREATE TABLE `{ds}.lj1` (id INT64, name STRING)");
		await Exec("CREATE TABLE `{ds}.lj2` (id INT64, score INT64)");
		await Exec("INSERT INTO `{ds}.lj1` VALUES (1,'Alice'),(2,'Bob'),(3,'Charlie')");
		await Exec("INSERT INTO `{ds}.lj2` VALUES (1,90),(2,85)");
		var rows = await Q("SELECT l.id, l.name, r.score FROM `{ds}.lj1` l LEFT JOIN `{ds}.lj2` r ON l.id = r.id ORDER BY l.id");
		Assert.Equal(3, rows.Count);
		Assert.Null(rows[2]["score"]);
	}

	// ===== QUALIFY clause =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#qualify_clause

	[Fact]
	public async Task Qualify_RowNumber()
	{
		await Exec("CREATE TABLE `{ds}.q1` (grp STRING, val INT64)");
		await Exec("INSERT INTO `{ds}.q1` VALUES ('A',10),('A',20),('B',5),('B',15)");
		var rows = await Q("SELECT grp, val FROM `{ds}.q1` QUALIFY ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val DESC) = 1 ORDER BY grp");
		Assert.Equal(2, rows.Count);
		Assert.Equal("A", rows[0]["grp"]?.ToString());
		Assert.Equal("20", rows[0]["val"]?.ToString());
		Assert.Equal("B", rows[1]["grp"]?.ToString());
		Assert.Equal("15", rows[1]["val"]?.ToString());
	}

	// ===== ARRAY_AGG with IGNORE NULLS =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#array_agg

	[Fact]
	public async Task ArrayAgg_IgnoreNulls()
	{
		var rows = await Q("SELECT v FROM UNNEST((SELECT ARRAY_AGG(x IGNORE NULLS) FROM UNNEST([1, NULL, 2, NULL, 3]) AS x)) AS v");
		Assert.Equal(3, rows.Count);
	}

	// ===== DATE_TRUNC =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_trunc

	[Fact]
	public async Task DateTrunc_Week()
	{
		// DATE '2024-03-15' is Friday. WEEK truncates to Sunday.
		var result = await S("SELECT CAST(DATE_TRUNC(DATE '2024-03-15', WEEK) AS STRING)");
		Assert.Equal("2024-03-10", result);
	}

	[Fact]
	public async Task DateTrunc_Month()
	{
		var result = await S("SELECT CAST(DATE_TRUNC(DATE '2024-03-15', MONTH) AS STRING)");
		Assert.Equal("2024-03-01", result);
	}

	[Fact]
	public async Task DateTrunc_Quarter()
	{
		var result = await S("SELECT CAST(DATE_TRUNC(DATE '2024-08-15', QUARTER) AS STRING)");
		Assert.Equal("2024-07-01", result);
	}

	// ===== TIMESTAMP_TRUNC =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_trunc

	[Fact]
	public async Task TimestampTrunc_Hour()
	{
		var result = await S("SELECT CAST(TIMESTAMP_TRUNC(TIMESTAMP '2024-03-15 14:35:22', HOUR) AS STRING)");
		Assert.Contains("14:00:00", result!);
	}

	// ===== CONCAT with mixed types =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#concat

	[Fact]
	public async Task Concat_NullReturnsNull()
	{
		var result = await S("SELECT CONCAT('hello', NULL)");
		Assert.Null(result);
	}

	// ===== FORMAT function =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#format_string

	[Fact]
	public async Task Format_Integer()
	{
		var result = await S("SELECT FORMAT('%d', 42)");
		Assert.Equal("42", result);
	}

	[Fact]
	public async Task Format_Float()
	{
		var result = await S("SELECT FORMAT('%.2f', 3.14159)");
		Assert.Equal("3.14", result);
	}

	[Fact]
	public async Task Format_String()
	{
		var result = await S("SELECT FORMAT('%s world', 'hello')");
		Assert.Equal("hello world", result);
	}

	// ===== SAFE_CAST =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#safe_casting

	[Fact]
	public async Task SafeCast_InvalidReturnsNull()
	{
		var result = await S("SELECT SAFE_CAST('abc' AS INT64)");
		Assert.Null(result);
	}

	// ===== COUNTIF =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#countif

	[Fact]
	public async Task CountIf_Basic()
	{
		var result = await S("SELECT COUNTIF(val > 2) FROM UNNEST([1,2,3,4,5]) AS val");
		Assert.Equal("3", result);
	}

	// ===== BIT_COUNT =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/bit_functions#bit_count

	[Fact]
	public async Task BitCount()
	{
		var result = await S("SELECT BIT_COUNT(5)");
		Assert.Equal("2", result); // 5 = 101 in binary
	}

	// ===== ARRAY_REVERSE =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_reverse

	[Fact]
	public async Task ArrayReverse()
	{
		var rows = await Q("SELECT v FROM UNNEST(ARRAY_REVERSE([1,2,3])) AS v");
		Assert.Equal(3, rows.Count);
		Assert.Equal("3", rows[0]["v"]?.ToString());
		Assert.Equal("1", rows[2]["v"]?.ToString());
	}

	// ===== GROUP BY ROLLUP =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#group_by_clause

	[Fact]
	public async Task GroupBy_Rollup()
	{
		await Exec("CREATE TABLE `{ds}.gr1` (cat STRING, sub STRING, val INT64)");
		await Exec("INSERT INTO `{ds}.gr1` VALUES ('A','x',1),('A','y',2),('B','x',3)");
		var rows = await Q("SELECT cat, sub, SUM(val) AS total FROM `{ds}.gr1` GROUP BY ROLLUP(cat, sub) ORDER BY cat NULLS LAST, sub NULLS LAST");
		// ROLLUP(cat, sub) generates: (cat,sub), (cat,NULL), (NULL,NULL)
		// A,x,1  A,y,2  A,NULL,3  B,x,3  B,NULL,3  NULL,NULL,6
		Assert.Equal(6, rows.Count);
	}

	// ===== PERCENT_RANK =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions#percent_rank

	[Fact]
	public async Task PercentRank()
	{
		await Exec("CREATE TABLE `{ds}.pr1` (val INT64)");
		await Exec("INSERT INTO `{ds}.pr1` VALUES (10),(20),(30),(40)");
		var rows = await Q("SELECT val, PERCENT_RANK() OVER (ORDER BY val) AS pr FROM `{ds}.pr1` ORDER BY val");
		Assert.Equal(4, rows.Count);
		// PERCENT_RANK = (rank - 1) / (total - 1)
		// For first row: (1-1)/(4-1) = 0
		Assert.Equal("0", rows[0]["pr"]?.ToString());
	}

	// ===== CUME_DIST =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions#cume_dist

	[Fact]
	public async Task CumeDist()
	{
		await Exec("CREATE TABLE `{ds}.cd1` (val INT64)");
		await Exec("INSERT INTO `{ds}.cd1` VALUES (10),(20),(30),(40)");
		var rows = await Q("SELECT val, CUME_DIST() OVER (ORDER BY val) AS cd FROM `{ds}.cd1` ORDER BY val");
		Assert.Equal(4, rows.Count);
		// CUME_DIST = row_number / total
		Assert.Equal("0.25", rows[0]["cd"]?.ToString());
	}

	// ===== NTH_VALUE =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#nth_value

	[Fact]
	public async Task NthValue()
	{
		await Exec("CREATE TABLE `{ds}.nv1` (val INT64)");
		await Exec("INSERT INTO `{ds}.nv1` VALUES (10),(20),(30),(40),(50)");
		var rows = await Q("SELECT val, NTH_VALUE(val, 3) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nth FROM `{ds}.nv1` ORDER BY val");
		Assert.Equal(5, rows.Count);
		Assert.Equal("30", rows[0]["nth"]?.ToString());
		Assert.Equal("30", rows[4]["nth"]?.ToString());
	}

	// ===== NULLS FIRST / NULLS LAST in ORDER BY =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#order_by_clause

	[Fact]
	public async Task OrderBy_NullsFirst()
	{
		await Exec("CREATE TABLE `{ds}.nf1` (val INT64)");
		await Exec("INSERT INTO `{ds}.nf1` (val) VALUES (3),(1),(NULL),(2)");
		var rows = await Q("SELECT val FROM `{ds}.nf1` ORDER BY val ASC NULLS FIRST");
		Assert.Equal(4, rows.Count);
		Assert.Null(rows[0]["val"]);
		Assert.Equal("1", rows[1]["val"]?.ToString());
	}

	[Fact]
	public async Task OrderBy_NullsLast()
	{
		await Exec("CREATE TABLE `{ds}.nl1` (val INT64)");
		await Exec("INSERT INTO `{ds}.nl1` (val) VALUES (3),(1),(NULL),(2)");
		var rows = await Q("SELECT val FROM `{ds}.nl1` ORDER BY val ASC NULLS LAST");
		Assert.Equal(4, rows.Count);
		Assert.Equal("1", rows[0]["val"]?.ToString());
		Assert.Null(rows[3]["val"]);
	}

	// ===== CASE with no match and no ELSE =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions#case_expr
	// "If no match is found and no ELSE clause exists, the result is NULL."

	[Fact]
	public async Task Case_NoMatch_NoElse_ReturnsNull()
	{
		var result = await S("SELECT CASE 'x' WHEN 'a' THEN 1 WHEN 'b' THEN 2 END");
		Assert.Null(result);
	}

	// ===== COALESCE with complex expressions =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions#coalesce

	[Fact]
	public async Task Coalesce_SkipsNulls()
	{
		var result = await S("SELECT COALESCE(NULL, NULL, NULL, 'found', 'ignored')");
		Assert.Equal("found", result);
	}

	// ===== Multiple aggregates in HAVING =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#having_clause

	[Fact]
	public async Task Having_MultipleAggregates()
	{
		await Exec("CREATE TABLE `{ds}.hm1` (grp STRING, val INT64)");
		await Exec("INSERT INTO `{ds}.hm1` VALUES ('A',10),('A',20),('B',5),('C',100)");
		var rows = await Q("SELECT grp, COUNT(*) AS cnt, SUM(val) AS total FROM `{ds}.hm1` GROUP BY grp HAVING COUNT(*) > 1 AND SUM(val) < 50 ORDER BY grp");
		Assert.Single(rows);
		Assert.Equal("A", rows[0]["grp"]?.ToString());
	}

	// ===== EXISTS subquery =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries#exists_subquery_concepts

	[Fact]
	public async Task Exists_InWhere()
	{
		await Exec("CREATE TABLE `{ds}.ex1` (id INT64, name STRING)");
		await Exec("CREATE TABLE `{ds}.ex2` (parent_id INT64)");
		await Exec("INSERT INTO `{ds}.ex1` VALUES (1,'Alice'),(2,'Bob'),(3,'Charlie')");
		await Exec("INSERT INTO `{ds}.ex2` VALUES (1),(3)");
		var rows = await Q("SELECT name FROM `{ds}.ex1` t WHERE EXISTS (SELECT 1 FROM `{ds}.ex2` t2 WHERE t2.parent_id = t.id) ORDER BY name");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("Charlie", rows[1]["name"]?.ToString());
	}

	// ===== NOT EXISTS =====

	[Fact]
	public async Task NotExists_InWhere()
	{
		await Exec("CREATE TABLE `{ds}.ne1` (id INT64, name STRING)");
		await Exec("CREATE TABLE `{ds}.ne2` (parent_id INT64)");
		await Exec("INSERT INTO `{ds}.ne1` VALUES (1,'Alice'),(2,'Bob'),(3,'Charlie')");
		await Exec("INSERT INTO `{ds}.ne2` VALUES (1),(3)");
		var rows = await Q("SELECT name FROM `{ds}.ne1` t WHERE NOT EXISTS (SELECT 1 FROM `{ds}.ne2` t2 WHERE t2.parent_id = t.id) ORDER BY name");
		Assert.Single(rows);
		Assert.Equal("Bob", rows[0]["name"]?.ToString());
	}

	// ===== OFFSET/ORDINAL array access =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#array_subscript_operator

	[Fact]
	public async Task Array_Offset_ZeroBased()
	{
		var result = await S("SELECT [10,20,30][OFFSET(1)]");
		Assert.Equal("20", result);
	}

	[Fact]
	public async Task Array_Ordinal_OneBased()
	{
		var result = await S("SELECT [10,20,30][ORDINAL(1)]");
		Assert.Equal("10", result);
	}

	// ===== DATE_FROM_UNIX_DATE =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_from_unix_date

	[Fact]
	public async Task DateFromUnixDate()
	{
		// Day 0 = 1970-01-01, Day 19797 = 2024-03-15
		var result = await S("SELECT CAST(DATE_FROM_UNIX_DATE(19797) AS STRING)");
		Assert.Equal("2024-03-15", result);
	}

	// ===== UNIX_DATE =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#unix_date

	[Fact]
	public async Task UnixDate()
	{
		var result = await S("SELECT UNIX_DATE(DATE '2024-03-15')");
		Assert.Equal("19797", result);
	}
}
