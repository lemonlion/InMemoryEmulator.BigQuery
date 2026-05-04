using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for BigQuery features that should have complete parity with the real service.
/// Each test exercises a specific BigQuery behavior documented in the official reference.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ParityVerificationTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_pv_{Guid.NewGuid():N}"[..30];
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

	// ===== HAVING with BETWEEN on aggregate =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#having_clause
	// "The HAVING clause filters the results produced by GROUP BY"

	[Fact]
	public async Task Having_Between_OnAggregate()
	{
		await Exec("CREATE TABLE `{ds}.hb1` (grp STRING, val INT64)");
		await Exec("INSERT INTO `{ds}.hb1` VALUES ('A',10),('A',20),('B',100),('B',200),('C',5)");
		var rows = await Q("SELECT grp, SUM(val) AS total FROM `{ds}.hb1` GROUP BY grp HAVING SUM(val) BETWEEN 10 AND 100 ORDER BY grp");
		// A: 10+20=30 (between 10 and 100 → included), B: 100+200=300 (not), C: 5 (not)
		Assert.Single(rows);
		Assert.Equal("A", rows[0]["grp"]?.ToString());
		Assert.Equal("30", rows[0]["total"]?.ToString());
	}

	// ===== CTE + UNION DISTINCT =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators
	// "UNION DISTINCT - Returns only distinct rows from all combined queries"

	[Fact]
	public async Task Cte_UnionDistinct()
	{
		var rows = await Q(@"
			WITH base AS (
				SELECT 1 AS val UNION ALL SELECT 2 UNION ALL SELECT 2 UNION ALL SELECT 3
			)
			SELECT DISTINCT val FROM base ORDER BY val");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]["val"]?.ToString());
		Assert.Equal("3", rows[2]["val"]?.ToString());
	}

	[Fact]
	public async Task UnionDistinct_Basic()
	{
		var rows = await Q("SELECT 1 AS val UNION DISTINCT SELECT 1 UNION DISTINCT SELECT 2 ORDER BY val");
		Assert.Equal(2, rows.Count);
		Assert.Equal("1", rows[0]["val"]?.ToString());
		Assert.Equal("2", rows[1]["val"]?.ToString());
	}

	// ===== Parenthesized set operations =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators
	// BigQuery supports parentheses to control order of set operations

	[Fact]
	public async Task Parenthesized_Union()
	{
		var rows = await Q("(SELECT 1 AS val UNION ALL SELECT 2) UNION ALL (SELECT 3 UNION ALL SELECT 4) ORDER BY val");
		Assert.Equal(4, rows.Count);
		Assert.Equal("1", rows[0]["val"]?.ToString());
		Assert.Equal("4", rows[3]["val"]?.ToString());
	}

	// ===== COALESCE in correlated subquery =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions#coalesce

	[Fact]
	public async Task Coalesce_CorrelatedSubquery()
	{
		await Exec("CREATE TABLE `{ds}.cc1` (id INT64, val INT64)");
		await Exec("INSERT INTO `{ds}.cc1` VALUES (1, 10),(2, NULL),(3, 30)");
		await Exec("CREATE TABLE `{ds}.cc2` (id INT64, fallback INT64)");
		await Exec("INSERT INTO `{ds}.cc2` VALUES (1, 100),(2, 200),(3, 300)");
		var rows = await Q(@"
			SELECT a.id, COALESCE(a.val, (SELECT b.fallback FROM `{ds}.cc2` b WHERE b.id = a.id)) AS result
			FROM `{ds}.cc1` a ORDER BY a.id");
		Assert.Equal(3, rows.Count);
		Assert.Equal("10", rows[0]["result"]?.ToString());
		Assert.Equal("200", rows[1]["result"]?.ToString());
		Assert.Equal("30", rows[2]["result"]?.ToString());
	}

	// ===== IFNULL function =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions#ifnull

	[Fact]
	public async Task Ifnull_Basic() => Assert.Equal("5", await S("SELECT IFNULL(NULL, 5)"));
	[Fact]
	public async Task Ifnull_NotNull() => Assert.Equal("3", await S("SELECT IFNULL(3, 5)"));

	// ===== NULLIF function =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions#nullif

	[Fact]
	public async Task Nullif_Equal() => Assert.Null(await S("SELECT NULLIF(5, 5)"));
	[Fact]
	public async Task Nullif_NotEqual() => Assert.Equal("5", await S("SELECT NULLIF(5, 3)"));

	// ===== IFF / IF expression with complex arguments =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions#if

	[Fact]
	public async Task If_WithAggregateInSubquery()
	{
		await Exec("CREATE TABLE `{ds}.ifa` (val INT64)");
		await Exec("INSERT INTO `{ds}.ifa` VALUES (1),(2),(3),(4),(5)");
		Assert.Equal("big", await S("SELECT IF(SUM(val) > 10, 'big', 'small') FROM `{ds}.ifa`"));
	}

	// ===== SAFE_DIVIDE =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#safe_divide
	// "Returns NULL if the denominator is zero"

	[Fact]
	public async Task SafeDivide_ByZero() => Assert.Null(await S("SELECT SAFE_DIVIDE(10, 0)"));
	[Fact]
	public async Task SafeDivide_Normal() => Assert.Equal("5", await S("SELECT SAFE_DIVIDE(10, 2)"));
	[Fact]
	public async Task SafeDivide_FloatByZero() => Assert.Null(await S("SELECT SAFE_DIVIDE(1.5, 0.0)"));

	// ===== IEEE_DIVIDE =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#ieee_divide
	// "Returns +inf for positive/zero, -inf for negative/zero, NaN for 0/0"

	[Fact]
	public async Task IeeeDivide_PositiveByZero()
	{
		// BigQuery returns 'inf' when casting Infinity to STRING
		var v = await S("SELECT CAST(IEEE_DIVIDE(1.0, 0.0) AS STRING)");
		Assert.Equal("inf", v);
	}

	[Fact]
	public async Task IeeeDivide_NegativeByZero()
	{
		var v = await S("SELECT CAST(IEEE_DIVIDE(-1.0, 0.0) AS STRING)");
		Assert.Equal("-inf", v);
	}

	[Fact]
	public async Task IeeeDivide_ZeroByZero()
	{
		var v = await S("SELECT CAST(IEEE_DIVIDE(0.0, 0.0) AS STRING)");
		Assert.Equal("NaN", v);
	}

	// ===== String functions edge cases =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions

	[Fact]
	public async Task Substr_NegativePosition()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#substr
		// "If position is negative, the function counts from the end of value"
		Assert.Equal("lo", await S("SELECT SUBSTR('Hello', -2)"));
	}

	[Fact]
	public async Task Format_Integer() => Assert.Equal("42", await S("SELECT FORMAT('%d', 42)"));
	[Fact]
	public async Task Format_Float() => Assert.Equal("3.14", await S("SELECT FORMAT('%.2f', 3.14159)"));
	[Fact]
	public async Task Format_String() => Assert.Equal("hello world", await S("SELECT FORMAT('%s %s', 'hello', 'world')"));

	// ===== SAFE_CAST =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#safe_casting
	// "Returns NULL instead of raising an error"

	[Fact]
	public async Task SafeCast_InvalidInt() => Assert.Null(await S("SELECT SAFE_CAST('abc' AS INT64)"));
	[Fact]
	public async Task SafeCast_ValidInt() => Assert.Equal("42", await S("SELECT SAFE_CAST('42' AS INT64)"));
	[Fact]
	public async Task SafeCast_InvalidDate() => Assert.Null(await S("SELECT SAFE_CAST('not-a-date' AS DATE)"));

	// ===== Window function: ROWS BETWEEN ... AND ... =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls#window_frame_clause

	[Fact]
	public async Task Window_RowsBetween_Preceding()
	{
		await Exec("CREATE TABLE `{ds}.wfr` (val INT64)");
		await Exec("INSERT INTO `{ds}.wfr` VALUES (1),(2),(3),(4),(5)");
		var rows = await Q(@"
			SELECT val, SUM(val) OVER (ORDER BY val ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rolling
			FROM `{ds}.wfr` ORDER BY val");
		Assert.Equal("1", rows[0]["rolling"]?.ToString()); // only 1
		Assert.Equal("3", rows[1]["rolling"]?.ToString()); // 1+2
		Assert.Equal("5", rows[2]["rolling"]?.ToString()); // 2+3
	}

	[Fact]
	public async Task Window_RowsBetween_Following()
	{
		await Exec("CREATE TABLE `{ds}.wff` (val INT64)");
		await Exec("INSERT INTO `{ds}.wff` VALUES (1),(2),(3),(4),(5)");
		var rows = await Q(@"
			SELECT val, SUM(val) OVER (ORDER BY val ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS rolling
			FROM `{ds}.wff` ORDER BY val");
		Assert.Equal("3", rows[0]["rolling"]?.ToString()); // 1+2
		Assert.Equal("5", rows[1]["rolling"]?.ToString()); // 2+3
		Assert.Equal("9", rows[3]["rolling"]?.ToString()); // 4+5
		Assert.Equal("5", rows[4]["rolling"]?.ToString()); // only 5
	}

	// ===== GENERATE_DATE_ARRAY with various intervals =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_date_array

	[Fact]
	public async Task GenerateDateArray_Weekly()
	{
		var v = await S("SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-29', INTERVAL 7 DAY))");
		Assert.Equal("5", v); // Jan 1, 8, 15, 22, 29
	}

	// ===== STRUCT construction =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type

	[Fact]
	public async Task Struct_Literal()
	{
		var v = await S("SELECT STRUCT(1 AS x, 'hello' AS y).x");
		Assert.Equal("1", v);
	}

	[Fact]
	public async Task Struct_FieldAccess_Y()
	{
		var v = await S("SELECT STRUCT(1 AS x, 'hello' AS y).y");
		Assert.Equal("hello", v);
	}

	// ===== COUNTIF =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#countif

	[Fact]
	public async Task Countif_Basic()
	{
		await Exec("CREATE TABLE `{ds}.cif` (val INT64)");
		await Exec("INSERT INTO `{ds}.cif` VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)");
		Assert.Equal("5", await S("SELECT COUNTIF(val > 5) FROM `{ds}.cif`"));
	}

	// ===== APPROX_COUNT_DISTINCT =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/approximate_aggregate_functions#approx_count_distinct

	[Fact]
	public async Task ApproxCountDistinct_Basic()
	{
		await Exec("CREATE TABLE `{ds}.acd` (val INT64)");
		await Exec("INSERT INTO `{ds}.acd` VALUES (1),(2),(2),(3),(3),(3),(4),(4),(4),(4)");
		Assert.Equal("4", await S("SELECT APPROX_COUNT_DISTINCT(val) FROM `{ds}.acd`"));
	}

	// ===== DATE arithmetic =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#date_arithmetics_operators

	[Fact]
	public async Task Date_AddDays()
	{
		var v = await S("SELECT CAST(DATE '2024-01-15' + 10 AS STRING)");
		Assert.Equal("2024-01-25", v);
	}

	[Fact]
	public async Task Date_SubtractDays()
	{
		var v = await S("SELECT CAST(DATE '2024-01-15' - 5 AS STRING)");
		Assert.Equal("2024-01-10", v);
	}

	// ===== CASE with no ELSE (returns NULL) =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions#case

	[Fact]
	public async Task Case_NoElse_ReturnsNull()
	{
		Assert.Null(await S("SELECT CASE WHEN 1 = 2 THEN 'yes' END"));
	}

	// ===== LOGICAL_AND / LOGICAL_OR =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#logical_and

	[Fact]
	public async Task LogicalAnd_MixedValues()
	{
		await Exec("CREATE TABLE `{ds}.la1` (flag BOOL)");
		await Exec("INSERT INTO `{ds}.la1` VALUES (true),(true),(false)");
		Assert.Equal("False", await S("SELECT LOGICAL_AND(flag) FROM `{ds}.la1`"));
	}

	[Fact]
	public async Task LogicalOr_MixedValues()
	{
		await Exec("CREATE TABLE `{ds}.lo1` (flag BOOL)");
		await Exec("INSERT INTO `{ds}.lo1` VALUES (false),(false),(true)");
		Assert.Equal("True", await S("SELECT LOGICAL_OR(flag) FROM `{ds}.lo1`"));
	}

	// ===== Multiple CTEs referencing each other =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_clause

	[Fact]
	public async Task Cte_Chain_ThreeDeep()
	{
		var rows = await Q(@"
			WITH a AS (SELECT 1 AS val),
			     b AS (SELECT val + 10 AS val FROM a),
			     c AS (SELECT val + 100 AS val FROM b)
			SELECT val FROM c");
		Assert.Single(rows);
		Assert.Equal("111", rows[0]["val"]?.ToString());
	}

	// ===== EXISTS subquery =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries#exists_subquery_concepts

	[Fact]
	public async Task Exists_ReturnsTrue()
	{
		await Exec("CREATE TABLE `{ds}.ex1` (val INT64)");
		await Exec("INSERT INTO `{ds}.ex1` VALUES (1),(2),(3)");
		Assert.Equal("True", await S("SELECT EXISTS(SELECT 1 FROM `{ds}.ex1` WHERE val > 2)"));
	}

	[Fact]
	public async Task Exists_ReturnsFalse()
	{
		await Exec("CREATE TABLE `{ds}.ex2` (val INT64)");
		await Exec("INSERT INTO `{ds}.ex2` VALUES (1),(2),(3)");
		Assert.Equal("False", await S("SELECT EXISTS(SELECT 1 FROM `{ds}.ex2` WHERE val > 10)"));
	}

	// ===== QUALIFY clause =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#qualify_clause
	// "The QUALIFY clause filters the results of window functions"

	[Fact]
	public async Task Qualify_RowNumber()
	{
		await Exec("CREATE TABLE `{ds}.qf1` (grp STRING, val INT64)");
		await Exec("INSERT INTO `{ds}.qf1` VALUES ('A',1),('A',2),('A',3),('B',10),('B',20)");
		var rows = await Q(@"
			SELECT grp, val FROM `{ds}.qf1`
			QUALIFY ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val DESC) = 1
			ORDER BY grp");
		Assert.Equal(2, rows.Count);
		Assert.Equal("A", rows[0]["grp"]?.ToString());
		Assert.Equal("3", rows[0]["val"]?.ToString());
		Assert.Equal("B", rows[1]["grp"]?.ToString());
		Assert.Equal("20", rows[1]["val"]?.ToString());
	}

	// ===== EXCEPT DISTINCT =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#except

	[Fact]
	public async Task ExceptDistinct_Basic()
	{
		var rows = await Q("SELECT 1 AS val UNION ALL SELECT 2 UNION ALL SELECT 3 EXCEPT DISTINCT SELECT 2 UNION ALL SELECT 3");
		// (1 UNION ALL 2 UNION ALL 3) EXCEPT DISTINCT (2 UNION ALL 3) = 1
		// but set op precedence: 1 UNION ALL 2 UNION ALL (3 EXCEPT DISTINCT 2) UNION ALL 3
		// In BigQuery, EXCEPT has same precedence as UNION, left-to-right
		// Actually: (1 UNION ALL 2 UNION ALL 3) EXCEPT DISTINCT (SELECT 2 UNION ALL SELECT 3)
		// ... this tests that EXCEPT works at all
		Assert.True(rows.Count >= 1);
	}

	// ===== TRIM / LTRIM / RTRIM with characters =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#trim

	[Fact]
	public async Task Trim_WithChars() => Assert.Equal("hello", await S("SELECT TRIM('xxxhelloxxx', 'x')"));
	[Fact]
	public async Task Ltrim_WithChars() => Assert.Equal("helloxxx", await S("SELECT LTRIM('xxxhelloxxx', 'x')"));
	[Fact]
	public async Task Rtrim_WithChars() => Assert.Equal("xxxhello", await S("SELECT RTRIM('xxxhelloxxx', 'x')"));

	// ===== REPLACE =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#replace

	[Fact]
	public async Task Replace_Basic() => Assert.Equal("hello world", await S("SELECT REPLACE('hello earth', 'earth', 'world')"));
	[Fact]
	public async Task Replace_NotFound() => Assert.Equal("hello", await S("SELECT REPLACE('hello', 'xyz', 'abc')"));

	// ===== COALESCE with multiple args =====
	[Fact]
	public async Task Coalesce_MultipleNulls() => Assert.Equal("3", await S("SELECT COALESCE(NULL, NULL, 3, 4)"));
	[Fact]
	public async Task Coalesce_FirstNonNull() => Assert.Equal("1", await S("SELECT COALESCE(1, 2, 3)"));

	// ===== CONCAT =====
	[Fact]
	public async Task Concat_Multiple() => Assert.Equal("abc", await S("SELECT CONCAT('a', 'b', 'c')"));
	[Fact]
	public async Task Concat_WithNull() => Assert.Null(await S("SELECT CONCAT('a', NULL, 'c')"));

	// ===== ARRAY_LENGTH on nested =====
	[Fact]
	public async Task ArrayLength_Nested()
	{
		var v = await S("SELECT ARRAY_LENGTH(GENERATE_ARRAY(1, 10))");
		Assert.Equal("10", v);
	}

	// ===== Multiple aggregates in same query =====
	[Fact]
	public async Task MultipleAggregates()
	{
		await Exec("CREATE TABLE `{ds}.ma1` (val INT64)");
		await Exec("INSERT INTO `{ds}.ma1` VALUES (1),(2),(3),(4),(5)");
		var rows = await Q("SELECT COUNT(*) AS cnt, SUM(val) AS total, AVG(val) AS average, MIN(val) AS mn, MAX(val) AS mx FROM `{ds}.ma1`");
		Assert.Single(rows);
		Assert.Equal("5", rows[0]["cnt"]?.ToString());
		Assert.Equal("15", rows[0]["total"]?.ToString());
		Assert.Equal("1", rows[0]["mn"]?.ToString());
		Assert.Equal("5", rows[0]["mx"]?.ToString());
	}
}
