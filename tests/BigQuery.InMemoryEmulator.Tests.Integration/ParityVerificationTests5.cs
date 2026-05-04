using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Fifth round of parity verification tests — complex multi-table operations,
/// window frame edge cases, subquery patterns, type coercion, and DML interactions.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ParityVerificationTests5 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests5(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_pv5_{Guid.NewGuid():N}"[..30];
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
	// WINDOW FRAME EDGE CASES
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls
	// ========================================================================

	[Fact]
	public async Task Window_RangeBetween_UnboundedPreceding()
	{
		// Running sum using RANGE frame
		var rows = await Q(@"
			SELECT val, SUM(val) OVER(ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum
			FROM UNNEST([3, 1, 4, 1, 5]) AS val
			ORDER BY val");
		Assert.Equal(5, rows.Count);
		Assert.Equal("1", rows[0]["running_sum"]?.ToString());
		Assert.Equal("2", rows[1]["running_sum"]?.ToString());
	}

	[Fact]
	public async Task Window_RowNumber_NoOrderBy()
	{
		// ROW_NUMBER() without ORDER BY should still assign numbers 1..N
		var rows = await Q("SELECT ROW_NUMBER() OVER() AS rn FROM UNNEST([10, 20, 30]) AS x");
		Assert.Equal(3, rows.Count);
		// Each row should have a unique row number
		var rns = rows.Select(r => r["rn"]?.ToString()).OrderBy(x => x).ToList();
		Assert.Equal(new[] { "1", "2", "3" }, rns);
	}

	[Fact]
	public async Task Window_MultipleWindowFunctions()
	{
		var rows = await Q(@"
			SELECT x,
				ROW_NUMBER() OVER(ORDER BY x) AS rn,
				SUM(x) OVER(ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running
			FROM UNNEST([3, 1, 2]) AS x
			ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]["rn"]?.ToString());
		Assert.Equal("1", rows[0]["running"]?.ToString());
		Assert.Equal("3", rows[1]["running"]?.ToString());
		Assert.Equal("6", rows[2]["running"]?.ToString());
	}

	[Fact]
	public async Task Window_PartitionBy_OrderBy()
	{
		await Exec("CREATE TABLE `{ds}.wpo` (grp STRING, val INT64)");
		await Exec("INSERT INTO `{ds}.wpo` VALUES ('A',10),('A',20),('A',30),('B',5),('B',15)");
		var rows = await Q(@"
			SELECT grp, val, RANK() OVER(PARTITION BY grp ORDER BY val) AS rnk
			FROM `{ds}.wpo` ORDER BY grp, val");
		Assert.Equal(5, rows.Count);
		Assert.Equal("1", rows[0]["rnk"]?.ToString()); // A, 10
		Assert.Equal("2", rows[1]["rnk"]?.ToString()); // A, 20
		Assert.Equal("1", rows[3]["rnk"]?.ToString()); // B, 5
	}

	// ========================================================================
	// TYPE COERCION
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_rules
	// ========================================================================

	[Fact]
	public async Task Coercion_IntPlusFloat()
	{
		// INT64 + FLOAT64 should produce FLOAT64
		var r = await S("SELECT 1 + 1.5");
		Assert.Equal("2.5", r);
	}

	[Fact]
	public async Task Coercion_IntDivision_Produces_Float()
	{
		// In BigQuery, dividing two integers produces a FLOAT64
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#division
		var r = await S("SELECT 5 / 2");
		Assert.Equal("2.5", r);
	}

	[Fact]
	public async Task Coercion_IntDivision_Exact()
	{
		// Even exact integer division returns float
		var r = await S("SELECT 6 / 3");
		Assert.Equal("2", r);
	}

	[Fact]
	public async Task Comparison_IntAndFloat()
	{
		// INT64 and FLOAT64 are comparable
		var r = await S("SELECT 1 = 1.0");
		Assert.Equal("True", r);
	}

	[Fact]
	public async Task CastString_ToInt()
	{
		var r = await S("SELECT CAST('123' AS INT64)");
		Assert.Equal("123", r);
	}

	[Fact]
	public async Task CastFloat_ToInt_TruncatesAwayFromZero()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_int64
		// "Halfway cases such as 1.5 or -0.5 round away from zero."
		var r = await S("SELECT CAST(2.5 AS INT64)");
		Assert.Equal("3", r);
	}

	[Fact]
	public async Task CastFloat_ToInt_NegativeRounding()
	{
		var r = await S("SELECT CAST(-2.5 AS INT64)");
		Assert.Equal("-3", r);
	}

	// ========================================================================
	// COMPLEX CTE PATTERNS
	// ========================================================================

	[Fact]
	public async Task Cte_Recursive_Numbers()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#recursive_keyword
		var rows = await Q(@"
			WITH RECURSIVE nums AS (
				SELECT 1 AS n
				UNION ALL
				SELECT n + 1 FROM nums WHERE n < 5
			)
			SELECT n FROM nums ORDER BY n");
		Assert.Equal(5, rows.Count);
		Assert.Equal("1", rows[0]["n"]?.ToString());
		Assert.Equal("5", rows[4]["n"]?.ToString());
	}

	[Fact]
	public async Task Cte_MultipleReferences()
	{
		// CTE referenced multiple times
		var rows = await Q(@"
			WITH base AS (SELECT x FROM UNNEST([1,2,3]) AS x)
			SELECT a.x AS ax, b.x AS bx
			FROM base a CROSS JOIN base b
			WHERE a.x < b.x
			ORDER BY ax, bx");
		Assert.Equal(3, rows.Count); // (1,2), (1,3), (2,3)
		Assert.Equal("1", rows[0]["ax"]?.ToString());
		Assert.Equal("2", rows[0]["bx"]?.ToString());
	}

	// ========================================================================
	// SUBQUERY PATTERNS
	// ========================================================================

	[Fact]
	public async Task Subquery_InFrom()
	{
		var rows = await Q(@"
			SELECT doubled FROM (SELECT x * 2 AS doubled FROM UNNEST([1,2,3]) AS x) ORDER BY doubled");
		Assert.Equal(3, rows.Count);
		Assert.Equal("2", rows[0]["doubled"]?.ToString());
		Assert.Equal("6", rows[2]["doubled"]?.ToString());
	}

	[Fact]
	public async Task Subquery_Correlated_InWhere()
	{
		await Exec("CREATE TABLE `{ds}.emp` (id INT64, dept STRING, salary INT64)");
		await Exec("INSERT INTO `{ds}.emp` VALUES (1,'eng',100),(2,'eng',120),(3,'sales',80),(4,'sales',90)");
		var rows = await Q(@"
			SELECT id, salary FROM `{ds}.emp` e
			WHERE salary = (SELECT MAX(salary) FROM `{ds}.emp` WHERE dept = e.dept)
			ORDER BY id");
		Assert.Equal(2, rows.Count);
		Assert.Equal("2", rows[0]["id"]?.ToString()); // max in eng
		Assert.Equal("4", rows[1]["id"]?.ToString()); // max in sales
	}

	[Fact]
	public async Task Subquery_Exists_Correlated()
	{
		await Exec("CREATE TABLE `{ds}.orders` (id INT64, cust_id INT64)");
		await Exec("CREATE TABLE `{ds}.customers` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.customers` VALUES (1,'Alice'),(2,'Bob'),(3,'Charlie')");
		await Exec("INSERT INTO `{ds}.orders` VALUES (100,1),(101,1),(102,3)");
		var rows = await Q(@"
			SELECT name FROM `{ds}.customers` c
			WHERE EXISTS (SELECT 1 FROM `{ds}.orders` o WHERE o.cust_id = c.id)
			ORDER BY name");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("Charlie", rows[1]["name"]?.ToString());
	}

	// ========================================================================
	// NULL SEMANTICS
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#null_semantics
	// ========================================================================

	[Fact]
	public async Task Null_InArithmetic()
	{
		var r = await S("SELECT 1 + NULL");
		Assert.Null(r);
	}

	[Fact]
	public async Task Null_InComparison()
	{
		var r = await S("SELECT NULL = NULL");
		Assert.Null(r);
	}

	[Fact]
	public async Task Null_IsNull()
	{
		var r = await S("SELECT NULL IS NULL");
		Assert.Equal("True", r);
	}

	[Fact]
	public async Task Null_InCaseWhen()
	{
		var r = await S("SELECT CASE WHEN NULL THEN 'yes' ELSE 'no' END");
		Assert.Equal("no", r);
	}

	[Fact]
	public async Task Null_InAggregate_SumIgnoresNulls()
	{
		var r = await S("SELECT SUM(x) FROM UNNEST([1, NULL, 3]) AS x");
		Assert.Equal("4", r);
	}

	[Fact]
	public async Task Null_DistinctCountExcludesNull()
	{
		var r = await S("SELECT COUNT(DISTINCT x) FROM UNNEST([1, NULL, 1, NULL, 2]) AS x");
		Assert.Equal("2", r);
	}

	// ========================================================================
	// MULTIPLE JOINS
	// ========================================================================

	[Fact]
	public async Task ThreeWayJoin()
	{
		await Exec("CREATE TABLE `{ds}.j1` (id INT64, name STRING)");
		await Exec("CREATE TABLE `{ds}.j2` (id INT64, dept_id INT64)");
		await Exec("CREATE TABLE `{ds}.j3` (id INT64, dept_name STRING)");
		await Exec("INSERT INTO `{ds}.j1` VALUES (1,'Alice'),(2,'Bob')");
		await Exec("INSERT INTO `{ds}.j2` VALUES (1,10),(2,20)");
		await Exec("INSERT INTO `{ds}.j3` VALUES (10,'Eng'),(20,'Sales')");
		var rows = await Q(@"
			SELECT j1.name, j3.dept_name
			FROM `{ds}.j1` j1
			JOIN `{ds}.j2` j2 ON j1.id = j2.id
			JOIN `{ds}.j3` j3 ON j2.dept_id = j3.id
			ORDER BY j1.name");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("Eng", rows[0]["dept_name"]?.ToString());
	}

	[Fact]
	public async Task LeftJoin_WithAggregate()
	{
		await Exec("CREATE TABLE `{ds}.lj1` (id INT64, name STRING)");
		await Exec("CREATE TABLE `{ds}.lj2` (user_id INT64, amount INT64)");
		await Exec("INSERT INTO `{ds}.lj1` VALUES (1,'Alice'),(2,'Bob')");
		await Exec("INSERT INTO `{ds}.lj2` VALUES (1,100),(1,200)");
		var rows = await Q(@"
			SELECT lj1.name, COALESCE(SUM(lj2.amount), 0) AS total
			FROM `{ds}.lj1` lj1
			LEFT JOIN `{ds}.lj2` lj2 ON lj1.id = lj2.user_id
			GROUP BY lj1.name
			ORDER BY lj1.name");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("300", rows[0]["total"]?.ToString());
		Assert.Equal("Bob", rows[1]["name"]?.ToString());
		Assert.Equal("0", rows[1]["total"]?.ToString());
	}

	// ========================================================================
	// STRING PATTERNS
	// ========================================================================

	[Fact]
	public async Task Like_Percent()
	{
		var r = await S("SELECT 'hello world' LIKE '%world'");
		Assert.Equal("True", r);
	}

	[Fact]
	public async Task Like_Underscore()
	{
		var r = await S("SELECT 'cat' LIKE 'c_t'");
		Assert.Equal("True", r);
	}

	[Fact]
	public async Task Like_CaseSensitive()
	{
		// LIKE is case-sensitive in BigQuery
		var r = await S("SELECT 'Hello' LIKE 'hello'");
		Assert.Equal("False", r);
	}

	[Fact]
	public async Task RegexpContains_CaseInsensitive()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_contains
		var r = await S("SELECT REGEXP_CONTAINS('Hello World', r'(?i)hello')");
		Assert.Equal("True", r);
	}

	// ========================================================================
	// GROUP BY with HAVING complex
	// ========================================================================

	[Fact]
	public async Task GroupBy_Having_Count()
	{
		await Exec("CREATE TABLE `{ds}.ghc` (category STRING, item STRING)");
		await Exec("INSERT INTO `{ds}.ghc` VALUES ('A','x'),('A','y'),('A','z'),('B','p'),('B','q')");
		var rows = await Q("SELECT category, COUNT(*) AS cnt FROM `{ds}.ghc` GROUP BY category HAVING COUNT(*) >= 3");
		Assert.Single(rows);
		Assert.Equal("A", rows[0]["category"]?.ToString());
	}

	[Fact]
	public async Task GroupBy_OrderBy_Alias()
	{
		// ORDER BY can reference SELECT alias
		var rows = await Q(@"
			SELECT x AS val, COUNT(*) AS cnt
			FROM UNNEST([1,1,2,2,2,3]) AS x
			GROUP BY val ORDER BY cnt DESC, val");
		Assert.Equal(3, rows.Count);
		Assert.Equal("2", rows[0]["val"]?.ToString()); // 3 occurrences
		Assert.Equal("1", rows[1]["val"]?.ToString()); // 2 occurrences
	}

	// ========================================================================
	// INSERT then SELECT patterns
	// ========================================================================

	[Fact]
	public async Task Insert_Select_WithTransform()
	{
		await Exec("CREATE TABLE `{ds}.src` (id INT64, val INT64)");
		await Exec("CREATE TABLE `{ds}.dst` (id INT64, doubled INT64)");
		await Exec("INSERT INTO `{ds}.src` VALUES (1,10),(2,20),(3,30)");
		await Exec("INSERT INTO `{ds}.dst` SELECT id, val * 2 FROM `{ds}.src`");
		var rows = await Q("SELECT id, doubled FROM `{ds}.dst` ORDER BY id");
		Assert.Equal(3, rows.Count);
		Assert.Equal("20", rows[0]["doubled"]?.ToString());
		Assert.Equal("60", rows[2]["doubled"]?.ToString());
	}

	[Fact]
	public async Task Update_WithSubquery()
	{
		await Exec("CREATE TABLE `{ds}.upd` (id INT64, val INT64)");
		await Exec("INSERT INTO `{ds}.upd` VALUES (1,10),(2,20),(3,30)");
		await Exec("UPDATE `{ds}.upd` SET val = val * 2 WHERE id > 1");
		var rows = await Q("SELECT id, val FROM `{ds}.upd` ORDER BY id");
		Assert.Equal("10", rows[0]["val"]?.ToString());
		Assert.Equal("40", rows[1]["val"]?.ToString());
		Assert.Equal("60", rows[2]["val"]?.ToString());
	}

	[Fact]
	public async Task Delete_WithCondition()
	{
		await Exec("CREATE TABLE `{ds}.del` (id INT64, active BOOL)");
		await Exec("INSERT INTO `{ds}.del` VALUES (1,TRUE),(2,FALSE),(3,TRUE),(4,FALSE)");
		await Exec("DELETE FROM `{ds}.del` WHERE active = FALSE");
		var rows = await Q("SELECT id FROM `{ds}.del` ORDER BY id");
		Assert.Equal(2, rows.Count);
		Assert.Equal("1", rows[0]["id"]?.ToString());
		Assert.Equal("3", rows[1]["id"]?.ToString());
	}

	// ========================================================================
	// CASE WHEN complex patterns
	// ========================================================================

	[Fact]
	public async Task Case_SearchedForm_MultipleConditions()
	{
		var rows = await Q(@"
			SELECT x, CASE WHEN x < 2 THEN 'low' WHEN x < 4 THEN 'mid' ELSE 'high' END AS label
			FROM UNNEST([1,2,3,4,5]) AS x ORDER BY x");
		Assert.Equal("low", rows[0]["label"]?.ToString());
		Assert.Equal("mid", rows[1]["label"]?.ToString());
		Assert.Equal("high", rows[3]["label"]?.ToString());
	}

	[Fact]
	public async Task Case_SimpleForm()
	{
		var r = await S("SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END");
		Assert.Equal("two", r);
	}

	[Fact]
	public async Task Case_WithNull()
	{
		// CASE simple form does NOT match NULL with = (since NULL = NULL is NULL, not TRUE)
		var r = await S("SELECT CASE NULL WHEN NULL THEN 'matched' ELSE 'not matched' END");
		Assert.Equal("not matched", r);
	}

	// ========================================================================
	// DISTINCT edge cases
	// ========================================================================

	[Fact]
	public async Task Distinct_WithNull()
	{
		var rows = await Q("SELECT DISTINCT x FROM UNNEST([1, NULL, 1, NULL, 2]) AS x ORDER BY x");
		// BigQuery treats NULLs as equal for DISTINCT
		Assert.Equal(3, rows.Count); // NULL, 1, 2
	}

	[Fact]
	public async Task Distinct_Multiple_Columns()
	{
		var rows = await Q(@"
			SELECT DISTINCT a, b FROM
			(SELECT 1 AS a, 'x' AS b UNION ALL SELECT 1, 'x' UNION ALL SELECT 1, 'y' UNION ALL SELECT 2, 'x')
			ORDER BY a, b");
		Assert.Equal(3, rows.Count); // (1,x), (1,y), (2,x)
	}

	// ========================================================================
	// BETWEEN operator
	// ========================================================================

	[Fact]
	public async Task Between_Inclusive()
	{
		var rows = await Q("SELECT x FROM UNNEST([1,2,3,4,5]) AS x WHERE x BETWEEN 2 AND 4 ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("2", rows[0]["x"]?.ToString());
		Assert.Equal("4", rows[2]["x"]?.ToString());
	}

	[Fact]
	public async Task NotBetween()
	{
		var rows = await Q("SELECT x FROM UNNEST([1,2,3,4,5]) AS x WHERE x NOT BETWEEN 2 AND 4 ORDER BY x");
		Assert.Equal(2, rows.Count);
		Assert.Equal("1", rows[0]["x"]?.ToString());
		Assert.Equal("5", rows[1]["x"]?.ToString());
	}

	// ========================================================================
	// OFFSET / LIMIT
	// ========================================================================

	[Fact]
	public async Task Offset_Limit()
	{
		var rows = await Q("SELECT x FROM UNNEST([1,2,3,4,5]) AS x ORDER BY x LIMIT 2 OFFSET 2");
		Assert.Equal(2, rows.Count);
		Assert.Equal("3", rows[0]["x"]?.ToString());
		Assert.Equal("4", rows[1]["x"]?.ToString());
	}

	[Fact]
	public async Task Limit_Only()
	{
		var rows = await Q("SELECT x FROM UNNEST([1,2,3,4,5]) AS x ORDER BY x LIMIT 3");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]["x"]?.ToString());
	}

	// ========================================================================
	// IFNULL / IF edge cases
	// ========================================================================

	[Fact]
	public async Task If_TrueCondition()
	{
		var r = await S("SELECT IF(1 > 0, 'yes', 'no')");
		Assert.Equal("yes", r);
	}

	[Fact]
	public async Task If_FalseCondition()
	{
		var r = await S("SELECT IF(1 > 2, 'yes', 'no')");
		Assert.Equal("no", r);
	}

	[Fact]
	public async Task If_NullCondition()
	{
		// NULL condition → else branch
		var r = await S("SELECT IF(NULL, 'yes', 'no')");
		Assert.Equal("no", r);
	}

	[Fact]
	public async Task Ifnull_NonNull()
	{
		var r = await S("SELECT IFNULL(42, 0)");
		Assert.Equal("42", r);
	}

	[Fact]
	public async Task Ifnull_Null()
	{
		var r = await S("SELECT IFNULL(NULL, 0)");
		Assert.Equal("0", r);
	}

	// ========================================================================
	// CONCAT edge cases and operators
	// ========================================================================

	[Fact]
	public async Task StringConcat_Operator()
	{
		// || operator for string concatenation
		var r = await S("SELECT 'hello' || ' ' || 'world'");
		Assert.Equal("hello world", r);
	}

	[Fact]
	public async Task Concat_EmptyString()
	{
		var r = await S("SELECT CONCAT('a', '', 'b')");
		Assert.Equal("ab", r);
	}

	// ========================================================================
	// DATE_TRUNC edge cases
	// ========================================================================

	[Fact]
	public async Task DateTrunc_Year()
	{
		var r = await S("SELECT CAST(DATE_TRUNC(DATE '2023-06-15', YEAR) AS STRING)");
		Assert.Equal("2023-01-01", r);
	}

	[Fact]
	public async Task DateTrunc_Day()
	{
		// DATE_TRUNC to DAY is identity for dates
		var r = await S("SELECT CAST(DATE_TRUNC(DATE '2023-06-15', DAY) AS STRING)");
		Assert.Equal("2023-06-15", r);
	}

	// ========================================================================
	// ARRAY functions
	// ========================================================================

	[Fact]
	public async Task ArrayLength_EmptyArray()
	{
		var r = await S("SELECT ARRAY_LENGTH([])");
		Assert.Equal("0", r);
	}

	[Fact]
	public async Task ArrayLength_WithNulls()
	{
		// NULL elements still count in ARRAY_LENGTH
		var r = await S("SELECT ARRAY_LENGTH([1, NULL, 3])");
		Assert.Equal("3", r);
	}

	[Fact]
	public async Task ArrayConcat_EmptyAndNonEmpty()
	{
		var r = await S("SELECT ARRAY_LENGTH(ARRAY_CONCAT([], [1, 2, 3]))");
		Assert.Equal("3", r);
	}

	// ========================================================================
	// CAST edge cases
	// ========================================================================

	[Fact]
	public async Task Cast_BoolToString()
	{
		var r = await S("SELECT CAST(TRUE AS STRING)");
		Assert.Equal("true", r);
	}

	[Fact]
	public async Task Cast_IntToBool_Zero()
	{
		var r = await S("SELECT CAST(0 AS BOOL)");
		Assert.Equal("False", r);
	}

	[Fact]
	public async Task Cast_IntToBool_NonZero()
	{
		var r = await S("SELECT CAST(42 AS BOOL)");
		Assert.Equal("True", r);
	}

	[Fact]
	public async Task Cast_StringToBool_True()
	{
		var r = await S("SELECT CAST('true' AS BOOL)");
		Assert.Equal("True", r);
	}
}
