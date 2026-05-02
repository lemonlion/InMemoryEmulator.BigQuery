using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for STRUCT operations, set operations, CTE patterns, subquery patterns,
/// and miscellaneous SQL expression edge cases.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class SqlExpressionAdvancedTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public SqlExpressionAdvancedTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await _fixture.GetClientAsync();
		return (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
	}

	// ============================================================
	// STRUCT operations
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type
	// ============================================================

	// ---- STRUCT construction ----
	[Fact] public async Task Struct_FieldAccess_Int() => Assert.Equal("1", await Scalar("SELECT STRUCT(1 AS x, 2 AS y).x"));
	[Fact] public async Task Struct_FieldAccess() => Assert.Equal("42", await Scalar("SELECT STRUCT(42 AS val).val"));
	[Fact] public async Task Struct_NestedFieldAccess() => Assert.Equal("inner", await Scalar("SELECT STRUCT(STRUCT('inner' AS a) AS nested).nested.a"));
	[Fact] public async Task Struct_MultiField() => Assert.Equal("hello", await Scalar("SELECT STRUCT(1 AS x, 'hello' AS y, TRUE AS z).y"));
	[Fact] public async Task Struct_NumericField() => Assert.Equal("3", await Scalar("SELECT STRUCT(1 AS a, 2 AS b, 3 AS c).c"));
	[Fact] public async Task Struct_WithNull() => Assert.Null(await Scalar("SELECT STRUCT(CAST(NULL AS STRING) AS v).v"));

	// ============================================================
	// CTE (Common Table Expressions) patterns
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_clause
	// ============================================================

	// ---- Basic CTE ----
	[Fact]
	public async Task Cte_Basic()
	{
		var v = await Scalar("WITH t AS (SELECT 42 AS v) SELECT v FROM t");
		Assert.Equal("42", v);
	}

	[Fact]
	public async Task Cte_Multiple()
	{
		var v = await Scalar("WITH a AS (SELECT 10 AS v), b AS (SELECT 20 AS v) SELECT a.v + b.v FROM a, b");
		Assert.Equal("30", v);
	}

	[Fact]
	public async Task Cte_ReferencesPrior()
	{
		var v = await Scalar("WITH a AS (SELECT 5 AS v), b AS (SELECT v * 2 AS v FROM a) SELECT v FROM b");
		Assert.Equal("10", v);
	}

	[Fact]
	public async Task Cte_UsedMultipleTimes()
	{
		var v = await Scalar("WITH t AS (SELECT 3 AS v) SELECT a.v + b.v FROM t a, t b");
		Assert.Equal("6", v);
	}

	[Fact]
	public async Task Cte_WithUnionAll()
	{
		var rows = await Query("WITH t AS (SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3) SELECT v FROM t ORDER BY v");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
	}

	[Fact]
	public async Task Cte_WithAggregation()
	{
		var v = await Scalar("WITH t AS (SELECT x FROM UNNEST([1, 2, 3, 4, 5]) AS x) SELECT SUM(x) FROM t");
		Assert.Equal("15", v);
	}

	[Fact]
	public async Task Cte_WithWindowFunction()
	{
		var v = await Scalar("WITH t AS (SELECT x FROM UNNEST([10, 20, 30]) AS x) SELECT SUM(x) OVER () FROM t LIMIT 1");
		Assert.Equal("60", v);
	}

	// ============================================================
	// Set operations
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators
	// ============================================================

	[Fact]
	public async Task Union_All()
	{
		var rows = await Query("SELECT 1 AS v UNION ALL SELECT 1 UNION ALL SELECT 2");
		Assert.Equal(3, rows.Count);
	}

	[Fact]
	public async Task Union_Distinct()
	{
		var rows = await Query("SELECT 1 AS v UNION DISTINCT SELECT 1 UNION DISTINCT SELECT 2");
		Assert.Equal(2, rows.Count);
	}

	[Fact]
	public async Task Intersect_Distinct()
	{
		var rows = await Query("SELECT 1 AS v UNION ALL SELECT 2 INTERSECT DISTINCT (SELECT 1 UNION ALL SELECT 1)");
		Assert.True(rows.Count >= 1);
	}

	[Fact]
	public async Task Except_Distinct()
	{
		var v = await Scalar("SELECT 1 AS v EXCEPT DISTINCT SELECT 2");
		Assert.Equal("1", v);
	}

	[Fact]
	public async Task UnionAll_WithOrderBy()
	{
		var rows = await Query("SELECT v FROM (SELECT 2 AS v UNION ALL SELECT 1 UNION ALL SELECT 3) ORDER BY v");
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("3", rows[2][0]?.ToString());
	}

	[Fact]
	public async Task UnionAll_WithLimit()
	{
		var rows = await Query("SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3) LIMIT 2");
		Assert.Equal(2, rows.Count);
	}

	// ============================================================
	// Subquery patterns
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries
	// ============================================================

	[Fact]
	public async Task ScalarSubquery_InSelect()
	{
		var v = await Scalar("SELECT (SELECT MAX(x) FROM UNNEST([1, 2, 3]) AS x)");
		Assert.Equal("3", v);
	}

	[Fact]
	public async Task ExistsSubquery()
	{
		var v = await Scalar("SELECT EXISTS(SELECT 1 FROM UNNEST([1, 2, 3]) AS x WHERE x > 2)");
		Assert.Equal("True", v);
	}

	[Fact]
	public async Task ExistsSubquery_False()
	{
		var v = await Scalar("SELECT EXISTS(SELECT 1 FROM UNNEST([1, 2, 3]) AS x WHERE x > 10)");
		Assert.Equal("False", v);
	}

	[Fact]
	public async Task InSubquery()
	{
		var v = await Scalar("SELECT 2 IN (SELECT x FROM UNNEST([1, 2, 3]) AS x)");
		Assert.Equal("True", v);
	}

	[Fact]
	public async Task InSubquery_False()
	{
		var v = await Scalar("SELECT 5 IN (SELECT x FROM UNNEST([1, 2, 3]) AS x)");
		Assert.Equal("False", v);
	}

	[Fact]
	public async Task ArraySubquery()
	{
		var v = await Scalar("SELECT ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST([1, 2, 3]) AS x WHERE x > 1))");
		Assert.Equal("2", v);
	}

	// ============================================================
	// UNNEST patterns
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unnest
	// ============================================================

	[Fact]
	public async Task Unnest_Basic()
	{
		var rows = await Query("SELECT x FROM UNNEST([10, 20, 30]) AS x");
		Assert.Equal(3, rows.Count);
	}

	[Fact]
	public async Task Unnest_WithOffset()
	{
		var rows = await Query("SELECT x, off FROM UNNEST(['a', 'b', 'c']) AS x WITH OFFSET AS off ORDER BY off");
		Assert.Equal(3, rows.Count);
		Assert.Equal("a", rows[0][0]?.ToString());
		Assert.Equal("0", rows[0][1]?.ToString());
	}

	[Fact]
	public async Task Unnest_WithWhere()
	{
		var rows = await Query("SELECT x FROM UNNEST([1, 2, 3, 4, 5]) AS x WHERE x > 3");
		Assert.Equal(2, rows.Count);
	}

	[Fact]
	public async Task Unnest_EmptyArray()
	{
		var rows = await Query("SELECT x FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x");
		Assert.Empty(rows);
	}

	[Fact]
	public async Task Unnest_WithAggregation()
	{
		var v = await Scalar("SELECT COUNT(*) FROM UNNEST([1, 2, 3, 4, 5]) AS x");
		Assert.Equal("5", v);
	}

	// ============================================================
	// Miscellaneous expression edge cases
	// ============================================================

	// ---- Ternary / IIF-like via CASE ----
	[Fact] public async Task Case_Simple() => Assert.Equal("yes", await Scalar("SELECT CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END"));
	[Fact] public async Task Case_SimpleElse() => Assert.Equal("no", await Scalar("SELECT CASE WHEN 1 = 2 THEN 'yes' ELSE 'no' END"));
	[Fact] public async Task Case_ValueMatch() => Assert.Equal("two", await Scalar("SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' WHEN 3 THEN 'three' END"));
	[Fact] public async Task Case_NoMatch() => Assert.Null(await Scalar("SELECT CASE 5 WHEN 1 THEN 'one' WHEN 2 THEN 'two' END"));
	[Fact] public async Task Case_MultipleBranches() => Assert.Equal("medium", await Scalar("SELECT CASE WHEN 50 < 10 THEN 'low' WHEN 50 < 100 THEN 'medium' ELSE 'high' END"));

	// ---- BETWEEN ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#between
	[Fact] public async Task Between_True() => Assert.Equal("True", await Scalar("SELECT 5 BETWEEN 1 AND 10"));
	[Fact] public async Task Between_False() => Assert.Equal("False", await Scalar("SELECT 15 BETWEEN 1 AND 10"));
	[Fact] public async Task Between_Inclusive() => Assert.Equal("True", await Scalar("SELECT 10 BETWEEN 1 AND 10"));
	[Fact] public async Task Between_Strings() => Assert.Equal("True", await Scalar("SELECT 'b' BETWEEN 'a' AND 'c'"));
	[Fact] public async Task NotBetween() => Assert.Equal("True", await Scalar("SELECT 15 NOT BETWEEN 1 AND 10"));

	// ---- IN ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#in_operators
	[Fact] public async Task In_True() => Assert.Equal("True", await Scalar("SELECT 2 IN (1, 2, 3)"));
	[Fact] public async Task In_False() => Assert.Equal("False", await Scalar("SELECT 5 IN (1, 2, 3)"));
	[Fact] public async Task In_Strings() => Assert.Equal("True", await Scalar("SELECT 'b' IN ('a', 'b', 'c')"));
	[Fact] public async Task NotIn() => Assert.Equal("True", await Scalar("SELECT 5 NOT IN (1, 2, 3)"));

	// ---- LIKE ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#like_operator
	[Fact] public async Task Like_Percent() => Assert.Equal("True", await Scalar("SELECT 'hello world' LIKE 'hello%'"));
	[Fact] public async Task Like_Underscore() => Assert.Equal("True", await Scalar("SELECT 'cat' LIKE 'c_t'"));
	[Fact] public async Task Like_Exact() => Assert.Equal("True", await Scalar("SELECT 'abc' LIKE 'abc'"));
	[Fact] public async Task Like_NoMatch() => Assert.Equal("False", await Scalar("SELECT 'abc' LIKE 'xyz'"));
	[Fact] public async Task NotLike() => Assert.Equal("True", await Scalar("SELECT 'abc' NOT LIKE 'xyz'"));

	// ---- IS NULL / IS NOT NULL ----
	[Fact] public async Task IsNull_True() => Assert.Equal("True", await Scalar("SELECT CAST(NULL AS INT64) IS NULL"));
	[Fact] public async Task IsNull_False() => Assert.Equal("False", await Scalar("SELECT 42 IS NULL"));
	[Fact] public async Task IsNotNull_True() => Assert.Equal("True", await Scalar("SELECT 42 IS NOT NULL"));
	[Fact] public async Task IsNotNull_False() => Assert.Equal("False", await Scalar("SELECT CAST(NULL AS INT64) IS NOT NULL"));

	// ---- Null coalescing / IFNULL / COALESCE ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions#coalesce
	[Fact] public async Task Coalesce_First() => Assert.Equal("1", await Scalar("SELECT COALESCE(1, 2, 3)"));
	[Fact] public async Task Coalesce_SkipsNull() => Assert.Equal("2", await Scalar("SELECT COALESCE(NULL, 2, 3)"));
	[Fact] public async Task Coalesce_AllNull() => Assert.Null(await Scalar("SELECT COALESCE(CAST(NULL AS INT64), CAST(NULL AS INT64))"));
	[Fact] public async Task Ifnull_NonNull() => Assert.Equal("1", await Scalar("SELECT IFNULL(1, 99)"));
	[Fact] public async Task Ifnull_Null() => Assert.Equal("99", await Scalar("SELECT IFNULL(CAST(NULL AS INT64), 99)"));
	[Fact] public async Task Nullif_Equal() => Assert.Null(await Scalar("SELECT NULLIF(1, 1)"));
	[Fact] public async Task Nullif_NotEqual() => Assert.Equal("1", await Scalar("SELECT NULLIF(1, 2)"));

	// ---- Arithmetic operators ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#arithmetic_operators
	[Fact] public async Task Arith_Add() => Assert.Equal("7", await Scalar("SELECT 3 + 4"));
	[Fact] public async Task Arith_Sub() => Assert.Equal("6", await Scalar("SELECT 10 - 4"));
	[Fact] public async Task Arith_Mul() => Assert.Equal("12", await Scalar("SELECT 3 * 4"));
	[Fact] public async Task Arith_Div() => Assert.Equal("2.5", await Scalar("SELECT 5.0 / 2"));
	[Fact] public async Task Arith_UnaryMinus() => Assert.Equal("-5", await Scalar("SELECT -5"));
	[Fact] public async Task Arith_Modulo() => Assert.Equal("1", await Scalar("SELECT MOD(10, 3)"));
	[Fact] public async Task Arith_IntegerDiv() => Assert.Equal("3", await Scalar("SELECT DIV(10, 3)"));
}
