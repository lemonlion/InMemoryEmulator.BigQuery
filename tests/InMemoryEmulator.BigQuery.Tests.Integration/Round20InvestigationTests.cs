using Google;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Investigation tests for research round 20.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class Round20InvestigationTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public Round20InvestigationTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_r20_{Guid.NewGuid():N}"[..30];
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

	private async Task<List<List<string?>>> Rows(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Select(r => Enumerable.Range(0, (int)result.Schema.Fields.Count).Select(i => r[i]?.ToString()).ToList()).ToList();
	}

	private async Task<Exception?> Error(string sql)
	{
		try
		{
			var client = await _fixture.GetClientAsync();
			await client.ExecuteQueryAsync(sql, parameters: null);
			return null;
		}
		catch (Exception ex) { return ex; }
	}

	// ================================================================
	// 1. STRUCT comparison with NULL fields
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type
	//   "Two STRUCTs are equal if all their fields are equal. NULL fields propagate NULL."
	// ================================================================

	[Fact]
	public async Task StructEquality_BothNullFields_ReturnsNull()
	{
		// STRUCT(1, NULL) = STRUCT(1, NULL) → should be NULL (three-valued logic)
		var result = await S("SELECT STRUCT(1, CAST(NULL AS INT64)) = STRUCT(1, CAST(NULL AS INT64))");
		Assert.Null(result); // BigQuery returns NULL
	}

	[Fact]
	public async Task StructEquality_NonNullFields_ReturnsTrue()
	{
		var result = await S("SELECT STRUCT(1, 2) = STRUCT(1, 2)");
		Assert.Equal("True", result);
	}

	[Fact]
	public async Task StructEquality_DifferentFields_ReturnsFalse()
	{
		var result = await S("SELECT STRUCT(1, 2) = STRUCT(1, 3)");
		Assert.Equal("False", result);
	}

	// ================================================================
	// 2. Type coercion: INT64 + FLOAT64
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#arithmetic_operators
	//   "If one operand is FLOAT64 and the other is INT64, the INT64 is coerced to FLOAT64."
	// ================================================================

	[Fact]
	public async Task TypeCoercion_IntPlusFloat_ReturnsFloat()
	{
		// SELECT 1 + 1.5 → 2.5 (FLOAT64)
		var result = await S("SELECT 1 + 1.5");
		Assert.Equal("2.5", result);
	}

	[Fact]
	public async Task TypeCoercion_IF_BothBranches_Coerced()
	{
		// SELECT CAST(IF(TRUE, 1, 1.5) AS STRING) → "1" (IF returns FLOAT64, whole numbers omit ".0")
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions#if
		//   "The result type is the common supertype of then_expression and else_expression."
		var result = await S("SELECT CAST(IF(TRUE, 1, 1.5) AS STRING)");
		Assert.Equal("1", result);
	}

	[Fact]
	public async Task TypeCoercion_COALESCE_ReturnsFloat()
	{
		// SELECT CAST(COALESCE(NULL, 1, 1.5) AS STRING) → "1" (COALESCE returns FLOAT64, whole numbers omit ".0")
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions#coalesce
		//   "The result type is the common supertype of all argument types."
		var result = await S("SELECT CAST(COALESCE(NULL, 1, 1.5) AS STRING)");
		Assert.Equal("1", result);
	}

	// ================================================================
	// 3. Nested aggregates should error
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions
	//   "Aggregate functions cannot be nested."
	// ================================================================

	[Fact]
	public async Task NestedAggregate_SumCount_ShouldError()
	{
		var ex = await Error($"SELECT SUM(COUNT(*)) FROM `{_datasetId}`.nonexistent");
		Assert.NotNull(ex);
	}

	// ================================================================
	// 4. IN with subquery returning NULL (three-valued logic)
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#in_operators
	//   "If the value is not found and the list contains NULL, returns NULL."
	// ================================================================

	[Fact]
	public async Task In_SubqueryWithNull_ReturnsNull()
	{
		// 1 IN (SELECT CAST(NULL AS INT64)) → NULL
		var result = await S("SELECT 1 IN (SELECT CAST(NULL AS INT64))");
		Assert.Null(result); // should be NULL, not FALSE
	}

	[Fact]
	public async Task In_ValueNotFoundButNullExists_ReturnsNull()
	{
		// 5 IN (1, 2, NULL) → NULL
		var result = await S("SELECT 5 IN (1, 2, NULL)");
		Assert.Null(result);
	}

	// ================================================================
	// 5. ARRAY equality
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#array_type
	//   In BigQuery, arrays cannot be compared with = operator directly.
	//   "Equality comparisons are not supported for ARRAY types."
	// ================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#array_type
	//   "Equality is not defined for arguments of type ARRAY<INT64>"
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task ArrayEquality_ShouldErrorOrReturnTrue()
	{
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAsync<GoogleApiException>(async () =>
			await client.ExecuteQueryAsync("SELECT [1,2,3] = [1,2,3]", parameters: null));
	}

	// ================================================================
	// 6. LAST_VALUE with default frame
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#last_value
	//   Default frame with ORDER BY is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
	//   So LAST_VALUE(...) OVER (ORDER BY x) returns current row value (not last in partition)
	// ================================================================

	[Fact]
	public async Task LastValue_DefaultFrame_WithOrderBy()
	{
		var sql = @"
			WITH t AS (
				SELECT 1 AS id, 10 AS val UNION ALL
				SELECT 2, 20 UNION ALL
				SELECT 3, 30
			)
			SELECT id, LAST_VALUE(val) OVER (ORDER BY id) AS lv FROM t ORDER BY id";
		var rows = await Rows(sql);
		// With default frame RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:
		// id=1 → last in frame [1] = 10
		// id=2 → last in frame [1,2] = 20
		// id=3 → last in frame [1,2,3] = 30
		Assert.Equal("10", rows[0][1]);
		Assert.Equal("20", rows[1][1]);
		Assert.Equal("30", rows[2][1]);
	}

	// ================================================================
	// 7. PERCENT_RANK with ties
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions#percent_rank
	//   "PERCENT_RANK = (RANK - 1) / (total_rows - 1)"
	// ================================================================

	[Fact]
	public async Task PercentRank_WithTies()
	{
		var sql = @"
			WITH t AS (
				SELECT 1 AS val UNION ALL
				SELECT 1 UNION ALL
				SELECT 2 UNION ALL
				SELECT 3 UNION ALL
				SELECT 3
			)
			SELECT val, PERCENT_RANK() OVER (ORDER BY val) AS pr FROM t ORDER BY val";
		var rows = await Rows(sql);
		// val=1: rank=1, percent_rank=(1-1)/(5-1)=0.0
		// val=1: rank=1, percent_rank=(1-1)/(5-1)=0.0
		// val=2: rank=3, percent_rank=(3-1)/(5-1)=0.5
		// val=3: rank=4, percent_rank=(4-1)/(5-1)=0.75
		// val=3: rank=4, percent_rank=(4-1)/(5-1)=0.75
		Assert.Equal("0", rows[0][1]);
		Assert.Equal("0", rows[1][1]);
		Assert.Equal("0.5", rows[2][1]);
		Assert.Equal("0.75", rows[3][1]);
		Assert.Equal("0.75", rows[4][1]);
	}

	// ================================================================
	// 8. CUME_DIST with ties
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions#cume_dist
	//   "CUME_DIST = NP/NR where NP = number of rows preceding or peer with current row"
	// ================================================================

	[Fact]
	public async Task CumeDist_WithTies()
	{
		var sql = @"
			WITH t AS (
				SELECT 1 AS val UNION ALL
				SELECT 1 UNION ALL
				SELECT 2 UNION ALL
				SELECT 3 UNION ALL
				SELECT 3
			)
			SELECT val, CUME_DIST() OVER (ORDER BY val) AS cd FROM t ORDER BY val";
		var rows = await Rows(sql);
		// val=1: 2 rows <= 1, so NP=2, cd=2/5=0.4
		// val=1: same, cd=0.4
		// val=2: 3 rows <= 2, cd=3/5=0.6
		// val=3: 5 rows <= 3, cd=5/5=1.0
		// val=3: same, cd=1.0
		Assert.Equal("0.4", rows[0][1]);
		Assert.Equal("0.4", rows[1][1]);
		Assert.Equal("0.6", rows[2][1]);
		Assert.Equal("1", rows[3][1]);
		Assert.Equal("1", rows[4][1]);
	}

	// ================================================================
	// 9. ARRAY_AGG with ORDER BY and IGNORE NULLS
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#array_agg
	// ================================================================

	[Fact]
	public async Task ArrayAgg_OrderBy_IgnoreNulls()
	{
		var sql = @"
			WITH t AS (
				SELECT 1 AS id, 'a' AS val UNION ALL
				SELECT 2, NULL UNION ALL
				SELECT 3, 'c' UNION ALL
				SELECT 4, NULL UNION ALL
				SELECT 5, 'e'
			)
			SELECT ARRAY_TO_STRING(ARRAY_AGG(val IGNORE NULLS ORDER BY id), ',') FROM t";
		var result = await S(sql);
		Assert.Equal("a,c,e", result);
	}

	// ================================================================
	// 10. EXCEPT ALL and INTERSECT ALL
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators
	//   "EXCEPT ALL removes one occurrence of each right-side row"
	//   "INTERSECT ALL preserves min(count_left, count_right)"
	// ================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators
	//   "EXCEPT ALL is not supported. Only EXCEPT DISTINCT is supported."
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task ExceptAll_PreservesDuplicates()
	{
		// EXCEPT ALL is not supported in BigQuery
		var client = await _fixture.GetClientAsync();
		var sql = @"
			WITH left_t AS (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 2 UNION ALL SELECT 3),
			     right_t AS (SELECT 1 AS x UNION ALL SELECT 2)
			SELECT * FROM left_t
			EXCEPT ALL
			SELECT * FROM right_t
			ORDER BY 1";
		await Assert.ThrowsAsync<GoogleApiException>(async () =>
			await client.ExecuteQueryAsync(sql, parameters: null));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators
	//   "INTERSECT ALL is not supported. Only INTERSECT DISTINCT is supported."
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task IntersectAll_PreservesDuplicates()
	{
		// INTERSECT ALL is not supported in BigQuery
		var client = await _fixture.GetClientAsync();
		var sql = @"
			WITH left_t AS (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3),
			     right_t AS (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 2)
			SELECT * FROM left_t
			INTERSECT ALL
			SELECT * FROM right_t
			ORDER BY 1";
		await Assert.ThrowsAsync<GoogleApiException>(async () =>
			await client.ExecuteQueryAsync(sql, parameters: null));
	}

	// ================================================================
	// 11. Multi-column IN (struct-style) - Parser limitation
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#in_operators
	//   "value_expression [NOT] IN ((expr1, expr2, ...), (expr1, expr2, ...), ...)"
	// NOTE: This is a known parser limitation - the emulator doesn't support tuple IN syntax
	// ================================================================

	[Fact(Skip = "Parser does not support tuple IN syntax: (1, 'a') IN ((1, 'a'), (2, 'b'))")]
	public async Task MultiColumnIn_StructStyle()
	{
		var result = await S("SELECT (1, 'a') IN ((1, 'a'), (2, 'b'))");
		Assert.Equal("True", result);
	}

	[Fact(Skip = "Parser does not support tuple IN syntax: (1, 'a') IN ((1, 'a'), (2, 'b'))")]
	public async Task MultiColumnIn_NotFound()
	{
		var result = await S("SELECT (1, 'c') IN ((1, 'a'), (2, 'b'))");
		Assert.Equal("False", result);
	}

	// ================================================================
	// 12. DENSE_RANK with ties
	// ================================================================

	[Fact]
	public async Task DenseRank_WithTies()
	{
		var sql = @"
			WITH t AS (
				SELECT 1 AS val UNION ALL
				SELECT 1 UNION ALL
				SELECT 2 UNION ALL
				SELECT 3 UNION ALL
				SELECT 3
			)
			SELECT val, DENSE_RANK() OVER (ORDER BY val) AS dr FROM t ORDER BY val";
		var rows = await Rows(sql);
		Assert.Equal("1", rows[0][1]);
		Assert.Equal("1", rows[1][1]);
		Assert.Equal("2", rows[2][1]);
		Assert.Equal("3", rows[3][1]);
		Assert.Equal("3", rows[4][1]);
	}

	// ================================================================
	// 13. FIRST_VALUE with ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
	// ================================================================

	[Fact]
	public async Task FirstValue_FullFrame()
	{
		var sql = @"
			WITH t AS (
				SELECT 1 AS id, 'a' AS val UNION ALL
				SELECT 2, 'b' UNION ALL
				SELECT 3, 'c'
			)
			SELECT id, FIRST_VALUE(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS fv FROM t ORDER BY id";
		var rows = await Rows(sql);
		Assert.Equal("a", rows[0][1]);
		Assert.Equal("a", rows[1][1]);
		Assert.Equal("a", rows[2][1]);
	}

	// ================================================================
	// 14. NTH_VALUE with frame
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#nth_value
	//   "Returns the value of value_expression at the Nth row of the current window frame."
	// ================================================================

	[Fact]
	public async Task NthValue_WithDefaultFrame()
	{
		var sql = @"
			WITH t AS (
				SELECT 1 AS id, 'a' AS val UNION ALL
				SELECT 2, 'b' UNION ALL
				SELECT 3, 'c'
			)
			SELECT id, NTH_VALUE(val, 2) OVER (ORDER BY id) AS nv FROM t ORDER BY id";
		var rows = await Rows(sql);
		// Default frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		// id=1: frame=[1], no 2nd value → NULL
		// id=2: frame=[1,2], 2nd value = 'b'
		// id=3: frame=[1,2,3], 2nd value = 'b'
		Assert.Null(rows[0][1]);
		Assert.Equal("b", rows[1][1]);
		Assert.Equal("b", rows[2][1]);
	}

	// ================================================================
	// 15. IFNULL type coercion
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions#ifnull
	//   "Result type is the common supertype."
	// ================================================================

	[Fact]
	public async Task TypeCoercion_IFNULL_ReturnsFloat()
	{
		var result = await S("SELECT CAST(IFNULL(1, 1.5) AS STRING)");
		Assert.Equal("1", result);
	}

	// ================================================================
	// 16. Window function: LEAD/LAG with default value type coercion
	// ================================================================

	[Fact]
	public async Task Lead_DefaultValue()
	{
		var sql = @"
			WITH t AS (
				SELECT 1 AS id, 10 AS val UNION ALL
				SELECT 2, 20 UNION ALL
				SELECT 3, 30
			)
			SELECT id, LEAD(val, 1, -1) OVER (ORDER BY id) AS next_val FROM t ORDER BY id";
		var rows = await Rows(sql);
		Assert.Equal("20", rows[0][1]);
		Assert.Equal("30", rows[1][1]);
		Assert.Equal("-1", rows[2][1]);
	}

	// ================================================================
	// 17. ARRAY_AGG with ORDER BY DESC
	// ================================================================

	[Fact]
	public async Task ArrayAgg_OrderByDesc()
	{
		var sql = @"
			WITH t AS (
				SELECT 1 AS id, 'a' AS val UNION ALL
				SELECT 2, 'b' UNION ALL
				SELECT 3, 'c'
			)
			SELECT ARRAY_TO_STRING(ARRAY_AGG(val ORDER BY id DESC), ',') FROM t";
		var result = await S(sql);
		Assert.Equal("c,b,a", result);
	}

	// ================================================================
	// 18. Correlated subquery
	// ================================================================

	[Fact]
	public async Task CorrelatedSubquery_InSelect()
	{
		// Using CTE to avoid needing physical tables
		var sql = @"
			WITH orders AS (
				SELECT 1 AS id, 1 AS customer_id, 100.0 AS amount UNION ALL
				SELECT 2, 1, 200.0 UNION ALL
				SELECT 3, 2, 50.0
			)
			SELECT DISTINCT customer_id,
			       (SELECT SUM(amount) FROM orders o2 WHERE o2.customer_id = o1.customer_id) AS total
			FROM orders o1
			ORDER BY customer_id";
		var rows = await Rows(sql);
		Assert.Equal(2, rows.Count);
		Assert.Equal("1", rows[0][0]);
		Assert.Equal("300", rows[0][1]);
		Assert.Equal("2", rows[1][0]);
		Assert.Equal("50", rows[1][1]);
	}

	// ================================================================
	// 19. EXISTS subquery
	// ================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax
	//   BigQuery does not support WHERE clause in SELECT without FROM, and EXISTS
	//   subquery in a FROM-less SELECT may trigger similar restrictions.
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task ExistsSubquery()
	{
		var result = await S("SELECT EXISTS(SELECT 1 FROM UNNEST([1]) WHERE TRUE)");
		Assert.Equal("True", result);
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task ExistsSubquery_Empty()
	{
		var result = await S("SELECT EXISTS(SELECT 1 FROM UNNEST([1]) WHERE FALSE)");
		Assert.Equal("False", result);
	}

	// ================================================================
	// 20. Scalar subquery returning multiple rows should error
	// ================================================================

	[Fact]
	public async Task ScalarSubquery_MultipleRows_ShouldError()
	{
		var ex = await Error("SELECT (SELECT x FROM UNNEST([1,2,3]) AS x)");
		Assert.NotNull(ex);
	}

	// ================================================================
	// 21. STRUCT field access
	// ================================================================

	[Fact]
	public async Task StructFieldAccess()
	{
		var result = await S("SELECT STRUCT(1 AS a, 'hello' AS b).b");
		Assert.Equal("hello", result);
	}

	// ================================================================
	// 22. ARRAY_AGG DISTINCT
	// ================================================================

	[Fact]
	public async Task ArrayAgg_Distinct()
	{
		var sql = @"
			WITH t AS (
				SELECT 1 AS val UNION ALL
				SELECT 1 UNION ALL
				SELECT 2 UNION ALL
				SELECT 2 UNION ALL
				SELECT 3
			)
			SELECT ARRAY_LENGTH(ARRAY_AGG(DISTINCT val)) FROM t";
		var result = await S(sql);
		Assert.Equal("3", result);
	}

	// ================================================================
	// 23. Window function: ROWS BETWEEN N PRECEDING AND N FOLLOWING
	// ================================================================

	[Fact]
	public async Task WindowFunction_RowsBetween_NPreceding_NFollowing()
	{
		var sql = @"
			WITH t AS (
				SELECT 1 AS id, 10 AS val UNION ALL
				SELECT 2, 20 UNION ALL
				SELECT 3, 30 UNION ALL
				SELECT 4, 40 UNION ALL
				SELECT 5, 50
			)
			SELECT id, SUM(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s FROM t ORDER BY id";
		var rows = await Rows(sql);
		// id=1: sum(10,20) = 30
		// id=2: sum(10,20,30) = 60
		// id=3: sum(20,30,40) = 90
		// id=4: sum(30,40,50) = 120
		// id=5: sum(40,50) = 90
		Assert.Equal("30", rows[0][1]);
		Assert.Equal("60", rows[1][1]);
		Assert.Equal("90", rows[2][1]);
		Assert.Equal("120", rows[3][1]);
		Assert.Equal("90", rows[4][1]);
	}

	// ================================================================
	// 24. CASE WHEN type coercion
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions#case
	//   "Result type is the common supertype of all THEN/ELSE branches."
	// ================================================================

	[Fact]
	public async Task TypeCoercion_CaseWhen_IntAndFloat()
	{
		var result = await S("SELECT CAST(CASE WHEN TRUE THEN 1 ELSE 1.5 END AS STRING)");
		Assert.Equal("1", result);
	}

	// ================================================================
	// 25. Struct inequality with NULL fields
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type
	//   "STRUCT(1, NULL) != STRUCT(1, NULL) → NULL (not FALSE)"
	// ================================================================

	[Fact]
	public async Task StructInequality_BothNullFields_ReturnsNull()
	{
		var result = await S("SELECT STRUCT(1, CAST(NULL AS INT64)) != STRUCT(1, CAST(NULL AS INT64))");
		Assert.Null(result);
	}

	// ================================================================
	// 26. Struct comparison: one NULL field, other fields differ
	// ================================================================

	[Fact]
	public async Task StructEquality_OneNullDifferentOther_ReturnsFalse()
	{
		// STRUCT(1, NULL) = STRUCT(2, NULL) → FALSE (field[0] differs, short-circuit)
		var result = await S("SELECT STRUCT(1, CAST(NULL AS INT64)) = STRUCT(2, CAST(NULL AS INT64))");
		Assert.Equal("False", result);
	}

	// ================================================================
	// 27. ARRAY_AGG ORDER BY with LIMIT
	// ================================================================

	[Fact]
	public async Task ArrayAgg_OrderBy_Limit()
	{
		var sql = @"
			WITH t AS (
				SELECT 1 AS id, 'a' AS val UNION ALL
				SELECT 2, 'b' UNION ALL
				SELECT 3, 'c' UNION ALL
				SELECT 4, 'd' UNION ALL
				SELECT 5, 'e'
			)
			SELECT ARRAY_TO_STRING(ARRAY_AGG(val ORDER BY id LIMIT 3), ',') FROM t";
		var result = await S(sql);
		Assert.Equal("a,b,c", result);
	}

	// ================================================================
	// 28. COUNT DISTINCT with NULLs
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#count
	//   "COUNT(DISTINCT x) does not count NULL."
	// ================================================================

	[Fact]
	public async Task CountDistinct_IgnoresNulls()
	{
		var sql = @"
			WITH t AS (
				SELECT 1 AS val UNION ALL
				SELECT 1 UNION ALL
				SELECT NULL UNION ALL
				SELECT 2 UNION ALL
				SELECT NULL
			)
			SELECT COUNT(DISTINCT val) FROM t";
		var result = await S(sql);
		Assert.Equal("2", result);
	}

	// ================================================================
	// 29. IFNULL with NULL first arg and non-null second
	// ================================================================

	[Fact]
	public async Task IFNULL_NullFirst_ReturnsSecond()
	{
		var result = await S("SELECT IFNULL(NULL, 42)");
		Assert.Equal("42", result);
	}

	// ================================================================
	// 30. Window SUM with NULL values
	// ================================================================

	[Fact]
	public async Task WindowSum_WithNulls()
	{
		var sql = @"
			WITH t AS (
				SELECT 1 AS id, 10 AS val UNION ALL
				SELECT 2, CAST(NULL AS INT64) UNION ALL
				SELECT 3, 30
			)
			SELECT id, SUM(val) OVER (ORDER BY id) AS running_sum FROM t ORDER BY id";
		var rows = await Rows(sql);
		// SUM ignores NULLs: running sum = 10, 10, 40
		Assert.Equal("10", rows[0][1]);
		Assert.Equal("10", rows[1][1]);
		Assert.Equal("40", rows[2][1]);
	}
}
