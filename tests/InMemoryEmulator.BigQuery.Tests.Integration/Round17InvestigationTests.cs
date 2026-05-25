using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Research round 17: NaN comparison, CAST formatting, OFFSET/ORDINAL edge cases, ANY_VALUE with all NULLs.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class Round17InvestigationTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public Round17InvestigationTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> S(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	private async Task<List<BigQueryRow>> Q(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		return result.ToList();
	}

	// ===== NaN comparisons =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#ieee_divide
	//   NaN follows IEEE 754 semantics: NaN is not equal to anything, including itself.
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#comparison_operators
	//   "All comparisons with NaN return FALSE, except for != which returns TRUE"

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task NaN_Equals_NaN_ReturnsFalse()
	{
		// In BigQuery: SELECT IEEE_DIVIDE(0,0) = IEEE_DIVIDE(0,0) → false
		var v = await S("SELECT IEEE_DIVIDE(0, 0) = IEEE_DIVIDE(0, 0)");
		Assert.Equal("False", v);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task NaN_NotEquals_NaN_ReturnsTrue()
	{
		// In BigQuery: SELECT IEEE_DIVIDE(0,0) != IEEE_DIVIDE(0,0) → true
		var v = await S("SELECT IEEE_DIVIDE(0, 0) != IEEE_DIVIDE(0, 0)");
		Assert.Equal("True", v);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task NaN_LessThan_ReturnsNotTrue()
	{
		// In BigQuery: SELECT IEEE_DIVIDE(0,0) < 1 → false
		var v = await S("SELECT IEEE_DIVIDE(0, 0) < 1");
		Assert.Equal("False", v);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task NaN_GreaterThan_ReturnsNotTrue()
	{
		// In BigQuery: SELECT IEEE_DIVIDE(0,0) > 1 → false
		var v = await S("SELECT IEEE_DIVIDE(0, 0) > 1");
		Assert.Equal("False", v);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task NaN_IsNotNull()
	{
		// NaN is not NULL
		var v = await S("SELECT IEEE_DIVIDE(0, 0) IS NULL");
		Assert.Equal("False", v);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task NaN_LessThanOrEqual_ReturnsFalse()
	{
		var v = await S("SELECT IEEE_DIVIDE(0, 0) <= 1");
		Assert.Equal("False", v);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task NaN_GreaterThanOrEqual_ReturnsFalse()
	{
		var v = await S("SELECT IEEE_DIVIDE(0, 0) >= 1");
		Assert.Equal("False", v);
	}

	// ===== CAST FLOAT64 to STRING with NaN =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_string
	//   "Returns an approximate string representation. A returned NaN or 0 will not be signed."
	//   BigQuery returns "nan" for CAST(NaN AS STRING), "inf" for positive infinity.

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Cast_NaN_ToString_Casing()
	{
		var v = await S("SELECT CAST(IEEE_DIVIDE(0, 0) AS STRING)");
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_string
		//   BigQuery returns "nan" (lowercase) for CAST(NaN AS STRING).
		Assert.Equal("nan", v);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Cast_Inf_ToString()
	{
		var v = await S("SELECT CAST(IEEE_DIVIDE(1, 0) AS STRING)");
		Assert.Equal("inf", v);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Cast_NegInf_ToString()
	{
		var v = await S("SELECT CAST(IEEE_DIVIDE(-1, 0) AS STRING)");
		Assert.Equal("-inf", v);
	}

	// ===== OFFSET / ORDINAL =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#array_subscript_operator
	//   "[OFFSET(n)]" returns 0-based element
	//   "[ORDINAL(n)]" returns 1-based element

	[Fact]
	public async Task Offset_ZeroBased()
	{
		var v = await S("SELECT [10, 20, 30][OFFSET(0)]");
		Assert.Equal("10", v);
	}

	[Fact]
	public async Task Ordinal_OneBased()
	{
		var v = await S("SELECT [10, 20, 30][ORDINAL(1)]");
		Assert.Equal("10", v);
	}

	[Fact]
	public async Task SafeOffset_OutOfBounds_ReturnsNull()
	{
		var v = await S("SELECT [10, 20, 30][SAFE_OFFSET(10)]");
		Assert.Null(v);
	}

	[Fact]
	public async Task SafeOrdinal_OutOfBounds_ReturnsNull()
	{
		var v = await S("SELECT [10, 20, 30][SAFE_ORDINAL(10)]");
		Assert.Null(v);
	}

	// ===== ANY_VALUE with all NULLs =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#any_value
	//   "Returns NULL when the input contains zero rows, or when expr evaluates to NULL for all rows."

	[Fact]
	public async Task AnyValue_AllNulls_ReturnsNull()
	{
		var v = await S("SELECT ANY_VALUE(x) FROM UNNEST([CAST(NULL AS INT64), NULL, NULL]) AS x");
		Assert.Null(v);
	}

	[Fact]
	public async Task AnyValue_EmptyInput_ReturnsNull()
	{
		var v = await S("SELECT ANY_VALUE(x) FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x");
		Assert.Null(v);
	}

	// ===== Infinity comparisons =====
	// GO emulator bug: cannot parse inf/nan string literals to FLOAT64.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Infinity_GreaterThan_LargeNumber()
	{
		var v = await S("SELECT CAST('inf' AS FLOAT64) > 1e308");
		Assert.Equal("True", v);
	}

	// GO emulator bug: cannot parse inf/nan string literals to FLOAT64.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task NegInfinity_LessThan_LargeNegNumber()
	{
		var v = await S("SELECT CAST('-inf' AS FLOAT64) < -1e308");
		Assert.Equal("True", v);
	}

	// ===== CAST formatting =====
	[Fact]
	public async Task Cast_Int_As_String()
	{
		// CAST(1 AS STRING) → '1'
		var v = await S("SELECT CAST(1 AS STRING)");
		Assert.Equal("1", v);
	}

	[Fact]
	public async Task Cast_Float_OnePointZero_As_String()
	{
		// CAST(1.0 AS STRING) → '1' (BigQuery omits ".0" for whole-number floats)
		var v = await S("SELECT CAST(1.0 AS STRING)");
		Assert.Equal("1", v);
	}

	[Fact]
	public async Task Cast_Float_LargeScientific_As_String()
	{
		// In BigQuery: CAST(1e10 AS STRING) → '10000000000' (whole-number floats omit ".0")
		var v = await S("SELECT CAST(1e10 AS STRING)");
		Assert.Equal("10000000000", v);
	}

	// ===== NaN in BETWEEN =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#floating_point_type
	//   "All comparisons with NaN return FALSE, except for != which returns TRUE."
	[Fact]
	public async Task NaN_Between_ReturnsFalse()
	{
		var v = await S("SELECT IEEE_DIVIDE(0, 0) BETWEEN -1 AND 1");
		Assert.Equal("False", v);
	}

	[Fact]
	public async Task Value_Between_NaN_And_Something_ReturnsFalse()
	{
		var v = await S("SELECT 0 BETWEEN IEEE_DIVIDE(0, 0) AND 1");
		Assert.Equal("False", v);
	}

	// ===== NaN in IN =====
	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task NaN_In_List_ReturnsFalse()
	{
		var v = await S("SELECT IEEE_DIVIDE(0, 0) IN (1, 2, 3)");
		Assert.Equal("False", v);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task NaN_In_NaN_ReturnsFalse()
	{
		// Even NaN IN (NaN) should be false because NaN != NaN
		var v = await S("SELECT IEEE_DIVIDE(0, 0) IN (IEEE_DIVIDE(0, 0))");
		Assert.Equal("False", v);
	}

	// ===== HAVING without GROUP BY =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#having_clause
	//   HAVING without GROUP BY treats entire table as single group
	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Having_Without_GroupBy()
	{
		var v = await S("SELECT SUM(x) FROM UNNEST([1,2,3]) AS x HAVING SUM(x) > 5");
		Assert.Equal("6", v);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Having_Without_GroupBy_NoMatch()
	{
		var rows = await Q("SELECT SUM(x) FROM UNNEST([1,2,3]) AS x HAVING SUM(x) > 100");
		Assert.Empty(rows);
	}

	// ===== CAST negative zero =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_string
	//   "A returned NaN or 0 will not be signed."
	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Cast_NegativeZero_As_String()
	{
		var v = await S("SELECT CAST(-0.0 AS STRING)");
		Assert.Equal("0", v);
	}

	// ===== CAST large float =====
	[Fact]
	public async Task Cast_LargeFloat_As_String()
	{
		// 1e20 exceeds long.MaxValue, BigQuery uses scientific notation with lowercase 'e'
		var v = await S("SELECT CAST(1e20 AS STRING)");
		Assert.Equal("1e+20", v);
	}

	// ===== IS DISTINCT FROM with NaN =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#is_distinct_from
	//   "IS DISTINCT FROM is similar to != but treats NaN values as equal"
	[Fact]
	public async Task NaN_IsNotDistinctFrom_NaN()
	{
		var v = await S("SELECT IEEE_DIVIDE(0, 0) IS NOT DISTINCT FROM IEEE_DIVIDE(0, 0)");
		Assert.Equal("True", v);
	}

	[Fact]
	public async Task NaN_IsDistinctFrom_One()
	{
		var v = await S("SELECT IEEE_DIVIDE(0, 0) IS DISTINCT FROM 1.0");
		Assert.Equal("True", v);
	}

	// ===== Simple CASE with NaN =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions#case_expr
	//   Simple CASE uses equality comparison. NaN != NaN so CASE NaN never matches.
	[Fact]
	public async Task Case_NaN_NoMatch()
	{
		var v = await S("SELECT CASE IEEE_DIVIDE(0, 0) WHEN IEEE_DIVIDE(0, 0) THEN 'match' ELSE 'no match' END");
		Assert.Equal("no match", v);
	}

	[Fact]
	public async Task Case_NaN_InWhenBranch_NoMatch()
	{
		var v = await S("SELECT CASE 1.0 WHEN IEEE_DIVIDE(0, 0) THEN 'nan' WHEN 1.0 THEN 'one' ELSE 'other' END");
		Assert.Equal("one", v);
	}

	// ===== IN subquery with NULL =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#in_operators
	//   "Returns NULL if search_value is NULL."
	[Fact]
	public async Task InSubquery_NullSearch_ReturnsNull()
	{
		var v = await S("SELECT CAST(NULL AS INT64) IN (SELECT 1)");
		Assert.Null(v);
	}

	// ===== NaN in WHERE clause =====
	[Fact]
	public async Task Where_NaN_Equality_FiltersCorrectly()
	{
		// NaN = NaN is false, so WHERE x = IEEE_DIVIDE(0,0) should not match NaN rows
		var rows = await Q("SELECT x FROM UNNEST([1.0, IEEE_DIVIDE(0,0), 3.0]) AS x WHERE x = IEEE_DIVIDE(0, 0)");
		Assert.Empty(rows);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Where_NaN_NotEquals_MatchesAll()
	{
		// NaN != x is always true for all x including NaN
		var rows = await Q("SELECT x FROM UNNEST([1.0, IEEE_DIVIDE(0,0), 3.0]) AS x WHERE x != 0");
		// 1.0 != 0 is true, NaN != 0 is true, 3.0 != 0 is true
		Assert.Equal(3, rows.Count);
	}
}
