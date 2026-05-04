using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Parity verification tests batch 6: Window functions, FLOAT formatting,
/// STRUCT operations, NULL handling, array operations, string functions,
/// QUALIFY clause, GROUP BY ROLLUP.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests6 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests6(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv6_{Guid.NewGuid():N}"[..28];
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

	private async Task<List<string?>> Col(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null);
		return result.ToList().Select(r => r[0]?.ToString()).ToList();
	}

	// ===== FLOAT64 formatting =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast
	// "FLOAT64 → STRING: Returns an approximate string representation. A returned NaN or 0 will not be signed."

	[Fact]
	public async Task Float_WholeNumber_Format()
	{
		// BigQuery: SELECT CAST(1.0 AS STRING) returns "1.0"
		var result = await S("SELECT CAST(1.0 AS STRING)");
		Assert.Equal("1.0", result);
	}

	[Fact]
	public async Task Float_Zero_Format()
	{
		// BigQuery: CAST(0.0 AS STRING) returns "0.0"
		var result = await S("SELECT CAST(CAST(0 AS FLOAT64) AS STRING)");
		Assert.Equal("0.0", result);
	}

	[Fact]
	public async Task Float_NegativeWholeNumber_Format()
	{
		var result = await S("SELECT CAST(-5.0 AS STRING)");
		Assert.Equal("-5.0", result);
	}

	// ===== IFNULL / NULLIF / COALESCE =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions

	[Fact]
	public async Task Ifnull_NonNull()
	{
		var result = await S("SELECT IFNULL(10, 20)");
		Assert.Equal("10", result);
	}

	[Fact]
	public async Task Ifnull_Null()
	{
		var result = await S("SELECT IFNULL(NULL, 20)");
		Assert.Equal("20", result);
	}

	[Fact]
	public async Task Nullif_Equal()
	{
		var result = await S("SELECT NULLIF(10, 10)");
		Assert.Null(result);
	}

	[Fact]
	public async Task Nullif_NotEqual()
	{
		var result = await S("SELECT NULLIF(10, 20)");
		Assert.Equal("10", result);
	}

	[Fact]
	public async Task Coalesce_FirstNonNull()
	{
		var result = await S("SELECT COALESCE(NULL, NULL, 3, 4)");
		Assert.Equal("3", result);
	}

	[Fact]
	public async Task Coalesce_AllNull()
	{
		var result = await S("SELECT COALESCE(NULL, NULL, NULL)");
		Assert.Null(result);
	}

	// ===== GREATEST / LEAST =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#greatest

	[Fact]
	public async Task Greatest_Integers()
	{
		var result = await S("SELECT GREATEST(1, 5, 3, 2)");
		Assert.Equal("5", result);
	}

	[Fact]
	public async Task Greatest_WithNull()
	{
		// Ref: BigQuery docs: "If any argument is NULL, returns NULL"
		var result = await S("SELECT GREATEST(1, NULL, 3)");
		Assert.Null(result);
	}

	[Fact]
	public async Task Least_Integers()
	{
		var result = await S("SELECT LEAST(5, 1, 3, 2)");
		Assert.Equal("1", result);
	}

	[Fact]
	public async Task Least_WithNull()
	{
		// Ref: BigQuery docs: "If any argument is NULL, returns NULL"
		var result = await S("SELECT LEAST(5, NULL, 2)");
		Assert.Null(result);
	}

	// ===== SAFE_DIVIDE =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#safe_divide

	[Fact]
	public async Task SafeDivide_Normal()
	{
		// Ref: SAFE_DIVIDE returns FLOAT64; SDK parses to double, whole numbers display without .0
		var result = await S("SELECT SAFE_DIVIDE(10, 2)");
		Assert.Equal("5", result);
	}

	[Fact]
	public async Task SafeDivide_ByZero_ReturnsNull()
	{
		var result = await S("SELECT SAFE_DIVIDE(10, 0)");
		Assert.Null(result);
	}

	// ===== MOD =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#mod

	[Fact]
	public async Task Mod_Positive()
	{
		var result = await S("SELECT MOD(10, 3)");
		Assert.Equal("1", result);
	}

	[Fact]
	public async Task Mod_Negative()
	{
		// BigQuery: MOD(-10, 3) = -1 (sign of dividend)
		var result = await S("SELECT MOD(-10, 3)");
		Assert.Equal("-1", result);
	}

	// ===== TRUNC / CEIL / FLOOR / ROUND =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions

	[Fact]
	public async Task Trunc_Positive()
	{
		var result = await S("SELECT TRUNC(2.7)");
		Assert.Equal("2", result);
	}

	[Fact]
	public async Task Trunc_Negative()
	{
		var result = await S("SELECT TRUNC(-2.7)");
		Assert.Equal("-2", result);
	}

	[Fact]
	public async Task Ceil_Positive()
	{
		var result = await S("SELECT CEIL(2.3)");
		Assert.Equal("3", result);
	}

	[Fact]
	public async Task Floor_Positive()
	{
		var result = await S("SELECT FLOOR(2.7)");
		Assert.Equal("2", result);
	}

	[Fact]
	public async Task Round_Default()
	{
		// Ref: ROUND(x) rounds to nearest, halfway away from zero
		var result = await S("SELECT ROUND(2.5)");
		Assert.Equal("3", result);
	}

	[Fact]
	public async Task Round_WithDigits()
	{
		var result = await S("SELECT ROUND(2.345, 2)");
		Assert.Equal("2.35", result);
	}

	// ===== Window functions: LAG / LEAD =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions

	[Fact]
	public async Task Lag_Basic()
	{
		var result = await Col(@"
			SELECT LAG(x) OVER (ORDER BY x) 
			FROM UNNEST([10, 20, 30]) AS x");
		Assert.Equal(new[] { null, "10", "20" }, result.ToArray());
	}

	[Fact]
	public async Task Lead_Basic()
	{
		var result = await Col(@"
			SELECT LEAD(x) OVER (ORDER BY x) 
			FROM UNNEST([10, 20, 30]) AS x");
		Assert.Equal(new[] { "20", "30", null }, result.ToArray());
	}

	[Fact]
	public async Task Lag_WithOffset()
	{
		var result = await Col(@"
			SELECT LAG(x, 2) OVER (ORDER BY x) 
			FROM UNNEST([10, 20, 30, 40]) AS x");
		Assert.Equal(new[] { null, null, "10", "20" }, result.ToArray());
	}

	[Fact]
	public async Task Lead_WithDefault()
	{
		var result = await Col(@"
			SELECT LEAD(x, 1, -1) OVER (ORDER BY x) 
			FROM UNNEST([10, 20, 30]) AS x");
		Assert.Equal(new[] { "20", "30", "-1" }, result.ToArray());
	}

	// ===== NTILE =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions#ntile

	[Fact]
	public async Task Ntile_Basic()
	{
		var result = await Col(@"
			SELECT NTILE(3) OVER (ORDER BY x)
			FROM UNNEST([1, 2, 3, 4, 5, 6]) AS x");
		Assert.Equal(new[] { "1", "1", "2", "2", "3", "3" }, result.ToArray());
	}

	// ===== FIRST_VALUE / LAST_VALUE =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#first_value

	[Fact]
	public async Task FirstValue_Basic()
	{
		var result = await Col(@"
			SELECT FIRST_VALUE(x) OVER (ORDER BY x)
			FROM UNNEST([30, 10, 20]) AS x");
		// All rows see first value = 10 (sorted ascending)
		Assert.Equal(new[] { "10", "10", "10" }, result.ToArray());
	}

	[Fact]
	public async Task LastValue_WithUnboundedFrame()
	{
		var result = await Col(@"
			SELECT LAST_VALUE(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
			FROM UNNEST([30, 10, 20]) AS x");
		Assert.Equal(new[] { "30", "30", "30" }, result.ToArray());
	}

	// ===== STRING_AGG with ORDER BY =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#string_agg

	[Fact]
	public async Task StringAgg_WithOrderBy()
	{
		var result = await S(@"
			SELECT STRING_AGG(x, ',' ORDER BY x)
			FROM UNNEST(['banana', 'apple', 'cherry']) AS x");
		Assert.Equal("apple,banana,cherry", result);
	}

	[Fact]
	public async Task StringAgg_WithOrderByDesc()
	{
		var result = await S(@"
			SELECT STRING_AGG(x, '-' ORDER BY x DESC)
			FROM UNNEST(['a', 'b', 'c']) AS x");
		Assert.Equal("c-b-a", result);
	}

	// ===== ARRAY_AGG with ORDER BY =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#array_agg

	[Fact]
	public async Task ArrayAgg_WithOrderBy()
	{
		var result = await S(@"
			SELECT ARRAY_TO_STRING(ARRAY_AGG(x ORDER BY x), ',')
			FROM UNNEST([3, 1, 2]) AS x");
		Assert.Equal("1,2,3", result);
	}

	[Fact]
	public async Task ArrayAgg_WithOrderByDesc()
	{
		var result = await S(@"
			SELECT ARRAY_TO_STRING(ARRAY_AGG(x ORDER BY x DESC), ',')
			FROM UNNEST([3, 1, 2]) AS x");
		Assert.Equal("3,2,1", result);
	}

	// ===== SPLIT =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#split

	[Fact]
	public async Task Split_Basic()
	{
		var result = await S("SELECT ARRAY_TO_STRING(SPLIT('a,b,c', ','), '|')");
		Assert.Equal("a|b|c", result);
	}

	[Fact]
	public async Task Split_EmptyDelimiter()
	{
		// BigQuery: SPLIT with empty delimiter splits each character
		var result = await S("SELECT ARRAY_LENGTH(SPLIT('abc', ''))");
		Assert.Equal("3", result);
	}

	// ===== STARTS_WITH / ENDS_WITH =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions

	[Fact]
	public async Task StartsWith_True()
	{
		var result = await S("SELECT STARTS_WITH('hello world', 'hello')");
		Assert.Equal("True", result);
	}

	[Fact]
	public async Task StartsWith_False()
	{
		var result = await S("SELECT STARTS_WITH('hello world', 'world')");
		Assert.Equal("False", result);
	}

	[Fact]
	public async Task EndsWith_True()
	{
		var result = await S("SELECT ENDS_WITH('hello world', 'world')");
		Assert.Equal("True", result);
	}

	// ===== LPAD / RPAD =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#lpad

	[Fact]
	public async Task Lpad_Basic()
	{
		var result = await S("SELECT LPAD('abc', 6, '0')");
		Assert.Equal("000abc", result);
	}

	[Fact]
	public async Task Rpad_Basic()
	{
		var result = await S("SELECT RPAD('abc', 6, '0')");
		Assert.Equal("abc000", result);
	}

	[Fact]
	public async Task Lpad_TruncateWhenShorter()
	{
		// Ref: If target_length < LENGTH(original), LPAD truncates
		var result = await S("SELECT LPAD('abcdef', 3, '0')");
		Assert.Equal("abc", result);
	}

	// ===== REPEAT =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#repeat

	[Fact]
	public async Task Repeat_Basic()
	{
		var result = await S("SELECT REPEAT('ab', 3)");
		Assert.Equal("ababab", result);
	}

	[Fact]
	public async Task Repeat_Zero()
	{
		var result = await S("SELECT REPEAT('ab', 0)");
		Assert.Equal("", result);
	}

	// ===== REGEXP_EXTRACT / REGEXP_REPLACE =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_extract

	[Fact]
	public async Task RegexpExtract_Basic()
	{
		var result = await S("SELECT REGEXP_EXTRACT('foo bar123 baz', r'(\\d+)')");
		Assert.Equal("123", result);
	}

	[Fact]
	public async Task RegexpReplace_Basic()
	{
		var result = await S("SELECT REGEXP_REPLACE('hello 123 world 456', r'\\d+', 'X')");
		Assert.Equal("hello X world X", result);
	}

	// ===== STRUCT creation and access =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type

	[Fact]
	public async Task Struct_FieldAccess()
	{
		var result = await S("SELECT (STRUCT(1 AS a, 'hello' AS b)).a");
		Assert.Equal("1", result);
	}

	[Fact]
	public async Task Struct_FieldAccess_String()
	{
		var result = await S("SELECT (STRUCT(1 AS a, 'hello' AS b)).b");
		Assert.Equal("hello", result);
	}

	// ===== GROUP BY ROLLUP =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#group_by_rollup

	[Fact]
	public async Task GroupBy_Rollup()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($"CREATE TABLE `{_ds}.rollup_t` (category STRING, value INT64)", parameters: null);
		await client.ExecuteQueryAsync($"INSERT INTO `{_ds}.rollup_t` (category, value) VALUES ('A', 10), ('A', 20), ('B', 30)", parameters: null);

		var result = await client.ExecuteQueryAsync(
			$"SELECT category, SUM(value) as total FROM `{_ds}.rollup_t` GROUP BY ROLLUP(category) ORDER BY category",
			parameters: null);
		var rows = result.ToList();

		// ROLLUP produces: (NULL, 60 grand total), (A, 30), (B, 30)
		Assert.Equal(3, rows.Count);
		// NULL row is the grand total
		Assert.Null(rows[0]["category"]);
		Assert.Equal("60", rows[0]["total"]?.ToString());
		Assert.Equal("A", rows[1]["category"]?.ToString());
		Assert.Equal("30", rows[1]["total"]?.ToString());
		Assert.Equal("B", rows[2]["category"]?.ToString());
		Assert.Equal("30", rows[2]["total"]?.ToString());
	}

	// ===== QUALIFY clause =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#qualify_clause

	[Fact]
	public async Task Qualify_RowNumber()
	{
		var result = await Col(@"
			SELECT x FROM UNNEST([5, 3, 8, 1, 7]) AS x
			QUALIFY ROW_NUMBER() OVER (ORDER BY x) <= 3
			ORDER BY x");
		Assert.Equal(new[] { "1", "3", "5" }, result.ToArray());
	}

	// ===== DIV and MOD edge cases =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#div

	[Fact]
	public async Task Div_NegativeDividend()
	{
		// DIV truncates toward zero
		var result = await S("SELECT DIV(-7, 2)");
		Assert.Equal("-3", result);
	}

	[Fact]
	public async Task Div_NegativeDivisor()
	{
		var result = await S("SELECT DIV(7, -2)");
		Assert.Equal("-3", result);
	}

	// ===== IEEE_DIVIDE =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#ieee_divide

	[Fact]
	public async Task IeeeDivide_Normal()
	{
		var result = await S("SELECT IEEE_DIVIDE(10, 4)");
		Assert.Equal("2.5", result);
	}

	[Fact]
	public async Task IeeeDivide_ByZero_ReturnsInfinity()
	{
		var result = await S("SELECT IEEE_DIVIDE(1, 0)");
		Assert.Equal(double.PositiveInfinity.ToString(), result);
	}

	[Fact]
	public async Task IeeeDivide_ZeroByZero_ReturnsNaN()
	{
		var result = await S("SELECT IEEE_DIVIDE(0, 0)");
		Assert.Equal(double.NaN.ToString(), result);
	}

	// ===== ARRAY_LENGTH =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_length

	[Fact]
	public async Task ArrayLength_Basic()
	{
		var result = await S("SELECT ARRAY_LENGTH([1, 2, 3, 4])");
		Assert.Equal("4", result);
	}

	[Fact]
	public async Task ArrayLength_Empty()
	{
		var result = await S("SELECT ARRAY_LENGTH([])");
		Assert.Equal("0", result);
	}

	// ===== ARRAY_REVERSE =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_reverse

	[Fact]
	public async Task ArrayReverse_Basic()
	{
		var result = await S("SELECT ARRAY_TO_STRING(ARRAY_REVERSE([1, 2, 3]), ',')");
		Assert.Equal("3,2,1", result);
	}

	// ===== CONCAT with NULL =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#concat
	// "If any argument is NULL, the function returns NULL."

	[Fact]
	public async Task Concat_WithNull_ReturnsNull()
	{
		var result = await S("SELECT CONCAT('hello', NULL, 'world')");
		Assert.Null(result);
	}

	[Fact]
	public async Task Concat_Normal()
	{
		var result = await S("SELECT CONCAT('hello', ' ', 'world')");
		Assert.Equal("hello world", result);
	}

	// ===== SAFE functions =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-reference#safe_prefix

	[Fact]
	public async Task Safe_Log_NegativeReturnsNull()
	{
		var result = await S("SELECT SAFE.LOG(10, -1)");
		Assert.Null(result);
	}

	[Fact]
	public async Task Safe_Sqrt_NegativeReturnsNull()
	{
		var result = await S("SELECT SAFE.SQRT(-1)");
		Assert.Null(result);
	}

	// ===== IF expression =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions#if

	[Fact]
	public async Task If_True()
	{
		var result = await S("SELECT IF(1 > 0, 'yes', 'no')");
		Assert.Equal("yes", result);
	}

	[Fact]
	public async Task If_False()
	{
		var result = await S("SELECT IF(1 > 2, 'yes', 'no')");
		Assert.Equal("no", result);
	}

	[Fact]
	public async Task If_NullCondition()
	{
		// NULL condition evaluates to false branch
		var result = await S("SELECT IF(NULL, 'yes', 'no')");
		Assert.Equal("no", result);
	}

	// ===== GENERATE_DATE_ARRAY =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_date_array

	[Fact]
	public async Task GenerateDateArray_Basic()
	{
		var result = await S(@"
			SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY('2024-01-01', '2024-01-05'))");
		Assert.Equal("5", result);
	}

	// ===== CASE WHEN with multiple conditions =====
	[Fact]
	public async Task Case_MultipleWhen()
	{
		var result = await S("SELECT CASE WHEN 1 > 2 THEN 'a' WHEN 2 > 1 THEN 'b' ELSE 'c' END");
		Assert.Equal("b", result);
	}

	[Fact]
	public async Task Case_NoMatch_Else()
	{
		var result = await S("SELECT CASE WHEN 1 > 2 THEN 'a' WHEN 3 > 4 THEN 'b' ELSE 'c' END");
		Assert.Equal("c", result);
	}

	[Fact]
	public async Task Case_NoMatch_NoElse_Null()
	{
		var result = await S("SELECT CASE WHEN 1 > 2 THEN 'a' END");
		Assert.Null(result);
	}

	// ===== IN with subquery =====
	[Fact]
	public async Task In_Subquery()
	{
		var result = await S("SELECT 2 IN (SELECT x FROM UNNEST([1, 2, 3]) AS x)");
		Assert.Equal("True", result);
	}

	[Fact]
	public async Task NotIn_Subquery()
	{
		var result = await S("SELECT 5 IN (SELECT x FROM UNNEST([1, 2, 3]) AS x)");
		Assert.Equal("False", result);
	}

	// ===== EXISTS =====
	[Fact]
	public async Task Exists_True()
	{
		var result = await S("SELECT EXISTS(SELECT 1 FROM UNNEST([1,2,3]) AS x WHERE x > 2)");
		Assert.Equal("True", result);
	}

	[Fact]
	public async Task Exists_False()
	{
		var result = await S("SELECT EXISTS(SELECT 1 FROM UNNEST([1,2,3]) AS x WHERE x > 5)");
		Assert.Equal("False", result);
	}

	// ===== BETWEEN =====
	[Fact]
	public async Task Between_True()
	{
		var result = await S("SELECT 5 BETWEEN 1 AND 10");
		Assert.Equal("True", result);
	}

	[Fact]
	public async Task Between_False()
	{
		var result = await S("SELECT 15 BETWEEN 1 AND 10");
		Assert.Equal("False", result);
	}

	// ===== LIKE =====
	[Fact]
	public async Task Like_Percent()
	{
		var result = await S("SELECT 'hello world' LIKE '%world'");
		Assert.Equal("True", result);
	}

	[Fact]
	public async Task Like_Underscore()
	{
		var result = await S("SELECT 'abc' LIKE 'a_c'");
		Assert.Equal("True", result);
	}

	// ===== Arithmetic with FLOAT64 output =====
	// Ref: BigQuery / operator always returns FLOAT64

	[Fact]
	public async Task Division_IntByInt_ReturnsFloat()
	{
		var result = await S("SELECT 10 / 4");
		Assert.Equal("2.5", result);
	}

	[Fact]
	public async Task Division_IntByInt_NonExact()
	{
		var result = await S("SELECT 1 / 3");
		Assert.Equal("0.3333333333333333", result);
	}
}
