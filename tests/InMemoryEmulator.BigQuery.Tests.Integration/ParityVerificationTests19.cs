using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 19: SUBSTR edge cases, NULLS FIRST/LAST ordering,
/// ARRAY operations, STRUCT access patterns, window frames, QUALIFY,
/// complex expression evaluation.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests19 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests19(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv19_{Guid.NewGuid():N}"[..28];
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
	// SUBSTR edge cases
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#substr
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Substr_FromEnd()
	{
		// SUBSTR('hello', -3) → 'llo' (last 3 chars)
		var result = await S("SELECT SUBSTR('hello', -3)");
		Assert.Equal("llo", result);
	}

	[Fact] public async Task Substr_PosZero_TreatedAsOne()
	{
		// Position 0 is treated as position 1
		var result = await S("SELECT SUBSTR('hello', 0, 3)");
		Assert.Equal("hel", result);
	}

	[Fact] public async Task Substr_StartBeyondLength()
	{
		var result = await S("SELECT SUBSTR('hello', 10)");
		Assert.Equal("", result);
	}

	[Fact] public async Task Substr_LengthZero()
	{
		var result = await S("SELECT SUBSTR('hello', 1, 0)");
		Assert.Equal("", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// ORDER BY NULLS FIRST / NULLS LAST
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#order_by_clause
	//   "NULLS FIRST: NULL values are returned before non-NULL values."
	//   "NULLS LAST: NULL values are returned after non-NULL values."
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task OrderBy_NullsFirst()
	{
		var rows = await Q(@"
			SELECT x FROM UNNEST([3, CAST(NULL AS INT64), 1, 2]) AS x
			ORDER BY x NULLS FIRST");
		Assert.Null(rows[0][0]);
		Assert.Equal("1", rows[1][0]?.ToString());
	}

	[Fact] public async Task OrderBy_NullsLast()
	{
		var rows = await Q(@"
			SELECT x FROM UNNEST([3, CAST(NULL AS INT64), 1, 2]) AS x
			ORDER BY x NULLS LAST");
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("2", rows[1][0]?.ToString());
		Assert.Equal("3", rows[2][0]?.ToString());
		Assert.Null(rows[3][0]);
	}

	[Fact] public async Task OrderBy_DescNullsFirst()
	{
		var rows = await Q(@"
			SELECT x FROM UNNEST([3, CAST(NULL AS INT64), 1, 2]) AS x
			ORDER BY x DESC NULLS FIRST");
		Assert.Null(rows[0][0]);
		Assert.Equal("3", rows[1][0]?.ToString());
	}

	[Fact] public async Task OrderBy_DescNullsLast()
	{
		var rows = await Q(@"
			SELECT x FROM UNNEST([3, CAST(NULL AS INT64), 1, 2]) AS x
			ORDER BY x DESC NULLS LAST");
		Assert.Equal("3", rows[0][0]?.ToString());
		Assert.Equal("2", rows[1][0]?.ToString());
		Assert.Equal("1", rows[2][0]?.ToString());
		Assert.Null(rows[3][0]);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// ARRAY operations
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ArrayLength_Normal()
	{
		var result = await S("SELECT ARRAY_LENGTH([1, 2, 3])");
		Assert.Equal("3", result);
	}

	[Fact] public async Task ArrayLength_Empty()
	{
		var result = await S("SELECT ARRAY_LENGTH([])");
		Assert.Equal("0", result);
	}

	[Fact] public async Task ArrayReverse_Normal()
	{
		var result = await S("SELECT ARRAY_TO_STRING(ARRAY(SELECT CAST(x AS STRING) FROM UNNEST(ARRAY_REVERSE([1, 2, 3])) AS x), ',')");
		Assert.Equal("3,2,1", result);
	}

	// Go emulator rejects ARRAY_TO_STRING with non-STRING arrays.
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_to_string
	//   "The value for array_expression can either be an array of STRING or BYTES data type."
	//   BigQuery implicitly coerces INT64 to STRING, but the Go emulator does not.
	[Fact]
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	public async Task ArrayConcat_TwoArrays()
	{
		var result = await S("SELECT ARRAY_TO_STRING(ARRAY(SELECT CAST(x AS STRING) FROM UNNEST(ARRAY_CONCAT([1, 2], [3, 4])) AS x), ',')");
		Assert.Equal("1,2,3,4", result);
	}

	// Go emulator rejects ARRAY_TO_STRING with non-STRING arrays.
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_to_string
	//   "The value for array_expression can either be an array of STRING or BYTES data type."
	//   BigQuery implicitly coerces INT64 to STRING, but the Go emulator does not.
	[Fact]
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	public async Task ArrayConcat_ThreeArrays()
	{
		var result = await S("SELECT ARRAY_TO_STRING(ARRAY(SELECT CAST(x AS STRING) FROM UNNEST(ARRAY_CONCAT([1], [2], [3, 4])) AS x), ',')");
		Assert.Equal("1,2,3,4", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// STRUCT access and construction
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#constructing_a_struct
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Struct_NamedField_Access()
	{
		var result = await S("SELECT s.name FROM (SELECT STRUCT('Alice' AS name, 30 AS age) AS s)");
		Assert.Equal("Alice", result);
	}

	[Fact] public async Task Struct_NamedField_NumericAccess()
	{
		var result = await S("SELECT s.age FROM (SELECT STRUCT('Alice' AS name, 30 AS age) AS s)");
		Assert.Equal("30", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Window function: QUALIFY
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#qualify_clause
	// ───────────────────────────────────────────────────────────────────────────

	// GO emulator does not correctly support QUALIFY clause.
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#qualify_clause
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Qualify_RowNumber()
	{
		var rows = await Q(@"
			SELECT x FROM UNNEST([10, 20, 30, 40, 50]) AS x
			QUALIFY ROW_NUMBER() OVER (ORDER BY x DESC) <= 3
			ORDER BY x DESC");
		Assert.Equal(3, rows.Count);
		Assert.Equal("50", rows[0][0]?.ToString());
		Assert.Equal("40", rows[1][0]?.ToString());
		Assert.Equal("30", rows[2][0]?.ToString());
	}

	// GO emulator does not correctly support QUALIFY clause.
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#qualify_clause
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Qualify_DenseRank()
	{
		var rows = await Q(@"
			SELECT val FROM UNNEST([1, 1, 2, 3, 3]) AS val
			QUALIFY DENSE_RANK() OVER (ORDER BY val) = 1");
		Assert.Equal(2, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Complex expressions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Ternary_IfExpression()
	{
		var result = await S("SELECT IF(5 > 3, 'yes', 'no')");
		Assert.Equal("yes", result);
	}

	[Fact] public async Task Ternary_IfNull()
	{
		var result = await S("SELECT IF(NULL, 'yes', 'no')");
		Assert.Equal("no", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// CASE with NULL comparisons
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Case_WithNull()
	{
		// NULL = NULL is NOT true in CASE WHEN, so falls through
		var result = await S("SELECT CASE NULL WHEN NULL THEN 'match' ELSE 'no match' END");
		Assert.Equal("no match", result);
	}

	[Fact] public async Task Case_SearchedWithIsNull()
	{
		var result = await S("SELECT CASE WHEN NULL IS NULL THEN 'null' ELSE 'not null' END");
		Assert.Equal("null", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Division and modulo
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task IntegerDivision_Truncates()
	{
		// BigQuery integer DIV truncates toward zero
		var result = await S("SELECT DIV(7, 2)");
		Assert.Equal("3", result);
	}

	[Fact] public async Task IntegerDivision_NegativeTruncates()
	{
		var result = await S("SELECT DIV(-7, 2)");
		Assert.Equal("-3", result);
	}

	[Fact] public async Task Mod_Positive()
	{
		var result = await S("SELECT MOD(7, 3)");
		Assert.Equal("1", result);
	}

	[Fact] public async Task Mod_Negative()
	{
		// MOD result takes sign of dividend in BigQuery
		var result = await S("SELECT MOD(-7, 3)");
		Assert.Equal("-1", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// String functions: INSTR, BYTE_LENGTH, CHAR_LENGTH
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
	// ───────────────────────────────────────────────────────────────────────────

	// Go emulator errors: "invalid position number" when position arg omitted.
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#instr
	//   "If position is specified, the search starts at this position in value, otherwise it starts at 1."
	[Fact]
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	public async Task Instr_Found()
	{
		var result = await S("SELECT INSTR('hello world', 'world')");
		Assert.Equal("7", result);
	}

	// Go emulator errors: "invalid position number" when position arg omitted.
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#instr
	//   "If position is specified, the search starts at this position in value, otherwise it starts at 1."
	[Fact]
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	public async Task Instr_NotFound()
	{
		var result = await S("SELECT INSTR('hello', 'xyz')");
		Assert.Equal("0", result);
	}

	[Fact] public async Task CharLength_Unicode()
	{
		var result = await S("SELECT CHAR_LENGTH('héllo')");
		Assert.Equal("5", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// GREATEST / LEAST with NULLs
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#greatest
	//   "Returns NULL if any input is NULL."
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Greatest_WithNull()
	{
		var result = await S("SELECT GREATEST(1, NULL, 3)");
		Assert.Null(result);
	}

	[Fact] public async Task Least_WithNull()
	{
		var result = await S("SELECT LEAST(1, NULL, 3)");
		Assert.Null(result);
	}

	[Fact] public async Task Greatest_Normal()
	{
		var result = await S("SELECT GREATEST(1, 5, 3)");
		Assert.Equal("5", result);
	}

	[Fact] public async Task Least_Normal()
	{
		var result = await S("SELECT LEAST(1, 5, 3)");
		Assert.Equal("1", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// CONCAT with various types
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#concat
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Concat_MultipleStrings()
	{
		var result = await S("SELECT CONCAT('a', 'b', 'c', 'd')");
		Assert.Equal("abcd", result);
	}

	[Fact] public async Task Concat_WithNull()
	{
		// CONCAT returns NULL if any argument is NULL
		var result = await S("SELECT CONCAT('a', NULL, 'c')");
		Assert.Null(result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// SAFE_CAST edge cases
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#safe_casting
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task SafeCast_InvalidNumber()
	{
		var result = await S("SELECT SAFE_CAST('abc' AS INT64)");
		Assert.Null(result);
	}

	[Fact] public async Task SafeCast_InvalidDate()
	{
		var result = await S("SELECT SAFE_CAST('not-a-date' AS DATE)");
		Assert.Null(result);
	}

	[Fact] public async Task SafeCast_ValidConversion()
	{
		var result = await S("SELECT SAFE_CAST('123' AS INT64)");
		Assert.Equal("123", result);
	}
}
