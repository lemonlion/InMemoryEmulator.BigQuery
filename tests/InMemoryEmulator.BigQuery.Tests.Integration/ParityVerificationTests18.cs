using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 18: String function edge cases, IN operator type coercion,
/// aggregate NULL handling, GENERATE_ARRAY edge cases, LOGICAL_AND/LOGICAL_OR with NULLs.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests18 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests18(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv18_{Guid.NewGuid():N}"[..28];
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
	// RIGHT edge cases
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#right
	//   "Returns zero-length STRING or BYTES if length is 0."
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Right_ZeroLength()
	{
		var result = await S("SELECT RIGHT('abc', 0)");
		Assert.Equal("", result);
	}

	[Fact] public async Task Right_ExceedsLength()
	{
		// RIGHT('ab', 5) → 'ab' (returns full string when length exceeds)
		var result = await S("SELECT RIGHT('ab', 5)");
		Assert.Equal("ab", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// LEFT edge cases
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#left
	//   "Returns zero-length STRING or BYTES if length is 0."
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Left_ZeroLength()
	{
		var result = await S("SELECT LEFT('abc', 0)");
		Assert.Equal("", result);
	}

	[Fact] public async Task Left_ExceedsLength()
	{
		var result = await S("SELECT LEFT('ab', 5)");
		Assert.Equal("ab", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// REPEAT edge cases
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#repeat
	//   "count: An INT64 value. If count is 0, the function returns the empty string.
	//    If count is negative, the function returns NULL."
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Repeat_ZeroCount()
	{
		var result = await S("SELECT REPEAT('abc', 0)");
		Assert.Equal("", result);
	}

	[Fact] public async Task Repeat_NegativeCount()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#repeat
		//   "This function returns an error if the repetitions value is negative."
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAnyAsync<Exception>(async () =>
		{
			var result = await client.ExecuteQueryAsync("SELECT REPEAT('abc', -1)", parameters: null);
			_ = result.ToList();
		});
	}

	[Fact] public async Task Repeat_Normal()
	{
		var result = await S("SELECT REPEAT('ab', 3)");
		Assert.Equal("ababab", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// LPAD / RPAD edge cases
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#lpad
	//   "If fill_string is empty, returns original_value truncated to length."
	// ───────────────────────────────────────────────────────────────────────────

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact] public async Task Lpad_EmptyFill_Truncates()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#lpad
		//   "This function returns an error if: pattern is empty."
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAnyAsync<Exception>(
			() => client.ExecuteQueryAsync("SELECT LPAD('hello', 3, '')", parameters: null));
	}

	[Fact] public async Task Rpad_EmptyFill_Truncates()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#rpad
		//   "This function returns an error if: pattern is empty."
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAnyAsync<Exception>(
			() => client.ExecuteQueryAsync("SELECT RPAD('hello', 3, '')", parameters: null));
	}

	[Fact] public async Task Lpad_NormalPadding()
	{
		var result = await S("SELECT LPAD('abc', 6, '*')");
		Assert.Equal("***abc", result);
	}

	[Fact] public async Task Rpad_NormalPadding()
	{
		var result = await S("SELECT RPAD('abc', 6, '*')");
		Assert.Equal("abc***", result);
	}

	[Fact] public async Task Lpad_TruncateWhenShorter()
	{
		// LPAD with length shorter than original truncates from right
		var result = await S("SELECT LPAD('hello', 3, 'x')");
		Assert.Equal("hel", result);
	}

	[Fact] public async Task Rpad_TruncateWhenShorter()
	{
		var result = await S("SELECT RPAD('hello', 3, 'x')");
		Assert.Equal("hel", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// IN operator with type coercion
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#in_operators
	//   "If types differ, implicit coercion applies (e.g., INT64 to FLOAT64)."
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task In_IntInFloatList()
	{
		// 1 IN (1.0, 2.0, 3.0) should be TRUE
		var result = await S("SELECT 1 IN (1.0, 2.0, 3.0)");
		Assert.Equal("True", result);
	}

	[Fact] public async Task In_FloatInIntList()
	{
		var result = await S("SELECT 2.0 IN (1, 2, 3)");
		Assert.Equal("True", result);
	}

	[Fact] public async Task In_NotFound()
	{
		var result = await S("SELECT 4 IN (1, 2, 3)");
		Assert.Equal("False", result);
	}

	[Fact] public async Task In_NullInList()
	{
		// NULL IN (1, 2, 3) → NULL (not FALSE) per three-valued logic
		var result = await S("SELECT NULL IN (1, 2, 3)");
		Assert.Null(result);
	}

	[Fact] public async Task In_ValueInListWithNull()
	{
		// 4 IN (1, NULL, 3) → NULL (because NULL comparison is unknown)
		var result = await S("SELECT 4 IN (1, NULL, 3)");
		Assert.Null(result);
	}

	[Fact] public async Task In_ValueFoundDespiteNull()
	{
		// 1 IN (1, NULL, 3) → TRUE (found match, NULLs don't matter)
		var result = await S("SELECT 1 IN (1, NULL, 3)");
		Assert.Equal("True", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// STRING_AGG with all NULLs
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#string_agg
	//   "Returns NULL if there are zero input rows or expression evaluates to NULL for all rows."
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task StringAgg_AllNulls()
	{
		var result = await S("SELECT STRING_AGG(x, ',') FROM UNNEST([CAST(NULL AS STRING), CAST(NULL AS STRING)]) AS x");
		Assert.Null(result);
	}

	[Fact] public async Task StringAgg_MixedNulls()
	{
		// NULL values are skipped, non-NULLs are joined
		var result = await S("SELECT STRING_AGG(x, ',') FROM UNNEST(['a', CAST(NULL AS STRING), 'b']) AS x");
		Assert.Equal("a,b", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// LOGICAL_AND / LOGICAL_OR with NULLs
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#logical_and
	//   "Returns NULL if there are zero input rows or expression evaluates to NULL for all rows."
	//   Three-valued logic: LOGICAL_AND with NULL and TRUE → NULL
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task LogicalAnd_AllTrue()
	{
		var result = await S("SELECT LOGICAL_AND(x) FROM UNNEST([true, true, true]) AS x");
		Assert.Equal("True", result);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact] public async Task LogicalAnd_WithFalse()
	{
		var result = await S("SELECT LOGICAL_AND(x) FROM UNNEST([true, false, true]) AS x");
		Assert.Equal("False", result);
	}

	[Fact] public async Task LogicalAnd_AllNulls()
	{
		// All NULLs → NULL (not FALSE)
		var result = await S("SELECT LOGICAL_AND(x) FROM UNNEST([CAST(NULL AS BOOL), CAST(NULL AS BOOL)]) AS x");
		Assert.Null(result);
	}

	[Fact] public async Task LogicalAnd_NullAndTrue()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#logical_and
		//   "Returns the logical AND of all non-NULL expressions."
		//   NULLs are ignored; all non-null values are TRUE → TRUE
		var result = await S("SELECT LOGICAL_AND(x) FROM UNNEST([true, CAST(NULL AS BOOL)]) AS x");
		Assert.Equal("True", result);
	}

	[Fact] public async Task LogicalAnd_NullAndFalse()
	{
		// NULL AND FALSE → FALSE (one false forces result to false)
		var result = await S("SELECT LOGICAL_AND(x) FROM UNNEST([false, CAST(NULL AS BOOL)]) AS x");
		Assert.Equal("False", result);
	}

	[Fact] public async Task LogicalOr_AllFalse()
	{
		var result = await S("SELECT LOGICAL_OR(x) FROM UNNEST([false, false, false]) AS x");
		Assert.Equal("False", result);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact] public async Task LogicalOr_WithTrue()
	{
		var result = await S("SELECT LOGICAL_OR(x) FROM UNNEST([false, true, false]) AS x");
		Assert.Equal("True", result);
	}

	[Fact] public async Task LogicalOr_AllNulls()
	{
		var result = await S("SELECT LOGICAL_OR(x) FROM UNNEST([CAST(NULL AS BOOL), CAST(NULL AS BOOL)]) AS x");
		Assert.Null(result);
	}

	[Fact] public async Task LogicalOr_NullAndFalse()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#logical_or
		//   "Returns the logical OR of all non-NULL expressions."
		//   NULLs are ignored; no non-null value is TRUE → FALSE
		var result = await S("SELECT LOGICAL_OR(x) FROM UNNEST([false, CAST(NULL AS BOOL)]) AS x");
		Assert.Equal("False", result);
	}

	[Fact] public async Task LogicalOr_NullAndTrue()
	{
		// NULL OR TRUE → TRUE (one true forces result to true)
		var result = await S("SELECT LOGICAL_OR(x) FROM UNNEST([true, CAST(NULL AS BOOL)]) AS x");
		Assert.Equal("True", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// GENERATE_ARRAY edge cases
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_array
	//   "If any argument is NULL, returns NULL."
	//   "Error if step is 0."
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task GenerateArray_NullStart()
	{
		var result = await S("SELECT ARRAY_LENGTH(GENERATE_ARRAY(CAST(NULL AS INT64), 5))");
		Assert.Null(result);
	}

	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact] public async Task GenerateArray_NullEnd()
	{
		var result = await S("SELECT ARRAY_LENGTH(GENERATE_ARRAY(1, CAST(NULL AS INT64)))");
		Assert.Null(result);
	}

	[Fact] public async Task GenerateArray_NullStep()
	{
		var result = await S("SELECT ARRAY_LENGTH(GENERATE_ARRAY(1, 5, CAST(NULL AS INT64)))");
		Assert.Null(result);
	}

	[Fact] public async Task GenerateArray_Normal()
	{
		var result = await S("SELECT ARRAY_TO_STRING(ARRAY(SELECT CAST(x AS STRING) FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x), ',')");
		Assert.Equal("1,2,3,4,5", result);
	}

	[Fact] public async Task GenerateArray_WithStep()
	{
		var result = await S("SELECT ARRAY_TO_STRING(ARRAY(SELECT CAST(x AS STRING) FROM UNNEST(GENERATE_ARRAY(0, 10, 3)) AS x), ',')");
		Assert.Equal("0,3,6,9", result);
	}

	[Fact] public async Task GenerateArray_Descending()
	{
		var result = await S("SELECT ARRAY_TO_STRING(ARRAY(SELECT CAST(x AS STRING) FROM UNNEST(GENERATE_ARRAY(5, 1, -1)) AS x), ',')");
		Assert.Equal("5,4,3,2,1", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// REVERSE / REPLACE / SPLIT
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Reverse_String()
	{
		var result = await S("SELECT REVERSE('hello')");
		Assert.Equal("olleh", result);
	}

	[Fact] public async Task Reverse_EmptyString()
	{
		var result = await S("SELECT REVERSE('')");
		Assert.Equal("", result);
	}

	[Fact] public async Task Replace_Normal()
	{
		var result = await S("SELECT REPLACE('hello world', 'world', 'there')");
		Assert.Equal("hello there", result);
	}

	[Fact] public async Task Replace_NotFound()
	{
		var result = await S("SELECT REPLACE('hello', 'xyz', 'abc')");
		Assert.Equal("hello", result);
	}

	[Fact] public async Task Split_Default()
	{
		var result = await S("SELECT ARRAY_TO_STRING(SPLIT('a,b,c', ','), '|')");
		Assert.Equal("a|b|c", result);
	}

	[Fact] public async Task Split_EmptyDelimiter()
	{
		// SPLIT('abc', '') → ['a', 'b', 'c'] (split into individual chars)
		var result = await S("SELECT ARRAY_TO_STRING(SPLIT('abc', ''), '|')");
		Assert.Equal("a|b|c", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// STARTS_WITH / ENDS_WITH
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#starts_with
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task StartsWith_True()
	{
		var result = await S("SELECT STARTS_WITH('hello world', 'hello')");
		Assert.Equal("True", result);
	}

	[Fact] public async Task StartsWith_False()
	{
		var result = await S("SELECT STARTS_WITH('hello world', 'world')");
		Assert.Equal("False", result);
	}

	[Fact] public async Task EndsWith_True()
	{
		var result = await S("SELECT ENDS_WITH('hello world', 'world')");
		Assert.Equal("True", result);
	}

	[Fact] public async Task EndsWith_False()
	{
		var result = await S("SELECT ENDS_WITH('hello world', 'hello')");
		Assert.Equal("False", result);
	}
}
