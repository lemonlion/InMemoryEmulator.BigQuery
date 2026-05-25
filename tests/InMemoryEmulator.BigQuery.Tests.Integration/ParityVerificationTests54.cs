using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 54: INITCAP NULL delimiters, TRANSLATE duplicate source chars,
/// ARRAY_TO_STRING NULL delimiter.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests54 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public ParityVerificationTests54(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
	}

	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string> ScalarAsync(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var row = result.First();
		return row[0]?.ToString() ?? "NULL";
	}

	// === Bug 1: INITCAP with NULL delimiters should return NULL ===
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#initcap
	//   "If value or delimiters is NULL, the function returns NULL."

	[Fact]
	public async Task Initcap_NullDelimiters_ReturnsNull()
	{
		var result = await ScalarAsync("SELECT INITCAP('hello world', CAST(NULL AS STRING))");
		Assert.Equal("NULL", result);
	}

	[Fact]
	public async Task Initcap_NullValue_ReturnsNull()
	{
		var result = await ScalarAsync("SELECT INITCAP(CAST(NULL AS STRING))");
		Assert.Equal("NULL", result);
	}

	[Fact]
	public async Task Initcap_CustomDelimiters_Works()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#initcap
		//   Custom delimiters override the default set.
		var result = await ScalarAsync("SELECT INITCAP('apples1oranges2pears', '12')");
		Assert.Equal("Apples1Oranges2Pears", result);
	}

	// === Bug 2: TRANSLATE should error on duplicate source characters ===
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#translate
	//   "A duplicate character in source_characters results in an error."

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Translate_DuplicateSourceChars_ThrowsError()
	{
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAsync<Google.GoogleApiException>(
			() => client.ExecuteQueryAsync("SELECT TRANSLATE('abc', 'aab', 'xyz')", parameters: null));
	}

	[Fact]
	public async Task Translate_NormalCase_Works()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#translate
		//   "In expression, replaces each character in source_characters with the
		//    corresponding character in target_characters."
		var result = await ScalarAsync("SELECT TRANSLATE('This is a cookie', 'sco', 'zku')");
		Assert.Equal("Thiz iz a kuukie", result);
	}

	[Fact]
	public async Task Translate_SourceLongerThanTarget_OmitsChars()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#translate
		//   "A character in source_characters without a corresponding character in
		//    target_characters is omitted from the result."
		var result = await ScalarAsync("SELECT TRANSLATE('abcdef', 'abcde', 'xy')");
		Assert.Equal("xyf", result);
	}

	[Fact]
	public async Task Translate_NullArgs_ReturnsNull()
	{
		var result = await ScalarAsync("SELECT TRANSLATE(CAST(NULL AS STRING), 'abc', 'xyz')");
		Assert.Equal("NULL", result);
	}

	// === Bug 3: ARRAY_TO_STRING with NULL delimiter should return NULL ===
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-reference
	//   General rule: "If an operand is NULL, the function result is NULL."

	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task ArrayToString_NullDelimiter_ReturnsNull()
	{
		var result = await ScalarAsync("SELECT ARRAY_TO_STRING(['a', 'b', 'c'], CAST(NULL AS STRING))");
		Assert.Equal("NULL", result);
	}

	[Fact]
	public async Task ArrayToString_NormalCase_Works()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_to_string
		var result = await ScalarAsync("SELECT ARRAY_TO_STRING(['a', 'b', 'c'], '-')");
		Assert.Equal("a-b-c", result);
	}

	[Fact]
	public async Task ArrayToString_WithNullText_ReplacesNulls()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_to_string
		//   "If null_text is specified, the function replaces any NULL values in
		//    the array with null_text."
		var result = await ScalarAsync("SELECT ARRAY_TO_STRING(['a', NULL, 'c'], '-', 'N')");
		Assert.Equal("a-N-c", result);
	}

	[Fact]
	public async Task ArrayToString_WithoutNullText_OmitsNulls()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_to_string
		//   "If null_text is not specified, NULL values are omitted."
		var result = await ScalarAsync("SELECT ARRAY_TO_STRING(['a', NULL, 'c'], '-')");
		Assert.Equal("a-c", result);
	}
}
