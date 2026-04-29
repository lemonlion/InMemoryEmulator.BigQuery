using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for REGEXP_CONTAINS, REGEXP_EXTRACT, REGEXP_REPLACE, REGEXP_EXTRACT_ALL.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_contains
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class RegexpFunctionDeepTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public RegexpFunctionDeepTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- REGEXP_CONTAINS ----
	[Fact] public async Task RegexpContains_Simple() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('hello world', 'hello')"));
	[Fact] public async Task RegexpContains_NoMatch() => Assert.Equal("False", await Scalar("SELECT REGEXP_CONTAINS('hello world', 'xyz')"));
	[Fact] public async Task RegexpContains_Dot() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('hello', 'h.llo')"));
	[Fact] public async Task RegexpContains_Star() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('hello', 'hel*o')"));
	[Fact] public async Task RegexpContains_Caret() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('hello', '^hel')"));
	[Fact] public async Task RegexpContains_Dollar() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('hello', 'llo$')"));
	[Fact] public async Task RegexpContains_Plus() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('123', '[0-9]+')"));
	[Fact] public async Task RegexpContains_CharClass() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('abc', '[a-c]+')"));
	[Fact] public async Task RegexpContains_NoCharClass() => Assert.Equal("False", await Scalar("SELECT REGEXP_CONTAINS('xyz', '^[a-c]+$')"));
	[Fact] public async Task RegexpContains_WordBoundary() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('the cat sat', 'cat')"));
	[Fact] public async Task RegexpContains_Email() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('test@example.com', '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+')"));
	[Fact] public async Task RegexpContains_Digits() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('abc123', '[0-9]')"));
	[Fact] public async Task RegexpContains_NoDigits() => Assert.Equal("False", await Scalar("SELECT REGEXP_CONTAINS('abcdef', '[0-9]')"));
	[Fact] public async Task RegexpContains_Optional() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('color', 'colou?r')"));
	[Fact] public async Task RegexpContains_Alternate() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('cat', 'cat|dog')"));
	[Fact] public async Task RegexpContains_AlternateFalse() => Assert.Equal("False", await Scalar("SELECT REGEXP_CONTAINS('fish', 'cat|dog')"));
	[Fact] public async Task RegexpContains_Exact() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('abc', '^abc$')"));
	[Fact] public async Task RegexpContains_ExactFalse() => Assert.Equal("False", await Scalar("SELECT REGEXP_CONTAINS('abcd', '^abc$')"));
	[Fact] public async Task RegexpContains_Empty() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('hello', '')"));
	[Fact] public async Task RegexpContains_AllDigits() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('12345', '^[0-9]+$')"));
	[Fact] public async Task RegexpContains_NotAllDigits() => Assert.Equal("False", await Scalar("SELECT REGEXP_CONTAINS('123a5', '^[0-9]+$')"));

	// ---- REGEXP_EXTRACT ----
	[Fact] public async Task RegexpExtract_Basic() => Assert.Equal("123", await Scalar("SELECT REGEXP_EXTRACT('abc123def', '[0-9]+')"));
	[Fact] public async Task RegexpExtract_First() => Assert.Equal("abc", await Scalar("SELECT REGEXP_EXTRACT('abc123', '[a-z]+')"));
	[Fact] public async Task RegexpExtract_NoMatch() => Assert.Null(await Scalar("SELECT REGEXP_EXTRACT('hello', '[0-9]+')"));
	[Fact] public async Task RegexpExtract_Group() => Assert.Equal("123", await Scalar("SELECT REGEXP_EXTRACT('abc123def', '([0-9]+)')"));
	[Fact] public async Task RegexpExtract_Word() => Assert.Equal("hello", await Scalar("SELECT REGEXP_EXTRACT('hello world', '[a-z]+')"));
	[Fact] public async Task RegexpExtract_Email() => Assert.Equal("test", await Scalar("SELECT REGEXP_EXTRACT('test@example.com', '([a-zA-Z]+)@')"));
	[Fact] public async Task RegexpExtract_Dot() => Assert.Equal("hel", await Scalar("SELECT REGEXP_EXTRACT('hello', 'h.l')"));
	[Fact] public async Task RegexpExtract_Start() => Assert.Equal("abc", await Scalar("SELECT REGEXP_EXTRACT('abcdef', '^[a-c]+')"));

	// ---- REGEXP_REPLACE ----
	[Fact] public async Task RegexpReplace_Basic() => Assert.Equal("abc___def", await Scalar("SELECT REGEXP_REPLACE('abc123def', '[0-9]+', '___')"));
	[Fact] public async Task RegexpReplace_Remove() => Assert.Equal("abcdef", await Scalar("SELECT REGEXP_REPLACE('abc123def', '[0-9]+', '')"));
	[Fact] public async Task RegexpReplace_Multi() => Assert.Equal("X23X56", await Scalar("SELECT REGEXP_REPLACE('123456', '[14]', 'X')"));
	[Fact] public async Task RegexpReplace_NoMatch() => Assert.Equal("hello", await Scalar("SELECT REGEXP_REPLACE('hello', '[0-9]+', 'X')"));
	[Fact] public async Task RegexpReplace_Spaces() => Assert.Equal("hello-world", await Scalar("SELECT REGEXP_REPLACE('hello world', ' ', '-')"));
	[Fact] public async Task RegexpReplace_AllDigits() => Assert.Equal("***", await Scalar("SELECT REGEXP_REPLACE('123', '[0-9]', '*')"));
	[Fact] public async Task RegexpReplace_AllAlpha() => Assert.Equal("***", await Scalar("SELECT REGEXP_REPLACE('abc', '[a-z]', '*')"));
	[Fact] public async Task RegexpReplace_Empty() => Assert.Equal("hello", await Scalar("SELECT REGEXP_REPLACE('hello', 'xyz', 'X')"));

	// ---- REGEXP_EXTRACT_ALL ----
	[Fact(Skip = "Regex backslash escape in integration path")] public async Task RegexpExtractAll_Digits() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(REGEXP_EXTRACT_ALL('a1b2c3', '[0-9]'))"));
	[Fact] public async Task RegexpExtractAll_Words() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(REGEXP_EXTRACT_ALL('one two three', '[a-z]+'))"));
	[Fact] public async Task RegexpExtractAll_NoMatch() => Assert.Equal("0", await Scalar("SELECT ARRAY_LENGTH(REGEXP_EXTRACT_ALL('hello', '[0-9]+'))"));
	[Fact] public async Task RegexpExtractAll_All() => Assert.Equal("5", await Scalar("SELECT ARRAY_LENGTH(REGEXP_EXTRACT_ALL('hello', '.'))"));
	[Fact] public async Task RegexpExtractAll_Groups() => Assert.Equal("2", await Scalar("SELECT ARRAY_LENGTH(REGEXP_EXTRACT_ALL('abc123def456', '([0-9]+)'))"));

	// ---- REGEXP patterns with special characters ----
	[Fact] public async Task Regexp_SpaceSpecial() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('hello world', 'hello world')"));
	[Fact] public async Task Regexp_RepeatN() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('aaa', 'a{3}')"));
	[Fact] public async Task Regexp_RepeatRange() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('aaa', 'a{2,4}')"));
	[Fact] public async Task Regexp_RepeatMin() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('aaaa', 'a{2,}')"));
	[Fact] public async Task Regexp_RepeatNFalse() => Assert.Equal("False", await Scalar("SELECT REGEXP_CONTAINS('aa', 'a{3}')"));
	[Fact] public async Task Regexp_Lazy() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('aab', 'a+?b')"));
	[Fact] public async Task Regexp_Greedy() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('aab', 'a+b')"));

	// ---- LIKE (not regex, but pattern matching) ----
	[Fact] public async Task Like_Percent() => Assert.Equal("True", await Scalar("SELECT 'hello world' LIKE '%world'"));
	[Fact] public async Task Like_StartPercent() => Assert.Equal("True", await Scalar("SELECT 'hello world' LIKE 'hello%'"));
	[Fact] public async Task Like_BothPercent() => Assert.Equal("True", await Scalar("SELECT 'hello world' LIKE '%llo wor%'"));
	[Fact] public async Task Like_Underscore() => Assert.Equal("True", await Scalar("SELECT 'abc' LIKE 'a_c'"));
	[Fact] public async Task Like_UnderscoreFalse() => Assert.Equal("False", await Scalar("SELECT 'abbc' LIKE 'a_c'"));
	[Fact] public async Task Like_Exact() => Assert.Equal("True", await Scalar("SELECT 'abc' LIKE 'abc'"));
	[Fact] public async Task Like_ExactFalse() => Assert.Equal("False", await Scalar("SELECT 'abc' LIKE 'abcd'"));
	[Fact] public async Task NotLike_True() => Assert.Equal("True", await Scalar("SELECT 'hello' NOT LIKE '%world'"));
	[Fact] public async Task NotLike_False() => Assert.Equal("False", await Scalar("SELECT 'hello world' NOT LIKE '%world'"));
}
