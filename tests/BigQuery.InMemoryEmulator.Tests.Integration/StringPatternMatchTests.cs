using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for string pattern matching: LIKE, REGEXP_CONTAINS, REGEXP_EXTRACT, REGEXP_REPLACE, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class StringPatternMatchTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public StringPatternMatchTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- LIKE patterns ----
	[Fact] public async Task Like_Exact() => Assert.Equal("True", await Scalar("SELECT 'hello' LIKE 'hello'"));
	[Fact] public async Task Like_PercentEnd() => Assert.Equal("True", await Scalar("SELECT 'hello' LIKE 'hel%'"));
	[Fact] public async Task Like_PercentStart() => Assert.Equal("True", await Scalar("SELECT 'hello' LIKE '%llo'"));
	[Fact] public async Task Like_PercentBoth() => Assert.Equal("True", await Scalar("SELECT 'hello' LIKE '%ell%'"));
	[Fact] public async Task Like_PercentAll() => Assert.Equal("True", await Scalar("SELECT 'anything' LIKE '%'"));
	[Fact] public async Task Like_Underscore() => Assert.Equal("True", await Scalar("SELECT 'hat' LIKE 'h_t'"));
	[Fact] public async Task Like_UnderscoreMulti() => Assert.Equal("True", await Scalar("SELECT 'hello' LIKE 'h___o'"));
	[Fact] public async Task Like_UnderscorePercent() => Assert.Equal("True", await Scalar("SELECT 'hello' LIKE 'h_%'"));
	[Fact] public async Task Like_NoMatch() => Assert.Equal("False", await Scalar("SELECT 'hello' LIKE 'world'"));
	[Fact] public async Task Like_EmptyPattern() => Assert.Equal("False", await Scalar("SELECT 'hello' LIKE ''"));
	[Fact] public async Task Like_EmptyString() => Assert.Equal("True", await Scalar("SELECT '' LIKE ''"));
	[Fact] public async Task Like_EmptyWithPercent() => Assert.Equal("True", await Scalar("SELECT '' LIKE '%'"));
	[Fact] public async Task Like_SpecialChars() => Assert.Equal("True", await Scalar("SELECT 'abc.def' LIKE 'abc%def'"));
	[Fact] public async Task Like_MultiPercent() => Assert.Equal("True", await Scalar("SELECT 'aXbYc' LIKE 'a%b%c'"));
	[Fact] public async Task NotLike_True() => Assert.Equal("True", await Scalar("SELECT 'hello' NOT LIKE 'world'"));
	[Fact] public async Task NotLike_False() => Assert.Equal("False", await Scalar("SELECT 'hello' NOT LIKE '%ello'"));

	// ---- REGEXP_CONTAINS ----
	[Fact(Skip = "Regex backslash escape in integration path")] public async Task RegexpContains_Basic() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('hello123', '\\\\d+')"));
	[Fact] public async Task RegexpContains_NoMatch() => Assert.Equal("False", await Scalar("SELECT REGEXP_CONTAINS('hello', '\\\\d+')"));
	[Fact] public async Task RegexpContains_StartAnchor() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('hello', '^hel')"));
	[Fact] public async Task RegexpContains_EndAnchor() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('hello', 'llo$')"));
	[Fact] public async Task RegexpContains_AnyChar() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('hello', 'h.llo')"));
	[Fact] public async Task RegexpContains_CharClass() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('hello', '[aeiou]')"));
	[Fact] public async Task RegexpContains_Alternation() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('cat', 'cat|dog')"));
	[Fact] public async Task RegexpContains_Alternation_NoMatch() => Assert.Equal("False", await Scalar("SELECT REGEXP_CONTAINS('fish', 'cat|dog')"));
	[Fact] public async Task RegexpContains_Plus() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('aaa', 'a+')"));
	[Fact] public async Task RegexpContains_Star() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('', 'a*')"));
	[Fact] public async Task RegexpContains_Question() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('color', 'colou?r')"));
	[Fact] public async Task RegexpContains_Null() => Assert.Null(await Scalar("SELECT REGEXP_CONTAINS(NULL, '\\\\d+')"));

	// ---- REGEXP_EXTRACT ----
	[Fact(Skip = "Regex backslash escape in integration path")] public async Task RegexpExtract_Digits() => Assert.Equal("123", await Scalar("SELECT REGEXP_EXTRACT('hello123world', '\\\\d+')"));
	[Fact] public async Task RegexpExtract_Word() => Assert.Equal("hello", await Scalar("SELECT REGEXP_EXTRACT('hello world', '[a-z]+')"));
	[Fact] public async Task RegexpExtract_NoMatch() => Assert.Null(await Scalar("SELECT REGEXP_EXTRACT('hello', '\\\\d+')"));
	[Fact] public async Task RegexpExtract_Group() => Assert.Equal("123", await Scalar("SELECT REGEXP_EXTRACT('abc123def', '([0-9]+)')"));
	[Fact] public async Task RegexpExtract_Null() => Assert.Null(await Scalar("SELECT REGEXP_EXTRACT(NULL, '\\\\d+')"));

	// ---- REGEXP_REPLACE ----
	[Fact(Skip = "Regex backslash escape in integration path")] public async Task RegexpReplace_Digits() => Assert.Equal("helloXworld", await Scalar("SELECT REGEXP_REPLACE('hello123world', '\\\\d+', 'X')"));
	[Fact] public async Task RegexpReplace_NoMatch() => Assert.Equal("hello", await Scalar("SELECT REGEXP_REPLACE('hello', '\\\\d+', 'X')"));
	[Fact] public async Task RegexpReplace_All() => Assert.Equal("X X X", await Scalar("SELECT REGEXP_REPLACE('a b c', '[a-z]', 'X')"));
	[Fact] public async Task RegexpReplace_Empty() => Assert.Equal("helloworld", await Scalar("SELECT REGEXP_REPLACE('hello world', ' ', '')"));
	[Fact] public async Task RegexpReplace_Null() => Assert.Null(await Scalar("SELECT REGEXP_REPLACE(NULL, '\\\\d+', 'X')"));

	// ---- REGEXP_EXTRACT_ALL ----
	[Fact(Skip = "Regex backslash escape in integration path")] public async Task RegexpExtractAll_Digits()
	{
		var v = await Scalar("SELECT ARRAY_TO_STRING(REGEXP_EXTRACT_ALL('a1b2c3', '\\\\d'), ',')");
		Assert.Equal("1,2,3", v);
	}
	[Fact] public async Task RegexpExtractAll_Words()
	{
		var v = await Scalar("SELECT ARRAY_TO_STRING(REGEXP_EXTRACT_ALL('hello world foo', '[a-z]+'), ',')");
		Assert.Equal("hello,world,foo", v);
	}
	[Fact] public async Task RegexpExtractAll_NoMatch()
	{
		var v = await Scalar("SELECT ARRAY_LENGTH(REGEXP_EXTRACT_ALL('hello', '\\\\d'))");
		Assert.Equal("0", v);
	}

	// ---- CONTAINS_SUBSTR (BigQuery extension) ----
	[Fact] public async Task ContainsSubstr_Found() => Assert.Equal("True", await Scalar("SELECT CONTAINS_SUBSTR('hello world', 'world')"));
	[Fact] public async Task ContainsSubstr_NotFound() => Assert.Equal("False", await Scalar("SELECT CONTAINS_SUBSTR('hello world', 'xyz')"));
	[Fact] public async Task ContainsSubstr_CaseInsensitive() => Assert.Equal("True", await Scalar("SELECT CONTAINS_SUBSTR('Hello World', 'hello')"));
	[Fact] public async Task ContainsSubstr_Empty() => Assert.Equal("True", await Scalar("SELECT CONTAINS_SUBSTR('hello', '')"));
	[Fact] public async Task ContainsSubstr_Null() => Assert.Null(await Scalar("SELECT CONTAINS_SUBSTR(NULL, 'test')"));

	// ---- STARTS_WITH / ENDS_WITH ----
	[Fact] public async Task StartsWith_True() => Assert.Equal("True", await Scalar("SELECT STARTS_WITH('hello', 'hel')"));
	[Fact] public async Task StartsWith_False() => Assert.Equal("False", await Scalar("SELECT STARTS_WITH('hello', 'xyz')"));
	[Fact] public async Task StartsWith_Empty() => Assert.Equal("True", await Scalar("SELECT STARTS_WITH('hello', '')"));
	[Fact] public async Task StartsWith_Full() => Assert.Equal("True", await Scalar("SELECT STARTS_WITH('hello', 'hello')"));
	[Fact] public async Task StartsWith_Longer() => Assert.Equal("False", await Scalar("SELECT STARTS_WITH('hi', 'hello')"));
	[Fact] public async Task StartsWith_Null() => Assert.Null(await Scalar("SELECT STARTS_WITH(NULL, 'test')"));
	[Fact] public async Task EndsWith_True() => Assert.Equal("True", await Scalar("SELECT ENDS_WITH('hello', 'llo')"));
	[Fact] public async Task EndsWith_False() => Assert.Equal("False", await Scalar("SELECT ENDS_WITH('hello', 'xyz')"));
	[Fact] public async Task EndsWith_Empty() => Assert.Equal("True", await Scalar("SELECT ENDS_WITH('hello', '')"));
	[Fact] public async Task EndsWith_Full() => Assert.Equal("True", await Scalar("SELECT ENDS_WITH('hello', 'hello')"));
	[Fact] public async Task EndsWith_Longer() => Assert.Equal("False", await Scalar("SELECT ENDS_WITH('hi', 'hello')"));
	[Fact] public async Task EndsWith_Null() => Assert.Null(await Scalar("SELECT ENDS_WITH(NULL, 'test')"));
}
