using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Additional string function tests: TRANSLATE, NORMALIZE, SOUNDEX, FORMAT,
/// and complex string manipulation patterns.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class StringManipulationTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public StringManipulationTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- TRANSLATE ----
	[Fact] public async Task Translate_Basic() => Assert.Equal("xbcde", await Scalar("SELECT TRANSLATE('abcde', 'a', 'x')"));
	[Fact] public async Task Translate_Multi() => Assert.Equal("xycde", await Scalar("SELECT TRANSLATE('abcde', 'ab', 'xy')"));
	[Fact] public async Task Translate_NoMatch() => Assert.Equal("abcde", await Scalar("SELECT TRANSLATE('abcde', 'z', 'x')"));

	// ---- NORMALIZE / NORMALIZE_AND_CASEFOLD ----
	[Fact] public async Task Normalize_Basic() { var v = await Scalar("SELECT NORMALIZE('hello')"); Assert.Equal("hello", v); }
	[Fact] public async Task NormalizeCasefold_Basic() { var v = await Scalar("SELECT NORMALIZE_AND_CASEFOLD('Hello')"); Assert.Equal("hello", v); }

	// ---- Complex STRPOS patterns ----
	[Fact] public async Task Strpos_InReplace() => Assert.Equal("3", await Scalar("SELECT STRPOS('abcabc', 'c')"));
	[Fact] public async Task Strpos_CaseCheck() => Assert.Equal("0", await Scalar("SELECT STRPOS('HELLO', 'hello')"));
	[Fact] public async Task Strpos_Multichar() => Assert.Equal("3", await Scalar("SELECT STRPOS('abcdef', 'cd')"));
	[Fact] public async Task Strpos_AtEnd() => Assert.Equal("4", await Scalar("SELECT STRPOS('abcdef', 'def')"));
	[Fact] public async Task Strpos_Beginning() => Assert.Equal("1", await Scalar("SELECT STRPOS('abcdef', 'abc')"));
	[Fact] public async Task Strpos_NotPresent() => Assert.Equal("0", await Scalar("SELECT STRPOS('abcdef', 'xyz')"));
	[Fact] public async Task Strpos_Single() => Assert.Equal("3", await Scalar("SELECT STRPOS('abcdef', 'c')"));
	[Fact] public async Task Strpos_FullMatch() => Assert.Equal("1", await Scalar("SELECT STRPOS('abc', 'abc')"));

	// ---- Complex SUBSTR patterns ----
	[Fact] public async Task Substr_NegCount() => Assert.Equal("wor", await Scalar("SELECT SUBSTR('hello world', -5, 3)"));
	[Fact] public async Task Substr_LastTwo() => Assert.Equal("ld", await Scalar("SELECT SUBSTR('hello world', -2)"));
	[Fact] public async Task Substr_Middle()  => Assert.Equal("lo w", await Scalar("SELECT SUBSTR('hello world', 4, 4)"));
	[Fact] public async Task Substr_OneChar() => Assert.Equal("e", await Scalar("SELECT SUBSTR('hello', 2, 1)"));

	// ---- REPEAT with expressions ----
	[Fact] public async Task Repeat_Ten() { var v = await Scalar("SELECT LENGTH(REPEAT('ab', 10))"); Assert.Equal("20", v); }
	[Fact] public async Task Repeat_Large() { var v = await Scalar("SELECT LENGTH(REPEAT('x', 100))"); Assert.Equal("100", v); }

	// ---- STARTS_WITH / ENDS_WITH combinations ----
	[Fact] public async Task StartsWith_Empty() => Assert.Equal("True", await Scalar("SELECT STARTS_WITH('hello', '')"));
	[Fact] public async Task EndsWith_Empty() => Assert.Equal("True", await Scalar("SELECT ENDS_WITH('hello', '')"));
	[Fact] public async Task StartsWith_Longer() => Assert.Equal("False", await Scalar("SELECT STARTS_WITH('hi', 'hello')"));
	[Fact] public async Task EndsWith_Longer() => Assert.Equal("False", await Scalar("SELECT ENDS_WITH('hi', 'hello')"));

	// ---- FORMAT_DATE patterns ----
	[Fact] public async Task FormatDate_DayName() { var v = await Scalar("SELECT FORMAT_DATE('%A', DATE '2024-01-15')"); Assert.NotNull(v); }
	[Fact] public async Task FormatDate_MonthName() { var v = await Scalar("SELECT FORMAT_DATE('%B', DATE '2024-01-15')"); Assert.NotNull(v); }
	[Fact] public async Task FormatDate_DayOfMonth() => Assert.Equal("15", await Scalar("SELECT FORMAT_DATE('%d', DATE '2024-01-15')"));
	[Fact] public async Task FormatDate_Month_m() => Assert.Equal("01", await Scalar("SELECT FORMAT_DATE('%m', DATE '2024-01-15')"));

	// ---- String comparison patterns ----
	[Fact] public async Task StrEq_True() => Assert.Equal("True", await Scalar("SELECT 'hello' = 'hello'"));
	[Fact] public async Task StrEq_False() => Assert.Equal("False", await Scalar("SELECT 'hello' = 'world'"));
	[Fact] public async Task StrEq_CaseSensitive() => Assert.Equal("False", await Scalar("SELECT 'Hello' = 'hello'"));
	[Fact] public async Task StrNeq_True() => Assert.Equal("True", await Scalar("SELECT 'hello' != 'world'"));
	[Fact] public async Task StrLt_True2() => Assert.Equal("True", await Scalar("SELECT 'abc' < 'abd'"));
	[Fact] public async Task StrGt_True2() => Assert.Equal("True", await Scalar("SELECT 'abd' > 'abc'"));
	[Fact] public async Task StrIn_True() => Assert.Equal("True", await Scalar("SELECT 'b' IN ('a', 'b', 'c')"));
	[Fact] public async Task StrIn_False() => Assert.Equal("False", await Scalar("SELECT 'd' IN ('a', 'b', 'c')"));
	[Fact] public async Task StrBetween_True() => Assert.Equal("True", await Scalar("SELECT 'b' BETWEEN 'a' AND 'c'"));
	[Fact] public async Task StrBetween_False() => Assert.Equal("False", await Scalar("SELECT 'd' BETWEEN 'a' AND 'c'"));

	// ---- String + number conversion patterns ----
	[Fact] public async Task CastIntToConcat() => Assert.Equal("item_42", await Scalar("SELECT CONCAT('item_', CAST(42 AS STRING))"));
	[Fact] public async Task CastFloatToConcat() => Assert.Equal("val_3.14", await Scalar("SELECT CONCAT('val_', CAST(3.14 AS STRING))"));
	[Fact] public async Task CastBoolToConcat() => Assert.Equal("flag_true", await Scalar("SELECT CONCAT('flag_', CAST(TRUE AS STRING))"));

	// ---- Complex string building ----
	[Fact]
	public async Task BuildCSV() => Assert.Equal("1,2,3", await Scalar("SELECT ARRAY_TO_STRING(['1','2','3'], ',')"));

	[Fact]
	public async Task BuildPath() => Assert.Equal("/users/alice/profile", await Scalar("SELECT CONCAT('/', ARRAY_TO_STRING(['users','alice','profile'], '/'))"));

	[Fact]
	public async Task PadAndConcat() => Assert.Equal("001-002-003", await Scalar("SELECT CONCAT(LPAD('1', 3, '0'), '-', LPAD('2', 3, '0'), '-', LPAD('3', 3, '0'))"));

	// ---- Unicode ----
	[Fact] public async Task Unicode_Length() => Assert.Equal("5", await Scalar("SELECT LENGTH('héllo')"));
	[Fact] public async Task Unicode_Upper() => Assert.Equal("HÉLLO", await Scalar("SELECT UPPER('héllo')"));
	[Fact] public async Task Unicode_Lower() => Assert.Equal("héllo", await Scalar("SELECT LOWER('HÉLLO')"));
	[Fact] public async Task Unicode_Reverse() => Assert.Equal("olléh", await Scalar("SELECT REVERSE('héllo')"));
	[Fact] public async Task Unicode_Substr() => Assert.Equal("hé", await Scalar("SELECT SUBSTR('héllo', 1, 2)"));

	// ---- Edge cases ----
	[Fact] public async Task SingleChar_Upper() => Assert.Equal("A", await Scalar("SELECT UPPER('a')"));
	[Fact] public async Task SingleChar_Lower() => Assert.Equal("a", await Scalar("SELECT LOWER('A')"));
	[Fact] public async Task SingleChar_Reverse() => Assert.Equal("a", await Scalar("SELECT REVERSE('a')"));
	[Fact] public async Task SingleChar_Length() => Assert.Equal("1", await Scalar("SELECT LENGTH('a')"));
	[Fact] public async Task LongString_Length() => Assert.Equal("52", await Scalar("SELECT LENGTH(REPEAT('ab', 26))"));
	[Fact] public async Task LongString_Substr() => Assert.Equal("ab", await Scalar("SELECT SUBSTR(REPEAT('ab', 10), 1, 2)"));
}
