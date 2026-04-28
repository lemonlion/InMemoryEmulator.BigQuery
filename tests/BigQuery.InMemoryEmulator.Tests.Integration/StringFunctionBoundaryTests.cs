using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Comprehensive tests for string function boundary conditions and combinations.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class StringFunctionBoundaryTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public StringFunctionBoundaryTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- CONCAT combinations ----
	[Fact] public async Task Concat_TwoEmpty() => Assert.Equal("", await Scalar("SELECT CONCAT('', '')"));
	[Fact] public async Task Concat_FiveStrings() => Assert.Equal("abcde", await Scalar("SELECT CONCAT('a','b','c','d','e')"));
	[Fact] public async Task Concat_NumbersAsString() => Assert.Equal("123", await Scalar("SELECT CONCAT('1','2','3')"));
	[Fact] public async Task Concat_SpecialChars() => Assert.Equal("a!b@c", await Scalar("SELECT CONCAT('a!','b@','c')"));
	[Fact] public async Task Concat_Spaces() => Assert.Equal("a b", await Scalar("SELECT CONCAT('a',' ','b')"));
	[Fact] public async Task Concat_LongString() => Assert.Equal("aaaaaaaaaa", await Scalar("SELECT CONCAT('aaaaa','aaaaa')"));
	[Fact] public async Task ConcatOp_TwoStrings() => Assert.Equal("ab", await Scalar("SELECT 'a' || 'b'"));
	[Fact] public async Task ConcatOp_ThreeStrings() => Assert.Equal("abc", await Scalar("SELECT 'a' || 'b' || 'c'"));
	[Fact] public async Task ConcatOp_Empty() => Assert.Equal("a", await Scalar("SELECT 'a' || ''"));
	[Fact] public async Task ConcatOp_EmptyBoth() => Assert.Equal("", await Scalar("SELECT '' || ''"));

	// ---- UPPER boundary ----
	[Fact] public async Task Upper_Numbers() => Assert.Equal("123", await Scalar("SELECT UPPER('123')"));
	[Fact] public async Task Upper_Symbols() => Assert.Equal("!@#", await Scalar("SELECT UPPER('!@#')"));
	[Fact] public async Task Upper_SingleChar() => Assert.Equal("A", await Scalar("SELECT UPPER('a')"));
	[Fact] public async Task Upper_Space() => Assert.Equal(" ", await Scalar("SELECT UPPER(' ')"));

	// ---- LOWER boundary ----
	[Fact] public async Task Lower_Numbers() => Assert.Equal("123", await Scalar("SELECT LOWER('123')"));
	[Fact] public async Task Lower_Symbols() => Assert.Equal("!@#", await Scalar("SELECT LOWER('!@#')"));
	[Fact] public async Task Lower_SingleChar() => Assert.Equal("a", await Scalar("SELECT LOWER('A')"));

	// ---- LENGTH variants ----
	[Fact] public async Task Length_One() => Assert.Equal("1", await Scalar("SELECT LENGTH('a')"));
	[Fact] public async Task Length_Spaces() => Assert.Equal("3", await Scalar("SELECT LENGTH('   ')"));
	[Fact] public async Task Length_Numbers() => Assert.Equal("5", await Scalar("SELECT LENGTH('12345')"));
	[Fact] public async Task Length_SpecialChars() => Assert.Equal("3", await Scalar("SELECT LENGTH('!@#')"));
	[Fact] public async Task Length_Long() { var v = int.Parse(await Scalar("SELECT LENGTH(REPEAT('a', 100))") ?? "0"); Assert.Equal(100, v); }

	// ---- TRIM variants ----
	[Fact] public async Task Trim_OnlySpaces() => Assert.Equal("", await Scalar("SELECT TRIM('   ')"));
	[Fact] public async Task Trim_NoSpaces() => Assert.Equal("hello", await Scalar("SELECT TRIM('hello')"));
	[Fact] public async Task Trim_LeftOnly() => Assert.Equal("hello", await Scalar("SELECT TRIM('  hello')"));
	[Fact] public async Task Trim_RightOnly() => Assert.Equal("hello", await Scalar("SELECT TRIM('hello  ')"));
	[Fact] public async Task Trim_Tabs() => Assert.Equal("hello", await Scalar("SELECT TRIM('\thello\t')"));
	[Fact] public async Task Ltrim_NoMatch() => Assert.Equal("hello", await Scalar("SELECT LTRIM('hello')"));
	[Fact] public async Task Ltrim_AllSpaces() => Assert.Equal("", await Scalar("SELECT LTRIM('   ')"));
	[Fact] public async Task Rtrim_NoMatch() => Assert.Equal("hello", await Scalar("SELECT RTRIM('hello')"));
	[Fact] public async Task Rtrim_AllSpaces() => Assert.Equal("", await Scalar("SELECT RTRIM('   ')"));

	// ---- SUBSTR boundaries ----
	[Fact] public async Task Substr_FullString() => Assert.Equal("Hello", await Scalar("SELECT SUBSTR('Hello', 1)"));
	[Fact] public async Task Substr_LastChar() => Assert.Equal("o", await Scalar("SELECT SUBSTR('Hello', 5)"));
	[Fact] public async Task Substr_LengthBeyond() => Assert.Equal("llo", await Scalar("SELECT SUBSTR('Hello', 3, 100)"));
	[Fact] public async Task Substr_ZeroStart() => Assert.Equal("", await Scalar("SELECT SUBSTR('Hello', 0)"));
	[Fact] public async Task Substr_NegOne() => Assert.Equal("o", await Scalar("SELECT SUBSTR('Hello', -1)"));
	[Fact] public async Task Substr_NegThree() => Assert.Equal("llo", await Scalar("SELECT SUBSTR('Hello', -3)"));
	[Fact] public async Task Substr_SingleChar() => Assert.Equal("H", await Scalar("SELECT SUBSTR('Hello', 1, 1)"));
	[Fact] public async Task Substr_EmptyInput() => Assert.Equal("", await Scalar("SELECT SUBSTR('', 1)"));
	[Fact] public async Task Substr_NullInput() => Assert.Null(await Scalar("SELECT SUBSTR(NULL, 1)"));
	[Fact] public async Task Substr_NullLen() => Assert.Equal("", await Scalar("SELECT SUBSTR('Hello', 1, NULL)"));

	// ---- REVERSE boundaries ----
	[Fact] public async Task Reverse_SingleChar() => Assert.Equal("a", await Scalar("SELECT REVERSE('a')"));
	[Fact] public async Task Reverse_TwoChars() => Assert.Equal("ba", await Scalar("SELECT REVERSE('ab')"));
	[Fact] public async Task Reverse_WithSpaces() => Assert.Equal(" ba", await Scalar("SELECT REVERSE('ab ')"));
	[Fact] public async Task Reverse_Numbers() => Assert.Equal("321", await Scalar("SELECT REVERSE('123')"));

	// ---- REPLACE boundaries ----
	[Fact] public async Task Replace_EntireString() => Assert.Equal("world", await Scalar("SELECT REPLACE('hello', 'hello', 'world')"));
	[Fact] public async Task Replace_CaseSensitive() => Assert.Equal("Hello", await Scalar("SELECT REPLACE('Hello', 'hello', 'world')"));
	[Fact] public async Task Replace_Repeated() => Assert.Equal("xxx", await Scalar("SELECT REPLACE('aaa', 'a', 'x')"));
	[Fact] public async Task Replace_LongerReplacement() => Assert.Equal("hXXllo", await Scalar("SELECT REPLACE('hello', 'e', 'XX')"));
	[Fact] public async Task Replace_ShorterResult() => Assert.Equal("hllo", await Scalar("SELECT REPLACE('hello', 'e', '')"));

	// ---- STARTS_WITH/ENDS_WITH boundaries ----
	[Fact] public async Task StartsWith_ExactMatch() => Assert.Equal("True", await Scalar("SELECT STARTS_WITH('hello', 'hello')"));
	[Fact] public async Task StartsWith_LongerPrefix() => Assert.Equal("False", await Scalar("SELECT STARTS_WITH('hi', 'hello')"));
	[Fact] public async Task StartsWith_SingleChar() => Assert.Equal("True", await Scalar("SELECT STARTS_WITH('hello', 'h')"));
	[Fact] public async Task EndsWith_ExactMatch() => Assert.Equal("True", await Scalar("SELECT ENDS_WITH('hello', 'hello')"));
	[Fact] public async Task EndsWith_LongerSuffix() => Assert.Equal("False", await Scalar("SELECT ENDS_WITH('hi', 'hello')"));
	[Fact] public async Task EndsWith_SingleChar() => Assert.Equal("True", await Scalar("SELECT ENDS_WITH('hello', 'o')"));

	// ---- STRPOS/INSTR boundaries ----
	[Fact] public async Task Strpos_AtStart() => Assert.Equal("1", await Scalar("SELECT STRPOS('hello', 'h')"));
	[Fact] public async Task Strpos_AtEnd() => Assert.Equal("5", await Scalar("SELECT STRPOS('hello', 'o')"));
	[Fact] public async Task Strpos_EmptyInput() => Assert.Equal("0", await Scalar("SELECT STRPOS('', 'a')"));
	[Fact] public async Task Strpos_BothEmpty() => Assert.Equal("1", await Scalar("SELECT STRPOS('', '')"));
	[Fact] public async Task Strpos_MultiMatch() => Assert.Equal("1", await Scalar("SELECT STRPOS('abab', 'ab')"));
	[Fact] public async Task Instr_AtStart() => Assert.Equal("1", await Scalar("SELECT INSTR('hello', 'hel')"));
	[Fact] public async Task Instr_NotFound() => Assert.Equal("0", await Scalar("SELECT INSTR('hello', 'xyz')"));

	// ---- LPAD/RPAD boundaries ----
	[Fact] public async Task Lpad_ZeroLength() => Assert.Equal("", await Scalar("SELECT LPAD('abc', 0, 'x')"));
	[Fact] public async Task Lpad_ExactLength() => Assert.Equal("abc", await Scalar("SELECT LPAD('abc', 3, 'x')"));
	[Fact] public async Task Lpad_PadByOne() => Assert.Equal("xabc", await Scalar("SELECT LPAD('abc', 4, 'x')"));
	[Fact] public async Task Lpad_MultiChar() => Assert.Equal("xyxyab", await Scalar("SELECT LPAD('abc', 6, 'xy')"));
	[Fact] public async Task Rpad_ZeroLength() => Assert.Equal("", await Scalar("SELECT RPAD('abc', 0, 'x')"));
	[Fact] public async Task Rpad_ExactLength() => Assert.Equal("abc", await Scalar("SELECT RPAD('abc', 3, 'x')"));
	[Fact] public async Task Rpad_PadByOne() => Assert.Equal("abcx", await Scalar("SELECT RPAD('abc', 4, 'x')"));
	[Fact] public async Task Rpad_MultiChar() => Assert.Equal("abcxyx", await Scalar("SELECT RPAD('abc', 6, 'xy')"));

	// ---- LEFT/RIGHT boundaries ----
	[Fact] public async Task Left_Full() => Assert.Equal("abc", await Scalar("SELECT LEFT('abc', 5)"));
	[Fact] public async Task Left_One() => Assert.Equal("a", await Scalar("SELECT LEFT('abc', 1)"));
	[Fact] public async Task Left_Empty() => Assert.Equal("", await Scalar("SELECT LEFT('', 5)"));
	[Fact] public async Task Right_Full() => Assert.Equal("abc", await Scalar("SELECT RIGHT('abc', 5)"));
	[Fact] public async Task Right_One() => Assert.Equal("c", await Scalar("SELECT RIGHT('abc', 1)"));
	[Fact] public async Task Right_Empty() => Assert.Equal("", await Scalar("SELECT RIGHT('', 5)"));

	// ---- REPEAT boundaries ----
	[Fact] public async Task Repeat_Large() { var v = int.Parse(await Scalar("SELECT LENGTH(REPEAT('a', 50))") ?? "0"); Assert.Equal(50, v); }
	[Fact] public async Task Repeat_Two() => Assert.Equal("xx", await Scalar("SELECT REPEAT('x', 2)"));
	[Fact] public async Task Repeat_EmptyString() => Assert.Equal("", await Scalar("SELECT REPEAT('', 10)"));

	// ---- SPLIT boundaries ----
	[Fact] public async Task Split_SingleElement() => Assert.Equal("1", await Scalar("SELECT ARRAY_LENGTH(SPLIT('abc', ','))"));
	[Fact] public async Task Split_MultiDelim() => Assert.Equal("4", await Scalar("SELECT ARRAY_LENGTH(SPLIT('a,b,c,d', ','))"));
	[Fact] public async Task Split_ConsecutiveDelim() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(SPLIT('a,,c', ','))"));
	[Fact] public async Task Split_TrailingDelim() => Assert.Equal("2", await Scalar("SELECT ARRAY_LENGTH(SPLIT('a,', ','))"));
	[Fact] public async Task Split_LeadingDelim() => Assert.Equal("2", await Scalar("SELECT ARRAY_LENGTH(SPLIT(',a', ','))"));

	// ---- CONTAINS_SUBSTR deeper ----
	[Fact] public async Task ContainsSubstr_Empty() => Assert.Equal("True", await Scalar("SELECT CONTAINS_SUBSTR('hello', '')"));
	[Fact] public async Task ContainsSubstr_SingleChar() => Assert.Equal("True", await Scalar("SELECT CONTAINS_SUBSTR('hello', 'h')"));
	[Fact] public async Task ContainsSubstr_ExactMatch() => Assert.Equal("True", await Scalar("SELECT CONTAINS_SUBSTR('hello', 'hello')"));
	[Fact] public async Task ContainsSubstr_LongerSubstr() => Assert.Equal("False", await Scalar("SELECT CONTAINS_SUBSTR('hi', 'hello')"));

	// ---- REGEXP deeper (without r'' prefix) ----
	[Fact] public async Task Regexp_DigitPattern() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('abc123', '[0-9]+')"));
	[Fact] public async Task Regexp_AlphaPattern() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('abc123', '[a-z]+')"));
	[Fact] public async Task Regexp_AnchorStart() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('hello', '^hel')"));
	[Fact] public async Task Regexp_AnchorEnd() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('hello', 'llo$')"));
	[Fact] public async Task Regexp_DotStar() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('hello world', 'hello.*world')"));
	[Fact] public async Task Regexp_NoMatch() => Assert.Equal("False", await Scalar("SELECT REGEXP_CONTAINS('hello', '^world')"));
	[Fact] public async Task RegexpExtract_Group() => Assert.Equal("123", await Scalar("SELECT REGEXP_EXTRACT('abc123def', '([0-9]+)')"));
	[Fact] public async Task RegexpExtract_NoGroup() => Assert.Equal("abc", await Scalar("SELECT REGEXP_EXTRACT('abc123', '[a-z]+')"));
	[Fact] public async Task RegexpReplace_Digit() => Assert.Equal("abc___def", await Scalar("SELECT REGEXP_REPLACE('abc123def', '[0-9]+', '___')"));
	[Fact] public async Task RegexpReplace_NoOp() => Assert.Equal("abc", await Scalar("SELECT REGEXP_REPLACE('abc', '[0-9]+', 'X')"));
	[Fact] public async Task RegexpExtractAll_Count() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(REGEXP_EXTRACT_ALL('a1b2c3', '[0-9]'))"));

	// ---- INITCAP ----
	[Fact] public async Task Initcap_Lower() => Assert.Equal("Hello World", await Scalar("SELECT INITCAP('hello world')"));
	[Fact] public async Task Initcap_Upper() => Assert.Equal("Hello World", await Scalar("SELECT INITCAP('HELLO WORLD')"));
	[Fact] public async Task Initcap_SingleWord() => Assert.Equal("Hello", await Scalar("SELECT INITCAP('hello')"));
	[Fact] public async Task Initcap_Empty() => Assert.Equal("", await Scalar("SELECT INITCAP('')"));
	[Fact] public async Task Initcap_SingleChar() => Assert.Equal("A", await Scalar("SELECT INITCAP('a')"));

	// ---- SOUNDEX ----
	[Fact] public async Task Soundex_Robert() => Assert.Equal("R163", await Scalar("SELECT SOUNDEX('Robert')"));

	// ---- ASCII/CHR/UNICODE ----
	[Fact] public async Task Ascii_Space() => Assert.Equal("32", await Scalar("SELECT ASCII(' ')"));
	[Fact] public async Task Ascii_Zero() => Assert.Equal("48", await Scalar("SELECT ASCII('0')"));
	[Fact] public async Task Ascii_Z() => Assert.Equal("90", await Scalar("SELECT ASCII('Z')"));
	[Fact] public async Task Chr_Space() => Assert.Equal(" ", await Scalar("SELECT CHR(32)"));
	[Fact] public async Task Chr_ZeroChar() => Assert.Equal("0", await Scalar("SELECT CHR(48)"));
	[Fact] public async Task Unicode_Space() => Assert.Equal("32", await Scalar("SELECT UNICODE(' ')"));

	// ---- TO_CODE_POINTS / CODE_POINTS_TO_STRING ----
	[Fact] public async Task ToCodePoints_Empty() => Assert.Equal("0", await Scalar("SELECT ARRAY_LENGTH(TO_CODE_POINTS(''))"));
	[Fact] public async Task ToCodePoints_Single() => Assert.Equal("1", await Scalar("SELECT ARRAY_LENGTH(TO_CODE_POINTS('A'))"));
	[Fact] public async Task ToCodePoints_Three() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(TO_CODE_POINTS('ABC'))"));
	[Fact] public async Task CodePointsToString_Roundtrip() => Assert.Equal("Hi", await Scalar("SELECT CODE_POINTS_TO_STRING(TO_CODE_POINTS('Hi'))"));

	// ---- Combinations ----
	[Fact] public async Task UpperThenLower() => Assert.Equal("hello", await Scalar("SELECT LOWER(UPPER('Hello'))"));
	[Fact] public async Task TrimThenLength() => Assert.Equal("5", await Scalar("SELECT LENGTH(TRIM('  hello  '))"));
	[Fact] public async Task ReverseReverse() => Assert.Equal("hello", await Scalar("SELECT REVERSE(REVERSE('hello'))"));
	[Fact] public async Task SubstrThenUpper() => Assert.Equal("HE", await Scalar("SELECT UPPER(SUBSTR('hello', 1, 2))"));
	[Fact] public async Task ConcatThenLength() => Assert.Equal("10", await Scalar("SELECT LENGTH(CONCAT('hello','world'))"));
	[Fact] public async Task ReplaceThenReverse() => Assert.Equal("ollxh", await Scalar("SELECT REVERSE(REPLACE('hello', 'e', 'x'))"));
	[Fact] public async Task LpadThenSubstr() => Assert.Equal("00", await Scalar("SELECT SUBSTR(LPAD('x', 3, '0'), 1, 2)"));
	[Fact] public async Task RepeatThenLength() => Assert.Equal("15", await Scalar("SELECT LENGTH(REPEAT('abc', 5))"));
	[Fact] public async Task SplitThenLength() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(SPLIT(CONCAT('a',',','b',',','c'), ','))"));
}
