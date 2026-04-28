using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for string function patterns: UPPER, LOWER, TRIM, REPLACE, REVERSE, REPEAT, SUBSTR, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class StringFunctionPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public StringFunctionPatternTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- UPPER ----
	[Fact] public async Task Upper_Lower() => Assert.Equal("HELLO", await Scalar("SELECT UPPER('hello')"));
	[Fact] public async Task Upper_Mixed() => Assert.Equal("HELLO WORLD", await Scalar("SELECT UPPER('Hello World')"));
	[Fact] public async Task Upper_Already() => Assert.Equal("ABC", await Scalar("SELECT UPPER('ABC')"));
	[Fact] public async Task Upper_Empty() => Assert.Equal("", await Scalar("SELECT UPPER('')"));
	[Fact] public async Task Upper_WithNumbers() => Assert.Equal("ABC123", await Scalar("SELECT UPPER('abc123')"));

	// ---- LOWER ----
	[Fact] public async Task Lower_Upper() => Assert.Equal("hello", await Scalar("SELECT LOWER('HELLO')"));
	[Fact] public async Task Lower_Mixed() => Assert.Equal("hello world", await Scalar("SELECT LOWER('Hello World')"));
	[Fact] public async Task Lower_Already() => Assert.Equal("abc", await Scalar("SELECT LOWER('abc')"));
	[Fact] public async Task Lower_Empty() => Assert.Equal("", await Scalar("SELECT LOWER('')"));
	[Fact] public async Task Lower_WithNumbers() => Assert.Equal("abc123", await Scalar("SELECT LOWER('ABC123')"));

	// ---- TRIM ----
	[Fact] public async Task Trim_Spaces() => Assert.Equal("hello", await Scalar("SELECT TRIM('  hello  ')"));
	[Fact] public async Task Trim_LeadingOnly() => Assert.Equal("hello", await Scalar("SELECT TRIM('   hello')"));
	[Fact] public async Task Trim_TrailingOnly() => Assert.Equal("hello", await Scalar("SELECT TRIM('hello   ')"));
	[Fact] public async Task Trim_NoSpaces() => Assert.Equal("hello", await Scalar("SELECT TRIM('hello')"));
	[Fact] public async Task Trim_AllSpaces() => Assert.Equal("", await Scalar("SELECT TRIM('   ')"));
	[Fact] public async Task Trim_Empty() => Assert.Equal("", await Scalar("SELECT TRIM('')"));

	// ---- LTRIM / RTRIM ----
	[Fact] public async Task Ltrim_Spaces() => Assert.Equal("hello  ", await Scalar("SELECT LTRIM('  hello  ')"));
	[Fact] public async Task Rtrim_Spaces() => Assert.Equal("  hello", await Scalar("SELECT RTRIM('  hello  ')"));

	// ---- LENGTH / CHAR_LENGTH / CHARACTER_LENGTH ----
	[Fact] public async Task Length_Hello() => Assert.Equal("5", await Scalar("SELECT LENGTH('Hello')"));
	[Fact] public async Task Length_Empty() => Assert.Equal("0", await Scalar("SELECT LENGTH('')"));
	[Fact] public async Task Length_WithSpaces() => Assert.Equal("11", await Scalar("SELECT LENGTH('Hello World')"));
	[Fact] public async Task CharLength_Hello() => Assert.Equal("5", await Scalar("SELECT CHAR_LENGTH('Hello')"));
	[Fact] public async Task CharacterLength_Hello() => Assert.Equal("5", await Scalar("SELECT CHARACTER_LENGTH('Hello')"));

	// ---- REPLACE ----
	[Fact] public async Task Replace_Single() => Assert.Equal("hi world", await Scalar("SELECT REPLACE('hello world', 'hello', 'hi')"));
	[Fact] public async Task Replace_Multiple() => Assert.Equal("hellx wxrld", await Scalar("SELECT REPLACE('hello world', 'o', 'x')"));
	[Fact] public async Task Replace_Remove() => Assert.Equal("hell wrld", await Scalar("SELECT REPLACE('hello world', 'o', '')"));
	[Fact] public async Task Replace_NoMatch() => Assert.Equal("hello", await Scalar("SELECT REPLACE('hello', 'xyz', 'abc')"));
	[Fact] public async Task Replace_Empty() => Assert.Equal("", await Scalar("SELECT REPLACE('', 'a', 'b')"));

	// ---- REVERSE ----
	[Fact] public async Task Reverse_Hello() => Assert.Equal("olleH", await Scalar("SELECT REVERSE('Hello')"));
	[Fact] public async Task Reverse_Palindrome() => Assert.Equal("racecar", await Scalar("SELECT REVERSE('racecar')"));
	[Fact] public async Task Reverse_Empty() => Assert.Equal("", await Scalar("SELECT REVERSE('')"));
	[Fact] public async Task Reverse_Single() => Assert.Equal("a", await Scalar("SELECT REVERSE('a')"));

	// ---- REPEAT ----
	[Fact] public async Task Repeat_3() => Assert.Equal("abcabcabc", await Scalar("SELECT REPEAT('abc', 3)"));
	[Fact] public async Task Repeat_1() => Assert.Equal("abc", await Scalar("SELECT REPEAT('abc', 1)"));
	[Fact] public async Task Repeat_0() => Assert.Equal("", await Scalar("SELECT REPEAT('abc', 0)"));
	[Fact] public async Task Repeat_SingleChar() => Assert.Equal("xxxxx", await Scalar("SELECT REPEAT('x', 5)"));

	// ---- SUBSTR ----
	[Fact] public async Task Substr_From2() => Assert.Equal("ello", await Scalar("SELECT SUBSTR('Hello', 2)"));
	[Fact] public async Task Substr_From1Len3() => Assert.Equal("Hel", await Scalar("SELECT SUBSTR('Hello', 1, 3)"));
	[Fact] public async Task Substr_From3Len2() => Assert.Equal("ll", await Scalar("SELECT SUBSTR('Hello', 3, 2)"));
	[Fact] public async Task Substr_Full() => Assert.Equal("Hello", await Scalar("SELECT SUBSTR('Hello', 1, 5)"));
	[Fact] public async Task Substr_Last() => Assert.Equal("o", await Scalar("SELECT SUBSTR('Hello', 5)"));
	[Fact] public async Task Substr_Negative() => Assert.Equal("lo", await Scalar("SELECT SUBSTR('Hello', -2)"));

	// ---- STARTS_WITH / ENDS_WITH ----
	[Fact] public async Task StartsWith_True() => Assert.Equal("True", await Scalar("SELECT STARTS_WITH('Hello', 'He')"));
	[Fact] public async Task StartsWith_False() => Assert.Equal("False", await Scalar("SELECT STARTS_WITH('Hello', 'lo')"));
	[Fact] public async Task StartsWith_Full() => Assert.Equal("True", await Scalar("SELECT STARTS_WITH('Hello', 'Hello')"));
	[Fact] public async Task StartsWith_Empty() => Assert.Equal("True", await Scalar("SELECT STARTS_WITH('Hello', '')"));
	[Fact] public async Task EndsWith_True() => Assert.Equal("True", await Scalar("SELECT ENDS_WITH('Hello', 'lo')"));
	[Fact] public async Task EndsWith_False() => Assert.Equal("False", await Scalar("SELECT ENDS_WITH('Hello', 'He')"));
	[Fact] public async Task EndsWith_Full() => Assert.Equal("True", await Scalar("SELECT ENDS_WITH('Hello', 'Hello')"));
	[Fact] public async Task EndsWith_Empty() => Assert.Equal("True", await Scalar("SELECT ENDS_WITH('Hello', '')"));

	// ---- STRPOS / INSTR ----
	[Fact] public async Task Strpos_Found() => Assert.Equal("7", await Scalar("SELECT STRPOS('Hello World', 'World')"));
	[Fact] public async Task Strpos_NotFound() => Assert.Equal("0", await Scalar("SELECT STRPOS('Hello World', 'xyz')"));
	[Fact] public async Task Strpos_FirstOccurrence() => Assert.Equal("3", await Scalar("SELECT STRPOS('abcabc', 'c')"));
	[Fact] public async Task Instr_Found() => Assert.Equal("7", await Scalar("SELECT INSTR('Hello World', 'World')"));

	// ---- LPAD / RPAD ----
	[Fact] public async Task Lpad_Zeros() => Assert.Equal("00042", await Scalar("SELECT LPAD('42', 5, '0')"));
	[Fact] public async Task Lpad_Spaces() => Assert.Equal("   Hi", await Scalar("SELECT LPAD('Hi', 5, ' ')"));
	[Fact] public async Task Lpad_NoChange() => Assert.Equal("Hello", await Scalar("SELECT LPAD('Hello', 5, '0')"));
	[Fact] public async Task Rpad_Zeros() => Assert.Equal("42000", await Scalar("SELECT RPAD('42', 5, '0')"));
	[Fact] public async Task Rpad_Spaces() => Assert.Equal("Hi   ", await Scalar("SELECT RPAD('Hi', 5, ' ')"));

	// ---- SPLIT ----
	[Fact] public async Task Split_Comma()
	{
		var v = await Scalar("SELECT ARRAY_LENGTH(SPLIT('a,b,c', ','))");
		Assert.Equal("3", v);
	}

	[Fact] public async Task Split_Space()
	{
		var v = await Scalar("SELECT ARRAY_LENGTH(SPLIT('hello world foo', ' '))");
		Assert.Equal("3", v);
	}

	// ---- CONCAT with many args ----
	[Fact] public async Task Concat_4Args() => Assert.Equal("abcd", await Scalar("SELECT CONCAT('a', 'b', 'c', 'd')"));
	[Fact] public async Task Concat_5Args() => Assert.Equal("abcde", await Scalar("SELECT CONCAT('a', 'b', 'c', 'd', 'e')"));
	[Fact] public async Task Concat_WithEmpty() => Assert.Equal("ac", await Scalar("SELECT CONCAT('a', '', 'c')"));

	// ---- LEFT / RIGHT ----
	[Fact] public async Task Left_3() => Assert.Equal("Hel", await Scalar("SELECT LEFT('Hello', 3)"));
	[Fact] public async Task Left_Full() => Assert.Equal("Hello", await Scalar("SELECT LEFT('Hello', 10)"));
	[Fact] public async Task Left_0() => Assert.Equal("", await Scalar("SELECT LEFT('Hello', 0)"));
	[Fact] public async Task Right_3() => Assert.Equal("llo", await Scalar("SELECT RIGHT('Hello', 3)"));
	[Fact] public async Task Right_Full() => Assert.Equal("Hello", await Scalar("SELECT RIGHT('Hello', 10)"));
	[Fact] public async Task Right_0() => Assert.Equal("", await Scalar("SELECT RIGHT('Hello', 0)"));

	// ---- Nested string functions ----
	[Fact] public async Task Nested_UpperTrim() => Assert.Equal("HELLO", await Scalar("SELECT UPPER(TRIM('  hello  '))"));
	[Fact] public async Task Nested_LowerReplace() => Assert.Equal("hi world", await Scalar("SELECT LOWER(REPLACE('HELLO WORLD', 'HELLO', 'HI'))"));
	[Fact] public async Task Nested_ConcatUpper() => Assert.Equal("HELLOWORLD", await Scalar("SELECT UPPER(CONCAT('hello', 'world'))"));
	[Fact] public async Task Nested_ReverseUpper() => Assert.Equal("OLLEH", await Scalar("SELECT UPPER(REVERSE('hello'))"));
	[Fact] public async Task Nested_RepeatConcat() => Assert.Equal("ababab!", await Scalar("SELECT CONCAT(REPEAT('ab', 3), '!')"));
	[Fact] public async Task Nested_LengthOfConcat() => Assert.Equal("10", await Scalar("SELECT LENGTH(CONCAT('hello', 'world'))"));
	[Fact] public async Task Nested_SubstrOfReplace() => Assert.Equal("Hi", await Scalar("SELECT SUBSTR(REPLACE('Hello', 'ello', 'i'), 1, 2)"));
}
