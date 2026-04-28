using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Extensive tests for string manipulation functions.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class StringFunctionDeepTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public StringFunctionDeepTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- CONCAT ----
	[Fact] public async Task Concat_Two() => Assert.Equal("ab", await Scalar("SELECT CONCAT('a', 'b')"));
	[Fact] public async Task Concat_Three() => Assert.Equal("abc", await Scalar("SELECT CONCAT('a', 'b', 'c')"));
	[Fact] public async Task Concat_Four() => Assert.Equal("abcd", await Scalar("SELECT CONCAT('a', 'b', 'c', 'd')"));
	[Fact] public async Task Concat_Five() => Assert.Equal("abcde", await Scalar("SELECT CONCAT('a', 'b', 'c', 'd', 'e')"));
	[Fact] public async Task Concat_Empty() => Assert.Equal("ab", await Scalar("SELECT CONCAT('a', '', 'b')"));
	[Fact] public async Task Concat_AllEmpty() => Assert.Equal("", await Scalar("SELECT CONCAT('', '', '')"));
	[Fact] public async Task Concat_Spaces() => Assert.Equal("hello world", await Scalar("SELECT CONCAT('hello', ' ', 'world')"));
	[Fact] public async Task Concat_Numbers() => Assert.Equal("12", await Scalar("SELECT CONCAT('1', '2')"));
	[Fact] public async Task Concat_Long() => Assert.Equal("abcdefghij", await Scalar("SELECT CONCAT('abcde', 'fghij')"));
	[Fact] public async Task Concat_Unicode() => Assert.Equal("héllo", await Scalar("SELECT CONCAT('hé', 'llo')"));

	// ---- UPPER / LOWER ----
	[Fact] public async Task Upper_Basic() => Assert.Equal("HELLO", await Scalar("SELECT UPPER('hello')"));
	[Fact] public async Task Upper_Mixed() => Assert.Equal("HELLO WORLD", await Scalar("SELECT UPPER('Hello World')"));
	[Fact] public async Task Upper_AlreadyUpper() => Assert.Equal("ABC", await Scalar("SELECT UPPER('ABC')"));
	[Fact] public async Task Upper_Empty() => Assert.Equal("", await Scalar("SELECT UPPER('')"));
	[Fact] public async Task Upper_Numbers() => Assert.Equal("ABC123", await Scalar("SELECT UPPER('abc123')"));
	[Fact] public async Task Upper_Special() => Assert.Equal("A-B.C", await Scalar("SELECT UPPER('a-b.c')"));
	[Fact] public async Task Lower_Basic() => Assert.Equal("hello", await Scalar("SELECT LOWER('HELLO')"));
	[Fact] public async Task Lower_Mixed() => Assert.Equal("hello world", await Scalar("SELECT LOWER('Hello World')"));
	[Fact] public async Task Lower_AlreadyLower() => Assert.Equal("abc", await Scalar("SELECT LOWER('abc')"));
	[Fact] public async Task Lower_Empty() => Assert.Equal("", await Scalar("SELECT LOWER('')"));
	[Fact] public async Task Lower_Numbers() => Assert.Equal("abc123", await Scalar("SELECT LOWER('ABC123')"));
	[Fact] public async Task Lower_Special() => Assert.Equal("a-b.c", await Scalar("SELECT LOWER('A-B.C')"));

	// ---- LENGTH / CHAR_LENGTH / CHARACTER_LENGTH ----
	[Fact] public async Task Length_Basic() => Assert.Equal("5", await Scalar("SELECT LENGTH('hello')"));
	[Fact] public async Task Length_Empty() => Assert.Equal("0", await Scalar("SELECT LENGTH('')"));
	[Fact] public async Task Length_Space() => Assert.Equal("1", await Scalar("SELECT LENGTH(' ')"));
	[Fact] public async Task Length_Spaces() => Assert.Equal("3", await Scalar("SELECT LENGTH('   ')"));
	[Fact] public async Task Length_Numbers() => Assert.Equal("4", await Scalar("SELECT LENGTH('1234')"));
	[Fact] public async Task Length_Long() => Assert.Equal("26", await Scalar("SELECT LENGTH('abcdefghijklmnopqrstuvwxyz')"));
	[Fact] public async Task CharLength_Basic() => Assert.Equal("5", await Scalar("SELECT CHAR_LENGTH('hello')"));
	[Fact] public async Task CharacterLength_Basic() => Assert.Equal("5", await Scalar("SELECT CHARACTER_LENGTH('hello')"));

	// ---- TRIM / LTRIM / RTRIM ----
	[Fact] public async Task Trim_Both() => Assert.Equal("hello", await Scalar("SELECT TRIM('  hello  ')"));
	[Fact] public async Task Trim_Left() => Assert.Equal("hello  ", await Scalar("SELECT LTRIM('  hello  ')"));
	[Fact] public async Task Trim_Right() => Assert.Equal("  hello", await Scalar("SELECT RTRIM('  hello  ')"));
	[Fact] public async Task Trim_NoSpaces() => Assert.Equal("hello", await Scalar("SELECT TRIM('hello')"));
	[Fact] public async Task Trim_AllSpaces() => Assert.Equal("", await Scalar("SELECT TRIM('   ')"));
	[Fact(Skip = "Emulator limitation")] public async Task Trim_Chars() => Assert.Equal("hello", await Scalar("SELECT TRIM('xxhelloxx', 'x')"));
	[Fact(Skip = "Emulator limitation")] public async Task Ltrim_Chars() => Assert.Equal("helloxx", await Scalar("SELECT LTRIM('xxhelloxx', 'x')"));
	[Fact(Skip = "Emulator limitation")] public async Task Rtrim_Chars() => Assert.Equal("xxhello", await Scalar("SELECT RTRIM('xxhelloxx', 'x')"));
	[Fact(Skip = "Emulator limitation")] public async Task Trim_MultiChar() => Assert.Equal("hello", await Scalar("SELECT TRIM('xyxyhellyxyxy', 'xy')"));
	[Fact] public async Task Trim_Empty() => Assert.Equal("", await Scalar("SELECT TRIM('')"));

	// ---- SUBSTR / SUBSTRING ----
	[Fact] public async Task Substr_From1() => Assert.Equal("hello", await Scalar("SELECT SUBSTR('hello', 1)"));
	[Fact] public async Task Substr_From3() => Assert.Equal("llo", await Scalar("SELECT SUBSTR('hello', 3)"));
	[Fact] public async Task Substr_Len2() => Assert.Equal("he", await Scalar("SELECT SUBSTR('hello', 1, 2)"));
	[Fact] public async Task Substr_Len1() => Assert.Equal("h", await Scalar("SELECT SUBSTR('hello', 1, 1)"));
	[Fact] public async Task Substr_Mid() => Assert.Equal("ell", await Scalar("SELECT SUBSTR('hello', 2, 3)"));
	[Fact] public async Task Substr_Last() => Assert.Equal("o", await Scalar("SELECT SUBSTR('hello', 5)"));
	[Fact] public async Task Substr_Negative() => Assert.Equal("llo", await Scalar("SELECT SUBSTR('hello', -3)"));
	[Fact] public async Task Substr_NegWithLen() => Assert.Equal("ll", await Scalar("SELECT SUBSTR('hello', -3, 2)"));
	[Fact] public async Task Substr_Empty() => Assert.Equal("", await Scalar("SELECT SUBSTR('', 1)"));
	[Fact] public async Task Substr_OverLen() => Assert.Equal("hello", await Scalar("SELECT SUBSTR('hello', 1, 100)"));
	[Fact] public async Task Substring_Alias() => Assert.Equal("hel", await Scalar("SELECT SUBSTRING('hello', 1, 3)"));

	// ---- REPLACE ----
	[Fact] public async Task Replace_Basic() => Assert.Equal("hxllo", await Scalar("SELECT REPLACE('hello', 'e', 'x')"));
	[Fact] public async Task Replace_Multi() => Assert.Equal("aXcXe", await Scalar("SELECT REPLACE('abcbe', 'b', 'X')"));
	[Fact] public async Task Replace_NoMatch() => Assert.Equal("hello", await Scalar("SELECT REPLACE('hello', 'z', 'x')"));
	[Fact] public async Task Replace_Remove() => Assert.Equal("hllo", await Scalar("SELECT REPLACE('hello', 'e', '')"));
	[Fact] public async Task Replace_Longer() => Assert.Equal("hXXXllo", await Scalar("SELECT REPLACE('hello', 'e', 'XXX')"));
	[Fact] public async Task Replace_Word() => Assert.Equal("hello universe", await Scalar("SELECT REPLACE('hello world', 'world', 'universe')"));
	[Fact] public async Task Replace_Empty() => Assert.Equal("", await Scalar("SELECT REPLACE('', 'a', 'b')"));
	[Fact] public async Task Replace_AllChars() => Assert.Equal("xxxxx", await Scalar("SELECT REPLACE('aaaaa', 'a', 'x')"));

	// ---- REVERSE ----
	[Fact] public async Task Reverse_Basic() => Assert.Equal("olleh", await Scalar("SELECT REVERSE('hello')"));
	[Fact] public async Task Reverse_Palindrome() => Assert.Equal("racecar", await Scalar("SELECT REVERSE('racecar')"));
	[Fact] public async Task Reverse_Single() => Assert.Equal("a", await Scalar("SELECT REVERSE('a')"));
	[Fact] public async Task Reverse_Empty() => Assert.Equal("", await Scalar("SELECT REVERSE('')"));
	[Fact] public async Task Reverse_Spaces() => Assert.Equal("dlrow olleh", await Scalar("SELECT REVERSE('hello world')"));
	[Fact] public async Task Reverse_Numbers() => Assert.Equal("54321", await Scalar("SELECT REVERSE('12345')"));

	// ---- REPEAT ----
	[Fact] public async Task Repeat_Three() => Assert.Equal("abcabcabc", await Scalar("SELECT REPEAT('abc', 3)"));
	[Fact] public async Task Repeat_One() => Assert.Equal("abc", await Scalar("SELECT REPEAT('abc', 1)"));
	[Fact] public async Task Repeat_Zero() => Assert.Equal("", await Scalar("SELECT REPEAT('abc', 0)"));
	[Fact] public async Task Repeat_Single() => Assert.Equal("aaa", await Scalar("SELECT REPEAT('a', 3)"));
	[Fact] public async Task Repeat_Five() => Assert.Equal("xyxyxyxyxy", await Scalar("SELECT REPEAT('xy', 5)"));
	[Fact] public async Task Repeat_Empty() => Assert.Equal("", await Scalar("SELECT REPEAT('', 5)"));

	// ---- LPAD / RPAD ----
	[Fact] public async Task Lpad_Basic() => Assert.Equal("  abc", await Scalar("SELECT LPAD('abc', 5)"));
	[Fact] public async Task Lpad_Char() => Assert.Equal("00abc", await Scalar("SELECT LPAD('abc', 5, '0')"));
	[Fact] public async Task Lpad_NoChange() => Assert.Equal("hello", await Scalar("SELECT LPAD('hello', 5)"));
	[Fact] public async Task Lpad_Truncate() => Assert.Equal("hel", await Scalar("SELECT LPAD('hello', 3)"));
	[Fact] public async Task Lpad_One() => Assert.Equal("h", await Scalar("SELECT LPAD('hello', 1)"));
	[Fact] public async Task Rpad_Basic() => Assert.Equal("abc  ", await Scalar("SELECT RPAD('abc', 5)"));
	[Fact] public async Task Rpad_Char() => Assert.Equal("abc00", await Scalar("SELECT RPAD('abc', 5, '0')"));
	[Fact] public async Task Rpad_NoChange() => Assert.Equal("hello", await Scalar("SELECT RPAD('hello', 5)"));
	[Fact] public async Task Rpad_Truncate() => Assert.Equal("hel", await Scalar("SELECT RPAD('hello', 3)"));

	// ---- LEFT / RIGHT ----
	[Fact] public async Task Left_Basic() => Assert.Equal("hel", await Scalar("SELECT LEFT('hello', 3)"));
	[Fact] public async Task Left_Full() => Assert.Equal("hello", await Scalar("SELECT LEFT('hello', 5)"));
	[Fact] public async Task Left_One() => Assert.Equal("h", await Scalar("SELECT LEFT('hello', 1)"));
	[Fact] public async Task Left_Over() => Assert.Equal("hello", await Scalar("SELECT LEFT('hello', 10)"));
	[Fact] public async Task Right_Basic() => Assert.Equal("llo", await Scalar("SELECT RIGHT('hello', 3)"));
	[Fact] public async Task Right_Full() => Assert.Equal("hello", await Scalar("SELECT RIGHT('hello', 5)"));
	[Fact] public async Task Right_One() => Assert.Equal("o", await Scalar("SELECT RIGHT('hello', 1)"));
	[Fact] public async Task Right_Over() => Assert.Equal("hello", await Scalar("SELECT RIGHT('hello', 10)"));

	// ---- STRPOS / INSTR ----
	[Fact] public async Task Strpos_Found() => Assert.Equal("2", await Scalar("SELECT STRPOS('hello', 'ell')"));
	[Fact] public async Task Strpos_Start() => Assert.Equal("1", await Scalar("SELECT STRPOS('hello', 'hel')"));
	[Fact] public async Task Strpos_End() => Assert.Equal("4", await Scalar("SELECT STRPOS('hello', 'lo')"));
	[Fact] public async Task Strpos_NotFound() => Assert.Equal("0", await Scalar("SELECT STRPOS('hello', 'xyz')"));
	[Fact] public async Task Strpos_Single() => Assert.Equal("1", await Scalar("SELECT STRPOS('hello', 'h')"));
	[Fact] public async Task Strpos_Last() => Assert.Equal("5", await Scalar("SELECT STRPOS('hello', 'o')"));
	[Fact] public async Task Strpos_Empty() => Assert.Equal("1", await Scalar("SELECT STRPOS('hello', '')"));
	[Fact] public async Task Instr_Basic() => Assert.Equal("2", await Scalar("SELECT INSTR('hello', 'ell')"));

	// ---- SPLIT ----
	[Fact] public async Task Split_Comma() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(SPLIT('a,b,c', ','))"));
	[Fact] public async Task Split_Space() => Assert.Equal("2", await Scalar("SELECT ARRAY_LENGTH(SPLIT('hello world', ' '))"));
	[Fact] public async Task Split_Single() => Assert.Equal("1", await Scalar("SELECT ARRAY_LENGTH(SPLIT('hello', ','))"));
	[Fact] public async Task Split_Empty() => Assert.Equal("1", await Scalar("SELECT ARRAY_LENGTH(SPLIT('', ','))"));
	[Fact] public async Task Split_Multi() => Assert.Equal("4", await Scalar("SELECT ARRAY_LENGTH(SPLIT('a|b|c|d', '|'))"));
	[Fact(Skip = "Emulator limitation")] public async Task Split_FirstElem() => Assert.Equal("a", await Scalar("SELECT SPLIT('a,b,c', ',')[OFFSET(0)]"));
	[Fact(Skip = "Emulator limitation")] public async Task Split_LastElem() => Assert.Equal("c", await Scalar("SELECT SPLIT('a,b,c', ',')[OFFSET(2)]"));

	// ---- STARTS_WITH / ENDS_WITH ----
	[Fact] public async Task StartsWith_True2() => Assert.Equal("True", await Scalar("SELECT STARTS_WITH('bigquery', 'big')"));
	[Fact] public async Task StartsWith_False2() => Assert.Equal("False", await Scalar("SELECT STARTS_WITH('bigquery', 'small')"));
	[Fact] public async Task StartsWith_Full2() => Assert.Equal("True", await Scalar("SELECT STARTS_WITH('hello', 'hello')"));
	[Fact] public async Task StartsWith_Case() => Assert.Equal("False", await Scalar("SELECT STARTS_WITH('Hello', 'hello')"));
	[Fact] public async Task EndsWith_True2() => Assert.Equal("True", await Scalar("SELECT ENDS_WITH('bigquery', 'query')"));
	[Fact] public async Task EndsWith_False2() => Assert.Equal("False", await Scalar("SELECT ENDS_WITH('bigquery', 'table')"));
	[Fact] public async Task EndsWith_Full2() => Assert.Equal("True", await Scalar("SELECT ENDS_WITH('hello', 'hello')"));
	[Fact] public async Task EndsWith_Case() => Assert.Equal("False", await Scalar("SELECT ENDS_WITH('Hello', 'HELLO')"));

	// ---- INITCAP ----
	[Fact] public async Task Initcap_Basic() => Assert.Equal("Hello World", await Scalar("SELECT INITCAP('hello world')"));
	[Fact] public async Task Initcap_AllUpper() => Assert.Equal("Hello World", await Scalar("SELECT INITCAP('HELLO WORLD')"));
	[Fact] public async Task Initcap_Mixed() => Assert.Equal("Hello World", await Scalar("SELECT INITCAP('hElLo wOrLd')"));
	[Fact] public async Task Initcap_Single() => Assert.Equal("Hello", await Scalar("SELECT INITCAP('hello')"));
	[Fact] public async Task Initcap_OneChar() => Assert.Equal("A", await Scalar("SELECT INITCAP('a')"));
	[Fact] public async Task Initcap_Empty() => Assert.Equal("", await Scalar("SELECT INITCAP('')"));
	[Fact] public async Task Initcap_ThreeWords() => Assert.Equal("Foo Bar Baz", await Scalar("SELECT INITCAP('foo bar baz')"));

	// ---- SOUNDEX ----
	[Fact] public async Task Soundex_Robert() => Assert.Equal("R163", await Scalar("SELECT SOUNDEX('Robert')"));
	[Fact] public async Task Soundex_Rupert() => Assert.Equal("R163", await Scalar("SELECT SOUNDEX('Rupert')"));
	[Fact] public async Task Soundex_Hello() { var v = await Scalar("SELECT SOUNDEX('Hello')"); Assert.NotNull(v); Assert.Equal(4, v!.Length); }
	[Fact] public async Task Soundex_Empty() { var v = await Scalar("SELECT SOUNDEX('')"); Assert.NotNull(v); }

	// ---- ASCII / CHR ----
	[Fact] public async Task Ascii_A() => Assert.Equal("65", await Scalar("SELECT ASCII('A')"));
	[Fact] public async Task Ascii_a() => Assert.Equal("97", await Scalar("SELECT ASCII('a')"));
	[Fact] public async Task Ascii_Zero() => Assert.Equal("48", await Scalar("SELECT ASCII('0')"));
	[Fact] public async Task Ascii_Space() => Assert.Equal("32", await Scalar("SELECT ASCII(' ')"));
	[Fact] public async Task Chr_65() => Assert.Equal("A", await Scalar("SELECT CHR(65)"));
	[Fact] public async Task Chr_97() => Assert.Equal("a", await Scalar("SELECT CHR(97)"));
	[Fact] public async Task Chr_48() => Assert.Equal("0", await Scalar("SELECT CHR(48)"));
	[Fact] public async Task Chr_32() => Assert.Equal(" ", await Scalar("SELECT CHR(32)"));

	// ---- UNICODE / TO_CODE_POINTS ----
	[Fact] public async Task Unicode_A() => Assert.Equal("65", await Scalar("SELECT UNICODE('A')"));
	[Fact] public async Task Unicode_a() => Assert.Equal("97", await Scalar("SELECT UNICODE('a')"));
	[Fact] public async Task ToCodePoints_ABC() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(TO_CODE_POINTS('ABC'))"));
	[Fact] public async Task ToCodePoints_Single() => Assert.Equal("1", await Scalar("SELECT ARRAY_LENGTH(TO_CODE_POINTS('A'))"));

	// ---- SAFE_CONVERT_BYTES_TO_STRING ----
	[Fact] public async Task SafeConvertBytes_Basic() { var v = await Scalar("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'hello')"); Assert.Equal("hello", v); }

	// ---- Nested string operations ----
	[Fact] public async Task Nested_UpperReverse() => Assert.Equal("OLLEH", await Scalar("SELECT UPPER(REVERSE('hello'))"));
	[Fact] public async Task Nested_LowerTrim2() => Assert.Equal("hello", await Scalar("SELECT LOWER(TRIM('  HELLO  '))"));
	[Fact] public async Task Nested_ConcatUpper() => Assert.Equal("HELLO WORLD", await Scalar("SELECT UPPER(CONCAT('hello', ' ', 'world'))"));
	[Fact] public async Task Nested_SubstrReplace() => Assert.Equal("XXllo", await Scalar("SELECT REPLACE(SUBSTR('hello', 1), 'he', 'XX')"));
	[Fact] public async Task Nested_LengthConcat() => Assert.Equal("10", await Scalar("SELECT LENGTH(CONCAT('hello', 'world'))"));
	[Fact] public async Task Nested_TrimLength() => Assert.Equal("5", await Scalar("SELECT LENGTH(TRIM('  hello  '))"));
	[Fact] public async Task Nested_RepeatLength() => Assert.Equal("9", await Scalar("SELECT LENGTH(REPEAT('abc', 3))"));
	[Fact] public async Task Nested_UpperSubstr() => Assert.Equal("HEL", await Scalar("SELECT UPPER(SUBSTR('hello', 1, 3))"));
	[Fact] public async Task Nested_ReverseReverse() => Assert.Equal("hello", await Scalar("SELECT REVERSE(REVERSE('hello'))"));
	[Fact] public async Task Nested_ReplaceReplace() => Assert.Equal("xcxde", await Scalar("SELECT REPLACE(REPLACE('abade', 'a', 'x'), 'b', 'c')"));
}
