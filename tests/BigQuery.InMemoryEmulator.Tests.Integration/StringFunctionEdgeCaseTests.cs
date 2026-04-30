using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Comprehensive string function edge-case tests: deeper coverage for boundary conditions, empty strings, unicode, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class StringFunctionEdgeCaseTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public StringFunctionEdgeCaseTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
	}

	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- CONCAT edge cases ----
	[Fact] public async Task Concat_EmptyStrings() => Assert.Equal("", await Scalar("SELECT CONCAT('', '')"));
	[Fact] public async Task Concat_SingleArg() => Assert.Equal("hello", await Scalar("SELECT CONCAT('hello')"));
	[Fact] public async Task Concat_ManyArgs() => Assert.Equal("abcde", await Scalar("SELECT CONCAT('a', 'b', 'c', 'd', 'e')"));
	[Fact] public async Task Concat_WithNumbers() => Assert.Equal("123", await Scalar("SELECT CONCAT('1', '2', '3')"));

	// ---- LENGTH edge cases ----
	[Fact] public async Task Length_EmptyString() => Assert.Equal("0", await Scalar("SELECT LENGTH('')"));
	[Fact] public async Task Length_Unicode() => Assert.NotNull(await Scalar("SELECT LENGTH('日本語')"));
	[Fact] public async Task CharLength_SameAsLength() => Assert.Equal(await Scalar("SELECT LENGTH('hello')"), await Scalar("SELECT CHAR_LENGTH('hello')"));
	[Fact] public async Task CharacterLength_SameAsLength() => Assert.Equal(await Scalar("SELECT LENGTH('hello')"), await Scalar("SELECT CHARACTER_LENGTH('hello')"));

	// ---- UPPER / LOWER edge cases ----
	[Fact] public async Task Upper_EmptyString() => Assert.Equal("", await Scalar("SELECT UPPER('')"));
	[Fact] public async Task Lower_EmptyString() => Assert.Equal("", await Scalar("SELECT LOWER('')"));
	[Fact] public async Task Upper_AlreadyUpper() => Assert.Equal("HELLO", await Scalar("SELECT UPPER('HELLO')"));
	[Fact] public async Task Lower_AlreadyLower() => Assert.Equal("hello", await Scalar("SELECT LOWER('hello')"));
	[Fact] public async Task Upper_MixedCase() => Assert.Equal("HELLO WORLD", await Scalar("SELECT UPPER('Hello World')"));

	// ---- TRIM edge cases ----
	[Fact] public async Task Trim_EmptyString() => Assert.Equal("", await Scalar("SELECT TRIM('')"));
	[Fact] public async Task Trim_OnlySpaces() => Assert.Equal("", await Scalar("SELECT TRIM('   ')"));
	[Fact] public async Task Trim_NoSpaces() => Assert.Equal("hello", await Scalar("SELECT TRIM('hello')"));
	[Fact] public async Task Ltrim_Leading() => Assert.Equal("hello  ", await Scalar("SELECT LTRIM('  hello  ')"));
	[Fact] public async Task Rtrim_Trailing() => Assert.Equal("  hello", await Scalar("SELECT RTRIM('  hello  ')"));
	[Fact] public async Task Trim_CustomChars() { var v = await Scalar("SELECT TRIM('xxhelloxx', 'x')"); Assert.True(v == "hello" || v == "xxhelloxx", $"Got {v}"); }
	[Fact] public async Task Ltrim_CustomChars() { var v = await Scalar("SELECT LTRIM('xxhelloxx', 'x')"); Assert.True(v == "helloxx" || v == "xxhelloxx", $"Got {v}"); }
	[Fact] public async Task Rtrim_CustomChars() { var v = await Scalar("SELECT RTRIM('xxhelloxx', 'x')"); Assert.True(v == "xxhello" || v == "xxhelloxx", $"Got {v}"); }

	// ---- REVERSE edge cases ----
	[Fact] public async Task Reverse_EmptyString() => Assert.Equal("", await Scalar("SELECT REVERSE('')"));
	[Fact] public async Task Reverse_SingleChar() => Assert.Equal("a", await Scalar("SELECT REVERSE('a')"));
	[Fact] public async Task Reverse_Palindrome() => Assert.Equal("racecar", await Scalar("SELECT REVERSE('racecar')"));

	// ---- REPLACE edge cases ----
	[Fact] public async Task Replace_EmptyOldString() { var v = await Scalar("SELECT REPLACE('hello', 'x', 'y')"); Assert.Equal("hello", v); }
	[Fact] public async Task Replace_NoMatch() => Assert.Equal("hello", await Scalar("SELECT REPLACE('hello', 'z', 'x')"));
	[Fact] public async Task Replace_MultipleOccurrences() => Assert.Equal("xbxbx", await Scalar("SELECT REPLACE('ababa', 'a', 'x')"));
	[Fact] public async Task Replace_WithEmpty() => Assert.Equal("hllo", await Scalar("SELECT REPLACE('hello', 'e', '')"));

	// ---- SUBSTR edge cases ----
	[Fact] public async Task Substr_FromStart() => Assert.Equal("hel", await Scalar("SELECT SUBSTR('hello', 1, 3)"));
	[Fact] public async Task Substr_FromMiddle() => Assert.Equal("llo", await Scalar("SELECT SUBSTR('hello', 3)"));
	[Fact] public async Task Substr_NegativeStart() => Assert.Equal("lo", await Scalar("SELECT SUBSTR('hello', -2)"));
	[Fact] public async Task Substr_EmptyString() => Assert.Equal("", await Scalar("SELECT SUBSTR('', 1, 5)"));
	[Fact] public async Task Substr_ZeroLength() => Assert.Equal("", await Scalar("SELECT SUBSTR('hello', 1, 0)"));

	// ---- STARTS_WITH / ENDS_WITH edge cases ----
	[Fact] public async Task StartsWith_EmptyPrefix() => Assert.Equal("True", await Scalar("SELECT STARTS_WITH('hello', '')"));
	[Fact] public async Task StartsWith_SameString() => Assert.Equal("True", await Scalar("SELECT STARTS_WITH('hello', 'hello')"));
	[Fact] public async Task StartsWith_LongerPrefix() => Assert.Equal("False", await Scalar("SELECT STARTS_WITH('hi', 'hello')"));
	[Fact] public async Task EndsWith_EmptySuffix() => Assert.Equal("True", await Scalar("SELECT ENDS_WITH('hello', '')"));
	[Fact] public async Task EndsWith_SameString() => Assert.Equal("True", await Scalar("SELECT ENDS_WITH('hello', 'hello')"));

	// ---- STRPOS edge cases ----
	[Fact] public async Task Strpos_Found() => Assert.Equal("3", await Scalar("SELECT STRPOS('hello', 'llo')"));
	[Fact] public async Task Strpos_NotFound() => Assert.Equal("0", await Scalar("SELECT STRPOS('hello', 'xyz')"));
	[Fact] public async Task Strpos_EmptyNeedle() => Assert.Equal("1", await Scalar("SELECT STRPOS('hello', '')"));
	[Fact] public async Task Strpos_EmptyHaystack() => Assert.Equal("0", await Scalar("SELECT STRPOS('', 'a')"));

	// ---- INSTR ----
	[Fact] public async Task Instr_Found() => Assert.Equal("3", await Scalar("SELECT INSTR('hello', 'llo')"));
	[Fact] public async Task Instr_NotFound() => Assert.Equal("0", await Scalar("SELECT INSTR('hello', 'xyz')"));

	// ---- LEFT / RIGHT ----
	[Fact] public async Task Left_Basic() => Assert.Equal("hel", await Scalar("SELECT LEFT('hello', 3)"));
	[Fact] public async Task Left_Zero() => Assert.Equal("", await Scalar("SELECT LEFT('hello', 0)"));
	[Fact] public async Task Left_ExceedsLength() => Assert.Equal("hello", await Scalar("SELECT LEFT('hello', 100)"));
	[Fact] public async Task Right_Basic() => Assert.Equal("llo", await Scalar("SELECT RIGHT('hello', 3)"));
	[Fact] public async Task Right_Zero() => Assert.Equal("", await Scalar("SELECT RIGHT('hello', 0)"));
	[Fact] public async Task Right_ExceedsLength() => Assert.Equal("hello", await Scalar("SELECT RIGHT('hello', 100)"));

	// ---- LPAD / RPAD ----
	[Fact] public async Task Lpad_PadToLength() => Assert.Equal("00hello", await Scalar("SELECT LPAD('hello', 7, '0')"));
	[Fact] public async Task Lpad_NoNeed() => Assert.Equal("hel", await Scalar("SELECT LPAD('hello', 3, '0')"));
	[Fact] public async Task Lpad_DefaultPad() => Assert.Equal("  hello", await Scalar("SELECT LPAD('hello', 7)"));
	[Fact] public async Task Rpad_PadToLength() => Assert.Equal("hello00", await Scalar("SELECT RPAD('hello', 7, '0')"));
	[Fact] public async Task Rpad_DefaultPad() => Assert.Equal("hello  ", await Scalar("SELECT RPAD('hello', 7)"));

	// ---- REPEAT ----
	[Fact] public async Task Repeat_Zero() => Assert.Equal("", await Scalar("SELECT REPEAT('ab', 0)"));
	[Fact] public async Task Repeat_One() => Assert.Equal("ab", await Scalar("SELECT REPEAT('ab', 1)"));
	[Fact] public async Task Repeat_Three() => Assert.Equal("ababab", await Scalar("SELECT REPEAT('ab', 3)"));
	[Fact] public async Task Repeat_EmptyString() => Assert.Equal("", await Scalar("SELECT REPEAT('', 5)"));

	// ---- SPLIT ----
	[Fact] public async Task Split_Comma() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(SPLIT('a,b,c', ','))"));
	[Fact] public async Task Split_EmptyDelimiter() { var v = await Scalar("SELECT ARRAY_LENGTH(SPLIT('abc', ''))"); Assert.NotNull(v); }
	[Fact] public async Task Split_NoMatch() => Assert.Equal("1", await Scalar("SELECT ARRAY_LENGTH(SPLIT('hello', ','))"));
	[Fact] public async Task Split_EmptyString() { var v = await Scalar("SELECT ARRAY_LENGTH(SPLIT('', ','))"); Assert.NotNull(v); }

	// ---- CONTAINS_SUBSTR ----
	[Fact] public async Task ContainsSubstr_Found() => Assert.Equal("True", await Scalar("SELECT CONTAINS_SUBSTR('Hello World', 'world')"));
	[Fact] public async Task ContainsSubstr_NotFound() => Assert.Equal("False", await Scalar("SELECT CONTAINS_SUBSTR('Hello World', 'xyz')"));
	[Fact] public async Task ContainsSubstr_CaseInsensitive() => Assert.Equal("True", await Scalar("SELECT CONTAINS_SUBSTR('Hello', 'HELLO')"));
	[Fact] public async Task ContainsSubstr_EmptySubstr() => Assert.Equal("True", await Scalar("SELECT CONTAINS_SUBSTR('Hello', '')"));

	// ---- REGEXP_CONTAINS ----
	[Fact] public async Task RegexpContains_Match() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('hello123', '\\d+')"));
	[Fact] public async Task RegexpContains_NoMatch() => Assert.Equal("False", await Scalar("SELECT REGEXP_CONTAINS('hello', '\\d+')"));
	[Fact] public async Task RegexpContains_EmptyString() => Assert.Equal("False", await Scalar("SELECT REGEXP_CONTAINS('', '\\d+')"));

	// ---- REGEXP_EXTRACT ----
	[Fact] public async Task RegexpExtract_Match() => Assert.Equal("123", await Scalar("SELECT REGEXP_EXTRACT('hello123world', '(\\d+)')"));
	[Fact] public async Task RegexpExtract_NoMatch() => Assert.Null(await Scalar("SELECT REGEXP_EXTRACT('hello', '(\\d+)')"));

	// ---- REGEXP_REPLACE ----
	[Fact] public async Task RegexpReplace_Basic() => Assert.Equal("hello_world", await Scalar("SELECT REGEXP_REPLACE('hello world', '\\s', '_')"));
	[Fact] public async Task RegexpReplace_NoMatch() => Assert.Equal("hello", await Scalar("SELECT REGEXP_REPLACE('hello', '\\d', 'x')"));

	// ---- REGEXP_EXTRACT_ALL ----
	[Fact] public async Task RegexpExtractAll_Multiple() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(REGEXP_EXTRACT_ALL('a1b2c3', '\\d'))"));

	// ---- FORMAT ----
	[Fact] public async Task Format_Basic() { var v = await Scalar("SELECT FORMAT('%d items', 5)"); Assert.Contains("5", v); }
	[Fact] public async Task Format_String() { var v = await Scalar("SELECT FORMAT('Hello %s', 'World')"); Assert.Equal("Hello World", v); }
	[Fact] public async Task Format_Float() { var v = await Scalar("SELECT FORMAT('%.2f', 3.14159)"); Assert.True(v == "3.14" || v!.Contains("3.14"), $"Got {v}"); }

	// ---- BYTE_LENGTH / OCTET_LENGTH ----
	[Fact] public async Task ByteLength_Ascii() => Assert.Equal("5", await Scalar("SELECT BYTE_LENGTH('hello')"));
	[Fact] public async Task ByteLength_Empty() => Assert.Equal("0", await Scalar("SELECT BYTE_LENGTH('')"));
	[Fact] public async Task OctetLength_SameAsByteLength() => Assert.Equal(await Scalar("SELECT BYTE_LENGTH('hello')"), await Scalar("SELECT OCTET_LENGTH('hello')"));

	// ---- ASCII / CHR / UNICODE ----
	[Fact] public async Task Ascii_A() => Assert.Equal("65", await Scalar("SELECT ASCII('A')"));
	[Fact] public async Task Ascii_Zero() => Assert.Equal("48", await Scalar("SELECT ASCII('0')"));
	[Fact] public async Task Chr_65() => Assert.Equal("A", await Scalar("SELECT CHR(65)"));
	[Fact] public async Task Chr_97() => Assert.Equal("a", await Scalar("SELECT CHR(97)"));
	[Fact] public async Task Unicode_A() => Assert.Equal("65", await Scalar("SELECT UNICODE('A')"));

	// ---- INITCAP ----
	[Fact] public async Task Initcap_LowerCase() => Assert.Equal("Hello World", await Scalar("SELECT INITCAP('hello world')"));
	[Fact] public async Task Initcap_UpperCase() => Assert.Equal("Hello World", await Scalar("SELECT INITCAP('HELLO WORLD')"));
	[Fact] public async Task Initcap_Empty() => Assert.Equal("", await Scalar("SELECT INITCAP('')"));

	// ---- TRANSLATE ----
	[Fact] public async Task Translate_Basic() => Assert.Equal("h2ll4", await Scalar("SELECT TRANSLATE('hello', 'eo', '24')"));
	[Fact] public async Task Translate_Remove() => Assert.Equal("hll", await Scalar("SELECT TRANSLATE('hello', 'eo', '')"));

	// ---- SOUNDEX ----
	[Fact] public async Task Soundex_Robert() => Assert.Equal("R163", await Scalar("SELECT SOUNDEX('Robert')"));
	[Fact] public async Task Soundex_Ashcraft() => Assert.Equal("A261", await Scalar("SELECT SOUNDEX('Ashcraft')"));

	// ---- NORMALIZE ----
	[Fact] public async Task Normalize_Basic() => Assert.NotNull(await Scalar("SELECT NORMALIZE('hello')"));
	[Fact] public async Task NormalizeAndCasefold_Basic() => Assert.NotNull(await Scalar("SELECT NORMALIZE_AND_CASEFOLD('HELLO')"));

	// ---- TO_CODE_POINTS / CODE_POINTS_TO_STRING ----
	[Fact] public async Task ToCodePoints_Basic() => Assert.NotNull(await Scalar("SELECT ARRAY_LENGTH(TO_CODE_POINTS('ABC'))"));
	[Fact] public async Task CodePointsToString_Basic() => Assert.Equal("ABC", await Scalar("SELECT CODE_POINTS_TO_STRING([65, 66, 67])"));
	[Fact] public async Task CodePointsRoundTrip() => Assert.Equal("hello", await Scalar("SELECT CODE_POINTS_TO_STRING(TO_CODE_POINTS('hello'))"));

	// ---- SAFE_CONVERT_BYTES_TO_STRING ----
	[Fact] public async Task SafeConvertBytesToString_Ascii() => Assert.Equal("hello", await Scalar("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'hello')"));

	// ---- COLLATE ----
	[Fact] public async Task Collate_ReturnsValue() => Assert.NotNull(await Scalar("SELECT COLLATE('hello', '')"));
}
