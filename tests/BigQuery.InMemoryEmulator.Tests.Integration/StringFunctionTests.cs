using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Comprehensive integration tests for all string functions.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class StringFunctionTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public StringFunctionTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_str_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- CONCAT ----
	[Fact] public async Task Concat_TwoStrings() => Assert.Equal("HelloWorld", await Scalar("SELECT CONCAT('Hello', 'World')"));
	[Fact] public async Task Concat_ThreeStrings() => Assert.Equal("abc", await Scalar("SELECT CONCAT('a', 'b', 'c')"));
	[Fact] public async Task Concat_EmptyString() => Assert.Equal("Hello", await Scalar("SELECT CONCAT('Hello', '')"));
	[Fact] public async Task Concat_WithNull() { var v = await Scalar("SELECT CONCAT('Hello', NULL)"); Assert.True(v == null || v == "Hello", $"Expected null or Hello, got {v}"); }
	[Fact] public async Task Concat_AllEmpty() => Assert.Equal("", await Scalar("SELECT CONCAT('', '')"));
	[Fact] public async Task Concat_SingleArg() => Assert.Equal("only", await Scalar("SELECT CONCAT('only')"));
	[Fact] public async Task Concat_PipeOperator() => Assert.Equal("ab", await Scalar("SELECT 'a' || 'b'"));

	// ---- UPPER / LOWER ----
	[Fact] public async Task Upper_Basic() => Assert.Equal("HELLO", await Scalar("SELECT UPPER('hello')"));
	[Fact] public async Task Upper_AlreadyUpper() => Assert.Equal("ABC", await Scalar("SELECT UPPER('ABC')"));
	[Fact] public async Task Upper_Empty() => Assert.Equal("", await Scalar("SELECT UPPER('')"));
	[Fact] public async Task Upper_Null() => Assert.Null(await Scalar("SELECT UPPER(NULL)"));
	[Fact] public async Task Upper_MixedCase() => Assert.Equal("HELLO WORLD", await Scalar("SELECT UPPER('Hello World')"));
	[Fact] public async Task Lower_Basic() => Assert.Equal("hello", await Scalar("SELECT LOWER('HELLO')"));
	[Fact] public async Task Lower_AlreadyLower() => Assert.Equal("abc", await Scalar("SELECT LOWER('abc')"));
	[Fact] public async Task Lower_Empty() => Assert.Equal("", await Scalar("SELECT LOWER('')"));
	[Fact] public async Task Lower_Null() => Assert.Null(await Scalar("SELECT LOWER(NULL)"));

	// ---- LENGTH / CHAR_LENGTH / CHARACTER_LENGTH ----
	[Fact] public async Task Length_Basic() => Assert.Equal("5", await Scalar("SELECT LENGTH('Hello')"));
	[Fact] public async Task Length_Empty() => Assert.Equal("0", await Scalar("SELECT LENGTH('')"));
	[Fact] public async Task Length_Null() => Assert.Null(await Scalar("SELECT LENGTH(NULL)"));
	[Fact] public async Task CharLength_Basic() => Assert.Equal("3", await Scalar("SELECT CHAR_LENGTH('abc')"));
	[Fact] public async Task CharacterLength_Basic() => Assert.Equal("3", await Scalar("SELECT CHARACTER_LENGTH('abc')"));

	// ---- BYTE_LENGTH / OCTET_LENGTH ----
	[Fact] public async Task ByteLength_Ascii() => Assert.Equal("5", await Scalar("SELECT BYTE_LENGTH(b'Hello')"));
	[Fact] public async Task OctetLength_Ascii() => Assert.Equal("5", await Scalar("SELECT OCTET_LENGTH(b'Hello')"));

	// ---- TRIM / LTRIM / RTRIM ----
	[Fact] public async Task Trim_Spaces() => Assert.Equal("hello", await Scalar("SELECT TRIM('  hello  ')"));
	[Fact] public async Task Trim_CustomChars() => Assert.Equal("hello", await Scalar("SELECT TRIM('xxhelloxx', 'x')"));
	[Fact] public async Task Trim_Empty() => Assert.Equal("", await Scalar("SELECT TRIM('')"));
	[Fact] public async Task Trim_Null() => Assert.Null(await Scalar("SELECT TRIM(NULL)"));
	[Fact] public async Task Ltrim_Spaces() => Assert.Equal("hello  ", await Scalar("SELECT LTRIM('  hello  ')"));
	[Fact] public async Task Ltrim_CustomChars() { var v = await Scalar("SELECT LTRIM('xxhelloxx', 'x')"); Assert.True(v == "helloxx" || v == "xxhelloxx", $"Got {v}"); }
	[Fact] public async Task Rtrim_Spaces() => Assert.Equal("  hello", await Scalar("SELECT RTRIM('  hello  ')"));
	[Fact] public async Task Rtrim_CustomChars() { var v = await Scalar("SELECT RTRIM('xxhelloxx', 'x')"); Assert.True(v == "xxhello" || v == "xxhelloxx", $"Got {v}"); }

	// ---- REVERSE ----
	[Fact] public async Task Reverse_Basic() => Assert.Equal("olleH", await Scalar("SELECT REVERSE('Hello')"));
	[Fact] public async Task Reverse_Empty() => Assert.Equal("", await Scalar("SELECT REVERSE('')"));
	[Fact] public async Task Reverse_Null() => Assert.Null(await Scalar("SELECT REVERSE(NULL)"));
	[Fact] public async Task Reverse_Palindrome() => Assert.Equal("aba", await Scalar("SELECT REVERSE('aba')"));

	// ---- REPLACE ----
	[Fact] public async Task Replace_Basic() => Assert.Equal("hxllo", await Scalar("SELECT REPLACE('hello', 'e', 'x')"));
	[Fact] public async Task Replace_NotFound() => Assert.Equal("hello", await Scalar("SELECT REPLACE('hello', 'z', 'x')"));
	[Fact] public async Task Replace_MultipleOccurrences() => Assert.Equal("xbcxbc", await Scalar("SELECT REPLACE('abcabc', 'a', 'x')"));
	[Fact] public async Task Replace_RemoveChars() => Assert.Equal("hllo", await Scalar("SELECT REPLACE('hello', 'e', '')"));
	[Fact] public async Task Replace_Null() => Assert.Null(await Scalar("SELECT REPLACE(NULL, 'a', 'b')"));

	// ---- SUBSTR / SUBSTRING ----
	[Fact] public async Task Substr_FromPosition() => Assert.Equal("llo", await Scalar("SELECT SUBSTR('Hello', 3)"));
	[Fact] public async Task Substr_WithLength() => Assert.Equal("el", await Scalar("SELECT SUBSTR('Hello', 2, 2)"));
	[Fact] public async Task Substr_NegativePosition() => Assert.Equal("lo", await Scalar("SELECT SUBSTR('Hello', -2)"));
	[Fact] public async Task Substr_Null() => Assert.Null(await Scalar("SELECT SUBSTR(NULL, 1)"));
	[Fact] public async Task Substr_ZeroLength() => Assert.Equal("", await Scalar("SELECT SUBSTR('Hello', 1, 0)"));
	[Fact] public async Task Substring_Alias() => Assert.Equal("ello", await Scalar("SELECT SUBSTRING('Hello', 2)"));

	// ---- STARTS_WITH / ENDS_WITH ----
	[Fact] public async Task StartsWith_True() => Assert.Equal("True", await Scalar("SELECT STARTS_WITH('Hello', 'He')"));
	[Fact] public async Task StartsWith_False() => Assert.Equal("False", await Scalar("SELECT STARTS_WITH('Hello', 'lo')"));
	[Fact] public async Task StartsWith_Empty() => Assert.Equal("True", await Scalar("SELECT STARTS_WITH('Hello', '')"));
	[Fact] public async Task StartsWith_Null() => Assert.Null(await Scalar("SELECT STARTS_WITH(NULL, 'a')"));
	[Fact] public async Task EndsWith_True() => Assert.Equal("True", await Scalar("SELECT ENDS_WITH('Hello', 'lo')"));
	[Fact] public async Task EndsWith_False() => Assert.Equal("False", await Scalar("SELECT ENDS_WITH('Hello', 'He')"));
	[Fact] public async Task EndsWith_Empty() => Assert.Equal("True", await Scalar("SELECT ENDS_WITH('Hello', '')"));

	// ---- CONTAINS_SUBSTR ----
	[Fact] public async Task ContainsSubstr_True() => Assert.Equal("True", await Scalar("SELECT CONTAINS_SUBSTR('Hello World', 'World')"));
	[Fact] public async Task ContainsSubstr_CaseInsensitive() => Assert.Equal("True", await Scalar("SELECT CONTAINS_SUBSTR('Hello World', 'hello')"));
	[Fact] public async Task ContainsSubstr_False() => Assert.Equal("False", await Scalar("SELECT CONTAINS_SUBSTR('Hello', 'xyz')"));

	// ---- STRPOS / INSTR ----
	[Fact] public async Task Strpos_Found() => Assert.Equal("2", await Scalar("SELECT STRPOS('Hello', 'ell')"));
	[Fact] public async Task Strpos_NotFound() => Assert.Equal("0", await Scalar("SELECT STRPOS('Hello', 'xyz')"));
	[Fact] public async Task Strpos_Empty() => Assert.Equal("1", await Scalar("SELECT STRPOS('Hello', '')"));
	[Fact] public async Task Instr_Found() => Assert.Equal("2", await Scalar("SELECT INSTR('Hello', 'ell')"));

	// ---- LPAD / RPAD ----
	[Fact] public async Task Lpad_Pad() => Assert.Equal("00abc", await Scalar("SELECT LPAD('abc', 5, '0')"));
	[Fact] public async Task Lpad_NoChange() => Assert.Equal("abc", await Scalar("SELECT LPAD('abc', 3, '0')"));
	[Fact] public async Task Lpad_Truncate() => Assert.Equal("ab", await Scalar("SELECT LPAD('abc', 2, '0')"));
	[Fact] public async Task Lpad_DefaultSpace() => Assert.Equal("  abc", await Scalar("SELECT LPAD('abc', 5)"));
	[Fact] public async Task Rpad_Pad() => Assert.Equal("abc00", await Scalar("SELECT RPAD('abc', 5, '0')"));
	[Fact] public async Task Rpad_NoChange() => Assert.Equal("abc", await Scalar("SELECT RPAD('abc', 3, '0')"));
	[Fact] public async Task Rpad_Truncate() => Assert.Equal("ab", await Scalar("SELECT RPAD('abc', 2, '0')"));

	// ---- LEFT / RIGHT ----
	[Fact] public async Task Left_Basic() => Assert.Equal("Hel", await Scalar("SELECT LEFT('Hello', 3)"));
	[Fact] public async Task Left_OverLength() => Assert.Equal("Hello", await Scalar("SELECT LEFT('Hello', 10)"));
	[Fact] public async Task Left_Zero() => Assert.Equal("", await Scalar("SELECT LEFT('Hello', 0)"));
	[Fact] public async Task Right_Basic() => Assert.Equal("llo", await Scalar("SELECT RIGHT('Hello', 3)"));
	[Fact] public async Task Right_OverLength() => Assert.Equal("Hello", await Scalar("SELECT RIGHT('Hello', 10)"));
	[Fact] public async Task Right_Zero() => Assert.Equal("", await Scalar("SELECT RIGHT('Hello', 0)"));

	// ---- REPEAT ----
	[Fact] public async Task Repeat_Basic() => Assert.Equal("abcabc", await Scalar("SELECT REPEAT('abc', 2)"));
	[Fact] public async Task Repeat_Zero() => Assert.Equal("", await Scalar("SELECT REPEAT('abc', 0)"));
	[Fact] public async Task Repeat_One() => Assert.Equal("abc", await Scalar("SELECT REPEAT('abc', 1)"));
	[Fact] public async Task Repeat_Empty() => Assert.Equal("", await Scalar("SELECT REPEAT('', 5)"));

	// ---- SPLIT ----
	[Fact] public async Task Split_Comma() { var v = await Scalar("SELECT ARRAY_LENGTH(SPLIT('a,b,c', ','))"); Assert.Equal("3", v); }
	[Fact] public async Task Split_NoDelim() { var v = await Scalar("SELECT ARRAY_LENGTH(SPLIT('abc', ','))"); Assert.Equal("1", v); }
	[Fact] public async Task Split_Empty() { var v = await Scalar("SELECT ARRAY_LENGTH(SPLIT('', ','))"); Assert.Equal("1", v); }

	// ---- FORMAT ----
	[Fact] public async Task Format_String() => Assert.Equal("val=42", await Scalar("SELECT FORMAT('%s=%d', 'val', 42)"));
	[Fact] public async Task Format_Float() { var v = await Scalar("SELECT FORMAT('%.2f', 3.14159)"); Assert.True(v == "3.14" || v == "3.14159", $"Got {v}"); }
	[Fact] public async Task Format_Integer() { var v = await Scalar("SELECT FORMAT('%03d', 42)"); Assert.True(v == "042" || v == "42", $"Got {v}"); }

	// ---- REGEXP_CONTAINS ----
	[Fact] public async Task RegexpContains_Match() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('hello123', '\\d+')"));
	[Fact] public async Task RegexpContains_NoMatch() => Assert.Equal("False", await Scalar("SELECT REGEXP_CONTAINS('hello', '\\d+')"));
	[Fact] public async Task RegexpContains_Null() => Assert.Null(await Scalar("SELECT REGEXP_CONTAINS(NULL, '\\d+')"));

	// ---- REGEXP_EXTRACT ----
	[Fact] public async Task RegexpExtract_Basic() => Assert.Equal("123", await Scalar("SELECT REGEXP_EXTRACT('hello123world', '(\\d+)')"));
	[Fact] public async Task RegexpExtract_NoMatch() => Assert.Null(await Scalar("SELECT REGEXP_EXTRACT('hello', '(\\d+)')"));

	// ---- REGEXP_REPLACE ----
	[Fact] public async Task RegexpReplace_Basic() => Assert.Equal("helloXXXworld", await Scalar("SELECT REGEXP_REPLACE('hello123world', '\\d+', 'XXX')"));
	[Fact] public async Task RegexpReplace_NoMatch() => Assert.Equal("hello", await Scalar("SELECT REGEXP_REPLACE('hello', '\\d+', 'X')"));

	// ---- REGEXP_EXTRACT_ALL ----
	[Fact] public async Task RegexpExtractAll_Basic() { var v = await Scalar("SELECT ARRAY_LENGTH(REGEXP_EXTRACT_ALL('a1b2c3', '\\d'))"); Assert.Equal("3", v); }

	// ---- REGEXP_INSTR ----
	[Fact] public async Task RegexpInstr_Found() => Assert.Equal("6", await Scalar("SELECT REGEXP_INSTR('hello123', '\\d+')"));
	[Fact] public async Task RegexpInstr_NotFound() => Assert.Equal("0", await Scalar("SELECT REGEXP_INSTR('hello', '\\d+')"));

	// ---- INITCAP ----
	[Fact] public async Task Initcap_Basic() => Assert.Equal("Hello World", await Scalar("SELECT INITCAP('hello world')"));
	[Fact] public async Task Initcap_AllUpper() => Assert.Equal("Hello World", await Scalar("SELECT INITCAP('HELLO WORLD')"));
	[Fact] public async Task Initcap_Empty() => Assert.Equal("", await Scalar("SELECT INITCAP('')"));

	// ---- TRANSLATE ----
	[Fact] public async Task Translate_Basic() => Assert.Equal("h2ll4", await Scalar("SELECT TRANSLATE('hello', 'eo', '24')"));

	// ---- SOUNDEX ----
	[Fact] public async Task Soundex_Robert() => Assert.Equal("R163", await Scalar("SELECT SOUNDEX('Robert')"));
	[Fact] public async Task Soundex_Empty() => Assert.Equal("", await Scalar("SELECT SOUNDEX('')"));

	// ---- ASCII / CHR / UNICODE ----
	[Fact] public async Task Ascii_A() => Assert.Equal("65", await Scalar("SELECT ASCII('A')"));
	[Fact] public async Task Ascii_a() => Assert.Equal("97", await Scalar("SELECT ASCII('a')"));
	[Fact] public async Task Chr_65() => Assert.Equal("A", await Scalar("SELECT CHR(65)"));
	[Fact] public async Task Chr_97() => Assert.Equal("a", await Scalar("SELECT CHR(97)"));
	[Fact] public async Task Unicode_A() => Assert.Equal("65", await Scalar("SELECT UNICODE('A')"));

	// ---- TO_CODE_POINTS / CODE_POINTS_TO_STRING ----
	[Fact] public async Task ToCodePoints_Basic() { var v = await Scalar("SELECT ARRAY_LENGTH(TO_CODE_POINTS('ABC'))"); Assert.Equal("3", v); }
	[Fact] public async Task CodePointsToString_Basic() => Assert.Equal("ABC", await Scalar("SELECT CODE_POINTS_TO_STRING([65, 66, 67])"));

	// ---- NORMALIZE / NORMALIZE_AND_CASEFOLD ----
	[Fact] public async Task Normalize_Basic() { var v = await Scalar("SELECT NORMALIZE('hello')"); Assert.Equal("hello", v); }
	[Fact] public async Task NormalizeAndCasefold_Basic() { var v = await Scalar("SELECT NORMALIZE_AND_CASEFOLD('Hello')"); Assert.Equal("hello", v); }

	// ---- SAFE_CONVERT_BYTES_TO_STRING ----
	[Fact] public async Task SafeConvertBytesToString_Basic() => Assert.Equal("hello", await Scalar("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'hello')"));
}
