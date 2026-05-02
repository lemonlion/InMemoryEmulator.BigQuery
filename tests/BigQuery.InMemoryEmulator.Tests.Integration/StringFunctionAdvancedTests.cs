using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for string functions: SOUNDEX, TRANSLATE, INITCAP, CODE_POINTS,
/// ASCII, CHR, UNICODE, BYTE_LENGTH, NORMALIZE, and edge cases.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class StringFunctionAdvancedTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public StringFunctionAdvancedTests(BigQuerySession session) => _session = session;

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

	private async Task<string?> S(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	private async Task<List<BigQueryRow>> Q(string sql)
	{
		var client = await _fixture.GetClientAsync();
		return (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
	}

	// ---- ASCII ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#ascii
	[Fact] public async Task Ascii_A() => Assert.Equal("65", await S("SELECT ASCII('A')"));
	[Fact] public async Task Ascii_a() => Assert.Equal("97", await S("SELECT ASCII('a')"));
	[Fact] public async Task Ascii_Space() => Assert.Equal("32", await S("SELECT ASCII(' ')"));
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#ascii
	//   "Returns 0 if the string is empty."
	[Fact] public async Task Ascii_EmptyString() => Assert.Equal("0", await S("SELECT ASCII('')"));
	[Fact] public async Task Ascii_MultiChar() => Assert.Equal("65", await S("SELECT ASCII('ABC')")); // first char
	[Fact] public async Task Ascii_Null() => Assert.Null(await S("SELECT ASCII(NULL)"));
	[Fact] public async Task Ascii_Zero() => Assert.Equal("48", await S("SELECT ASCII('0')"));
	[Fact] public async Task Ascii_Newline() => Assert.Equal("10", await S("SELECT ASCII('\\n')"));

	// ---- CHR ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#chr
	[Fact] public async Task Chr_65() => Assert.Equal("A", await S("SELECT CHR(65)"));
	[Fact] public async Task Chr_97() => Assert.Equal("a", await S("SELECT CHR(97)"));
	[Fact] public async Task Chr_32() => Assert.Equal(" ", await S("SELECT CHR(32)"));
	[Fact] public async Task Chr_Null() => Assert.Null(await S("SELECT CHR(NULL)"));

	// ---- UNICODE ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#unicode
	[Fact] public async Task Unicode_A() => Assert.Equal("65", await S("SELECT UNICODE('A')"));
	[Fact] public async Task Unicode_a() => Assert.Equal("97", await S("SELECT UNICODE('a')"));
	[Fact] public async Task Unicode_Null() => Assert.Null(await S("SELECT UNICODE(NULL)"));

	// ---- BYTE_LENGTH ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#byte_length
	[Fact] public async Task ByteLength_Ascii() => Assert.Equal("5", await S("SELECT BYTE_LENGTH('hello')"));
	[Fact] public async Task ByteLength_Empty() => Assert.Equal("0", await S("SELECT BYTE_LENGTH('')"));
	[Fact] public async Task ByteLength_Null() => Assert.Null(await S("SELECT BYTE_LENGTH(NULL)"));

	// ---- CHAR_LENGTH / CHARACTER_LENGTH ----
	[Fact] public async Task CharLength_Basic() => Assert.Equal("5", await S("SELECT CHAR_LENGTH('hello')"));
	[Fact] public async Task CharacterLength_Basic() => Assert.Equal("5", await S("SELECT CHARACTER_LENGTH('hello')"));
	[Fact] public async Task CharLength_Empty() => Assert.Equal("0", await S("SELECT CHAR_LENGTH('')"));

	// ---- OCTET_LENGTH (alias for BYTE_LENGTH) ----
	[Fact] public async Task OctetLength_Basic() => Assert.Equal("5", await S("SELECT OCTET_LENGTH('hello')"));

	// ---- INITCAP ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#initcap
	[Fact] public async Task Initcap_Basic() => Assert.Equal("Hello World", await S("SELECT INITCAP('hello world')"));
	[Fact] public async Task Initcap_AllCaps() => Assert.Equal("Hello World", await S("SELECT INITCAP('HELLO WORLD')"));
	[Fact] public async Task Initcap_Mixed() => Assert.Equal("Hello World", await S("SELECT INITCAP('hELLO wORLD')"));
	[Fact] public async Task Initcap_SingleWord() => Assert.Equal("Hello", await S("SELECT INITCAP('hello')"));
	[Fact] public async Task Initcap_Empty() => Assert.Equal("", await S("SELECT INITCAP('')"));
	[Fact] public async Task Initcap_Null() => Assert.Null(await S("SELECT INITCAP(NULL)"));
	[Fact] public async Task Initcap_WithNumbers() => Assert.Equal("Hello123world", await S("SELECT INITCAP('hello123world')"));
	[Fact] public async Task Initcap_MultipleSpaces() => Assert.Equal("A  B  C", await S("SELECT INITCAP('a  b  c')"));
	[Fact] public async Task Initcap_Hyphenated() => Assert.Equal("Mary-Jane", await S("SELECT INITCAP('mary-jane')"));

	// ---- SOUNDEX ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#soundex
	[Fact] public async Task Soundex_Robert() => Assert.Equal("R163", await S("SELECT SOUNDEX('Robert')"));
	[Fact] public async Task Soundex_Rupert() => Assert.Equal("R163", await S("SELECT SOUNDEX('Rupert')"));
	[Fact] public async Task Soundex_Ashcraft() => Assert.Equal("A261", await S("SELECT SOUNDEX('Ashcraft')"));
	[Fact] public async Task Soundex_Empty() => Assert.Equal("", await S("SELECT SOUNDEX('')"));
	[Fact] public async Task Soundex_Null() => Assert.Null(await S("SELECT SOUNDEX(NULL)"));
	[Fact] public async Task Soundex_SingleChar() => Assert.Equal("A000", await S("SELECT SOUNDEX('A')"));

	// ---- TRANSLATE ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#translate
	[Fact] public async Task Translate_Basic() => Assert.Equal("HeLLo", await S("SELECT TRANSLATE('Hello', 'elo', 'eLo')"));
	[Fact] public async Task Translate_Remove() => Assert.Equal("Hll", await S("SELECT TRANSLATE('Hello', 'eo', '')"));
	[Fact] public async Task Translate_Null() => Assert.Null(await S("SELECT TRANSLATE(NULL, 'a', 'b')"));

	// ---- TO_CODE_POINTS ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#to_code_points
	[Fact] public async Task ToCodePoints_Basic()
	{
		var v = await S("SELECT ARRAY_LENGTH(TO_CODE_POINTS('ABC'))");
		Assert.Equal("3", v);
	}
	[Fact] public async Task ToCodePoints_Empty()
	{
		var v = await S("SELECT ARRAY_LENGTH(TO_CODE_POINTS(''))");
		Assert.Equal("0", v);
	}
	[Fact] public async Task ToCodePoints_Null() => Assert.Null(await S("SELECT TO_CODE_POINTS(NULL)"));

	// ---- CODE_POINTS_TO_STRING ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#code_points_to_string
	[Fact] public async Task CodePointsToString_Basic() => Assert.Equal("ABC", await S("SELECT CODE_POINTS_TO_STRING([65, 66, 67])"));
	[Fact] public async Task CodePointsToString_Empty() => Assert.Equal("", await S("SELECT CODE_POINTS_TO_STRING([])"));

	// ---- SAFE_CONVERT_BYTES_TO_STRING ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#safe_convert_bytes_to_string
	[Fact] public async Task SafeConvertBytesToString_Valid() => Assert.Equal("hello", await S("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'hello')"));

	// ---- NORMALIZE / NORMALIZE_AND_CASEFOLD ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#normalize
	[Fact] public async Task Normalize_Basic() => Assert.Equal("hello", await S("SELECT NORMALIZE('hello')"));
	[Fact] public async Task Normalize_Null() => Assert.Null(await S("SELECT NORMALIZE(NULL)"));
	[Fact] public async Task NormalizeAndCasefold_Basic() => Assert.Equal("hello", await S("SELECT NORMALIZE_AND_CASEFOLD('Hello')"));
	[Fact] public async Task NormalizeAndCasefold_Null() => Assert.Null(await S("SELECT NORMALIZE_AND_CASEFOLD(NULL)"));

	// ---- SPLIT edge cases ----
	[Fact] public async Task Split_EmptyString()
	{
		var v = await S("SELECT ARRAY_LENGTH(SPLIT('', ','))");
		Assert.Equal("1", v);
	}
	[Fact] public async Task Split_NoDelimiter()
	{
		var v = await S("SELECT ARRAY_LENGTH(SPLIT('hello', ','))");
		Assert.Equal("1", v);
	}
	[Fact] public async Task Split_MultipleDelimiters()
	{
		var v = await S("SELECT ARRAY_LENGTH(SPLIT('a,b,c,d', ','))");
		Assert.Equal("4", v);
	}
	[Fact] public async Task Split_DefaultDelimiter()
	{
		var v = await S("SELECT ARRAY_LENGTH(SPLIT('a,b,c'))");
		Assert.Equal("3", v);
	}

	// ---- REGEXP_EXTRACT_ALL ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_extract_all
	[Fact] public async Task RegexpExtractAll_Basic()
	{
		var v = await S("SELECT ARRAY_LENGTH(REGEXP_EXTRACT_ALL('hello world 123', '\\d+'))");
		Assert.Equal("1", v);
	}
	[Fact] public async Task RegexpExtractAll_NoMatch()
	{
		var v = await S("SELECT ARRAY_LENGTH(REGEXP_EXTRACT_ALL('hello', '\\d+'))");
		Assert.Equal("0", v);
	}

	// ---- REGEXP_INSTR ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_instr
	[Fact] public async Task RegexpInstr_Found() => Assert.Equal("4", await S("SELECT REGEXP_INSTR('abc123def', '\\d+')"));
	[Fact] public async Task RegexpInstr_NotFound() => Assert.Equal("0", await S("SELECT REGEXP_INSTR('hello', '\\d+')"));

	// ---- INSTR extra cases ----
	[Fact] public async Task Instr_NotFound() => Assert.Equal("0", await S("SELECT INSTR('hello', 'xyz')"));
	[Fact] public async Task Instr_EmptyNeedle() => Assert.Equal("1", await S("SELECT INSTR('hello', '')"));

	// ---- CONTAINS_SUBSTR edge ----
	[Fact] public async Task ContainsSubstr_CaseInsensitive()
	{
		var v = await S("SELECT CONTAINS_SUBSTR('Hello World', 'hello')");
		Assert.Equal("True", v);
	}
	[Fact] public async Task ContainsSubstr_EmptyNeedle()
	{
		var v = await S("SELECT CONTAINS_SUBSTR('Hello', '')");
		Assert.Equal("True", v);
	}

	// ---- STARTS_WITH / ENDS_WITH ----
	[Fact] public async Task StartsWith_True() => Assert.Equal("True", await S("SELECT STARTS_WITH('hello world', 'hello')"));
	[Fact] public async Task StartsWith_False() => Assert.Equal("False", await S("SELECT STARTS_WITH('hello world', 'world')"));
	[Fact] public async Task StartsWith_Empty() => Assert.Equal("True", await S("SELECT STARTS_WITH('hello', '')"));
	[Fact] public async Task EndsWith_True() => Assert.Equal("True", await S("SELECT ENDS_WITH('hello world', 'world')"));
	[Fact] public async Task EndsWith_False() => Assert.Equal("False", await S("SELECT ENDS_WITH('hello world', 'hello')"));
	[Fact] public async Task EndsWith_Empty() => Assert.Equal("True", await S("SELECT ENDS_WITH('hello', '')"));

	// ---- FORMAT function ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#format_string
	[Fact] public async Task Format_String() => Assert.Equal("Hello World", await S("SELECT FORMAT('%s %s', 'Hello', 'World')"));
	[Fact] public async Task Format_Integer() => Assert.Equal("42", await S("SELECT FORMAT('%d', 42)"));
	[Fact] public async Task Format_Float() => Assert.Equal("3.14", await S("SELECT FORMAT('%.2f', 3.14159)"));
	[Fact] public async Task Format_Percent() => Assert.Equal("100%", await S("SELECT FORMAT('%d%%', 100)"));

	// ---- REVERSE extras ----
	[Fact] public async Task Reverse_Palindrome() => Assert.Equal("madam", await S("SELECT REVERSE('madam')"));
	[Fact] public async Task Reverse_Empty() => Assert.Equal("", await S("SELECT REVERSE('')"));
	[Fact] public async Task Reverse_SingleChar() => Assert.Equal("a", await S("SELECT REVERSE('a')"));

	// ---- REPLACE extras ----
	[Fact] public async Task Replace_NotFound() => Assert.Equal("hello", await S("SELECT REPLACE('hello', 'xyz', 'abc')"));
	[Fact] public async Task Replace_Empty() => Assert.Equal("hello", await S("SELECT REPLACE('hello', '', 'x')"));
	[Fact] public async Task Replace_Multiple() => Assert.Equal("hxllo", await S("SELECT REPLACE('hello', 'e', 'x')"));
	[Fact] public async Task Replace_CaseSensitive() => Assert.Equal("hello", await S("SELECT REPLACE('hello', 'H', 'x')"));

	// ---- TRIM variations ----
	[Fact] public async Task Trim_Custom() => Assert.Equal("hello", await S("SELECT TRIM('xxhelloxx', 'x')"));
	[Fact] public async Task Ltrim_Custom() => Assert.Equal("helloxx", await S("SELECT LTRIM('xxhelloxx', 'x')"));
	[Fact] public async Task Rtrim_Custom() => Assert.Equal("xxhello", await S("SELECT RTRIM('xxhelloxx', 'x')"));

	// ---- LPAD/RPAD ----
	[Fact] public async Task Lpad_Shorter() => Assert.Equal("00042", await S("SELECT LPAD('42', 5, '0')"));
	[Fact] public async Task Lpad_LongerInput() => Assert.Equal("hel", await S("SELECT LPAD('hello', 3, '0')"));
	[Fact] public async Task Rpad_Shorter() => Assert.Equal("42000", await S("SELECT RPAD('42', 5, '0')"));
	[Fact] public async Task Rpad_LongerInput() => Assert.Equal("hel", await S("SELECT RPAD('hello', 3, '0')"));

	// ---- LEFT/RIGHT ----
	[Fact] public async Task Left_Basic() => Assert.Equal("hel", await S("SELECT LEFT('hello', 3)"));
	[Fact] public async Task Left_Full() => Assert.Equal("hello", await S("SELECT LEFT('hello', 10)"));
	[Fact] public async Task Left_Zero() => Assert.Equal("", await S("SELECT LEFT('hello', 0)"));
	[Fact] public async Task Right_Basic() => Assert.Equal("llo", await S("SELECT RIGHT('hello', 3)"));
	[Fact] public async Task Right_Full() => Assert.Equal("hello", await S("SELECT RIGHT('hello', 10)"));

	// ---- CONCAT with many args ----
	[Fact] public async Task Concat_MultipleArgs()
	{
		var v = await S("SELECT CONCAT('a', 'b', 'c', 'd', 'e')");
		Assert.Equal("abcde", v);
	}
	[Fact] public async Task Concat_WithNull() => Assert.Null(await S("SELECT CONCAT('a', NULL, 'c')"));
	[Fact] public async Task Concat_EmptyStrings() => Assert.Equal("", await S("SELECT CONCAT('', '', '')"));

	// ---- REPEAT ----
	[Fact] public async Task Repeat_Basic() => Assert.Equal("abcabcabc", await S("SELECT REPEAT('abc', 3)"));
	[Fact] public async Task Repeat_Zero() => Assert.Equal("", await S("SELECT REPEAT('abc', 0)"));
	[Fact] public async Task Repeat_Null() => Assert.Null(await S("SELECT REPEAT(NULL, 3)"));

	// ---- STRPOS ----
	[Fact] public async Task Strpos_Found() => Assert.Equal("3", await S("SELECT STRPOS('hello world', 'llo')"));
	[Fact] public async Task Strpos_NotFound() => Assert.Equal("0", await S("SELECT STRPOS('hello', 'xyz')"));
	[Fact] public async Task Strpos_Empty() => Assert.Equal("1", await S("SELECT STRPOS('hello', '')"));

	// ---- SUBSTR ----
	[Fact] public async Task Substr_NegativeStart() => Assert.Equal("lo", await S("SELECT SUBSTR('hello', -2)"));
	[Fact] public async Task Substr_WithLength() => Assert.Equal("ell", await S("SELECT SUBSTR('hello', 2, 3)"));
	[Fact] public async Task Substr_BeyondEnd() => Assert.Equal("lo", await S("SELECT SUBSTR('hello', 4, 100)"));

	// ---- UPPER/LOWER extras ----
	[Fact] public async Task Upper_NumbersUnchanged() => Assert.Equal("ABC123", await S("SELECT UPPER('abc123')"));
	[Fact] public async Task Lower_NumbersUnchanged() => Assert.Equal("abc123", await S("SELECT LOWER('ABC123')"));
}
