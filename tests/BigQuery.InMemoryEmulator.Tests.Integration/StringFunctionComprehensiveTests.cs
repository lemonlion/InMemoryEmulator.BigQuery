using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Comprehensive tests for string functions beyond basic coverage.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class StringFunctionComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public StringFunctionComprehensiveTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql, parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }

	// ---- LENGTH ----
	[Fact] public async Task Length_Empty() => Assert.Equal("0", await S("SELECT LENGTH('')"));
	[Fact] public async Task Length_Basic() => Assert.Equal("5", await S("SELECT LENGTH('hello')"));
	[Fact] public async Task Length_Unicode() => Assert.Equal("4", await S("SELECT LENGTH('café')")); // café = 4 characters
	[Fact] public async Task Length_Null() => Assert.Null(await S("SELECT LENGTH(CAST(NULL AS STRING))"));

	// ---- CHAR_LENGTH / CHARACTER_LENGTH ----
	[Fact] public async Task CharLength_Basic() => Assert.Equal("5", await S("SELECT CHAR_LENGTH('hello')"));

	// ---- CONCAT ----
	[Fact] public async Task Concat_Two() => Assert.Equal("ab", await S("SELECT CONCAT('a','b')"));
	[Fact] public async Task Concat_Three() => Assert.Equal("abc", await S("SELECT CONCAT('a','b','c')"));
	[Fact] public async Task Concat_WithNull() => Assert.Null(await S("SELECT CONCAT('a', NULL, 'b')"));
	[Fact] public async Task Concat_Empty() => Assert.Equal("ab", await S("SELECT CONCAT('a','','b')"));

	// ---- UPPER / LOWER ----
	[Fact] public async Task Upper_Basic() => Assert.Equal("HELLO", await S("SELECT UPPER('hello')"));
	[Fact] public async Task Lower_Basic() => Assert.Equal("hello", await S("SELECT LOWER('HELLO')"));
	[Fact] public async Task Upper_Mixed() => Assert.Equal("HELLO WORLD", await S("SELECT UPPER('Hello World')"));
	[Fact] public async Task Lower_Null() => Assert.Null(await S("SELECT LOWER(CAST(NULL AS STRING))"));

	// ---- TRIM / LTRIM / RTRIM ----
	[Fact] public async Task Trim_Spaces() => Assert.Equal("hello", await S("SELECT TRIM('  hello  ')"));
	[Fact] public async Task Ltrim_Spaces() => Assert.Equal("hello  ", await S("SELECT LTRIM('  hello  ')"));
	[Fact] public async Task Rtrim_Spaces() => Assert.Equal("  hello", await S("SELECT RTRIM('  hello  ')"));
	[Fact] public async Task Trim_Custom() => Assert.Equal("hello", await S("SELECT TRIM('xxhelloxx', 'x')"));
	[Fact] public async Task Ltrim_Custom() => Assert.Equal("helloxx", await S("SELECT LTRIM('xxhelloxx', 'x')"));
	[Fact] public async Task Rtrim_Custom() => Assert.Equal("xxhello", await S("SELECT RTRIM('xxhelloxx', 'x')"));

	// ---- SUBSTR / SUBSTRING ----
	[Fact] public async Task Substr_Basic() => Assert.Equal("llo", await S("SELECT SUBSTR('hello', 3)"));
	[Fact] public async Task Substr_WithLength() => Assert.Equal("el", await S("SELECT SUBSTR('hello', 2, 2)"));
	[Fact] public async Task Substr_Negative() => Assert.Equal("lo", await S("SELECT SUBSTR('hello', -2)"));
	[Fact] public async Task Substring_Alias() => Assert.Equal("llo", await S("SELECT SUBSTRING('hello', 3)"));

	// ---- REPLACE ----
	[Fact] public async Task Replace_Basic() => Assert.Equal("hello world", await S("SELECT REPLACE('hello earth', 'earth', 'world')"));
	[Fact] public async Task Replace_Multiple() => Assert.Equal("b-b-b", await S("SELECT REPLACE('a-a-a', 'a', 'b')"));
	[Fact] public async Task Replace_NoMatch() => Assert.Equal("hello", await S("SELECT REPLACE('hello', 'xyz', '!')"));
	[Fact] public async Task Replace_Empty() => Assert.Equal("hllo", await S("SELECT REPLACE('hello', 'e', '')"));

	// ---- REVERSE ----
	[Fact] public async Task Reverse_Basic() => Assert.Equal("olleh", await S("SELECT REVERSE('hello')"));
	[Fact] public async Task Reverse_Empty() => Assert.Equal("", await S("SELECT REVERSE('')"));
	[Fact] public async Task Reverse_Single() => Assert.Equal("a", await S("SELECT REVERSE('a')"));

	// ---- REPEAT ----
	[Fact] public async Task Repeat_Basic() => Assert.Equal("abcabcabc", await S("SELECT REPEAT('abc', 3)"));
	[Fact] public async Task Repeat_Zero() => Assert.Equal("", await S("SELECT REPEAT('abc', 0)"));
	[Fact] public async Task Repeat_One() => Assert.Equal("x", await S("SELECT REPEAT('x', 1)"));

	// ---- LPAD / RPAD ----
	[Fact] public async Task Lpad_Basic() => Assert.Equal("00042", await S("SELECT LPAD('42', 5, '0')"));
	[Fact] public async Task Rpad_Basic() => Assert.Equal("42000", await S("SELECT RPAD('42', 5, '0')"));
	[Fact] public async Task Lpad_Default() => Assert.Equal("   42", await S("SELECT LPAD('42', 5)"));
	[Fact] public async Task Rpad_Default() => Assert.Equal("42   ", await S("SELECT RPAD('42', 5)"));
	[Fact] public async Task Lpad_Truncate() => Assert.Equal("hel", await S("SELECT LPAD('hello', 3)"));
	[Fact] public async Task Rpad_Truncate() => Assert.Equal("hel", await S("SELECT RPAD('hello', 3)"));

	// ---- STARTS_WITH / ENDS_WITH ----
	[Fact] public async Task StartsWith_True() => Assert.Equal("True", await S("SELECT STARTS_WITH('hello world', 'hello')"));
	[Fact] public async Task StartsWith_False() => Assert.Equal("False", await S("SELECT STARTS_WITH('hello world', 'world')"));
	[Fact] public async Task EndsWith_True() => Assert.Equal("True", await S("SELECT ENDS_WITH('hello world', 'world')"));
	[Fact] public async Task EndsWith_False() => Assert.Equal("False", await S("SELECT ENDS_WITH('hello world', 'hello')"));
	[Fact] public async Task StartsWith_Empty() => Assert.Equal("True", await S("SELECT STARTS_WITH('hello', '')"));

	// ---- CONTAINS_SUBSTR ----
	[Fact] public async Task ContainsSubstr_True() => Assert.Equal("True", await S("SELECT CONTAINS_SUBSTR('hello world', 'world')"));
	[Fact] public async Task ContainsSubstr_CaseInsensitive() => Assert.Equal("True", await S("SELECT CONTAINS_SUBSTR('Hello World', 'HELLO')"));

	// ---- STRPOS / INSTR ----
	[Fact] public async Task Strpos_Found() => Assert.Equal("7", await S("SELECT STRPOS('hello world', 'world')"));
	[Fact] public async Task Strpos_NotFound() => Assert.Equal("0", await S("SELECT STRPOS('hello', 'xyz')"));
	[Fact] public async Task Instr_Basic() => Assert.Equal("7", await S("SELECT INSTR('hello world', 'world')"));

	// ---- LEFT / RIGHT ----
	[Fact] public async Task Left_Basic() => Assert.Equal("hel", await S("SELECT LEFT('hello', 3)"));
	[Fact] public async Task Right_Basic() => Assert.Equal("llo", await S("SELECT RIGHT('hello', 3)"));
	[Fact] public async Task Left_Zero() => Assert.Equal("", await S("SELECT LEFT('hello', 0)"));

	// ---- SPLIT ----
	[Fact] public async Task Split_Basic()
	{
		var v = await S("SELECT ARRAY_LENGTH(SPLIT('a,b,c', ','))");
		Assert.Equal("3", v);
	}
	[Fact] public async Task Split_Default()
	{
		var v = await S("SELECT ARRAY_LENGTH(SPLIT('a,b,c'))");
		Assert.Equal("3", v); // default delimiter is comma
	}

	// ---- REGEXP_CONTAINS ----
	[Fact] public async Task RegexpContains_True() => Assert.Equal("True", await S("SELECT REGEXP_CONTAINS('hello123', r'[0-9]+')"));
	[Fact] public async Task RegexpContains_False() => Assert.Equal("False", await S("SELECT REGEXP_CONTAINS('hello', r'[0-9]+')"));

	// ---- REGEXP_EXTRACT ----
	[Fact] public async Task RegexpExtract_Basic() => Assert.Equal("123", await S("SELECT REGEXP_EXTRACT('hello123world', r'[0-9]+')"));
	[Fact] public async Task RegexpExtract_NoMatch() => Assert.Null(await S("SELECT REGEXP_EXTRACT('hello', r'[0-9]+')"));

	// ---- REGEXP_REPLACE ----
	[Fact] public async Task RegexpReplace_Basic() => Assert.Equal("hello_world", await S("SELECT REGEXP_REPLACE('hello world', r' ', '_')"));
	[Fact] public async Task RegexpReplace_Digits() => Assert.Equal("abc##xyz", await S("SELECT REGEXP_REPLACE('abc123xyz', r'[0-9]+', '##')"));

	// ---- SAFE_CONVERT_BYTES_TO_STRING ----
	[Fact] public async Task SafeConvert_Valid() => Assert.Equal("hello", await S("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'hello')"));

	// ---- FORMAT ----
	[Fact] public async Task Format_String() => Assert.Equal("hello world", await S("SELECT FORMAT('%s %s', 'hello', 'world')"));
	[Fact] public async Task Format_Int() => Assert.Equal("42", await S("SELECT FORMAT('%d', 42)"));
	[Fact] public async Task Format_Float() => Assert.Equal("3.14", await S("SELECT FORMAT('%.2f', 3.14159)"));

	// ---- TO_HEX / FROM_HEX ----
	[Fact] public async Task ToHex_Basic() => Assert.Equal("68656c6c6f", await S("SELECT TO_HEX(b'hello')"));
	[Fact] public async Task FromHex_Basic() => Assert.Equal("hello", await S("SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_HEX('68656c6c6f'))"));

	// ---- TO_BASE64 / FROM_BASE64 ----
	[Fact] public async Task ToBase64_Basic() => Assert.Equal("aGVsbG8=", await S("SELECT TO_BASE64(b'hello')"));
	[Fact] public async Task FromBase64_Basic() => Assert.Equal("hello", await S("SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_BASE64('aGVsbG8='))"));

	// ---- INITCAP ----
	[Fact] public async Task Initcap_Basic() => Assert.Equal("Hello World", await S("SELECT INITCAP('hello world')"));
	[Fact] public async Task Initcap_Mixed() => Assert.Equal("Hello World", await S("SELECT INITCAP('HELLO WORLD')"));

	// ---- NORMALIZE ----
	[Fact] public async Task Normalize_Basic() => Assert.NotNull(await S("SELECT NORMALIZE('hello')"));

	// ---- ASCII / CHR ----
	[Fact] public async Task Ascii_Basic() => Assert.Equal("65", await S("SELECT ASCII('A')"));
	[Fact] public async Task Chr_Basic() => Assert.Equal("A", await S("SELECT CHR(65)"));
	[Fact] public async Task Chr_Space() => Assert.Equal(" ", await S("SELECT CHR(32)"));

	// ---- TRANSLATE ----
	[Fact] public async Task Translate_Basic() => Assert.Equal("hxllw", await S("SELECT TRANSLATE('hello', 'eo', 'xw')"));

	// ---- Multiple string functions combined ----
	[Fact] public async Task Combined_UpperTrim() => Assert.Equal("HELLO", await S("SELECT UPPER(TRIM('  hello  '))"));
	[Fact] public async Task Combined_ConcatReverse() => Assert.Equal("cba", await S("SELECT REVERSE(CONCAT('a','b','c'))"));
	[Fact] public async Task Combined_SubstrLength() => Assert.Equal("3", await S("SELECT LENGTH(SUBSTR('hello world', 1, 3))"));
	[Fact] public async Task Combined_ReplaceUpper() => Assert.Equal("HELLO_WORLD", await S("SELECT UPPER(REPLACE('hello world', ' ', '_'))"));
	[Fact] public async Task Combined_LpadRepeat() => Assert.Equal("00000", await S("SELECT LPAD(REPEAT('0', 3), 5, '0')"));

	// ---- Null propagation ----
	[Fact] public async Task NullProp_Upper() => Assert.Null(await S("SELECT UPPER(CAST(NULL AS STRING))"));
	[Fact] public async Task NullProp_Replace() => Assert.Null(await S("SELECT REPLACE(CAST(NULL AS STRING), 'a', 'b')"));
	[Fact] public async Task NullProp_Substr() => Assert.Null(await S("SELECT SUBSTR(CAST(NULL AS STRING), 1)"));
	[Fact] public async Task NullProp_Concat_BQ() => Assert.Null(await S("SELECT CONCAT(CAST(NULL AS STRING), 'b')"));
	[Fact] public async Task NullProp_Length() => Assert.Null(await S("SELECT LENGTH(CAST(NULL AS STRING))"));
}
