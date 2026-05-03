using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for advanced string manipulation: REGEXP_REPLACE, REPLACE, REPEAT, REVERSE, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class StringManipulationAdvancedTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public StringManipulationAdvancedTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_strm_{Guid.NewGuid():N}"[..30];
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

	// CONCAT
	[Fact] public async Task Concat_TwoStrings() => Assert.Equal("hello world", await Scalar("SELECT CONCAT('hello', ' world')"));
	[Fact] public async Task Concat_MultipleStrings() => Assert.Equal("abc", await Scalar("SELECT CONCAT('a', 'b', 'c')"));
	[Fact] public async Task Concat_WithNumber() => Assert.Equal("val42", await Scalar("SELECT CONCAT('val', CAST(42 AS STRING))"));
	[Fact] public async Task Concat_NullReturnsNull() => Assert.Null(await Scalar("SELECT CONCAT('hello', NULL)"));
	[Fact] public async Task Concat_EmptyString() => Assert.Equal("hello", await Scalar("SELECT CONCAT('hello', '')"));

	// || operator
	[Fact] public async Task ConcatOperator_Basic() => Assert.Equal("hello world", await Scalar("SELECT 'hello' || ' world'"));
	[Fact] public async Task ConcatOperator_Chain() => Assert.Equal("abc", await Scalar("SELECT 'a' || 'b' || 'c'"));

	// REPLACE
	[Fact] public async Task Replace_Basic() => Assert.Equal("hello planet", await Scalar("SELECT REPLACE('hello world', 'world', 'planet')"));
	[Fact] public async Task Replace_Multiple() => Assert.Equal("b-b-b", await Scalar("SELECT REPLACE('a-a-a', 'a', 'b')"));
	[Fact] public async Task Replace_NotFound() => Assert.Equal("hello", await Scalar("SELECT REPLACE('hello', 'xyz', 'abc')"));
	[Fact] public async Task Replace_Empty() => Assert.Equal("hllo", await Scalar("SELECT REPLACE('hello', 'e', '')"));
	[Fact] public async Task Replace_Null() => Assert.Null(await Scalar("SELECT REPLACE(NULL, 'a', 'b')"));

	// REGEXP_REPLACE
	[Fact] public async Task RegexpReplace_Basic() => Assert.Equal("hello world", await Scalar("SELECT REGEXP_REPLACE('hello 123', r'[0-9]+', 'world')"));
	[Fact] public async Task RegexpReplace_Multiple() => Assert.Equal("X-X-X", await Scalar("SELECT REGEXP_REPLACE('1-2-3', r'[0-9]', 'X')"));
	[Fact] public async Task RegexpReplace_Group() => Assert.Equal("bar_foo", await Scalar(@"SELECT REGEXP_REPLACE('foo_bar', r'(\w+)_(\w+)', r'\2_\1')"));
	[Fact] public async Task RegexpReplace_Null() => Assert.Null(await Scalar("SELECT REGEXP_REPLACE(NULL, r'a', 'b')"));

	// REVERSE
	[Fact] public async Task Reverse_Basic() => Assert.Equal("olleh", await Scalar("SELECT REVERSE('hello')"));
	[Fact] public async Task Reverse_Empty() => Assert.Equal("", await Scalar("SELECT REVERSE('')"));
	[Fact] public async Task Reverse_SingleChar() => Assert.Equal("a", await Scalar("SELECT REVERSE('a')"));
	[Fact] public async Task Reverse_Null() => Assert.Null(await Scalar("SELECT REVERSE(NULL)"));

	// REPEAT
	[Fact] public async Task Repeat_Basic() => Assert.Equal("abcabcabc", await Scalar("SELECT REPEAT('abc', 3)"));
	[Fact] public async Task Repeat_Zero() => Assert.Equal("", await Scalar("SELECT REPEAT('abc', 0)"));
	[Fact] public async Task Repeat_One() => Assert.Equal("abc", await Scalar("SELECT REPEAT('abc', 1)"));
	[Fact] public async Task Repeat_Null() => Assert.Null(await Scalar("SELECT REPEAT(NULL, 3)"));

	// LPAD / RPAD
	[Fact] public async Task Lpad_Basic() => Assert.Equal("  hi", await Scalar("SELECT LPAD('hi', 4, ' ')"));
	[Fact] public async Task Lpad_WithChar() => Assert.Equal("00042", await Scalar("SELECT LPAD('42', 5, '0')"));
	[Fact] public async Task Lpad_Truncate() => Assert.Equal("he", await Scalar("SELECT LPAD('hello', 2, ' ')"));
	[Fact] public async Task Rpad_Basic() => Assert.Equal("hi  ", await Scalar("SELECT RPAD('hi', 4, ' ')"));
	[Fact] public async Task Rpad_WithChar() => Assert.Equal("42000", await Scalar("SELECT RPAD('42', 5, '0')"));

	// LTRIM / RTRIM / TRIM
	[Fact] public async Task Ltrim_Spaces() => Assert.Equal("hello  ", await Scalar("SELECT LTRIM('  hello  ')"));
	[Fact] public async Task Rtrim_Spaces() => Assert.Equal("  hello", await Scalar("SELECT RTRIM('  hello  ')"));
	[Fact] public async Task Trim_Spaces() => Assert.Equal("hello", await Scalar("SELECT TRIM('  hello  ')"));
	[Fact] public async Task Trim_CustomChars() => Assert.Equal("hello", await Scalar("SELECT TRIM('xxhelloxx', 'x')"));
	[Fact] public async Task Ltrim_CustomChars() => Assert.Equal("helloxx", await Scalar("SELECT LTRIM('xxhelloxx', 'x')"));
	[Fact] public async Task Rtrim_CustomChars() => Assert.Equal("xxhello", await Scalar("SELECT RTRIM('xxhelloxx', 'x')"));

	// SUBSTR
	[Fact] public async Task Substr_FromStart() => Assert.Equal("hel", await Scalar("SELECT SUBSTR('hello', 1, 3)"));
	[Fact] public async Task Substr_FromMiddle() => Assert.Equal("llo", await Scalar("SELECT SUBSTR('hello', 3, 3)"));
	[Fact] public async Task Substr_NoLength() => Assert.Equal("llo", await Scalar("SELECT SUBSTR('hello', 3)"));
	[Fact] public async Task Substr_Negative() => Assert.Equal("lo", await Scalar("SELECT SUBSTR('hello', -2)"));
	[Fact] public async Task Substr_Null() => Assert.Null(await Scalar("SELECT SUBSTR(NULL, 1, 3)"));

	// LEFT / RIGHT
	[Fact] public async Task Left_Basic() => Assert.Equal("hel", await Scalar("SELECT LEFT('hello', 3)"));
	[Fact] public async Task Left_ExceedsLength() => Assert.Equal("hi", await Scalar("SELECT LEFT('hi', 10)"));
	[Fact] public async Task Right_Basic() => Assert.Equal("llo", await Scalar("SELECT RIGHT('hello', 3)"));
	[Fact] public async Task Right_ExceedsLength() => Assert.Equal("hi", await Scalar("SELECT RIGHT('hi', 10)"));

	// STARTS_WITH / ENDS_WITH
	[Fact] public async Task StartsWith_True() => Assert.Equal("True", await Scalar("SELECT STARTS_WITH('hello world', 'hello')"));
	[Fact] public async Task StartsWith_False() => Assert.Equal("False", await Scalar("SELECT STARTS_WITH('hello world', 'world')"));
	[Fact] public async Task EndsWith_True() => Assert.Equal("True", await Scalar("SELECT ENDS_WITH('hello world', 'world')"));
	[Fact] public async Task EndsWith_False() => Assert.Equal("False", await Scalar("SELECT ENDS_WITH('hello world', 'hello')"));

	// INSTR
	[Fact] public async Task Instr_Found() => Assert.Equal("7", await Scalar("SELECT INSTR('hello world', 'world')"));
	[Fact] public async Task Instr_NotFound() => Assert.Equal("0", await Scalar("SELECT INSTR('hello', 'xyz')"));
	[Fact] public async Task Instr_FirstOccurrence() => Assert.Equal("3", await Scalar("SELECT INSTR('abcabc', 'c')"));

	// SPLIT
	[Fact] public async Task Split_Comma() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(SPLIT('a,b,c', ','))"));
	[Fact] public async Task Split_Space() => Assert.Equal("2", await Scalar("SELECT ARRAY_LENGTH(SPLIT('hello world', ' '))"));
	[Fact] public async Task Split_NoDelimiter() => Assert.Equal("1", await Scalar("SELECT ARRAY_LENGTH(SPLIT('hello', ','))"));

	// ASCII / CHR
	[Fact] public async Task Ascii_Basic() => Assert.Equal("65", await Scalar("SELECT ASCII('A')"));
	[Fact] public async Task Chr_Basic() => Assert.Equal("A", await Scalar("SELECT CHR(65)"));
	[Fact] public async Task Ascii_Lowercase() => Assert.Equal("97", await Scalar("SELECT ASCII('a')"));

	// UPPER / LOWER / INITCAP
	[Fact] public async Task Upper_Basic() => Assert.Equal("HELLO", await Scalar("SELECT UPPER('hello')"));
	[Fact] public async Task Lower_Basic() => Assert.Equal("hello", await Scalar("SELECT LOWER('HELLO')"));
	[Fact] public async Task Initcap_Basic() => Assert.Equal("Hello World", await Scalar("SELECT INITCAP('hello world')"));
	[Fact] public async Task Initcap_Mixed() => Assert.Equal("Hello World", await Scalar("SELECT INITCAP('hELLO wORLD')"));

	// LENGTH / BYTE_LENGTH
	[Fact] public async Task Length_Basic() => Assert.Equal("5", await Scalar("SELECT LENGTH('hello')"));
	[Fact] public async Task Length_Empty() => Assert.Equal("0", await Scalar("SELECT LENGTH('')"));
	[Fact] public async Task Length_Null() => Assert.Null(await Scalar("SELECT LENGTH(NULL)"));
	[Fact] public async Task ByteLength_Ascii() => Assert.Equal("5", await Scalar("SELECT BYTE_LENGTH('hello')"));

	// STRPOS (alias for INSTR)
	[Fact] public async Task Strpos_Basic() => Assert.Equal("7", await Scalar("SELECT STRPOS('hello world', 'world')"));

	// SAFE_CONVERT_BYTES_TO_STRING
	[Fact] public async Task SafeConvertBytesToString_Basic() => Assert.Equal("hello", await Scalar("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'hello')"));
}
