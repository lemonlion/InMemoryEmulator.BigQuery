using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for additional string functions not covered elsewhere.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class StringFunctionExtendedTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public StringFunctionExtendedTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_strex_{Guid.NewGuid():N}"[..30];
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
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// LENGTH / CHAR_LENGTH
	[Fact] public async Task Length_Basic() => Assert.Equal("5", await Scalar("SELECT LENGTH('hello')"));
	[Fact] public async Task Length_Empty() => Assert.Equal("0", await Scalar("SELECT LENGTH('')"));
	[Fact] public async Task Length_Null() => Assert.Null(await Scalar("SELECT LENGTH(NULL)"));
	[Fact] public async Task CharLength() => Assert.Equal("5", await Scalar("SELECT CHAR_LENGTH('hello')"));

	// CONCAT
	[Fact] public async Task Concat_Two() => Assert.Equal("helloworld", await Scalar("SELECT CONCAT('hello', 'world')"));
	[Fact] public async Task Concat_Three() => Assert.Equal("abc", await Scalar("SELECT CONCAT('a', 'b', 'c')"));
	[Fact] public async Task Concat_Operator() => Assert.Equal("helloworld", await Scalar("SELECT 'hello' || 'world'"));

	// UPPER / LOWER / INITCAP
	[Fact] public async Task Upper() => Assert.Equal("HELLO", await Scalar("SELECT UPPER('hello')"));
	[Fact] public async Task Lower() => Assert.Equal("hello", await Scalar("SELECT LOWER('HELLO')"));
	[Fact] public async Task Upper_Null() => Assert.Null(await Scalar("SELECT UPPER(NULL)"));
	[Fact] public async Task Initcap() => Assert.Equal("Hello World", await Scalar("SELECT INITCAP('hello world')"));

	// TRIM / LTRIM / RTRIM
	[Fact] public async Task Trim_Spaces() => Assert.Equal("hello", await Scalar("SELECT TRIM('  hello  ')"));
	[Fact] public async Task Ltrim_Spaces() => Assert.Equal("hello  ", await Scalar("SELECT LTRIM('  hello  ')"));
	[Fact] public async Task Rtrim_Spaces() => Assert.Equal("  hello", await Scalar("SELECT RTRIM('  hello  ')"));
	[Fact] public async Task Trim_Custom() => Assert.Equal("hello", await Scalar("SELECT TRIM('xxhelloxx', 'x')"));

	// SUBSTR
	[Fact] public async Task Substr_FromStart() => Assert.Equal("hel", await Scalar("SELECT SUBSTR('hello', 1, 3)"));
	[Fact] public async Task Substr_FromMiddle() => Assert.Equal("llo", await Scalar("SELECT SUBSTR('hello', 3, 3)"));
	[Fact] public async Task Substr_ToEnd() => Assert.Equal("llo", await Scalar("SELECT SUBSTR('hello', 3)"));
	[Fact] public async Task Substr_Null() => Assert.Null(await Scalar("SELECT SUBSTR(NULL, 1, 3)"));

	// LEFT / RIGHT
	[Fact] public async Task Left_Basic() => Assert.Equal("hel", await Scalar("SELECT LEFT('hello', 3)"));
	[Fact] public async Task Right_Basic() => Assert.Equal("llo", await Scalar("SELECT RIGHT('hello', 3)"));

	// REPLACE
	[Fact] public async Task Replace_Basic() => Assert.Equal("hxllo", await Scalar("SELECT REPLACE('hello', 'e', 'x')"));
	[Fact] public async Task Replace_All() => Assert.Equal("xxx", await Scalar("SELECT REPLACE('aaa', 'a', 'x')"));
	[Fact] public async Task Replace_NotFound() => Assert.Equal("hello", await Scalar("SELECT REPLACE('hello', 'z', 'x')"));
	[Fact] public async Task Replace_Null() => Assert.Null(await Scalar("SELECT REPLACE(NULL, 'a', 'b')"));

	// REVERSE
	[Fact] public async Task Reverse_Basic() => Assert.Equal("olleh", await Scalar("SELECT REVERSE('hello')"));
	[Fact] public async Task Reverse_Empty() => Assert.Equal("", await Scalar("SELECT REVERSE('')"));

	// REPEAT
	[Fact] public async Task Repeat_Basic() => Assert.Equal("abcabcabc", await Scalar("SELECT REPEAT('abc', 3)"));
	[Fact] public async Task Repeat_Zero() => Assert.Equal("", await Scalar("SELECT REPEAT('abc', 0)"));

	// LPAD / RPAD
	[Fact] public async Task Lpad_Basic() => Assert.Equal("00042", await Scalar("SELECT LPAD('42', 5, '0')"));
	[Fact] public async Task Rpad_Basic() => Assert.Equal("42000", await Scalar("SELECT RPAD('42', 5, '0')"));
	[Fact] public async Task Lpad_Default() => Assert.Equal("   42", await Scalar("SELECT LPAD('42', 5)"));

	// STARTS_WITH / ENDS_WITH
	[Fact] public async Task StartsWith_True() => Assert.Equal("True", await Scalar("SELECT STARTS_WITH('hello world', 'hello')"));
	[Fact] public async Task StartsWith_False() => Assert.Equal("False", await Scalar("SELECT STARTS_WITH('hello world', 'world')"));
	[Fact] public async Task EndsWith_True() => Assert.Equal("True", await Scalar("SELECT ENDS_WITH('hello world', 'world')"));
	[Fact] public async Task EndsWith_False() => Assert.Equal("False", await Scalar("SELECT ENDS_WITH('hello world', 'hello')"));

	// INSTR / STRPOS
	[Fact] public async Task Instr_Found() => Assert.Equal("7", await Scalar("SELECT INSTR('hello world', 'world')"));
	[Fact] public async Task Instr_NotFound() => Assert.Equal("0", await Scalar("SELECT INSTR('hello', 'xyz')"));

	// SPLIT
	[Fact] public async Task Split_Count() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(SPLIT('a,b,c', ','))"));
	[Fact] public async Task Split_Element() => Assert.Equal("world", await Scalar("SELECT SPLIT('hello world', ' ')[OFFSET(1)]"));

	// REGEXP_EXTRACT / REGEXP_CONTAINS
	[Fact] public async Task RegexpExtract_Basic() => Assert.Equal("123", await Scalar("SELECT REGEXP_EXTRACT('hello123world', '[0-9]+')"));
	[Fact] public async Task RegexpExtract_NoMatch() => Assert.Null(await Scalar("SELECT REGEXP_EXTRACT('hello', '[0-9]+')"));
	[Fact] public async Task RegexpContains_True() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('hello123', '[0-9]+')"));
	[Fact] public async Task RegexpContains_False() => Assert.Equal("False", await Scalar("SELECT REGEXP_CONTAINS('hello', '[0-9]+')"));

	// ASCII / CHR
	[Fact] public async Task Ascii_Letter() => Assert.Equal("65", await Scalar("SELECT ASCII('A')"));
	[Fact] public async Task Ascii_Empty() => Assert.Equal("0", await Scalar("SELECT ASCII('')"));
	[Fact] public async Task Chr_Letter() => Assert.Equal("A", await Scalar("SELECT CHR(65)"));

	// BYTE_LENGTH
	[Fact] public async Task ByteLength() => Assert.Equal("5", await Scalar("SELECT BYTE_LENGTH('hello')"));

	// CONTAINS_SUBSTR
	[Fact] public async Task ContainsSubstr_True() => Assert.Equal("True", await Scalar("SELECT CONTAINS_SUBSTR('hello world', 'world')"));
	[Fact] public async Task ContainsSubstr_CaseInsensitive() => Assert.Equal("True", await Scalar("SELECT CONTAINS_SUBSTR('Hello World', 'hello')"));

	// FORMAT string
	[Fact] public async Task Format_Int() => Assert.Equal("42", await Scalar("SELECT FORMAT('%d', 42)"));
	[Fact] public async Task Format_String() => Assert.Equal("hello", await Scalar("SELECT FORMAT('%s', 'hello')"));

	// SAFE_CONVERT_BYTES_TO_STRING
	[Fact] public async Task SafeConvertBytes() => Assert.NotNull(await Scalar("SELECT SAFE_CONVERT_BYTES_TO_STRING(CAST('hello' AS BYTES))"));

	// TO_BASE64 / FROM_BASE64
	[Fact] public async Task ToBase64() => Assert.Equal("aGVsbG8=", await Scalar("SELECT TO_BASE64(CAST('hello' AS BYTES))"));

	// NORMALIZE
	[Fact] public async Task Normalize_Basic() => Assert.Equal("hello", await Scalar("SELECT NORMALIZE('hello')"));
}
