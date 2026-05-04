using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// String function comprehensive patterns: STARTS_WITH, ENDS_WITH, STRPOS, LPAD, RPAD, REVERSE, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class StringFunctionCoverageTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public StringFunctionCoverageTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_sfc_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.t` (id INT64, val STRING)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.t` VALUES
			(1,'Hello World'),(2,'bigquery'),(3,'foo bar baz'),(4,'123-456-7890'),(5,'  spaces  '),
			(6,'UPPERCASE'),(7,'MixedCase'),(8,''),(9,'repeat'),(10,'special!@#$')", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- STARTS_WITH / ENDS_WITH ----
	[Fact] public async Task StartsWith_True() => Assert.Equal("True", await S("SELECT STARTS_WITH('Hello World', 'Hello')"));
	[Fact] public async Task StartsWith_False() => Assert.Equal("False", await S("SELECT STARTS_WITH('Hello World', 'World')"));
	[Fact] public async Task StartsWith_CaseSensitive() => Assert.Equal("False", await S("SELECT STARTS_WITH('Hello', 'hello')"));
	[Fact] public async Task StartsWith_Empty() => Assert.Equal("True", await S("SELECT STARTS_WITH('Hello', '')"));
	[Fact] public async Task StartsWith_Null() => Assert.Null(await S("SELECT STARTS_WITH(NULL, 'x')"));
	[Fact] public async Task EndsWith_True() => Assert.Equal("True", await S("SELECT ENDS_WITH('Hello World', 'World')"));
	[Fact] public async Task EndsWith_False() => Assert.Equal("False", await S("SELECT ENDS_WITH('Hello World', 'Hello')"));
	[Fact] public async Task EndsWith_CaseSensitive() => Assert.Equal("False", await S("SELECT ENDS_WITH('Hello', 'HELLO')"));
	[Fact] public async Task EndsWith_Null() => Assert.Null(await S("SELECT ENDS_WITH(NULL, 'x')"));

	// ---- STRPOS / INSTR ----
	[Fact] public async Task Strpos_Found() => Assert.Equal("7", await S("SELECT STRPOS('Hello World', 'World')"));
	[Fact] public async Task Strpos_NotFound() => Assert.Equal("0", await S("SELECT STRPOS('Hello', 'xyz')"));
	[Fact] public async Task Strpos_Beginning() => Assert.Equal("1", await S("SELECT STRPOS('Hello', 'H')"));
	[Fact] public async Task Strpos_Null() => Assert.Null(await S("SELECT STRPOS(NULL, 'x')"));
	[Fact] public async Task Instr_Basic() => Assert.Equal("7", await S("SELECT INSTR('Hello World', 'World')"));

	// ---- LPAD / RPAD ----
	[Fact] public async Task Lpad_Basic() => Assert.Equal("00042", await S("SELECT LPAD('42', 5, '0')"));
	[Fact] public async Task Lpad_NoFill() => Assert.Equal("Hel", await S("SELECT LPAD('Hello', 3, '0')"));
	[Fact] public async Task Lpad_Spaces() => Assert.Equal("   hi", await S("SELECT LPAD('hi', 5, ' ')"));
	[Fact] public async Task Lpad_Null() => Assert.Null(await S("SELECT LPAD(NULL, 5, '0')"));
	[Fact] public async Task Rpad_Basic() => Assert.Equal("42000", await S("SELECT RPAD('42', 5, '0')"));
	[Fact] public async Task Rpad_NoFill() => Assert.Equal("Hel", await S("SELECT RPAD('Hello', 3, '0')"));
	[Fact] public async Task Rpad_Null() => Assert.Null(await S("SELECT RPAD(NULL, 5, '0')"));

	// ---- REVERSE ----
	[Fact] public async Task Reverse_Basic() => Assert.Equal("olleH", await S("SELECT REVERSE('Hello')"));
	[Fact] public async Task Reverse_Empty() => Assert.Equal("", await S("SELECT REVERSE('')"));
	[Fact] public async Task Reverse_Null() => Assert.Null(await S("SELECT REVERSE(NULL)"));
	[Fact] public async Task Reverse_Palindrome() => Assert.Equal("abcba", await S("SELECT REVERSE('abcba')"));

	// ---- REPLACE ----
	[Fact] public async Task Replace_Basic() => Assert.Equal("Hello Earth", await S("SELECT REPLACE('Hello World', 'World', 'Earth')"));
	[Fact] public async Task Replace_All() => Assert.Equal("x-x-x", await S("SELECT REPLACE('a-a-a', 'a', 'x')"));
	[Fact] public async Task Replace_NotFound() => Assert.Equal("Hello", await S("SELECT REPLACE('Hello', 'xyz', 'abc')"));
	[Fact] public async Task Replace_Remove() => Assert.Equal("Hllo", await S("SELECT REPLACE('Hello', 'e', '')"));
	[Fact] public async Task Replace_Null() => Assert.Null(await S("SELECT REPLACE(NULL, 'a', 'b')"));

	// ---- SUBSTR ----
	[Fact] public async Task Substr_FromStart() => Assert.Equal("Hello", await S("SELECT SUBSTR('Hello World', 1, 5)"));
	[Fact] public async Task Substr_FromMiddle() => Assert.Equal("World", await S("SELECT SUBSTR('Hello World', 7)"));
	[Fact] public async Task Substr_NegativeStart() => Assert.Equal("rld", await S("SELECT SUBSTR('Hello World', -3)"));
	[Fact] public async Task Substr_Null() => Assert.Null(await S("SELECT SUBSTR(NULL, 1)"));

	// ---- TRIM / LTRIM / RTRIM ----
	[Fact] public async Task Trim_Spaces() => Assert.Equal("hello", await S("SELECT TRIM('  hello  ')"));
	[Fact] public async Task Trim_Custom() => Assert.Equal("hello", await S("SELECT TRIM('xxhelloxx', 'x')"));
	[Fact] public async Task Ltrim_Basic() => Assert.Equal("hello  ", await S("SELECT LTRIM('  hello  ')"));
	[Fact] public async Task Rtrim_Basic() => Assert.Equal("  hello", await S("SELECT RTRIM('  hello  ')"));
	[Fact] public async Task Trim_Null() => Assert.Null(await S("SELECT TRIM(NULL)"));

	// ---- UPPER / LOWER / INITCAP ----
	[Fact] public async Task Upper_Basic() => Assert.Equal("HELLO", await S("SELECT UPPER('hello')"));
	[Fact] public async Task Lower_Basic() => Assert.Equal("hello", await S("SELECT LOWER('HELLO')"));
	[Fact] public async Task Initcap_Basic() => Assert.Equal("Hello World", await S("SELECT INITCAP('hello world')"));
	[Fact] public async Task Initcap_Mixed() => Assert.Equal("Foo Bar Baz", await S("SELECT INITCAP('foo bar baz')"));
	[Fact] public async Task Upper_Null() => Assert.Null(await S("SELECT UPPER(NULL)"));

	// ---- CONCAT ----
	[Fact] public async Task Concat_Two() => Assert.Equal("HelloWorld", await S("SELECT CONCAT('Hello', 'World')"));
	[Fact] public async Task Concat_Three() => Assert.Equal("a-b-c", await S("SELECT CONCAT('a', '-', 'b', '-', 'c')"));
	[Fact] public async Task Concat_WithNull() => Assert.Null(await S("SELECT CONCAT('Hello', NULL)"));
	[Fact] public async Task Concat_BarOp() => Assert.Equal("ab", await S("SELECT 'a' || 'b'"));

	// ---- LENGTH ----
	[Fact] public async Task Length_Basic() => Assert.Equal("5", await S("SELECT LENGTH('Hello')"));
	[Fact] public async Task Length_Empty() => Assert.Equal("0", await S("SELECT LENGTH('')"));
	[Fact] public async Task Length_Null() => Assert.Null(await S("SELECT LENGTH(NULL)"));

	// ---- REPEAT ----
	[Fact] public async Task Repeat_Basic() => Assert.Equal("abcabc", await S("SELECT REPEAT('abc', 2)"));
	[Fact] public async Task Repeat_Zero() => Assert.Equal("", await S("SELECT REPEAT('abc', 0)"));
	[Fact] public async Task Repeat_Null() => Assert.Null(await S("SELECT REPEAT(NULL, 3)"));

	// ---- SPLIT ----
	[Fact] public async Task Split_Basic()
	{
		var v = await S("SELECT ARRAY_TO_STRING(SPLIT('a,b,c', ','), '|')");
		Assert.Equal("a|b|c", v);
	}
	[Fact] public async Task Split_Space()
	{
		var v = await S("SELECT ARRAY_LENGTH(SPLIT('foo bar baz', ' '))");
		Assert.Equal("3", v);
	}

	// ---- SAFE_CAST in string context ----
	[Fact] public async Task SafeCast_IntToString() => Assert.Equal("42", await S("SELECT SAFE_CAST(42 AS STRING)"));
	[Fact] public async Task SafeCast_FloatToString()
	{
		var v = await S("SELECT SAFE_CAST(3.14 AS STRING)");
		Assert.NotNull(v);
		Assert.StartsWith("3.14", v);
	}
	[Fact] public async Task SafeCast_InvalidToInt() => Assert.Null(await S("SELECT SAFE_CAST('abc' AS INT64)"));

	// ---- LEFT / RIGHT ----
	[Fact] public async Task Left_Basic() => Assert.Equal("Hel", await S("SELECT LEFT('Hello', 3)"));
	[Fact] public async Task Right_Basic() => Assert.Equal("llo", await S("SELECT RIGHT('Hello', 3)"));
	[Fact] public async Task Left_Null() => Assert.Null(await S("SELECT LEFT(NULL, 3)"));

	// ---- Table queries ----
	[Fact] public async Task Table_UpperAll()
	{
		var rows = await Q("SELECT UPPER(val) AS uval FROM `{ds}.t` WHERE id = 2");
		Assert.Equal("BIGQUERY", rows[0]["uval"]?.ToString());
	}
	[Fact] public async Task Table_LengthFilter()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.t` WHERE LENGTH(val) > 8");
		Assert.Equal("7", v); // Hello World(11), foo bar baz(11), 123-456-7890(12), spaces(10), UPPERCASE(9), MixedCase(9), special!@#$(11)
	}
	[Fact] public async Task Table_ReverseCheck()
	{
		var rows = await Q("SELECT val FROM `{ds}.t` WHERE val = REVERSE(val)");
		// '' is a palindrome
		Assert.True(rows.Count >= 1);
	}
}
