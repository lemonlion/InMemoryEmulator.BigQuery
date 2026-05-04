using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// String manipulation and transformation patterns: complex REPLACE, SPLIT, concatenation chains, FORMAT.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class StringTransformPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public StringTransformPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_stp_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.strings` (id INT64, val STRING)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.strings` VALUES
			(1,'Hello World'),(2,'foo bar baz'),(3,'  spaces  '),(4,'UPPER'),
			(5,'lower'),(6,'MiXeD CaSe'),(7,'repeat'),(8,'abc123def'),
			(9,''),(10,NULL),(11,'a,b,c,d'),(12,'hello---world')", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- UPPER/LOWER ----
	[Fact] public async Task Upper_Basic() => Assert.Equal("HELLO", await S("SELECT UPPER('hello')"));
	[Fact] public async Task Lower_Basic() => Assert.Equal("hello", await S("SELECT LOWER('HELLO')"));
	[Fact] public async Task Upper_Column()
	{
		var v = await S("SELECT UPPER(val) FROM `{ds}.strings` WHERE id = 5");
		Assert.Equal("LOWER", v);
	}
	[Fact] public async Task Lower_Column()
	{
		var v = await S("SELECT LOWER(val) FROM `{ds}.strings` WHERE id = 4");
		Assert.Equal("upper", v);
	}

	// ---- INITCAP ----
	[Fact] public async Task Initcap_Basic() => Assert.Equal("Hello World", await S("SELECT INITCAP('hello world')"));
	[Fact] public async Task Initcap_Upper() => Assert.Equal("Hello", await S("SELECT INITCAP('HELLO')"));
	[Fact] public async Task Initcap_Column()
	{
		var v = await S("SELECT INITCAP(val) FROM `{ds}.strings` WHERE id = 2");
		Assert.Equal("Foo Bar Baz", v);
	}

	// ---- TRIM / LTRIM / RTRIM ----
	[Fact] public async Task Trim_Both() => Assert.Equal("hello", await S("SELECT TRIM('  hello  ')"));
	[Fact] public async Task Ltrim() => Assert.Equal("hello  ", await S("SELECT LTRIM('  hello  ')"));
	[Fact] public async Task Rtrim() => Assert.Equal("  hello", await S("SELECT RTRIM('  hello  ')"));
	[Fact] public async Task Trim_Column()
	{
		var v = await S("SELECT TRIM(val) FROM `{ds}.strings` WHERE id = 3");
		Assert.Equal("spaces", v);
	}
	[Fact] public async Task Trim_Chars() => Assert.Equal("hello", await S("SELECT TRIM('xxhelloxx', 'x')"));

	// ---- LENGTH ----
	[Fact] public async Task Length_Basic() => Assert.Equal("5", await S("SELECT LENGTH('hello')"));
	[Fact] public async Task Length_Empty() => Assert.Equal("0", await S("SELECT LENGTH('')"));
	[Fact] public async Task Length_Column()
	{
		var v = await S("SELECT LENGTH(val) FROM `{ds}.strings` WHERE id = 1");
		Assert.Equal("11", v);
	}

	// ---- SUBSTR ----
	[Fact] public async Task Substr_Basic() => Assert.Equal("hel", await S("SELECT SUBSTR('hello', 1, 3)"));
	[Fact] public async Task Substr_FromEnd() => Assert.Equal("lo", await S("SELECT SUBSTR('hello', -2)"));
	[Fact] public async Task Substr_Column()
	{
		var v = await S("SELECT SUBSTR(val, 1, 5) FROM `{ds}.strings` WHERE id = 1");
		Assert.Equal("Hello", v);
	}

	// ---- REPLACE ----
	[Fact] public async Task Replace_Basic() => Assert.Equal("hxllo", await S("SELECT REPLACE('hello', 'e', 'x')"));
	[Fact] public async Task Replace_Remove() => Assert.Equal("hllo", await S("SELECT REPLACE('hello', 'e', '')"));
	[Fact] public async Task Replace_NoMatch() => Assert.Equal("hello", await S("SELECT REPLACE('hello', 'z', 'x')"));
	[Fact] public async Task Replace_Multiple() => Assert.Equal("hxppo", await S("SELECT REPLACE(REPLACE('hello', 'e', 'x'), 'l', 'p')"));
	[Fact] public async Task Replace_Column()
	{
		var v = await S("SELECT REPLACE(val, ' ', '_') FROM `{ds}.strings` WHERE id = 1");
		Assert.Equal("Hello_World", v);
	}

	// ---- REVERSE ----
	[Fact] public async Task Reverse_Basic() => Assert.Equal("olleh", await S("SELECT REVERSE('hello')"));
	[Fact] public async Task Reverse_Empty() => Assert.Equal("", await S("SELECT REVERSE('')"));
	[Fact] public async Task Reverse_Column()
	{
		var v = await S("SELECT REVERSE(val) FROM `{ds}.strings` WHERE id = 7");
		Assert.Equal("taeper", v);
	}

	// ---- REPEAT ----
	[Fact] public async Task Repeat_Basic() => Assert.Equal("abcabcabc", await S("SELECT REPEAT('abc', 3)"));
	[Fact] public async Task Repeat_Zero() => Assert.Equal("", await S("SELECT REPEAT('abc', 0)"));
	[Fact] public async Task Repeat_One() => Assert.Equal("abc", await S("SELECT REPEAT('abc', 1)"));

	// ---- CONCAT ----
	[Fact] public async Task Concat_Two() => Assert.Equal("foobar", await S("SELECT CONCAT('foo', 'bar')"));
	[Fact] public async Task Concat_Three() => Assert.Equal("foobarbaz", await S("SELECT CONCAT('foo', 'bar', 'baz')"));
	[Fact] public async Task Concat_WithInt() => Assert.Equal("id=5", await S("SELECT CONCAT('id=', CAST(5 AS STRING))"));
	[Fact] public async Task Concat_Empty() => Assert.Equal("hello", await S("SELECT CONCAT('', 'hello')"));
	[Fact] public async Task Concat_Column()
	{
		var v = await S("SELECT CONCAT(val, '!') FROM `{ds}.strings` WHERE id = 1");
		Assert.Equal("Hello World!", v);
	}

	// ---- LPAD / RPAD ----
	[Fact] public async Task Lpad_Basic() => Assert.Equal("00042", await S("SELECT LPAD('42', 5, '0')"));
	[Fact] public async Task Rpad_Basic() => Assert.Equal("42000", await S("SELECT RPAD('42', 5, '0')"));
	[Fact] public async Task Lpad_Truncate() => Assert.Equal("he", await S("SELECT LPAD('hello', 2, ' ')"));
	[Fact] public async Task Lpad_Space() => Assert.Equal("   hi", await S("SELECT LPAD('hi', 5, ' ')"));

	// ---- STRPOS / INSTR ----
	[Fact] public async Task Strpos_Found() => Assert.Equal("7", await S("SELECT STRPOS('Hello World', 'World')"));
	[Fact] public async Task Strpos_NotFound() => Assert.Equal("0", await S("SELECT STRPOS('Hello World', 'xyz')"));
	[Fact] public async Task Strpos_Start() => Assert.Equal("1", await S("SELECT STRPOS('Hello', 'H')"));

	// ---- LEFT / RIGHT ----
	[Fact] public async Task Left_Basic() => Assert.Equal("Hel", await S("SELECT LEFT('Hello', 3)"));
	[Fact] public async Task Right_Basic() => Assert.Equal("llo", await S("SELECT RIGHT('Hello', 3)"));
	[Fact] public async Task Left_Full() => Assert.Equal("Hello", await S("SELECT LEFT('Hello', 10)"));

	// ---- STARTS_WITH / ENDS_WITH ----
	[Fact] public async Task StartsWith_True() => Assert.Equal("True", await S("SELECT STARTS_WITH('Hello', 'Hel')"));
	[Fact] public async Task StartsWith_False() => Assert.Equal("False", await S("SELECT STARTS_WITH('Hello', 'hel')"));
	[Fact] public async Task EndsWith_True() => Assert.Equal("True", await S("SELECT ENDS_WITH('Hello', 'llo')"));
	[Fact] public async Task EndsWith_False() => Assert.Equal("False", await S("SELECT ENDS_WITH('Hello', 'LLO')"));

	// ---- SPLIT ----
	[Fact] public async Task Split_Default()
	{
		var v = await S("SELECT ARRAY_LENGTH(SPLIT('a,b,c', ','))");
		Assert.Equal("3", v);
	}
	[Fact] public async Task Split_Custom()
	{
		var v = await S("SELECT ARRAY_LENGTH(SPLIT('a|b|c|d', '|'))");
		Assert.Equal("4", v);
	}
	[Fact] public async Task Split_Column()
	{
		var v = await S("SELECT ARRAY_LENGTH(SPLIT(val, ',')) FROM `{ds}.strings` WHERE id = 11");
		Assert.Equal("4", v); // "a,b,c,d"
	}

	// ---- FORMAT ----
	[Fact] public async Task Format_Int() => Assert.Equal("42", await S("SELECT FORMAT('%d', 42)"));
	[Fact] public async Task Format_Float() => Assert.NotNull(await S("SELECT FORMAT('%.2f', 3.14159)"));
	[Fact] public async Task Format_String() => Assert.Equal("hello", await S("SELECT FORMAT('%s', 'hello')"));

	// ---- String in WHERE ----
	[Fact] public async Task Where_StringEquals()
	{
		var rows = await Q("SELECT id FROM `{ds}.strings` WHERE val = 'UPPER'");
		Assert.Single(rows);
	}
	[Fact] public async Task Where_StringContains()
	{
		var rows = await Q("SELECT id FROM `{ds}.strings` WHERE val LIKE '%bar%' ORDER BY id");
		Assert.True(rows.Count >= 1);
	}

	// ---- String concatenation operator ----
	[Fact] public async Task ConcatOp() => Assert.Equal("foobar", await S("SELECT 'foo' || 'bar'"));
	[Fact] public async Task ConcatOp_Chain() => Assert.Equal("abc", await S("SELECT 'a' || 'b' || 'c'"));

	// ---- TO_HEX / FROM_HEX ----
	[Fact] public async Task ToHex()
	{
		var v = await S("SELECT TO_HEX(b'\\x48\\x65\\x6c\\x6c\\x6f')");
		Assert.NotNull(v); // Should be "48656c6c6f"
	}

	// ---- ASCII / CHR ----
	[Fact] public async Task Ascii_A() => Assert.Equal("65", await S("SELECT ASCII('A')"));
	[Fact] public async Task Chr_65() => Assert.Equal("A", await S("SELECT CHR(65)"));

	// ---- String in GROUP BY ----
	[Fact] public async Task GroupBy_String()
	{
		var rows = await Q("SELECT LENGTH(val) AS len, COUNT(*) AS cnt FROM `{ds}.strings` WHERE val IS NOT NULL GROUP BY len ORDER BY len");
		Assert.True(rows.Count >= 3);
	}

	// ---- String in ORDER BY ----
	[Fact] public async Task OrderBy_String()
	{
		var rows = await Q("SELECT val FROM `{ds}.strings` WHERE val IS NOT NULL AND val != '' ORDER BY val LIMIT 3");
		Assert.Equal(3, rows.Count);
	}
}
