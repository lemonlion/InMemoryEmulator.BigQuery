using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Regex function tests: REGEXP_CONTAINS, REGEXP_EXTRACT, REGEXP_REPLACE, REGEXP_EXTRACT_ALL, REGEXP_INSTR.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_contains
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class RegexFunctionPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public RegexFunctionPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_rfp_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.emails` (id INT64, email STRING, note STRING)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.emails` VALUES
			(1,'alice@example.com','Phone: 123-456-7890'),
			(2,'bob.smith@test.org','Phone: 987-654-3210'),
			(3,'carol_99@domain.co.uk','No phone'),
			(4,'dave@sub.domain.com','Phones: 111-222-3333, 444-555-6666'),
			(5,'eve123@gmail.com','Contact: 000-000-0000')", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- REGEXP_CONTAINS ----
	[Fact] public async Task RegexpContains_True() => Assert.Equal("True", await S("SELECT REGEXP_CONTAINS('hello world', r'world')"));
	[Fact] public async Task RegexpContains_False() => Assert.Equal("False", await S("SELECT REGEXP_CONTAINS('hello', r'xyz')"));
	[Fact] public async Task RegexpContains_Pattern() => Assert.Equal("True", await S("SELECT REGEXP_CONTAINS('abc123', r'[0-9]+')"));
	[Fact] public async Task RegexpContains_Email() => Assert.Equal("True", await S("SELECT REGEXP_CONTAINS('test@example.com', r'@[a-z]+\\.[a-z]+')"));
	[Fact] public async Task RegexpContains_Anchored() => Assert.Equal("True", await S("SELECT REGEXP_CONTAINS('hello', r'^he')"));
	[Fact] public async Task RegexpContains_AnchoredEnd() => Assert.Equal("True", await S("SELECT REGEXP_CONTAINS('hello', r'lo$')"));
	[Fact] public async Task RegexpContains_CaseSensitive() => Assert.Equal("False", await S("SELECT REGEXP_CONTAINS('Hello', r'^h')"));
	[Fact] public async Task RegexpContains_Dot() => Assert.Equal("True", await S("SELECT REGEXP_CONTAINS('a1b', r'a.b')"));
	[Fact] public async Task RegexpContains_Alternation() => Assert.Equal("True", await S("SELECT REGEXP_CONTAINS('cat', r'cat|dog')"));
	[Fact] public async Task RegexpContains_NullInput() => Assert.Null(await S("SELECT REGEXP_CONTAINS(NULL, r'test')"));
	[Fact] public async Task RegexpContains_InWhere()
	{
		var rows = await Q("SELECT email FROM `{ds}.emails` WHERE REGEXP_CONTAINS(email, r'@.*\\.com$') ORDER BY id");
		Assert.Equal(3, rows.Count); // alice@example.com, dave@sub.domain.com, eve123@gmail.com
	}
	[Fact] public async Task RegexpContains_NotMatch()
	{
		var rows = await Q("SELECT email FROM `{ds}.emails` WHERE NOT REGEXP_CONTAINS(email, r'\\.com') ORDER BY id");
		Assert.Equal(2, rows.Count); // bob (.org), carol (.co.uk)
	}

	// ---- REGEXP_EXTRACT ----
	[Fact] public async Task RegexpExtract_Basic() => Assert.Equal("123", await S("SELECT REGEXP_EXTRACT('abc123def', r'[0-9]+')"));
	[Fact] public async Task RegexpExtract_Group() => Assert.Equal("example", await S("SELECT REGEXP_EXTRACT('test@example.com', r'@([a-z]+)')"));
	[Fact] public async Task RegexpExtract_NoMatch() => Assert.Null(await S("SELECT REGEXP_EXTRACT('hello', r'[0-9]+')"));
	[Fact] public async Task RegexpExtract_NullInput() => Assert.Null(await S("SELECT REGEXP_EXTRACT(NULL, r'test')"));
	[Fact] public async Task RegexpExtract_FirstMatch() => Assert.Equal("123", await S("SELECT REGEXP_EXTRACT('123-456', r'[0-9]+')"));
	[Fact] public async Task RegexpExtract_Anchored() => Assert.Equal("hello", await S("SELECT REGEXP_EXTRACT('hello world', r'^(\\w+)')"));
	[Fact] public async Task RegexpExtract_FromTable()
	{
		var rows = await Q("SELECT id, REGEXP_EXTRACT(email, r'^([^@]+)') AS username FROM `{ds}.emails` ORDER BY id");
		Assert.Equal("alice", rows[0]["username"]?.ToString());
		Assert.Equal("bob.smith", rows[1]["username"]?.ToString());
		Assert.Equal("carol_99", rows[2]["username"]?.ToString());
	}
	[Fact] public async Task RegexpExtract_PhoneNumber()
	{
		var v = await S("SELECT REGEXP_EXTRACT(note, r'(\\d{3}-\\d{3}-\\d{4})') FROM `{ds}.emails` WHERE id = 1");
		Assert.Equal("123-456-7890", v);
	}

	// ---- REGEXP_REPLACE ----
	[Fact] public async Task RegexpReplace_Basic() => Assert.Equal("hello_world", await S("SELECT REGEXP_REPLACE('hello world', r' ', '_')"));
	[Fact] public async Task RegexpReplace_Digits() => Assert.Equal("abc***def", await S("SELECT REGEXP_REPLACE('abc123def', r'[0-9]+', '***')"));
	[Fact] public async Task RegexpReplace_Remove() => Assert.Equal("abcdef", await S("SELECT REGEXP_REPLACE('abc123def', r'[0-9]+', '')"));
	[Fact] public async Task RegexpReplace_AllOccurrences() => Assert.Equal("x-x-x", await S("SELECT REGEXP_REPLACE('1-2-3', r'[0-9]', 'x')"));
	[Fact] public async Task RegexpReplace_NullInput() => Assert.Null(await S("SELECT REGEXP_REPLACE(NULL, r'test', 'x')"));
	[Fact] public async Task RegexpReplace_NoMatch() => Assert.Equal("hello", await S("SELECT REGEXP_REPLACE('hello', r'[0-9]+', 'x')"));
	[Fact] public async Task RegexpReplace_Special() => Assert.Equal("hello...", await S("SELECT REGEXP_REPLACE('hello!!!', r'!+', '...')"));
	[Fact] public async Task RegexpReplace_Whitespace() => Assert.Equal("a b c", await S("SELECT REGEXP_REPLACE('a  b  c', r' +', ' ')"));
	[Fact] public async Task RegexpReplace_FromTable()
	{
		var rows = await Q("SELECT id, REGEXP_REPLACE(email, r'@.*', '@redacted.com') AS masked FROM `{ds}.emails` ORDER BY id LIMIT 2");
		Assert.Equal("alice@redacted.com", rows[0]["masked"]?.ToString());
		Assert.Equal("bob.smith@redacted.com", rows[1]["masked"]?.ToString());
	}

	// ---- REGEXP_EXTRACT_ALL ----
	[Fact] public async Task RegexpExtractAll_Basic()
	{
		var v = await S("SELECT ARRAY_TO_STRING(REGEXP_EXTRACT_ALL('a1b2c3', r'[0-9]'), ',')");
		Assert.Equal("1,2,3", v);
	}
	[Fact] public async Task RegexpExtractAll_Words()
	{
		var v = await S("SELECT ARRAY_TO_STRING(REGEXP_EXTRACT_ALL('hello world foo', r'\\w+'), ',')");
		Assert.Equal("hello,world,foo", v);
	}
	[Fact] public async Task RegexpExtractAll_NoMatch()
	{
		var v = await S("SELECT ARRAY_LENGTH(REGEXP_EXTRACT_ALL('hello', r'[0-9]+'))");
		Assert.Equal("0", v);
	}
	[Fact] public async Task RegexpExtractAll_NullInput() => Assert.Null(await S("SELECT REGEXP_EXTRACT_ALL(NULL, r'test')"));
	[Fact] public async Task RegexpExtractAll_Phones()
	{
		var v = await S("SELECT ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(note, r'\\d{3}-\\d{3}-\\d{4}'), ';') FROM `{ds}.emails` WHERE id = 4");
		Assert.Equal("111-222-3333;444-555-6666", v);
	}
	[Fact] public async Task RegexpExtractAll_Count()
	{
		var v = await S("SELECT ARRAY_LENGTH(REGEXP_EXTRACT_ALL(note, r'\\d{3}-\\d{3}-\\d{4}')) FROM `{ds}.emails` WHERE id = 4");
		Assert.Equal("2", v);
	}

	// ---- REGEXP_INSTR ----
	[Fact] public async Task RegexpInstr_Basic()
	{
		var v = await S("SELECT REGEXP_INSTR('hello123world', r'[0-9]+')");
		Assert.Equal("6", v);
	}
	[Fact] public async Task RegexpInstr_NotFound() => Assert.Equal("0", await S("SELECT REGEXP_INSTR('hello', r'[0-9]+')"));
	[Fact] public async Task RegexpInstr_Start() => Assert.Equal("1", await S("SELECT REGEXP_INSTR('123abc', r'[0-9]+')"));

	// ---- Complex patterns ----
	[Fact] public async Task Complex_ExtractAndReplace()
	{
		var v = await S("SELECT REGEXP_REPLACE(REGEXP_EXTRACT('test@domain.com', r'@(.+)'), r'\\.com$', '.org')");
		Assert.Equal("domain.org", v);
	}
	[Fact] public async Task Complex_ContainsMultiDomain()
	{
		var rows = await Q("SELECT email FROM `{ds}.emails` WHERE REGEXP_CONTAINS(email, r'\\.(com|org)$') ORDER BY id");
		Assert.Equal(4, rows.Count); // alice(.com), bob(.org), dave(.com), eve(.com)
	}
	[Fact] public async Task Complex_ExtractDomain()
	{
		var rows = await Q("SELECT REGEXP_EXTRACT(email, r'@([^.]+)') AS domain FROM `{ds}.emails` ORDER BY id");
		Assert.Equal("example", rows[0]["domain"]?.ToString());
		Assert.Equal("test", rows[1]["domain"]?.ToString());
		Assert.Equal("domain", rows[2]["domain"]?.ToString());
		Assert.Equal("sub", rows[3]["domain"]?.ToString());
		Assert.Equal("gmail", rows[4]["domain"]?.ToString());
	}
	[Fact] public async Task Complex_CountMatches()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.emails` WHERE REGEXP_CONTAINS(email, r'[0-9]')");
		Assert.Equal("2", v); // carol_99, eve123
	}
}
