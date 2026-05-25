using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 35: String function edge cases,
/// BYTE_LENGTH vs LENGTH for multi-byte characters, NORMALIZE,
/// INITCAP, TO_HEX/FROM_HEX, SAFE_CONVERT_BYTES_TO_STRING,
/// REGEXP edge cases, and type coercion in comparisons.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests35 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests35(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv35_{Guid.NewGuid():N}"[..28];
		await _fixture.CreateDatasetAsync(_ds);
	}

	public async ValueTask DisposeAsync()
	{
		try
		{
			var c = await _fixture.GetClientAsync();
			await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true });
		}
		catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null);
		var rows = result.ToList();
		return rows.Count == 0 ? null : rows[0][0]?.ToString();
	}

	private async Task<List<BigQueryRow>> Q(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null);
		return result.ToList();
	}

	// ───────────────────────────────────────────────────────────────────────────
	// LENGTH vs BYTE_LENGTH (multibyte)
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Length_Ascii()
	{
		var result = await S("SELECT LENGTH('hello')");
		Assert.Equal("5", result);
	}

	[Fact] public async Task Length_Unicode()
	{
		// Each emoji is 1 character
		var result = await S("SELECT CHAR_LENGTH('héllo')");
		Assert.Equal("5", result);
	}

	[Fact] public async Task ByteLength_Ascii()
	{
		var result = await S("SELECT BYTE_LENGTH(CAST('hello' AS BYTES))");
		Assert.Equal("5", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// INITCAP
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Initcap_Basic()
	{
		var result = await S("SELECT INITCAP('hello world')");
		Assert.Equal("Hello World", result);
	}

	[Fact] public async Task Initcap_Mixed()
	{
		var result = await S("SELECT INITCAP('hELLO wORLD')");
		Assert.Equal("Hello World", result);
	}

	[Fact] public async Task Initcap_WithSeparators()
	{
		var result = await S("SELECT INITCAP('hello-world_test')");
		Assert.Equal("Hello-World_Test", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// TO_HEX / FROM_HEX
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ToHex_Basic()
	{
		var result = await S("SELECT TO_HEX(CAST('Hello' AS BYTES))");
		Assert.Equal("48656c6c6f", result);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact] public async Task FromHex_Basic()
	{
		var result = await S("SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_HEX('48656c6c6f'))");
		Assert.Equal("Hello", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// REGEXP_REPLACE with backreferences
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task RegexpReplace_Backreference()
	{
		var result = await S(@"SELECT REGEXP_REPLACE('hello world', r'(\w+) (\w+)', '\\2 \\1')");
		Assert.Equal("world hello", result);
	}

	[Fact] public async Task RegexpReplace_Global()
	{
		// REGEXP_REPLACE replaces ALL occurrences (global)
		var result = await S(@"SELECT REGEXP_REPLACE('aabbcc', r'[a-z]{2}', 'X')");
		Assert.Equal("XXX", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// REGEXP_EXTRACT with capture group
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task RegexpExtract_CaptureGroup()
	{
		var result = await S(@"SELECT REGEXP_EXTRACT('abc123def', r'[a-z]+(\d+)')");
		Assert.Equal("123", result); // Returns first capture group
	}

	[Fact] public async Task RegexpExtract_NoCaptureGroup()
	{
		var result = await S(@"SELECT REGEXP_EXTRACT('abc123def', r'\d+')");
		Assert.Equal("123", result); // Returns full match when no capture group
	}

	// ───────────────────────────────────────────────────────────────────────────
	// REGEXP_CONTAINS
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task RegexpContains_True()
	{
		var result = await S(@"SELECT REGEXP_CONTAINS('hello world', r'wor\w+')");
		Assert.Equal("True", result);
	}

	[Fact] public async Task RegexpContains_False()
	{
		var result = await S(@"SELECT REGEXP_CONTAINS('hello world', r'xyz')");
		Assert.Equal("False", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Type coercion in comparisons
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task TypeCoercion_IntFloat()
	{
		// INT64 compared to FLOAT64 should work
		var result = await S("SELECT 1 = 1.0");
		Assert.Equal("True", result);
	}

	[Fact] public async Task TypeCoercion_IntFloat_InWhere()
	{
		var rows = await Q("SELECT x FROM UNNEST([1, 2, 3]) AS x WHERE x = 2.0");
		Assert.Single(rows);
		Assert.Equal("2", rows[0][0]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// STARTS_WITH / ENDS_WITH
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task StartsWith_True()
	{
		var result = await S("SELECT STARTS_WITH('hello world', 'hello')");
		Assert.Equal("True", result);
	}

	[Fact] public async Task EndsWith_True()
	{
		var result = await S("SELECT ENDS_WITH('hello world', 'world')");
		Assert.Equal("True", result);
	}

	// GO emulator bug: INSTR fails with 'invalid position number' even with valid default position.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	// GO emulator bug: INSTR fails with 'invalid position number' even with valid default position.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact] public async Task StartsWith_CaseSensitive()
	{
		var result = await S("SELECT STARTS_WITH('Hello', 'hello')");
		Assert.Equal("False", result); // Case sensitive
	}

	// ───────────────────────────────────────────────────────────────────────────
	// INSTR function
	// ───────────────────────────────────────────────────────────────────────────

	// GO emulator bug: INSTR fails with 'invalid position number' even with valid default position.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact] public async Task Instr_Found()
	{
		var result = await S("SELECT INSTR('hello world', 'world')");
		Assert.Equal("7", result); // 1-based position
	}

	[Fact] public async Task Instr_NotFound()
	{
		var result = await S("SELECT INSTR('hello world', 'xyz')");
		Assert.Equal("0", result); // 0 when not found
	}

	[Fact] public async Task Instr_OccurrenceTwo()
	{
		var result = await S("SELECT INSTR('abcabc', 'bc', 1, 2)");
		Assert.Equal("5", result); // Second occurrence of 'bc'
	}

	// ───────────────────────────────────────────────────────────────────────────
	// ARRAY_LENGTH edge cases
	// ───────────────────────────────────────────────────────────────────────────

	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact] public async Task ArrayLength_Empty()
	{
		var result = await S("SELECT ARRAY_LENGTH(CAST([] AS ARRAY<INT64>))");
		Assert.Equal("0", result);
	}

	[Fact] public async Task ArrayLength_Null()
	{
		var result = await S("SELECT ARRAY_LENGTH(CAST(NULL AS ARRAY<INT64>))");
		Assert.Null(result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// GREATEST / LEAST with multiple args
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Greatest_Multiple()
	{
		var result = await S("SELECT GREATEST(3, 7, 1, 9, 2)");
		Assert.Equal("9", result);
	}

	[Fact] public async Task Least_Multiple()
	{
		var result = await S("SELECT LEAST(3, 7, 1, 9, 2)");
		Assert.Equal("1", result);
	}

	[Fact] public async Task Greatest_WithNull()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#greatest
		//   "Returns NULL if any input is NULL."
		var result = await S("SELECT GREATEST(3, CAST(NULL AS INT64), 7)");
		Assert.Null(result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// CHAR / CODE_POINTS_TO_STRING
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Chr_Basic()
	{
		var result = await S("SELECT CHR(65)");
		Assert.Equal("A", result);
	}

	[Fact] public async Task Chr_Space()
	{
		var result = await S("SELECT CHR(32)");
		Assert.Equal(" ", result);
	}
}
