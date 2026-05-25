using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 50: LIKE backslash escape, BETWEEN NULL three-valued logic,
/// SUBSTR NULL position/length.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests50 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public ParityVerificationTests50(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
	}

	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<BigQueryRow> ScalarRowAsync(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		return result.First();
	}

	// --- LIKE backslash escape ---

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#like_operator
	//   BigQuery does not support \% or \_ escape sequences in LIKE patterns.
	//   "Syntax error: Illegal escape sequence: \%"
	//   Backslash is NOT a default escape character for % and _ in BigQuery LIKE.

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Like_EscapedPercent_MatchesLiteralPercent()
	{
		// SQL: 'test%' LIKE 'test\%'  (the \% matches literal %)
		var row = await ScalarRowAsync("SELECT 'test%' LIKE r'test\\%'");
		Assert.Equal(true, row[0]);
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Like_EscapedUnderscore_MatchesLiteralUnderscore()
	{
		var row = await ScalarRowAsync("SELECT 'test_' LIKE r'test\\_'");
		Assert.Equal(true, row[0]);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Like_EscapedBackslash_MatchesLiteralBackslash()
	{
		// In SQL string literal: '\\' = one backslash char
		// In LIKE pattern: '\\\\' = escaped backslash (matches one literal backslash)
		var row = await ScalarRowAsync("SELECT '\\\\' LIKE '\\\\\\\\'");
		Assert.Equal(true, row[0]);
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Like_EscapedPercentInMiddle_MatchesLiteralPercent()
	{
		var row = await ScalarRowAsync("SELECT 'a%b' LIKE r'a\\%b'");
		Assert.Equal(true, row[0]);
	}

	[Fact]
	public async Task Like_UnescapedPercent_StillMatchesWildcard()
	{
		var row = await ScalarRowAsync("SELECT 'anything' LIKE '%thing'");
		Assert.Equal(true, row[0]);
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Like_EscapedPercent_DoesNotMatchWildcard()
	{
		// 'testX' should NOT match 'test\%' because \% means literal %
		var row = await ScalarRowAsync("SELECT 'testX' LIKE r'test\\%'");
		Assert.Equal(false, row[0]);
	}

	// --- BETWEEN NULL three-valued logic ---

	// Ref: SQL standard three-valued logic: BETWEEN is equivalent to (val >= low AND val <= high)
	// NULL AND FALSE = FALSE; NULL AND TRUE = NULL; FALSE AND NULL = FALSE

	[Fact]
	public async Task Between_NullLow_ValueAboveHigh_ReturnsFalse()
	{
		// 5 >= NULL AND 5 <= 3 → NULL AND FALSE → FALSE
		var row = await ScalarRowAsync("SELECT 5 BETWEEN CAST(NULL AS INT64) AND 3");
		Assert.Equal(false, row[0]);
	}

	[Fact]
	public async Task Between_NullHigh_ValueBelowLow_ReturnsFalse()
	{
		// 1 >= 5 AND 1 <= NULL → FALSE AND NULL → FALSE
		var row = await ScalarRowAsync("SELECT 1 BETWEEN 5 AND CAST(NULL AS INT64)");
		Assert.Equal(false, row[0]);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Between_NullLow_ValueBelowHigh_ReturnsNull()
	{
		// 5 >= NULL AND 5 <= 10 → NULL AND TRUE → NULL
		var row = await ScalarRowAsync("SELECT 5 BETWEEN CAST(NULL AS INT64) AND 10");
		Assert.Null(row[0]);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Between_NullHigh_ValueAboveLow_ReturnsNull()
	{
		// 5 >= 1 AND 5 <= NULL → TRUE AND NULL → NULL
		var row = await ScalarRowAsync("SELECT 5 BETWEEN 1 AND CAST(NULL AS INT64)");
		Assert.Null(row[0]);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Between_NullValue_ReturnsNull()
	{
		// NULL >= 1 AND NULL <= 10 → NULL AND NULL → NULL
		var row = await ScalarRowAsync("SELECT CAST(NULL AS INT64) BETWEEN 1 AND 10");
		Assert.Null(row[0]);
	}

	// --- SUBSTR NULL position/length ---

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#substr
	//   "Returns NULL if any input is NULL."

	[Fact]
	public async Task Substr_NullPosition_ReturnsNull()
	{
		var row = await ScalarRowAsync("SELECT SUBSTR('hello', CAST(NULL AS INT64))");
		Assert.Null(row[0]);
	}

	[Fact]
	public async Task Substr_NullLength_ReturnsNull()
	{
		var row = await ScalarRowAsync("SELECT SUBSTR('hello', 1, CAST(NULL AS INT64))");
		Assert.Null(row[0]);
	}

	[Fact]
	public async Task Substr_NullString_ReturnsNull()
	{
		var row = await ScalarRowAsync("SELECT SUBSTR(CAST(NULL AS STRING), 1, 2)");
		Assert.Null(row[0]);
	}
}
