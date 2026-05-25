using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 49: Integer overflow errors, unary negate overflow,
/// float modulo by zero.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests49 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public ParityVerificationTests49(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
	}

	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string> ScalarAsync(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var row = result.First();
		return row[0]?.ToString() ?? "NULL";
	}

	// ============================================================
	// Integer overflow on + operator → error
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators
	//   "All operators will throw an error if the computation result overflows."
	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Add_IntegerOverflow_ThrowsError()
	{
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAnyAsync<Exception>(async () =>
			await client.ExecuteQueryAsync(
				"SELECT 9223372036854775807 + 1", parameters: null));
	}

	// ============================================================
	// Integer overflow on * operator → error
	// ============================================================

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Multiply_IntegerOverflow_ThrowsError()
	{
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAnyAsync<Exception>(async () =>
			await client.ExecuteQueryAsync(
				"SELECT 9223372036854775807 * 2", parameters: null));
	}

	// ============================================================
	// Integer overflow on - operator → error
	// ============================================================

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Subtract_IntegerOverflow_ThrowsError()
	{
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAnyAsync<Exception>(async () =>
			await client.ExecuteQueryAsync(
				"SELECT CAST(-9223372036854775808 AS INT64) - 1", parameters: null));
	}

	// ============================================================
	// Unary negate overflow → error
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators
	//   "-X (unary minus): negates X. Throws error on overflow."
	[Fact]
	public async Task Negate_MinInt64_ThrowsError()
	{
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAnyAsync<Exception>(async () =>
			await client.ExecuteQueryAsync(
				"SELECT -(CAST(-9223372036854775808 AS INT64))", parameters: null));
	}

	// ============================================================
	// Float modulo by zero → error
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#arithmetic_operators
	//   "Division by zero using the % operator generates an error."
	[Fact]
	public async Task Mod_FloatByZero_ThrowsError()
	{
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAnyAsync<Exception>(async () =>
			await client.ExecuteQueryAsync(
				"SELECT MOD(5.0, 0.0)", parameters: null));
	}

	// ============================================================
	// SAFE versions correctly return NULL for overflow
	// ============================================================

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task SafeAdd_Overflow_ReturnsNull()
	{
		var result = await ScalarAsync("SELECT SAFE_ADD(9223372036854775807, 1)");
		Assert.Equal("NULL", result);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task SafeMultiply_Overflow_ReturnsNull()
	{
		var result = await ScalarAsync("SELECT SAFE_MULTIPLY(9223372036854775807, 2)");
		Assert.Equal("NULL", result);
	}
}
