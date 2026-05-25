using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Tests for LOGICAL_AND and LOGICAL_OR aggregate functions with NULL handling.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#logical_and
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#logical_or
/// </summary>
[Collection(IntegrationCollection.Name)]
public class LogicalAggregateNullTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public LogicalAggregateNullTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- LOGICAL_AND ----

	/// <summary>
	/// LOGICAL_AND with TRUE and NULL should return TRUE (NULLs are ignored).
	/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#logical_and
	///   "Returns the logical AND of all non-NULL expressions."
	/// </summary>
	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task LogicalAnd_TrueAndNull_ReturnsTrue()
	{
		var result = await Scalar("SELECT LOGICAL_AND(x) FROM UNNEST([TRUE, NULL, TRUE]) AS x");
		Assert.Equal("True", result);
	}

	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task LogicalAnd_AllTrue_ReturnsTrue()
	{
		var result = await Scalar("SELECT LOGICAL_AND(x) FROM UNNEST([TRUE, TRUE]) AS x");
		Assert.Equal("True", result);
	}

	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task LogicalAnd_FalseAndNull_ReturnsFalse()
	{
		var result = await Scalar("SELECT LOGICAL_AND(x) FROM UNNEST([TRUE, NULL, FALSE]) AS x");
		Assert.Equal("False", result);
	}

	/// <summary>
	/// LOGICAL_AND with all NULL returns NULL.
	/// Ref: "Returns NULL if there are zero input rows or expression evaluates to NULL for all rows."
	/// </summary>
	[Fact]
	public async Task LogicalAnd_AllNull_ReturnsNull()
	{
		var result = await Scalar("SELECT LOGICAL_AND(x) FROM UNNEST([CAST(NULL AS BOOL), NULL]) AS x");
		Assert.Null(result);
	}

	// ---- LOGICAL_OR ----

	/// <summary>
	/// LOGICAL_OR with FALSE and NULL should return FALSE (NULLs are ignored).
	/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#logical_or
	///   "Returns the logical OR of all non-NULL expressions."
	/// </summary>
	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task LogicalOr_FalseAndNull_ReturnsFalse()
	{
		var result = await Scalar("SELECT LOGICAL_OR(x) FROM UNNEST([FALSE, NULL, FALSE]) AS x");
		Assert.Equal("False", result);
	}

	[Fact]
	public async Task LogicalOr_AllFalse_ReturnsFalse()
	{
		var result = await Scalar("SELECT LOGICAL_OR(x) FROM UNNEST([FALSE, FALSE]) AS x");
		Assert.Equal("False", result);
	}

	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task LogicalOr_TrueAndNull_ReturnsTrue()
	{
		var result = await Scalar("SELECT LOGICAL_OR(x) FROM UNNEST([FALSE, NULL, TRUE]) AS x");
		Assert.Equal("True", result);
	}

	/// <summary>
	/// LOGICAL_OR with all NULL returns NULL.
	/// </summary>
	[Fact]
	public async Task LogicalOr_AllNull_ReturnsNull()
	{
		var result = await Scalar("SELECT LOGICAL_OR(x) FROM UNNEST([CAST(NULL AS BOOL), NULL]) AS x");
		Assert.Null(result);
	}
}
