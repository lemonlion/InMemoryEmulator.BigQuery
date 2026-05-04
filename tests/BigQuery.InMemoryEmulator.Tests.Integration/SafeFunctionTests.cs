using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for SAFE_ prefix functions and error handling in BigQuery.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-reference#safe_prefix
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class SafeFunctionTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public SafeFunctionTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_safe_{Guid.NewGuid():N}"[..30];
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

	// SAFE_CAST
	[Fact] public async Task SafeCast_ValidInt() => Assert.Equal("42", await Scalar("SELECT SAFE_CAST('42' AS INT64)"));
	[Fact] public async Task SafeCast_InvalidInt() => Assert.Null(await Scalar("SELECT SAFE_CAST('abc' AS INT64)"));
	[Fact] public async Task SafeCast_ValidFloat() => Assert.Equal("3.14", await Scalar("SELECT SAFE_CAST('3.14' AS FLOAT64)"));
	[Fact] public async Task SafeCast_InvalidFloat() => Assert.Null(await Scalar("SELECT SAFE_CAST('xyz' AS FLOAT64)"));
	[Fact] public async Task SafeCast_ValidBool() => Assert.Equal("True", await Scalar("SELECT SAFE_CAST('true' AS BOOL)"));
	[Fact] public async Task SafeCast_InvalidBool() => Assert.Null(await Scalar("SELECT SAFE_CAST('maybe' AS BOOL)"));
	[Fact] public async Task SafeCast_ValidDate() => Assert.Equal("2024-01-15", await Scalar("SELECT CAST(SAFE_CAST('2024-01-15' AS DATE) AS STRING)"));
	[Fact] public async Task SafeCast_InvalidDate() => Assert.Null(await Scalar("SELECT SAFE_CAST('not-a-date' AS DATE)"));
	[Fact] public async Task SafeCast_NullInput() => Assert.Null(await Scalar("SELECT SAFE_CAST(NULL AS INT64)"));
	[Fact] public async Task SafeCast_IntToString() => Assert.Equal("100", await Scalar("SELECT SAFE_CAST(100 AS STRING)"));
	[Fact] public async Task SafeCast_FloatToInt() => Assert.Equal("4", await Scalar("SELECT SAFE_CAST(3.7 AS INT64)"));
	[Fact] public async Task SafeCast_StringToBytes() => Assert.NotNull(await Scalar("SELECT SAFE_CAST('hello' AS BYTES)"));

	// SAFE_DIVIDE
	[Fact] public async Task SafeDivide_Normal() => Assert.Equal("5", await Scalar("SELECT SAFE_DIVIDE(10, 2)"));
	[Fact] public async Task SafeDivide_ByZero() => Assert.Null(await Scalar("SELECT SAFE_DIVIDE(10, 0)"));
	[Fact] public async Task SafeDivide_ZeroByZero() => Assert.Null(await Scalar("SELECT SAFE_DIVIDE(0, 0)"));
	[Fact] public async Task SafeDivide_NullNumerator() => Assert.Null(await Scalar("SELECT SAFE_DIVIDE(NULL, 5)"));
	[Fact] public async Task SafeDivide_NullDenominator() => Assert.Null(await Scalar("SELECT SAFE_DIVIDE(10, NULL)"));
	[Fact] public async Task SafeDivide_FloatResult() => Assert.Equal("3.3333333333333335", await Scalar("SELECT SAFE_DIVIDE(10.0, 3.0)"));
	[Fact] public async Task SafeDivide_NegativeResult() => Assert.Equal("-5", await Scalar("SELECT SAFE_DIVIDE(-10, 2)"));

	// SAFE_MULTIPLY
	[Fact] public async Task SafeMultiply_Normal() => Assert.Equal("20", await Scalar("SELECT SAFE_MULTIPLY(4, 5)"));
	[Fact] public async Task SafeMultiply_NullArg() => Assert.Null(await Scalar("SELECT SAFE_MULTIPLY(NULL, 5)"));

	// SAFE_NEGATE
	[Fact] public async Task SafeNegate_Positive() => Assert.Equal("-5", await Scalar("SELECT SAFE_NEGATE(5)"));
	[Fact] public async Task SafeNegate_Negative() => Assert.Equal("5", await Scalar("SELECT SAFE_NEGATE(-5)"));
	[Fact] public async Task SafeNegate_Zero() => Assert.Equal("0", await Scalar("SELECT SAFE_NEGATE(0)"));
	[Fact] public async Task SafeNegate_Null() => Assert.Null(await Scalar("SELECT SAFE_NEGATE(NULL)"));

	// SAFE_ADD
	[Fact] public async Task SafeAdd_Normal() => Assert.Equal("15", await Scalar("SELECT SAFE_ADD(10, 5)"));
	[Fact] public async Task SafeAdd_NullArg() => Assert.Null(await Scalar("SELECT SAFE_ADD(NULL, 5)"));

	// SAFE_SUBTRACT
	[Fact] public async Task SafeSubtract_Normal() => Assert.Equal("5", await Scalar("SELECT SAFE_SUBTRACT(10, 5)"));
	[Fact] public async Task SafeSubtract_NullArg() => Assert.Null(await Scalar("SELECT SAFE_SUBTRACT(10, NULL)"));

	// IF and IFNull patterns (common error-handling patterns)
	[Fact] public async Task IfNull_WithDefault() => Assert.Equal("0", await Scalar("SELECT IFNULL(NULL, 0)"));
	[Fact] public async Task IfNull_NonNull() => Assert.Equal("42", await Scalar("SELECT IFNULL(42, 0)"));
	[Fact] public async Task NullIf_Equal() => Assert.Null(await Scalar("SELECT NULLIF(5, 5)"));
	[Fact] public async Task NullIf_NotEqual() => Assert.Equal("5", await Scalar("SELECT NULLIF(5, 3)"));
	[Fact] public async Task Coalesce_FirstNonNull() => Assert.Equal("3", await Scalar("SELECT COALESCE(NULL, NULL, 3, 4)"));
	[Fact] public async Task Coalesce_AllNull() => Assert.Null(await Scalar("SELECT COALESCE(NULL, NULL, NULL)"));
	[Fact] public async Task If_True() => Assert.Equal("yes", await Scalar("SELECT IF(1 = 1, 'yes', 'no')"));
	[Fact] public async Task If_False() => Assert.Equal("no", await Scalar("SELECT IF(1 = 2, 'yes', 'no')"));
	[Fact] public async Task If_NullCondition() => Assert.Equal("no", await Scalar("SELECT IF(NULL, 'yes', 'no')"));

	// SAFE division combined with other functions
	[Fact] public async Task SafeDivide_InExpression() => Assert.Equal("50", await Scalar("SELECT SAFE_DIVIDE(100, 2) * 1"));
	[Fact] public async Task SafeDivide_WithCoalesce() => Assert.Equal("0", await Scalar("SELECT COALESCE(SAFE_DIVIDE(1, 0), 0)"));
}
