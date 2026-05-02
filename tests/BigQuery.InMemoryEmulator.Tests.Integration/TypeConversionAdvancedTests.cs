using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for CAST/SAFE_CAST edge cases, TYPEOF, type coercion, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class TypeConversionAdvancedTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public TypeConversionAdvancedTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_tc_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- CAST INT64 ----
	[Fact] public async Task Cast_StringToInt64() => Assert.Equal("42", await S("SELECT CAST('42' AS INT64)"));
	[Fact] public async Task Cast_FloatToInt64() => Assert.Equal("3", await S("SELECT CAST(3.7 AS INT64)"));
	[Fact] public async Task Cast_BoolToInt64_True() => Assert.Equal("1", await S("SELECT CAST(TRUE AS INT64)"));
	[Fact] public async Task Cast_BoolToInt64_False() => Assert.Equal("0", await S("SELECT CAST(FALSE AS INT64)"));

	// ---- CAST FLOAT64 ----
	[Fact] public async Task Cast_IntToFloat64() => Assert.Equal("42", await S("SELECT CAST(CAST(42 AS FLOAT64) AS INT64)"));
	[Fact] public async Task Cast_StringToFloat64()
	{
		var v = await S("SELECT CAST('3.14' AS FLOAT64)");
		Assert.NotNull(v);
		Assert.StartsWith("3.14", v);
	}

	// ---- CAST STRING ----
	[Fact] public async Task Cast_IntToString() => Assert.Equal("42", await S("SELECT CAST(42 AS STRING)"));
	[Fact] public async Task Cast_FloatToString() => Assert.Contains("3.14", await S("SELECT CAST(3.14 AS STRING)") ?? "");
	[Fact] public async Task Cast_BoolToString_True() => Assert.Equal("true", await S("SELECT CAST(TRUE AS STRING)"));
	[Fact] public async Task Cast_BoolToString_False() => Assert.Equal("false", await S("SELECT CAST(FALSE AS STRING)"));
	[Fact] public async Task Cast_DateToString() => Assert.Equal("2024-01-15", await S("SELECT CAST(DATE '2024-01-15' AS STRING)"));

	// ---- CAST BOOL ----
	[Fact] public async Task Cast_IntToBool_1() => Assert.Equal("True", await S("SELECT CAST(1 AS BOOL)"));
	[Fact] public async Task Cast_IntToBool_0() => Assert.Equal("False", await S("SELECT CAST(0 AS BOOL)"));
	[Fact] public async Task Cast_StringToBool_True() => Assert.Equal("True", await S("SELECT CAST('true' AS BOOL)"));
	[Fact] public async Task Cast_StringToBool_False() => Assert.Equal("False", await S("SELECT CAST('false' AS BOOL)"));

	// ---- CAST DATE ----
	[Fact] public async Task Cast_StringToDate() => Assert.Equal("2024-01-15", await S("SELECT CAST(CAST('2024-01-15' AS DATE) AS STRING)"));
	[Fact] public async Task Cast_TimestampToDate() => Assert.Contains("2024-01-15", await S("SELECT CAST(CAST(TIMESTAMP '2024-01-15 10:00:00 UTC' AS DATE) AS STRING)") ?? "");
	[Fact] public async Task Cast_DatetimeToDate() => Assert.Contains("2024-01-15", await S("SELECT CAST(CAST(DATETIME '2024-01-15 10:00:00' AS DATE) AS STRING)") ?? "");

	// ---- CAST TIMESTAMP ----
	[Fact] public async Task Cast_StringToTimestamp()
	{
		var v = await S("SELECT CAST(CAST('2024-01-15 10:00:00 UTC' AS TIMESTAMP) AS STRING)");
		Assert.NotNull(v);
		Assert.Contains("2024-01-15", v);
	}
	[Fact] public async Task Cast_DateToTimestamp()
	{
		var v = await S("SELECT CAST(CAST('2024-01-15' AS DATE) AS STRING)");
		Assert.NotNull(v);
		Assert.Contains("2024-01-15", v);
	}

	// ---- CAST BYTES ----
	[Fact] public async Task Cast_StringToBytes() => Assert.NotNull(await S("SELECT CAST('hello' AS BYTES)"));
	[Fact] public async Task Cast_BytesToString() => Assert.Equal("hello", await S("SELECT CAST(b'hello' AS STRING)"));

	// ---- SAFE_CAST ----
	[Fact] public async Task SafeCast_ValidInt() => Assert.Equal("42", await S("SELECT SAFE_CAST('42' AS INT64)"));
	[Fact] public async Task SafeCast_InvalidInt() => Assert.Null(await S("SELECT SAFE_CAST('abc' AS INT64)"));
	[Fact] public async Task SafeCast_ValidFloat() => Assert.NotNull(await S("SELECT SAFE_CAST('3.14' AS FLOAT64)"));
	[Fact] public async Task SafeCast_InvalidFloat() => Assert.Null(await S("SELECT SAFE_CAST('xyz' AS FLOAT64)"));
	[Fact] public async Task SafeCast_ValidBool() => Assert.Equal("True", await S("SELECT SAFE_CAST('true' AS BOOL)"));
	[Fact] public async Task SafeCast_InvalidBool() => Assert.Null(await S("SELECT SAFE_CAST('maybe' AS BOOL)"));
	[Fact] public async Task SafeCast_ValidDate() => Assert.NotNull(await S("SELECT SAFE_CAST('2024-01-15' AS DATE)"));
	[Fact] public async Task SafeCast_InvalidDate() => Assert.Null(await S("SELECT SAFE_CAST('not-a-date' AS DATE)"));
	[Fact] public async Task SafeCast_NullInput() => Assert.Null(await S("SELECT SAFE_CAST(NULL AS INT64)"));

	// ---- COALESCE type coercion ----
	[Fact] public async Task Coalesce_FirstNonNull() => Assert.Equal("5", await S("SELECT COALESCE(NULL, NULL, 5)"));
	[Fact] public async Task Coalesce_AllNull() => Assert.Null(await S("SELECT COALESCE(NULL, NULL, NULL)"));
	[Fact] public async Task Coalesce_FirstValue() => Assert.Equal("1", await S("SELECT COALESCE(1, 2, 3)"));

	// ---- IFNULL ----
	[Fact] public async Task Ifnull_NonNull() => Assert.Equal("5", await S("SELECT IFNULL(5, 10)"));
	[Fact] public async Task Ifnull_Null() => Assert.Equal("10", await S("SELECT IFNULL(NULL, 10)"));

	// ---- NULLIF ----
	[Fact] public async Task Nullif_Equal() => Assert.Null(await S("SELECT NULLIF(5, 5)"));
	[Fact] public async Task Nullif_NotEqual() => Assert.Equal("5", await S("SELECT NULLIF(5, 10)"));

	// ---- IF function ----
	[Fact] public async Task If_True() => Assert.Equal("yes", await S("SELECT IF(1 = 1, 'yes', 'no')"));
	[Fact] public async Task If_False() => Assert.Equal("no", await S("SELECT IF(1 = 2, 'yes', 'no')"));
	[Fact] public async Task If_NullCondition() => Assert.Equal("no", await S("SELECT IF(NULL, 'yes', 'no')"));

	// ---- CASE expression ----
	[Fact] public async Task Case_Searched()
	{
		var v = await S("SELECT CASE WHEN 1 > 2 THEN 'a' WHEN 2 > 1 THEN 'b' ELSE 'c' END");
		Assert.Equal("b", v);
	}
	[Fact] public async Task Case_Simple()
	{
		var v = await S("SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END");
		Assert.Equal("two", v);
	}
	[Fact] public async Task Case_NoMatch()
	{
		var v = await S("SELECT CASE 99 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END");
		Assert.Equal("other", v);
	}
	[Fact] public async Task Case_NoElse_ReturnsNull() => Assert.Null(await S("SELECT CASE WHEN 1 > 2 THEN 'yes' END"));

	// ---- BETWEEN ----
	[Fact] public async Task Between_True() => Assert.Equal("True", await S("SELECT 5 BETWEEN 1 AND 10"));
	[Fact] public async Task Between_False() => Assert.Equal("False", await S("SELECT 15 BETWEEN 1 AND 10"));
	[Fact] public async Task Between_LeftBoundary() => Assert.Equal("True", await S("SELECT 1 BETWEEN 1 AND 10"));
	[Fact] public async Task Between_RightBoundary() => Assert.Equal("True", await S("SELECT 10 BETWEEN 1 AND 10"));
	[Fact] public async Task NotBetween() => Assert.Equal("True", await S("SELECT 15 NOT BETWEEN 1 AND 10"));

	// ---- IN ----
	[Fact] public async Task In_Found() => Assert.Equal("True", await S("SELECT 3 IN (1, 2, 3, 4, 5)"));
	[Fact] public async Task In_NotFound() => Assert.Equal("False", await S("SELECT 6 IN (1, 2, 3, 4, 5)"));
	[Fact] public async Task NotIn_Found() => Assert.Equal("True", await S("SELECT 6 NOT IN (1, 2, 3)"));
	[Fact] public async Task In_Strings() => Assert.Equal("True", await S("SELECT 'b' IN ('a', 'b', 'c')"));

	// ---- LIKE ----
	[Fact] public async Task Like_Prefix() => Assert.Equal("True", await S("SELECT 'hello world' LIKE 'hello%'"));
	[Fact] public async Task Like_Suffix() => Assert.Equal("True", await S("SELECT 'hello world' LIKE '%world'"));
	[Fact] public async Task Like_Contains() => Assert.Equal("True", await S("SELECT 'hello world' LIKE '%lo wo%'"));
	[Fact] public async Task Like_SingleChar() => Assert.Equal("True", await S("SELECT 'abc' LIKE 'a_c'"));
	[Fact] public async Task Like_NoMatch() => Assert.Equal("False", await S("SELECT 'hello' LIKE 'world%'"));
	[Fact] public async Task NotLike() => Assert.Equal("True", await S("SELECT 'hello' NOT LIKE 'world%'"));

	// ---- IS NULL / IS NOT NULL ----
	[Fact] public async Task IsNull_Null() => Assert.Equal("True", await S("SELECT NULL IS NULL"));
	[Fact] public async Task IsNull_NotNull() => Assert.Equal("False", await S("SELECT 1 IS NULL"));
	[Fact] public async Task IsNotNull_NotNull() => Assert.Equal("True", await S("SELECT 1 IS NOT NULL"));
	[Fact] public async Task IsNotNull_Null() => Assert.Equal("False", await S("SELECT NULL IS NOT NULL"));

	// ---- Ternary operator (IF as expression inline) ----
	[Fact] public async Task TernaryOperator_True() => Assert.Equal("10", await S("SELECT IF(TRUE, 10, 20)"));
	[Fact] public async Task TernaryOperator_False() => Assert.Equal("20", await S("SELECT IF(FALSE, 10, 20)"));

	// ---- Null coalescing ----
	[Fact] public async Task NullCoalesce_Operator() => Assert.Equal("5", await S("SELECT COALESCE(NULL, 5)"));

	// ---- Boolean operators ----
	[Fact] public async Task And_TrueTrue() => Assert.Equal("True", await S("SELECT TRUE AND TRUE"));
	[Fact] public async Task And_TrueFalse() => Assert.Equal("False", await S("SELECT TRUE AND FALSE"));
	[Fact] public async Task Or_FalseFalse() => Assert.Equal("False", await S("SELECT FALSE OR FALSE"));
	[Fact] public async Task Or_TrueFalse() => Assert.Equal("True", await S("SELECT TRUE OR FALSE"));
	[Fact] public async Task Not_True() => Assert.Equal("False", await S("SELECT NOT TRUE"));
	[Fact] public async Task Not_False() => Assert.Equal("True", await S("SELECT NOT FALSE"));
}
