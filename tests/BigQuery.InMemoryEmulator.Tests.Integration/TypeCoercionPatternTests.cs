using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for type coercion, CAST, SAFE_CAST with various type combinations.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class TypeCoercionPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public TypeCoercionPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_coerce_{Guid.NewGuid():N}"[..30];
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

	// INT64 casts
	[Fact] public async Task Cast_StringToInt() => Assert.Equal("42", await Scalar("SELECT CAST('42' AS INT64)"));
	[Fact] public async Task Cast_FloatToInt() => Assert.Equal("3", await Scalar("SELECT CAST(3.7 AS INT64)"));
	[Fact] public async Task Cast_BoolToInt_True() => Assert.Equal("1", await Scalar("SELECT CAST(TRUE AS INT64)"));
	[Fact] public async Task Cast_BoolToInt_False() => Assert.Equal("0", await Scalar("SELECT CAST(FALSE AS INT64)"));
	[Fact] public async Task Cast_NullToInt() => Assert.Null(await Scalar("SELECT CAST(NULL AS INT64)"));

	// FLOAT64 casts
	[Fact] public async Task Cast_IntToFloat() => Assert.Equal("42", await Scalar("SELECT CAST(CAST(42 AS FLOAT64) AS INT64)"));
	[Fact] public async Task Cast_StringToFloat()
	{
		var v = double.Parse((await Scalar("SELECT CAST('3.14' AS FLOAT64)"))!);
		Assert.True(v > 3.13 && v < 3.15);
	}

	// STRING casts
	[Fact] public async Task Cast_IntToString() => Assert.Equal("123", await Scalar("SELECT CAST(123 AS STRING)"));
	[Fact] public async Task Cast_FloatToString() => Assert.Contains("3.14", await Scalar("SELECT CAST(3.14 AS STRING)")!);
	[Fact] public async Task Cast_BoolToString_True() => Assert.Equal("true", await Scalar("SELECT CAST(TRUE AS STRING)"));
	[Fact] public async Task Cast_BoolToString_False() => Assert.Equal("false", await Scalar("SELECT CAST(FALSE AS STRING)"));
	[Fact] public async Task Cast_DateToString() => Assert.Equal("2024-03-15", await Scalar("SELECT CAST(DATE '2024-03-15' AS STRING)"));
	[Fact] public async Task Cast_NullToString() => Assert.Null(await Scalar("SELECT CAST(NULL AS STRING)"));

	// BOOL casts
	[Fact] public async Task Cast_IntToBool_Nonzero() => Assert.Equal("True", await Scalar("SELECT CAST(1 AS BOOL)"));
	[Fact] public async Task Cast_IntToBool_Zero() => Assert.Equal("False", await Scalar("SELECT CAST(0 AS BOOL)"));

	// DATE casts
	[Fact] public async Task Cast_StringToDate() => Assert.Equal("2024-06-15", await Scalar("SELECT CAST(CAST('2024-06-15' AS DATE) AS STRING)"));
	[Fact] public async Task Cast_TimestampToDate() => Assert.Contains("2024-01-15", (await Scalar("SELECT CAST(CAST(CAST('2024-01-15 10:30:00' AS TIMESTAMP) AS DATE) AS STRING)"))!);

	// TIMESTAMP casts
	[Fact] public async Task Cast_StringToTimestamp()
	{
		var result = await Scalar("SELECT CAST(CAST('2024-01-15 10:30:00' AS TIMESTAMP) AS STRING)");
		Assert.Contains("2024-01-15", result!);
		Assert.Contains("10:30:00", result!);
	}

	// SAFE_CAST
	[Fact] public async Task SafeCast_ValidInt() => Assert.Equal("42", await Scalar("SELECT SAFE_CAST('42' AS INT64)"));
	[Fact] public async Task SafeCast_InvalidInt() => Assert.Null(await Scalar("SELECT SAFE_CAST('abc' AS INT64)"));
	[Fact] public async Task SafeCast_InvalidFloat() => Assert.Null(await Scalar("SELECT SAFE_CAST('not_a_number' AS FLOAT64)"));
	[Fact] public async Task SafeCast_InvalidBool() => Assert.Null(await Scalar("SELECT SAFE_CAST('maybe' AS BOOL)"));
	[Fact] public async Task SafeCast_ValidBool() => Assert.Equal("True", await Scalar("SELECT SAFE_CAST('true' AS BOOL)"));
	[Fact] public async Task SafeCast_ValidDate() => Assert.Equal("2024-03-15", await Scalar("SELECT CAST(SAFE_CAST('2024-03-15' AS DATE) AS STRING)"));
	[Fact] public async Task SafeCast_InvalidDate() => Assert.Null(await Scalar("SELECT SAFE_CAST('not-a-date' AS DATE)"));
	[Fact] public async Task SafeCast_NullPreserved() => Assert.Null(await Scalar("SELECT SAFE_CAST(NULL AS INT64)"));

	// Implicit coercion in expressions
	[Fact] public async Task Coerce_IntPlusFloat()
	{
		var v = double.Parse((await Scalar("SELECT 1 + 2.5"))!);
		Assert.True(v > 3.4 && v < 3.6);
	}
	[Fact] public async Task Coerce_IntCompareFloat() => Assert.Equal("True", await Scalar("SELECT 5 > 3.5"));

	// CAST in WHERE
	[Fact] public async Task Cast_InWhere()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($"CREATE TABLE `{_datasetId}.t` (val STRING)", parameters: null);
		await client.ExecuteQueryAsync($"INSERT INTO `{_datasetId}.t` (val) VALUES ('10'), ('20'), ('30')", parameters: null);
		var result = await Scalar("SELECT val FROM `{ds}.t` WHERE CAST(val AS INT64) > 15 ORDER BY val LIMIT 1");
		Assert.Equal("20", result);
	}

	// Multiple CASTs
	[Fact] public async Task Cast_Chain() => Assert.Equal("42", await Scalar("SELECT CAST(CAST(CAST(42 AS STRING) AS INT64) AS STRING)"));
	[Fact] public async Task Cast_ExpressionResult() => Assert.Equal("15", await Scalar("SELECT CAST(5 + 10 AS STRING)"));

	// BYTES
	[Fact] public async Task Cast_StringToBytes() => Assert.NotNull(await Scalar("SELECT CAST('hello' AS BYTES)"));

	// NUMERIC
	[Fact] public async Task Cast_IntToNumeric() => Assert.Contains("42", (await Scalar("SELECT CAST(42 AS NUMERIC)"))!.ToString());
}