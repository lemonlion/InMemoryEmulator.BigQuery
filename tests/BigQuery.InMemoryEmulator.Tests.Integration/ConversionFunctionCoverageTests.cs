using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Conversion and CAST function patterns: CAST, SAFE_CAST, type coercion.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ConversionFunctionCoverageTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ConversionFunctionCoverageTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_conv_{Guid.NewGuid():N}"[..29];
		await _fixture.CreateDatasetAsync(_ds);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); if (rows.Count == 0) return null; var val = rows[0][0]; if (val is DateTime dt) return dt.ToString("yyyy-MM-dd"); return val?.ToString(); }

	// ---- CAST to INT64 ----
	[Fact] public async Task Cast_StringToInt() => Assert.Equal("42", await S("SELECT CAST('42' AS INT64)"));
	[Fact] public async Task Cast_FloatToInt() => Assert.Equal("4", await S("SELECT CAST(3.7 AS INT64)"));
	[Fact] public async Task Cast_BoolToInt_True() => Assert.Equal("1", await S("SELECT CAST(true AS INT64)"));
	[Fact] public async Task Cast_BoolToInt_False() => Assert.Equal("0", await S("SELECT CAST(false AS INT64)"));
	[Fact] public async Task Cast_NullToInt() => Assert.Null(await S("SELECT CAST(NULL AS INT64)"));

	// ---- CAST to FLOAT64 ----
	[Fact] public async Task Cast_StringToFloat() { var v = await S("SELECT CAST('3.14' AS FLOAT64)"); Assert.NotNull(v); Assert.StartsWith("3.14", v); }
	[Fact] public async Task Cast_IntToFloat() => Assert.Equal("42", await S("SELECT CAST(42 AS FLOAT64)"));
	[Fact] public async Task Cast_NullToFloat() => Assert.Null(await S("SELECT CAST(NULL AS FLOAT64)"));

	// ---- CAST to STRING ----
	[Fact] public async Task Cast_IntToString() => Assert.Equal("42", await S("SELECT CAST(42 AS STRING)"));
	[Fact] public async Task Cast_FloatToString() { var v = await S("SELECT CAST(3.14 AS STRING)"); Assert.NotNull(v); Assert.StartsWith("3.14", v); }
	[Fact] public async Task Cast_BoolToString_True() => Assert.Equal("true", await S("SELECT CAST(true AS STRING)"));
	[Fact] public async Task Cast_BoolToString_False() => Assert.Equal("false", await S("SELECT CAST(false AS STRING)"));
	[Fact] public async Task Cast_NullToString() => Assert.Null(await S("SELECT CAST(NULL AS STRING)"));

	// ---- CAST to BOOL ----
	[Fact] public async Task Cast_IntToBool_Nonzero() => Assert.Equal("True", await S("SELECT CAST(1 AS BOOL)"));
	[Fact] public async Task Cast_IntToBool_Zero() => Assert.Equal("False", await S("SELECT CAST(0 AS BOOL)"));
	[Fact] public async Task Cast_StringToBool_True() => Assert.Equal("True", await S("SELECT CAST('true' AS BOOL)"));
	[Fact] public async Task Cast_StringToBool_False() => Assert.Equal("False", await S("SELECT CAST('false' AS BOOL)"));

	// ---- CAST to DATE ----
	[Fact] public async Task Cast_StringToDate() => Assert.Equal("2024-01-15", await S("SELECT CAST('2024-01-15' AS DATE)"));
	[Fact] public async Task Cast_NullToDate() => Assert.Null(await S("SELECT CAST(NULL AS DATE)"));

	// ---- SAFE_CAST (returns NULL on failure) ----
	[Fact] public async Task SafeCast_InvalidInt() => Assert.Null(await S("SELECT SAFE_CAST('abc' AS INT64)"));
	[Fact] public async Task SafeCast_ValidInt() => Assert.Equal("42", await S("SELECT SAFE_CAST('42' AS INT64)"));
	[Fact] public async Task SafeCast_InvalidFloat() => Assert.Null(await S("SELECT SAFE_CAST('xyz' AS FLOAT64)"));
	[Fact] public async Task SafeCast_ValidFloat() { var v = await S("SELECT SAFE_CAST('3.14' AS FLOAT64)"); Assert.NotNull(v); Assert.StartsWith("3.14", v); }
	[Fact] public async Task SafeCast_InvalidBool() => Assert.Null(await S("SELECT SAFE_CAST('maybe' AS BOOL)"));
	[Fact] public async Task SafeCast_InvalidDate() => Assert.Null(await S("SELECT SAFE_CAST('not-a-date' AS DATE)"));
	[Fact] public async Task SafeCast_NullAlwaysNull() => Assert.Null(await S("SELECT SAFE_CAST(NULL AS INT64)"));

	// ---- Implicit conversions via arithmetic ----
	[Fact] public async Task Implicit_IntPlusFloat() { var v = await S("SELECT 1 + 0.5"); Assert.NotNull(v); Assert.StartsWith("1.5", v); }
	[Fact] public async Task Implicit_IntTimesFloat() { var v = await S("SELECT 2 * 1.5"); Assert.NotNull(v); Assert.StartsWith("3", v); }

	// ---- CAST in expressions ----
	[Fact] public async Task CastExpr_InArithmetic() => Assert.Equal("52", await S("SELECT CAST('42' AS INT64) + 10"));
	[Fact] public async Task CastExpr_InConcat() => Assert.Equal("Age: 30", await S("SELECT CONCAT('Age: ', CAST(30 AS STRING))"));
	[Fact] public async Task CastExpr_InWhere()
	{
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.t` (id INT64, val STRING)", parameters: null);
		await c.ExecuteQueryAsync($"INSERT INTO `{_ds}.t` VALUES (1,'10'),(2,'20'),(3,'30')", parameters: null);
		var v = await S("SELECT COUNT(*) FROM `{ds}.t` WHERE CAST(val AS INT64) > 15");
		Assert.Equal("2", v);
	}

	// ---- Multiple CAST chain ----
	[Fact] public async Task CastChain_IntToStringToInt() => Assert.Equal("42", await S("SELECT CAST(CAST(42 AS STRING) AS INT64)"));
	[Fact] public async Task CastChain_FloatToIntToString() => Assert.Equal("4", await S("SELECT CAST(CAST(3.7 AS INT64) AS STRING)"));
}
