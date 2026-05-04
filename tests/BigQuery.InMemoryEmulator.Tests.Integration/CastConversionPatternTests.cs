using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// CAST/SAFE_CAST and type coercion edge cases.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class CastConversionPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public CastConversionPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_ccp_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }

	// ---- CAST to INT64 ----
	[Fact] public async Task Cast_StringToInt() => Assert.Equal("42", await S("SELECT CAST('42' AS INT64)"));
	[Fact] public async Task Cast_FloatToInt() => Assert.Equal("3", await S("SELECT CAST(3.7 AS INT64)"));
	[Fact] public async Task Cast_BoolToInt_True() => Assert.Equal("1", await S("SELECT CAST(true AS INT64)"));
	[Fact] public async Task Cast_BoolToInt_False() => Assert.Equal("0", await S("SELECT CAST(false AS INT64)"));
	[Fact] public async Task Cast_NegFloatToInt() => Assert.Equal("-3", await S("SELECT CAST(-3.7 AS INT64)"));

	// ---- CAST to FLOAT64 ----
	[Fact] public async Task Cast_StringToFloat()
	{
		var v = await S("SELECT CAST('3.14' AS FLOAT64)");
		Assert.NotNull(v);
		Assert.StartsWith("3.14", v);
	}
	[Fact] public async Task Cast_IntToFloat()
	{
		var v = await S("SELECT CAST(5 AS FLOAT64)");
		Assert.NotNull(v);
		Assert.Contains("5", v);
	}

	// ---- CAST to STRING ----
	[Fact] public async Task Cast_IntToString() => Assert.Equal("123", await S("SELECT CAST(123 AS STRING)"));
	[Fact] public async Task Cast_FloatToString()
	{
		var v = await S("SELECT CAST(3.14 AS STRING)");
		Assert.NotNull(v);
		Assert.StartsWith("3.14", v);
	}
	[Fact] public async Task Cast_BoolToString_True() => Assert.Equal("true", await S("SELECT CAST(true AS STRING)"));
	[Fact] public async Task Cast_BoolToString_False() => Assert.Equal("false", await S("SELECT CAST(false AS STRING)"));

	// ---- CAST to BOOL ----
	[Fact] public async Task Cast_IntToBool_One() => Assert.Equal("True", await S("SELECT CAST(1 AS BOOL)"));
	[Fact] public async Task Cast_IntToBool_Zero() => Assert.Equal("False", await S("SELECT CAST(0 AS BOOL)"));
	[Fact] public async Task Cast_StringToBool_True() => Assert.Equal("True", await S("SELECT CAST('true' AS BOOL)"));
	[Fact] public async Task Cast_StringToBool_False() => Assert.Equal("False", await S("SELECT CAST('false' AS BOOL)"));

	// ---- CAST to DATE ----
	[Fact] public async Task Cast_StringToDate()
	{
		var v = await S("SELECT CAST('2024-06-15' AS DATE)");
		Assert.NotNull(v);
		Assert.Contains("2024", v);
	}
	[Fact] public async Task Cast_TimestampToDate()
	{
		var v = await S("SELECT CAST(TIMESTAMP '2024-06-15 10:30:00' AS DATE)");
		Assert.NotNull(v);
		Assert.Contains("2024", v);
	}

	// ---- CAST to TIMESTAMP ----
	[Fact] public async Task Cast_StringToTimestamp()
	{
		var v = await S("SELECT CAST('2024-06-15 10:30:00' AS TIMESTAMP)");
		Assert.NotNull(v);
		Assert.Contains("2024", v);
	}

	// ---- CAST NULL ----
	[Fact] public async Task Cast_NullToInt() => Assert.Null(await S("SELECT CAST(NULL AS INT64)"));
	[Fact] public async Task Cast_NullToString() => Assert.Null(await S("SELECT CAST(NULL AS STRING)"));
	[Fact] public async Task Cast_NullToFloat() => Assert.Null(await S("SELECT CAST(NULL AS FLOAT64)"));
	[Fact] public async Task Cast_NullToBool() => Assert.Null(await S("SELECT CAST(NULL AS BOOL)"));

	// ---- SAFE_CAST ----
	[Fact] public async Task SafeCast_ValidInt() => Assert.Equal("42", await S("SELECT SAFE_CAST('42' AS INT64)"));
	[Fact] public async Task SafeCast_InvalidInt() => Assert.Null(await S("SELECT SAFE_CAST('abc' AS INT64)"));
	[Fact] public async Task SafeCast_ValidFloat()
	{
		var v = await S("SELECT SAFE_CAST('3.14' AS FLOAT64)");
		Assert.NotNull(v);
		Assert.StartsWith("3.14", v);
	}
	[Fact] public async Task SafeCast_InvalidFloat() => Assert.Null(await S("SELECT SAFE_CAST('xxx' AS FLOAT64)"));
	[Fact] public async Task SafeCast_ValidBool() => Assert.Equal("True", await S("SELECT SAFE_CAST('true' AS BOOL)"));
	[Fact] public async Task SafeCast_InvalidBool() => Assert.Null(await S("SELECT SAFE_CAST('maybe' AS BOOL)"));
	[Fact] public async Task SafeCast_Null() => Assert.Null(await S("SELECT SAFE_CAST(NULL AS INT64)"));

	// ---- Implicit coercion in arithmetic ----
	[Fact] public async Task Implicit_IntPlusFloat() => Assert.Equal("2.5", await S("SELECT 1 + 1.5"));
	[Fact] public async Task Implicit_IntDivFloat() => Assert.Equal("2.5", await S("SELECT CAST(5 AS FLOAT64) / 2"));

	// ---- CAST in expressions ----
	[Fact] public async Task Cast_InWhere()
	{
		await Exec("CREATE TABLE `{ds}.tc` (id INT64, val STRING)");
		await Exec("INSERT INTO `{ds}.tc` VALUES (1,'10'),(2,'20'),(3,'30')");
		var v = await S("SELECT COUNT(*) FROM `{ds}.tc` WHERE CAST(val AS INT64) > 15");
		Assert.Equal("2", v);
	}
	[Fact] public async Task Cast_InOrderBy()
	{
		await Exec("CREATE TABLE `{ds}.to1` (id INT64, num STRING)");
		await Exec("INSERT INTO `{ds}.to1` VALUES (1,'10'),(2,'2'),(3,'20')");
		var rows = await Q("SELECT id FROM `{ds}.to1` ORDER BY CAST(num AS INT64)");
		Assert.Equal("2", rows[0]["id"]?.ToString()); // 2
		Assert.Equal("1", rows[1]["id"]?.ToString()); // 10
		Assert.Equal("3", rows[2]["id"]?.ToString()); // 20
	}
	[Fact] public async Task Cast_Nested() => Assert.Equal("42", await S("SELECT CAST(CAST(42 AS STRING) AS INT64)"));
	[Fact] public async Task Cast_TripleNested() => Assert.Equal("3", await S("SELECT CAST(CAST(CAST(3.14 AS STRING) AS FLOAT64) AS INT64)"));

	// ---- FORMAT ----
	[Fact] public async Task Format_Int() => Assert.Equal("42", await S("SELECT FORMAT('%d', 42)"));
	[Fact] public async Task Format_Float() => Assert.Equal("3.14", await S("SELECT FORMAT('%.2f', 3.14159)"));
	[Fact] public async Task Format_String() => Assert.Equal("hello", await S("SELECT FORMAT('%s', 'hello')"));

	// ---- CAST with BYTES ----
	[Fact] public async Task Cast_StringToBytes()
	{
		var v = await S("SELECT CAST('hello' AS BYTES)");
		Assert.NotNull(v);
	}

	// ---- CAST NUMERIC ----
	[Fact] public async Task Cast_IntToNumeric()
	{
		var v = await S("SELECT CAST(42 AS NUMERIC)");
		Assert.NotNull(v);
		Assert.Contains("42", v);
	}
	[Fact] public async Task Cast_StringToNumeric()
	{
		var v = await S("SELECT CAST('123.45' AS NUMERIC)");
		Assert.NotNull(v);
		Assert.Contains("123.45", v);
	}

	private async Task Exec(string sql) { var c = await _fixture.GetClientAsync(); await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }
}
