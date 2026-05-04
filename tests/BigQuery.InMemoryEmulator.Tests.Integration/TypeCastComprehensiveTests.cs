using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Comprehensive tests for type casting and conversion: CAST, SAFE_CAST, implicit coercion.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class TypeCastComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public TypeCastComprehensiveTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> S(string sql)
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync(sql, parameters: null);
		var rows = r.ToList();
		if (rows.Count == 0) return null;
		var val = rows[0][0];
		if (val is DateTime dt) return dt.TimeOfDay == TimeSpan.Zero ? dt.ToString("yyyy-MM-dd") : dt.ToString("yyyy-MM-dd HH:mm:ss");
		if (val is DateTimeOffset dto) return dto.TimeOfDay == TimeSpan.Zero ? dto.ToString("yyyy-MM-dd") : dto.ToString("yyyy-MM-dd HH:mm:ss");
		return val?.ToString();
	}

	// ---- INT64 conversions ----
	[Fact] public async Task Cast_StringToInt() => Assert.Equal("42", await S("SELECT CAST('42' AS INT64)"));
	[Fact] public async Task Cast_FloatToInt() => Assert.Equal("43", await S("SELECT CAST(42.9 AS INT64)"));
	[Fact] public async Task Cast_BoolToInt_True() => Assert.Equal("1", await S("SELECT CAST(true AS INT64)"));
	[Fact] public async Task Cast_BoolToInt_False() => Assert.Equal("0", await S("SELECT CAST(false AS INT64)"));
	[Fact] public async Task Cast_NegativeStringToInt() => Assert.Equal("-5", await S("SELECT CAST('-5' AS INT64)"));

	// ---- FLOAT64 conversions ----
	[Fact] public async Task Cast_StringToFloat() => Assert.Equal("3.14", await S("SELECT CAST('3.14' AS FLOAT64)"));
	[Fact] public async Task Cast_IntToFloat() => Assert.Equal("42", await S("SELECT CAST(42 AS FLOAT64)"));
	[Fact] public async Task Cast_BoolToFloat() => Assert.Equal("1", await S("SELECT CAST(true AS FLOAT64)"));

	// ---- STRING conversions ----
	[Fact] public async Task Cast_IntToString() => Assert.Equal("42", await S("SELECT CAST(42 AS STRING)"));
	[Fact] public async Task Cast_FloatToString() => Assert.Equal("3.14", await S("SELECT CAST(3.14 AS STRING)"));
	[Fact] public async Task Cast_BoolToString_True() => Assert.Equal("true", await S("SELECT CAST(true AS STRING)"));
	[Fact] public async Task Cast_BoolToString_False() => Assert.Equal("false", await S("SELECT CAST(false AS STRING)"));
	[Fact] public async Task Cast_DateToString() => Assert.Equal("2024-01-15", await S("SELECT CAST(DATE '2024-01-15' AS STRING)"));

	// ---- BOOL conversions ----
	[Fact] public async Task Cast_IntToBool_One() => Assert.Equal("True", await S("SELECT CAST(1 AS BOOL)"));
	[Fact] public async Task Cast_IntToBool_Zero() => Assert.Equal("False", await S("SELECT CAST(0 AS BOOL)"));
	[Fact] public async Task Cast_StringToBool_True() => Assert.Equal("True", await S("SELECT CAST('true' AS BOOL)"));
	[Fact] public async Task Cast_StringToBool_False() => Assert.Equal("False", await S("SELECT CAST('false' AS BOOL)"));

	// ---- DATE conversions ----
	[Fact] public async Task Cast_StringToDate() => Assert.Equal("2024-01-15", await S("SELECT CAST('2024-01-15' AS DATE)"));
	[Fact] public async Task Cast_TimestampToDate() => Assert.Equal("2024-01-15", await S("SELECT CAST(TIMESTAMP '2024-01-15 10:30:00 UTC' AS DATE)"));

	// ---- TIMESTAMP conversions ----
	[Fact] public async Task Cast_StringToTimestamp()
	{
		var v = await S("SELECT CAST('2024-01-15 10:30:00' AS TIMESTAMP)");
		Assert.NotNull(v);
		Assert.Contains("2024-01-15", v);
	}
	[Fact] public async Task Cast_DateToTimestamp()
	{
		var v = await S("SELECT CAST(DATE '2024-01-15' AS TIMESTAMP)");
		Assert.NotNull(v);
		Assert.Contains("2024-01-15", v);
	}

	// ---- SAFE_CAST ----
	[Fact] public async Task SafeCast_ValidInt() => Assert.Equal("42", await S("SELECT SAFE_CAST('42' AS INT64)"));
	[Fact] public async Task SafeCast_InvalidInt() => Assert.Null(await S("SELECT SAFE_CAST('abc' AS INT64)"));
	[Fact] public async Task SafeCast_ValidFloat() => Assert.Equal("3.14", await S("SELECT SAFE_CAST('3.14' AS FLOAT64)"));
	[Fact] public async Task SafeCast_InvalidFloat() => Assert.Null(await S("SELECT SAFE_CAST('xyz' AS FLOAT64)"));
	[Fact] public async Task SafeCast_ValidBool() => Assert.Equal("True", await S("SELECT SAFE_CAST('true' AS BOOL)"));
	[Fact] public async Task SafeCast_InvalidBool() => Assert.Null(await S("SELECT SAFE_CAST('maybe' AS BOOL)"));
	[Fact] public async Task SafeCast_Null() => Assert.Null(await S("SELECT SAFE_CAST(NULL AS INT64)"));
	[Fact] public async Task SafeCast_ValidDate() => Assert.Equal("2024-01-15", await S("SELECT SAFE_CAST('2024-01-15' AS DATE)"));
	[Fact] public async Task SafeCast_InvalidDate() => Assert.Null(await S("SELECT SAFE_CAST('not-a-date' AS DATE)"));

	// ---- NUMERIC precision ----
	[Fact] public async Task Cast_StringToNumeric() => Assert.NotNull(await S("SELECT CAST('123.456' AS NUMERIC)"));
	[Fact] public async Task Cast_IntToNumeric() => Assert.Equal("42", await S("SELECT CAST(42 AS NUMERIC)"));

	// ---- BYTES conversions ----
	[Fact] public async Task Cast_StringToBytes() => Assert.NotNull(await S("SELECT CAST('hello' AS BYTES)"));
	[Fact] public async Task Cast_BytesToString() => Assert.Equal("hello", await S("SELECT CAST(b'hello' AS STRING)"));

	// ---- Chained casts ----
	[Fact] public async Task Cast_IntToStringToInt() => Assert.Equal("42", await S("SELECT CAST(CAST(42 AS STRING) AS INT64)"));
	[Fact] public async Task Cast_FloatToIntToString() => Assert.Equal("43", await S("SELECT CAST(CAST(42.7 AS INT64) AS STRING)"));
	[Fact] public async Task Cast_BoolToIntToString() => Assert.Equal("1", await S("SELECT CAST(CAST(true AS INT64) AS STRING)"));

	// ---- CAST with NULL ----
	[Fact] public async Task Cast_NullToInt() => Assert.Null(await S("SELECT CAST(NULL AS INT64)"));
	[Fact] public async Task Cast_NullToString() => Assert.Null(await S("SELECT CAST(NULL AS STRING)"));
	[Fact] public async Task Cast_NullToFloat() => Assert.Null(await S("SELECT CAST(NULL AS FLOAT64)"));
	[Fact] public async Task Cast_NullToBool() => Assert.Null(await S("SELECT CAST(NULL AS BOOL)"));
	[Fact] public async Task Cast_NullToDate() => Assert.Null(await S("SELECT CAST(NULL AS DATE)"));

	// ---- Edge cases ----
	[Fact] public async Task Cast_EmptyStringToBytes() => Assert.NotNull(await S("SELECT CAST('' AS BYTES)"));
	[Fact] public async Task Cast_ZeroToFloat() => Assert.Equal("0", await S("SELECT CAST(0 AS FLOAT64)"));
	[Fact] public async Task Cast_NegativeFloatToInt() => Assert.Equal("-4", await S("SELECT CAST(-3.7 AS INT64)"));

	// ---- In expressions ----
	[Fact] public async Task Cast_InArithmetic() => Assert.Equal("52", await S("SELECT CAST('42' AS INT64) + 10"));
	[Fact] public async Task Cast_InComparison() => Assert.Equal("True", await S("SELECT CAST('42' AS INT64) > 10"));
	[Fact] public async Task Cast_InConcat() => Assert.Equal("Value: 42", await S("SELECT CONCAT('Value: ', CAST(42 AS STRING))"));
}
