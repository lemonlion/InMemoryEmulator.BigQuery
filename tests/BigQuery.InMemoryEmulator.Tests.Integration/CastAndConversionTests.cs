using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for CAST and type conversion functions.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class CastAndConversionTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public CastAndConversionTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		if (rows.Count == 0) return null;
		var val = rows[0][0];
		return val switch
		{
			DateTime dt => dt.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture),
			_ => val?.ToString()
		};
	}

	// ---- INT64 casts ----
	[Fact] public async Task Cast_StringToInt() => Assert.Equal("42", await Scalar("SELECT CAST('42' AS INT64)"));
	[Fact] public async Task Cast_NegStringToInt() => Assert.Equal("-42", await Scalar("SELECT CAST('-42' AS INT64)"));
	[Fact] public async Task Cast_FloatToInt() => Assert.Equal("3", await Scalar("SELECT CAST(3.9 AS INT64)"));
	[Fact] public async Task Cast_BoolTrueToInt() => Assert.Equal("1", await Scalar("SELECT CAST(TRUE AS INT64)"));
	[Fact] public async Task Cast_BoolFalseToInt() => Assert.Equal("0", await Scalar("SELECT CAST(FALSE AS INT64)"));
	[Fact] public async Task Cast_IntToInt() => Assert.Equal("42", await Scalar("SELECT CAST(42 AS INT64)"));
	[Fact] public async Task Cast_ZeroToInt() => Assert.Equal("0", await Scalar("SELECT CAST(0 AS INT64)"));
	[Fact] public async Task Cast_NullToInt() => Assert.Null(await Scalar("SELECT CAST(NULL AS INT64)"));

	// ---- FLOAT64 casts ----
	[Fact] public async Task Cast_StringToFloat() { var v = double.Parse(await Scalar("SELECT CAST('3.14' AS FLOAT64)") ?? "0"); Assert.Equal(3.14, v, 2); }
	[Fact] public async Task Cast_IntToFloat() { var v = double.Parse(await Scalar("SELECT CAST(42 AS FLOAT64)") ?? "0"); Assert.Equal(42.0, v); }
	[Fact] public async Task Cast_BoolToFloat() { var v = double.Parse(await Scalar("SELECT CAST(TRUE AS FLOAT64)") ?? "0"); Assert.Equal(1.0, v); }
	[Fact] public async Task Cast_NullToFloat() => Assert.Null(await Scalar("SELECT CAST(NULL AS FLOAT64)"));
	[Fact] public async Task Cast_NegFloatStr() { var v = double.Parse(await Scalar("SELECT CAST('-1.5' AS FLOAT64)") ?? "0"); Assert.Equal(-1.5, v, 1); }

	// ---- STRING casts ----
	[Fact] public async Task Cast_IntToString() => Assert.Equal("42", await Scalar("SELECT CAST(42 AS STRING)"));
	[Fact] public async Task Cast_FloatToString() { var v = await Scalar("SELECT CAST(3.14 AS STRING)"); Assert.Contains("3.14", v); }
	[Fact] public async Task Cast_BoolTrueToString() { var v = await Scalar("SELECT CAST(TRUE AS STRING)"); Assert.NotNull(v); }
	[Fact] public async Task Cast_BoolFalseToString() { var v = await Scalar("SELECT CAST(FALSE AS STRING)"); Assert.NotNull(v); }
	[Fact] public async Task Cast_NullToString() => Assert.Null(await Scalar("SELECT CAST(NULL AS STRING)"));

	// ---- BOOL casts ----
	[Fact] public async Task Cast_IntToBool_One() => Assert.Equal("True", await Scalar("SELECT CAST(1 AS BOOL)"));
	[Fact] public async Task Cast_IntToBool_Zero() => Assert.Equal("False", await Scalar("SELECT CAST(0 AS BOOL)"));
	[Fact] public async Task Cast_StringToBool_True() => Assert.Equal("True", await Scalar("SELECT CAST('true' AS BOOL)"));
	[Fact] public async Task Cast_StringToBool_False() => Assert.Equal("False", await Scalar("SELECT CAST('false' AS BOOL)"));
	[Fact] public async Task Cast_NullToBool() => Assert.Null(await Scalar("SELECT CAST(NULL AS BOOL)"));

	// ---- DATE casts ----
	[Fact] public async Task Cast_StringToDate() { var v = await Scalar("SELECT CAST('2024-01-15' AS DATE)"); Assert.Contains("2024-01-15", v); }
	[Fact] public async Task Cast_NullToDate() => Assert.Null(await Scalar("SELECT CAST(NULL AS DATE)"));

	// ---- SAFE_CAST ----
	[Fact] public async Task SafeCast_ValidInt() => Assert.Equal("42", await Scalar("SELECT SAFE_CAST('42' AS INT64)"));
	[Fact] public async Task SafeCast_InvalidInt() => Assert.Null(await Scalar("SELECT SAFE_CAST('abc' AS INT64)"));
	[Fact] public async Task SafeCast_ValidFloat() { var v = double.Parse(await Scalar("SELECT SAFE_CAST('3.14' AS FLOAT64)") ?? "0"); Assert.Equal(3.14, v, 2); }
	[Fact] public async Task SafeCast_InvalidFloat() => Assert.Null(await Scalar("SELECT SAFE_CAST('xyz' AS FLOAT64)"));
	[Fact] public async Task SafeCast_ValidBool() => Assert.Equal("True", await Scalar("SELECT SAFE_CAST('true' AS BOOL)"));
	[Fact] public async Task SafeCast_InvalidBool() => Assert.Null(await Scalar("SELECT SAFE_CAST('maybe' AS BOOL)"));
	[Fact] public async Task SafeCast_ValidDate() { var v = await Scalar("SELECT SAFE_CAST('2024-01-15' AS DATE)"); Assert.NotNull(v); }
	[Fact] public async Task SafeCast_InvalidDate() => Assert.Null(await Scalar("SELECT SAFE_CAST('not-a-date' AS DATE)"));
	[Fact] public async Task SafeCast_Null() => Assert.Null(await Scalar("SELECT SAFE_CAST(NULL AS INT64)"));
	[Fact] public async Task SafeCast_IntToString() => Assert.Equal("42", await Scalar("SELECT SAFE_CAST(42 AS STRING)"));

	// ---- Implicit conversions ----
	[Fact] public async Task Implicit_IntPlusFloat() { var v = double.Parse(await Scalar("SELECT 1 + 2.5") ?? "0"); Assert.Equal(3.5, v); }
	[Fact] public async Task Implicit_IntCompareFloat() => Assert.Equal("True", await Scalar("SELECT 3 = 3.0"));
	[Fact] public async Task Implicit_IntInFloatExpr() { var v = double.Parse(await Scalar("SELECT 10 / 3.0") ?? "0"); Assert.Equal(3.333, v, 2); }

	// ---- Nested casts ----
	[Fact] public async Task Cast_IntToFloatToString() { var v = await Scalar("SELECT CAST(CAST(42 AS FLOAT64) AS STRING)"); Assert.NotNull(v); }
	[Fact] public async Task Cast_StringToIntToFloat() { var v = double.Parse(await Scalar("SELECT CAST(CAST('42' AS INT64) AS FLOAT64)") ?? "0"); Assert.Equal(42.0, v); }
	[Fact] public async Task Cast_BoolToIntToString() { var v = await Scalar("SELECT CAST(CAST(TRUE AS INT64) AS STRING)"); Assert.Equal("1", v); }

	// ---- BYTES casts ----
	[Fact] public async Task Cast_StringToBytes() { var v = await Scalar("SELECT CAST(CAST('hello' AS BYTES) AS STRING)"); Assert.Equal("hello", v); }

	// ---- Numeric type edge cases ----
	[Fact] public async Task Cast_LargeIntToString() => Assert.Equal("9999999999", await Scalar("SELECT CAST(9999999999 AS STRING)"));
	[Fact] public async Task Cast_NegIntToFloat() { var v = double.Parse(await Scalar("SELECT CAST(-42 AS FLOAT64)") ?? "0"); Assert.Equal(-42.0, v); }
	[Fact] public async Task Cast_ZeroToFloat() { var v = double.Parse(await Scalar("SELECT CAST(0 AS FLOAT64)") ?? "0"); Assert.Equal(0.0, v); }
	[Fact] public async Task Cast_ZeroToString() => Assert.Equal("0", await Scalar("SELECT CAST(0 AS STRING)"));
	[Fact] public async Task Cast_EmptyStringToBytes() { var v = await Scalar("SELECT CAST(CAST('' AS BYTES) AS STRING)"); Assert.Equal("", v); }

	// ---- FORMAT function ----
	[Fact] public async Task Format_Int() => Assert.Equal("42", await Scalar("SELECT FORMAT('%d', 42)"));
	[Fact] public async Task Format_String() => Assert.Equal("hello", await Scalar("SELECT FORMAT('%s', 'hello')"));
	[Fact] public async Task Format_Float() { var v = await Scalar("SELECT FORMAT('%f', 3.14)"); Assert.NotNull(v); Assert.Contains("3.14", v); }
	[Fact] public async Task Format_MultiArg() => Assert.Contains("42", await Scalar("SELECT FORMAT('%d items', 42)") ?? "");
}
