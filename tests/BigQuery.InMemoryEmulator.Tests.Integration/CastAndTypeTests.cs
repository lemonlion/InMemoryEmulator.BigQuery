using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for CAST, SAFE_CAST, and type coercion.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class CastAndTypeTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public CastAndTypeTests(BigQuerySession session) => _session = session;
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

	// ---- CAST to INT64 ----
	[Fact] public async Task Cast_StrToInt() => Assert.Equal("42", await Scalar("SELECT CAST('42' AS INT64)"));
	[Fact] public async Task Cast_StrToInt_Neg() => Assert.Equal("-5", await Scalar("SELECT CAST('-5' AS INT64)"));
	[Fact] public async Task Cast_StrToInt_Zero() => Assert.Equal("0", await Scalar("SELECT CAST('0' AS INT64)"));
	[Fact] public async Task Cast_FloatToInt() => Assert.Equal("4", await Scalar("SELECT CAST(3.7 AS INT64)"));
	[Fact] public async Task Cast_BoolToInt_True() => Assert.Equal("1", await Scalar("SELECT CAST(TRUE AS INT64)"));
	[Fact] public async Task Cast_BoolToInt_False() => Assert.Equal("0", await Scalar("SELECT CAST(FALSE AS INT64)"));
	[Fact] public async Task Cast_FloatToInt_Neg() => Assert.Equal("-4", await Scalar("SELECT CAST(-3.7 AS INT64)"));

	// ---- CAST to FLOAT64 ----
	[Fact] public async Task Cast_StrToFloat() => Assert.Equal("3.14", await Scalar("SELECT CAST('3.14' AS FLOAT64)"));
	[Fact] public async Task Cast_IntToFloat() => Assert.Equal("5", await Scalar("SELECT CAST(CAST(5 AS FLOAT64) AS INT64)"));
	[Fact] public async Task Cast_StrToFloat_Neg() => Assert.Equal("-2.5", await Scalar("SELECT CAST('-2.5' AS FLOAT64)"));
	[Fact] public async Task Cast_StrToFloat_Zero() => Assert.Equal("0", await Scalar("SELECT CAST('0.0' AS FLOAT64)"));

	// ---- CAST to STRING ----
	[Fact] public async Task Cast_IntToStr() => Assert.Equal("42", await Scalar("SELECT CAST(42 AS STRING)"));
	[Fact] public async Task Cast_FloatToStr() => Assert.Equal("3.14", await Scalar("SELECT CAST(3.14 AS STRING)"));
	[Fact] public async Task Cast_BoolToStr_True() => Assert.Equal("true", await Scalar("SELECT CAST(TRUE AS STRING)"));
	[Fact] public async Task Cast_BoolToStr_False() => Assert.Equal("false", await Scalar("SELECT CAST(FALSE AS STRING)"));
	[Fact] public async Task Cast_DateToStr() => Assert.Equal("2024-01-15", await Scalar("SELECT CAST(DATE '2024-01-15' AS STRING)"));
	[Fact] public async Task Cast_NegIntToStr() => Assert.Equal("-5", await Scalar("SELECT CAST(-5 AS STRING)"));

	// ---- CAST to BOOL ----
	[Fact] public async Task Cast_IntToBool_True() => Assert.Equal("True", await Scalar("SELECT CAST(1 AS BOOL)"));
	[Fact] public async Task Cast_IntToBool_False() => Assert.Equal("False", await Scalar("SELECT CAST(0 AS BOOL)"));
	[Fact] public async Task Cast_StrToBool_True() => Assert.Equal("True", await Scalar("SELECT CAST('true' AS BOOL)"));
	[Fact] public async Task Cast_StrToBool_False() => Assert.Equal("False", await Scalar("SELECT CAST('false' AS BOOL)"));

	// ---- CAST to DATE ----
	[Fact] public async Task Cast_StrToDate()
	{
		var v = await Scalar("SELECT CAST('2024-01-15' AS DATE)");
		Assert.NotNull(v);
		Assert.Contains("2024-01-15", v!);
	}

	[Fact] public async Task Cast_TimestampToDate()
	{
		var v = await Scalar("SELECT CAST(TIMESTAMP '2024-01-15T10:30:00+00:00' AS DATE)");
		Assert.NotNull(v);
		Assert.Contains("2024-01-15", v!);
	}

	// ---- CAST to NUMERIC ----
	[Fact] public async Task Cast_IntToNumeric() => Assert.Equal("42", await Scalar("SELECT CAST(42 AS NUMERIC)"));
	[Fact] public async Task Cast_FloatToNumeric() => Assert.Equal("3.14", await Scalar("SELECT CAST(3.14 AS NUMERIC)"));
	[Fact] public async Task Cast_StrToNumeric() => Assert.Equal("3.14", await Scalar("SELECT CAST('3.14' AS NUMERIC)"));

	// ---- SAFE_CAST ----
	[Fact] public async Task SafeCast_Valid() => Assert.Equal("42", await Scalar("SELECT SAFE_CAST('42' AS INT64)"));
	[Fact] public async Task SafeCast_Invalid() => Assert.Null(await Scalar("SELECT SAFE_CAST('abc' AS INT64)"));
	[Fact] public async Task SafeCast_FloatValid() => Assert.Equal("3.14", await Scalar("SELECT SAFE_CAST('3.14' AS FLOAT64)"));
	[Fact] public async Task SafeCast_FloatInvalid() => Assert.Null(await Scalar("SELECT SAFE_CAST('xyz' AS FLOAT64)"));
	[Fact] public async Task SafeCast_BoolValid() => Assert.Equal("True", await Scalar("SELECT SAFE_CAST('true' AS BOOL)"));
	[Fact] public async Task SafeCast_BoolInvalid() => Assert.Null(await Scalar("SELECT SAFE_CAST('maybe' AS BOOL)"));
	[Fact] public async Task SafeCast_DateValid()
	{
		var v = await Scalar("SELECT SAFE_CAST('2024-01-15' AS DATE)");
		Assert.NotNull(v);
		Assert.Contains("2024-01-15", v!);
	}
	[Fact] public async Task SafeCast_DateInvalid() => Assert.Null(await Scalar("SELECT SAFE_CAST('not-a-date' AS DATE)"));
	[Fact] public async Task SafeCast_NullToInt() => Assert.Null(await Scalar("SELECT SAFE_CAST(NULL AS INT64)"));
	[Fact] public async Task SafeCast_NullToStr() => Assert.Null(await Scalar("SELECT SAFE_CAST(NULL AS STRING)"));

	// ---- Implicit type coercion ----
	[Fact] public async Task Coerce_IntPlusFloat() { var v = await Scalar("SELECT 1 + 2.5"); Assert.Equal("3.5", v); }
	[Fact] public async Task Coerce_IntDivision() { var v = await Scalar("SELECT 10 / 3"); Assert.NotNull(v); }

	// ---- Nested CAST ----
	[Fact] public async Task Cast_IntToStrToInt() => Assert.Equal("42", await Scalar("SELECT CAST(CAST(42 AS STRING) AS INT64)"));
	[Fact] public async Task Cast_FloatToIntToStr() => Assert.Equal("4", await Scalar("SELECT CAST(CAST(3.7 AS INT64) AS STRING)"));
	[Fact] public async Task Cast_BoolToIntToStr() => Assert.Equal("1", await Scalar("SELECT CAST(CAST(TRUE AS INT64) AS STRING)"));

	// ---- CAST in expressions ----
	[Fact] public async Task Cast_InAdd() => Assert.Equal("5", await Scalar("SELECT CAST('3' AS INT64) + 2"));
	[Fact] public async Task Cast_InConcat() => Assert.Equal("value42", await Scalar("SELECT CONCAT('value', CAST(42 AS STRING))"));
	[Fact] public async Task Cast_InWhere()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT x FROM UNNEST([1,2,3,4,5]) AS x WHERE CAST(x AS STRING) = '3'",
			parameters: null);
		var rows = result.ToList();
		Assert.Single(rows);
		Assert.Equal("3", rows[0][0]?.ToString());
	}

	// ---- Special float values ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#floating_point_literals
	//   "inf and -inf represent positive and negative infinity."
	[Fact] public async Task Float_Infinity()
	{
		var v = await Scalar("SELECT CAST('inf' AS FLOAT64)");
		Assert.NotNull(v);
		// .NET 8 returns ∞ for double.PositiveInfinity.ToString()
		Assert.True(v == "Infinity" || v == "\u221E", $"Expected Infinity or ∞, got: {v}");
	}

	[Fact] public async Task Float_NegInfinity()
	{
		var v = await Scalar("SELECT CAST('-inf' AS FLOAT64)");
		Assert.NotNull(v);
		Assert.True(v == "-Infinity" || v == "-\u221E", $"Expected -Infinity or -∞, got: {v}");
	}

	[Fact] public async Task Float_NaN()
	{
		var v = await Scalar("SELECT CAST('nan' AS FLOAT64)");
		Assert.NotNull(v);
		Assert.Contains("NaN", v!, StringComparison.OrdinalIgnoreCase);
	}

	// ---- Type checking / IS ----
	[Fact] public async Task IsNull_CastNull() => Assert.Equal("True", await Scalar("SELECT CAST(NULL AS INT64) IS NULL"));

	// ---- CAST with ARRAY ----
	[Fact] public async Task Cast_ArrayLength() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH([1,2,3])"));
}
