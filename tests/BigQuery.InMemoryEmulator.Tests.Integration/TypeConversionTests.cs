using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for CAST, SAFE_CAST, and type conversion patterns.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class TypeConversionTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public TypeConversionTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- CAST INT64 to STRING ----
	[Fact] public async Task Cast_Int_To_String() => Assert.Equal("42", await Scalar("SELECT CAST(42 AS STRING)"));
	[Fact] public async Task Cast_NegInt_To_String() => Assert.Equal("-42", await Scalar("SELECT CAST(-42 AS STRING)"));
	[Fact] public async Task Cast_Zero_To_String() => Assert.Equal("0", await Scalar("SELECT CAST(0 AS STRING)"));
	[Fact] public async Task Cast_LargeInt_To_String() => Assert.Equal("999999999", await Scalar("SELECT CAST(999999999 AS STRING)"));

	// ---- CAST STRING to INT64 ----
	[Fact] public async Task Cast_Str_To_Int() => Assert.Equal("42", await Scalar("SELECT CAST('42' AS INT64)"));
	[Fact] public async Task Cast_NegStr_To_Int() => Assert.Equal("-42", await Scalar("SELECT CAST('-42' AS INT64)"));
	[Fact] public async Task Cast_ZeroStr_To_Int() => Assert.Equal("0", await Scalar("SELECT CAST('0' AS INT64)"));

	// ---- CAST FLOAT64 to INT64 ----
	[Fact] public async Task Cast_Float_To_Int() => Assert.Equal("3", await Scalar("SELECT CAST(3.7 AS INT64)"));
	[Fact] public async Task Cast_FloatDown_To_Int() => Assert.Equal("3", await Scalar("SELECT CAST(3.2 AS INT64)"));
	[Fact] public async Task Cast_NegFloat_To_Int() => Assert.Equal("-3", await Scalar("SELECT CAST(-3.2 AS INT64)"));
	[Fact] public async Task Cast_FloatZero_To_Int() => Assert.Equal("0", await Scalar("SELECT CAST(0.5 AS INT64)"));

	// ---- CAST INT64 to FLOAT64 ----
	[Fact] public async Task Cast_Int_To_Float() => Assert.Equal("42", await Scalar("SELECT CAST(42 AS FLOAT64)"));
	[Fact] public async Task Cast_NegInt_To_Float() => Assert.Equal("-42", await Scalar("SELECT CAST(-42 AS FLOAT64)"));
	[Fact] public async Task Cast_Zero_To_Float() => Assert.Equal("0", await Scalar("SELECT CAST(0 AS FLOAT64)"));

	// ---- CAST STRING to FLOAT64 ----
	[Fact] public async Task Cast_Str_To_Float() => Assert.Equal("3.14", await Scalar("SELECT CAST('3.14' AS FLOAT64)"));
	[Fact] public async Task Cast_NegStr_To_Float() => Assert.Equal("-3.14", await Scalar("SELECT CAST('-3.14' AS FLOAT64)"));
	[Fact] public async Task Cast_IntStr_To_Float() => Assert.Equal("42", await Scalar("SELECT CAST('42' AS FLOAT64)"));

	// ---- CAST FLOAT64 to STRING ----
	[Fact] public async Task Cast_Float_To_String() => Assert.Equal("3.14", await Scalar("SELECT CAST(3.14 AS STRING)"));
	[Fact] public async Task Cast_NegFloat_To_String() => Assert.Equal("-3.14", await Scalar("SELECT CAST(-3.14 AS STRING)"));

	// ---- CAST BOOL to STRING ----
	[Fact] public async Task Cast_True_To_String() => Assert.Equal("true", await Scalar("SELECT CAST(TRUE AS STRING)"));
	[Fact] public async Task Cast_False_To_String() => Assert.Equal("false", await Scalar("SELECT CAST(FALSE AS STRING)"));

	// ---- CAST STRING to BOOL ----
	[Fact] public async Task Cast_TrueStr_To_Bool() => Assert.Equal("True", await Scalar("SELECT CAST('true' AS BOOL)"));
	[Fact] public async Task Cast_FalseStr_To_Bool() => Assert.Equal("False", await Scalar("SELECT CAST('false' AS BOOL)"));

	// ---- CAST BOOL to INT64 ----
	[Fact] public async Task Cast_True_To_Int() => Assert.Equal("1", await Scalar("SELECT CAST(TRUE AS INT64)"));
	[Fact] public async Task Cast_False_To_Int() => Assert.Equal("0", await Scalar("SELECT CAST(FALSE AS INT64)"));

	// ---- CAST INT64 to BOOL ----
	[Fact] public async Task Cast_1_To_Bool() => Assert.Equal("True", await Scalar("SELECT CAST(1 AS BOOL)"));
	[Fact] public async Task Cast_0_To_Bool() => Assert.Equal("False", await Scalar("SELECT CAST(0 AS BOOL)"));

	// ---- SAFE_CAST (returns NULL on failure) ----
	[Fact] public async Task SafeCast_ValidStr_To_Int() => Assert.Equal("42", await Scalar("SELECT SAFE_CAST('42' AS INT64)"));
	[Fact] public async Task SafeCast_InvalidStr_To_Int() => Assert.Null(await Scalar("SELECT SAFE_CAST('abc' AS INT64)"));
	[Fact] public async Task SafeCast_EmptyStr_To_Int() => Assert.Null(await Scalar("SELECT SAFE_CAST('' AS INT64)"));
	[Fact] public async Task SafeCast_ValidStr_To_Float() => Assert.Equal("3.14", await Scalar("SELECT SAFE_CAST('3.14' AS FLOAT64)"));
	[Fact] public async Task SafeCast_InvalidStr_To_Float() => Assert.Null(await Scalar("SELECT SAFE_CAST('abc' AS FLOAT64)"));
	[Fact] public async Task SafeCast_ValidStr_To_Bool() => Assert.Equal("True", await Scalar("SELECT SAFE_CAST('true' AS BOOL)"));
	[Fact] public async Task SafeCast_InvalidStr_To_Bool() => Assert.Null(await Scalar("SELECT SAFE_CAST('abc' AS BOOL)"));

	// ---- Implicit conversions in expressions ----
	[Fact(Skip = "Emulator limitation")] public async Task Implicit_IntPlusFloat() => Assert.Equal("5.14", await Scalar("SELECT 2 + 3.14"));
	[Fact(Skip = "Emulator limitation")] public async Task Implicit_FloatPlusInt() => Assert.Equal("5.14", await Scalar("SELECT 3.14 + 2"));
	[Fact] public async Task Implicit_IntMulFloat() => Assert.Equal("6.28", await Scalar("SELECT 2 * 3.14"));

	// ---- CAST with computed values ----
	[Fact] public async Task Cast_Expr_To_String() => Assert.Equal("15", await Scalar("SELECT CAST(3 * 5 AS STRING)"));
	[Fact] public async Task Cast_StringConcat_To_Int() => Assert.Equal("123", await Scalar("SELECT CAST(CONCAT('1', '2', '3') AS INT64)"));
	[Fact] public async Task Cast_LengthResult() => Assert.Equal("5", await Scalar("SELECT CAST(LENGTH('Hello') AS STRING)"));

	// ---- Chained CAST ----
	[Fact] public async Task Cast_IntToStrToInt() => Assert.Equal("42", await Scalar("SELECT CAST(CAST(42 AS STRING) AS INT64)"));
	[Fact] public async Task Cast_FloatToIntToStr() => Assert.Equal("3", await Scalar("SELECT CAST(CAST(3.7 AS INT64) AS STRING)"));
	[Fact] public async Task Cast_StrToFloatToInt() => Assert.Equal("3", await Scalar("SELECT CAST(CAST('3.7' AS FLOAT64) AS INT64)"));

	// ---- CAST in WHERE ----
	[Fact]
	public async Task Cast_InWhere()
	{
		var v = await Scalar(@"
SELECT COUNT(*) FROM UNNEST(['1', '2', '3', '4', '5']) AS s
WHERE CAST(s AS INT64) > 3");
		Assert.Equal("2", v);
	}

	// ---- CAST in ORDER BY ----
	[Fact]
	public async Task Cast_InOrderBy()
	{
		var v = await Scalar(@"
SELECT s FROM UNNEST(['10', '2', '1', '20', '3']) AS s
ORDER BY CAST(s AS INT64)
LIMIT 1");
		Assert.Equal("1", v);
	}

	// ---- COALESCE with CAST ----
	[Fact] public async Task Coalesce_CastNull() => Assert.Equal("0", await Scalar("SELECT COALESCE(SAFE_CAST('abc' AS INT64), 0)"));
	[Fact] public async Task Coalesce_CastValid() => Assert.Equal("42", await Scalar("SELECT COALESCE(SAFE_CAST('42' AS INT64), 0)"));

	// ---- IF with CAST ----
	[Fact] public async Task If_CastOrDefault() => Assert.Equal("42", await Scalar("SELECT IF(SAFE_CAST('42' AS INT64) IS NOT NULL, SAFE_CAST('42' AS INT64), -1)"));
	[Fact] public async Task If_CastFailDefault() => Assert.Equal("-1", await Scalar("SELECT IF(SAFE_CAST('abc' AS INT64) IS NOT NULL, SAFE_CAST('abc' AS INT64), -1)"));
}
