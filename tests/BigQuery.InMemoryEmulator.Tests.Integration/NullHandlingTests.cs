using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for NULL handling: NULL comparisons, NULL in functions, NULL propagation,
/// COALESCE, IFNULL, NULLIF, IS NULL, IS NOT NULL, NVL patterns.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class NullHandlingTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public NullHandlingTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- NULL arithmetic ----
	[Fact] public async Task Null_PlusInt() => Assert.Null(await Scalar("SELECT NULL + 1"));
	[Fact] public async Task Null_MinusInt() => Assert.Null(await Scalar("SELECT NULL - 1"));
	[Fact] public async Task Null_MulInt() => Assert.Null(await Scalar("SELECT NULL * 5"));
	[Fact] public async Task Null_PlusNull() => Assert.Null(await Scalar("SELECT CAST(NULL AS INT64) + CAST(NULL AS INT64)"));
	[Fact] public async Task Int_PlusNull() => Assert.Null(await Scalar("SELECT 1 + NULL"));

	// ---- NULL comparison ----
	[Fact] public async Task Null_Eq_Null() => Assert.Null(await Scalar("SELECT NULL = NULL"));
	[Fact] public async Task Null_Neq_Null() => Assert.Null(await Scalar("SELECT NULL != NULL"));
	[Fact] public async Task Null_Lt_Null() => Assert.Null(await Scalar("SELECT NULL < NULL"));
	[Fact] public async Task Null_Gt_Null() => Assert.Null(await Scalar("SELECT NULL > NULL"));
	[Fact] public async Task Int_Eq_Null() => Assert.Null(await Scalar("SELECT 1 = NULL"));
	[Fact] public async Task Null_Eq_Int() => Assert.Null(await Scalar("SELECT NULL = 1"));

	// ---- IS NULL / IS NOT NULL ----
	[Fact] public async Task IsNull_Null() => Assert.Equal("True", await Scalar("SELECT NULL IS NULL"));
	[Fact] public async Task IsNull_Int() => Assert.Equal("False", await Scalar("SELECT 1 IS NULL"));
	[Fact] public async Task IsNull_EmptyStr() => Assert.Equal("False", await Scalar("SELECT '' IS NULL"));
	[Fact] public async Task IsNull_Zero() => Assert.Equal("False", await Scalar("SELECT 0 IS NULL"));
	[Fact] public async Task IsNull_False2() => Assert.Equal("False", await Scalar("SELECT FALSE IS NULL"));
	[Fact] public async Task IsNotNull_Null() => Assert.Equal("False", await Scalar("SELECT NULL IS NOT NULL"));
	[Fact] public async Task IsNotNull_Int() => Assert.Equal("True", await Scalar("SELECT 1 IS NOT NULL"));
	[Fact] public async Task IsNotNull_EmptyStr() => Assert.Equal("True", await Scalar("SELECT '' IS NOT NULL"));
	[Fact] public async Task IsNotNull_Zero() => Assert.Equal("True", await Scalar("SELECT 0 IS NOT NULL"));

	// ---- NULL in CASE ----
	[Fact] public async Task Case_NullWhen() => Assert.Equal("was null", await Scalar("SELECT CASE WHEN NULL IS NULL THEN 'was null' ELSE 'not null' END"));
	[Fact] public async Task Case_NullResult() => Assert.Null(await Scalar("SELECT CASE WHEN FALSE THEN 'x' END"));
	[Fact] public async Task Case_NullElse() => Assert.Null(await Scalar("SELECT CASE WHEN FALSE THEN 'x' ELSE NULL END"));

	// ---- COALESCE with NULL ----
	[Fact] public async Task Coalesce_FirstNull() => Assert.Equal("2", await Scalar("SELECT COALESCE(NULL, 2)"));
	[Fact] public async Task Coalesce_AllNull2() => Assert.Null(await Scalar("SELECT COALESCE(NULL, NULL)"));
	[Fact] public async Task Coalesce_ManyNull() => Assert.Equal("5", await Scalar("SELECT COALESCE(NULL, NULL, NULL, NULL, 5)"));
	[Fact] public async Task Coalesce_FirstNonNull() => Assert.Equal("1", await Scalar("SELECT COALESCE(1, NULL)"));
	[Fact] public async Task Coalesce_ZeroNotNull() => Assert.Equal("0", await Scalar("SELECT COALESCE(0, 5)"));

	// ---- IFNULL ----
	[Fact] public async Task Ifnull_NonNull2() => Assert.Equal("1", await Scalar("SELECT IFNULL(1, 2)"));
	[Fact] public async Task Ifnull_Null2() => Assert.Equal("2", await Scalar("SELECT IFNULL(CAST(NULL AS INT64), 2)"));
	[Fact] public async Task Ifnull_Zero2() => Assert.Equal("0", await Scalar("SELECT IFNULL(0, 5)"));
	[Fact] public async Task Ifnull_EmptyStr2() => Assert.Equal("", await Scalar("SELECT IFNULL('', 'x')"));

	// ---- NULLIF ----
	[Fact] public async Task Nullif_Same2() => Assert.Null(await Scalar("SELECT NULLIF(1, 1)"));
	[Fact] public async Task Nullif_Diff2() => Assert.Equal("1", await Scalar("SELECT NULLIF(1, 2)"));
	[Fact] public async Task Nullif_StrSame2() => Assert.Null(await Scalar("SELECT NULLIF('a', 'a')"));
	[Fact] public async Task Nullif_StrDiff2() => Assert.Equal("a", await Scalar("SELECT NULLIF('a', 'b')"));

	// ---- NULL in string functions ----
	[Fact] public async Task Null_Length() => Assert.Null(await Scalar("SELECT LENGTH(CAST(NULL AS STRING))"));
	[Fact] public async Task Null_Upper() => Assert.Null(await Scalar("SELECT UPPER(CAST(NULL AS STRING))"));
	[Fact] public async Task Null_Lower() => Assert.Null(await Scalar("SELECT LOWER(CAST(NULL AS STRING))"));
	[Fact] public async Task Null_Trim() => Assert.Null(await Scalar("SELECT TRIM(CAST(NULL AS STRING))"));
	[Fact] public async Task Null_Substr() => Assert.Null(await Scalar("SELECT SUBSTR(CAST(NULL AS STRING), 1)"));
	[Fact] public async Task Null_Replace() => Assert.Null(await Scalar("SELECT REPLACE(CAST(NULL AS STRING), 'a', 'b')"));
	[Fact] public async Task Null_Reverse() => Assert.Null(await Scalar("SELECT REVERSE(CAST(NULL AS STRING))"));

	// ---- NULL in math functions ----
	[Fact] public async Task Null_Abs() => Assert.Null(await Scalar("SELECT ABS(CAST(NULL AS INT64))"));
	[Fact] public async Task Null_Round() => Assert.Null(await Scalar("SELECT ROUND(CAST(NULL AS FLOAT64))"));
	[Fact] public async Task Null_Ceil() => Assert.Null(await Scalar("SELECT CEIL(CAST(NULL AS FLOAT64))"));
	[Fact] public async Task Null_Floor() => Assert.Null(await Scalar("SELECT FLOOR(CAST(NULL AS FLOAT64))"));
	[Fact] public async Task Null_Sign() => Assert.Null(await Scalar("SELECT SIGN(CAST(NULL AS INT64))"));
	[Fact] public async Task Null_Mod() => Assert.Null(await Scalar("SELECT MOD(CAST(NULL AS INT64), 2)"));

	// ---- NULL in aggregates ----
	[Fact] public async Task Null_CountStar() => Assert.Equal("3", await Scalar("SELECT COUNT(*) FROM UNNEST([1, NULL, 3]) AS x"));
	[Fact] public async Task Null_CountExpr() => Assert.Equal("2", await Scalar("SELECT COUNT(x) FROM UNNEST([1, NULL, 3]) AS x"));
	[Fact] public async Task Null_Sum() => Assert.Equal("4", await Scalar("SELECT SUM(x) FROM UNNEST([1, NULL, 3]) AS x"));
	[Fact] public async Task Null_Min() => Assert.Equal("1", await Scalar("SELECT MIN(x) FROM UNNEST([1, NULL, 3]) AS x"));
	[Fact] public async Task Null_Max() => Assert.Equal("3", await Scalar("SELECT MAX(x) FROM UNNEST([1, NULL, 3]) AS x"));

	// ---- NULL in BETWEEN ----
	[Fact] public async Task Null_Between() => Assert.Null(await Scalar("SELECT NULL BETWEEN 1 AND 10"));
	[Fact] public async Task Null_NotBetween() => Assert.Null(await Scalar("SELECT NULL NOT BETWEEN 1 AND 10"));

	// ---- NULL in IN ----
	[Fact] public async Task Null_In() => Assert.Null(await Scalar("SELECT NULL IN (1, 2, 3)"));

	// ---- NULL boolean logic ----
	[Fact] public async Task Null_And_False() => Assert.Equal("False", await Scalar("SELECT NULL AND FALSE"));
	[Fact] public async Task Null_Or_True() => Assert.Equal("True", await Scalar("SELECT NULL OR TRUE"));
	[Fact] public async Task Null_Or_False() => Assert.Null(await Scalar("SELECT NULL OR FALSE"));
	[Fact] public async Task Not_Null() => Assert.Null(await Scalar("SELECT NOT CAST(NULL AS BOOL)"));

	// ---- NULL CAST ----
	[Fact] public async Task Cast_NullInt() => Assert.Null(await Scalar("SELECT CAST(NULL AS INT64)"));
	[Fact] public async Task Cast_NullStr() => Assert.Null(await Scalar("SELECT CAST(NULL AS STRING)"));
	[Fact] public async Task Cast_NullFloat() => Assert.Null(await Scalar("SELECT CAST(NULL AS FLOAT64)"));
	[Fact] public async Task Cast_NullBool() => Assert.Null(await Scalar("SELECT CAST(NULL AS BOOL)"));
	[Fact] public async Task Cast_NullDate() => Assert.Null(await Scalar("SELECT CAST(NULL AS DATE)"));

	// ---- NULL in GREATEST / LEAST ----
	[Fact] public async Task Greatest_WithNull() => Assert.Null(await Scalar("SELECT GREATEST(1, NULL, 3)"));
	[Fact] public async Task Least_WithNull() => Assert.Null(await Scalar("SELECT LEAST(1, NULL, 3)"));
	[Fact] public async Task Greatest_AllNull2() => Assert.Null(await Scalar("SELECT GREATEST(CAST(NULL AS INT64), CAST(NULL AS INT64))"));
	[Fact] public async Task Least_AllNull2() => Assert.Null(await Scalar("SELECT LEAST(CAST(NULL AS INT64), CAST(NULL AS INT64))"));

	// ---- Complex NULL expressions ----
	[Fact] public async Task Complex_CoalesceCase() => Assert.Equal("5", await Scalar("SELECT COALESCE(CASE WHEN FALSE THEN 1 END, 5)"));
	[Fact] public async Task Complex_NullifCoalesce() => Assert.Equal("10", await Scalar("SELECT COALESCE(NULLIF(1, 1), 10)"));
	[Fact] public async Task Complex_IfnullNullif() => Assert.Equal("99", await Scalar("SELECT IFNULL(NULLIF(5, 5), 99)"));
	[Fact] public async Task Complex_ChainedCoalesce() => Assert.Equal("42", await Scalar("SELECT COALESCE(NULL, NULL, COALESCE(NULL, 42))"));
}
