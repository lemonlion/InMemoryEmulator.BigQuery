using Google;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Comprehensive tests for NULL handling: IS NULL, IS NOT NULL, NULL propagation, NULL in comparisons, NULL in aggregates.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#null_semantics
/// </summary>
[Collection(IntegrationCollection.Name)]
public class NullSemanticsAdvancedTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public NullSemanticsAdvancedTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_ns_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.t` (id INT64, val INT64, name STRING, flag BOOL)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.t` (id, val, name, flag) VALUES
			(1,10,'a',true),(2,NULL,'b',false),(3,30,NULL,true),
			(4,NULL,NULL,NULL),(5,50,'e',true),(6,60,'f',false)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- IS NULL / IS NOT NULL ----
	[Fact] public async Task IsNull_Column() => Assert.Equal("2", await S("SELECT COUNT(*) FROM `{ds}.t` WHERE val IS NULL"));
	[Fact] public async Task IsNotNull_Column() => Assert.Equal("4", await S("SELECT COUNT(*) FROM `{ds}.t` WHERE val IS NOT NULL"));
	[Fact] public async Task IsNull_String() => Assert.Equal("2", await S("SELECT COUNT(*) FROM `{ds}.t` WHERE name IS NULL"));
	[Fact] public async Task IsNull_Bool() => Assert.Equal("1", await S("SELECT COUNT(*) FROM `{ds}.t` WHERE flag IS NULL"));
	[Fact] public async Task IsNull_Literal() => Assert.Equal("True", await S("SELECT NULL IS NULL"));
	[Fact] public async Task IsNotNull_Literal() => Assert.Equal("True", await S("SELECT 1 IS NOT NULL"));

	// ---- NULL in comparisons ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators
	//   "Operands of <op> cannot be literal NULL"
	[Fact]
	public async Task Null_EqualNull()
	{
		var c = await _fixture.GetClientAsync();
		await Assert.ThrowsAsync<GoogleApiException>(
			() => c.ExecuteQueryAsync("SELECT NULL = NULL", parameters: null));
	}
	[Fact]
	public async Task Null_NotEqualNull()
	{
		var c = await _fixture.GetClientAsync();
		await Assert.ThrowsAsync<GoogleApiException>(
			() => c.ExecuteQueryAsync("SELECT NULL != NULL", parameters: null));
	}
	[Fact]
	public async Task Null_LessThan()
	{
		var c = await _fixture.GetClientAsync();
		await Assert.ThrowsAsync<GoogleApiException>(
			() => c.ExecuteQueryAsync("SELECT NULL < 1", parameters: null));
	}
	[Fact]
	public async Task Null_GreaterThan()
	{
		var c = await _fixture.GetClientAsync();
		await Assert.ThrowsAsync<GoogleApiException>(
			() => c.ExecuteQueryAsync("SELECT NULL > 1", parameters: null));
	}
	[Fact] public async Task NullComparison_WhereExcluded() => Assert.Equal("4", await S("SELECT COUNT(*) FROM `{ds}.t` WHERE val > 0"));

	// ---- NULL in arithmetic ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators
	//   "Operands of <op> cannot be literal NULL"
	[Fact]
	public async Task NullArith_Add()
	{
		var c = await _fixture.GetClientAsync();
		await Assert.ThrowsAsync<GoogleApiException>(
			() => c.ExecuteQueryAsync("SELECT NULL + 1", parameters: null));
	}
	[Fact]
	public async Task NullArith_Sub()
	{
		var c = await _fixture.GetClientAsync();
		await Assert.ThrowsAsync<GoogleApiException>(
			() => c.ExecuteQueryAsync("SELECT 1 - NULL", parameters: null));
	}
	[Fact]
	public async Task NullArith_Mul()
	{
		var c = await _fixture.GetClientAsync();
		await Assert.ThrowsAsync<GoogleApiException>(
			() => c.ExecuteQueryAsync("SELECT NULL * 5", parameters: null));
	}
	[Fact]
	public async Task NullArith_Div()
	{
		var c = await _fixture.GetClientAsync();
		await Assert.ThrowsAsync<GoogleApiException>(
			() => c.ExecuteQueryAsync("SELECT 10 / NULL", parameters: null));
	}

	// ---- NULL in logical operations ----
	[Fact] public async Task NullLogic_AndTrue() => Assert.Null(await S("SELECT NULL AND true"));
	[Fact] public async Task NullLogic_AndFalse() => Assert.Equal("False", await S("SELECT NULL AND false"));
	[Fact] public async Task NullLogic_OrTrue() => Assert.Equal("True", await S("SELECT NULL OR true"));
	[Fact] public async Task NullLogic_OrFalse() => Assert.Null(await S("SELECT NULL OR false"));
	[Fact]
	public async Task NullLogic_Not()
	{
		var c = await _fixture.GetClientAsync();
		await Assert.ThrowsAsync<GoogleApiException>(
			() => c.ExecuteQueryAsync("SELECT NOT NULL", parameters: null));
	}

	// ---- NULL in aggregates ----
	[Fact] public async Task NullAgg_Count() => Assert.Equal("6", await S("SELECT COUNT(*) FROM `{ds}.t`"));
	[Fact] public async Task NullAgg_CountCol() => Assert.Equal("4", await S("SELECT COUNT(val) FROM `{ds}.t`"));
	[Fact] public async Task NullAgg_Sum() => Assert.Equal("150", await S("SELECT SUM(val) FROM `{ds}.t`"));
	[Fact] public async Task NullAgg_Avg()
	{
		var v = double.Parse(await S("SELECT AVG(val) FROM `{ds}.t`") ?? "0");
		Assert.Equal(37.5, v);
	}
	[Fact] public async Task NullAgg_Min() => Assert.Equal("10", await S("SELECT MIN(val) FROM `{ds}.t`"));
	[Fact] public async Task NullAgg_Max() => Assert.Equal("60", await S("SELECT MAX(val) FROM `{ds}.t`"));
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#sum
	//   BigQuery may reject aggregate on UNNEST with all-NULL literal arrays in certain contexts.
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task NullAgg_AllNull() => Assert.Null(await S("SELECT SUM(x) FROM UNNEST([CAST(NULL AS INT64), NULL, NULL]) AS x"));

	// ---- NULL in BETWEEN ----
	[Fact] public async Task NullBetween_NullVal() => Assert.Null(await S("SELECT CAST(NULL AS INT64) BETWEEN 1 AND 10"));
	[Fact] public async Task NullBetween_NullBound() => Assert.Null(await S("SELECT 5 BETWEEN CAST(NULL AS INT64) AND 10"));

	// ---- NULL in IN ----
	[Fact] public async Task NullIn_NullInList() => Assert.Null(await S("SELECT NULL IN (1, 2, 3)"));
	[Fact] public async Task NullIn_ListHasNull() => Assert.Null(await S("SELECT 1 IN (NULL, 2, 3)"));
	[Fact] public async Task NullIn_MatchExists() => Assert.Equal("True", await S("SELECT 1 IN (1, NULL, 3)"));

	// ---- NULL in LIKE ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators
	//   "Operands of LIKE cannot be literal NULL"
	[Fact]
	public async Task NullLike_NullPattern()
	{
		var c = await _fixture.GetClientAsync();
		await Assert.ThrowsAsync<GoogleApiException>(
			() => c.ExecuteQueryAsync("SELECT NULL LIKE '%'", parameters: null));
	}
	[Fact]
	public async Task NullLike_NullValue()
	{
		var c = await _fixture.GetClientAsync();
		await Assert.ThrowsAsync<GoogleApiException>(
			() => c.ExecuteQueryAsync("SELECT NULL LIKE 'a'", parameters: null));
	}

	// ---- NULL in CASE ----
	[Fact] public async Task NullCase_WhenNull() => Assert.Equal("is_null", await S("SELECT CASE WHEN NULL IS NULL THEN 'is_null' ELSE 'not_null' END"));
	[Fact] public async Task NullCase_SimpleNull() => Assert.Equal("other", await S("SELECT CASE NULL WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END"));

	// ---- NULL with DISTINCT ----
	[Fact] public async Task NullDistinct_Count()
	{
		var v = await S("SELECT COUNT(DISTINCT val) FROM `{ds}.t`");
		Assert.Equal("4", v); // 10, 30, 50, 60
	}
	[Fact] public async Task NullDistinct_Select()
	{
		var rows = await Q("SELECT DISTINCT val FROM `{ds}.t` ORDER BY val");
		Assert.Equal(5, rows.Count); // NULL, 10, 30, 50, 60
	}

	// ---- NULL ORDER BY behavior ----
	[Fact] public async Task NullOrderBy_AscFirst()
	{
		var rows = await Q("SELECT val FROM `{ds}.t` ORDER BY val ASC");
		Assert.Null(rows[0]["val"]); // NULLs first in ASC
	}
	[Fact] public async Task NullOrderBy_DescLast()
	{
		var rows = await Q("SELECT val FROM `{ds}.t` ORDER BY val DESC");
		Assert.Equal("60", rows[0]["val"]?.ToString());
	}

	// ---- NULL in string functions ----
	[Fact] public async Task NullStr_Concat() => Assert.Null(await S("SELECT CONCAT(NULL, 'a')"));
	[Fact] public async Task NullStr_Upper() => Assert.Null(await S("SELECT UPPER(CAST(NULL AS STRING))"));
	[Fact] public async Task NullStr_Length() => Assert.Null(await S("SELECT LENGTH(CAST(NULL AS STRING))"));

	// ---- NULL in math functions ----
	[Fact] public async Task NullMath_Abs() => Assert.Null(await S("SELECT ABS(CAST(NULL AS INT64))"));
	[Fact] public async Task NullMath_Round() => Assert.Null(await S("SELECT ROUND(CAST(NULL AS FLOAT64))"));
	[Fact] public async Task NullMath_Mod() => Assert.Null(await S("SELECT MOD(NULL, 3)"));
}
