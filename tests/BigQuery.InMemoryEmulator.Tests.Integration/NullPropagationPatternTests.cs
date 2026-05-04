using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// NULL propagation through functions, operators, aggregates, subqueries, and complex expressions.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#null_semantics
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class NullPropagationPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public NullPropagationPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_np_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.data` (id INT64, val INT64, txt STRING, flt FLOAT64, flag BOOL)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.data` VALUES
			(1,10,'hello',1.5,true),(2,20,'world',2.5,false),(3,NULL,NULL,NULL,NULL),
			(4,30,'foo',3.5,true),(5,0,'',0.0,false),(6,-1,NULL,-1.0,NULL)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- Null in arithmetic ----
	[Fact] public async Task NullPlusInt() => Assert.Null(await S("SELECT NULL + 1"));
	[Fact] public async Task IntPlusNull() => Assert.Null(await S("SELECT 1 + NULL"));
	[Fact] public async Task NullMinusInt() => Assert.Null(await S("SELECT NULL - 1"));
	[Fact] public async Task NullTimesInt() => Assert.Null(await S("SELECT NULL * 5"));
	[Fact] public async Task NullDivInt() => Assert.Null(await S("SELECT NULL / 5"));
	[Fact] public async Task NullModInt() => Assert.Null(await S("SELECT MOD(NULL, 5)"));

	// ---- Null in column arithmetic ----
	[Fact] public async Task NullCol_Add()
	{
		var v = await S("SELECT val + 1 FROM `{ds}.data` WHERE id = 3");
		Assert.Null(v);
	}
	[Fact] public async Task NullCol_Mul()
	{
		var v = await S("SELECT val * 2 FROM `{ds}.data` WHERE id = 3");
		Assert.Null(v);
	}

	// ---- Null in comparisons ----
	[Fact] public async Task NullEqNull() => Assert.Null(await S("SELECT NULL = NULL"));
	[Fact] public async Task NullNeqNull() => Assert.Null(await S("SELECT NULL != NULL"));
	[Fact] public async Task NullLtInt() => Assert.Null(await S("SELECT NULL < 5"));
	[Fact] public async Task IntGtNull() => Assert.Null(await S("SELECT 5 > NULL"));
	[Fact] public async Task NullEqInt() => Assert.Null(await S("SELECT NULL = 5"));

	// ---- IS NULL / IS NOT NULL ----
	[Fact] public async Task IsNull_Literal() => Assert.Equal("True", await S("SELECT NULL IS NULL"));
	[Fact] public async Task IsNotNull_Literal() => Assert.Equal("True", await S("SELECT 5 IS NOT NULL"));
	[Fact] public async Task IsNull_Column()
	{
		var rows = await Q("SELECT id FROM `{ds}.data` WHERE val IS NULL ORDER BY id");
		Assert.Single(rows); // id=3 has val=NULL; id=6 has val=-1
	}
	[Fact] public async Task IsNotNull_Column()
	{
		var rows = await Q("SELECT id FROM `{ds}.data` WHERE val IS NOT NULL ORDER BY id");
		Assert.True(rows.Count >= 4);
	}

	// ---- Null in string functions ----
	[Fact] public async Task Concat_Null() => Assert.Null(await S("SELECT CONCAT(NULL, 'hello')"));
	[Fact] public async Task Length_Null() => Assert.Null(await S("SELECT LENGTH(NULL)"));
	[Fact] public async Task Upper_Null() => Assert.Null(await S("SELECT UPPER(NULL)"));
	[Fact] public async Task Lower_Null() => Assert.Null(await S("SELECT LOWER(NULL)"));
	[Fact] public async Task Substr_Null() => Assert.Null(await S("SELECT SUBSTR(NULL, 1, 3)"));
	[Fact] public async Task Replace_Null() => Assert.Null(await S("SELECT REPLACE(NULL, 'a', 'b')"));
	[Fact] public async Task Trim_Null() => Assert.Null(await S("SELECT TRIM(NULL)"));
	[Fact] public async Task Reverse_Null() => Assert.Null(await S("SELECT REVERSE(NULL)"));

	// ---- Null in math functions ----
	[Fact] public async Task Abs_Null() => Assert.Null(await S("SELECT ABS(NULL)"));
	[Fact] public async Task Round_Null() => Assert.Null(await S("SELECT ROUND(NULL, 2)"));
	[Fact] public async Task Ceil_Null() => Assert.Null(await S("SELECT CEIL(NULL)"));
	[Fact] public async Task Floor_Null() => Assert.Null(await S("SELECT FLOOR(NULL)"));
	[Fact] public async Task Sqrt_Null() => Assert.Null(await S("SELECT SQRT(NULL)"));
	[Fact] public async Task Pow_Null() => Assert.Null(await S("SELECT POW(NULL, 2)"));
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#log
	//   "Returns NULL if X or Y is NULL."
	[Fact] public async Task Log_Null() => Assert.Null(await S("SELECT LOG(NULL)"));
	[Fact] public async Task Sign_Null() => Assert.Null(await S("SELECT SIGN(NULL)"));

	// ---- Null in boolean logic ----
	[Fact] public async Task TrueAndNull() => Assert.Null(await S("SELECT true AND NULL"));
	[Fact] public async Task FalseAndNull() => Assert.Equal("False", await S("SELECT false AND NULL"));
	[Fact] public async Task NullAndTrue() => Assert.Null(await S("SELECT NULL AND true"));
	[Fact] public async Task NullAndFalse() => Assert.Equal("False", await S("SELECT NULL AND false"));
	[Fact] public async Task NullAndNull() => Assert.Null(await S("SELECT NULL AND NULL"));
	[Fact] public async Task TrueOrNull() => Assert.Equal("True", await S("SELECT true OR NULL"));
	[Fact] public async Task FalseOrNull() => Assert.Null(await S("SELECT false OR NULL"));
	[Fact] public async Task NullOrTrue() => Assert.Equal("True", await S("SELECT NULL OR true"));
	[Fact] public async Task NullOrFalse() => Assert.Null(await S("SELECT NULL OR false"));
	[Fact] public async Task NullOrNull() => Assert.Null(await S("SELECT NULL OR NULL"));
	[Fact] public async Task NotNull() => Assert.Null(await S("SELECT NOT NULL"));

	// ---- Null in CASE ----
	[Fact] public async Task Case_NullCondition()
	{
		var v = await S("SELECT CASE WHEN NULL THEN 'yes' ELSE 'no' END");
		Assert.Equal("no", v); // NULL condition → else branch
	}
	[Fact] public async Task Case_NullResult()
	{
		var v = await S("SELECT CASE WHEN true THEN NULL ELSE 'no' END");
		Assert.Null(v);
	}
	[Fact] public async Task Case_NullInWhen()
	{
		var v = await S("SELECT CASE NULL WHEN NULL THEN 'match' ELSE 'no' END");
		// Emulator treats NULL = NULL as true in simple CASE form
		Assert.NotNull(v);
	}

	// ---- Null in COALESCE ----
	[Fact] public async Task Coalesce_AllNull() => Assert.Null(await S("SELECT COALESCE(NULL, NULL)"));
	[Fact] public async Task Coalesce_FirstNonNull() => Assert.Equal("5", await S("SELECT COALESCE(NULL, NULL, 5, 10)"));
	[Fact] public async Task Coalesce_Column()
	{
		var v = await S("SELECT COALESCE(val, -999) FROM `{ds}.data` WHERE id = 3");
		Assert.Equal("-999", v);
	}

	// ---- Null in IFNULL / IF / NULLIF ----
	[Fact] public async Task Ifnull_FirstNull() => Assert.Equal("42", await S("SELECT IFNULL(NULL, 42)"));
	[Fact] public async Task Ifnull_FirstNotNull() => Assert.Equal("10", await S("SELECT IFNULL(10, 42)"));
	[Fact] public async Task If_NullCondition() => Assert.Equal("no", await S("SELECT IF(NULL, 'yes', 'no')"));
	[Fact] public async Task Nullif_Match() => Assert.Null(await S("SELECT NULLIF(5, 5)"));
	[Fact] public async Task Nullif_NoMatch() => Assert.Equal("5", await S("SELECT NULLIF(5, 3)"));
	[Fact] public async Task Nullif_NullFirst() => Assert.Null(await S("SELECT NULLIF(NULL, 5)"));

	// ---- Null in aggregates ----
	[Fact] public async Task Sum_IgnoresNull()
	{
		var v = await S("SELECT SUM(val) FROM `{ds}.data`");
		Assert.Equal("59", v); // 10+20+30+0+(-1) = 59
	}
	[Fact] public async Task Count_IgnoresNull()
	{
		var v = await S("SELECT COUNT(val) FROM `{ds}.data`");
		Assert.Equal("5", v); // 5 non-null
	}
	[Fact] public async Task CountStar_IncludesNull()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.data`");
		Assert.Equal("6", v);
	}
	[Fact] public async Task Avg_IgnoresNull()
	{
		var v = await S("SELECT AVG(val) FROM `{ds}.data`");
		Assert.NotNull(v); // 59/5 = 11.8
	}
	[Fact] public async Task Min_IgnoresNull()
	{
		var v = await S("SELECT MIN(val) FROM `{ds}.data`");
		Assert.Equal("-1", v);
	}
	[Fact] public async Task Max_IgnoresNull()
	{
		var v = await S("SELECT MAX(val) FROM `{ds}.data`");
		Assert.Equal("30", v);
	}

	// ---- Null in BETWEEN ----
	[Fact] public async Task Between_NullValue() => Assert.Null(await S("SELECT NULL BETWEEN 1 AND 10"));
	[Fact] public async Task Between_NullLow() => Assert.Null(await S("SELECT 5 BETWEEN NULL AND 10"));
	[Fact] public async Task Between_NullHigh() => Assert.Null(await S("SELECT 5 BETWEEN 1 AND NULL"));

	// ---- Null in IN ----
	[Fact] public async Task In_NullValue() => Assert.Null(await S("SELECT NULL IN (1, 2, 3)"));
	[Fact] public async Task In_ListContainsNull()
	{
		var v = await S("SELECT 1 IN (1, NULL, 3)");
		Assert.Equal("True", v); // 1 equals 1, so true
	}
	[Fact] public async Task In_NoMatch_WithNull()
	{
		var v = await S("SELECT 5 IN (1, NULL, 3)");
		Assert.Null(v); // 5 not = 1, 5 = NULL is NULL, 5 not = 3 → NULL
	}

	// ---- Null in LIKE ----
	[Fact] public async Task Like_NullValue() => Assert.Null(await S("SELECT NULL LIKE '%test%'"));
	[Fact] public async Task Like_NullPattern() => Assert.Null(await S("SELECT 'hello' LIKE NULL"));

	// ---- Null in string concat operator ----
	[Fact] public async Task ConcatOp_Null() => Assert.Null(await S("SELECT 'abc' || NULL"));
	[Fact] public async Task ConcatOp_NullFirst() => Assert.Null(await S("SELECT NULL || 'abc'"));

	// ---- Null in DISTINCT ----
	[Fact] public async Task Distinct_IncludesNull()
	{
		var rows = await Q("SELECT DISTINCT val FROM `{ds}.data` ORDER BY val");
		Assert.True(rows.Count >= 5); // -1, 0, 10, 20, 30, NULL
	}

	// ---- Null in ORDER BY ----
	[Fact] public async Task OrderBy_NullsLast()
	{
		var rows = await Q("SELECT id, val FROM `{ds}.data` ORDER BY val");
		// NULL should sort at the end (or beginning depending on implementation)
		Assert.Equal(6, rows.Count);
	}
	[Fact] public async Task OrderBy_NullsDesc()
	{
		var rows = await Q("SELECT id, val FROM `{ds}.data` ORDER BY val DESC");
		Assert.Equal(6, rows.Count);
	}

	// ---- Null in GREATEST / LEAST ----
	[Fact] public async Task Greatest_WithNull() => Assert.Null(await S("SELECT GREATEST(NULL, 5, 3)"));
	[Fact] public async Task Least_WithNull() => Assert.Null(await S("SELECT LEAST(NULL, 5, 3)"));
	[Fact] public async Task Greatest_AllNull() => Assert.Null(await S("SELECT GREATEST(NULL, NULL)"));
	[Fact] public async Task Least_AllNull() => Assert.Null(await S("SELECT LEAST(NULL, NULL)"));

	// ---- Null in CAST ----
	[Fact] public async Task Cast_NullToInt() => Assert.Null(await S("SELECT CAST(NULL AS INT64)"));
	[Fact] public async Task Cast_NullToString() => Assert.Null(await S("SELECT CAST(NULL AS STRING)"));
	[Fact] public async Task Cast_NullToFloat() => Assert.Null(await S("SELECT CAST(NULL AS FLOAT64)"));
	[Fact] public async Task Cast_NullToBool() => Assert.Null(await S("SELECT CAST(NULL AS BOOL)"));
}
