using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Boolean expression patterns: AND, OR, NOT, comparison chains, truthiness.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class BooleanExpressionPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public BooleanExpressionPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_bep_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.t` (id INT64, active BOOL, name STRING, val INT64, score FLOAT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.t` VALUES
			(1,true,'Alice',100,9.5),(2,false,'Bob',200,7.0),(3,true,'Carol',150,8.5),
			(4,NULL,'Dave',NULL,NULL),(5,true,'Eve',300,6.0),(6,false,'Frank',50,4.5),
			(7,true,'Grace',250,9.0),(8,false,'Hank',175,5.5)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- Basic AND ----
	[Fact] public async Task And_TrueTrue() => Assert.Equal("True", await S("SELECT true AND true"));
	[Fact] public async Task And_TrueFalse() => Assert.Equal("False", await S("SELECT true AND false"));
	[Fact] public async Task And_FalseFalse() => Assert.Equal("False", await S("SELECT false AND false"));

	// ---- Basic OR ----
	[Fact] public async Task Or_TrueTrue() => Assert.Equal("True", await S("SELECT true OR true"));
	[Fact] public async Task Or_TrueFalse() => Assert.Equal("True", await S("SELECT true OR false"));
	[Fact] public async Task Or_FalseFalse() => Assert.Equal("False", await S("SELECT false OR false"));

	// ---- NOT ----
	[Fact] public async Task Not_True() => Assert.Equal("False", await S("SELECT NOT true"));
	[Fact] public async Task Not_False() => Assert.Equal("True", await S("SELECT NOT false"));

	// ---- NULL in boolean logic ----
	[Fact] public async Task And_TrueNull() => Assert.Null(await S("SELECT true AND NULL"));
	[Fact] public async Task And_FalseNull() => Assert.Equal("False", await S("SELECT false AND NULL"));
	[Fact] public async Task Or_TrueNull() => Assert.Equal("True", await S("SELECT true OR NULL"));
	[Fact] public async Task Or_FalseNull() => Assert.Null(await S("SELECT false OR NULL"));
	[Fact] public async Task Not_Null() => Assert.Null(await S("SELECT NOT CAST(NULL AS BOOL)"));

	// ---- Comparison operators ----
	[Fact] public async Task Eq_Int() => Assert.Equal("True", await S("SELECT 1 = 1"));
	[Fact] public async Task Neq_Int() => Assert.Equal("True", await S("SELECT 1 != 2"));
	[Fact] public async Task Lt_Int() => Assert.Equal("True", await S("SELECT 1 < 2"));
	[Fact] public async Task Gt_Int() => Assert.Equal("True", await S("SELECT 2 > 1"));
	[Fact] public async Task Lte_Int() => Assert.Equal("True", await S("SELECT 1 <= 1"));
	[Fact] public async Task Gte_Int() => Assert.Equal("True", await S("SELECT 2 >= 1"));

	// ---- String comparison ----
	[Fact] public async Task Eq_String() => Assert.Equal("True", await S("SELECT 'abc' = 'abc'"));
	[Fact] public async Task Neq_String() => Assert.Equal("True", await S("SELECT 'abc' != 'def'"));
	[Fact] public async Task Lt_String() => Assert.Equal("True", await S("SELECT 'abc' < 'def'"));
	[Fact] public async Task Gt_String() => Assert.Equal("True", await S("SELECT 'def' > 'abc'"));

	// ---- NULL comparison ----
	[Fact] public async Task Eq_Null() => Assert.Null(await S("SELECT NULL = NULL"));
	[Fact] public async Task Neq_Null() => Assert.Null(await S("SELECT NULL != 1"));
	[Fact] public async Task IsNull() => Assert.Equal("True", await S("SELECT NULL IS NULL"));
	[Fact] public async Task IsNotNull() => Assert.Equal("True", await S("SELECT 1 IS NOT NULL"));
	[Fact] public async Task IsNull_False() => Assert.Equal("False", await S("SELECT 1 IS NULL"));

	// ---- Complex boolean expressions ----
	[Fact] public async Task Complex_AndOr()
	{
		var v = await S("SELECT (true AND false) OR true");
		Assert.Equal("True", v);
	}
	[Fact] public async Task Complex_OrAnd()
	{
		var v = await S("SELECT true OR (false AND false)");
		Assert.Equal("True", v);
	}
	[Fact] public async Task Complex_NotAnd()
	{
		var v = await S("SELECT NOT (true AND false)");
		Assert.Equal("True", v);
	}
	[Fact] public async Task Complex_DeMorgan()
	{
		// NOT (A AND B) = (NOT A) OR (NOT B)
		var v = await S("SELECT NOT (true AND false) = ((NOT true) OR (NOT false))");
		Assert.Equal("True", v);
	}

	// ---- WHERE with boolean column ----
	[Fact] public async Task Where_BoolTrue()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.t` WHERE active = true");
		Assert.Equal("4", v);
	}
	[Fact] public async Task Where_BoolFalse()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.t` WHERE active = false");
		Assert.Equal("3", v);
	}
	[Fact] public async Task Where_BoolIsNull()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.t` WHERE active IS NULL");
		Assert.Equal("1", v);
	}
	[Fact] public async Task Where_BoolIsNotNull()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.t` WHERE active IS NOT NULL");
		Assert.Equal("7", v);
	}

	// ---- BETWEEN ----
	[Fact] public async Task Between_Int()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.t` WHERE val BETWEEN 100 AND 200");
		Assert.Equal("4", v); // 100, 200, 150, 175
	}
	[Fact] public async Task Between_Float()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.t` WHERE score BETWEEN 5.0 AND 8.0");
		Assert.Equal("3", v); // 7.0, 6.0, 5.5
	}
	[Fact] public async Task NotBetween()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.t` WHERE val NOT BETWEEN 100 AND 200");
		Assert.Equal("3", v); // 300, 50, 250 (NULL excluded)
	}

	// ---- IN ----
	[Fact] public async Task In_Int()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.t` WHERE id IN (1,3,5)");
		Assert.Equal("3", v);
	}
	[Fact] public async Task In_String()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.t` WHERE name IN ('Alice','Bob','Carol')");
		Assert.Equal("3", v);
	}
	[Fact] public async Task NotIn_Int()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.t` WHERE id NOT IN (1,2,3)");
		Assert.Equal("5", v);
	}

	// ---- IF ----
	[Fact] public async Task If_True() => Assert.Equal("yes", await S("SELECT IF(true, 'yes', 'no')"));
	[Fact] public async Task If_False() => Assert.Equal("no", await S("SELECT IF(false, 'yes', 'no')"));
	[Fact] public async Task If_Null() => Assert.Equal("no", await S("SELECT IF(NULL, 'yes', 'no')"));

	// ---- IFF / IFNULL ----
	[Fact] public async Task IfNull_NonNull() => Assert.Equal("5", await S("SELECT IFNULL(5, 10)"));
	[Fact] public async Task IfNull_Null() => Assert.Equal("10", await S("SELECT IFNULL(NULL, 10)"));

	// ---- NULLIF ----
	[Fact] public async Task NullIf_Equal() => Assert.Null(await S("SELECT NULLIF(5, 5)"));
	[Fact] public async Task NullIf_NotEqual() => Assert.Equal("5", await S("SELECT NULLIF(5, 10)"));

	// ---- COALESCE ----
	[Fact] public async Task Coalesce_First() => Assert.Equal("1", await S("SELECT COALESCE(1, 2, 3)"));
	[Fact] public async Task Coalesce_Second() => Assert.Equal("2", await S("SELECT COALESCE(NULL, 2, 3)"));
	[Fact] public async Task Coalesce_Third() => Assert.Equal("3", await S("SELECT COALESCE(NULL, NULL, 3)"));
	[Fact] public async Task Coalesce_AllNull() => Assert.Null(await S("SELECT COALESCE(NULL, NULL, NULL)"));

	// ---- Chained comparisons ----
	[Fact] public async Task Chained_And()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.t` WHERE active = true AND val > 100 AND score > 8.0");
		Assert.Equal("2", v); // Carol(150,8.5), Grace(250,9.0)
	}
	[Fact] public async Task Chained_Or()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.t` WHERE val < 100 OR score > 9.0");
		Assert.Equal("2", v); // Frank(50,4.5), Alice(100,9.5) — wait, Alice val=100, not <100; score=9.5>9.0
		// Actually: Frank val=50<100, Alice score=9.5>9.0 = 2
	}
	[Fact] public async Task Mixed_And_Or()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.t` WHERE (active = true AND val >= 150) OR (active = false AND score < 5.0)");
		Assert.Equal("4", v); // Carol(150), Eve(300), Grace(250), Frank(4.5)
	}

	// ---- CASE with boolean ----
	[Fact] public async Task Case_Bool()
	{
		var rows = await Q("SELECT id, CASE WHEN active = true THEN 'Y' WHEN active = false THEN 'N' ELSE 'U' END AS flag FROM `{ds}.t` ORDER BY id");
		Assert.Equal("Y", rows[0]["flag"]?.ToString()); // id=1 true
		Assert.Equal("N", rows[1]["flag"]?.ToString()); // id=2 false
		Assert.Equal("U", rows[3]["flag"]?.ToString()); // id=4 NULL
	}

	// ---- Boolean aggregation via COUNTIF ----
	[Fact] public async Task CountIf()
	{
		var v = await S("SELECT COUNTIF(active) FROM `{ds}.t`");
		Assert.Equal("4", v);
	}
	[Fact] public async Task CountIf_Expr()
	{
		var v = await S("SELECT COUNTIF(val > 100) FROM `{ds}.t`");
		Assert.Equal("5", v); // 200,150,300,250,175
	}

	// ---- Boolean in SELECT ----
	[Fact] public async Task Select_BoolExpr()
	{
		var rows = await Q("SELECT id, val > 150 AS high_val FROM `{ds}.t` WHERE id IN (1,2,3) ORDER BY id");
		Assert.Equal("False", rows[0]["high_val"]?.ToString()); // 100
		Assert.Equal("True", rows[1]["high_val"]?.ToString());  // 200
		Assert.Equal("False", rows[2]["high_val"]?.ToString()); // 150
	}

	// ---- Double negation ----
	[Fact] public async Task Not_Not() => Assert.Equal("True", await S("SELECT NOT NOT true"));
	[Fact] public async Task Not_Not_False() => Assert.Equal("False", await S("SELECT NOT NOT false"));

	// ---- XOR via (A OR B) AND NOT (A AND B) ----
	[Fact] public async Task Xor_TrueFalse()
	{
		var v = await S("SELECT (true OR false) AND NOT (true AND false)");
		Assert.Equal("True", v);
	}
	[Fact] public async Task Xor_TrueTrue()
	{
		var v = await S("SELECT (true OR true) AND NOT (true AND true)");
		Assert.Equal("False", v);
	}
}
