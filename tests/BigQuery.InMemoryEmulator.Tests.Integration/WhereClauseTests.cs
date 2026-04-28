using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for WHERE clause patterns: comparisons, logical operators, IN, LIKE, BETWEEN.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class WhereClauseTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public WhereClauseTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	private async Task<List<string?>> Column(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		return result.Select(r => r[0]?.ToString()).ToList();
	}

	// ---- Comparison operators ----
	[Fact] public async Task Where_EQ() => Assert.Equal("1", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x WHERE x = 5"));
	[Fact] public async Task Where_NEQ() => Assert.Equal("9", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x WHERE x != 5"));
	[Fact] public async Task Where_GT() => Assert.Equal("5", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x WHERE x > 5"));
	[Fact] public async Task Where_GTE() => Assert.Equal("6", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x WHERE x >= 5"));
	[Fact] public async Task Where_LT() => Assert.Equal("4", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x WHERE x < 5"));
	[Fact] public async Task Where_LTE() => Assert.Equal("5", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x WHERE x <= 5"));

	// ---- AND / OR ----
	[Fact] public async Task Where_AND() => Assert.Equal("3", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x WHERE x > 3 AND x < 7"));
	[Fact] public async Task Where_OR() => Assert.Equal("4", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x WHERE x < 3 OR x > 8"));
	[Fact] public async Task Where_AND_OR() => Assert.Equal("3", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x WHERE (x > 3 AND x < 6) OR x = 10"));
	[Fact] public async Task Where_NOT() => Assert.Equal("8", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x WHERE NOT (x <= 2)"));

	// ---- IN list ----
	[Fact] public async Task Where_IN_List() => Assert.Equal("3", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x WHERE x IN (2, 5, 8)"));
	[Fact] public async Task Where_IN_SingleValue() => Assert.Equal("1", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x WHERE x IN (7)"));
	[Fact] public async Task Where_IN_NoMatch() => Assert.Equal("0", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x WHERE x IN (20, 30)"));
	[Fact] public async Task Where_IN_Strings() => Assert.Equal("2", await Scalar("SELECT COUNT(*) FROM UNNEST(['a', 'b', 'c', 'd', 'e']) AS x WHERE x IN ('b', 'd')"));

	// ---- BETWEEN ----
	[Fact] public async Task Where_BETWEEN() => Assert.Equal("5", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x WHERE x BETWEEN 3 AND 7"));
	[Fact] public async Task Where_BETWEEN_Single() => Assert.Equal("1", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x WHERE x BETWEEN 5 AND 5"));
	[Fact] public async Task Where_BETWEEN_All() => Assert.Equal("10", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x WHERE x BETWEEN 1 AND 10"));
	[Fact] public async Task Where_BETWEEN_None() => Assert.Equal("0", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x WHERE x BETWEEN 20 AND 30"));

	// ---- LIKE patterns ----
	[Fact] public async Task Where_LIKE_Prefix() => Assert.Equal("3", await Scalar("SELECT COUNT(*) FROM UNNEST(['apple', 'apricot', 'banana', 'avocado', 'app']) AS x WHERE x LIKE 'ap%'"));
	[Fact] public async Task Where_LIKE_Suffix() => Assert.Equal("1", await Scalar("SELECT COUNT(*) FROM UNNEST(['apple', 'apricot', 'banana', 'avocado']) AS x WHERE x LIKE '%ana'"));
	[Fact] public async Task Where_LIKE_Contains() => Assert.Equal("2", await Scalar("SELECT COUNT(*) FROM UNNEST(['apple', 'pineapple', 'banana', 'grape']) AS x WHERE x LIKE '%apple%'"));
	[Fact] public async Task Where_LIKE_Underscore() => Assert.Equal("1", await Scalar("SELECT COUNT(*) FROM UNNEST(['cat', 'bat', 'hat', 'cats', 'bats']) AS x WHERE x LIKE 'c_t'"));
	[Fact] public async Task Where_LIKE_Exact() => Assert.Equal("1", await Scalar("SELECT COUNT(*) FROM UNNEST(['hello', 'world', 'help']) AS x WHERE x LIKE 'hello'"));
	[Fact] public async Task Where_LIKE_NoMatch() => Assert.Equal("0", await Scalar("SELECT COUNT(*) FROM UNNEST(['hello', 'world', 'help']) AS x WHERE x LIKE 'xyz%'"));

	// ---- IS NULL / IS NOT NULL ----
	[Fact] public async Task Where_IsNull() => Assert.Equal("True", await Scalar("SELECT NULL IS NULL"));
	[Fact] public async Task Where_IsNotNull() => Assert.Equal("False", await Scalar("SELECT NULL IS NOT NULL"));
	[Fact] public async Task Where_ValueIsNull() => Assert.Equal("False", await Scalar("SELECT 42 IS NULL"));
	[Fact] public async Task Where_ValueIsNotNull() => Assert.Equal("True", await Scalar("SELECT 42 IS NOT NULL"));

	// ---- Filtering on expressions ----
	[Fact] public async Task Where_ModFilter() => Assert.Equal("5", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x WHERE MOD(x, 2) = 0"));
	[Fact] public async Task Where_FunctionFilter() => Assert.Equal("2", await Scalar("SELECT COUNT(*) FROM UNNEST(['hello', 'world', 'hi', 'hey', 'ok']) AS x WHERE LENGTH(x) = 5"));
	[Fact] public async Task Where_CastFilter() => Assert.Equal("5", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x WHERE CAST(x AS FLOAT64) > 5.0"));
	[Fact] public async Task Where_ConcatFilter() => Assert.Equal("1", await Scalar("SELECT COUNT(*) FROM UNNEST(['a', 'b', 'c']) AS x WHERE CONCAT(x, 'b') = 'ab'"));

	// ---- Complex WHERE chains ----
	[Fact]
	public async Task Where_MultipleConditions()
	{
		var v = await Scalar(@"
SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 100)) AS x
WHERE x > 10 AND x < 90 AND MOD(x, 10) = 0");
		Assert.Equal("7", v);
	}

	[Fact]
	public async Task Where_OrWithAnd()
	{
		var v = await Scalar(@"
SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 20)) AS x
WHERE (x < 5 AND MOD(x, 2) = 0) OR (x > 15 AND MOD(x, 2) = 1)");
		Assert.Equal("4", v);
	}

	[Fact]
	public async Task Where_NestedParens()
	{
		var v = await Scalar(@"
SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x
WHERE (x > 3 AND (x < 7 OR x = 10))");
		Assert.Equal("4", v);
	}

	// ---- WHERE with aggregate in HAVING ----
	[Fact]
	public async Task Having_CountGT3()
	{
		var v = await Scalar(@"
SELECT COUNT(*) FROM (
    SELECT MOD(x, 3) AS grp, COUNT(*) AS cnt
    FROM UNNEST(GENERATE_ARRAY(1, 30)) AS x
    GROUP BY grp
    HAVING COUNT(*) = 10
) AS t");
		Assert.Equal("3", v);
	}

	// ---- WHERE with CASE ----
	[Fact]
	public async Task Where_CaseResult()
	{
		var v = await Scalar(@"
SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x
WHERE CASE WHEN x > 5 THEN 'high' ELSE 'low' END = 'high'");
		Assert.Equal("5", v);
	}

	// ---- WHERE with IF ----
	[Fact]
	public async Task Where_IfResult()
	{
		var v = await Scalar(@"
SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x
WHERE IF(MOD(x, 2) = 0, 'even', 'odd') = 'even'");
		Assert.Equal("5", v);
	}

	// ---- WHERE with subquery ----
	[Fact]
	public async Task Where_InSubquery()
	{
		var v = await Scalar(@"
SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 20)) AS x
WHERE x IN (SELECT y FROM UNNEST(GENERATE_ARRAY(5, 15)) AS y)");
		Assert.Equal("11", v);
	}

	[Fact(Skip = "Emulator limitation")]
	public async Task Where_ExistsSubquery()
	{
		var v = await Scalar(@"
SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x
WHERE EXISTS(SELECT 1 FROM UNNEST([2, 4, 6]) AS y WHERE y = x)");
		Assert.Equal("2", v);
	}

	// ---- WHERE combining string and numeric ----
	[Fact]
	public async Task Where_StringLength()
	{
		var v = await Column(@"
SELECT x FROM UNNEST(['a', 'bb', 'ccc', 'dddd', 'eeeee']) AS x
WHERE LENGTH(x) > 2
ORDER BY LENGTH(x)");
		Assert.Equal(new[] { "ccc", "dddd", "eeeee" }, v);
	}

	[Fact]
	public async Task Where_StringAndPosition()
	{
		var v = await Scalar(@"
SELECT COUNT(*) FROM UNNEST(['hello world', 'foo bar', 'hello there', 'goodbye']) AS x
WHERE STARTS_WITH(x, 'hello')");
		Assert.Equal("2", v);
	}
}
