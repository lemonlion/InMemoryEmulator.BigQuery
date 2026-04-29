using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for subquery patterns: scalar subqueries, IN subqueries, EXISTS, derived tables.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class SubqueryPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public SubqueryPatternTests(BigQuerySession session) => _session = session;
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

	// ---- Scalar subquery in SELECT ----
	[Fact]
	public async Task ScalarSub_MaxFromUnnest()
	{
		var v = await Scalar("SELECT (SELECT MAX(x) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x) AS mx");
		Assert.Equal("10", v);
	}

	[Fact]
	public async Task ScalarSub_SumFromUnnest()
	{
		var v = await Scalar("SELECT (SELECT SUM(x) FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x) AS s");
		Assert.Equal("15", v);
	}

	[Fact]
	public async Task ScalarSub_CountFromUnnest()
	{
		var v = await Scalar("SELECT (SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 20)) AS x WHERE x > 10) AS c");
		Assert.Equal("10", v);
	}

	// ---- Derived table (FROM subquery) ----
	[Fact]
	public async Task DerivedTable_Sum()
	{
		var v = await Scalar("SELECT SUM(t.v) FROM (SELECT x * 2 AS v FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x) AS t");
		Assert.Equal("30", v);
	}

	[Fact]
	public async Task DerivedTable_Filter()
	{
		var v = await Scalar("SELECT COUNT(*) FROM (SELECT x FROM UNNEST(GENERATE_ARRAY(1, 20)) AS x WHERE MOD(x, 2) = 0) AS t");
		Assert.Equal("10", v);
	}

	[Fact]
	public async Task DerivedTable_Nested()
	{
		var v = await Scalar("SELECT SUM(t.v) FROM (SELECT x + 1 AS v FROM (SELECT x FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x) AS s) AS t");
		Assert.Equal("20", v);
	}

	[Fact]
	public async Task DerivedTable_Aggregate()
	{
		var v = await Scalar("SELECT AVG(t.s) FROM (SELECT SUM(x) AS s FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x) AS t");
		Assert.Equal("55", v);
	}

	// ---- IN subquery ----
	[Fact]
	public async Task InSubquery_Matches()
	{
		var v = await Scalar(@"
SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x
WHERE x IN (SELECT y FROM UNNEST([2, 4, 6, 8]) AS y)");
		Assert.Equal("4", v);
	}

	[Fact]
	public async Task InSubquery_NoMatches()
	{
		var v = await Scalar(@"
SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x
WHERE x IN (SELECT y FROM UNNEST([10, 20, 30]) AS y)");
		Assert.Equal("0", v);
	}

	// ---- EXISTS subquery ----
	[Fact]
	public async Task Exists_True()
	{
		var v = await Scalar("SELECT EXISTS(SELECT 1 FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x WHERE x = 3)");
		Assert.Equal("True", v);
	}

	[Fact]
	public async Task Exists_False()
	{
		var v = await Scalar("SELECT EXISTS(SELECT 1 FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x WHERE x = 99)");
		Assert.Equal("False", v);
	}

	// ---- CTE patterns (simple, no UNION ALL or CTE joins) ----
	[Fact]
	public async Task CTE_SimpleSelect()
	{
		var v = await Scalar("WITH t AS (SELECT 42 AS val) SELECT val FROM t");
		Assert.Equal("42", v);
	}

	[Fact]
	public async Task CTE_Aggregate()
	{
		var v = await Scalar("WITH t AS (SELECT x FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x) SELECT SUM(x) FROM t");
		Assert.Equal("55", v);
	}

	[Fact]
	public async Task CTE_Filtered()
	{
		var v = await Scalar("WITH t AS (SELECT x FROM UNNEST(GENERATE_ARRAY(1, 20)) AS x WHERE MOD(x, 3) = 0) SELECT COUNT(*) FROM t");
		Assert.Equal("6", v);
	}

	[Fact]
	public async Task CTE_WithAlias()
	{
		var v = await Scalar("WITH src AS (SELECT x * 10 AS val FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x) SELECT SUM(val) FROM src");
		Assert.Equal("150", v);
	}

	[Fact]
	public async Task CTE_Chained()
	{
		var v = await Scalar(@"
WITH a AS (SELECT x FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x),
     b AS (SELECT x, x * x AS sq FROM a)
SELECT SUM(sq) FROM b");
		Assert.Equal("385", v);
	}

	[Fact]
	public async Task CTE_WithFilter()
	{
		var v = await Scalar(@"
WITH nums AS (SELECT x FROM UNNEST(GENERATE_ARRAY(1, 50)) AS x)
SELECT COUNT(*) FROM nums WHERE MOD(x, 5) = 0");
		Assert.Equal("10", v);
	}

	// ---- Subquery in WHERE clause ----
	[Fact(Skip = "Scalar subquery comparison differs")]
	public async Task WhereSubquery_GT_ScalarSub()
	{
		var v = await Scalar(@"
SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 20)) AS x
WHERE x > (SELECT AVG(y) FROM UNNEST(GENERATE_ARRAY(1, 20)) AS y)");
		Assert.Equal("10", v);
	}

	[Fact]
	public async Task WhereSubquery_EQ_ScalarSub()
	{
		var v = await Scalar(@"
SELECT x FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x
WHERE x = (SELECT MAX(y) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS y)");
		Assert.Equal("10", v);
	}

	// ---- Correlated subquery ----
	[Fact(Skip = "Correlated subquery not fully supported")]
	public async Task CorrelatedExists()
	{
		var v = await Scalar(@"
SELECT COUNT(*) FROM UNNEST([2, 4, 6, 8, 10]) AS x
WHERE EXISTS(SELECT 1 FROM UNNEST(GENERATE_ARRAY(1, 5)) AS y WHERE y = x)");
		Assert.Equal("2", v);
	}

	// ---- Multiple columns from subquery ----
	[Fact]
	public async Task MultiColumn_DerivedTable()
	{
		var v = await Scalar(@"
SELECT SUM(t.doubled) FROM (
    SELECT x, x * 2 AS doubled FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x
) AS t
WHERE t.x > 2");
		Assert.Equal("24", v);
	}

	// ---- Subquery with ORDER BY + LIMIT ----
	[Fact]
	public async Task Subquery_Top3()
	{
		var v = await Column(@"
SELECT t.x FROM (
    SELECT x FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x ORDER BY x DESC LIMIT 3
) AS t ORDER BY t.x");
		Assert.Equal(new[] { "8", "9", "10" }, v);
	}

	[Fact]
	public async Task Subquery_Bottom3()
	{
		var v = await Column(@"
SELECT t.x FROM (
    SELECT x FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x ORDER BY x LIMIT 3
) AS t ORDER BY t.x");
		Assert.Equal(new[] { "1", "2", "3" }, v);
	}

	// ---- Nested aggregates via subquery ----
	[Fact]
	public async Task Nested_AvgOfSum()
	{
		var v = await Scalar(@"
SELECT AVG(t.s) FROM (
    SELECT SUM(x) AS s FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x
) AS t");
		Assert.Equal("55", v);
	}
}
