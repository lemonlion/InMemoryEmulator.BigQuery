using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for subqueries: scalar subqueries, IN subqueries, EXISTS, correlated subqueries.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class SubqueryTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public SubqueryTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		return result.ToList();
	}

	private async Task<string?> Scalar(string sql)
	{
		var rows = await Query(sql);
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- Scalar subquery ----
	[Fact] public async Task ScalarSub_Select() => Assert.Equal("3", await Scalar("SELECT (SELECT 3)"));
	[Fact] public async Task ScalarSub_WithExpr() => Assert.Equal("5", await Scalar("SELECT (SELECT 2 + 3)"));
	[Fact] public async Task ScalarSub_InExpr() => Assert.Equal("7", await Scalar("SELECT 2 + (SELECT 5)"));
	[Fact] public async Task ScalarSub_String() => Assert.Equal("hello", await Scalar("SELECT (SELECT 'hello')"));
	[Fact] public async Task ScalarSub_Null() => Assert.Null(await Scalar("SELECT (SELECT NULL)"));
	[Fact] public async Task ScalarSub_Nested() => Assert.Equal("42", await Scalar("SELECT (SELECT (SELECT 42))"));
	[Fact] public async Task ScalarSub_WithFunction() => Assert.Equal("5", await Scalar("SELECT (SELECT LENGTH('hello'))"));
	[Fact] public async Task ScalarSub_Arithmetic() => Assert.Equal("6", await Scalar("SELECT (SELECT 2) * (SELECT 3)"));

	// ---- FROM subquery ----
	[Fact]
	public async Task FromSub_Simple()
	{
		var rows = await Query("SELECT v FROM (SELECT 42 AS v) AS t");
		Assert.Single(rows);
		Assert.Equal("42", rows[0]["v"]?.ToString());
	}

	[Fact]
	public async Task FromSub_Multiple()
	{
		var rows = await Query("SELECT v FROM (SELECT x AS v FROM UNNEST([1,2,3]) AS x) AS t ORDER BY v");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]["v"]?.ToString());
	}

	[Fact]
	public async Task FromSub_WithAlias()
	{
		var rows = await Query("SELECT t.col FROM (SELECT 'hello' AS col) AS t");
		Assert.Equal("hello", rows[0]["col"]?.ToString());
	}

	[Fact]
	public async Task FromSub_WithWhere()
	{
		var rows = await Query("SELECT v FROM (SELECT x AS v FROM UNNEST([1,2,3,4,5]) AS x) AS t WHERE v > 3 ORDER BY v");
		Assert.Equal(2, rows.Count);
		Assert.Equal("4", rows[0]["v"]?.ToString());
		Assert.Equal("5", rows[1]["v"]?.ToString());
	}

	[Fact]
	public async Task FromSub_Aggregate()
	{
		var v = await Scalar("SELECT SUM(v) FROM (SELECT x AS v FROM UNNEST([1,2,3,4,5]) AS x) AS t");
		Assert.Equal("15", v);
	}

	// ---- IN subquery ----
	[Fact]
	public async Task InSub_Found()
	{
		var rows = await Query("SELECT x FROM UNNEST([1,2,3,4,5]) AS x WHERE x IN (SELECT y FROM UNNEST([2,4]) AS y) ORDER BY x");
		Assert.Equal(2, rows.Count);
		Assert.Equal("2", rows[0][0]?.ToString());
		Assert.Equal("4", rows[1][0]?.ToString());
	}

	[Fact]
	public async Task InSub_NotFound()
	{
		var rows = await Query("SELECT x FROM UNNEST([1,2,3]) AS x WHERE x IN (SELECT y FROM UNNEST([10,20]) AS y)");
		Assert.Empty(rows);
	}

	[Fact] public async Task NotInSub_Basic()
	{
		var rows = await Query("SELECT x FROM UNNEST([1,2,3,4,5]) AS x WHERE x NOT IN (SELECT y FROM UNNEST([2,4]) AS y) ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("3", rows[1][0]?.ToString());
		Assert.Equal("5", rows[2][0]?.ToString());
	}

	// ---- EXISTS subquery ----
	[Fact] public async Task Exists_True() => Assert.Equal("True", await Scalar("SELECT EXISTS(SELECT 1)"));
	[Fact] public async Task Exists_TrueWithData() => Assert.Equal("True", await Scalar("SELECT EXISTS(SELECT x FROM UNNEST([1,2,3]) AS x)"));
	[Fact] public async Task Exists_FalseEmpty() => Assert.Equal("False", await Scalar("SELECT EXISTS(SELECT x FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x)"));
	[Fact] public async Task Exists_WithFilter() => Assert.Equal("True", await Scalar("SELECT EXISTS(SELECT x FROM UNNEST([1,2,3]) AS x WHERE x > 2)"));
	[Fact] public async Task Exists_NoMatchFilter() => Assert.Equal("False", await Scalar("SELECT EXISTS(SELECT x FROM UNNEST([1,2,3]) AS x WHERE x > 10)"));
	[Fact] public async Task NotExists_True() => Assert.Equal("True", await Scalar("SELECT NOT EXISTS(SELECT x FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x)"));
	[Fact] public async Task NotExists_False() => Assert.Equal("False", await Scalar("SELECT NOT EXISTS(SELECT 1)"));

	// ---- Nested FROM subqueries ----
	[Fact]
	public async Task NestedFromSub()
	{
		var v = await Scalar("SELECT v FROM (SELECT v FROM (SELECT 99 AS v) AS inner_t) AS outer_t");
		Assert.Equal("99", v);
	}

	[Fact]
	public async Task NestedFromSub_WithAgg()
	{
		var v = await Scalar("SELECT total FROM (SELECT SUM(x) AS total FROM UNNEST([1,2,3]) AS x) AS t");
		Assert.Equal("6", v);
	}

	// ---- ARRAY subquery ----
	[Fact] public async Task ArraySub_Length()
	{
		var v = await Scalar("SELECT ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST([1,2,3,4,5]) AS x WHERE x > 2))");
		Assert.Equal("3", v);
	}

	[Fact] public async Task ArraySub_ToString()
	{
		var v = await Scalar("SELECT ARRAY_TO_STRING(ARRAY(SELECT x FROM UNNEST([3,1,2]) AS x ORDER BY x), ',')");
		Assert.Equal("1,2,3", v);
	}

	// ---- Subquery with LIMIT ----
	[Fact]
	public async Task SubLimit_Top3()
	{
		var v = await Scalar("SELECT COUNT(*) FROM (SELECT x FROM UNNEST([1,2,3,4,5]) AS x LIMIT 3) AS t");
		Assert.Equal("3", v);
	}

	// ---- Subquery in CASE ----
	[Fact]
	public async Task SubInCase()
	{
		var v = await Scalar("SELECT CASE WHEN (SELECT COUNT(*) FROM UNNEST([1,2,3]) AS x) > 2 THEN 'many' ELSE 'few' END");
		Assert.Equal("many", v);
	}

	// ---- WITH (CTE) ----
	[Fact]
	public async Task CTE_Simple()
	{
		var v = await Scalar("WITH nums AS (SELECT x FROM UNNEST([1,2,3,4,5]) AS x) SELECT SUM(x) FROM nums");
		Assert.Equal("15", v);
	}

	[Fact]
	public async Task CTE_MultiRef()
	{
		var rows = await Query(@"
			WITH nums AS (SELECT x FROM UNNEST([1,2,3]) AS x)
			SELECT a.x + b.x AS total
			FROM nums a CROSS JOIN nums b
			ORDER BY total LIMIT 3");
		Assert.Equal(3, rows.Count);
		Assert.Equal("2", rows[0]["total"]?.ToString());
	}

	[Fact]
	public async Task CTE_Chain()
	{
		var v = await Scalar(@"
			WITH step1 AS (SELECT x FROM UNNEST([1,2,3,4,5]) AS x),
			     step2 AS (SELECT x FROM step1 WHERE x > 2)
			SELECT SUM(x) FROM step2");
		Assert.Equal("12", v);
	}

	[Fact]
	public async Task CTE_WithGroupBy()
	{
		var rows = await Query(@"
			WITH data AS (
				SELECT x FROM UNNEST([1,1,2,2,3]) AS x
			)
			SELECT x, COUNT(*) as cnt FROM data GROUP BY x ORDER BY x");
		Assert.Equal(3, rows.Count);
	}
}
