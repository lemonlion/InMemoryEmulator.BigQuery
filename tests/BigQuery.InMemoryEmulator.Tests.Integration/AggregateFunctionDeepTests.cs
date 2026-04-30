using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for aggregate functions: COUNT, SUM, AVG, MIN, MAX, COUNTIF, LOGICAL_AND, LOGICAL_OR,
/// STRING_AGG, ARRAY_AGG, ANY_VALUE, APPROX_COUNT_DISTINCT, BIT_AND, BIT_OR, BIT_XOR.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class AggregateFunctionDeepTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public AggregateFunctionDeepTests(BigQuerySession session) => _session = session;
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

	// ---- COUNT ----
	[Fact(Skip = "UNNEST in FROM without parent table not supported in parser")] public async Task Count_Empty() => Assert.Equal("0", await Scalar("SELECT COUNT(*) FROM UNNEST(CAST([] AS ARRAY<INT64>))"));
	[Fact] public async Task Count_One() => Assert.Equal("1", await Scalar("SELECT COUNT(*) FROM UNNEST([1]) AS x"));
	[Fact] public async Task Count_Three() => Assert.Equal("3", await Scalar("SELECT COUNT(*) FROM UNNEST([1,2,3]) AS x"));
	[Fact] public async Task Count_Five() => Assert.Equal("5", await Scalar("SELECT COUNT(*) FROM UNNEST([1,2,3,4,5]) AS x"));
	[Fact] public async Task Count_Expr() => Assert.Equal("3", await Scalar("SELECT COUNT(x) FROM UNNEST([1,2,3]) AS x"));
	[Fact] public async Task Count_Distinct() => Assert.Equal("3", await Scalar("SELECT COUNT(DISTINCT x) FROM UNNEST([1,1,2,2,3,3]) AS x"));
	[Fact] public async Task Count_DistinctStr() => Assert.Equal("2", await Scalar("SELECT COUNT(DISTINCT x) FROM UNNEST(['a','a','b']) AS x"));
	[Fact] public async Task Count_Star_Strings() => Assert.Equal("3", await Scalar("SELECT COUNT(*) FROM UNNEST(['a','b','c']) AS x"));

	// ---- SUM ----
	[Fact] public async Task Sum_Basic() => Assert.Equal("6", await Scalar("SELECT SUM(x) FROM UNNEST([1,2,3]) AS x"));
	[Fact] public async Task Sum_One() => Assert.Equal("5", await Scalar("SELECT SUM(x) FROM UNNEST([5]) AS x"));
	[Fact] public async Task Sum_Negative() => Assert.Equal("-6", await Scalar("SELECT SUM(x) FROM UNNEST([-1,-2,-3]) AS x"));
	[Fact] public async Task Sum_Mixed() => Assert.Equal("0", await Scalar("SELECT SUM(x) FROM UNNEST([-1,0,1]) AS x"));
	[Fact] public async Task Sum_Large() => Assert.Equal("55", await Scalar("SELECT SUM(x) FROM UNNEST([1,2,3,4,5,6,7,8,9,10]) AS x"));
	[Fact] public async Task Sum_Distinct() => Assert.Equal("6", await Scalar("SELECT SUM(DISTINCT x) FROM UNNEST([1,1,2,2,3,3]) AS x"));
	[Fact] public async Task Sum_AllSame() => Assert.Equal("15", await Scalar("SELECT SUM(x) FROM UNNEST([5,5,5]) AS x"));

	// ---- AVG ----
	[Fact] public async Task Avg_Basic() => Assert.Equal("2", await Scalar("SELECT CAST(AVG(x) AS INT64) FROM UNNEST([1,2,3]) AS x"));
	[Fact] public async Task Avg_OneVal() => Assert.Equal("5", await Scalar("SELECT CAST(AVG(x) AS INT64) FROM UNNEST([5]) AS x"));
	[Fact] public async Task Avg_EvenCount() => Assert.Equal("2.5", await Scalar("SELECT AVG(x) FROM UNNEST([1.0, 2.0, 3.0, 4.0]) AS x"));
	[Fact] public async Task Avg_Same() => Assert.Equal("5", await Scalar("SELECT CAST(AVG(x) AS INT64) FROM UNNEST([5,5,5]) AS x"));

	// ---- MIN / MAX ----
	[Fact] public async Task Min_Basic() => Assert.Equal("1", await Scalar("SELECT MIN(x) FROM UNNEST([3,1,2]) AS x"));
	[Fact] public async Task Min_OneVal() => Assert.Equal("5", await Scalar("SELECT MIN(x) FROM UNNEST([5]) AS x"));
	[Fact] public async Task Min_Negative() => Assert.Equal("-3", await Scalar("SELECT MIN(x) FROM UNNEST([1,-3,2]) AS x"));
	[Fact] public async Task Min_AllSame() => Assert.Equal("5", await Scalar("SELECT MIN(x) FROM UNNEST([5,5,5]) AS x"));
	[Fact] public async Task Min_String() => Assert.Equal("a", await Scalar("SELECT MIN(x) FROM UNNEST(['c','a','b']) AS x"));
	[Fact] public async Task Max_Basic() => Assert.Equal("3", await Scalar("SELECT MAX(x) FROM UNNEST([3,1,2]) AS x"));
	[Fact] public async Task Max_OneVal() => Assert.Equal("5", await Scalar("SELECT MAX(x) FROM UNNEST([5]) AS x"));
	[Fact] public async Task Max_Negative() => Assert.Equal("2", await Scalar("SELECT MAX(x) FROM UNNEST([1,-3,2]) AS x"));
	[Fact] public async Task Max_AllSame() => Assert.Equal("5", await Scalar("SELECT MAX(x) FROM UNNEST([5,5,5]) AS x"));
	[Fact] public async Task Max_String() => Assert.Equal("c", await Scalar("SELECT MAX(x) FROM UNNEST(['c','a','b']) AS x"));

	// ---- COUNTIF ----
	[Fact] public async Task Countif_All() => Assert.Equal("3", await Scalar("SELECT COUNTIF(x > 0) FROM UNNEST([1,2,3]) AS x"));
	[Fact] public async Task Countif_None() => Assert.Equal("0", await Scalar("SELECT COUNTIF(x > 10) FROM UNNEST([1,2,3]) AS x"));
	[Fact] public async Task Countif_Some() => Assert.Equal("2", await Scalar("SELECT COUNTIF(x > 1) FROM UNNEST([1,2,3]) AS x"));
	[Fact] public async Task Countif_Bool() => Assert.Equal("2", await Scalar("SELECT COUNTIF(x) FROM UNNEST([TRUE, TRUE, FALSE]) AS x"));

	// ---- LOGICAL_AND / LOGICAL_OR ----
	[Fact] public async Task LogicalAnd_AllTrue() => Assert.Equal("True", await Scalar("SELECT LOGICAL_AND(x) FROM UNNEST([TRUE, TRUE, TRUE]) AS x"));
	[Fact] public async Task LogicalAnd_OneFalse() => Assert.Equal("False", await Scalar("SELECT LOGICAL_AND(x) FROM UNNEST([TRUE, FALSE, TRUE]) AS x"));
	[Fact] public async Task LogicalOr_AllFalse() => Assert.Equal("False", await Scalar("SELECT LOGICAL_OR(x) FROM UNNEST([FALSE, FALSE, FALSE]) AS x"));
	[Fact] public async Task LogicalOr_OneTrue() => Assert.Equal("True", await Scalar("SELECT LOGICAL_OR(x) FROM UNNEST([FALSE, TRUE, FALSE]) AS x"));

	// ---- STRING_AGG ----
	[Fact] public async Task StringAgg_Comma() => Assert.Contains(",", await Scalar("SELECT STRING_AGG(x, ',') FROM UNNEST(['a','b','c']) AS x") ?? "");
	[Fact] public async Task StringAgg_Pipe() => Assert.Contains("|", await Scalar("SELECT STRING_AGG(x, '|') FROM UNNEST(['x','y']) AS x") ?? "");
	[Fact] public async Task StringAgg_Single() => Assert.Equal("hello", await Scalar("SELECT STRING_AGG(x, ',') FROM UNNEST(['hello']) AS x"));
	[Fact] public async Task StringAgg_Length() { var v = await Scalar("SELECT LENGTH(STRING_AGG(x, ',')) FROM UNNEST(['a','b','c']) AS x"); Assert.Equal("5", v); }

	// ---- ARRAY_AGG ----
	[Fact] public async Task ArrayAgg_Count() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(ARRAY_AGG(x)) FROM UNNEST([1,2,3]) AS x"));
	[Fact] public async Task ArrayAgg_Distinct() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(ARRAY_AGG(DISTINCT x)) FROM UNNEST([1,1,2,2,3,3]) AS x"));
	[Fact] public async Task ArrayAgg_Single() => Assert.Equal("1", await Scalar("SELECT ARRAY_LENGTH(ARRAY_AGG(x)) FROM UNNEST([42]) AS x"));

	// ---- ANY_VALUE ----
	[Fact] public async Task AnyValue_NotNull() { var v = await Scalar("SELECT ANY_VALUE(x) FROM UNNEST([1,2,3]) AS x"); Assert.NotNull(v); }
	[Fact] public async Task AnyValue_Single() => Assert.Equal("5", await Scalar("SELECT ANY_VALUE(x) FROM UNNEST([5]) AS x"));

	// ---- APPROX_COUNT_DISTINCT ----
	[Fact] public async Task ApproxCountDistinct_Basic() => Assert.Equal("3", await Scalar("SELECT APPROX_COUNT_DISTINCT(x) FROM UNNEST([1,1,2,2,3,3]) AS x"));
	[Fact] public async Task ApproxCountDistinct_All() => Assert.Equal("5", await Scalar("SELECT APPROX_COUNT_DISTINCT(x) FROM UNNEST([1,2,3,4,5]) AS x"));
	[Fact] public async Task ApproxCountDistinct_One() => Assert.Equal("1", await Scalar("SELECT APPROX_COUNT_DISTINCT(x) FROM UNNEST([1,1,1]) AS x"));

	// ---- BIT_AND / BIT_OR / BIT_XOR ----
	[Fact] public async Task BitAnd_Agg() => Assert.Equal("0", await Scalar("SELECT BIT_AND(x) FROM UNNEST([5,3,6]) AS x"));
	[Fact] public async Task BitOr_Agg() => Assert.Equal("7", await Scalar("SELECT BIT_OR(x) FROM UNNEST([1,2,4]) AS x"));
	[Fact] public async Task BitXor_Agg() => Assert.Equal("6", await Scalar("SELECT BIT_XOR(x) FROM UNNEST([3,5]) AS x"));

	// ---- Multiple aggregates in one query ----
	[Fact]
	public async Task Multi_Aggregates()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT COUNT(*), SUM(x), MIN(x), MAX(x) FROM UNNEST([1,2,3,4,5]) AS x",
			parameters: null);
		var row = result.Single();
		Assert.Equal("5", row[0]?.ToString());
		Assert.Equal("15", row[1]?.ToString());
		Assert.Equal("1", row[2]?.ToString());
		Assert.Equal("5", row[3]?.ToString());
	}

	// ---- GROUP BY with aggregates ----
	[Fact]
	public async Task GroupBy_Count()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT grp, COUNT(*) as cnt FROM UNNEST([1,1,2,2,2,3]) AS grp GROUP BY grp ORDER BY grp",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("2", rows[0][1]?.ToString());
		Assert.Equal("2", rows[1][0]?.ToString());
		Assert.Equal("3", rows[1][1]?.ToString());
		Assert.Equal("3", rows[2][0]?.ToString());
		Assert.Equal("1", rows[2][1]?.ToString());
	}

	[Fact]
	public async Task GroupBy_Sum()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH data AS (
  SELECT 'a' AS grp, 10 AS val UNION ALL
  SELECT 'a', 20 UNION ALL
  SELECT 'b', 30 UNION ALL
  SELECT 'b', 40
)
SELECT grp, SUM(val) as total FROM data GROUP BY grp ORDER BY grp",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(2, rows.Count);
		Assert.Equal("a", rows[0][0]?.ToString());
		Assert.Equal("30", rows[0][1]?.ToString());
		Assert.Equal("b", rows[1][0]?.ToString());
		Assert.Equal("70", rows[1][1]?.ToString());
	}

	[Fact]
	public async Task GroupBy_Having()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT grp, COUNT(*) as cnt FROM UNNEST([1,1,2,2,2,3]) AS grp GROUP BY grp HAVING COUNT(*) > 1 ORDER BY grp",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(2, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("2", rows[1][0]?.ToString());
	}

	[Fact]
	public async Task GroupBy_MinMax()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH data AS (
  SELECT 'a' AS grp, 10 AS val UNION ALL
  SELECT 'a', 20 UNION ALL
  SELECT 'a', 30 UNION ALL
  SELECT 'b', 5 UNION ALL
  SELECT 'b', 15
)
SELECT grp, MIN(val), MAX(val) FROM data GROUP BY grp ORDER BY grp",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(2, rows.Count);
		Assert.Equal("10", rows[0][1]?.ToString());
		Assert.Equal("30", rows[0][2]?.ToString());
		Assert.Equal("5", rows[1][1]?.ToString());
		Assert.Equal("15", rows[1][2]?.ToString());
	}

	[Fact]
	public async Task GroupBy_Avg()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH data AS (
  SELECT 'a' AS grp, 10.0 AS val UNION ALL
  SELECT 'a', 20.0 UNION ALL
  SELECT 'b', 30.0 UNION ALL
  SELECT 'b', 40.0
)
SELECT grp, AVG(val) as avg_val FROM data GROUP BY grp ORDER BY grp",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(2, rows.Count);
		Assert.Equal("15", rows[0][1]?.ToString());
		Assert.Equal("35", rows[1][1]?.ToString());
	}
}
