using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for aggregate functions: COUNT, SUM, AVG, MIN, MAX, STRING_AGG, LOGICAL_AND/OR, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class AggregatePatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public AggregatePatternTests(BigQuerySession session) => _session = session;
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

	// ---- COUNT variations ----
	[Fact] public async Task Count_Star_5() => Assert.Equal("5", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x"));
	[Fact] public async Task Count_Star_100() => Assert.Equal("100", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 100)) AS x"));
	[Fact] public async Task Count_Star_0() => Assert.Equal("0", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(5, 1)) AS x"));
	[Fact] public async Task Count_Col() => Assert.Equal("3", await Scalar("SELECT COUNT(x) FROM UNNEST([1, NULL, 2, NULL, 3]) AS x"));
	[Fact] public async Task Count_AllNull() => Assert.Equal("0", await Scalar("SELECT COUNT(x) FROM UNNEST([CAST(NULL AS INT64), NULL, NULL]) AS x"));
	[Fact] public async Task CountDistinct_Dups() => Assert.Equal("3", await Scalar("SELECT COUNT(DISTINCT x) FROM UNNEST([1, 2, 2, 3, 3, 3]) AS x"));
	[Fact] public async Task CountDistinct_Unique() => Assert.Equal("5", await Scalar("SELECT COUNT(DISTINCT x) FROM UNNEST([1, 2, 3, 4, 5]) AS x"));
	[Fact] public async Task CountDistinct_AllSame() => Assert.Equal("1", await Scalar("SELECT COUNT(DISTINCT x) FROM UNNEST([7, 7, 7, 7]) AS x"));

	// ---- SUM variations ----
	[Fact] public async Task Sum_Positive() => Assert.Equal("15", await Scalar("SELECT SUM(x) FROM UNNEST([1, 2, 3, 4, 5]) AS x"));
	[Fact] public async Task Sum_WithNulls() => Assert.Equal("6", await Scalar("SELECT SUM(x) FROM UNNEST([1, NULL, 2, NULL, 3]) AS x"));
	[Fact] public async Task Sum_AllNull() => Assert.Null(await Scalar("SELECT SUM(x) FROM UNNEST([CAST(NULL AS INT64), NULL]) AS x"));
	[Fact] public async Task Sum_Negative() => Assert.Equal("-6", await Scalar("SELECT SUM(x) FROM UNNEST([-1, -2, -3]) AS x"));
	[Fact] public async Task Sum_Mixed() => Assert.Equal("0", await Scalar("SELECT SUM(x) FROM UNNEST([-3, -2, -1, 1, 2, 3]) AS x"));
	[Fact] public async Task Sum_Large() => Assert.Equal("5050", await Scalar("SELECT SUM(x) FROM UNNEST(GENERATE_ARRAY(1, 100)) AS x"));

	// ---- AVG variations ----
	[Fact] public async Task Avg_Integers() => Assert.Equal("3", await Scalar("SELECT AVG(x) FROM UNNEST([1, 2, 3, 4, 5]) AS x"));
	[Fact] public async Task Avg_WithNulls() => Assert.Equal("2", await Scalar("SELECT AVG(x) FROM UNNEST([1, NULL, 2, NULL, 3]) AS x"));
	[Fact] public async Task Avg_Float() => Assert.Equal("5.5", await Scalar("SELECT AVG(x) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x"));
	[Fact] public async Task Avg_Single() => Assert.Equal("42", await Scalar("SELECT AVG(x) FROM UNNEST([42]) AS x"));

	// ---- MIN / MAX ----
	[Fact] public async Task Min_Integers() => Assert.Equal("1", await Scalar("SELECT MIN(x) FROM UNNEST([5, 3, 1, 4, 2]) AS x"));
	[Fact] public async Task Min_WithNulls() => Assert.Equal("1", await Scalar("SELECT MIN(x) FROM UNNEST([NULL, 5, 3, NULL, 1]) AS x"));
	[Fact] public async Task Min_Strings() => Assert.Equal("apple", await Scalar("SELECT MIN(x) FROM UNNEST(['cherry', 'apple', 'banana']) AS x"));
	[Fact] public async Task Min_Single() => Assert.Equal("42", await Scalar("SELECT MIN(x) FROM UNNEST([42]) AS x"));
	[Fact] public async Task Max_Integers() => Assert.Equal("5", await Scalar("SELECT MAX(x) FROM UNNEST([5, 3, 1, 4, 2]) AS x"));
	[Fact] public async Task Max_WithNulls() => Assert.Equal("5", await Scalar("SELECT MAX(x) FROM UNNEST([NULL, 5, 3, NULL, 1]) AS x"));
	[Fact] public async Task Max_Strings() => Assert.Equal("cherry", await Scalar("SELECT MAX(x) FROM UNNEST(['cherry', 'apple', 'banana']) AS x"));
	[Fact] public async Task Max_Negative() => Assert.Equal("-1", await Scalar("SELECT MAX(x) FROM UNNEST([-5, -3, -1, -4, -2]) AS x"));

	// ---- STRING_AGG ----
	[Fact]
	public async Task StringAgg_Default()
	{
		var v = await Scalar("SELECT STRING_AGG(x) FROM UNNEST(['a', 'b', 'c']) AS x");
		Assert.NotNull(v);
		Assert.Contains("a", v);
		Assert.Contains("b", v);
		Assert.Contains("c", v);
	}

	[Fact(Skip = "Emulator limitation")]
	public async Task StringAgg_OrderBy()
	{
		var v = await Scalar("SELECT STRING_AGG(x ORDER BY x) FROM UNNEST(['c', 'a', 'b']) AS x");
		Assert.NotNull(v);
		// Verify a comes before b, b before c
		Assert.True(v!.IndexOf("a") < v.IndexOf("b"));
		Assert.True(v.IndexOf("b") < v.IndexOf("c"));
	}

	[Fact(Skip = "Emulator limitation")]
	public async Task StringAgg_Integers()
	{
		var v = await Scalar("SELECT STRING_AGG(CAST(x AS STRING) ORDER BY x) FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x");
		Assert.NotNull(v);
		Assert.Contains("1", v);
		Assert.Contains("5", v);
	}

	// ---- LOGICAL_AND / LOGICAL_OR ----
	[Fact] public async Task LogicalAnd_AllTrue() => Assert.Equal("True", await Scalar("SELECT LOGICAL_AND(x) FROM UNNEST([TRUE, TRUE, TRUE]) AS x"));
	[Fact] public async Task LogicalAnd_MixedFalse() => Assert.Equal("False", await Scalar("SELECT LOGICAL_AND(x) FROM UNNEST([TRUE, FALSE, TRUE]) AS x"));
	[Fact] public async Task LogicalOr_AllFalse() => Assert.Equal("False", await Scalar("SELECT LOGICAL_OR(x) FROM UNNEST([FALSE, FALSE, FALSE]) AS x"));
	[Fact] public async Task LogicalOr_OneTrue() => Assert.Equal("True", await Scalar("SELECT LOGICAL_OR(x) FROM UNNEST([FALSE, TRUE, FALSE]) AS x"));

	// ---- GROUP BY aggregate patterns ----
	[Fact(Skip = "Emulator limitation")]
	public async Task GroupBy_CountPerGroup()
	{
		var v = await Scalar(@"
SELECT COUNT(*) FROM (
    SELECT MOD(x, 3) AS grp, COUNT(*) AS cnt
    FROM UNNEST(GENERATE_ARRAY(1, 9)) AS x
    GROUP BY grp
    HAVING COUNT(*) = 3
) AS t");
		Assert.Equal("3", v);
	}

	[Fact]
	public async Task GroupBy_SumPerGroup()
	{
		var v = await Column(@"
SELECT SUM(x) AS s FROM UNNEST(GENERATE_ARRAY(1, 6)) AS x
GROUP BY MOD(x, 2)
ORDER BY s");
		Assert.Equal(new[] { "9", "12" }, v);
	}

	[Fact(Skip = "Emulator limitation")]
	public async Task GroupBy_AvgPerGroup()
	{
		var v = await Scalar(@"
SELECT AVG(x) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x
GROUP BY MOD(x, 2)
ORDER BY AVG(x)
LIMIT 1");
		Assert.Equal("5", v);
	}

	[Fact]
	public async Task GroupBy_MinMaxPerGroup()
	{
		var v = await Scalar(@"
SELECT MAX(x) - MIN(x) AS rng
FROM UNNEST(GENERATE_ARRAY(1, 20)) AS x
GROUP BY MOD(x, 5)
ORDER BY rng DESC
LIMIT 1");
		Assert.Equal("15", v);
	}

	// ---- Aggregate with CASE ----
	[Fact]
	public async Task Aggregate_CountIf()
	{
		var v = await Scalar("SELECT COUNTIF(x > 5) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x");
		Assert.Equal("5", v);
	}

	[Fact]
	public async Task Aggregate_CountIf_None()
	{
		var v = await Scalar("SELECT COUNTIF(x > 100) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x");
		Assert.Equal("0", v);
	}

	[Fact]
	public async Task Aggregate_CountIf_All()
	{
		var v = await Scalar("SELECT COUNTIF(x > 0) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x");
		Assert.Equal("10", v);
	}

	// ---- Multiple aggregates in one query ----
	[Fact]
	public async Task MultiAgg_SumCountMinMax()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT SUM(x), COUNT(*), MIN(x), MAX(x) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal("55", rows[0][0]?.ToString());
		Assert.Equal("10", rows[0][1]?.ToString());
		Assert.Equal("1", rows[0][2]?.ToString());
		Assert.Equal("10", rows[0][3]?.ToString());
	}

	// ---- Aggregate on empty set ----
	[Fact] public async Task Sum_Empty() => Assert.Null(await Scalar("SELECT SUM(x) FROM UNNEST(GENERATE_ARRAY(5, 1)) AS x"));
	[Fact] public async Task Avg_Empty() => Assert.Null(await Scalar("SELECT AVG(x) FROM UNNEST(GENERATE_ARRAY(5, 1)) AS x"));
	[Fact] public async Task Min_Empty() => Assert.Null(await Scalar("SELECT MIN(x) FROM UNNEST(GENERATE_ARRAY(5, 1)) AS x"));
	[Fact] public async Task Max_Empty() => Assert.Null(await Scalar("SELECT MAX(x) FROM UNNEST(GENERATE_ARRAY(5, 1)) AS x"));
	[Fact] public async Task Count_Empty() => Assert.Equal("0", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(5, 1)) AS x"));
}
