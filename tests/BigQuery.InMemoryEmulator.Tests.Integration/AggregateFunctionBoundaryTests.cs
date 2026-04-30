using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for aggregate functions with various data patterns using UNNEST.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class AggregateFunctionBoundaryTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public AggregateFunctionBoundaryTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- COUNT ----
	[Fact] public async Task Count_Star() => Assert.Equal("5", await Scalar("SELECT COUNT(*) FROM UNNEST([1,2,3,4,5]) AS x"));
	[Fact] public async Task Count_Column() => Assert.Equal("5", await Scalar("SELECT COUNT(x) FROM UNNEST([1,2,3,4,5]) AS x"));
	[Fact] public async Task Count_Distinct() => Assert.Equal("3", await Scalar("SELECT COUNT(DISTINCT x) FROM UNNEST([1,2,2,3,3]) AS x"));
	[Fact] public async Task Count_Empty() => Assert.Equal("0", await Scalar("SELECT COUNT(*) FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x"));
	[Fact] public async Task Count_Single() => Assert.Equal("1", await Scalar("SELECT COUNT(*) FROM UNNEST([42]) AS x"));
	[Fact] public async Task Count_Large() => Assert.Equal("100", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1,100)) AS x"));
	[Fact] public async Task Count_AllSame() => Assert.Equal("1", await Scalar("SELECT COUNT(DISTINCT x) FROM UNNEST([5,5,5,5]) AS x"));

	// ---- SUM ----
	[Fact] public async Task Sum_Ints() => Assert.Equal("15", await Scalar("SELECT SUM(x) FROM UNNEST([1,2,3,4,5]) AS x"));
	[Fact] public async Task Sum_Single() => Assert.Equal("42", await Scalar("SELECT SUM(x) FROM UNNEST([42]) AS x"));
	[Fact] public async Task Sum_Negative() => Assert.Equal("-6", await Scalar("SELECT SUM(x) FROM UNNEST([-1,-2,-3]) AS x"));
	[Fact] public async Task Sum_Mixed() => Assert.Equal("0", await Scalar("SELECT SUM(x) FROM UNNEST([-1,0,1]) AS x"));
	[Fact] public async Task Sum_Large() => Assert.Equal("5050", await Scalar("SELECT SUM(x) FROM UNNEST(GENERATE_ARRAY(1,100)) AS x"));
	[Fact] public async Task Sum_Zeros() => Assert.Equal("0", await Scalar("SELECT SUM(x) FROM UNNEST([0,0,0]) AS x"));

	// ---- AVG ----
	[Fact] public async Task Avg_Ints() { var v = double.Parse(await Scalar("SELECT AVG(x) FROM UNNEST([1,2,3,4,5]) AS x") ?? "0"); Assert.Equal(3.0, v); }
	[Fact] public async Task Avg_Single() { var v = double.Parse(await Scalar("SELECT AVG(x) FROM UNNEST([42]) AS x") ?? "0"); Assert.Equal(42.0, v); }
	[Fact] public async Task Avg_Mixed() { var v = double.Parse(await Scalar("SELECT AVG(x) FROM UNNEST([-10, 10]) AS x") ?? "0"); Assert.Equal(0.0, v); }
	[Fact] public async Task Avg_Floats() { var v = double.Parse(await Scalar("SELECT AVG(x) FROM UNNEST([1.0, 2.0, 3.0]) AS x") ?? "0"); Assert.Equal(2.0, v); }

	// ---- MIN / MAX ----
	[Fact] public async Task Min_Ints() => Assert.Equal("1", await Scalar("SELECT MIN(x) FROM UNNEST([3,1,4,1,5]) AS x"));
	[Fact] public async Task Min_Negative() => Assert.Equal("-10", await Scalar("SELECT MIN(x) FROM UNNEST([-10, 0, 10]) AS x"));
	[Fact] public async Task Min_Single() => Assert.Equal("42", await Scalar("SELECT MIN(x) FROM UNNEST([42]) AS x"));
	[Fact] public async Task Min_Strings() => Assert.Equal("apple", await Scalar("SELECT MIN(x) FROM UNNEST(['cherry','apple','banana']) AS x"));
	[Fact] public async Task Max_Ints() => Assert.Equal("5", await Scalar("SELECT MAX(x) FROM UNNEST([3,1,4,1,5]) AS x"));
	[Fact] public async Task Max_Negative() => Assert.Equal("10", await Scalar("SELECT MAX(x) FROM UNNEST([-10, 0, 10]) AS x"));
	[Fact] public async Task Max_Single() => Assert.Equal("42", await Scalar("SELECT MAX(x) FROM UNNEST([42]) AS x"));
	[Fact] public async Task Max_Strings() => Assert.Equal("cherry", await Scalar("SELECT MAX(x) FROM UNNEST(['cherry','apple','banana']) AS x"));

	// ---- ANY_VALUE ----
	[Fact] public async Task AnyValue_NotNull() { var v = await Scalar("SELECT ANY_VALUE(x) FROM UNNEST([1,2,3]) AS x"); Assert.NotNull(v); }
	[Fact] public async Task AnyValue_Single() => Assert.Equal("42", await Scalar("SELECT ANY_VALUE(x) FROM UNNEST([42]) AS x"));
	[Fact] public async Task AnyValue_String() { var v = await Scalar("SELECT ANY_VALUE(x) FROM UNNEST(['a','b','c']) AS x"); Assert.NotNull(v); }

	// ---- STRING_AGG ----
	[Fact] public async Task StringAgg_Comma() { var v = await Scalar("SELECT STRING_AGG(x, ',') FROM UNNEST(['a','b','c']) AS x"); Assert.NotNull(v); Assert.Contains("a", v); Assert.Contains("b", v); Assert.Contains("c", v); }
	[Fact] public async Task StringAgg_Dash() { var v = await Scalar("SELECT STRING_AGG(x, '-') FROM UNNEST(['x','y','z']) AS x"); Assert.NotNull(v); }
	[Fact] public async Task StringAgg_Single() { var v = await Scalar("SELECT STRING_AGG(x, ',') FROM UNNEST(['only']) AS x"); Assert.Equal("only", v); }

	// ---- COUNTIF ----
	[Fact] public async Task CountIf_AllTrue() => Assert.Equal("3", await Scalar("SELECT COUNTIF(x > 0) FROM UNNEST([1,2,3]) AS x"));
	[Fact] public async Task CountIf_NoneTrue() => Assert.Equal("0", await Scalar("SELECT COUNTIF(x > 10) FROM UNNEST([1,2,3]) AS x"));
	[Fact] public async Task CountIf_SomeTrue() => Assert.Equal("2", await Scalar("SELECT COUNTIF(x > 2) FROM UNNEST([1,2,3,4]) AS x"));
	[Fact] public async Task CountIf_BoolArray() => Assert.Equal("2", await Scalar("SELECT COUNTIF(x) FROM UNNEST([TRUE, FALSE, TRUE]) AS x"));

	// ---- LOGICAL_AND / LOGICAL_OR ----
	[Fact] public async Task LogicalAnd_AllTrue() => Assert.Equal("True", await Scalar("SELECT LOGICAL_AND(x) FROM UNNEST([TRUE, TRUE, TRUE]) AS x"));
	[Fact] public async Task LogicalAnd_SomeFalse() => Assert.Equal("False", await Scalar("SELECT LOGICAL_AND(x) FROM UNNEST([TRUE, FALSE, TRUE]) AS x"));
	[Fact] public async Task LogicalOr_AllFalse() => Assert.Equal("False", await Scalar("SELECT LOGICAL_OR(x) FROM UNNEST([FALSE, FALSE, FALSE]) AS x"));
	[Fact] public async Task LogicalOr_SomeTrue() => Assert.Equal("True", await Scalar("SELECT LOGICAL_OR(x) FROM UNNEST([FALSE, TRUE, FALSE]) AS x"));

	// ---- BIT_AND / BIT_OR / BIT_XOR ----
	[Fact] public async Task BitAnd_AllOnes() => Assert.Equal("1", await Scalar("SELECT BIT_AND(x) FROM UNNEST([1, 3, 5]) AS x"));
	[Fact] public async Task BitOr_Basic() => Assert.Equal("7", await Scalar("SELECT BIT_OR(x) FROM UNNEST([1, 2, 4]) AS x"));
	[Fact] public async Task BitXor_Basic() => Assert.Equal("6", await Scalar("SELECT BIT_XOR(x) FROM UNNEST([3, 5]) AS x"));

	// ---- Multiple aggregates in one query ----
	[Fact] public async Task MultiAgg_SumCount()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync("SELECT SUM(x), COUNT(x) FROM UNNEST([1,2,3,4,5]) AS x", parameters: null);
		var rows = result.ToList();
		Assert.Equal("15", rows[0][0]?.ToString());
		Assert.Equal("5", rows[0][1]?.ToString());
	}
	[Fact] public async Task MultiAgg_MinMax()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync("SELECT MIN(x), MAX(x) FROM UNNEST([3,1,4,1,5,9]) AS x", parameters: null);
		var rows = result.ToList();
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("9", rows[0][1]?.ToString());
	}
	[Fact] public async Task MultiAgg_SumAvg()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync("SELECT SUM(x), AVG(x) FROM UNNEST([10, 20, 30]) AS x", parameters: null);
		var rows = result.ToList();
		Assert.Equal("60", rows[0][0]?.ToString());
		Assert.Equal("20", rows[0][1]?.ToString());
	}

	// ---- Aggregate with WHERE filter ----
	[Fact] public async Task SumWhere_Filter() => Assert.Equal("12", await Scalar("SELECT SUM(x) FROM UNNEST([1,2,3,4,5]) AS x WHERE x > 2"));
	[Fact] public async Task CountWhere_Filter() => Assert.Equal("3", await Scalar("SELECT COUNT(x) FROM UNNEST([1,2,3,4,5]) AS x WHERE x > 2"));
	[Fact] public async Task AvgWhere_Filter() { var v = double.Parse(await Scalar("SELECT AVG(x) FROM UNNEST([1,2,3,4,5]) AS x WHERE x > 2") ?? "0"); Assert.Equal(4.0, v); }
	[Fact] public async Task MinWhere_Filter() => Assert.Equal("3", await Scalar("SELECT MIN(x) FROM UNNEST([1,2,3,4,5]) AS x WHERE x > 2"));
	[Fact] public async Task MaxWhere_Filter() => Assert.Equal("5", await Scalar("SELECT MAX(x) FROM UNNEST([1,2,3,4,5]) AS x WHERE x > 2"));

	// ---- Aggregate with DISTINCT ----
	[Fact] public async Task SumDistinct() => Assert.Equal("6", await Scalar("SELECT SUM(DISTINCT x) FROM UNNEST([1,1,2,2,3]) AS x"));
	[Fact] public async Task AvgDistinct() { var v = double.Parse(await Scalar("SELECT AVG(DISTINCT x) FROM UNNEST([1,1,2,2,3]) AS x") ?? "0"); Assert.Equal(2.0, v); }
}
