using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for UNNEST patterns, WITH OFFSET, cross join unnest, nested UNNEST.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unnest
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class UnnestPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public UnnestPatternTests(BigQuerySession session) => _session = session;
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

	// ---- Basic UNNEST ----
	[Fact] public async Task Unnest_Count1() => Assert.Equal("1", await Scalar("SELECT COUNT(*) FROM UNNEST([42]) AS x"));
	[Fact] public async Task Unnest_Count3() => Assert.Equal("3", await Scalar("SELECT COUNT(*) FROM UNNEST([1,2,3]) AS x"));
	[Fact] public async Task Unnest_Count5() => Assert.Equal("5", await Scalar("SELECT COUNT(*) FROM UNNEST([1,2,3,4,5]) AS x"));
	[Fact] public async Task Unnest_Count10() => Assert.Equal("10", await Scalar("SELECT COUNT(*) FROM UNNEST([1,2,3,4,5,6,7,8,9,10]) AS x"));
	[Fact] public async Task Unnest_Sum() => Assert.Equal("15", await Scalar("SELECT SUM(x) FROM UNNEST([1,2,3,4,5]) AS x"));
	[Fact] public async Task Unnest_Max() => Assert.Equal("5", await Scalar("SELECT MAX(x) FROM UNNEST([1,2,3,4,5]) AS x"));
	[Fact] public async Task Unnest_Min() => Assert.Equal("1", await Scalar("SELECT MIN(x) FROM UNNEST([1,2,3,4,5]) AS x"));

	// ---- UNNEST with strings ----
	[Fact] public async Task Unnest_Strings_Count() => Assert.Equal("3", await Scalar("SELECT COUNT(*) FROM UNNEST(['a','b','c']) AS x"));
	[Fact] public async Task Unnest_Strings_Min() => Assert.Equal("a", await Scalar("SELECT MIN(x) FROM UNNEST(['c','a','b']) AS x"));
	[Fact] public async Task Unnest_Strings_Max() => Assert.Equal("c", await Scalar("SELECT MAX(x) FROM UNNEST(['c','a','b']) AS x"));

	// ---- UNNEST with ORDER BY ----
	[Fact]
	public async Task Unnest_OrderAsc()
	{
		var rows = await Column("SELECT x FROM UNNEST([3,1,2]) AS x ORDER BY x ASC");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]);
		Assert.Equal("2", rows[1]);
		Assert.Equal("3", rows[2]);
	}

	[Fact]
	public async Task Unnest_OrderDesc()
	{
		var rows = await Column("SELECT x FROM UNNEST([3,1,2]) AS x ORDER BY x DESC");
		Assert.Equal(3, rows.Count);
		Assert.Equal("3", rows[0]);
		Assert.Equal("2", rows[1]);
		Assert.Equal("1", rows[2]);
	}

	// ---- UNNEST with WHERE ----
	[Fact] public async Task Unnest_WhereGt() => Assert.Equal("2", await Scalar("SELECT COUNT(*) FROM UNNEST([1,2,3,4,5]) AS x WHERE x > 3"));
	[Fact] public async Task Unnest_WhereLt() => Assert.Equal("2", await Scalar("SELECT COUNT(*) FROM UNNEST([1,2,3,4,5]) AS x WHERE x < 3"));
	[Fact] public async Task Unnest_WhereEq() => Assert.Equal("1", await Scalar("SELECT COUNT(*) FROM UNNEST([1,2,3,4,5]) AS x WHERE x = 3"));
	[Fact] public async Task Unnest_WhereNeq() => Assert.Equal("4", await Scalar("SELECT COUNT(*) FROM UNNEST([1,2,3,4,5]) AS x WHERE x != 3"));
	[Fact] public async Task Unnest_WhereBetween() => Assert.Equal("3", await Scalar("SELECT COUNT(*) FROM UNNEST([1,2,3,4,5]) AS x WHERE x BETWEEN 2 AND 4"));
	[Fact] public async Task Unnest_WhereIn() => Assert.Equal("3", await Scalar("SELECT COUNT(*) FROM UNNEST([1,2,3,4,5]) AS x WHERE x IN (1, 3, 5)"));
	[Fact] public async Task Unnest_WhereLike2() => Assert.Equal("2", await Scalar("SELECT COUNT(*) FROM UNNEST(['apple','banana','avocado','cherry']) AS x WHERE x LIKE 'a%'"));
	[Fact] public async Task Unnest_WhereAnd() => Assert.Equal("2", await Scalar("SELECT COUNT(*) FROM UNNEST([1,2,3,4,5]) AS x WHERE x > 2 AND x < 5"));
	[Fact] public async Task Unnest_WhereOr() => Assert.Equal("2", await Scalar("SELECT COUNT(*) FROM UNNEST([1,2,3,4,5]) AS x WHERE x = 1 OR x = 5"));

	// ---- UNNEST with WITH OFFSET ----
	[Fact(Skip = "Emulator limitation")]
	public async Task Unnest_WithOffset_Values()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT x, off FROM UNNEST([10,20,30]) AS x WITH OFFSET AS off ORDER BY off",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(3, rows.Count);
		Assert.Equal("10", rows[0][0]?.ToString());
		Assert.Equal("0", rows[0][1]?.ToString());
		Assert.Equal("20", rows[1][0]?.ToString());
		Assert.Equal("1", rows[1][1]?.ToString());
		Assert.Equal("30", rows[2][0]?.ToString());
		Assert.Equal("2", rows[2][1]?.ToString());
	}

	[Fact(Skip = "Emulator limitation")]
	public async Task Unnest_WithOffset_Filter()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT x FROM UNNEST([10,20,30,40,50]) AS x WITH OFFSET AS off WHERE off < 3 ORDER BY off",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(3, rows.Count);
		Assert.Equal("10", rows[0][0]?.ToString());
		Assert.Equal("30", rows[2][0]?.ToString());
	}

	[Fact(Skip = "Emulator limitation")]
	public async Task Unnest_WithOffset_EvenOnly()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT x FROM UNNEST([10,20,30,40,50]) AS x WITH OFFSET AS off WHERE MOD(off, 2) = 0 ORDER BY off",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(3, rows.Count);
		Assert.Equal("10", rows[0][0]?.ToString());
		Assert.Equal("30", rows[1][0]?.ToString());
		Assert.Equal("50", rows[2][0]?.ToString());
	}

	// ---- UNNEST with LIMIT ----
	[Fact] public async Task Unnest_Limit() { var rows = await Column("SELECT x FROM UNNEST([1,2,3,4,5]) AS x ORDER BY x LIMIT 3"); Assert.Equal(3, rows.Count); }
	[Fact] public async Task Unnest_Offset() { var rows = await Column("SELECT x FROM UNNEST([1,2,3,4,5]) AS x ORDER BY x LIMIT 2 OFFSET 2"); Assert.Equal(2, rows.Count); Assert.Equal("3", rows[0]); }

	// ---- UNNEST with DISTINCT ----
	[Fact] public async Task Unnest_Distinct() { var rows = await Column("SELECT DISTINCT x FROM UNNEST([1,1,2,2,3,3]) AS x ORDER BY x"); Assert.Equal(3, rows.Count); }
	[Fact] public async Task Unnest_Distinct_Str() { var rows = await Column("SELECT DISTINCT x FROM UNNEST(['a','a','b','b','c']) AS x ORDER BY x"); Assert.Equal(3, rows.Count); }

	// ---- UNNEST cross join ----
	[Fact(Skip = "Emulator limitation")]
	public async Task Unnest_CrossJoin()
	{
		var v = await Scalar("SELECT COUNT(*) FROM UNNEST([1,2]) AS a, UNNEST([10,20,30]) AS b");
		Assert.Equal("6", v);
	}

	[Fact(Skip = "Emulator limitation")]
	public async Task Unnest_CrossJoin_Sum()
	{
		var v = await Scalar("SELECT SUM(a + b) FROM UNNEST([1,2]) AS a, UNNEST([10,20]) AS b");
		// (1+10) + (1+20) + (2+10) + (2+20) = 11 + 21 + 12 + 22 = 66
		Assert.Equal("66", v);
	}

	// ---- UNNEST with functions ----
	[Fact]
	public async Task Unnest_UpperStrings()
	{
		var rows = await Column("SELECT UPPER(x) FROM UNNEST(['hello','world']) AS x ORDER BY x");
		Assert.Equal(2, rows.Count);
		Assert.Equal("HELLO", rows[0]);
		Assert.Equal("WORLD", rows[1]);
	}

	[Fact]
	public async Task Unnest_LengthStrings()
	{
		var v = await Scalar("SELECT SUM(LENGTH(x)) FROM UNNEST(['hello','world','!']) AS x");
		Assert.Equal("11", v);
	}

	[Fact]
	public async Task Unnest_AbsValues()
	{
		var v = await Scalar("SELECT SUM(ABS(x)) FROM UNNEST([-1,-2,-3]) AS x");
		Assert.Equal("6", v);
	}

	// ---- UNNEST of GENERATE_ARRAY ----
	[Fact]
	public async Task Unnest_GenerateArray()
	{
		var rows = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x ORDER BY x");
		Assert.Equal(5, rows.Count);
		Assert.Equal("1", rows[0]);
		Assert.Equal("5", rows[4]);
	}

	[Fact]
	public async Task Unnest_GenerateArray_Sum() => Assert.Equal("55", await Scalar("SELECT SUM(x) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x"));

	// ---- Empty UNNEST ----
	[Fact(Skip = "Emulator limitation")] public async Task Unnest_Empty() => Assert.Equal("0", await Scalar("SELECT COUNT(*) FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x"));

	// ---- UNNEST booleans ----
	[Fact] public async Task Unnest_Booleans()
	{
		var v = await Scalar("SELECT COUNTIF(x) FROM UNNEST([TRUE, TRUE, FALSE, TRUE]) AS x");
		Assert.Equal("3", v);
	}
}
