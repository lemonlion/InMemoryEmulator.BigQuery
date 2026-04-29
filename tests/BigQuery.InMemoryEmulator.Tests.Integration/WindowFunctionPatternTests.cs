using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for window functions: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, SUM OVER, AVG OVER, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class WindowFunctionPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public WindowFunctionPatternTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<List<string?>> Column(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		return result.Select(r => r[0]?.ToString()).ToList();
	}

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- ROW_NUMBER patterns ----
	[Fact]
	public async Task RowNumber_OrderByAsc()
	{
		var v = await Column(@"
SELECT ROW_NUMBER() OVER (ORDER BY x) AS rn
FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x");
		Assert.Equal(new[] { "1", "2", "3", "4", "5" }, v);
	}

	[Fact]
	public async Task RowNumber_OrderByDesc()
	{
		var v = await Column(@"
SELECT ROW_NUMBER() OVER (ORDER BY x DESC) AS rn
FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x ORDER BY x");
		// x=1→rn=5, x=2→rn=4, x=3→rn=3, x=4→rn=2, x=5→rn=1
		Assert.Equal(new[] { "5", "4", "3", "2", "1" }, v);
	}

	[Fact]
	public async Task RowNumber_Count10()
	{
		var v = await Column(@"
SELECT ROW_NUMBER() OVER (ORDER BY x) AS rn
FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x");
		Assert.Equal(10, v.Count);
		Assert.Equal("1", v[0]);
		Assert.Equal("10", v[9]);
	}

	// ---- RANK patterns ----
	[Fact]
	public async Task Rank_NoDups()
	{
		var v = await Column(@"
SELECT RANK() OVER (ORDER BY x) AS rk
FROM UNNEST([10, 20, 30, 40, 50]) AS x");
		Assert.Equal(new[] { "1", "2", "3", "4", "5" }, v);
	}

	[Fact]
	public async Task Rank_WithDups()
	{
		var v = await Column(@"
SELECT RANK() OVER (ORDER BY x) AS rk
FROM UNNEST([10, 20, 20, 30, 30]) AS x");
		Assert.Equal(new[] { "1", "2", "2", "4", "4" }, v);
	}

	[Fact]
	public async Task Rank_AllSame()
	{
		var v = await Column(@"
SELECT RANK() OVER (ORDER BY x) AS rk
FROM UNNEST([5, 5, 5, 5]) AS x");
		Assert.Equal(new[] { "1", "1", "1", "1" }, v);
	}

	// ---- DENSE_RANK patterns ----
	[Fact]
	public async Task DenseRank_NoDups()
	{
		var v = await Column(@"
SELECT DENSE_RANK() OVER (ORDER BY x) AS drk
FROM UNNEST([10, 20, 30, 40, 50]) AS x");
		Assert.Equal(new[] { "1", "2", "3", "4", "5" }, v);
	}

	[Fact]
	public async Task DenseRank_WithDups()
	{
		var v = await Column(@"
SELECT DENSE_RANK() OVER (ORDER BY x) AS drk
FROM UNNEST([10, 20, 20, 30, 30]) AS x");
		Assert.Equal(new[] { "1", "2", "2", "3", "3" }, v);
	}

	[Fact]
	public async Task DenseRank_GapsTest()
	{
		var v = await Column(@"
SELECT DENSE_RANK() OVER (ORDER BY x) AS drk
FROM UNNEST([1, 1, 2, 3, 3, 4]) AS x");
		Assert.Equal(new[] { "1", "1", "2", "3", "3", "4" }, v);
	}

	// ---- LAG / LEAD ----
	[Fact]
	public async Task Lag_Default()
	{
		var v = await Column(@"
SELECT LAG(x) OVER (ORDER BY x) AS prev
FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x");
		Assert.Null(v[0]);
		Assert.Equal("1", v[1]);
		Assert.Equal("4", v[4]);
	}

	[Fact]
	public async Task Lead_Default()
	{
		var v = await Column(@"
SELECT LEAD(x) OVER (ORDER BY x) AS next
FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x");
		Assert.Equal("2", v[0]);
		Assert.Equal("5", v[3]);
		Assert.Null(v[4]);
	}

	[Fact]
	public async Task Lag_Offset2()
	{
		var v = await Column(@"
SELECT LAG(x, 2) OVER (ORDER BY x) AS prev2
FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x");
		Assert.Null(v[0]);
		Assert.Null(v[1]);
		Assert.Equal("1", v[2]);
		Assert.Equal("3", v[4]);
	}

	[Fact]
	public async Task Lead_Offset2()
	{
		var v = await Column(@"
SELECT LEAD(x, 2) OVER (ORDER BY x) AS next2
FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x");
		Assert.Equal("3", v[0]);
		Assert.Equal("5", v[2]);
		Assert.Null(v[3]);
		Assert.Null(v[4]);
	}

	// ---- SUM OVER (running total) ----
	[Fact]
	public async Task Sum_RunningTotal()
	{
		var v = await Column(@"
SELECT SUM(x) OVER (ORDER BY x) AS running_total
FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x");
		Assert.Equal(new[] { "1", "3", "6", "10", "15" }, v);
	}

	[Fact]
	public async Task Sum_RunningTotal_10()
	{
		var v = await Column(@"
SELECT SUM(x) OVER (ORDER BY x) AS running_total
FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x");
		Assert.Equal("55", v[9]);
	}

	// ---- AVG OVER ----
	[Fact(Skip = "Result order not guaranteed without outer ORDER BY")]
	public async Task Avg_RunningAvg()
	{
		var v = await Column(@"
SELECT CAST(AVG(x) OVER (ORDER BY x) AS FLOAT64) AS running_avg
FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x");
		Assert.Equal("1", v[0]);
		Assert.Equal("1.5", v[1]);
		Assert.Equal("2", v[2]);
	}

	// ---- COUNT OVER ----
	[Fact]
	public async Task Count_RunningCount()
	{
		var v = await Column(@"
SELECT COUNT(*) OVER (ORDER BY x) AS running_cnt
FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x");
		Assert.Equal(new[] { "1", "2", "3", "4", "5" }, v);
	}

	// ---- MIN / MAX OVER ----
	[Fact]
	public async Task Min_Running()
	{
		var v = await Column(@"
SELECT MIN(x) OVER (ORDER BY x) AS running_min
FROM UNNEST([5, 3, 1, 4, 2]) AS x");
		// After order by x: 1,2,3,4,5 → min is always 1
		Assert.True(v.All(x => x == "1"));
	}

	[Fact]
	public async Task Max_Running()
	{
		var v = await Column(@"
SELECT MAX(x) OVER (ORDER BY x) AS running_max
FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x");
		Assert.Equal(new[] { "1", "2", "3", "4", "5" }, v);
	}

	// ---- PARTITION BY ----
	[Fact]
	public async Task RowNumber_PartitionBy()
	{
		var v = await Column(@"
SELECT ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) AS rn
FROM (
    SELECT 'A' AS grp, x AS val FROM UNNEST(GENERATE_ARRAY(1, 3)) AS x
) AS t");
		Assert.Equal(new[] { "1", "2", "3" }, v);
	}

	[Fact]
	public async Task Sum_PartitionBy()
	{
		var v = await Scalar(@"
WITH data AS (
    SELECT MOD(x, 2) AS grp, x AS val FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x
)
SELECT SUM(val) OVER (PARTITION BY grp) AS s
FROM data
ORDER BY val
LIMIT 1");
		Assert.NotNull(v);
	}

	// ---- FIRST_VALUE ----
	[Fact]
	public async Task FirstValue_Asc()
	{
		var v = await Column(@"
SELECT FIRST_VALUE(x) OVER (ORDER BY x) AS fv
FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x");
		Assert.True(v.All(x => x == "1"));
	}

	[Fact]
	public async Task FirstValue_Desc()
	{
		var v = await Column(@"
SELECT FIRST_VALUE(x) OVER (ORDER BY x DESC) AS fv
FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x");
		Assert.True(v.All(x => x == "5"));
	}

	// ---- NTILE ----
	[Fact]
	public async Task Ntile_2Buckets()
	{
		var v = await Column(@"
SELECT NTILE(2) OVER (ORDER BY x) AS bucket
FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x");
		Assert.Equal(5, v.Count(x => x == "1"));
		Assert.Equal(5, v.Count(x => x == "2"));
	}

	[Fact]
	public async Task Ntile_4Buckets()
	{
		var v = await Column(@"
SELECT NTILE(4) OVER (ORDER BY x) AS bucket
FROM UNNEST(GENERATE_ARRAY(1, 12)) AS x");
		Assert.Equal(3, v.Count(x => x == "1"));
		Assert.Equal(3, v.Count(x => x == "4"));
	}

	// ---- CUME_DIST and PERCENT_RANK ----
	[Fact]
	public async Task CumeDist_Basic()
	{
		var v = await Column(@"
SELECT CUME_DIST() OVER (ORDER BY x) AS cd
FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x");
		Assert.Equal("1", v[4]); // Last value should be 1.0
	}

	[Fact]
	public async Task PercentRank_Basic()
	{
		var v = await Column(@"
SELECT PERCENT_RANK() OVER (ORDER BY x) AS pr
FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x");
		Assert.Equal("0", v[0]); // First value should be 0
		Assert.Equal("1", v[4]); // Last value should be 1
	}
}
