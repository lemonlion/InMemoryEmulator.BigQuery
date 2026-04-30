using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for window/analytic functions: ROW_NUMBER, RANK, DENSE_RANK, NTILE, LEAD, LAG, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class WindowFunctionTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public WindowFunctionTests(BigQuerySession session) => _session = session;
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

	// ---- ROW_NUMBER ----
	[Fact]
	public async Task RowNumber_Basic()
	{
		var rows = await Query(@"
			SELECT x, ROW_NUMBER() OVER (ORDER BY x) AS rn
			FROM UNNEST([30, 10, 20]) AS x ORDER BY rn");
		Assert.Equal("1", rows[0]["rn"]?.ToString());
		Assert.Equal("10", rows[0]["x"]?.ToString());
		Assert.Equal("2", rows[1]["rn"]?.ToString());
		Assert.Equal("3", rows[2]["rn"]?.ToString());
	}

	[Fact] public async Task RowNumber_Partitioned()
	{
		var rows = await Query(@"
			SELECT grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) AS rn
			FROM UNNEST([STRUCT('a' AS grp, 2 AS val), STRUCT('a', 1), STRUCT('b', 3), STRUCT('b', 1)]) AS t
			ORDER BY grp, rn");
		Assert.Equal("1", rows[0]["rn"]?.ToString());
		Assert.Equal("1", rows[0]["val"]?.ToString()); // a group, val=1
		Assert.Equal("2", rows[1]["rn"]?.ToString());
		Assert.Equal("2", rows[1]["val"]?.ToString()); // a group, val=2
	}

	[Fact]
	public async Task RowNumber_Five()
	{
		var rows = await Query("SELECT x, ROW_NUMBER() OVER (ORDER BY x) AS rn FROM UNNEST([5,3,1,4,2]) AS x ORDER BY rn");
		Assert.Equal(5, rows.Count);
		Assert.Equal("5", rows[4]["rn"]?.ToString());
	}

	// ---- RANK ----
	[Fact]
	public async Task Rank_Basic()
	{
		var rows = await Query(@"
			SELECT x, RANK() OVER (ORDER BY x) AS rnk
			FROM UNNEST([10, 20, 20, 30]) AS x ORDER BY rnk");
		Assert.Equal("1", rows[0]["rnk"]?.ToString());
		Assert.Equal("2", rows[1]["rnk"]?.ToString());
		Assert.Equal("2", rows[2]["rnk"]?.ToString()); // tie
		Assert.Equal("4", rows[3]["rnk"]?.ToString()); // skip 3
	}

	[Fact]
	public async Task Rank_AllTied()
	{
		var rows = await Query("SELECT x, RANK() OVER (ORDER BY x) AS rnk FROM UNNEST([5,5,5]) AS x");
		Assert.True(rows.All(r => r["rnk"]?.ToString() == "1"));
	}

	[Fact]
	public async Task Rank_Unique()
	{
		var rows = await Query("SELECT x, RANK() OVER (ORDER BY x) AS rnk FROM UNNEST([1,2,3]) AS x ORDER BY rnk");
		Assert.Equal("1", rows[0]["rnk"]?.ToString());
		Assert.Equal("2", rows[1]["rnk"]?.ToString());
		Assert.Equal("3", rows[2]["rnk"]?.ToString());
	}

	// ---- DENSE_RANK ----
	[Fact]
	public async Task DenseRank_Basic()
	{
		var rows = await Query(@"
			SELECT x, DENSE_RANK() OVER (ORDER BY x) AS drnk
			FROM UNNEST([10, 20, 20, 30]) AS x ORDER BY drnk");
		Assert.Equal("1", rows[0]["drnk"]?.ToString());
		Assert.Equal("2", rows[1]["drnk"]?.ToString());
		Assert.Equal("2", rows[2]["drnk"]?.ToString());
		Assert.Equal("3", rows[3]["drnk"]?.ToString()); // no gap
	}

	[Fact]
	public async Task DenseRank_AllTied()
	{
		var rows = await Query("SELECT x, DENSE_RANK() OVER (ORDER BY x) AS drnk FROM UNNEST([5,5,5]) AS x");
		Assert.True(rows.All(r => r["drnk"]?.ToString() == "1"));
	}

	// ---- NTILE ----
	[Fact]
	public async Task Ntile_Two()
	{
		var rows = await Query("SELECT x, NTILE(2) OVER (ORDER BY x) AS tile FROM UNNEST([1,2,3,4]) AS x ORDER BY x");
		Assert.Equal("1", rows[0]["tile"]?.ToString());
		Assert.Equal("1", rows[1]["tile"]?.ToString());
		Assert.Equal("2", rows[2]["tile"]?.ToString());
		Assert.Equal("2", rows[3]["tile"]?.ToString());
	}

	[Fact]
	public async Task Ntile_Three()
	{
		var rows = await Query("SELECT x, NTILE(3) OVER (ORDER BY x) AS tile FROM UNNEST([1,2,3,4,5,6]) AS x ORDER BY x");
		Assert.Equal("1", rows[0]["tile"]?.ToString());
		Assert.Equal("2", rows[2]["tile"]?.ToString());
		Assert.Equal("3", rows[4]["tile"]?.ToString());
	}

	// ---- LEAD / LAG ----
	[Fact]
	public async Task Lead_Basic()
	{
		var rows = await Query(@"
			SELECT x, LEAD(x) OVER (ORDER BY x) AS next_x
			FROM UNNEST([10, 20, 30]) AS x ORDER BY x");
		Assert.Equal("20", rows[0]["next_x"]?.ToString());
		Assert.Equal("30", rows[1]["next_x"]?.ToString());
		Assert.Null(rows[2]["next_x"]);
	}

	[Fact]
	public async Task Lead_Offset2()
	{
		var rows = await Query(@"
			SELECT x, LEAD(x, 2) OVER (ORDER BY x) AS ahead2
			FROM UNNEST([10, 20, 30, 40]) AS x ORDER BY x");
		Assert.Equal("30", rows[0]["ahead2"]?.ToString());
		Assert.Equal("40", rows[1]["ahead2"]?.ToString());
		Assert.Null(rows[2]["ahead2"]);
	}

	[Fact]
	public async Task Lead_Default()
	{
		var rows = await Query(@"
			SELECT x, LEAD(x, 1, -1) OVER (ORDER BY x) AS next_x
			FROM UNNEST([10, 20, 30]) AS x ORDER BY x");
		Assert.Equal("20", rows[0]["next_x"]?.ToString());
		Assert.Equal("-1", rows[2]["next_x"]?.ToString());
	}

	[Fact]
	public async Task Lag_Basic()
	{
		var rows = await Query(@"
			SELECT x, LAG(x) OVER (ORDER BY x) AS prev_x
			FROM UNNEST([10, 20, 30]) AS x ORDER BY x");
		Assert.Null(rows[0]["prev_x"]);
		Assert.Equal("10", rows[1]["prev_x"]?.ToString());
		Assert.Equal("20", rows[2]["prev_x"]?.ToString());
	}

	[Fact]
	public async Task Lag_Offset2()
	{
		var rows = await Query(@"
			SELECT x, LAG(x, 2) OVER (ORDER BY x) AS back2
			FROM UNNEST([10, 20, 30, 40]) AS x ORDER BY x");
		Assert.Null(rows[0]["back2"]);
		Assert.Null(rows[1]["back2"]);
		Assert.Equal("10", rows[2]["back2"]?.ToString());
		Assert.Equal("20", rows[3]["back2"]?.ToString());
	}

	[Fact]
	public async Task Lag_Default()
	{
		var rows = await Query(@"
			SELECT x, LAG(x, 1, -1) OVER (ORDER BY x) AS prev_x
			FROM UNNEST([10, 20, 30]) AS x ORDER BY x");
		Assert.Equal("-1", rows[0]["prev_x"]?.ToString());
		Assert.Equal("10", rows[1]["prev_x"]?.ToString());
	}

	// ---- FIRST_VALUE ----
	[Fact]
	public async Task FirstValue_Basic()
	{
		var rows = await Query(@"
			SELECT x, FIRST_VALUE(x) OVER (ORDER BY x) AS fv
			FROM UNNEST([30, 10, 20]) AS x ORDER BY x");
		Assert.True(rows.All(r => r["fv"]?.ToString() == "10"));
	}

	[Fact] public async Task FirstValue_Partitioned()
	{
		var rows = await Query(@"
			SELECT grp, val, FIRST_VALUE(val) OVER (PARTITION BY grp ORDER BY val) AS fv
			FROM UNNEST([STRUCT('a' AS grp, 3 AS val), STRUCT('a', 1), STRUCT('b', 5), STRUCT('b', 2)]) AS t
			ORDER BY grp, val");
		Assert.Equal("1", rows[0]["fv"]?.ToString()); // first in 'a'
		Assert.Equal("2", rows[2]["fv"]?.ToString()); // first in 'b'
	}

	// ---- Running aggregate ----
	[Fact]
	public async Task RunningSum()
	{
		var rows = await Query(@"
			SELECT x, SUM(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running
			FROM UNNEST([1, 2, 3, 4]) AS x ORDER BY x");
		Assert.Equal("1", rows[0]["running"]?.ToString());
		Assert.Equal("3", rows[1]["running"]?.ToString());
		Assert.Equal("6", rows[2]["running"]?.ToString());
		Assert.Equal("10", rows[3]["running"]?.ToString());
	}

	[Fact]
	public async Task RunningCount()
	{
		var rows = await Query(@"
			SELECT x, COUNT(*) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cnt
			FROM UNNEST([10, 20, 30]) AS x ORDER BY x");
		Assert.Equal("1", rows[0]["cnt"]?.ToString());
		Assert.Equal("2", rows[1]["cnt"]?.ToString());
		Assert.Equal("3", rows[2]["cnt"]?.ToString());
	}

	[Fact]
	public async Task RunningAvg()
	{
		var rows = await Query(@"
			SELECT x, AVG(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avg_val
			FROM UNNEST([10, 20, 30]) AS x ORDER BY x");
		Assert.Equal("10", rows[0]["avg_val"]?.ToString());
		Assert.Equal("15", rows[1]["avg_val"]?.ToString());
		Assert.Equal("20", rows[2]["avg_val"]?.ToString());
	}

	// ---- Total aggregate over window ----
	[Fact]
	public async Task WindowTotal_Sum()
	{
		var rows = await Query(@"
			SELECT x, SUM(x) OVER () AS total
			FROM UNNEST([1, 2, 3]) AS x ORDER BY x");
		Assert.True(rows.All(r => r["total"]?.ToString() == "6"));
	}

	[Fact]
	public async Task WindowTotal_Count()
	{
		var rows = await Query(@"
			SELECT x, COUNT(*) OVER () AS total
			FROM UNNEST([10, 20, 30]) AS x ORDER BY x");
		Assert.True(rows.All(r => r["total"]?.ToString() == "3"));
	}

	// ---- PERCENT_RANK / CUME_DIST ----
	[Fact]
	public async Task PercentRank_Basic()
	{
		var rows = await Query(@"
			SELECT x, PERCENT_RANK() OVER (ORDER BY x) AS pr
			FROM UNNEST([10, 20, 30]) AS x ORDER BY x");
		var pr0 = double.Parse(rows[0]["pr"]?.ToString() ?? "0");
		var pr2 = double.Parse(rows[2]["pr"]?.ToString() ?? "0");
		Assert.Equal(0.0, pr0);
		Assert.Equal(1.0, pr2);
	}

	[Fact]
	public async Task CumeDist_Basic()
	{
		var rows = await Query(@"
			SELECT x, CUME_DIST() OVER (ORDER BY x) AS cd
			FROM UNNEST([10, 20, 30]) AS x ORDER BY x");
		var cd2 = double.Parse(rows[2]["cd"]?.ToString() ?? "0");
		Assert.Equal(1.0, cd2);
	}

	// ---- Multiple windows ----
	[Fact]
	public async Task MultipleWindows()
	{
		var rows = await Query(@"
			SELECT x,
				ROW_NUMBER() OVER (ORDER BY x) AS rn,
				RANK() OVER (ORDER BY x) AS rnk,
				SUM(x) OVER () AS total
			FROM UNNEST([1,2,3]) AS x ORDER BY x");
		Assert.Equal("1", rows[0]["rn"]?.ToString());
		Assert.Equal("1", rows[0]["rnk"]?.ToString());
		Assert.Equal("6", rows[0]["total"]?.ToString());
	}
}
