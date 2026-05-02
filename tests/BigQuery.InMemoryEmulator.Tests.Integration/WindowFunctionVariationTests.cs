using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for window function variations, QUALIFY clause,
/// and advanced analytical patterns.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class WindowFunctionVariationTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public WindowFunctionVariationTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await _fixture.GetClientAsync();
		return (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
	}

	// ============================================================
	// ROW_NUMBER, RANK, DENSE_RANK
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions
	// ============================================================

	[Fact]
	public async Task RowNumber_Basic()
	{
		var rows = await Query("SELECT x, ROW_NUMBER() OVER (ORDER BY x) AS rn FROM UNNEST([30, 10, 20]) AS x ORDER BY rn");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]["rn"]?.ToString());
		Assert.Equal("10", rows[0]["x"]?.ToString());
	}

	[Fact]
	public async Task Rank_WithTies()
	{
		var rows = await Query("SELECT x, RANK() OVER (ORDER BY x) AS rnk FROM UNNEST([10, 20, 10, 30]) AS x ORDER BY x, rnk");
		Assert.Equal(4, rows.Count);
		Assert.Equal("1", rows[0]["rnk"]?.ToString());
		Assert.Equal("1", rows[1]["rnk"]?.ToString());
	}

	[Fact]
	public async Task DenseRank_WithTies()
	{
		var rows = await Query("SELECT x, DENSE_RANK() OVER (ORDER BY x) AS dr FROM UNNEST([10, 20, 10, 30]) AS x ORDER BY x, dr");
		Assert.Equal(4, rows.Count);
		Assert.Equal("1", rows[0]["dr"]?.ToString());
		Assert.Equal("2", rows[2]["dr"]?.ToString());
	}

	// ============================================================
	// LAG / LEAD
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions
	// ============================================================

	[Fact]
	public async Task Lag_Basic()
	{
		var rows = await Query("SELECT x, LAG(x) OVER (ORDER BY x) AS prev FROM UNNEST([10, 20, 30]) AS x ORDER BY x");
		Assert.Null(rows[0]["prev"]);
		Assert.Equal("10", rows[1]["prev"]?.ToString());
	}

	[Fact]
	public async Task Lead_Basic()
	{
		var rows = await Query("SELECT x, LEAD(x) OVER (ORDER BY x) AS nxt FROM UNNEST([10, 20, 30]) AS x ORDER BY x");
		Assert.Equal("20", rows[0]["nxt"]?.ToString());
		Assert.Null(rows[2]["nxt"]);
	}

	[Fact]
	public async Task Lag_WithDefault()
	{
		var rows = await Query("SELECT x, LAG(x, 1, -1) OVER (ORDER BY x) AS prev FROM UNNEST([10, 20, 30]) AS x ORDER BY x");
		Assert.Equal("-1", rows[0]["prev"]?.ToString());
	}

	[Fact]
	public async Task Lead_WithOffset()
	{
		var rows = await Query("SELECT x, LEAD(x, 2) OVER (ORDER BY x) AS nxt2 FROM UNNEST([10, 20, 30, 40]) AS x ORDER BY x");
		Assert.Equal("30", rows[0]["nxt2"]?.ToString());
		Assert.Null(rows[2]["nxt2"]);
	}

	// ============================================================
	// FIRST_VALUE / LAST_VALUE / NTH_VALUE
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions
	// ============================================================

	[Fact]
	public async Task FirstValue_Basic()
	{
		var v = await Scalar("SELECT FIRST_VALUE(x) OVER (ORDER BY x) FROM UNNEST([30, 10, 20]) AS x ORDER BY x LIMIT 1");
		Assert.Equal("10", v);
	}

	[Fact]
	public async Task LastValue_UnboundedFrame()
	{
		var v = await Scalar("SELECT LAST_VALUE(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM UNNEST([10, 20, 30]) AS x ORDER BY x LIMIT 1");
		Assert.Equal("30", v);
	}

	[Fact]
	public async Task NthValue_Second()
	{
		var v = await Scalar("SELECT NTH_VALUE(x, 2) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM UNNEST([10, 20, 30]) AS x ORDER BY x LIMIT 1");
		Assert.Equal("20", v);
	}

	// ============================================================
	// SUM / AVG / COUNT / MIN / MAX as window functions
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate-analytic-function-concepts
	// ============================================================

	[Fact]
	public async Task SumWindow_PartitionBy()
	{
		var rows = await Query(@"
			SELECT cat, val, SUM(val) OVER (PARTITION BY cat) AS cat_total
			FROM (SELECT 'A' AS cat, 10 AS val UNION ALL SELECT 'A', 20 UNION ALL SELECT 'B', 30)
			ORDER BY cat, val");
		Assert.Equal("30", rows[0]["cat_total"]?.ToString()); // A: 10+20
		Assert.Equal("30", rows[2]["cat_total"]?.ToString()); // B: 30
	}

	[Fact]
	public async Task CountWindow_All()
	{
		var v = await Scalar("SELECT COUNT(*) OVER () FROM UNNEST([1, 2, 3]) AS x LIMIT 1");
		Assert.Equal("3", v);
	}

	[Fact]
	public async Task MinMaxWindow()
	{
		var rows = await Query("SELECT x, MIN(x) OVER () AS mn, MAX(x) OVER () AS mx FROM UNNEST([10, 20, 30]) AS x ORDER BY x");
		Assert.Equal("10", rows[0]["mn"]?.ToString());
		Assert.Equal("30", rows[0]["mx"]?.ToString());
	}

	[Fact]
	public async Task AvgWindow()
	{
		var v = await Scalar("SELECT AVG(x) OVER () FROM UNNEST([10, 20, 30]) AS x LIMIT 1");
		Assert.Equal("20", v);
	}

	// ============================================================
	// Running totals / cumulative patterns
	// ============================================================

	[Fact]
	public async Task RunningTotal()
	{
		var rows = await Query("SELECT x, SUM(x) OVER (ORDER BY x) AS running FROM UNNEST([10, 20, 30]) AS x ORDER BY x");
		Assert.Equal("10", rows[0]["running"]?.ToString());
		Assert.Equal("30", rows[1]["running"]?.ToString());
		Assert.Equal("60", rows[2]["running"]?.ToString());
	}

	[Fact]
	public async Task MovingAvg_3Rows()
	{
		var rows = await Query(@"
			SELECT x, AVG(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS mavg
			FROM UNNEST([10, 20, 30, 40, 50]) AS x ORDER BY x");
		Assert.Equal(5, rows.Count);
		Assert.Equal("15", rows[0]["mavg"]?.ToString()); // avg(10,20)
		Assert.Equal("20", rows[1]["mavg"]?.ToString()); // avg(10,20,30)
	}

	// ============================================================
	// NTILE
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions#ntile
	// ============================================================

	[Fact]
	public async Task Ntile_Basic()
	{
		var rows = await Query("SELECT x, NTILE(2) OVER (ORDER BY x) AS bucket FROM UNNEST([10, 20, 30, 40]) AS x ORDER BY x");
		Assert.Equal("1", rows[0]["bucket"]?.ToString());
		Assert.Equal("2", rows[2]["bucket"]?.ToString());
	}

	// ============================================================
	// CUME_DIST / PERCENT_RANK
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions
	// ============================================================

	[Fact]
	public async Task CumeDist_Basic()
	{
		var rows = await Query("SELECT x, CUME_DIST() OVER (ORDER BY x) AS cd FROM UNNEST([10, 20, 30]) AS x ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.NotNull(rows[0]["cd"]);
	}

	[Fact]
	public async Task PercentRank_Basic()
	{
		var rows = await Query("SELECT x, PERCENT_RANK() OVER (ORDER BY x) AS pr FROM UNNEST([10, 20, 30]) AS x ORDER BY x");
		Assert.Equal("0", rows[0]["pr"]?.ToString());
	}
}
