using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for window functions: ROW_NUMBER, RANK, DENSE_RANK, LEAD, LAG, FIRST_VALUE,
/// NTILE, CUME_DIST, PERCENT_RANK, SUM OVER, AVG OVER, COUNT OVER, MIN OVER, MAX OVER.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class WindowFunctionDeepTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public WindowFunctionDeepTests(BigQuerySession session) => _session = session;
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

	// ---- ROW_NUMBER ----
	[Fact]
	public async Task RowNumber_Basic()
	{
		var rows = await Column("SELECT ROW_NUMBER() OVER (ORDER BY x) FROM UNNEST([30,10,20]) AS x ORDER BY 1");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]);
		Assert.Equal("2", rows[1]);
		Assert.Equal("3", rows[2]);
	}

	[Fact]
	public async Task RowNumber_Desc()
	{
		var rows = await Column("SELECT ROW_NUMBER() OVER (ORDER BY x DESC) FROM UNNEST([30,10,20]) AS x ORDER BY 1");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]);
		Assert.Equal("2", rows[1]);
		Assert.Equal("3", rows[2]);
	}

	[Fact]
	public async Task RowNumber_Five()
	{
		var rows = await Column("SELECT ROW_NUMBER() OVER (ORDER BY x) FROM UNNEST([50,40,30,20,10]) AS x ORDER BY 1");
		Assert.Equal(5, rows.Count);
		Assert.Equal("1", rows[0]);
		Assert.Equal("5", rows[4]);
	}

	// ---- RANK ----
	[Fact]
	public async Task Rank_Basic()
	{
		var rows = await Column("SELECT RANK() OVER (ORDER BY x) FROM UNNEST([10,20,30]) AS x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]);
		Assert.Equal("2", rows[1]);
		Assert.Equal("3", rows[2]);
	}

	[Fact]
	public async Task Rank_WithTies()
	{
		var rows = await Column("SELECT RANK() OVER (ORDER BY x) FROM UNNEST([10,10,20]) AS x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]);
		Assert.Equal("1", rows[1]);
		Assert.Equal("3", rows[2]);
	}

	// ---- DENSE_RANK ----
	[Fact]
	public async Task DenseRank_Basic()
	{
		var rows = await Column("SELECT DENSE_RANK() OVER (ORDER BY x) FROM UNNEST([10,20,30]) AS x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]);
		Assert.Equal("2", rows[1]);
		Assert.Equal("3", rows[2]);
	}

	[Fact]
	public async Task DenseRank_WithTies()
	{
		var rows = await Column("SELECT DENSE_RANK() OVER (ORDER BY x) FROM UNNEST([10,10,20]) AS x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]);
		Assert.Equal("1", rows[1]);
		Assert.Equal("2", rows[2]);
	}

	// ---- LEAD / LAG ----
	[Fact]
	public async Task Lead_Basic()
	{
		var rows = await Column("SELECT LEAD(x) OVER (ORDER BY x) FROM UNNEST([10,20,30]) AS x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("20", rows[0]);
		Assert.Equal("30", rows[1]);
		Assert.Null(rows[2]);
	}

	[Fact]
	public async Task Lead_WithOffset()
	{
		var rows = await Column("SELECT LEAD(x, 2) OVER (ORDER BY x) FROM UNNEST([10,20,30,40]) AS x");
		Assert.Equal(4, rows.Count);
		Assert.Equal("30", rows[0]);
		Assert.Equal("40", rows[1]);
		Assert.Null(rows[2]);
		Assert.Null(rows[3]);
	}

	[Fact]
	public async Task Lead_WithDefault()
	{
		var rows = await Column("SELECT LEAD(x, 1, -1) OVER (ORDER BY x) FROM UNNEST([10,20,30]) AS x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("20", rows[0]);
		Assert.Equal("30", rows[1]);
		Assert.Equal("-1", rows[2]);
	}

	[Fact]
	public async Task Lag_Basic()
	{
		var rows = await Column("SELECT LAG(x) OVER (ORDER BY x) FROM UNNEST([10,20,30]) AS x");
		Assert.Equal(3, rows.Count);
		Assert.Null(rows[0]);
		Assert.Equal("10", rows[1]);
		Assert.Equal("20", rows[2]);
	}

	[Fact]
	public async Task Lag_WithOffset()
	{
		var rows = await Column("SELECT LAG(x, 2) OVER (ORDER BY x) FROM UNNEST([10,20,30,40]) AS x");
		Assert.Equal(4, rows.Count);
		Assert.Null(rows[0]);
		Assert.Null(rows[1]);
		Assert.Equal("10", rows[2]);
		Assert.Equal("20", rows[3]);
	}

	[Fact]
	public async Task Lag_WithDefault()
	{
		var rows = await Column("SELECT LAG(x, 1, -1) OVER (ORDER BY x) FROM UNNEST([10,20,30]) AS x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("-1", rows[0]);
		Assert.Equal("10", rows[1]);
		Assert.Equal("20", rows[2]);
	}

	// ---- FIRST_VALUE ----
	[Fact]
	public async Task FirstValue_Basic()
	{
		var rows = await Column("SELECT FIRST_VALUE(x) OVER (ORDER BY x) FROM UNNEST([30,10,20]) AS x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("10", rows[0]);
		Assert.Equal("10", rows[1]);
		Assert.Equal("10", rows[2]);
	}

	// ---- NTILE ----
	[Fact]
	public async Task Ntile_Two()
	{
		var rows = await Column("SELECT NTILE(2) OVER (ORDER BY x) FROM UNNEST([10,20,30,40]) AS x");
		Assert.Equal(4, rows.Count);
		Assert.Equal("1", rows[0]);
		Assert.Equal("1", rows[1]);
		Assert.Equal("2", rows[2]);
		Assert.Equal("2", rows[3]);
	}

	[Fact]
	public async Task Ntile_Three()
	{
		var rows = await Column("SELECT NTILE(3) OVER (ORDER BY x) FROM UNNEST([10,20,30,40,50,60]) AS x");
		Assert.Equal(6, rows.Count);
		Assert.Equal("1", rows[0]);
		Assert.Equal("1", rows[1]);
		Assert.Equal("2", rows[2]);
		Assert.Equal("2", rows[3]);
		Assert.Equal("3", rows[4]);
		Assert.Equal("3", rows[5]);
	}

	// ---- Window aggregate functions ----
	[Fact]
	public async Task SumOver()
	{
		var rows = await Column("SELECT SUM(x) OVER () FROM UNNEST([10,20,30]) AS x");
		Assert.Equal(3, rows.Count);
		Assert.All(rows, r => Assert.Equal("60", r));
	}

	[Fact]
	public async Task CountOver()
	{
		var rows = await Column("SELECT COUNT(*) OVER () FROM UNNEST([10,20,30]) AS x");
		Assert.Equal(3, rows.Count);
		Assert.All(rows, r => Assert.Equal("3", r));
	}

	[Fact]
	public async Task MinOver()
	{
		var rows = await Column("SELECT MIN(x) OVER () FROM UNNEST([10,20,30]) AS x");
		Assert.Equal(3, rows.Count);
		Assert.All(rows, r => Assert.Equal("10", r));
	}

	[Fact]
	public async Task MaxOver()
	{
		var rows = await Column("SELECT MAX(x) OVER () FROM UNNEST([10,20,30]) AS x");
		Assert.Equal(3, rows.Count);
		Assert.All(rows, r => Assert.Equal("30", r));
	}

	// ---- Running / cumulative sum ----
	[Fact]
	public async Task RunningSum()
	{
		var rows = await Column("SELECT SUM(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM UNNEST([10,20,30]) AS x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("10", rows[0]);
		Assert.Equal("30", rows[1]);
		Assert.Equal("60", rows[2]);
	}

	[Fact]
	public async Task RunningCount()
	{
		var rows = await Column("SELECT COUNT(*) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM UNNEST([10,20,30]) AS x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]);
		Assert.Equal("2", rows[1]);
		Assert.Equal("3", rows[2]);
	}

	// ---- PARTITION BY ----
	[Fact]
	public async Task PartitionBy_RowNumber()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH data AS (
  SELECT 'a' AS grp, 10 AS val UNION ALL
  SELECT 'a', 20 UNION ALL
  SELECT 'b', 30 UNION ALL
  SELECT 'b', 40
)
SELECT grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) AS rn FROM data ORDER BY grp, val",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(4, rows.Count);
		Assert.Equal("1", rows[0][2]?.ToString()); // a, 10 → rn=1
		Assert.Equal("2", rows[1][2]?.ToString()); // a, 20 → rn=2
		Assert.Equal("1", rows[2][2]?.ToString()); // b, 30 → rn=1
		Assert.Equal("2", rows[3][2]?.ToString()); // b, 40 → rn=2
	}

	[Fact]
	public async Task PartitionBy_Sum()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH data AS (
  SELECT 'a' AS grp, 10 AS val UNION ALL
  SELECT 'a', 20 UNION ALL
  SELECT 'b', 30 UNION ALL
  SELECT 'b', 40
)
SELECT grp, val, SUM(val) OVER (PARTITION BY grp) AS grp_sum FROM data ORDER BY grp, val",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(4, rows.Count);
		Assert.Equal("30", rows[0][2]?.ToString()); // a group sum
		Assert.Equal("30", rows[1][2]?.ToString());
		Assert.Equal("70", rows[2][2]?.ToString()); // b group sum
		Assert.Equal("70", rows[3][2]?.ToString());
	}

	[Fact]
	public async Task PartitionBy_Rank()
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
SELECT grp, val, RANK() OVER (PARTITION BY grp ORDER BY val DESC) AS rnk FROM data ORDER BY grp, val DESC",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(5, rows.Count);
		Assert.Equal("1", rows[0][2]?.ToString()); // a, 30 → rank=1
		Assert.Equal("2", rows[1][2]?.ToString()); // a, 20 → rank=2
		Assert.Equal("3", rows[2][2]?.ToString()); // a, 10 → rank=3
		Assert.Equal("1", rows[3][2]?.ToString()); // b, 15 → rank=1
		Assert.Equal("2", rows[4][2]?.ToString()); // b, 5 → rank=2
	}

	// ---- Multiple window functions in one query ----
	[Fact]
	public async Task MultiWindow()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
SELECT x, ROW_NUMBER() OVER (ORDER BY x) AS rn, RANK() OVER (ORDER BY x) AS rnk, DENSE_RANK() OVER (ORDER BY x) AS dr
FROM UNNEST([10,20,20,30]) AS x",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(4, rows.Count);
		// First row: x=10, rn=1, rank=1, dense_rank=1
		Assert.Equal("10", rows[0][0]?.ToString());
		Assert.Equal("1", rows[0][1]?.ToString());
		Assert.Equal("1", rows[0][2]?.ToString());
		Assert.Equal("1", rows[0][3]?.ToString());
		// Tied rows: x=20, rn=2/3, rank=2/2, dense_rank=2/2
		Assert.Equal("2", rows[1][2]?.ToString());
		Assert.Equal("2", rows[2][2]?.ToString());
		Assert.Equal("2", rows[1][3]?.ToString());
		Assert.Equal("2", rows[2][3]?.ToString());
		// Last row: x=30, rank=4, dense_rank=3
		Assert.Equal("4", rows[3][2]?.ToString());
		Assert.Equal("3", rows[3][3]?.ToString());
	}
}
