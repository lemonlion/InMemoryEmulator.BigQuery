using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Deep integration tests for window functions: frame specs, navigation, QUALIFY, partitioning.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class WindowFrameAndQualifyTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public WindowFrameAndQualifyTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_wfq_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
		var client = await _fixture.GetClientAsync();

		var schema = new Google.Apis.Bigquery.v2.Data.TableSchema
		{
			Fields =
			[
				new() { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new() { Name = "dept", Type = "STRING", Mode = "NULLABLE" },
				new() { Name = "salary", Type = "INTEGER", Mode = "NULLABLE" },
				new() { Name = "hire_date", Type = "DATE", Mode = "NULLABLE" },
				new() { Name = "name", Type = "STRING", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "emp", schema);
		await client.InsertRowsAsync(_datasetId, "emp", new[]
		{
			new BigQueryInsertRow("e1") { ["id"] = 1, ["dept"] = "Eng", ["salary"] = 100, ["hire_date"] = "2020-01-01", ["name"] = "Alice" },
			new BigQueryInsertRow("e2") { ["id"] = 2, ["dept"] = "Eng", ["salary"] = 120, ["hire_date"] = "2020-06-15", ["name"] = "Bob" },
			new BigQueryInsertRow("e3") { ["id"] = 3, ["dept"] = "Eng", ["salary"] = 110, ["hire_date"] = "2021-03-01", ["name"] = "Charlie" },
			new BigQueryInsertRow("e4") { ["id"] = 4, ["dept"] = "Sales", ["salary"] = 90, ["hire_date"] = "2019-11-01", ["name"] = "Diana" },
			new BigQueryInsertRow("e5") { ["id"] = 5, ["dept"] = "Sales", ["salary"] = 95, ["hire_date"] = "2020-08-20", ["name"] = "Eve" },
			new BigQueryInsertRow("e6") { ["id"] = 6, ["dept"] = "Sales", ["salary"] = 110, ["hire_date"] = "2021-01-10", ["name"] = "Frank" },
			new BigQueryInsertRow("e7") { ["id"] = 7, ["dept"] = "HR", ["salary"] = 80, ["hire_date"] = "2018-05-01", ["name"] = "Grace" },
			new BigQueryInsertRow("e8") { ["id"] = 8, ["dept"] = "HR", ["salary"] = 85, ["hire_date"] = "2022-01-15", ["name"] = "Hank" },
		});
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

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

	// ============================================================
	// ROW_NUMBER, RANK, DENSE_RANK
	// ============================================================

	[Fact]
	public async Task RowNumber_PartitionByDept()
	{
		var rows = await Query($@"SELECT name, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
			FROM `{_datasetId}.emp` WHERE dept = 'Eng' ORDER BY rn");
		Assert.Equal("1", rows[0]["rn"].ToString());
		Assert.Equal("Bob", (string)rows[0]["name"]);
	}

	[Fact]
	public async Task Rank_WithTies()
	{
		var rows = await Query($@"SELECT name, RANK() OVER (ORDER BY salary) AS rnk
			FROM `{_datasetId}.emp` ORDER BY rnk, name");
		var rank6s = rows.Where(r => r["rnk"].ToString() == "6").ToList();
		Assert.Equal(2, rank6s.Count);
	}

	[Fact]
	public async Task DenseRank_NoGap()
	{
		var rows = await Query($@"SELECT name, DENSE_RANK() OVER (ORDER BY salary) AS drnk
			FROM `{_datasetId}.emp` ORDER BY drnk DESC LIMIT 1");
		Assert.Equal("7", rows[0]["drnk"].ToString());
	}

	[Fact]
	public async Task Ntile_2()
	{
		var rows = await Query($@"SELECT name, NTILE(2) OVER (ORDER BY salary) AS tile
			FROM `{_datasetId}.emp` ORDER BY salary");
		var tile1 = rows.Count(r => r["tile"].ToString() == "1");
		var tile2 = rows.Count(r => r["tile"].ToString() == "2");
		Assert.Equal(4, tile1);
		Assert.Equal(4, tile2);
	}

	[Fact]
	public async Task Ntile_MoreThanRows()
	{
		var rows = await Query($@"SELECT name, NTILE(20) OVER (ORDER BY salary) AS tile
			FROM `{_datasetId}.emp` ORDER BY salary");
		Assert.Equal("1", rows[0]["tile"].ToString());
		Assert.Equal("8", rows[7]["tile"].ToString());
	}

	[Fact]
	public async Task PercentRank_Basic()
	{
		var rows = await Query($@"SELECT name, salary, PERCENT_RANK() OVER (ORDER BY salary) AS prank
			FROM `{_datasetId}.emp` ORDER BY salary");
		Assert.Equal("0", rows[0]["prank"].ToString());
	}

	[Fact]
	public async Task CumeDist_Basic()
	{
		var rows = await Query($@"SELECT name, salary, CUME_DIST() OVER (ORDER BY salary) AS cd
			FROM `{_datasetId}.emp` ORDER BY salary");
		Assert.Equal("1", rows[^1]["cd"].ToString());
	}

	// ============================================================
	// LAG / LEAD
	// ============================================================

	[Fact]
	public async Task Lag_Default()
	{
		var rows = await Query($@"SELECT name, salary, LAG(salary) OVER (ORDER BY salary) AS prev_sal
			FROM `{_datasetId}.emp` ORDER BY salary");
		Assert.Null(rows[0]["prev_sal"]);
		Assert.Equal("80", rows[1]["prev_sal"].ToString());
	}

	[Fact]
	public async Task Lag_WithOffset()
	{
		var rows = await Query($@"SELECT name, LAG(name, 2) OVER (ORDER BY salary) AS prev2
			FROM `{_datasetId}.emp` ORDER BY salary");
		Assert.Null(rows[0]["prev2"]);
		Assert.Null(rows[1]["prev2"]);
		Assert.NotNull(rows[2]["prev2"]);
	}

	[Fact]
	public async Task Lag_WithDefaultValue()
	{
		var rows = await Query($@"SELECT name, LAG(salary, 1, -1) OVER (ORDER BY salary) AS prev_sal
			FROM `{_datasetId}.emp` ORDER BY salary");
		Assert.Equal("-1", rows[0]["prev_sal"].ToString());
	}

	[Fact]
	public async Task Lead_Basic()
	{
		var rows = await Query($@"SELECT name, salary, LEAD(salary) OVER (ORDER BY salary) AS next_sal
			FROM `{_datasetId}.emp` ORDER BY salary");
		Assert.Null(rows[^1]["next_sal"]);
		Assert.NotNull(rows[0]["next_sal"]);
	}

	[Fact]
	public async Task Lead_WithDefault()
	{
		var rows = await Query($@"SELECT name, LEAD(salary, 1, 0) OVER (ORDER BY salary) AS next_sal
			FROM `{_datasetId}.emp` ORDER BY salary");
		Assert.Equal("0", rows[^1]["next_sal"].ToString());
	}

	// ============================================================
	// FIRST_VALUE / LAST_VALUE / NTH_VALUE
	// ============================================================

	[Fact]
	public async Task FirstValue_InPartition()
	{
		var rows = await Query($@"SELECT name, dept, FIRST_VALUE(name) OVER (PARTITION BY dept ORDER BY salary) AS first_name
			FROM `{_datasetId}.emp` WHERE dept = 'Eng' ORDER BY salary");
		Assert.Equal("Alice", (string)rows[0]["first_name"]);
		Assert.Equal("Alice", (string)rows[2]["first_name"]);
	}

	[Fact]
	public async Task LastValue_WithFullFrame()
	{
		var rows = await Query($@"SELECT name, dept, 
			LAST_VALUE(name) OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_name
			FROM `{_datasetId}.emp` WHERE dept = 'Eng' ORDER BY salary");
		Assert.Equal("Bob", (string)rows[0]["last_name"]);
	}

	[Fact]
	public async Task NthValue_Second()
	{
		var rows = await Query($@"SELECT name, NTH_VALUE(name, 2) OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_name
			FROM `{_datasetId}.emp` WHERE dept = 'Eng' ORDER BY salary");
		Assert.Equal("Charlie", (string)rows[0]["second_name"]);
	}

	// ============================================================
	// Running Aggregates
	// ============================================================

	[Fact]
	public async Task RunningSum()
	{
		var rows = await Query($@"SELECT name, salary, 
			SUM(salary) OVER (ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum
			FROM `{_datasetId}.emp` ORDER BY salary");
		Assert.Equal("80", rows[0]["running_sum"].ToString());
		Assert.Equal("165", rows[1]["running_sum"].ToString());
	}

	[Fact]
	public async Task RunningCount()
	{
		var rows = await Query($@"SELECT name, COUNT(*) OVER (ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rc
			FROM `{_datasetId}.emp` ORDER BY salary");
		Assert.Equal("1", rows[0]["rc"].ToString());
		Assert.Equal("8", rows[^1]["rc"].ToString());
	}

	[Fact]
	public async Task PartitionAvg()
	{
		var rows = await Query($@"SELECT name, dept, AVG(salary) OVER (PARTITION BY dept) AS dept_avg
			FROM `{_datasetId}.emp` WHERE dept = 'HR' ORDER BY name");
		Assert.Equal(82.5, Convert.ToDouble(rows[0]["dept_avg"]), 1);
	}

	[Fact]
	public async Task RunningMin()
	{
		var rows = await Query($@"SELECT name, salary, MIN(salary) OVER (ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rmin
			FROM `{_datasetId}.emp` ORDER BY salary");
		Assert.Equal("80", rows[^1]["rmin"].ToString());
	}

	[Fact]
	public async Task RunningMax()
	{
		var rows = await Query($@"SELECT name, salary, MAX(salary) OVER (ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rmax
			FROM `{_datasetId}.emp` ORDER BY salary");
		Assert.Equal("120", rows[^1]["rmax"].ToString());
	}

	// ============================================================
	// Frame Specifications
	// ============================================================

	[Fact]
	public async Task Frame_1Prec_1Foll()
	{
		var rows = await Query($@"SELECT name, salary,
			SUM(salary) OVER (ORDER BY salary ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS ws
			FROM `{_datasetId}.emp` ORDER BY salary");
		Assert.Equal("165", rows[0]["ws"].ToString()); // 80+85
		Assert.Equal("255", rows[1]["ws"].ToString()); // 80+85+90
	}

	[Fact]
	public async Task Frame_CurrentRowOnly()
	{
		var rows = await Query($@"SELECT name, salary,
			SUM(salary) OVER (ORDER BY salary ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS self_sum
			FROM `{_datasetId}.emp` ORDER BY salary");
		Assert.Equal(rows[0]["salary"].ToString(), rows[0]["self_sum"].ToString());
	}

	[Fact]
	public async Task Frame_UnboundedFollowing()
	{
		var rows = await Query($@"SELECT name, salary,
			SUM(salary) OVER (ORDER BY salary ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS tail_sum
			FROM `{_datasetId}.emp` ORDER BY salary");
		Assert.Equal(rows[^1]["salary"].ToString(), rows[^1]["tail_sum"].ToString());
	}

	[Fact]
	public async Task Frame_3Preceding()
	{
		var rows = await Query($@"SELECT name, salary,
			COUNT(*) OVER (ORDER BY salary ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS cnt
			FROM `{_datasetId}.emp` ORDER BY salary");
		Assert.Equal("1", rows[0]["cnt"].ToString());
		Assert.Equal("4", rows[4]["cnt"].ToString());
	}

	// ============================================================
	// QUALIFY clause
	// ============================================================

	[Fact]
	public async Task Qualify_RowNumber1_PerDept()
	{
		var rows = await Query($@"SELECT name, dept, salary
			FROM `{_datasetId}.emp`
			QUALIFY ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) = 1
			ORDER BY dept");
		Assert.Equal(3, rows.Count);
	}

	[Fact]
	public async Task Qualify_RankLe2()
	{
		var rows = await Query($@"SELECT name, dept, salary
			FROM `{_datasetId}.emp`
			QUALIFY RANK() OVER (PARTITION BY dept ORDER BY salary DESC) <= 2
			ORDER BY dept, salary DESC");
		Assert.Equal(6, rows.Count);
	}

	[Fact]
	public async Task Qualify_WithWhere()
	{
		var rows = await Query($@"SELECT name, dept, salary
			FROM `{_datasetId}.emp`
			WHERE salary > 85
			QUALIFY ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) = 1
			ORDER BY dept");
		Assert.True(rows.Count >= 2);
	}

	// ============================================================
	// Multiple window functions in same SELECT
	// ============================================================

	[Fact]
	public async Task MultipleWindowFunctions()
	{
		var rows = await Query($@"SELECT name, salary,
			ROW_NUMBER() OVER (ORDER BY salary) AS rn,
			RANK() OVER (ORDER BY salary) AS rnk,
			SUM(salary) OVER (ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rsum
			FROM `{_datasetId}.emp` ORDER BY salary");
		Assert.Equal("1", rows[0]["rn"].ToString());
		Assert.Equal("1", rows[0]["rnk"].ToString());
		Assert.Equal("80", rows[0]["rsum"].ToString());
	}

	// ============================================================
	// COUNT OVER () = total per row
	// ============================================================

	[Fact]
	public async Task CountOver_NoOrderBy()
	{
		var rows = await Query($@"SELECT name, COUNT(*) OVER () AS total FROM `{_datasetId}.emp`");
		foreach (var row in rows)
			Assert.Equal("8", row["total"].ToString());
	}

	// ============================================================
	// Window in CTE
	// ============================================================

	[Fact]
	public async Task Window_InCte()
	{
		var rows = await Query($@"
			WITH ranked AS (
				SELECT name, dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
				FROM `{_datasetId}.emp`
			)
			SELECT name, dept FROM ranked WHERE rn = 1 ORDER BY dept");
		Assert.Equal(3, rows.Count);
	}

	// ============================================================
	// STRING_AGG OVER
	// ============================================================

	[Fact]
	public async Task StringAgg_OverPartition()
	{
		var rows = await Query($@"SELECT dept, STRING_AGG(name, ',') OVER (PARTITION BY dept ORDER BY name) AS names
			FROM `{_datasetId}.emp` WHERE dept = 'HR' ORDER BY name");
		Assert.Equal("Grace", (string)rows[0]["names"]);
		Assert.Equal("Grace,Hank", (string)rows[1]["names"]);
	}

	// ============================================================
	// Window in subquery
	// ============================================================

	[Fact]
	public async Task Window_InSubquery()
	{
		var rows = await Query($@"SELECT * FROM (
			SELECT name, dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
			FROM `{_datasetId}.emp`
		) WHERE rn <= 2 ORDER BY dept, salary DESC");
		Assert.Equal(6, rows.Count);
	}

	// ============================================================
	// SUM partition no order = total per partition
	// ============================================================

	[Fact]
	public async Task SumOver_PartitionNoOrder()
	{
		var rows = await Query($@"SELECT name, dept, salary, SUM(salary) OVER (PARTITION BY dept) AS dept_total
			FROM `{_datasetId}.emp` WHERE dept = 'Eng'");
		foreach (var row in rows)
			Assert.Equal("330", row["dept_total"].ToString());
	}

	// ============================================================
	// Window ORDER BY expression
	// ============================================================

	[Fact]
	public async Task Window_OrderByExpression()
	{
		var rows = await Query($@"SELECT name, ROW_NUMBER() OVER (ORDER BY salary * -1) AS rn
			FROM `{_datasetId}.emp` ORDER BY rn");
		Assert.Equal("Bob", (string)rows[0]["name"]);
	}

	// ============================================================
	// Window ORDER BY multiple columns
	// ============================================================

	[Fact]
	public async Task Window_OrderByMultiple()
	{
		var rows = await Query($@"SELECT name, ROW_NUMBER() OVER (ORDER BY dept, salary DESC) AS rn
			FROM `{_datasetId}.emp` ORDER BY rn");
		Assert.Equal(8, rows.Count);
	}

	// ============================================================
	// Window over empty result
	// ============================================================

	[Fact]
	public async Task Window_EmptyResult()
	{
		var rows = await Query($@"SELECT name, ROW_NUMBER() OVER (ORDER BY salary) AS rn
			FROM `{_datasetId}.emp` WHERE 1 = 0");
		Assert.Empty(rows);
	}

	// ============================================================
	// PERCENTILE_CONT / PERCENTILE_DISC
	// ============================================================

	[Fact]
	public async Task PercentileCont_Median()
	{
		var result = await Scalar($@"SELECT PERCENTILE_CONT(salary, 0.5) OVER () AS median
			FROM `{_datasetId}.emp` LIMIT 1");
		Assert.NotNull(result);
	}

	[Fact]
	public async Task PercentileDisc_Median()
	{
		var result = await Scalar($@"SELECT PERCENTILE_DISC(salary, 0.5) OVER () AS median
			FROM `{_datasetId}.emp` LIMIT 1");
		Assert.NotNull(result);
	}
}
