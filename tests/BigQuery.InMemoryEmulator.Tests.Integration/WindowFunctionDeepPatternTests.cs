using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class WindowFunctionDeepPatternTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public WindowFunctionDeepPatternTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_wdp_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"
            CREATE TABLE `{_datasetId}.emp` (id INT64, dept STRING, salary INT64, hire_date DATE)", parameters: null);
        await client.ExecuteQueryAsync($@"
            INSERT INTO `{_datasetId}.emp` (id, dept, salary, hire_date) VALUES
            (1, 'eng', 100000, DATE '2020-01-15'),
            (2, 'eng', 120000, DATE '2019-06-01'),
            (3, 'eng', 95000, DATE '2021-03-10'),
            (4, 'sales', 80000, DATE '2020-09-01'),
            (5, 'sales', 90000, DATE '2018-11-15'),
            (6, 'sales', 85000, DATE '2022-01-20'),
            (7, 'hr', 70000, DATE '2019-04-01'),
            (8, 'hr', 75000, DATE '2021-08-15'),
            (9, 'eng', 110000, DATE '2022-05-01'),
            (10, 'sales', 95000, DATE '2017-06-01')", parameters: null);
    }

    public async ValueTask DisposeAsync()
    {
        try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
        await _fixture.DisposeAsync();
    }

    private async Task<List<BigQueryRow>> Query(string sql)
    {
        var client = await _fixture.GetClientAsync();
        var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
        return result.ToList();
    }

    private async Task<string?> Scalar(string sql)
    {
        var rows = await Query(sql);
        return rows.Count > 0 ? rows[0][0]?.ToString() : null;
    }

    // ROW_NUMBER
    [Fact] public async Task RowNumber_Unpartitioned()
    {
        var rows = await Query("SELECT id, ROW_NUMBER() OVER (ORDER BY salary DESC) as rn FROM `{ds}.emp` ORDER BY rn");
        Assert.Equal("1", rows[0]["rn"]?.ToString());
        Assert.Equal("10", rows[9]["rn"]?.ToString());
    }
    [Fact] public async Task RowNumber_Partitioned()
    {
        var rows = await Query("SELECT id, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn FROM `{ds}.emp` WHERE dept = 'eng' ORDER BY rn");
        Assert.Equal(4, rows.Count);
        Assert.Equal("1", rows[0]["rn"]?.ToString());
    }

    // RANK
    [Fact] public async Task Rank_Basic()
    {
        var rows = await Query("SELECT id, RANK() OVER (ORDER BY salary DESC) as rnk FROM `{ds}.emp` ORDER BY rnk");
        Assert.Equal("1", rows[0]["rnk"]?.ToString());
    }
    [Fact] public async Task Rank_Partitioned()
    {
        var rows = await Query("SELECT id, dept, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) as rnk FROM `{ds}.emp` WHERE dept = 'sales' ORDER BY rnk");
        Assert.Equal(4, rows.Count);
    }

    // DENSE_RANK
    [Fact] public async Task DenseRank_Basic()
    {
        var rows = await Query("SELECT id, DENSE_RANK() OVER (ORDER BY dept) as dr FROM `{ds}.emp` ORDER BY dr, id");
        Assert.Equal("1", rows[0]["dr"]?.ToString());
    }

    // NTILE
    [Fact] public async Task Ntile_Basic()
    {
        var rows = await Query("SELECT id, NTILE(4) OVER (ORDER BY salary) as tile FROM `{ds}.emp` ORDER BY tile, id");
        Assert.Contains(rows, r => r["tile"]?.ToString() == "1");
        Assert.Contains(rows, r => r["tile"]?.ToString() == "4");
    }

    // LAG / LEAD
    [Fact] public async Task Lag_Basic()
    {
        var rows = await Query("SELECT id, salary, LAG(salary) OVER (ORDER BY salary) as prev_sal FROM `{ds}.emp` ORDER BY salary");
        Assert.Null(rows[0]["prev_sal"]);
        Assert.NotNull(rows[1]["prev_sal"]);
    }
    [Fact] public async Task Lead_Basic()
    {
        var rows = await Query("SELECT id, salary, LEAD(salary) OVER (ORDER BY salary) as next_sal FROM `{ds}.emp` ORDER BY salary");
        Assert.NotNull(rows[0]["next_sal"]);
        Assert.Null(rows[9]["next_sal"]);
    }
    [Fact] public async Task Lag_WithDefault()
    {
        var rows = await Query("SELECT id, LAG(salary, 1, 0) OVER (ORDER BY salary) as prev_sal FROM `{ds}.emp` ORDER BY salary");
        Assert.Equal("0", rows[0]["prev_sal"]?.ToString());
    }
    [Fact] public async Task Lead_Offset2()
    {
        var rows = await Query("SELECT id, LEAD(salary, 2) OVER (ORDER BY salary) as next2 FROM `{ds}.emp` ORDER BY salary");
        Assert.Null(rows[8]["next2"]);
        Assert.Null(rows[9]["next2"]);
    }

    // FIRST_VALUE / LAST_VALUE
    [Fact] public async Task FirstValue_Basic()
    {
        var rows = await Query("SELECT id, FIRST_VALUE(salary) OVER (PARTITION BY dept ORDER BY salary DESC) as top_sal FROM `{ds}.emp` WHERE dept = 'eng'");
        Assert.True(rows.All(r => r["top_sal"]?.ToString() == "120000"));
    }
    [Fact] public async Task LastValue_Basic()
    {
        var rows = await Query("SELECT id, LAST_VALUE(salary) OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as bottom FROM `{ds}.emp` WHERE dept = 'eng'");
        Assert.True(rows.All(r => r["bottom"]?.ToString() == "120000"));
    }

    // SUM window
    [Fact] public async Task Sum_Window()
    {
        var rows = await Query("SELECT id, SUM(salary) OVER (PARTITION BY dept) as dept_total FROM `{ds}.emp` WHERE dept = 'hr'");
        Assert.True(rows.All(r => r["dept_total"]?.ToString() == "145000"));
    }
    [Fact] public async Task Sum_RunningTotal()
    {
        var rows = await Query("SELECT id, salary, SUM(salary) OVER (ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running FROM `{ds}.emp` ORDER BY salary");
        Assert.Equal("70000", rows[0]["running"]?.ToString());
    }

    // AVG window
    [Fact] public async Task Avg_Window()
    {
        var rows = await Query("SELECT id, AVG(salary) OVER (PARTITION BY dept) as avg_sal FROM `{ds}.emp` WHERE dept = 'hr'");
        var avg = double.Parse(rows[0]["avg_sal"]!.ToString()!);
        Assert.True(avg > 72000 && avg < 73000);
    }

    // COUNT window
    [Fact] public async Task Count_Window()
    {
        var rows = await Query("SELECT id, COUNT(*) OVER (PARTITION BY dept) as cnt FROM `{ds}.emp` WHERE dept = 'eng'");
        Assert.True(rows.All(r => r["cnt"]?.ToString() == "4"));
    }

    // MIN/MAX window
    [Fact] public async Task Min_Window()
    {
        var rows = await Query("SELECT id, MIN(salary) OVER (PARTITION BY dept) as mn FROM `{ds}.emp` WHERE dept = 'eng'");
        Assert.True(rows.All(r => r["mn"]?.ToString() == "95000"));
    }
    [Fact] public async Task Max_Window()
    {
        var rows = await Query("SELECT id, MAX(salary) OVER (PARTITION BY dept) as mx FROM `{ds}.emp` WHERE dept = 'eng'");
        Assert.True(rows.All(r => r["mx"]?.ToString() == "120000"));
    }

    // Multiple windows in same query
    [Fact] public async Task MultipleWindows()
    {
        var rows = await Query("SELECT id, ROW_NUMBER() OVER (ORDER BY salary) as rn, RANK() OVER (ORDER BY dept) as rnk FROM `{ds}.emp` ORDER BY rn");
        Assert.Equal(10, rows.Count);
    }

    // Window with WHERE
    [Fact] public async Task Window_WithWhere()
    {
        var rows = await Query("SELECT id, ROW_NUMBER() OVER (ORDER BY salary DESC) as rn FROM `{ds}.emp` WHERE salary > 90000 ORDER BY rn");
        Assert.Equal(5, rows.Count);
        Assert.Equal("1", rows[0]["rn"]?.ToString());
    }

    // PERCENT_RANK
    [Fact] public async Task PercentRank_Basic()
    {
        var rows = await Query("SELECT id, PERCENT_RANK() OVER (ORDER BY salary) as pr FROM `{ds}.emp` ORDER BY salary");
        Assert.Equal("0", rows[0]["pr"]?.ToString());
    }

    // CUME_DIST
    [Fact] public async Task CumeDist_Basic()
    {
        var rows = await Query("SELECT id, CUME_DIST() OVER (ORDER BY salary) as cd FROM `{ds}.emp` ORDER BY salary");
        var last = double.Parse(rows[9]["cd"]!.ToString()!);
        Assert.Equal(1.0, last);
    }
}