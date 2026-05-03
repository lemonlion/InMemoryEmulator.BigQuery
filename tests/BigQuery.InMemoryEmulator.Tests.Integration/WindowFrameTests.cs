using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class WindowFrameTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public WindowFrameTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_wfr_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.sales` (id INT64, month INT64, amount INT64, region STRING)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.sales` (id, month, amount, region) VALUES
            (1,1,100,'East'),(2,2,150,'East'),(3,3,200,'East'),(4,4,180,'East'),(5,5,220,'East'),
            (6,6,190,'East'),(7,1,80,'West'),(8,2,120,'West'),(9,3,160,'West'),(10,4,140,'West'),
            (11,5,200,'West'),(12,6,170,'West')", parameters: null);
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
    [Fact] public async Task RowNumber_Basic()
    {
        var rows = await Query("SELECT id, ROW_NUMBER() OVER (ORDER BY id) as rn FROM `{ds}.sales` ORDER BY id");
        Assert.Equal("1", rows[0]["rn"]?.ToString());
        Assert.Equal("12", rows[11]["rn"]?.ToString());
    }
    [Fact] public async Task RowNumber_Partition()
    {
        var rows = await Query("SELECT id, region, ROW_NUMBER() OVER (PARTITION BY region ORDER BY month) as rn FROM `{ds}.sales` ORDER BY region, month");
        Assert.Equal("1", rows[0]["rn"]?.ToString());
    }

    // RANK / DENSE_RANK
    [Fact] public async Task Rank_Basic()
    {
        var rows = await Query("SELECT amount, RANK() OVER (ORDER BY amount DESC) as rnk FROM `{ds}.sales` ORDER BY amount DESC");
        Assert.Equal("1", rows[0]["rnk"]?.ToString());
    }
    [Fact] public async Task DenseRank_Basic()
    {
        var rows = await Query("SELECT amount, DENSE_RANK() OVER (ORDER BY amount DESC) as drnk FROM `{ds}.sales` ORDER BY amount DESC");
        Assert.Equal("1", rows[0]["drnk"]?.ToString());
    }

    // SUM window
    [Fact] public async Task Sum_Window()
    {
        var rows = await Query("SELECT id, SUM(amount) OVER (PARTITION BY region ORDER BY month) as running_total FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
        Assert.Equal("100", rows[0]["running_total"]?.ToString());
        Assert.Equal("250", rows[1]["running_total"]?.ToString());
    }

    // AVG window
    [Fact] public async Task Avg_Window()
    {
        var rows = await Query("SELECT id, CAST(AVG(amount) OVER (PARTITION BY region) AS INT64) as avg_amt FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
        Assert.NotNull(rows[0]["avg_amt"]);
    }

    // COUNT window
    [Fact] public async Task Count_Window()
    {
        var rows = await Query("SELECT id, COUNT(*) OVER (PARTITION BY region) as cnt FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
        Assert.Equal("6", rows[0]["cnt"]?.ToString());
    }

    // MIN/MAX window
    [Fact] public async Task Min_Window()
    {
        var rows = await Query("SELECT id, MIN(amount) OVER (PARTITION BY region) as min_amt FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
        Assert.Equal("100", rows[0]["min_amt"]?.ToString());
    }
    [Fact] public async Task Max_Window()
    {
        var rows = await Query("SELECT id, MAX(amount) OVER (PARTITION BY region) as max_amt FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
        Assert.Equal("220", rows[0]["max_amt"]?.ToString());
    }

    // LAG / LEAD
    [Fact] public async Task Lag_Basic()
    {
        var rows = await Query("SELECT month, amount, LAG(amount) OVER (PARTITION BY region ORDER BY month) as prev FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
        Assert.Null(rows[0]["prev"]);
        Assert.Equal("100", rows[1]["prev"]?.ToString());
    }
    [Fact] public async Task Lead_Basic()
    {
        var rows = await Query("SELECT month, amount, LEAD(amount) OVER (PARTITION BY region ORDER BY month) as next_amt FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
        Assert.Equal("150", rows[0]["next_amt"]?.ToString());
        Assert.Null(rows[5]["next_amt"]);
    }
    [Fact] public async Task Lag_WithDefault()
    {
        var rows = await Query("SELECT month, LAG(amount, 1, 0) OVER (PARTITION BY region ORDER BY month) as prev FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
        Assert.Equal("0", rows[0]["prev"]?.ToString());
    }
    [Fact] public async Task Lag_Offset2()
    {
        var rows = await Query("SELECT month, LAG(amount, 2) OVER (PARTITION BY region ORDER BY month) as prev2 FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
        Assert.Null(rows[0]["prev2"]);
        Assert.Null(rows[1]["prev2"]);
        Assert.Equal("100", rows[2]["prev2"]?.ToString());
    }

    // FIRST_VALUE / LAST_VALUE
    [Fact] public async Task FirstValue_Basic()
    {
        var rows = await Query("SELECT month, FIRST_VALUE(amount) OVER (PARTITION BY region ORDER BY month) as first_amt FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
        Assert.Equal("100", rows[0]["first_amt"]?.ToString());
        Assert.Equal("100", rows[5]["first_amt"]?.ToString());
    }
    [Fact] public async Task LastValue_Unbounded()
    {
        var rows = await Query("SELECT month, LAST_VALUE(amount) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_amt FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
        Assert.Equal("190", rows[0]["last_amt"]?.ToString());
    }

    // NTILE
    [Fact] public async Task Ntile_Basic()
    {
        var rows = await Query("SELECT month, NTILE(3) OVER (ORDER BY month) as tile FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
        Assert.Equal("1", rows[0]["tile"]?.ToString());
    }

    // ROWS BETWEEN
    [Fact] public async Task RowsBetween_Preceding()
    {
        var rows = await Query("SELECT month, SUM(amount) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as sum3 FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
        Assert.Equal("100", rows[0]["sum3"]?.ToString()); // only row 1
        Assert.Equal("250", rows[1]["sum3"]?.ToString()); // rows 1+2
        Assert.Equal("450", rows[2]["sum3"]?.ToString()); // rows 1+2+3
    }

    // Multiple windows in same query
    [Fact] public async Task MultipleWindows()
    {
        var rows = await Query("SELECT month, ROW_NUMBER() OVER (ORDER BY month) as rn, SUM(amount) OVER (ORDER BY month) as running FROM `{ds}.sales` WHERE region = 'East' ORDER BY month");
        Assert.Equal("1", rows[0]["rn"]?.ToString());
        Assert.Equal("100", rows[0]["running"]?.ToString());
    }

    // Window with WHERE
    [Fact] public async Task Window_WithWhere()
    {
        var rows = await Query("SELECT month, amount, RANK() OVER (ORDER BY amount DESC) as rnk FROM `{ds}.sales` WHERE region = 'East' AND amount > 150 ORDER BY amount DESC");
        Assert.Equal("1", rows[0]["rnk"]?.ToString());
    }
}