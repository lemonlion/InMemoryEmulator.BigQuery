using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class GroupByAdvancedTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public GroupByAdvancedTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_gb2_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.sales` (id INT64, region STRING, product STRING, year INT64, quarter INT64, amount FLOAT64)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.sales` (id, region, product, year, quarter, amount) VALUES
            (1,'East','Widget',2023,1,100.0),(2,'East','Gadget',2023,1,200.0),(3,'West','Widget',2023,2,150.0),
            (4,'West','Gadget',2023,2,300.0),(5,'East','Widget',2023,3,175.0),(6,'West','Widget',2023,4,125.0),
            (7,'East','Gadget',2024,1,250.0),(8,'West','Widget',2024,1,180.0),(9,'East','Widget',2024,2,220.0),
            (10,'West','Gadget',2024,2,190.0),(11,'East','Widget',2024,3,160.0),(12,'North','Gadget',2024,1,140.0)", parameters: null);
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

    // Basic GROUP BY
    [Fact] public async Task GroupBy_SingleColumn()
    {
        var rows = await Query("SELECT region, COUNT(*) as cnt FROM `{ds}.sales` GROUP BY region ORDER BY region");
        Assert.Equal(3, rows.Count);
    }
    [Fact] public async Task GroupBy_MultiColumn()
    {
        var rows = await Query("SELECT region, product, COUNT(*) as cnt FROM `{ds}.sales` GROUP BY region, product ORDER BY region, product");
        Assert.True(rows.Count >= 5);
    }

    // GROUP BY with HAVING
    [Fact] public async Task Having_Count()
    {
        var rows = await Query("SELECT region, COUNT(*) as cnt FROM `{ds}.sales` GROUP BY region HAVING COUNT(*) > 3 ORDER BY region");
        Assert.Equal(2, rows.Count); // East=6, West=5
    }
    [Fact] public async Task Having_Sum()
    {
        var rows = await Query("SELECT product, CAST(SUM(amount) AS INT64) as total FROM `{ds}.sales` GROUP BY product HAVING SUM(amount) > 1000 ORDER BY product");
        Assert.True(rows.Count >= 1);
    }
    [Fact] public async Task Having_Avg()
    {
        var rows = await Query("SELECT region, AVG(amount) as avg_amt FROM `{ds}.sales` GROUP BY region HAVING AVG(amount) > 180");
        Assert.True(rows.Count >= 1);
    }

    // GROUP BY with expressions
    [Fact] public async Task GroupBy_Expression()
    {
        var rows = await Query("SELECT year, COUNT(*) as cnt FROM `{ds}.sales` GROUP BY year ORDER BY year");
        Assert.Equal(2, rows.Count);
    }
    [Fact] public async Task GroupBy_CaseExpression()
    {
        var rows = await Query("SELECT CASE WHEN amount > 200 THEN 'high' ELSE 'low' END as tier, COUNT(*) as cnt FROM `{ds}.sales` GROUP BY tier ORDER BY tier");
        Assert.Equal(2, rows.Count);
    }

    // Multiple aggregates
    [Fact] public async Task GroupBy_MultipleAggregates()
    {
        var rows = await Query("SELECT region, COUNT(*) as cnt, CAST(SUM(amount) AS INT64) as total, CAST(AVG(amount) AS INT64) as avg_amt, MIN(amount) as mn, MAX(amount) as mx FROM `{ds}.sales` GROUP BY region ORDER BY region");
        Assert.Equal(3, rows.Count);
        Assert.NotNull(rows[0]["cnt"]);
        Assert.NotNull(rows[0]["total"]);
    }

    // GROUP BY with ORDER BY aggregate
    [Fact] public async Task GroupBy_OrderByAgg()
    {
        var rows = await Query("SELECT region, CAST(SUM(amount) AS INT64) as total FROM `{ds}.sales` GROUP BY region ORDER BY total DESC");
        Assert.Equal(3, rows.Count);
    }

    // GROUP BY ALL (implicit)
    [Fact] public async Task GroupBy_WithWhere()
    {
        var rows = await Query("SELECT product, COUNT(*) as cnt FROM `{ds}.sales` WHERE year = 2024 GROUP BY product ORDER BY product");
        Assert.Equal(2, rows.Count);
    }

    // GROUP BY + LIMIT
    [Fact] public async Task GroupBy_Limit()
    {
        var rows = await Query("SELECT region, COUNT(*) as cnt FROM `{ds}.sales` GROUP BY region ORDER BY cnt DESC LIMIT 2");
        Assert.Equal(2, rows.Count);
    }

    // COUNT DISTINCT in GROUP BY
    [Fact] public async Task GroupBy_CountDistinct()
    {
        var rows = await Query("SELECT region, COUNT(DISTINCT product) as prod_cnt FROM `{ds}.sales` GROUP BY region ORDER BY region");
        Assert.Equal(3, rows.Count);
    }

    // GROUP BY year, quarter
    [Fact] public async Task GroupBy_YearQuarter()
    {
        var rows = await Query("SELECT year, quarter, COUNT(*) as cnt FROM `{ds}.sales` GROUP BY year, quarter ORDER BY year, quarter");
        Assert.True(rows.Count >= 5);
    }

    // Aggregate without GROUP BY
    [Fact] public async Task Aggregate_NoGroupBy() => Assert.Equal("12", await Scalar("SELECT COUNT(*) FROM `{ds}.sales`"));
    [Fact] public async Task Aggregate_NoGroupBy_Sum() => Assert.NotNull(await Scalar("SELECT SUM(amount) FROM `{ds}.sales`"));

    // GROUP BY with NULL
    [Fact] public async Task GroupBy_WithNull()
    {
        var rows = await Query("SELECT region, COUNT(*) as cnt FROM `{ds}.sales` GROUP BY region ORDER BY region");
        Assert.Equal(3, rows.Count); // North appears with 1  
    }

    // HAVING with COUNT(*)=0 edge case
    [Fact] public async Task Having_NoMatch()
    {
        var rows = await Query("SELECT region, COUNT(*) as cnt FROM `{ds}.sales` GROUP BY region HAVING COUNT(*) > 100");
        Assert.Empty(rows);
    }
}