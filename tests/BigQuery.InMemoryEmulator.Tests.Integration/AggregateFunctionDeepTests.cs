using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class AggregateFunctionDeepTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public AggregateFunctionDeepTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_agg_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.sales` (id INT64, product STRING, region STRING, amount FLOAT64, qty INT64)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.sales` (id, product, region, amount, qty) VALUES
            (1,'A','East',100.0,10),(2,'A','West',150.0,15),(3,'B','East',200.0,20),
            (4,'B','West',250.0,25),(5,'C','East',300.0,NULL),(6,'C','West',NULL,35),
            (7,'A','East',120.0,12),(8,'B','East',180.0,18),(9,'C','West',350.0,30)", parameters: null);
    }

    public async ValueTask DisposeAsync()
    {
        try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
        await _fixture.DisposeAsync();
    }

    private async Task<string?> Scalar(string sql)
    {
        var client = await _fixture.GetClientAsync();
        var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
        var rows = result.ToList();
        return rows.Count > 0 ? rows[0][0]?.ToString() : null;
    }

    private async Task<List<BigQueryRow>> Query(string sql)
    {
        var client = await _fixture.GetClientAsync();
        var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
        return result.ToList();
    }

    [Fact] public async Task Count_All() => Assert.Equal("9", await Scalar("SELECT COUNT(*) FROM `{ds}.sales`"));
    [Fact] public async Task Count_Column() => Assert.Equal("8", await Scalar("SELECT COUNT(amount) FROM `{ds}.sales`"));
    [Fact] public async Task Count_Distinct() => Assert.Equal("3", await Scalar("SELECT COUNT(DISTINCT product) FROM `{ds}.sales`"));
    [Fact] public async Task Sum_Basic()
    {
        var v = double.Parse((await Scalar("SELECT SUM(amount) FROM `{ds}.sales`"))!);
        Assert.Equal(1650.0, v);
    }
    [Fact] public async Task Sum_Null_Excluded()
    {
        var v = double.Parse((await Scalar("SELECT SUM(amount) FROM `{ds}.sales` WHERE product = 'C'"))!);
        Assert.Equal(650.0, v);
    }
    [Fact] public async Task Avg_Basic()
    {
        var v = double.Parse((await Scalar("SELECT AVG(amount) FROM `{ds}.sales` WHERE product = 'A'"))!);
        Assert.InRange(v, 123.0, 124.0);
    }
    [Fact] public async Task Min_Basic() => Assert.Equal("100", await Scalar("SELECT CAST(MIN(amount) AS INT64) FROM `{ds}.sales`"));
    [Fact] public async Task Max_Basic() => Assert.Equal("350", await Scalar("SELECT CAST(MAX(amount) AS INT64) FROM `{ds}.sales`"));
    [Fact] public async Task Min_String() => Assert.Equal("A", await Scalar("SELECT MIN(product) FROM `{ds}.sales`"));
    [Fact] public async Task Max_String() => Assert.Equal("C", await Scalar("SELECT MAX(product) FROM `{ds}.sales`"));
    [Fact] public async Task GroupBy_Count()
    {
        var rows = await Query("SELECT product, COUNT(*) as cnt FROM `{ds}.sales` GROUP BY product ORDER BY product");
        Assert.Equal(3, rows.Count);
        Assert.Equal("3", rows[0]["cnt"]?.ToString());
    }
    [Fact] public async Task GroupBy_Sum()
    {
        var rows = await Query("SELECT product, CAST(SUM(amount) AS INT64) as total FROM `{ds}.sales` GROUP BY product ORDER BY product");
        Assert.Equal("370", rows[0]["total"]?.ToString()); // A: 100+150+120
    }
    [Fact] public async Task GroupBy_MultiColumn()
    {
        var rows = await Query("SELECT product, region, COUNT(*) as cnt FROM `{ds}.sales` GROUP BY product, region ORDER BY product, region");
        Assert.True(rows.Count >= 5);
    }
    [Fact] public async Task Having_Basic()
    {
        var rows = await Query("SELECT product, COUNT(*) as cnt FROM `{ds}.sales` GROUP BY product HAVING COUNT(*) = 3");
        Assert.Equal(3, rows.Count);
    }
    [Fact] public async Task Having_Sum()
    {
        var rows = await Query("SELECT product, CAST(SUM(amount) AS INT64) as total FROM `{ds}.sales` GROUP BY product HAVING SUM(amount) > 500 ORDER BY product");
        Assert.True(rows.Count >= 1);
    }
    [Fact] public async Task Countif_Basic() => Assert.Equal("5", await Scalar("SELECT COUNTIF(amount > 150) FROM `{ds}.sales`"));
    [Fact] public async Task Sum_WithFilter()
    {
        var v = double.Parse((await Scalar("SELECT SUM(amount) FROM `{ds}.sales` WHERE region = 'East'"))!);
        Assert.Equal(900.0, v); // 100+200+300+120+180
    }
    [Fact] public async Task Avg_GroupBy()
    {
        var rows = await Query("SELECT region, CAST(AVG(amount) AS INT64) as avg_amt FROM `{ds}.sales` GROUP BY region ORDER BY region");
        Assert.Equal(2, rows.Count);
    }
    [Fact] public async Task LogicalOr() => Assert.Equal("True", await Scalar("SELECT LOGICAL_OR(amount > 300) FROM `{ds}.sales`"));
    [Fact] public async Task LogicalAnd() => Assert.Equal("False", await Scalar("SELECT LOGICAL_AND(amount > 100) FROM `{ds}.sales` WHERE amount IS NOT NULL"));
    [Fact] public async Task ArrayAgg_Basic()
    {
        var rows = await Query("SELECT ARRAY_AGG(DISTINCT product ORDER BY product) as products FROM `{ds}.sales`");
        Assert.Single(rows);
    }
    [Fact] public async Task StringAgg_Basic() 
    {
        var result = await Scalar("SELECT STRING_AGG(product, ',' ORDER BY product) FROM `{ds}.sales`");
        Assert.NotNull(result);
        Assert.Contains("A", result!);
    }
    [Fact] public async Task Count_NullOnly() => Assert.Equal("0", await Scalar("SELECT COUNT(amount) FROM `{ds}.sales` WHERE amount IS NULL"));
    [Fact] public async Task Sum_AllNull() => Assert.Null(await Scalar("SELECT SUM(amount) FROM `{ds}.sales` WHERE amount IS NULL"));
    [Fact] public async Task GroupBy_Having_Count()
    {
        var rows = await Query("SELECT region, COUNT(*) as cnt FROM `{ds}.sales` GROUP BY region HAVING COUNT(*) > 4");
        Assert.Single(rows);
        Assert.Equal("East", rows[0]["region"]?.ToString());
    }
}