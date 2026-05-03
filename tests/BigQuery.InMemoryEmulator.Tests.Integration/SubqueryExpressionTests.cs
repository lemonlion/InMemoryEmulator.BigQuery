using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class SubqueryExpressionTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public SubqueryExpressionTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_sq2_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.products` (id INT64, name STRING, price FLOAT64, category STRING)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.products` (id, name, price, category) VALUES
            (1,'Laptop',1000,'Electronics'),(2,'Phone',800,'Electronics'),(3,'Tablet',600,'Electronics'),
            (4,'Desk',300,'Furniture'),(5,'Chair',200,'Furniture'),(6,'Lamp',50,'Furniture'),
            (7,'Book',20,'Books'),(8,'Pen',5,'Books')", parameters: null);
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

    // Scalar subquery in SELECT
    [Fact] public async Task ScalarSubquery_InSelect()
    {
        var rows = await Query("SELECT name, price, (SELECT AVG(price) FROM `{ds}.products`) as avg_price FROM `{ds}.products` WHERE id = 1");
        Assert.NotNull(rows[0]["avg_price"]);
    }

    // Scalar subquery in WHERE
    [Fact] public async Task ScalarSubquery_InWhere()
    {
        var rows = await Query("SELECT name FROM `{ds}.products` WHERE price > (SELECT AVG(price) FROM `{ds}.products`) ORDER BY name");
        Assert.True(rows.Count >= 2);
    }

    // EXISTS
    [Fact] public async Task Exists_True() => Assert.Equal("True", await Scalar("SELECT EXISTS(SELECT 1 FROM `{ds}.products` WHERE category = 'Electronics')"));
    [Fact] public async Task Exists_False() => Assert.Equal("False", await Scalar("SELECT EXISTS(SELECT 1 FROM `{ds}.products` WHERE category = 'Toys')"));
    [Fact] public async Task Exists_InWhere()
    {
        var rows = await Query("SELECT DISTINCT category FROM `{ds}.products` p WHERE EXISTS (SELECT 1 FROM `{ds}.products` p2 WHERE p2.category = p.category AND p2.price > 500) ORDER BY category");
        Assert.Single(rows);
        Assert.Equal("Electronics", rows[0]["category"]?.ToString());
    }

    // NOT EXISTS
    [Fact] public async Task NotExists_InWhere()
    {
        var rows = await Query("SELECT DISTINCT category FROM `{ds}.products` p WHERE NOT EXISTS (SELECT 1 FROM `{ds}.products` p2 WHERE p2.category = p.category AND p2.price > 500) ORDER BY category");
        Assert.Equal(2, rows.Count);
    }

    // IN subquery
    [Fact] public async Task In_Subquery()
    {
        var rows = await Query("SELECT name FROM `{ds}.products` WHERE category IN (SELECT category FROM `{ds}.products` WHERE price > 500) ORDER BY name");
        Assert.Equal(3, rows.Count); // All Electronics
    }

    // NOT IN subquery
    [Fact] public async Task NotIn_Subquery()
    {
        var rows = await Query("SELECT name FROM `{ds}.products` WHERE category NOT IN (SELECT category FROM `{ds}.products` WHERE price > 500) ORDER BY name");
        Assert.Equal(5, rows.Count);
    }

    // Correlated subquery
    [Fact] public async Task Correlated_InSelect()
    {
        var rows = await Query("SELECT name, price, (SELECT MAX(p2.price) FROM `{ds}.products` p2 WHERE p2.category = p.category) as max_in_cat FROM `{ds}.products` p WHERE id = 1");
        Assert.Equal("1000", rows[0]["max_in_cat"]?.ToString());
    }
    [Fact] public async Task Correlated_InWhere()
    {
        var rows = await Query("SELECT name FROM `{ds}.products` p WHERE price = (SELECT MAX(p2.price) FROM `{ds}.products` p2 WHERE p2.category = p.category) ORDER BY name");
        Assert.Equal(3, rows.Count); // Max in each category
    }

    // Subquery in FROM
    [Fact] public async Task Subquery_InFrom()
    {
        var rows = await Query("SELECT sub.category, sub.cnt FROM (SELECT category, COUNT(*) as cnt FROM `{ds}.products` GROUP BY category) sub ORDER BY sub.category");
        Assert.Equal(3, rows.Count);
    }
    [Fact] public async Task Subquery_InFrom_WithWhere()
    {
        var rows = await Query("SELECT sub.name FROM (SELECT name, price FROM `{ds}.products` WHERE category = 'Electronics') sub WHERE sub.price > 700 ORDER BY sub.name");
        Assert.Equal(2, rows.Count);
    }

    // Nested subqueries
    [Fact] public async Task Nested_Subquery()
    {
        var result = await Scalar("SELECT COUNT(*) FROM `{ds}.products` WHERE price > (SELECT AVG(price) FROM `{ds}.products` WHERE category = (SELECT category FROM `{ds}.products` WHERE id = 1))");
        Assert.NotNull(result);
    }

    // ARRAY subquery
    [Fact] public async Task Array_Subquery()
    {
        var result = await Scalar("SELECT ARRAY_LENGTH(ARRAY(SELECT name FROM `{ds}.products` WHERE category = 'Electronics'))");
        Assert.Equal("3", result);
    }
}