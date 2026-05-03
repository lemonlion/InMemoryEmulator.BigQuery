using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class JoinPatternDeepTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public JoinPatternDeepTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_jnd_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.customers` (id INT64, name STRING, city STRING)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.customers` (id, name, city) VALUES (1,'Alice','NYC'),(2,'Bob','LA'),(3,'Carol','NYC'),(4,'Dave','Chicago'),(5,'Eve',NULL)", parameters: null);
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.orders` (id INT64, customer_id INT64, amount FLOAT64, product STRING)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.orders` (id, customer_id, amount, product) VALUES (101,1,50.0,'A'),(102,1,75.0,'B'),(103,2,100.0,'A'),(104,3,200.0,'C'),(105,6,25.0,'A'),(106,2,60.0,'B'),(107,1,30.0,'A')", parameters: null);
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.products` (code STRING, name STRING, price FLOAT64)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.products` (code, name, price) VALUES ('A','Widget',10.0),('B','Gadget',20.0),('C','Doohickey',30.0),('D','Thingamajig',40.0)", parameters: null);
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

    // INNER JOIN
    [Fact] public async Task InnerJoin_Basic()
    {
        var rows = await Query("SELECT c.name, o.amount FROM `{ds}.customers` c INNER JOIN `{ds}.orders` o ON c.id = o.customer_id ORDER BY o.amount");
        Assert.Equal(6, rows.Count); // customer 6 doesn't exist
    }
    [Fact] public async Task InnerJoin_WithWhere()
    {
        var rows = await Query("SELECT c.name, o.amount FROM `{ds}.customers` c INNER JOIN `{ds}.orders` o ON c.id = o.customer_id WHERE o.amount > 60 ORDER BY o.amount");
        Assert.Equal(3, rows.Count);
    }

    // LEFT JOIN
    [Fact] public async Task LeftJoin_AllCustomers()
    {
        var rows = await Query("SELECT c.name, o.id as order_id FROM `{ds}.customers` c LEFT JOIN `{ds}.orders` o ON c.id = o.customer_id ORDER BY c.name");
        Assert.True(rows.Count >= 5); // every customer appears at least once
    }
    [Fact] public async Task LeftJoin_NullsForNoMatch()
    {
        var rows = await Query("SELECT c.name, o.amount FROM `{ds}.customers` c LEFT JOIN `{ds}.orders` o ON c.id = o.customer_id WHERE c.name = 'Dave'");
        Assert.Single(rows);
        Assert.Null(rows[0]["amount"]);
    }

    // RIGHT JOIN
    [Fact] public async Task RightJoin_AllOrders()
    {
        var rows = await Query("SELECT c.name, o.id as oid FROM `{ds}.customers` c RIGHT JOIN `{ds}.orders` o ON c.id = o.customer_id ORDER BY o.id");
        Assert.Equal(7, rows.Count);
    }
    [Fact] public async Task RightJoin_NullForNoMatch()
    {
        var rows = await Query("SELECT c.name, o.id as oid FROM `{ds}.customers` c RIGHT JOIN `{ds}.orders` o ON c.id = o.customer_id WHERE o.id = 105");
        Assert.Single(rows);
        Assert.Null(rows[0]["name"]);
    }

    // FULL OUTER JOIN
    [Fact] public async Task FullOuterJoin_All()
    {
        var rows = await Query("SELECT c.name, o.id as oid FROM `{ds}.customers` c FULL OUTER JOIN `{ds}.orders` o ON c.id = o.customer_id ORDER BY c.name, o.id");
        Assert.True(rows.Count >= 8);
    }

    // CROSS JOIN
    [Fact] public async Task CrossJoin_Cartesian()
    {
        var rows = await Query("SELECT c.name, p.name as pname FROM `{ds}.customers` c CROSS JOIN `{ds}.products` p WHERE c.id <= 2");
        Assert.Equal(8, rows.Count); // 2 customers * 4 products
    }

    // Multiple JOINs
    [Fact] public async Task MultiJoin_ThreeTables()
    {
        var rows = await Query("SELECT c.name, o.amount, p.name as pname FROM `{ds}.customers` c JOIN `{ds}.orders` o ON c.id = o.customer_id JOIN `{ds}.products` p ON o.product = p.code ORDER BY o.id");
        Assert.Equal(6, rows.Count);
    }

    // Self JOIN
    [Fact] public async Task SelfJoin()
    {
        var rows = await Query("SELECT c1.name as n1, c2.name as n2 FROM `{ds}.customers` c1 JOIN `{ds}.customers` c2 ON c1.city = c2.city AND c1.id < c2.id WHERE c1.city IS NOT NULL");
        Assert.True(rows.Count > 0); // Alice-Carol both in NYC
    }

    // JOIN with aggregation
    [Fact] public async Task Join_Aggregation()
    {
        var rows = await Query("SELECT c.name, COUNT(o.id) as order_count, CAST(SUM(o.amount) AS INT64) as total FROM `{ds}.customers` c LEFT JOIN `{ds}.orders` o ON c.id = o.customer_id GROUP BY c.name ORDER BY total DESC");
        Assert.Equal(5, rows.Count);
        Assert.NotNull(rows[0]["name"]);
    }

    // JOIN with subquery
    [Fact] public async Task Join_Subquery()
    {
        var rows = await Query("SELECT c.name, t.total FROM `{ds}.customers` c JOIN (SELECT customer_id, CAST(SUM(amount) AS INT64) as total FROM `{ds}.orders` GROUP BY customer_id) t ON c.id = t.customer_id ORDER BY t.total DESC");
        Assert.Equal(3, rows.Count); // only 3 valid customer_ids
    }

    // JOIN with USING
    [Fact] public async Task Join_Using()
    {
        var rows = await Query("SELECT c.name, o.amount FROM `{ds}.customers` c JOIN (SELECT customer_id as id, amount FROM `{ds}.orders`) o USING (id) ORDER BY o.amount");
        Assert.True(rows.Count >= 5);
    }

    // JOIN condition with multiple columns
    [Fact] public async Task Join_CompoundCondition()
    {
        var rows = await Query("SELECT c.name, o.amount FROM `{ds}.customers` c JOIN `{ds}.orders` o ON c.id = o.customer_id AND o.amount > 50 ORDER BY o.amount");
        Assert.Equal(4, rows.Count);
    }

    // LEFT JOIN with IS NULL filter (anti-join)
    [Fact] public async Task AntiJoin()
    {
        var rows = await Query("SELECT c.name FROM `{ds}.customers` c LEFT JOIN `{ds}.orders` o ON c.id = o.customer_id WHERE o.id IS NULL ORDER BY c.name");
        Assert.Equal(2, rows.Count); // Dave and Eve
    }

    // JOIN with DISTINCT
    [Fact] public async Task Join_Distinct()
    {
        var rows = await Query("SELECT DISTINCT c.name FROM `{ds}.customers` c JOIN `{ds}.orders` o ON c.id = o.customer_id ORDER BY c.name");
        Assert.Equal(3, rows.Count); // Alice, Bob, Carol
    }
}