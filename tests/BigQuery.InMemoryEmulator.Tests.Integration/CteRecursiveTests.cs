using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class CteRecursiveTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public CteRecursiveTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_cte_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.categories` (id INT64, name STRING, parent_id INT64)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.categories` (id, name, parent_id) VALUES
            (1,'Root',NULL),(2,'Electronics',1),(3,'Computers',2),(4,'Laptops',3),
            (5,'Phones',2),(6,'Furniture',1),(7,'Desks',6),(8,'Chairs',6)", parameters: null);
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

    // Basic CTE
    [Fact] public async Task Cte_Basic()
    {
        var rows = await Query("WITH top_cats AS (SELECT id, name FROM `{ds}.categories` WHERE parent_id IS NULL) SELECT * FROM top_cats");
        Assert.Single(rows);
        Assert.Equal("Root", rows[0]["name"]?.ToString());
    }

    // Multiple CTEs
    [Fact] public async Task Cte_Multiple()
    {
        var rows = await Query("WITH roots AS (SELECT id FROM `{ds}.categories` WHERE parent_id IS NULL), children AS (SELECT c.name FROM `{ds}.categories` c JOIN roots r ON c.parent_id = r.id) SELECT * FROM children ORDER BY name");
        Assert.Equal(2, rows.Count);
    }

    // CTE with aggregation
    [Fact] public async Task Cte_Aggregate()
    {
        var rows = await Query("WITH child_counts AS (SELECT parent_id, COUNT(*) as cnt FROM `{ds}.categories` WHERE parent_id IS NOT NULL GROUP BY parent_id) SELECT c.name, cc.cnt FROM `{ds}.categories` c JOIN child_counts cc ON c.id = cc.parent_id ORDER BY cc.cnt DESC");
        Assert.True(rows.Count >= 1);
    }

    // CTE referenced multiple times
    [Fact] public async Task Cte_MultipleReferences()
    {
        var result = await Scalar("WITH cats AS (SELECT id, name, parent_id FROM `{ds}.categories`) SELECT COUNT(*) FROM cats a JOIN cats b ON a.parent_id = b.id");
        Assert.NotNull(result);
        Assert.True(int.Parse(result!) > 0);
    }

    // Chained CTEs
    [Fact] public async Task Cte_Chained()
    {
        var rows = await Query("WITH level1 AS (SELECT id, name FROM `{ds}.categories` WHERE parent_id = 1), level2 AS (SELECT c.id, c.name FROM `{ds}.categories` c JOIN level1 l ON c.parent_id = l.id) SELECT name FROM level2 ORDER BY name");
        Assert.True(rows.Count >= 2);
    }

    // CTE with window function
    [Fact] public async Task Cte_Window()
    {
        var rows = await Query("WITH ranked AS (SELECT name, parent_id, ROW_NUMBER() OVER (PARTITION BY parent_id ORDER BY name) as rn FROM `{ds}.categories` WHERE parent_id IS NOT NULL) SELECT name, rn FROM ranked ORDER BY parent_id, rn");
        Assert.True(rows.Count >= 1);
        Assert.Equal("1", rows[0]["rn"]?.ToString());
    }

    // CTE with UNION
    [Fact] public async Task Cte_Union()
    {
        var result = await Scalar("WITH all_names AS (SELECT name FROM `{ds}.categories` WHERE parent_id IS NULL UNION ALL SELECT name FROM `{ds}.categories` WHERE parent_id = 1) SELECT COUNT(*) FROM all_names");
        Assert.Equal("3", result);
    }

    // CTE in subquery
    [Fact] public async Task Cte_InSubquery()
    {
        var rows = await Query("WITH leaf_cats AS (SELECT c.id FROM `{ds}.categories` c LEFT JOIN `{ds}.categories` child ON c.id = child.parent_id WHERE child.id IS NULL) SELECT c.name FROM `{ds}.categories` c WHERE c.id IN (SELECT id FROM leaf_cats) ORDER BY c.name");
        Assert.True(rows.Count >= 3);
    }
}