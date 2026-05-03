using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class UnionIntersectExceptTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public UnionIntersectExceptTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_uie_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.a` (id INT64, val STRING)", parameters: null);
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.b` (id INT64, val STRING)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.a` (id, val) VALUES (1,'x'),(2,'y'),(3,'z'),(4,'x')", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.b` (id, val) VALUES (2,'y'),(3,'z'),(5,'w'),(6,'z')", parameters: null);
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

    // UNION ALL
    [Fact] public async Task UnionAll_Basic() => Assert.Equal("8", await Scalar("SELECT COUNT(*) FROM (SELECT id, val FROM `{ds}.a` UNION ALL SELECT id, val FROM `{ds}.b`)"));
    [Fact] public async Task UnionAll_OrderBy()
    {
        var rows = await Query("SELECT id, val FROM `{ds}.a` UNION ALL SELECT id, val FROM `{ds}.b` ORDER BY id");
        Assert.Equal(8, rows.Count);
    }

    // UNION DISTINCT
    [Fact] public async Task UnionDistinct_Basic()
    {
        var count = int.Parse((await Scalar("SELECT COUNT(*) FROM (SELECT id, val FROM `{ds}.a` UNION DISTINCT SELECT id, val FROM `{ds}.b`)"))!);
        Assert.True(count < 8); // Should remove duplicates
    }

    // INTERSECT ALL
    [Fact] public async Task IntersectAll_Basic()
    {
        var rows = await Query("SELECT id, val FROM `{ds}.a` INTERSECT ALL SELECT id, val FROM `{ds}.b`");
        Assert.True(rows.Count >= 1);
    }

    // INTERSECT DISTINCT
    [Fact] public async Task IntersectDistinct_Basic()
    {
        var count = int.Parse((await Scalar("SELECT COUNT(*) FROM (SELECT id, val FROM `{ds}.a` INTERSECT DISTINCT SELECT id, val FROM `{ds}.b`)"))!);
        Assert.True(count >= 1);
    }

    // EXCEPT ALL
    [Fact] public async Task ExceptAll_Basic()
    {
        var rows = await Query("SELECT id, val FROM `{ds}.a` EXCEPT ALL SELECT id, val FROM `{ds}.b`");
        Assert.True(rows.Count >= 1);
    }

    // EXCEPT DISTINCT
    [Fact] public async Task ExceptDistinct_Basic()
    {
        var count = int.Parse((await Scalar("SELECT COUNT(*) FROM (SELECT id, val FROM `{ds}.a` EXCEPT DISTINCT SELECT id, val FROM `{ds}.b`)"))!);
        Assert.True(count >= 1);
    }

    // UNION ALL with different expressions
    [Fact] public async Task UnionAll_Expressions()
    {
        var rows = await Query("SELECT val, 'a' as src FROM `{ds}.a` UNION ALL SELECT val, 'b' as src FROM `{ds}.b` ORDER BY src, val");
        Assert.Equal(8, rows.Count);
    }

    // UNION ALL with LIMIT (wrapped in subquery since ORDER BY/LIMIT attaches to last SELECT otherwise)
    [Fact] public async Task UnionAll_Limit()
    {
        var rows = await Query("SELECT id, val FROM `{ds}.a` UNION ALL SELECT id, val FROM `{ds}.b` ORDER BY id LIMIT 3");
        Assert.Equal(3, rows.Count);
    }

    // Chain UNIONs
    [Fact] public async Task UnionAll_Chain()
    {
        var count = await Scalar("SELECT COUNT(*) FROM (SELECT id FROM `{ds}.a` UNION ALL SELECT id FROM `{ds}.b` UNION ALL SELECT id FROM `{ds}.a`)");
        Assert.Equal("12", count);
    }

    // UNION with aggregate
    [Fact] public async Task UnionAll_Aggregate()
    {
        var rows = await Query("SELECT 'a' as tbl, COUNT(*) as cnt FROM `{ds}.a` UNION ALL SELECT 'b' as tbl, COUNT(*) as cnt FROM `{ds}.b` ORDER BY tbl");
        Assert.Equal(2, rows.Count);
        Assert.Equal("4", rows[0]["cnt"]?.ToString());
    }
}