using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class SetOperationDeepTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public SetOperationDeepTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_set2_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.t1` (id INT64, val STRING)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.t1` (id, val) VALUES (1,'A'),(2,'B'),(3,'C'),(4,'A')", parameters: null);
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.t2` (id INT64, val STRING)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.t2` (id, val) VALUES (3,'C'),(4,'A'),(5,'D'),(6,'E')", parameters: null);
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

    // UNION ALL
    [Fact] public async Task UnionAll_Basic()
    {
        var rows = await Query("SELECT id, val FROM `{ds}.t1` UNION ALL SELECT id, val FROM `{ds}.t2`  ");
        Assert.Equal(8, rows.Count);
    }
    [Fact] public async Task UnionAll_PreservesDuplicates()
    {
        var result = await Scalar("SELECT COUNT(*) FROM (SELECT val FROM `{ds}.t1` UNION ALL SELECT val FROM `{ds}.t2`)");
        Assert.Equal("8", result);
    }

    // UNION DISTINCT
    [Fact] public async Task UnionDistinct_RemovesDuplicates()
    {
        var rows = await Query("SELECT val FROM `{ds}.t1` UNION DISTINCT SELECT val FROM `{ds}.t2`  ");
        Assert.Equal(5, rows.Count); // A, B, C, D, E
    }

    // INTERSECT ALL
    [Fact] public async Task IntersectAll_Basic()
    {
        var rows = await Query("SELECT id, val FROM `{ds}.t1` INTERSECT ALL SELECT id, val FROM `{ds}.t2`  ");
        Assert.True(rows.Count >= 2); // overlap rows
    }

    // INTERSECT DISTINCT
    [Fact] public async Task IntersectDistinct_Basic()
    {
        var rows = await Query("SELECT val FROM `{ds}.t1` INTERSECT DISTINCT SELECT val FROM `{ds}.t2`  ");
        Assert.Equal(2, rows.Count); // A, C
    }

    // EXCEPT ALL
    [Fact] public async Task ExceptAll_Basic()
    {
        var rows = await Query("SELECT id, val FROM `{ds}.t1` EXCEPT ALL SELECT id, val FROM `{ds}.t2`  ");
        Assert.True(rows.Count >= 2); // t1 minus t2
    }

    // EXCEPT DISTINCT
    [Fact] public async Task ExceptDistinct_Basic()
    {
        var rows = await Query("SELECT val FROM `{ds}.t1` EXCEPT DISTINCT SELECT val FROM `{ds}.t2`  ");
        Assert.Single(rows); // B
    }

    // Multiple UNION ALL
    [Fact] public async Task UnionAll_Three()
    {
        var rows = await Query("SELECT val FROM `{ds}.t1` UNION ALL SELECT val FROM `{ds}.t2` UNION ALL SELECT val FROM `{ds}.t1`  ");
        Assert.Equal(12, rows.Count);
    }

    // UNION ALL with different expressions
    [Fact] public async Task UnionAll_Expressions()
    {
        var rows = await Query("SELECT id * 2 as x FROM `{ds}.t1` UNION ALL SELECT id + 10 as x FROM `{ds}.t2`  ");
        Assert.Equal(8, rows.Count);
    }

    // UNION with WHERE
    [Fact] public async Task UnionAll_WithWhere()
    {
        var rows = await Query("SELECT id, val FROM `{ds}.t1` WHERE id > 2 UNION ALL SELECT id, val FROM `{ds}.t2` WHERE id < 5  ");
        Assert.Equal(4, rows.Count);
    }

    // UNION in subquery
    [Fact] public async Task Union_InSubquery()
    {
        var result = await Scalar("SELECT COUNT(*) FROM (SELECT val FROM `{ds}.t1` UNION ALL SELECT val FROM `{ds}.t2`)");
        Assert.Equal("8", result);
    }

    // UNION with ORDER BY (on outer)
    [Fact] public async Task Union_WithOrderBy()
    {
        var rows = await Query("SELECT * FROM (SELECT id, val FROM `{ds}.t1` UNION ALL SELECT id, val FROM `{ds}.t2`) ORDER BY id  ");
        Assert.Equal(8, rows.Count);
        Assert.Equal("1", rows[0]["id"]?.ToString());
    }

    // UNION with LIMIT (via subquery)
    [Fact] public async Task Union_WithLimit()
    {
        var rows = await Query("SELECT * FROM (SELECT id, val FROM `{ds}.t1` UNION ALL SELECT id, val FROM `{ds}.t2`) ORDER BY id LIMIT 3  ");
        Assert.Equal(3, rows.Count);
    }

    // NULL handling in UNION
    [Fact] public async Task UnionAll_WithNulls()
    {
        var rows = await Query("SELECT CAST(NULL AS STRING) as val UNION ALL SELECT 'hello' as val  ");
        Assert.Equal(2, rows.Count);
    }
}