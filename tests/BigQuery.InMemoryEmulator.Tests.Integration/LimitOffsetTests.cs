using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class LimitOffsetTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public LimitOffsetTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_lim_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.nums` (id INT64, val INT64)", parameters: null);
        var sql = $"INSERT INTO `{_datasetId}.nums` (id, val) VALUES ";
        var vals = string.Join(",", Enumerable.Range(1, 20).Select(i => $"({i}, {i * 10})"));
        await client.ExecuteQueryAsync(sql + vals, parameters: null);
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

    // LIMIT
    [Fact] public async Task Limit_Basic()
    {
        var rows = await Query("SELECT id FROM `{ds}.nums` ORDER BY id LIMIT 5");
        Assert.Equal(5, rows.Count);
    }
    [Fact] public async Task Limit_One()
    {
        var rows = await Query("SELECT id FROM `{ds}.nums` ORDER BY id LIMIT 1");
        Assert.Single(rows);
        Assert.Equal("1", rows[0]["id"]?.ToString());
    }
    [Fact] public async Task Limit_Zero()
    {
        var rows = await Query("SELECT id FROM `{ds}.nums` ORDER BY id LIMIT 0");
        Assert.Empty(rows);
    }
    [Fact] public async Task Limit_MoreThanRows()
    {
        var rows = await Query("SELECT id FROM `{ds}.nums` LIMIT 100");
        Assert.Equal(20, rows.Count);
    }

    // OFFSET
    [Fact] public async Task Offset_Basic()
    {
        var rows = await Query("SELECT id FROM `{ds}.nums` ORDER BY id LIMIT 5 OFFSET 5");
        Assert.Equal(5, rows.Count);
        Assert.Equal("6", rows[0]["id"]?.ToString());
    }
    [Fact] public async Task Offset_Last()
    {
        var rows = await Query("SELECT id FROM `{ds}.nums` ORDER BY id LIMIT 5 OFFSET 15");
        Assert.Equal(5, rows.Count);
        Assert.Equal("16", rows[0]["id"]?.ToString());
    }
    [Fact] public async Task Offset_BeyondEnd()
    {
        var rows = await Query("SELECT id FROM `{ds}.nums` ORDER BY id LIMIT 5 OFFSET 25");
        Assert.Empty(rows);
    }
    [Fact] public async Task Offset_Zero()
    {
        var rows = await Query("SELECT id FROM `{ds}.nums` ORDER BY id LIMIT 3 OFFSET 0");
        Assert.Equal(3, rows.Count);
        Assert.Equal("1", rows[0]["id"]?.ToString());
    }

    // Combined with ORDER BY
    [Fact] public async Task LimitOffset_Desc()
    {
        var rows = await Query("SELECT id FROM `{ds}.nums` ORDER BY id DESC LIMIT 3");
        Assert.Equal("20", rows[0]["id"]?.ToString());
    }
    [Fact] public async Task LimitOffset_DescOffset()
    {
        var rows = await Query("SELECT id FROM `{ds}.nums` ORDER BY id DESC LIMIT 3 OFFSET 2");
        Assert.Equal("18", rows[0]["id"]?.ToString());
    }

    // With WHERE
    [Fact] public async Task Limit_WithWhere()
    {
        var rows = await Query("SELECT id FROM `{ds}.nums` WHERE val > 100 ORDER BY id LIMIT 3");
        Assert.Equal(3, rows.Count);
        Assert.Equal("11", rows[0]["id"]?.ToString());
    }

    // With aggregation
    [Fact] public async Task Limit_AfterGroupBy()
    {
        var rows = await Query("SELECT val, COUNT(*) as cnt FROM `{ds}.nums` GROUP BY val ORDER BY val LIMIT 3");
        Assert.Equal(3, rows.Count);
    }

    // Subquery with LIMIT
    [Fact] public async Task Limit_InSubquery()
    {
        var result = await Scalar("SELECT COUNT(*) FROM (SELECT id FROM `{ds}.nums` ORDER BY id LIMIT 7)");
        Assert.Equal("7", result);
    }

    // LIMIT with expressions (not standard but test scalar)
    [Fact] public async Task Limit_LargeDataset()
    {
        var rows = await Query("SELECT id FROM `{ds}.nums` ORDER BY id LIMIT 10 OFFSET 10");
        Assert.Equal(10, rows.Count);
        Assert.Equal("11", rows[0]["id"]?.ToString());
        Assert.Equal("20", rows[9]["id"]?.ToString());
    }
}