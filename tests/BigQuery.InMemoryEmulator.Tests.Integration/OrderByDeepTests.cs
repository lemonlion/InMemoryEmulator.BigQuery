using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class OrderByDeepTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public OrderByDeepTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_ob2_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.data` (id INT64, name STRING, score INT64, grade STRING)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.data` (id, name, score, grade) VALUES
            (1,'Alice',90,'A'),(2,'Bob',85,'B'),(3,'Carol',90,'A'),(4,'Dave',70,'C'),
            (5,'Eve',85,'B'),(6,'Frank',NULL,'D'),(7,'Grace',95,'A'),(8,'Heidi',80,NULL)", parameters: null);
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

    [Fact] public async Task OrderBy_Asc()
    {
        var rows = await Query("SELECT name FROM `{ds}.data` WHERE score IS NOT NULL ORDER BY score ASC");
        Assert.Equal("Dave", rows[0]["name"]?.ToString());
    }
    [Fact] public async Task OrderBy_Desc()
    {
        var rows = await Query("SELECT name FROM `{ds}.data` WHERE score IS NOT NULL ORDER BY score DESC");
        Assert.Equal("Grace", rows[0]["name"]?.ToString());
    }
    [Fact] public async Task OrderBy_MultiColumn()
    {
        var rows = await Query("SELECT name, score FROM `{ds}.data` WHERE score IS NOT NULL ORDER BY score DESC, name ASC");
        Assert.Equal("Grace", rows[0]["name"]?.ToString());
    }
    [Fact] public async Task OrderBy_Alias()
    {
        var rows = await Query("SELECT name, score as s FROM `{ds}.data` WHERE score IS NOT NULL ORDER BY s DESC");
        Assert.Equal("Grace", rows[0]["name"]?.ToString());
    }
    [Fact] public async Task OrderBy_Ordinal()
    {
        var rows = await Query("SELECT name, score FROM `{ds}.data` WHERE score IS NOT NULL ORDER BY 2 DESC");
        Assert.Equal("Grace", rows[0]["name"]?.ToString());
    }
    [Fact] public async Task OrderBy_String_Asc()
    {
        var rows = await Query("SELECT name FROM `{ds}.data` ORDER BY name ASC");
        Assert.Equal("Alice", rows[0]["name"]?.ToString());
    }
    [Fact] public async Task OrderBy_String_Desc()
    {
        var rows = await Query("SELECT name FROM `{ds}.data` ORDER BY name DESC");
        Assert.Equal("Heidi", rows[0]["name"]?.ToString());
    }
    [Fact] public async Task OrderBy_Limit()
    {
        var rows = await Query("SELECT name FROM `{ds}.data` ORDER BY name LIMIT 3");
        Assert.Equal(3, rows.Count);
        Assert.Equal("Alice", rows[0]["name"]?.ToString());
    }
    [Fact] public async Task OrderBy_Offset()
    {
        var rows = await Query("SELECT name FROM `{ds}.data` ORDER BY name LIMIT 3 OFFSET 2");
        Assert.Equal(3, rows.Count);
        Assert.Equal("Carol", rows[0]["name"]?.ToString());
    }
    [Fact] public async Task OrderBy_Case()
    {
        var rows = await Query("SELECT name, grade FROM `{ds}.data` WHERE grade IS NOT NULL ORDER BY CASE grade WHEN 'A' THEN 1 WHEN 'B' THEN 2 WHEN 'C' THEN 3 ELSE 4 END");
        Assert.Equal("A", rows[0]["grade"]?.ToString());
    }
    [Fact] public async Task OrderBy_Function()
    {
        var rows = await Query("SELECT name FROM `{ds}.data` ORDER BY LENGTH(name) DESC, name");
        Assert.True(rows[0]["name"]?.ToString()!.Length >= 5);
    }
    [Fact] public async Task OrderBy_MixedDirection()
    {
        var rows = await Query("SELECT name, grade, score FROM `{ds}.data` WHERE grade IS NOT NULL AND score IS NOT NULL ORDER BY grade ASC, score DESC");
        Assert.Equal("A", rows[0]["grade"]?.ToString());
        Assert.Equal("95", rows[0]["score"]?.ToString());
    }
    [Fact] public async Task OrderBy_NullsLast()
    {
        var rows = await Query("SELECT name, score FROM `{ds}.data` ORDER BY score NULLS LAST");
        Assert.NotNull(rows[0]["score"]);
        Assert.Null(rows[^1]["score"]);
    }
    [Fact] public async Task OrderBy_NullsFirst()
    {
        var rows = await Query("SELECT name, score FROM `{ds}.data` ORDER BY score NULLS FIRST");
        Assert.Null(rows[0]["score"]);
    }
}