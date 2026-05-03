using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DmlDeepTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public DmlDeepTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_dml_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.t` (id INT64, name STRING, val INT64)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.t` (id, name, val) VALUES (1,'a',10),(2,'b',20),(3,'c',30),(4,'d',40),(5,'e',50)", parameters: null);
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

    private async Task Execute(string sql)
    {
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
    }

    // INSERT
    [Fact] public async Task Insert_Single()
    {
        await Execute("INSERT INTO `{ds}.t` (id, name, val) VALUES (6, 'f', 60)");
        Assert.Equal("6", await Scalar("SELECT COUNT(*) FROM `{ds}.t`"));
    }
    [Fact] public async Task Insert_Multiple()
    {
        await Execute("INSERT INTO `{ds}.t` (id, name, val) VALUES (6,'f',60),(7,'g',70)");
        Assert.Equal("7", await Scalar("SELECT COUNT(*) FROM `{ds}.t`"));
    }
    [Fact] public async Task Insert_FromSelect()
    {
        await Execute("INSERT INTO `{ds}.t` (id, name, val) SELECT id + 10, name, val * 2 FROM `{ds}.t` WHERE val > 30");
        Assert.Equal("7", await Scalar("SELECT COUNT(*) FROM `{ds}.t`"));
    }
    [Fact] public async Task Insert_NullValue()
    {
        await Execute("INSERT INTO `{ds}.t` (id, name, val) VALUES (6, NULL, NULL)");
        Assert.Equal("1", await Scalar("SELECT COUNT(*) FROM `{ds}.t` WHERE name IS NULL"));
    }

    // UPDATE
    [Fact] public async Task Update_Single()
    {
        await Execute("UPDATE `{ds}.t` SET val = 99 WHERE id = 1");
        Assert.Equal("99", await Scalar("SELECT val FROM `{ds}.t` WHERE id = 1"));
    }
    [Fact] public async Task Update_Multiple()
    {
        await Execute("UPDATE `{ds}.t` SET val = val * 2 WHERE val > 30");
        Assert.Equal("80", await Scalar("SELECT val FROM `{ds}.t` WHERE id = 4"));
    }
    [Fact] public async Task Update_All()
    {
        await Execute("UPDATE `{ds}.t` SET name = UPPER(name) WHERE TRUE");
        Assert.Equal("A", await Scalar("SELECT name FROM `{ds}.t` WHERE id = 1"));
    }
    [Fact] public async Task Update_SetNull()
    {
        await Execute("UPDATE `{ds}.t` SET val = NULL WHERE id = 1");
        Assert.Null(await Scalar("SELECT val FROM `{ds}.t` WHERE id = 1"));
    }
    [Fact] public async Task Update_Expression()
    {
        await Execute("UPDATE `{ds}.t` SET val = val + 5, name = CONCAT(name, '_updated') WHERE id = 1");
        Assert.Equal("15", await Scalar("SELECT val FROM `{ds}.t` WHERE id = 1"));
        Assert.Equal("a_updated", await Scalar("SELECT name FROM `{ds}.t` WHERE id = 1"));
    }

    // DELETE
    [Fact] public async Task Delete_Single()
    {
        await Execute("DELETE FROM `{ds}.t` WHERE id = 1");
        Assert.Equal("4", await Scalar("SELECT COUNT(*) FROM `{ds}.t`"));
    }
    [Fact] public async Task Delete_Multiple()
    {
        await Execute("DELETE FROM `{ds}.t` WHERE val > 30");
        Assert.Equal("3", await Scalar("SELECT COUNT(*) FROM `{ds}.t`"));
    }
    [Fact] public async Task Delete_All()
    {
        await Execute("DELETE FROM `{ds}.t` WHERE TRUE");
        Assert.Equal("0", await Scalar("SELECT COUNT(*) FROM `{ds}.t`"));
    }
    [Fact] public async Task Delete_NoMatch()
    {
        await Execute("DELETE FROM `{ds}.t` WHERE val > 999");
        Assert.Equal("5", await Scalar("SELECT COUNT(*) FROM `{ds}.t`"));
    }

    // MERGE
    [Fact] public async Task Merge_InsertWhenNotMatched()
    {
        await Execute("CREATE TABLE `{ds}.src` (id INT64, name STRING, val INT64)");
        await Execute("INSERT INTO `{ds}.src` (id, name, val) VALUES (6,'f',60),(1,'a',99)");
        await Execute("MERGE `{ds}.t` t USING `{ds}.src` s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id, name, val) VALUES (s.id, s.name, s.val)");
        Assert.Equal("6", await Scalar("SELECT COUNT(*) FROM `{ds}.t`"));
    }
    [Fact] public async Task Merge_UpdateWhenMatched()
    {
        await Execute("CREATE TABLE `{ds}.src2` (id INT64, name STRING, val INT64)");
        await Execute("INSERT INTO `{ds}.src2` (id, name, val) VALUES (1,'a',99)");
        await Execute("MERGE `{ds}.t` t USING `{ds}.src2` s ON t.id = s.id WHEN MATCHED THEN UPDATE SET val = s.val");
        Assert.Equal("99", await Scalar("SELECT val FROM `{ds}.t` WHERE id = 1"));
    }
    [Fact] public async Task Merge_DeleteWhenMatched()
    {
        await Execute("CREATE TABLE `{ds}.src3` (id INT64, name STRING, val INT64)");
        await Execute("INSERT INTO `{ds}.src3` (id, name, val) VALUES (1,'a',10)");
        await Execute("MERGE `{ds}.t` t USING `{ds}.src3` s ON t.id = s.id WHEN MATCHED THEN DELETE");
        Assert.Equal("4", await Scalar("SELECT COUNT(*) FROM `{ds}.t`"));
    }

    // DDL
    [Fact] public async Task CreateTable_IfNotExists()
    {
        await Execute("CREATE TABLE IF NOT EXISTS `{ds}.t` (id INT64, name STRING, val INT64)");
        Assert.Equal("5", await Scalar("SELECT COUNT(*) FROM `{ds}.t`"));
    }
    [Fact] public async Task DropTable()
    {
        await Execute("CREATE TABLE `{ds}.temp` (x INT64)");
        await Execute("DROP TABLE `{ds}.temp`");
        // just shouldn't throw
    }
    [Fact] public async Task DropTable_IfExists()
    {
        await Execute("DROP TABLE IF EXISTS `{ds}.nonexistent`");
        // shouldn't throw
    }
}