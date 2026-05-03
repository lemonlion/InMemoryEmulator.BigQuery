using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class InformationSchemaTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public InformationSchemaTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_is2_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.users` (id INT64, name STRING, email STRING)", parameters: null);
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.orders` (id INT64, user_id INT64, total FLOAT64)", parameters: null);
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

    [Fact] public async Task Tables_ListAll()
    {
        var rows = await Query("SELECT table_name FROM `{ds}.INFORMATION_SCHEMA.TABLES` ORDER BY table_name");
        Assert.Equal(2, rows.Count);
        Assert.Equal("orders", rows[0]["table_name"]?.ToString());
        Assert.Equal("users", rows[1]["table_name"]?.ToString());
    }

    [Fact] public async Task Tables_TableType()
    {
        var rows = await Query("SELECT table_name, table_type FROM `{ds}.INFORMATION_SCHEMA.TABLES`");
        Assert.All(rows, r => Assert.Equal("BASE TABLE", r["table_type"]?.ToString()));
    }

    [Fact] public async Task Columns_ListAll()
    {
        var rows = await Query("SELECT column_name FROM `{ds}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'users' ORDER BY ordinal_position");
        Assert.Equal(3, rows.Count);
        Assert.Equal("id", rows[0]["column_name"]?.ToString());
        Assert.Equal("name", rows[1]["column_name"]?.ToString());
        Assert.Equal("email", rows[2]["column_name"]?.ToString());
    }

    [Fact] public async Task Columns_DataType()
    {
        var rows = await Query("SELECT column_name, data_type FROM `{ds}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'users' ORDER BY ordinal_position");
        Assert.Equal("INT64", rows[0]["data_type"]?.ToString());
        Assert.Equal("STRING", rows[1]["data_type"]?.ToString());
    }

    [Fact] public async Task Columns_OtherTable()
    {
        var rows = await Query("SELECT column_name FROM `{ds}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'orders' ORDER BY ordinal_position");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task Tables_Count()
    {
        var result = await Scalar("SELECT COUNT(*) FROM `{ds}.INFORMATION_SCHEMA.TABLES`");
        Assert.Equal("2", result);
    }

    [Fact] public async Task Columns_Count()
    {
        var result = await Scalar("SELECT COUNT(*) FROM `{ds}.INFORMATION_SCHEMA.COLUMNS`");
        Assert.Equal("6", result);
    }
}