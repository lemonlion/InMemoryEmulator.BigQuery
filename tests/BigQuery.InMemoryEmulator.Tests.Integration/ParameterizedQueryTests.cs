using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ParameterizedQueryTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public ParameterizedQueryTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_prm_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.items` (id INT64, name STRING, price FLOAT64, active BOOL)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.items` (id, name, price, active) VALUES
            (1,'Widget',10.0,TRUE),(2,'Gadget',20.0,FALSE),(3,'Doohickey',30.0,TRUE),(4,'Thingamajig',40.0,TRUE),(5,'Gizmo',50.0,FALSE)", parameters: null);
    }

    public async ValueTask DisposeAsync()
    {
        try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
        await _fixture.DisposeAsync();
    }

    private async Task<List<BigQueryRow>> Query(string sql, IEnumerable<BigQueryParameter> parameters)
    {
        var client = await _fixture.GetClientAsync();
        var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters);
        return result.ToList();
    }

    private async Task<string?> Scalar(string sql, IEnumerable<BigQueryParameter> parameters)
    {
        var rows = await Query(sql, parameters);
        return rows.Count > 0 ? rows[0][0]?.ToString() : null;
    }

    // String parameter
    [Fact] public async Task Param_String()
    {
        var result = await Scalar("SELECT name FROM `{ds}.items` WHERE name = @name",
            new[] { new BigQueryParameter("name", BigQueryDbType.String, "Widget") });
        Assert.Equal("Widget", result);
    }

    // Int parameter
    [Fact] public async Task Param_Int()
    {
        var result = await Scalar("SELECT name FROM `{ds}.items` WHERE id = @id",
            new[] { new BigQueryParameter("id", BigQueryDbType.Int64, 3) });
        Assert.Equal("Doohickey", result);
    }

    // Float parameter
    [Fact] public async Task Param_Float()
    {
        var result = await Scalar("SELECT name FROM `{ds}.items` WHERE price > @minPrice ORDER BY price LIMIT 1",
            new[] { new BigQueryParameter("minPrice", BigQueryDbType.Float64, 25.0) });
        Assert.Equal("Doohickey", result);
    }

    // Bool parameter
    [Fact] public async Task Param_Bool()
    {
        var rows = await Query("SELECT name FROM `{ds}.items` WHERE active = @active ORDER BY name",
            new[] { new BigQueryParameter("active", BigQueryDbType.Bool, true) });
        Assert.Equal(3, rows.Count);
    }

    // Multiple parameters
    [Fact] public async Task Param_Multiple()
    {
        var rows = await Query("SELECT name FROM `{ds}.items` WHERE price >= @min AND price <= @max ORDER BY price",
            new[] {
                new BigQueryParameter("min", BigQueryDbType.Float64, 20.0),
                new BigQueryParameter("max", BigQueryDbType.Float64, 40.0)
            });
        Assert.Equal(3, rows.Count);
    }

    // Parameter in expression
    [Fact] public async Task Param_InExpression()
    {
        var result = await Scalar("SELECT CAST(price * @multiplier AS INT64) FROM `{ds}.items` WHERE id = 1",
            new[] { new BigQueryParameter("multiplier", BigQueryDbType.Float64, 2.0) });
        Assert.Equal("20", result);
    }

    // NULL parameter
    [Fact] public async Task Param_Null()
    {
        var rows = await Query("SELECT name FROM `{ds}.items` WHERE @val IS NULL",
            new[] { new BigQueryParameter("val", BigQueryDbType.String, null) });
        Assert.Equal(5, rows.Count);
    }

    // Parameter in SELECT
    [Fact] public async Task Param_InSelect()
    {
        var result = await Scalar("SELECT @greeting",
            new[] { new BigQueryParameter("greeting", BigQueryDbType.String, "Hello World") });
        Assert.Equal("Hello World", result);
    }

    // Parameter comparison operators
    [Fact] public async Task Param_LessThan()
    {
        var rows = await Query("SELECT name FROM `{ds}.items` WHERE price < @max ORDER BY price",
            new[] { new BigQueryParameter("max", BigQueryDbType.Float64, 30.0) });
        Assert.Equal(2, rows.Count);
    }

    // Parameter with LIKE
    [Fact] public async Task Param_Like()
    {
        var rows = await Query("SELECT name FROM `{ds}.items` WHERE name LIKE @pattern",
            new[] { new BigQueryParameter("pattern", BigQueryDbType.String, "G%") });
        Assert.Equal(2, rows.Count); // Gadget, Gizmo
    }

    // Parameter with IN (not directly supported as array in all implementations, use scalar)
    [Fact] public async Task Param_InWhere()
    {
        var result = await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE id = @id1 OR id = @id2",
            new[] {
                new BigQueryParameter("id1", BigQueryDbType.Int64, 1),
                new BigQueryParameter("id2", BigQueryDbType.Int64, 3)
            });
        Assert.Equal("2", result);
    }

    // Parameter with aggregation
    [Fact] public async Task Param_WithAgg()
    {
        var result = await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE price > @threshold",
            new[] { new BigQueryParameter("threshold", BigQueryDbType.Float64, 35.0) });
        Assert.Equal("2", result);
    }
}