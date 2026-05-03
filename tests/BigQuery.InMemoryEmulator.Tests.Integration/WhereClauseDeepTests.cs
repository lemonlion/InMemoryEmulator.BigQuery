using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class WhereClauseDeepTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public WhereClauseDeepTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_whr_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.items` (id INT64, name STRING, category STRING, price FLOAT64, qty INT64, active BOOL)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.items` (id, name, category, price, qty, active) VALUES
            (1,'Widget','A',10.0,100,TRUE),
            (2,'Gadget','B',25.0,50,TRUE),
            (3,'Doohickey','A',15.0,75,FALSE),
            (4,'Thingamajig','C',50.0,20,TRUE),
            (5,'Gizmo','B',30.0,NULL,FALSE),
            (6,'Whatchamacallit','A',NULL,60,NULL),
            (7,'Doodad','C',45.0,30,TRUE),
            (8,'Contraption','B',35.0,40,FALSE)", parameters: null);
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

    [Fact] public async Task Where_Equal() => Assert.Equal("1", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE name = 'Widget'"));
    [Fact] public async Task Where_NotEqual() => Assert.Equal("7", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE name != 'Widget'"));
    [Fact] public async Task Where_LessThan() => Assert.Equal("2", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE price < 20 AND price IS NOT NULL"));
    [Fact] public async Task Where_GreaterThan() => Assert.Equal("4", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE price > 25 AND price IS NOT NULL"));
    [Fact] public async Task Where_LessOrEqual() => Assert.Equal("3", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE price <= 25 AND price IS NOT NULL"));
    [Fact] public async Task Where_GreaterOrEqual() => Assert.Equal("5", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE price >= 25 AND price IS NOT NULL"));
    [Fact] public async Task Where_BoolTrue() => Assert.Equal("4", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE active = TRUE"));
    [Fact] public async Task Where_BoolFalse() => Assert.Equal("3", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE active = FALSE"));
    [Fact] public async Task Where_And() => Assert.Equal("1", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE category = 'A' AND active = TRUE"));
    [Fact] public async Task Where_Or() => Assert.Equal("6", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE category = 'A' OR category = 'B'"));
    [Fact] public async Task Where_NotParen() => Assert.Equal("5", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE NOT (category = 'A')"));
    [Fact] public async Task Where_Between() => Assert.Equal("5", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE price BETWEEN 15 AND 45"));
    [Fact] public async Task Where_NotBetween() => Assert.Equal("2", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE price NOT BETWEEN 15 AND 45 AND price IS NOT NULL"));
    [Fact] public async Task Where_In() => Assert.Equal("5", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE category IN ('A', 'C')"));
    [Fact] public async Task Where_NotIn() => Assert.Equal("3", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE category NOT IN ('A', 'C')"));
    [Fact] public async Task Where_Like_Prefix() => Assert.Equal("2", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE name LIKE 'G%'"));
    [Fact] public async Task Where_Like_Suffix() => Assert.Equal("2", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE name LIKE '%et'"));
    [Fact] public async Task Where_Like_Contains() => Assert.Equal("2", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE name LIKE '%dg%'"));
    [Fact] public async Task Where_IsNull() => Assert.Equal("1", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE price IS NULL"));
    [Fact] public async Task Where_IsNotNull() => Assert.Equal("7", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE price IS NOT NULL"));
    [Fact] public async Task Where_Expression() => Assert.Equal("4", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE price * qty > 1000 AND price IS NOT NULL AND qty IS NOT NULL"));
    [Fact] public async Task Where_Function_Length() => Assert.Equal("3", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE LENGTH(name) > 10"));
    [Fact] public async Task Where_Function_Upper() => Assert.Equal("1", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE UPPER(name) = 'WIDGET'"));
    [Fact] public async Task Where_Subquery() => Assert.Equal("3", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE price > (SELECT AVG(price) FROM `{ds}.items` WHERE price IS NOT NULL)"));
    [Fact] public async Task Where_True() => Assert.Equal("8", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE TRUE"));
    [Fact] public async Task Where_False() => Assert.Equal("0", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE FALSE"));
    [Fact] public async Task Where_Coalesce() => Assert.Equal("8", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE COALESCE(price, 0) >= 0"));
    [Fact] public async Task Where_Case() => Assert.Equal("4", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE CASE WHEN price > 25 THEN TRUE ELSE FALSE END = TRUE"));
    [Fact] public async Task Where_NestedOr() => Assert.Equal("3", await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE (category = 'A' AND price > 10) OR (category = 'C' AND price > 40)"));
    [Fact] public async Task Where_Exists() => Assert.Equal("8", await Scalar("SELECT COUNT(*) FROM `{ds}.items` i WHERE EXISTS (SELECT 1 FROM `{ds}.items` WHERE category = i.category)"));
}