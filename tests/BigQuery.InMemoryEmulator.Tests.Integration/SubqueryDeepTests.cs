using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class SubqueryDeepTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public SubqueryDeepTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_sq2_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.emp` (id INT64, name STRING, dept STRING, salary INT64, mgr_id INT64)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.emp` (id, name, dept, salary, mgr_id) VALUES
            (1,'Alice','eng',100000,NULL),(2,'Bob','eng',90000,1),(3,'Carol','sales',80000,1),
            (4,'Dave','eng',95000,1),(5,'Eve','sales',85000,3),(6,'Frank','hr',70000,1),
            (7,'Grace','hr',75000,6),(8,'Heidi','eng',110000,1)", parameters: null);
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

    // Scalar subquery in SELECT
    [Fact] public async Task ScalarSubquery_InSelect()
    {
        var rows = await Query("SELECT name, salary, (SELECT AVG(salary) FROM `{ds}.emp`) as avg_sal FROM `{ds}.emp` WHERE id = 1");
        Assert.Single(rows);
        Assert.NotNull(rows[0]["avg_sal"]);
    }

    // Subquery in WHERE with comparison
    [Fact] public async Task Subquery_InWhere_Comparison()
    {
        var rows = await Query("SELECT name FROM `{ds}.emp` WHERE salary > (SELECT AVG(salary) FROM `{ds}.emp`) ORDER BY name");
        Assert.True(rows.Count > 0);
    }

    // EXISTS subquery
    [Fact] public async Task Exists_True()
    {
        var rows = await Query("SELECT name FROM `{ds}.emp` e WHERE EXISTS (SELECT 1 FROM `{ds}.emp` e2 WHERE e2.mgr_id = e.id) ORDER BY name");
        Assert.True(rows.Count > 0); // managers
    }
    [Fact] public async Task NotExists()
    {
        var rows = await Query("SELECT name FROM `{ds}.emp` WHERE NOT EXISTS (SELECT 1 FROM `{ds}.emp` e2 WHERE e2.mgr_id = `{ds}.emp`.id) ORDER BY name");
        Assert.True(rows.Count > 0); // non-managers
    }

    // IN subquery
    [Fact] public async Task In_Subquery()
    {
        var rows = await Query("SELECT name FROM `{ds}.emp` WHERE dept IN (SELECT dept FROM `{ds}.emp` WHERE salary > 100000) ORDER BY name");
        Assert.True(rows.Count > 0);
    }
    [Fact] public async Task NotIn_Subquery()
    {
        var rows = await Query("SELECT name FROM `{ds}.emp` WHERE dept NOT IN (SELECT DISTINCT dept FROM `{ds}.emp` WHERE salary < 75000) ORDER BY name");
        Assert.True(rows.Count > 0);
    }

    // Derived table (FROM subquery)
    [Fact] public async Task DerivedTable_Basic()
    {
        var rows = await Query("SELECT dept, total FROM (SELECT dept, CAST(SUM(salary) AS INT64) as total FROM `{ds}.emp` GROUP BY dept) ORDER BY total DESC");
        Assert.Equal(3, rows.Count);
    }
    [Fact] public async Task DerivedTable_WithJoin()
    {
        var rows = await Query("SELECT e.name, d.avg_sal FROM `{ds}.emp` e JOIN (SELECT dept, CAST(AVG(salary) AS INT64) as avg_sal FROM `{ds}.emp` GROUP BY dept) d ON e.dept = d.dept WHERE e.id <= 3 ORDER BY e.name");
        Assert.Equal(3, rows.Count);
    }

    // Correlated subquery
    [Fact] public async Task CorrelatedSubquery_InSelect()
    {
        var rows = await Query("SELECT name, (SELECT COUNT(*) FROM `{ds}.emp` e2 WHERE e2.mgr_id = e.id) as report_count FROM `{ds}.emp` e WHERE id = 1");
        Assert.Single(rows);
        var cnt = int.Parse(rows[0]["report_count"]!.ToString()!);
        Assert.True(cnt > 0);
    }

    // Nested subqueries
    [Fact] public async Task NestedSubquery()
    {
        var result = await Scalar("SELECT COUNT(*) FROM `{ds}.emp` WHERE salary > (SELECT AVG(salary) FROM (SELECT salary FROM `{ds}.emp` WHERE dept = 'eng'))");
        Assert.NotNull(result);
    }

    // Subquery returning multiple rows in IN
    [Fact] public async Task Subquery_MultiRow_In()
    {
        var rows = await Query("SELECT name FROM `{ds}.emp` WHERE id IN (SELECT mgr_id FROM `{ds}.emp` WHERE mgr_id IS NOT NULL) ORDER BY name");
        Assert.True(rows.Count > 0);
    }

    // Subquery with aggregation
    [Fact] public async Task Subquery_Agg()
    {
        var result = await Scalar("SELECT MAX(total) FROM (SELECT dept, SUM(salary) as total FROM `{ds}.emp` GROUP BY dept)");
        Assert.NotNull(result);
    }

    // Subquery in CASE
    [Fact] public async Task Subquery_InCase()
    {
        var rows = await Query("SELECT name, CASE WHEN salary > (SELECT AVG(salary) FROM `{ds}.emp`) THEN 'above' ELSE 'below' END as level FROM `{ds}.emp` ORDER BY name");
        Assert.Equal(8, rows.Count);
    }

    // Scalar subquery returning NULL
    [Fact] public async Task ScalarSubquery_Null()
    {
        var result = await Scalar("SELECT (SELECT name FROM `{ds}.emp` WHERE id = 999)");
        Assert.Null(result);
    }
}