using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class JoinTypeTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public JoinTypeTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_jt2_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.emp` (id INT64, name STRING, dept_id INT64)", parameters: null);
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.dept` (id INT64, dept_name STRING)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.emp` (id, name, dept_id) VALUES (1,'Alice',1),(2,'Bob',2),(3,'Carol',1),(4,'Dave',3),(5,'Eve',NULL)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.dept` (id, dept_name) VALUES (1,'Engineering'),(2,'Marketing'),(4,'Sales')", parameters: null);
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

    // INNER JOIN
    [Fact] public async Task InnerJoin_Basic()
    {
        var rows = await Query("SELECT e.name, d.dept_name FROM `{ds}.emp` e INNER JOIN `{ds}.dept` d ON e.dept_id = d.id ORDER BY e.name");
        Assert.Equal(3, rows.Count); // Alice, Bob, Carol
    }
    [Fact] public async Task InnerJoin_FirstMatch()
    {
        var rows = await Query("SELECT e.name, d.dept_name FROM `{ds}.emp` e INNER JOIN `{ds}.dept` d ON e.dept_id = d.id ORDER BY e.name");
        Assert.Equal("Alice", rows[0]["name"]?.ToString());
        Assert.Equal("Engineering", rows[0]["dept_name"]?.ToString());
    }

    // LEFT JOIN
    [Fact] public async Task LeftJoin_Basic()
    {
        var rows = await Query("SELECT e.name, d.dept_name FROM `{ds}.emp` e LEFT JOIN `{ds}.dept` d ON e.dept_id = d.id ORDER BY e.name");
        Assert.Equal(5, rows.Count); // All employees
    }
    [Fact] public async Task LeftJoin_NullDept()
    {
        var rows = await Query("SELECT e.name, d.dept_name FROM `{ds}.emp` e LEFT JOIN `{ds}.dept` d ON e.dept_id = d.id WHERE d.dept_name IS NULL ORDER BY e.name");
        Assert.Equal(2, rows.Count); // Dave(dept 3 not in dept table), Eve(NULL dept_id)
    }

    // RIGHT JOIN
    [Fact] public async Task RightJoin_Basic()
    {
        var rows = await Query("SELECT e.name, d.dept_name FROM `{ds}.emp` e RIGHT JOIN `{ds}.dept` d ON e.dept_id = d.id ORDER BY d.dept_name");
        Assert.True(rows.Count >= 3); // At least all depts
    }
    [Fact] public async Task RightJoin_UnmatchedDept()
    {
        var rows = await Query("SELECT d.dept_name FROM `{ds}.emp` e RIGHT JOIN `{ds}.dept` d ON e.dept_id = d.id WHERE e.name IS NULL");
        Assert.Single(rows); // Sales dept has no employees
        Assert.Equal("Sales", rows[0]["dept_name"]?.ToString());
    }

    // FULL OUTER JOIN
    [Fact] public async Task FullOuterJoin_Basic()
    {
        var rows = await Query("SELECT e.name, d.dept_name FROM `{ds}.emp` e FULL OUTER JOIN `{ds}.dept` d ON e.dept_id = d.id");
        Assert.True(rows.Count >= 6); // All emps + unmatched depts
    }

    // CROSS JOIN
    [Fact] public async Task CrossJoin_Basic()
    {
        var rows = await Query("SELECT e.name, d.dept_name FROM `{ds}.emp` e CROSS JOIN `{ds}.dept` d");
        Assert.Equal(15, rows.Count); // 5 * 3
    }

    // Self join
    [Fact] public async Task SelfJoin()
    {
        var rows = await Query("SELECT a.name as name1, b.name as name2 FROM `{ds}.emp` a JOIN `{ds}.emp` b ON a.dept_id = b.dept_id AND a.id < b.id ORDER BY a.name");
        Assert.True(rows.Count >= 1); // Alice-Carol pair
    }

    // JOIN with aggregation
    [Fact] public async Task Join_Aggregate()
    {
        var rows = await Query("SELECT d.dept_name, COUNT(e.id) as emp_count FROM `{ds}.dept` d LEFT JOIN `{ds}.emp` e ON d.id = e.dept_id GROUP BY d.dept_name ORDER BY d.dept_name");
        Assert.Equal(3, rows.Count);
    }

    // JOIN with WHERE
    [Fact] public async Task Join_Where()
    {
        var result = await Scalar("SELECT COUNT(*) FROM `{ds}.emp` e JOIN `{ds}.dept` d ON e.dept_id = d.id WHERE d.dept_name = 'Engineering'");
        Assert.Equal("2", result);
    }

    // Multiple JOINs
    [Fact] public async Task MultiJoin()
    {
        var rows = await Query("SELECT e.name FROM `{ds}.emp` e JOIN `{ds}.dept` d ON e.dept_id = d.id JOIN `{ds}.emp` e2 ON e2.dept_id = d.id WHERE e.id != e2.id ORDER BY e.name");
        Assert.True(rows.Count >= 2);
    }

    // JOIN with subquery
    [Fact] public async Task Join_Subquery()
    {
        var rows = await Query("SELECT e.name FROM `{ds}.emp` e JOIN (SELECT id, dept_name FROM `{ds}.dept` WHERE dept_name = 'Engineering') d ON e.dept_id = d.id ORDER BY e.name");
        Assert.Equal(2, rows.Count);
    }

    // JOIN with expression in ON
    [Fact] public async Task Join_ExpressionOn()
    {
        var rows = await Query("SELECT e.name, d.dept_name FROM `{ds}.emp` e JOIN `{ds}.dept` d ON e.dept_id = d.id AND d.id < 3 ORDER BY e.name");
        Assert.Equal(3, rows.Count);
    }
}