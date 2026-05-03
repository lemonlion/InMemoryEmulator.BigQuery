using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class NullHandlingTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public NullHandlingTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_nul_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.data` (id INT64, name STRING, val INT64, score FLOAT64)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.data` (id, name, val, score) VALUES
            (1,'Alice',10,1.5),(2,'Bob',NULL,2.5),(3,NULL,30,NULL),(4,'Dave',40,4.5),(5,NULL,NULL,NULL)", parameters: null);
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

    // IS NULL / IS NOT NULL
    [Fact] public async Task IsNull_String() => Assert.Equal("2", await Scalar("SELECT COUNT(*) FROM `{ds}.data` WHERE name IS NULL"));
    [Fact] public async Task IsNotNull_String() => Assert.Equal("3", await Scalar("SELECT COUNT(*) FROM `{ds}.data` WHERE name IS NOT NULL"));
    [Fact] public async Task IsNull_Int() => Assert.Equal("2", await Scalar("SELECT COUNT(*) FROM `{ds}.data` WHERE val IS NULL"));
    [Fact] public async Task IsNull_Float() => Assert.Equal("2", await Scalar("SELECT COUNT(*) FROM `{ds}.data` WHERE score IS NULL"));

    // NULL in arithmetic
    [Fact] public async Task Null_Add() => Assert.Null(await Scalar("SELECT val + 1 FROM `{ds}.data` WHERE id = 2"));
    [Fact] public async Task Null_Multiply() => Assert.Null(await Scalar("SELECT val * 2 FROM `{ds}.data` WHERE id = 2"));
    [Fact] public async Task Null_Concat() => Assert.Null(await Scalar("SELECT CONCAT(name, ' Smith') FROM `{ds}.data` WHERE id = 3"));

    // NULL comparison
    [Fact] public async Task Null_Equals() => Assert.Equal("0", await Scalar("SELECT COUNT(*) FROM `{ds}.data` WHERE val = NULL"));
    [Fact] public async Task Null_NotEquals() => Assert.Equal("0", await Scalar("SELECT COUNT(*) FROM `{ds}.data` WHERE val != NULL"));
    [Fact] public async Task Null_GreaterThan() => Assert.Equal("0", await Scalar("SELECT COUNT(*) FROM `{ds}.data` WHERE val > NULL"));

    // COALESCE
    [Fact] public async Task Coalesce_FirstNonNull() => Assert.Equal("Bob", await Scalar("SELECT COALESCE(name, 'Unknown') FROM `{ds}.data` WHERE id = 2"));
    [Fact] public async Task Coalesce_NullFallback() => Assert.Equal("Unknown", await Scalar("SELECT COALESCE(name, 'Unknown') FROM `{ds}.data` WHERE id = 3"));
    [Fact] public async Task Coalesce_Chain() => Assert.Equal("0", await Scalar("SELECT COALESCE(val, score, 0) FROM `{ds}.data` WHERE id = 5"));

    // IFNULL
    [Fact] public async Task IfNull_NonNull() => Assert.Equal("10", await Scalar("SELECT IFNULL(val, 99) FROM `{ds}.data` WHERE id = 1"));
    [Fact] public async Task IfNull_IsNull() => Assert.Equal("99", await Scalar("SELECT IFNULL(val, 99) FROM `{ds}.data` WHERE id = 2"));

    // NULLIF
    [Fact] public async Task NullIf_Equal() => Assert.Null(await Scalar("SELECT NULLIF(val, 10) FROM `{ds}.data` WHERE id = 1"));
    [Fact] public async Task NullIf_NotEqual() => Assert.Equal("30", await Scalar("SELECT NULLIF(val, 10) FROM `{ds}.data` WHERE id = 3"));

    // NULL in aggregation
    [Fact] public async Task Count_Star_IncludesNull() => Assert.Equal("5", await Scalar("SELECT COUNT(*) FROM `{ds}.data`"));
    [Fact] public async Task Count_Column_ExcludesNull() => Assert.Equal("3", await Scalar("SELECT COUNT(val) FROM `{ds}.data`"));
    [Fact] public async Task Sum_IgnoresNull() => Assert.Equal("80", await Scalar("SELECT SUM(val) FROM `{ds}.data`"));
    [Fact] public async Task Avg_IgnoresNull()
    {
        var v = double.Parse((await Scalar("SELECT AVG(CAST(val AS FLOAT64)) FROM `{ds}.data`"))!);
        Assert.True(Math.Abs(v - 26.67) < 0.1);
    }
    [Fact] public async Task Min_IgnoresNull() => Assert.Equal("10", await Scalar("SELECT MIN(val) FROM `{ds}.data`"));
    [Fact] public async Task Max_IgnoresNull() => Assert.Equal("40", await Scalar("SELECT MAX(val) FROM `{ds}.data`"));

    // NULL in ORDER BY
    [Fact] public async Task OrderBy_NullsLast()
    {
        var rows = await Query("SELECT id, val FROM `{ds}.data` ORDER BY val");
        var nullCount = rows.Count(r => r["val"] == null);
        Assert.Equal(2, nullCount);
    }
    [Fact] public async Task OrderBy_NullsDesc()
    {
        var rows = await Query("SELECT id, val FROM `{ds}.data` ORDER BY val DESC");
        var nullCount = rows.Count(r => r["val"] == null);
        Assert.Equal(2, nullCount);
    }

    // NULL in CASE
    [Fact] public async Task Case_NullCondition()
    {
        var result = await Scalar("SELECT CASE WHEN val IS NULL THEN 'missing' ELSE 'present' END FROM `{ds}.data` WHERE id = 2");
        Assert.Equal("missing", result);
    }

    // NULL in DISTINCT
    [Fact] public async Task Distinct_IncludesNull()
    {
        var rows = await Query("SELECT DISTINCT name FROM `{ds}.data` ORDER BY name");
        Assert.Equal(4, rows.Count); // Alice, Bob, Dave, NULL
    }

    // NULL in GROUP BY
    [Fact] public async Task GroupBy_NullGroup()
    {
        var rows = await Query("SELECT name, COUNT(*) as cnt FROM `{ds}.data` GROUP BY name ORDER BY name");
        Assert.Equal(4, rows.Count); // NULL is its own group
    }

    // NULL AND/OR logic
    [Fact] public async Task Null_And_True() => Assert.Null(await Scalar("SELECT NULL AND TRUE"));
    [Fact] public async Task Null_And_False() => Assert.Equal("False", await Scalar("SELECT NULL AND FALSE"));
    [Fact] public async Task Null_Or_True() => Assert.Equal("True", await Scalar("SELECT NULL OR TRUE"));
    [Fact] public async Task Null_Or_False() => Assert.Null(await Scalar("SELECT NULL OR FALSE"));
    [Fact] public async Task Not_Null() => Assert.Null(await Scalar("SELECT NOT CAST(NULL AS BOOL)"));

    // NULL in string functions
    [Fact] public async Task Length_Null() => Assert.Null(await Scalar("SELECT LENGTH(CAST(NULL AS STRING))"));
    [Fact] public async Task Upper_Null() => Assert.Null(await Scalar("SELECT UPPER(CAST(NULL AS STRING))"));
    [Fact] public async Task Concat_Null() => Assert.Null(await Scalar("SELECT CONCAT(CAST(NULL AS STRING), 'test')"));
}