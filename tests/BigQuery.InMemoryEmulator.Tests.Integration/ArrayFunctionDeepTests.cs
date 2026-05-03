using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ArrayFunctionDeepTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public ArrayFunctionDeepTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_arr_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.t` (id INT64, tags ARRAY<STRING>, nums ARRAY<INT64>)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.t` (id, tags, nums) VALUES
            (1, ['a','b','c'], [1,2,3]),
            (2, ['x','y'], [10,20,30,40]),
            (3, [], []),
            (4, ['a','a','b'], [1,1,2,2,3])", parameters: null);
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

    private async Task<List<BigQueryRow>> Query(string sql)
    {
        var client = await _fixture.GetClientAsync();
        var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
        return result.ToList();
    }

    // ARRAY_LENGTH
    [Fact] public async Task ArrayLength_Basic() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(tags) FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task ArrayLength_Empty() => Assert.Equal("0", await Scalar("SELECT ARRAY_LENGTH(tags) FROM `{ds}.t` WHERE id = 3"));
    [Fact] public async Task ArrayLength_Literal() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH([1,2,3])"));

    // Array subscript (1-based in BigQuery via OFFSET/ORDINAL)
    [Fact] public async Task ArrayOffset_First() => Assert.Equal("a", await Scalar("SELECT tags[OFFSET(0)] FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task ArrayOffset_Last() => Assert.Equal("c", await Scalar("SELECT tags[OFFSET(2)] FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task ArrayOrdinal_First() => Assert.Equal("a", await Scalar("SELECT tags[ORDINAL(1)] FROM `{ds}.t` WHERE id = 1"));

    // ARRAY_CONCAT
    [Fact] public async Task ArrayConcat_Basic() => Assert.Equal("5", await Scalar("SELECT ARRAY_LENGTH(ARRAY_CONCAT(tags, ['d','e'])) FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task ArrayConcat_TwoArrays() => Assert.Equal("4", await Scalar("SELECT ARRAY_LENGTH(ARRAY_CONCAT([1,2], [3,4]))"));

    // ARRAY_REVERSE
    [Fact] public async Task ArrayReverse_Basic()
    {
        var result = await Scalar("SELECT ARRAY_REVERSE(tags)[OFFSET(0)] FROM `{ds}.t` WHERE id = 1");
        Assert.Equal("c", result);
    }

    // GENERATE_ARRAY
    [Fact] public async Task GenerateArray_Basic() => Assert.Equal("5", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(1, 5))"));
    [Fact] public async Task GenerateArray_Step() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(1, 5, 2))"));
    [Fact] public async Task GenerateArray_Empty() => Assert.Equal("0", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(5, 1))"));

    // UNNEST
    [Fact] public async Task Unnest_Basic()
    {
        var rows = await Query("SELECT elem FROM `{ds}.t`, UNNEST(tags) as elem WHERE id = 1 ORDER BY elem");
        Assert.Equal(3, rows.Count);
        Assert.Equal("a", rows[0]["elem"]?.ToString());
    }
    [Fact] public async Task Unnest_WithOffset()
    {
        var rows = await Query("SELECT elem, off FROM `{ds}.t`, UNNEST(tags) as elem WITH OFFSET as off WHERE id = 1 ORDER BY off");
        Assert.Equal(3, rows.Count);
        Assert.Equal("0", rows[0]["off"]?.ToString());
    }
    [Fact] public async Task Unnest_Empty()
    {
        var rows = await Query("SELECT elem FROM `{ds}.t`, UNNEST(tags) as elem WHERE id = 3");
        Assert.Empty(rows);
    }

    // ARRAY_TO_STRING
    [Fact] public async Task ArrayToString_Basic() => Assert.Equal("a,b,c", await Scalar("SELECT ARRAY_TO_STRING(tags, ',') FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task ArrayToString_Empty() => Assert.Equal("", await Scalar("SELECT ARRAY_TO_STRING(tags, ',') FROM `{ds}.t` WHERE id = 3"));

    // Array literal
    [Fact] public async Task ArrayLiteral_Int() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH([10, 20, 30])"));
    [Fact] public async Task ArrayLiteral_String() => Assert.Equal("2", await Scalar("SELECT ARRAY_LENGTH(['hello', 'world'])"));

    // ARRAY_AGG in subquery
    [Fact] public async Task ArrayAgg_Subquery()
    {
        var result = await Scalar("SELECT ARRAY_LENGTH(ARRAY(SELECT elem FROM UNNEST([1,2,3,4,5]) as elem WHERE elem > 2))");
        Assert.Equal("3", result);
    }

    // SAFE_OFFSET
    [Fact] public async Task SafeOffset_Valid() => Assert.Equal("a", await Scalar("SELECT tags[SAFE_OFFSET(0)] FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task SafeOffset_OutOfBounds() => Assert.Null(await Scalar("SELECT tags[SAFE_OFFSET(99)] FROM `{ds}.t` WHERE id = 1"));
}