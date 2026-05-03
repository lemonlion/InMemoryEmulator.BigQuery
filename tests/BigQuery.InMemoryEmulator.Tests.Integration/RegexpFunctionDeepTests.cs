using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class RegexpFunctionDeepTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public RegexpFunctionDeepTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_rgx_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
    }

    public async ValueTask DisposeAsync()
    {
        try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
        await _fixture.DisposeAsync();
    }

    private async Task<string?> Scalar(string sql)
    {
        var client = await _fixture.GetClientAsync();
        var result = await client.ExecuteQueryAsync(sql, parameters: null);
        var rows = result.ToList();
        return rows.Count > 0 ? rows[0][0]?.ToString() : null;
    }

    // REGEXP_CONTAINS
    [Fact] public async Task RegexpContains_Match() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('hello world', r'world')"));
    [Fact] public async Task RegexpContains_NoMatch() => Assert.Equal("False", await Scalar("SELECT REGEXP_CONTAINS('hello world', r'xyz')"));
    [Fact] public async Task RegexpContains_Digit() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('abc123', r'\\d+')"));
    [Fact] public async Task RegexpContains_Start() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('hello', r'^hel')"));
    [Fact] public async Task RegexpContains_End() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('hello', r'llo$')"));
    [Fact] public async Task RegexpContains_Null() => Assert.Null(await Scalar("SELECT REGEXP_CONTAINS(NULL, r'test')"));

    // REGEXP_EXTRACT
    [Fact] public async Task RegexpExtract_Basic() => Assert.Equal("123", await Scalar("SELECT REGEXP_EXTRACT('abc123def', r'(\\d+)')"));
    [Fact] public async Task RegexpExtract_Group() => Assert.Equal("world", await Scalar("SELECT REGEXP_EXTRACT('hello world', r'hello (\\w+)')"));
    [Fact] public async Task RegexpExtract_NoMatch() => Assert.Null(await Scalar("SELECT REGEXP_EXTRACT('hello', r'(\\d+)')"));
    [Fact] public async Task RegexpExtract_Null() => Assert.Null(await Scalar("SELECT REGEXP_EXTRACT(NULL, r'(\\d+)')"));

    // REGEXP_REPLACE
    [Fact] public async Task RegexpReplace_Basic() => Assert.Equal("hello universe", await Scalar("SELECT REGEXP_REPLACE('hello world', r'world', 'universe')"));
    [Fact] public async Task RegexpReplace_Digits() => Assert.Equal("abc***def", await Scalar("SELECT REGEXP_REPLACE('abc123def', r'\\d+', '***')"));
    [Fact] public async Task RegexpReplace_NoMatch() => Assert.Equal("hello", await Scalar("SELECT REGEXP_REPLACE('hello', r'\\d+', 'x')"));
    [Fact] public async Task RegexpReplace_Null() => Assert.Null(await Scalar("SELECT REGEXP_REPLACE(NULL, r'\\d+', 'x')"));
    [Fact] public async Task RegexpReplace_BackRef() => Assert.Equal("world hello", await Scalar(@"SELECT REGEXP_REPLACE('hello world', r'(\w+) (\w+)', '\\2 \\1')"));

    // REGEXP_EXTRACT_ALL
    [Fact] public async Task RegexpExtractAll_Basic()
    {
        var result = await Scalar("SELECT ARRAY_LENGTH(REGEXP_EXTRACT_ALL('a1b2c3', r'\\d'))");
        Assert.Equal("3", result);
    }
    [Fact] public async Task RegexpExtractAll_Words()
    {
        var result = await Scalar("SELECT ARRAY_LENGTH(REGEXP_EXTRACT_ALL('one two three', r'\\w+'))");
        Assert.Equal("3", result);
    }
    [Fact] public async Task RegexpExtractAll_NoMatch()
    {
        var result = await Scalar("SELECT ARRAY_LENGTH(REGEXP_EXTRACT_ALL('hello', r'\\d+'))");
        Assert.Equal("0", result);
    }

    // Complex patterns
    [Fact] public async Task Regexp_Email() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('user@example.com', r'^[\\w.]+@[\\w.]+\\.\\w+$')"));
    [Fact] public async Task Regexp_IpAddress() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('192.168.1.1', r'^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$')"));
    [Fact] public async Task Regexp_CaseInsensitive() => Assert.Equal("True", await Scalar("SELECT REGEXP_CONTAINS('Hello World', r'(?i)hello')"));
}