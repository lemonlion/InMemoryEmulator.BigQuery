using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class StringFunctionDeepTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public StringFunctionDeepTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_str_{Guid.NewGuid():N}"[..30];
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

    // LENGTH
    [Fact] public async Task Length_Basic() => Assert.Equal("5", await Scalar("SELECT LENGTH('hello')"));
    [Fact] public async Task Length_Empty() => Assert.Equal("0", await Scalar("SELECT LENGTH('')"));
    [Fact] public async Task Length_Null() => Assert.Null(await Scalar("SELECT LENGTH(NULL)"));
    [Fact] public async Task Length_Unicode() => Assert.Equal("3", await Scalar("SELECT LENGTH('abc')"));

    // UPPER / LOWER
    [Fact] public async Task Upper_Basic() => Assert.Equal("HELLO", await Scalar("SELECT UPPER('hello')"));
    [Fact] public async Task Lower_Basic() => Assert.Equal("hello", await Scalar("SELECT LOWER('HELLO')"));
    [Fact] public async Task Upper_Null() => Assert.Null(await Scalar("SELECT UPPER(NULL)"));
    [Fact] public async Task Lower_Null() => Assert.Null(await Scalar("SELECT LOWER(NULL)"));

    // TRIM / LTRIM / RTRIM
    [Fact] public async Task Trim_Spaces() => Assert.Equal("hello", await Scalar("SELECT TRIM('  hello  ')"));
    [Fact] public async Task Ltrim_Spaces() => Assert.Equal("hello  ", await Scalar("SELECT LTRIM('  hello  ')"));
    [Fact] public async Task Rtrim_Spaces() => Assert.Equal("  hello", await Scalar("SELECT RTRIM('  hello  ')"));
    [Fact] public async Task Trim_Custom() => Assert.Equal("hello", await Scalar("SELECT TRIM('xxhelloxx', 'x')"));
    [Fact] public async Task Trim_Null() => Assert.Null(await Scalar("SELECT TRIM(NULL)"));

    // SUBSTR
    [Fact] public async Task Substr_Basic() => Assert.Equal("llo", await Scalar("SELECT SUBSTR('hello', 3)"));
    [Fact] public async Task Substr_WithLength() => Assert.Equal("hel", await Scalar("SELECT SUBSTR('hello', 1, 3)"));
    [Fact] public async Task Substr_Negative() => Assert.Equal("lo", await Scalar("SELECT SUBSTR('hello', -2)"));
    [Fact] public async Task Substr_Null() => Assert.Null(await Scalar("SELECT SUBSTR(NULL, 1, 3)"));

    // REPLACE
    [Fact] public async Task Replace_Basic() => Assert.Equal("hxllo", await Scalar("SELECT REPLACE('hello', 'e', 'x')"));
    [Fact] public async Task Replace_Multiple() => Assert.Equal("hxllo", await Scalar("SELECT REPLACE('hello', 'e', 'x')"));
    [Fact] public async Task Replace_NoMatch() => Assert.Equal("hello", await Scalar("SELECT REPLACE('hello', 'z', 'x')"));
    [Fact] public async Task Replace_Null() => Assert.Null(await Scalar("SELECT REPLACE(NULL, 'e', 'x')"));

    // CONCAT
    [Fact] public async Task Concat_Two() => Assert.Equal("ab", await Scalar("SELECT CONCAT('a', 'b')"));
    [Fact] public async Task Concat_Three() => Assert.Equal("abc", await Scalar("SELECT CONCAT('a', 'b', 'c')"));
    [Fact] public async Task Concat_WithNull() => Assert.Null(await Scalar("SELECT CONCAT('a', NULL, 'b')"));

    // STARTS_WITH / ENDS_WITH
    [Fact] public async Task StartsWith_True() => Assert.Equal("True", await Scalar("SELECT STARTS_WITH('hello world', 'hello')"));
    [Fact] public async Task StartsWith_False() => Assert.Equal("False", await Scalar("SELECT STARTS_WITH('hello world', 'world')"));
    [Fact] public async Task EndsWith_True() => Assert.Equal("True", await Scalar("SELECT ENDS_WITH('hello world', 'world')"));
    [Fact] public async Task EndsWith_False() => Assert.Equal("False", await Scalar("SELECT ENDS_WITH('hello world', 'hello')"));
    [Fact] public async Task StartsWith_Null() => Assert.Null(await Scalar("SELECT STARTS_WITH(NULL, 'hello')"));
    [Fact] public async Task EndsWith_Null() => Assert.Null(await Scalar("SELECT ENDS_WITH(NULL, 'hello')"));

    // STRPOS / INSTR
    [Fact] public async Task Strpos_Found() => Assert.Equal("7", await Scalar("SELECT STRPOS('hello world', 'world')"));
    [Fact] public async Task Strpos_NotFound() => Assert.Equal("0", await Scalar("SELECT STRPOS('hello world', 'xyz')"));
    [Fact] public async Task Strpos_Null() => Assert.Null(await Scalar("SELECT STRPOS(NULL, 'hello')"));

    // REPEAT
    [Fact] public async Task Repeat_Basic() => Assert.Equal("abcabcabc", await Scalar("SELECT REPEAT('abc', 3)"));
    [Fact] public async Task Repeat_Zero() => Assert.Equal("", await Scalar("SELECT REPEAT('abc', 0)"));
    [Fact] public async Task Repeat_Null() => Assert.Null(await Scalar("SELECT REPEAT(NULL, 3)"));

    // REVERSE
    [Fact] public async Task Reverse_Basic() => Assert.Equal("olleh", await Scalar("SELECT REVERSE('hello')"));
    [Fact] public async Task Reverse_Null() => Assert.Null(await Scalar("SELECT REVERSE(NULL)"));

    // LPAD / RPAD
    [Fact] public async Task Lpad_Basic() => Assert.Equal("00hello", await Scalar("SELECT LPAD('hello', 7, '0')"));
    [Fact] public async Task Rpad_Basic() => Assert.Equal("hello00", await Scalar("SELECT RPAD('hello', 7, '0')"));
    [Fact] public async Task Lpad_Truncate() => Assert.Equal("hel", await Scalar("SELECT LPAD('hello', 3, '0')"));
    [Fact] public async Task Rpad_Truncate() => Assert.Equal("hel", await Scalar("SELECT RPAD('hello', 3, '0')"));

    // SPLIT
    [Fact] public async Task Split_Basic()
    {
        var result = await Scalar("SELECT ARRAY_LENGTH(SPLIT('a,b,c', ','))");
        Assert.Equal("3", result);
    }
    [Fact] public async Task Split_NoDelim()
    {
        var result = await Scalar("SELECT ARRAY_LENGTH(SPLIT('abc', ','))");
        Assert.Equal("1", result);
    }

    // FORMAT
    [Fact] public async Task Format_Int() => Assert.Equal("42", await Scalar("SELECT FORMAT('%d', 42)"));
    [Fact] public async Task Format_Float() => Assert.Contains("3.14", (await Scalar("SELECT FORMAT('%.2f', 3.14159)"))!);
    [Fact] public async Task Format_String() => Assert.Equal("hello world", await Scalar("SELECT FORMAT('%s %s', 'hello', 'world')"));

    // INITCAP
    [Fact] public async Task Initcap_Basic() => Assert.Equal("Hello World", await Scalar("SELECT INITCAP('hello world')"));
    [Fact] public async Task Initcap_AllCaps() => Assert.Equal("Hello World", await Scalar("SELECT INITCAP('HELLO WORLD')"));
}