using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class HashFunctionTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public HashFunctionTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_hsh_{Guid.NewGuid():N}"[..30];
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

    // FARM_FINGERPRINT
    [Fact] public async Task FarmFingerprint_Basic()
    {
        var result = await Scalar("SELECT FARM_FINGERPRINT('hello')");
        Assert.NotNull(result);
        long.Parse(result!); // should be a valid int64
    }
    [Fact] public async Task FarmFingerprint_Empty()
    {
        var result = await Scalar("SELECT FARM_FINGERPRINT('')");
        Assert.NotNull(result);
    }
    [Fact] public async Task FarmFingerprint_Deterministic()
    {
        var r1 = await Scalar("SELECT FARM_FINGERPRINT('test')");
        var r2 = await Scalar("SELECT FARM_FINGERPRINT('test')");
        Assert.Equal(r1, r2);
    }
    [Fact] public async Task FarmFingerprint_Null() => Assert.Null(await Scalar("SELECT FARM_FINGERPRINT(NULL)"));
    [Fact] public async Task FarmFingerprint_Different()
    {
        var r1 = await Scalar("SELECT FARM_FINGERPRINT('abc')");
        var r2 = await Scalar("SELECT FARM_FINGERPRINT('def')");
        Assert.NotEqual(r1, r2);
    }

    // MD5
    [Fact] public async Task Md5_Basic()
    {
        var result = await Scalar("SELECT TO_HEX(MD5('hello'))");
        Assert.NotNull(result);
        Assert.Equal(32, result!.Length); // MD5 = 16 bytes = 32 hex chars
    }
    [Fact] public async Task Md5_Deterministic()
    {
        var r1 = await Scalar("SELECT TO_HEX(MD5('test'))");
        var r2 = await Scalar("SELECT TO_HEX(MD5('test'))");
        Assert.Equal(r1, r2);
    }
    [Fact] public async Task Md5_Null() => Assert.Null(await Scalar("SELECT MD5(NULL)"));
    [Fact] public async Task Md5_KnownValue()
    {
        // MD5('hello') = 5d41402abc4b2a76b9719d911017c592
        var result = await Scalar("SELECT LOWER(TO_HEX(MD5('hello')))");
        Assert.Equal("5d41402abc4b2a76b9719d911017c592", result);
    }

    // SHA256
    [Fact] public async Task Sha256_Basic()
    {
        var result = await Scalar("SELECT TO_HEX(SHA256('hello'))");
        Assert.NotNull(result);
        Assert.Equal(64, result!.Length); // SHA256 = 32 bytes = 64 hex chars
    }
    [Fact] public async Task Sha256_Null() => Assert.Null(await Scalar("SELECT SHA256(NULL)"));
    [Fact] public async Task Sha256_KnownValue()
    {
        // SHA256('hello') = 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
        var result = await Scalar("SELECT LOWER(TO_HEX(SHA256('hello')))");
        Assert.Equal("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", result);
    }

    // SHA512
    [Fact] public async Task Sha512_Basic()
    {
        var result = await Scalar("SELECT TO_HEX(SHA512('hello'))");
        Assert.NotNull(result);
        Assert.Equal(128, result!.Length); // SHA512 = 64 bytes = 128 hex chars
    }
    [Fact] public async Task Sha512_Null() => Assert.Null(await Scalar("SELECT SHA512(NULL)"));

    // TO_HEX / FROM_HEX
    [Fact] public async Task ToHex_Basic()
    {
        var result = await Scalar("SELECT TO_HEX(b'\\x48\\x65\\x6c\\x6c\\x6f')");
        Assert.NotNull(result);
    }
    [Fact] public async Task ToHex_Null() => Assert.Null(await Scalar("SELECT TO_HEX(NULL)"));
    
    // TO_BASE64 / FROM_BASE64
    [Fact] public async Task ToBase64_Basic()
    {
        var result = await Scalar("SELECT TO_BASE64(b'hello')");
        Assert.NotNull(result);
        Assert.Equal("aGVsbG8=", result);
    }
    [Fact] public async Task FromBase64_Basic()
    {
        var result = await Scalar("SELECT CAST(FROM_BASE64('aGVsbG8=') AS STRING)");
        Assert.Equal("hello", result);
    }
    [Fact] public async Task ToBase64_Null() => Assert.Null(await Scalar("SELECT TO_BASE64(NULL)"));
    [Fact] public async Task FromBase64_Null() => Assert.Null(await Scalar("SELECT FROM_BASE64(NULL)"));
}