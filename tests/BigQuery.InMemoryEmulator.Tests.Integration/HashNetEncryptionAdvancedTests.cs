using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for hash/encoding functions, network functions, and encryption.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class HashNetEncryptionAdvancedTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public HashNetEncryptionAdvancedTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_hne_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- MD5 ----
	[Fact] public async Task Md5_NotNull() => Assert.NotNull(await S("SELECT MD5('hello')"));
	[Fact] public async Task Md5_Deterministic()
	{
		var v1 = await S("SELECT TO_HEX(MD5('hello'))");
		var v2 = await S("SELECT TO_HEX(MD5('hello'))");
		Assert.Equal(v1, v2);
	}
	[Fact] public async Task Md5_DifferentInputs()
	{
		var v1 = await S("SELECT TO_HEX(MD5('hello'))");
		var v2 = await S("SELECT TO_HEX(MD5('world'))");
		Assert.NotEqual(v1, v2);
	}

	// ---- SHA1 ----
	[Fact] public async Task Sha1_NotNull() => Assert.NotNull(await S("SELECT SHA1('hello')"));
	[Fact] public async Task Sha1_Length()
	{
		var v = await S("SELECT LENGTH(TO_HEX(SHA1('hello')))");
		Assert.Equal("40", v); // 20 bytes = 40 hex chars
	}

	// ---- SHA256 ----
	[Fact] public async Task Sha256_NotNull() => Assert.NotNull(await S("SELECT SHA256('hello')"));
	[Fact] public async Task Sha256_Length()
	{
		var v = await S("SELECT LENGTH(TO_HEX(SHA256('hello')))");
		Assert.Equal("64", v); // 32 bytes = 64 hex chars
	}

	// ---- SHA512 ----
	[Fact] public async Task Sha512_NotNull() => Assert.NotNull(await S("SELECT SHA512('hello')"));
	[Fact] public async Task Sha512_Length()
	{
		var v = await S("SELECT LENGTH(TO_HEX(SHA512('hello')))");
		Assert.Equal("128", v); // 64 bytes = 128 hex chars
	}

	// ---- FARM_FINGERPRINT ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#farm_fingerprint
	[Fact] public async Task FarmFingerprint_NotNull() => Assert.NotNull(await S("SELECT FARM_FINGERPRINT('hello')"));
	[Fact] public async Task FarmFingerprint_Deterministic()
	{
		var v1 = await S("SELECT FARM_FINGERPRINT('hello')");
		var v2 = await S("SELECT FARM_FINGERPRINT('hello')");
		Assert.Equal(v1, v2);
	}
	[Fact] public async Task FarmFingerprint_DifferentInputs()
	{
		var v1 = await S("SELECT FARM_FINGERPRINT('hello')");
		var v2 = await S("SELECT FARM_FINGERPRINT('world')");
		Assert.NotEqual(v1, v2);
	}
	[Fact] public async Task FarmFingerprint_Null() => Assert.Null(await S("SELECT FARM_FINGERPRINT(NULL)"));

	// ---- TO_BASE64 / FROM_BASE64 ----
	[Fact] public async Task ToBase64_Basic() => Assert.Equal("aGVsbG8=", await S("SELECT TO_BASE64(b'hello')"));
	[Fact] public async Task FromBase64_Basic() => Assert.Equal("hello", await S("SELECT CAST(FROM_BASE64('aGVsbG8=') AS STRING)"));
	[Fact] public async Task Base64_Roundtrip()
	{
		var v = await S("SELECT CAST(FROM_BASE64(TO_BASE64(b'test')) AS STRING)");
		Assert.Equal("test", v);
	}

	// ---- TO_HEX / FROM_HEX ----
	[Fact] public async Task ToHex_Basic() => Assert.Equal("68656c6c6f", await S("SELECT TO_HEX(b'hello')"));
	[Fact] public async Task FromHex_Basic() => Assert.Equal("hello", await S("SELECT CAST(FROM_HEX('68656c6c6f') AS STRING)"));
	[Fact] public async Task Hex_Roundtrip()
	{
		var v = await S("SELECT CAST(FROM_HEX(TO_HEX(b'test')) AS STRING)");
		Assert.Equal("test", v);
	}

	// ---- TO_BASE32 / FROM_BASE32 ----
	[Fact] public async Task ToBase32_Basic() => Assert.NotNull(await S("SELECT TO_BASE32(b'hello')"));
	[Fact] public async Task FromBase32_Roundtrip()
	{
		var v = await S("SELECT CAST(FROM_BASE32(TO_BASE32(b'test')) AS STRING)");
		Assert.Equal("test", v);
	}

	// ---- NET.HOST ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#nethosturl
	[Fact] public async Task NetHost_Basic() => Assert.Equal("www.google.com", await S("SELECT NET.HOST('https://www.google.com/search?q=test')"));
	[Fact] public async Task NetHost_NoScheme() => Assert.Equal("example.com", await S("SELECT NET.HOST('http://example.com/path')"));
	[Fact] public async Task NetHost_Null() => Assert.Null(await S("SELECT NET.HOST(NULL)"));

	// ---- NET.PUBLIC_SUFFIX ----
	[Fact] public async Task NetPublicSuffix_Com() => Assert.Equal("com", await S("SELECT NET.PUBLIC_SUFFIX('www.google.com')"));
	[Fact] public async Task NetPublicSuffix_CoUk() => Assert.Equal("co.uk", await S("SELECT NET.PUBLIC_SUFFIX('www.example.co.uk')"));

	// ---- NET.REG_DOMAIN ----
	[Fact] public async Task NetRegDomain_Basic() => Assert.Equal("google.com", await S("SELECT NET.REG_DOMAIN('www.google.com')"));

	// ---- NET.IP_FROM_STRING / NET.IP_TO_STRING ----
	[Fact] public async Task NetIpFromString_IPv4_NotNull() => Assert.NotNull(await S("SELECT NET.IP_FROM_STRING('192.168.1.1')"));
	[Fact] public async Task NetIpToString_Roundtrip()
	{
		var v = await S("SELECT NET.IP_TO_STRING(NET.IP_FROM_STRING('192.168.1.1'))");
		Assert.Equal("192.168.1.1", v);
	}
	[Fact] public async Task NetSafeIpFromString_Invalid() => Assert.Null(await S("SELECT NET.SAFE_IP_FROM_STRING('not-an-ip')"));
	[Fact] public async Task NetSafeIpFromString_Valid() => Assert.NotNull(await S("SELECT NET.SAFE_IP_FROM_STRING('10.0.0.1')"));

	// ---- NET.IPV4_FROM_INT64 / NET.IPV4_TO_INT64 ----
	[Fact] public async Task NetIpv4FromInt64_Roundtrip()
	{
		var v = await S("SELECT NET.IPV4_TO_INT64(NET.IPV4_FROM_INT64(3232235777))"); // 192.168.1.1
		Assert.Equal("3232235777", v);
	}

	// ---- KEYS functions ----
	[Fact] public async Task KeysNewKeyset_NotNull()
	{
		var v = await S("SELECT KEYS.NEW_KEYSET('AEAD_AES_GCM_256')");
		Assert.NotNull(v);
	}

	// ---- HLL_COUNT functions ----
	[Fact] public async Task HllCountInit_NotNull()
	{
		var v = await S("SELECT HLL_COUNT.INIT(1)");
		Assert.NotNull(v);
	}

	[Fact] public async Task HllCountExtract_Basic()
	{
		// HLL_COUNT.EXTRACT on a sketch returns the approximate distinct count
		// In-memory emulator returns the value directly for single-value sketches
		var v = await S("SELECT HLL_COUNT.EXTRACT(HLL_COUNT.INIT(42))");
		Assert.NotNull(v);
	}
}
