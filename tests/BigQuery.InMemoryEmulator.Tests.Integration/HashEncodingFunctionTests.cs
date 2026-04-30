using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Hash, encoding, and fingerprint function tests.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#to_base64
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class HashEncodingFunctionTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public HashEncodingFunctionTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
	}

	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- MD5 ----
	[Fact] public async Task Md5_EmptyString() => Assert.NotNull(await Scalar("SELECT MD5(b'')"));
	[Fact] public async Task Md5_HelloWorld() => Assert.NotNull(await Scalar("SELECT MD5(b'Hello, World!')"));
	[Fact] public async Task Md5_Null_ReturnsNull() => Assert.Null(await Scalar("SELECT MD5(NULL)"));
	[Fact] public async Task Md5_KnownHash() { var v = await Scalar("SELECT TO_HEX(MD5(b'abc'))"); Assert.Equal("900150983cd24fb0d6963f7d28e17f72", v); }

	// ---- SHA1 ----
	[Fact] public async Task Sha1_Basic() => Assert.NotNull(await Scalar("SELECT SHA1(b'test')"));
	[Fact] public async Task Sha1_Null_ReturnsNull() => Assert.Null(await Scalar("SELECT SHA1(NULL)"));
	[Fact] public async Task Sha1_KnownHash() { var v = await Scalar("SELECT TO_HEX(SHA1(b'abc'))"); Assert.Equal("a9993e364706816aba3e25717850c26c9cd0d89d", v); }

	// ---- SHA256 ----
	[Fact] public async Task Sha256_Basic() => Assert.NotNull(await Scalar("SELECT SHA256(b'test')"));
	[Fact] public async Task Sha256_Null_ReturnsNull() => Assert.Null(await Scalar("SELECT SHA256(NULL)"));
	[Fact] public async Task Sha256_KnownHash() { var v = await Scalar("SELECT TO_HEX(SHA256(b'abc'))"); Assert.Equal("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", v); }

	// ---- SHA512 ----
	[Fact] public async Task Sha512_Basic() => Assert.NotNull(await Scalar("SELECT SHA512(b'test')"));
	[Fact] public async Task Sha512_Null_ReturnsNull() => Assert.Null(await Scalar("SELECT SHA512(NULL)"));

	// ---- FARM_FINGERPRINT ----
	[Fact] public async Task FarmFingerprint_String() { var v = await Scalar("SELECT FARM_FINGERPRINT('test')"); Assert.NotNull(v); long.Parse(v!); }
	[Fact] public async Task FarmFingerprint_EmptyString() { var v = await Scalar("SELECT FARM_FINGERPRINT('')"); Assert.NotNull(v); }
	[Fact] public async Task FarmFingerprint_Null() => Assert.Null(await Scalar("SELECT FARM_FINGERPRINT(NULL)"));
	[Fact] public async Task FarmFingerprint_Deterministic() { var v1 = await Scalar("SELECT FARM_FINGERPRINT('hello')"); var v2 = await Scalar("SELECT FARM_FINGERPRINT('hello')"); Assert.Equal(v1, v2); }
	[Fact] public async Task FarmFingerprint_DifferentInputs() { var v1 = await Scalar("SELECT FARM_FINGERPRINT('a')"); var v2 = await Scalar("SELECT FARM_FINGERPRINT('b')"); Assert.NotEqual(v1, v2); }

	// ---- TO_HEX / FROM_HEX ----
	[Fact] public async Task ToHex_Basic() => Assert.Equal("48656c6c6f", await Scalar("SELECT TO_HEX(b'Hello')"));
	[Fact] public async Task FromHex_Basic() => Assert.NotNull(await Scalar("SELECT FROM_HEX('48656c6c6f')"));
	[Fact] public async Task ToHex_RoundTrip() => Assert.Equal("48656c6c6f", await Scalar("SELECT TO_HEX(FROM_HEX('48656c6c6f'))"));
	[Fact] public async Task ToHex_Null() => Assert.Null(await Scalar("SELECT TO_HEX(NULL)"));
	[Fact] public async Task FromHex_Null() => Assert.Null(await Scalar("SELECT FROM_HEX(NULL)"));
	[Fact] public async Task ToHex_EmptyBytes() => Assert.Equal("", await Scalar("SELECT TO_HEX(b'')"));

	// ---- TO_BASE64 / FROM_BASE64 ----
	[Fact] public async Task ToBase64_Basic() => Assert.Equal("SGVsbG8=", await Scalar("SELECT TO_BASE64(b'Hello')"));
	[Fact] public async Task FromBase64_Basic() => Assert.NotNull(await Scalar("SELECT FROM_BASE64('SGVsbG8=')"));
	[Fact] public async Task ToBase64_Null() => Assert.Null(await Scalar("SELECT TO_BASE64(NULL)"));
	[Fact] public async Task FromBase64_Null() => Assert.Null(await Scalar("SELECT FROM_BASE64(NULL)"));
	[Fact] public async Task ToBase64_EmptyBytes() => Assert.Equal("", await Scalar("SELECT TO_BASE64(b'')"));

	// ---- TO_BASE32 / FROM_BASE32 ----
	[Fact] public async Task ToBase32_Basic() => Assert.NotNull(await Scalar("SELECT TO_BASE32(b'Hello')"));
	[Fact] public async Task FromBase32_Basic() => Assert.NotNull(await Scalar("SELECT FROM_BASE32(TO_BASE32(b'Hello'))"));
	[Fact] public async Task ToBase32_Null() => Assert.Null(await Scalar("SELECT TO_BASE32(NULL)"));
	[Fact] public async Task FromBase32_Null() => Assert.Null(await Scalar("SELECT FROM_BASE32(NULL)"));

	// ---- BIT_COUNT ----
	[Fact] public async Task BitCount_Zero() => Assert.Equal("0", await Scalar("SELECT BIT_COUNT(0)"));
	[Fact] public async Task BitCount_One() => Assert.Equal("1", await Scalar("SELECT BIT_COUNT(1)"));
	[Fact] public async Task BitCount_Seven() => Assert.Equal("3", await Scalar("SELECT BIT_COUNT(7)"));
	[Fact] public async Task BitCount_255() => Assert.Equal("8", await Scalar("SELECT BIT_COUNT(255)"));
	[Fact] public async Task BitCount_Null() => Assert.Null(await Scalar("SELECT BIT_COUNT(NULL)"));
	[Fact] public async Task BitCount_NegativeOne() { var v = await Scalar("SELECT BIT_COUNT(-1)"); Assert.NotNull(v); }
}
