using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Hash, encoding, and conversion functions: MD5, SHA256, TO_HEX, TO_BASE64, FROM_HEX, FROM_BASE64, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class HashEncodingCoverageTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public HashEncodingCoverageTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_hec_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.t` (id INT64, val STRING)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.t` VALUES
			(1,'hello'),(2,'world'),(3,'test'),(4,'BigQuery'),(5,'')", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- MD5 ----
	[Fact] public async Task Md5_Hello()
	{
		var v = await S("SELECT TO_HEX(MD5('hello'))");
		Assert.Equal("5d41402abc4b2a76b9719d911017c592", v);
	}
	[Fact] public async Task Md5_Empty()
	{
		var v = await S("SELECT TO_HEX(MD5(''))");
		Assert.Equal("d41d8cd98f00b204e9800998ecf8427e", v);
	}
	[Fact] public async Task Md5_NullInput() => Assert.Null(await S("SELECT MD5(NULL)"));
	[Fact] public async Task Md5_FromTable()
	{
		var v = await S("SELECT TO_HEX(MD5(val)) FROM `{ds}.t` WHERE id = 2");
		Assert.Equal("7d793037a0760186574b0282f2f435e7", v);
	}

	// ---- SHA256 ----
	[Fact] public async Task Sha256_Hello()
	{
		var v = await S("SELECT TO_HEX(SHA256('hello'))");
		Assert.Equal("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", v);
	}
	[Fact] public async Task Sha256_Empty()
	{
		var v = await S("SELECT TO_HEX(SHA256(''))");
		Assert.Equal("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", v);
	}
	[Fact] public async Task Sha256_NullInput() => Assert.Null(await S("SELECT SHA256(NULL)"));

	// ---- SHA512 ----
	[Fact] public async Task Sha512_Hello()
	{
		var v = await S("SELECT TO_HEX(SHA512('hello'))");
		Assert.NotNull(v);
		Assert.Equal(128, v.Length); // 512 bits = 128 hex chars
		Assert.StartsWith("9b71d224bd62f3785d96d46ad3ea3d73", v);
	}
	[Fact] public async Task Sha512_NullInput() => Assert.Null(await S("SELECT SHA512(NULL)"));

	// ---- TO_HEX / FROM_HEX ----
	[Fact] public async Task ToHex_Bytes()
	{
		var v = await S("SELECT TO_HEX(CAST('Hello' AS BYTES))");
		Assert.Equal("48656c6c6f", v);
	}
	[Fact] public async Task ToHex_NullInput() => Assert.Null(await S("SELECT TO_HEX(NULL)"));
	[Fact] public async Task FromHex_Basic()
	{
		var v = await S("SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_HEX('48656c6c6f'))");
		Assert.Equal("Hello", v);
	}
	[Fact] public async Task FromHex_NullInput() => Assert.Null(await S("SELECT FROM_HEX(NULL)"));

	// ---- TO_BASE64 / FROM_BASE64 ----
	[Fact] public async Task ToBase64_Basic()
	{
		var v = await S("SELECT TO_BASE64(b'Hello')");
		Assert.Equal("SGVsbG8=", v);
	}
	[Fact] public async Task ToBase64_NullInput() => Assert.Null(await S("SELECT TO_BASE64(NULL)"));
	[Fact] public async Task FromBase64_Basic()
	{
		var v = await S("SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_BASE64('SGVsbG8='))");
		Assert.Equal("Hello", v);
	}
	[Fact] public async Task FromBase64_NullInput() => Assert.Null(await S("SELECT FROM_BASE64(NULL)"));

	// ---- FARM_FINGERPRINT ----
	[Fact] public async Task FarmFingerprint_Basic()
	{
		var v = await S("SELECT FARM_FINGERPRINT('hello')");
		Assert.NotNull(v);
		Assert.True(long.TryParse(v, out _));
	}
	[Fact] public async Task FarmFingerprint_Deterministic()
	{
		var v1 = await S("SELECT FARM_FINGERPRINT('test')");
		var v2 = await S("SELECT FARM_FINGERPRINT('test')");
		Assert.Equal(v1, v2);
	}
	[Fact] public async Task FarmFingerprint_Different()
	{
		var v1 = await S("SELECT FARM_FINGERPRINT('a')");
		var v2 = await S("SELECT FARM_FINGERPRINT('b')");
		Assert.NotEqual(v1, v2);
	}
	[Fact] public async Task FarmFingerprint_NullInput() => Assert.Null(await S("SELECT FARM_FINGERPRINT(NULL)"));

	// ---- BYTE_LENGTH / CHAR_LENGTH ----
	[Fact] public async Task ByteLength_Ascii() => Assert.Equal("5", await S("SELECT BYTE_LENGTH('hello')"));
	[Fact] public async Task ByteLength_NullInput() => Assert.Null(await S("SELECT BYTE_LENGTH(NULL)"));
	[Fact] public async Task CharLength_Ascii() => Assert.Equal("5", await S("SELECT CHAR_LENGTH('hello')"));
	[Fact] public async Task CharLength_NullInput() => Assert.Null(await S("SELECT CHAR_LENGTH(NULL)"));

	// ---- TO_CODE_POINTS / CODE_POINTS_TO_STRING ----
	[Fact] public async Task ToCodePoints_Basic()
	{
		var v = await S("SELECT ARRAY_TO_STRING(TO_CODE_POINTS('ABC'), ',')");
		Assert.Equal("65,66,67", v);
	}
	[Fact] public async Task CodePointsToString_Basic()
	{
		var v = await S("SELECT CODE_POINTS_TO_STRING([72, 105])");
		Assert.Equal("Hi", v);
	}

	// ---- FORMAT ----
	[Fact] public async Task Format_Int() => Assert.Equal("42", await S("SELECT FORMAT('%d', 42)"));
	[Fact] public async Task Format_Float() => Assert.Equal("3.14", await S("SELECT FORMAT('%.2f', 3.14159)"));
	[Fact] public async Task Format_String() => Assert.Equal("hello world", await S("SELECT FORMAT('%s %s', 'hello', 'world')"));
	[Fact] public async Task Format_Padded() => Assert.Equal("  42", await S("SELECT FORMAT('%4d', 42)"));

	// ---- Combined / complex ----
	[Fact] public async Task Complex_HashCompare()
	{
		var rows = await Q("SELECT id, TO_HEX(MD5(val)) AS hash FROM `{ds}.t` WHERE val != '' ORDER BY id LIMIT 2");
		Assert.Equal("5d41402abc4b2a76b9719d911017c592", rows[0]["hash"]?.ToString()); // hello
		Assert.Equal("7d793037a0760186574b0282f2f435e7", rows[1]["hash"]?.ToString()); // world
	}
	[Fact] public async Task Complex_DistinctFingerprint()
	{
		var v = await S("SELECT COUNT(DISTINCT FARM_FINGERPRINT(val)) FROM `{ds}.t`");
		Assert.Equal("5", v);
	}
	[Fact] public async Task Complex_HexRoundtrip()
	{
		var v = await S("SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_HEX(TO_HEX(b'test')))");
		Assert.Equal("test", v);
	}
	[Fact] public async Task Complex_Base64Roundtrip()
	{
		var v = await S("SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_BASE64(TO_BASE64(b'roundtrip')))");
		Assert.Equal("roundtrip", v);
	}
}
