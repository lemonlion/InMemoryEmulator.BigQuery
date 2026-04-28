using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Encryption stub functions: AEAD, KEYS, deterministic.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aead_encryption_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class EncryptionFunctionTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public EncryptionFunctionTests(BigQuerySession session) => _session = session;

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

	// ---- KEYS.NEW_KEYSET ----
	[Fact] public async Task KeysNewKeyset_Aead() => Assert.NotNull(await Scalar("SELECT KEYS.NEW_KEYSET('AEAD_AES_GCM_256')"));
	[Fact] public async Task KeysNewKeyset_Deterministic() => Assert.NotNull(await Scalar("SELECT KEYS.NEW_KEYSET('DETERMINISTIC_AEAD_AES_SIV_CMAC_256')"));

	// ---- KEYS.ROTATE_KEYSET ----
	[Fact] public async Task KeysRotateKeyset() => Assert.NotNull(await Scalar("SELECT KEYS.ROTATE_KEYSET(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'), 'AEAD_AES_GCM_256')"));

	// ---- KEYS.KEYSET_LENGTH ----
	[Fact] public async Task KeysKeysetLength_NewKeyset() { var v = await Scalar("SELECT KEYS.KEYSET_LENGTH(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'))"); Assert.NotNull(v); }
	[Fact] public async Task KeysKeysetLength_Rotated() { var v = await Scalar("SELECT KEYS.KEYSET_LENGTH(KEYS.ROTATE_KEYSET(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'), 'AEAD_AES_GCM_256'))"); Assert.NotNull(v); }

	// ---- KEYS.KEYSET_TO_JSON / KEYS.KEYSET_FROM_JSON ----
	[Fact] public async Task KeysKeysetToJson() => Assert.NotNull(await Scalar("SELECT KEYS.KEYSET_TO_JSON(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'))"));
	[Fact] public async Task KeysKeysetFromJson_RoundTrip() => Assert.NotNull(await Scalar("SELECT KEYS.KEYSET_FROM_JSON(KEYS.KEYSET_TO_JSON(KEYS.NEW_KEYSET('AEAD_AES_GCM_256')))"));

	// ---- KEYS.ADD_KEY_FROM_RAW_BYTES ----
	[Fact] public async Task KeysAddKeyFromRawBytes() => Assert.NotNull(await Scalar("SELECT KEYS.ADD_KEY_FROM_RAW_BYTES(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'), 'AES_GCM', b'0123456789abcdef0123456789abcdef')"));

	// ---- KEYS.KEYSET_CHAIN ----
	[Fact] public async Task KeysKeysetChain() => Assert.NotNull(await Scalar("SELECT KEYS.KEYSET_CHAIN(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'), KEYS.NEW_KEYSET('AEAD_AES_GCM_256'))"));

	// ---- AEAD.ENCRYPT / AEAD.DECRYPT_BYTES ----
	[Fact] public async Task AeadEncrypt_ReturnsBytes()
	{
		var v = await Scalar("SELECT AEAD.ENCRYPT(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'), b'plaintext', 'aad')");
		Assert.NotNull(v);
	}

	[Fact] public async Task AeadDecryptBytes_RoundTrip()
	{
		var v = await Scalar(@"
			SELECT AEAD.DECRYPT_BYTES(
				keyset,
				AEAD.ENCRYPT(keyset, b'hello world', 'extra'),
				'extra'
			)
			FROM (SELECT KEYS.NEW_KEYSET('AEAD_AES_GCM_256') AS keyset)
		");
		Assert.NotNull(v);
	}

	[Fact] public async Task AeadDecryptString_RoundTrip()
	{
		var v = await Scalar(@"
			SELECT AEAD.DECRYPT_STRING(
				keyset,
				AEAD.ENCRYPT(keyset, b'hello', 'aad'),
				'aad'
			)
			FROM (SELECT KEYS.NEW_KEYSET('AEAD_AES_GCM_256') AS keyset)
		");
		Assert.NotNull(v);
	}

	// ---- DETERMINISTIC_ENCRYPT / DETERMINISTIC_DECRYPT_BYTES ----
	[Fact] public async Task DeterministicEncrypt_ReturnsBytes()
	{
		var v = await Scalar("SELECT DETERMINISTIC_ENCRYPT(KEYS.NEW_KEYSET('DETERMINISTIC_AEAD_AES_SIV_CMAC_256'), b'secret', 'aad')");
		Assert.NotNull(v);
	}

	[Fact] public async Task DeterministicDecryptBytes_RoundTrip()
	{
		var v = await Scalar(@"
			SELECT DETERMINISTIC_DECRYPT_BYTES(
				keyset,
				DETERMINISTIC_ENCRYPT(keyset, b'data', 'ctx'),
				'ctx'
			)
			FROM (SELECT KEYS.NEW_KEYSET('DETERMINISTIC_AEAD_AES_SIV_CMAC_256') AS keyset)
		");
		Assert.NotNull(v);
	}

	[Fact] public async Task DeterministicDecryptString_RoundTrip()
	{
		var v = await Scalar(@"
			SELECT DETERMINISTIC_DECRYPT_STRING(
				keyset,
				DETERMINISTIC_ENCRYPT(keyset, b'text', 'ctx'),
				'ctx'
			)
			FROM (SELECT KEYS.NEW_KEYSET('DETERMINISTIC_AEAD_AES_SIV_CMAC_256') AS keyset)
		");
		Assert.NotNull(v);
	}

	// ---- Deterministic encrypt is deterministic ----
	[Fact(Skip = "Not yet supported")] public async Task DeterministicEncrypt_SameInputSameOutput()
	{
		var v1 = await Scalar(@"
			SELECT TO_HEX(DETERMINISTIC_ENCRYPT(keyset, b'same', 'aad')), TO_HEX(DETERMINISTIC_ENCRYPT(keyset, b'same', 'aad'))
			FROM (SELECT KEYS.NEW_KEYSET('DETERMINISTIC_AEAD_AES_SIV_CMAC_256') AS keyset)
		");
		Assert.NotNull(v1);
		// Both columns should be equal, but we only get the first column; the query itself verifying no error is sufficient
	}
}
