using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase28;

/// <summary>
/// Phase 28: AEAD encryption functions — KEYS.*, AEAD.*, DETERMINISTIC_*.
/// These are implemented with real AES-GCM/AES-SIV encryption using .NET crypto APIs.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aead_encryption_functions
/// </summary>
public class AeadEncryptionTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		store.Datasets["ds"] = new InMemoryDataset("ds");
		return new QueryExecutor(store, "ds");
	}

	private static (SqlEngine.ProceduralExecutor Exec, InMemoryDataStore Store) CreateProcExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		store.Datasets["ds"] = new InMemoryDataset("ds");
		return (new SqlEngine.ProceduralExecutor(store, "ds"), store);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aead_encryption_functions#keysnew_keyset
	//   "Returns a serialized keyset containing a new key based on key_type."
	[Fact]
	public void Keys_NewKeyset_ReturnsNonNullBytes()
	{
		var exec = CreateExecutor();
		var result = exec.Execute("SELECT KEYS.NEW_KEYSET('AEAD_AES_GCM_256') AS ks");
		Assert.Single(result.Rows);
		Assert.NotNull(result.Rows[0].F[0].V);
	}

	[Fact]
	public void Keys_NewKeyset_DifferentCallsProduceDifferentKeys()
	{
		var exec = CreateExecutor();
		var r1 = exec.Execute("SELECT KEYS.NEW_KEYSET('AEAD_AES_GCM_256') AS ks");
		var r2 = exec.Execute("SELECT KEYS.NEW_KEYSET('AEAD_AES_GCM_256') AS ks");
		// Different keysets should be different (with very high probability)
		Assert.NotEqual(r1.Rows[0].F[0].V?.ToString(), r2.Rows[0].F[0].V?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aead_encryption_functions#aeadencrypt
	//   "Encrypts plaintext using the primary cryptographic key in keyset."
	[Fact]
	public void Aead_Encrypt_ProducesCiphertext()
	{
		var exec = CreateExecutor();
		var result = exec.Execute(@"
			SELECT AEAD.ENCRYPT(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'), b'hello', b'aad') AS ct
		");
		Assert.Single(result.Rows);
		Assert.NotNull(result.Rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aead_encryption_functions#aeaddecrypt_bytes
	//   "Uses the matching key from keyset to decrypt ciphertext."
	[Fact]
	public void Aead_EncryptDecryptBytes_RoundTrip()
	{
		var (exec, _) = CreateProcExecutor();
		var result = exec.Execute(@"
			DECLARE ks STRING;
			DECLARE ct STRING;
			SET ks = KEYS.NEW_KEYSET('AEAD_AES_GCM_256');
			SET ct = AEAD.ENCRYPT(ks, 'secret', 'context');
			SELECT AEAD.DECRYPT_BYTES(ks, ct, 'context') AS pt;
		");
		Assert.Single(result.Rows);
		Assert.NotNull(result.Rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aead_encryption_functions#aeaddecrypt_string
	//   "Like AEAD.DECRYPT_BYTES, but returns STRING."
	[Fact]
	public void Aead_EncryptDecryptString_RoundTrip()
	{
		var (exec, _) = CreateProcExecutor();
		var result = exec.Execute(@"
			DECLARE ks STRING;
			DECLARE ct STRING;
			SET ks = KEYS.NEW_KEYSET('AEAD_AES_GCM_256');
			SET ct = AEAD.ENCRYPT(ks, 'hello world', 'aad');
			SELECT AEAD.DECRYPT_STRING(ks, ct, 'aad') AS pt;
		");
		Assert.Single(result.Rows);
		Assert.Equal("hello world", result.Rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aead_encryption_functions#keysrotate_keyset
	//   "Adds a new primary cryptographic key to a keyset."
	[Fact]
	public void Keys_RotateKeyset_ReturnsBytes()
	{
		var exec = CreateExecutor();
		var result = exec.Execute("SELECT KEYS.ROTATE_KEYSET(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'), 'AEAD_AES_GCM_256') AS ks");
		Assert.Single(result.Rows);
		Assert.NotNull(result.Rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aead_encryption_functions#keyskeyset_length
	//   "Gets the number of keys in the provided keyset."
	[Fact]
	public void Keys_KeysetLength_NewKeyset_Returns1()
	{
		var exec = CreateExecutor();
		var result = exec.Execute("SELECT KEYS.KEYSET_LENGTH(KEYS.NEW_KEYSET('AEAD_AES_GCM_256')) AS len");
		Assert.Single(result.Rows);
		Assert.Equal("1", result.Rows[0].F[0].V);
	}

	[Fact]
	public void Keys_KeysetLength_RotatedKeyset_Returns2()
	{
		var exec = CreateExecutor();
		var result = exec.Execute("SELECT KEYS.KEYSET_LENGTH(KEYS.ROTATE_KEYSET(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'), 'AEAD_AES_GCM_256')) AS len");
		Assert.Single(result.Rows);
		Assert.Equal("2", result.Rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aead_encryption_functions#keyskeyset_to_json
	//   "Gets a JSON STRING representation of a keyset."
	[Fact]
	public void Keys_KeysetToJson_ReturnsJsonString()
	{
		var exec = CreateExecutor();
		var result = exec.Execute("SELECT KEYS.KEYSET_TO_JSON(KEYS.NEW_KEYSET('AEAD_AES_GCM_256')) AS j");
		Assert.Single(result.Rows);
		var json = result.Rows[0].F[0].V?.ToString();
		Assert.NotNull(json);
		Assert.Contains("primaryKeyId", json);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aead_encryption_functions#keyskeyset_from_json
	//   "Returns the input json_keyset STRING as serialized BYTES."
	[Fact]
	public void Keys_KeysetFromJson_RoundTrip()
	{
		var exec = CreateExecutor();
		var result = exec.Execute(@"
			SELECT KEYS.KEYSET_LENGTH(
				KEYS.KEYSET_FROM_JSON(
					KEYS.KEYSET_TO_JSON(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'))
				)
			) AS len
		");
		Assert.Single(result.Rows);
		Assert.Equal("1", result.Rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aead_encryption_functions#keysadd_key_from_raw_bytes
	//   "Adds a key to a keyset."
	[Fact]
	public void Keys_AddKeyFromRawBytes_IncreasesKeyCount()
	{
		var exec = CreateExecutor();
		var result = exec.Execute(@"
			SELECT KEYS.KEYSET_LENGTH(
				KEYS.ADD_KEY_FROM_RAW_BYTES(
					KEYS.NEW_KEYSET('AEAD_AES_GCM_256'),
					'AES_GCM',
					b'0123456789012345'
				)
			) AS len
		");
		Assert.Single(result.Rows);
		Assert.Equal("2", result.Rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aead_encryption_functions#keyskeyset_chain
	//   "Produces a Tink keyset that is encrypted with a Cloud KMS key."
	[Fact]
	public void Keys_KeysetChain_ReturnsNonNull()
	{
		var exec = CreateExecutor();
		// In-memory: KEYS.KEYSET_CHAIN just passes-through the keyset (no real KMS)
		var result = exec.Execute("SELECT KEYS.KEYSET_CHAIN('gcp-kms://fake', KEYS.NEW_KEYSET('AEAD_AES_GCM_256')) AS ks");
		Assert.Single(result.Rows);
		Assert.NotNull(result.Rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aead_encryption_functions#deterministic_encrypt
	//   "Encrypts plaintext using deterministic AEAD encryption."
	[Fact]
	public void Deterministic_EncryptDecryptString_RoundTrip()
	{
		var (exec, _) = CreateProcExecutor();
		var result = exec.Execute(@"
			DECLARE ks STRING;
			DECLARE ct STRING;
			SET ks = KEYS.NEW_KEYSET('DETERMINISTIC_AEAD_AES_SIV_CMAC_256');
			SET ct = DETERMINISTIC_ENCRYPT(ks, 'hello', 'aad');
			SELECT DETERMINISTIC_DECRYPT_STRING(ks, ct, 'aad') AS pt;
		");
		Assert.Single(result.Rows);
		Assert.Equal("hello", result.Rows[0].F[0].V);
	}

	[Fact]
	public void Deterministic_Encrypt_SameInputSameOutput()
	{
		var (exec, _) = CreateProcExecutor();
		var result = exec.Execute(@"
			DECLARE ks STRING;
			SET ks = KEYS.NEW_KEYSET('DETERMINISTIC_AEAD_AES_SIV_CMAC_256');
			SELECT
				DETERMINISTIC_ENCRYPT(ks, 'hello', 'aad') AS ct1,
				DETERMINISTIC_ENCRYPT(ks, 'hello', 'aad') AS ct2;
		");
		Assert.Single(result.Rows);
		Assert.Equal(result.Rows[0].F[0].V, result.Rows[0].F[1].V);
	}

	[Fact]
	public void Deterministic_DecryptBytes_RoundTrip()
	{
		var (exec, _) = CreateProcExecutor();
		var result = exec.Execute(@"
			DECLARE ks STRING;
			DECLARE ct STRING;
			SET ks = KEYS.NEW_KEYSET('DETERMINISTIC_AEAD_AES_SIV_CMAC_256');
			SET ct = DETERMINISTIC_ENCRYPT(ks, 'binary', 'aad');
			SELECT DETERMINISTIC_DECRYPT_BYTES(ks, ct, 'aad') AS pt;
		");
		Assert.Single(result.Rows);
		Assert.NotNull(result.Rows[0].F[0].V);
	}

	[Fact]
	public void Aead_Encrypt_NullInput_ReturnsNull()
	{
		var exec = CreateExecutor();
		var result = exec.Execute("SELECT AEAD.ENCRYPT(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'), NULL, b'aad') AS ct");
		Assert.Single(result.Rows);
		Assert.Null(result.Rows[0].F[0].V);
	}
}
