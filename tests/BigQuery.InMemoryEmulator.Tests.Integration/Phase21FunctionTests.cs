using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for Phase 21: Missing Function Categories.
/// Covers JSON array/keys/set/strip/type, regex instr/substr, bit, net, HLL, hash functions.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class Phase21FunctionTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public Phase21FunctionTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_p21_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public async ValueTask DisposeAsync()
	{
		try
		{
			var client = await _fixture.GetClientAsync();
			await client.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true });
		}
		catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<BigQueryResults> Query(string sql) =>
		await (await _fixture.GetClientAsync()).ExecuteQueryAsync(sql, parameters: null);

	// ======================================================================
	// JSON_EXTRACT_ARRAY / JSON_QUERY_ARRAY
	// ======================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_extract_array
	//   "Extracts a JSON array and returns it as a SQL ARRAY<JSON>."
	// Note: Array values can't be read through SDK BigQueryRow directly (known limitation).
	// We verify via ARRAY_LENGTH instead.
	[Fact]
	public async Task JsonExtractArray_ReturnsArrayElements()
	{
		var results = await Query(
			@"SELECT ARRAY_LENGTH(JSON_EXTRACT_ARRAY('{""items"":[1,2,3]}', '$.items')) AS cnt");
		var rows = results.ToList();
		Assert.Single(rows);
		Assert.Equal(3L, Convert.ToInt64(rows[0]["cnt"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_extract_array
	[Fact]
	public async Task JsonExtractArray_NullInput_ReturnsNull()
	{
		var results = await Query(
			@"SELECT JSON_EXTRACT_ARRAY(NULL, '$.items') AS arr");
		var rows = results.ToList();
		Assert.Null(rows[0]["arr"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_extract_array
	[Fact]
	public async Task JsonQueryArray_SameAsExtractArray()
	{
		var results = await Query(
			@"SELECT ARRAY_LENGTH(JSON_QUERY_ARRAY('{""a"":[10,20]}', '$.a')) AS cnt");
		var rows = results.ToList();
		Assert.Equal(2L, Convert.ToInt64(rows[0]["cnt"]));
	}

	// ======================================================================
	// JSON_EXTRACT_STRING_ARRAY / JSON_VALUE_ARRAY
	// ======================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_value_array
	//   "Extracts a JSON array of scalar values and returns a SQL ARRAY<STRING>."
	[Fact]
	public async Task JsonValueArray_ReturnsStringArray()
	{
		var results = await Query(
			@"SELECT ARRAY_LENGTH(JSON_VALUE_ARRAY('{""tags"":[""a"",""b"",""c""]}', '$.tags')) AS cnt");
		var rows = results.ToList();
		Assert.Equal(3L, Convert.ToInt64(rows[0]["cnt"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_value_array
	[Fact]
	public async Task JsonExtractStringArray_SameAsValueArray()
	{
		var results = await Query(
			@"SELECT ARRAY_LENGTH(JSON_EXTRACT_STRING_ARRAY('{""tags"":[""x"",""y""]}', '$.tags')) AS cnt");
		var rows = results.ToList();
		Assert.Equal(2L, Convert.ToInt64(rows[0]["cnt"]));
	}

	// ======================================================================
	// JSON_KEYS
	// ======================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_keys
	//   "Returns the keys of the outermost JSON object as a SQL ARRAY<STRING>."
	// Note: Array values can't be read through SDK BigQueryRow directly (known limitation).
	[Fact]
	public async Task JsonKeys_ReturnsPropertyNames()
	{
		var results = await Query(
			@"SELECT ARRAY_LENGTH(JSON_KEYS('{""name"":""Alice"",""age"":30}')) AS cnt");
		var rows = results.ToList();
		Assert.Equal(2L, Convert.ToInt64(rows[0]["cnt"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_keys
	[Fact]
	public async Task JsonKeys_NullInput_ReturnsNull()
	{
		var results = await Query("SELECT JSON_KEYS(NULL) AS keys");
		var rows = results.ToList();
		Assert.Null(rows[0]["keys"]);
	}

	// ======================================================================
	// JSON_SET
	// ======================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_set
	//   "Produces a new SQL JSON value with the specified JSON data inserted or replaced."
	[Fact]
	public async Task JsonSet_SetsValue()
	{
		var results = await Query(
			@"SELECT JSON_SET('{""a"":1}', '$.b', '2') AS j");
		var rows = results.ToList();
		var val = rows[0]["j"]?.ToString();
		Assert.NotNull(val);
		Assert.Contains("b", val);
	}

	// ======================================================================
	// JSON_STRIP_NULLS
	// ======================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_strip_nulls
	//   "Removes all members that have NULL values from a JSON value."
	[Fact]
	public async Task JsonStripNulls_RemovesNullProperties()
	{
		var results = await Query(
			@"SELECT JSON_STRIP_NULLS('{""a"":1,""b"":null,""c"":3}') AS j");
		var rows = results.ToList();
		var val = rows[0]["j"]?.ToString();
		Assert.NotNull(val);
		Assert.DoesNotContain("null", val!);
	}

	// ======================================================================
	// JSON_TYPE
	// ======================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_type
	//   "Returns a STRING value that represents the JSON type."
	[Fact]
	public async Task JsonType_ReturnsObject()
	{
		var results = await Query(
			@"SELECT JSON_TYPE('{""a"":1}') AS t");
		var rows = results.ToList();
		Assert.Equal("object", (string)rows[0]["t"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_type
	[Fact]
	public async Task JsonType_ReturnsArray()
	{
		var results = await Query(@"SELECT JSON_TYPE('[1,2,3]') AS t");
		var rows = results.ToList();
		Assert.Equal("array", (string)rows[0]["t"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_type
	[Fact]
	public async Task JsonType_ReturnsString()
	{
		var results = await Query(@"SELECT JSON_TYPE('""hello""') AS t");
		var rows = results.ToList();
		Assert.Equal("string", (string)rows[0]["t"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_type
	[Fact]
	public async Task JsonType_ReturnsNumber()
	{
		var results = await Query(@"SELECT JSON_TYPE('42') AS t");
		var rows = results.ToList();
		Assert.Equal("number", (string)rows[0]["t"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_type
	[Fact]
	public async Task JsonType_ReturnsBoolean()
	{
		var results = await Query(@"SELECT JSON_TYPE('true') AS t");
		var rows = results.ToList();
		Assert.Equal("boolean", (string)rows[0]["t"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_type
	[Fact]
	public async Task JsonType_ReturnsNull()
	{
		var results = await Query(@"SELECT JSON_TYPE('null') AS t");
		var rows = results.ToList();
		Assert.Equal("null", (string)rows[0]["t"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_type
	[Fact]
	public async Task JsonType_NullInput_ReturnsNull()
	{
		var results = await Query("SELECT JSON_TYPE(NULL) AS t");
		var rows = results.ToList();
		Assert.Null(rows[0]["t"]);
	}

	// ======================================================================
	// REGEXP_INSTR
	// ======================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_instr
	//   "Returns the 1-based position of the nth occurrence of a regex pattern."
	[Fact]
	public async Task RegexpInstr_FindsPosition()
	{
		var results = await Query(
			"SELECT REGEXP_INSTR('abcdef', 'cd') AS pos");
		var rows = results.ToList();
		Assert.Equal(3L, Convert.ToInt64(rows[0]["pos"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_instr
	[Fact]
	public async Task RegexpInstr_NoMatch_ReturnsZero()
	{
		var results = await Query(
			"SELECT REGEXP_INSTR('abcdef', 'xyz') AS pos");
		var rows = results.ToList();
		Assert.Equal(0L, Convert.ToInt64(rows[0]["pos"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_instr
	[Fact]
	public async Task RegexpInstr_NullInput_ReturnsNull()
	{
		var results = await Query("SELECT REGEXP_INSTR(NULL, 'a') AS pos");
		var rows = results.ToList();
		Assert.Null(rows[0]["pos"]);
	}

	// ======================================================================
	// REGEXP_SUBSTR
	// ======================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_substr
	//   "Synonym for REGEXP_EXTRACT. Returns the substring that matches a regex."
	[Fact]
	public async Task RegexpSubstr_ReturnsMatch()
	{
		var results = await Query(
			@"SELECT REGEXP_SUBSTR('hello world 123', '[0-9]+') AS m");
		var rows = results.ToList();
		Assert.Equal("123", (string)rows[0]["m"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_substr
	[Fact]
	public async Task RegexpSubstr_NoMatch_ReturnsNull()
	{
		var results = await Query(
			@"SELECT REGEXP_SUBSTR('abcdef', '[0-9]+') AS m");
		var rows = results.ToList();
		Assert.Null(rows[0]["m"]);
	}

	// ======================================================================
	// BIT_COUNT
	// ======================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/bit_functions#bit_count
	//   "Returns the number of bits that are set in the input expression."
	[Fact]
	public async Task BitCount_ReturnsPopulationCount()
	{
		var results = await Query("SELECT BIT_COUNT(7) AS cnt");
		var rows = results.ToList();
		Assert.Equal(3L, Convert.ToInt64(rows[0]["cnt"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/bit_functions#bit_count
	[Fact]
	public async Task BitCount_Zero_ReturnsZero()
	{
		var results = await Query("SELECT BIT_COUNT(0) AS cnt");
		var rows = results.ToList();
		Assert.Equal(0L, Convert.ToInt64(rows[0]["cnt"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/bit_functions#bit_count
	[Fact]
	public async Task BitCount_PowerOfTwo()
	{
		var results = await Query("SELECT BIT_COUNT(256) AS cnt");
		var rows = results.ToList();
		Assert.Equal(1L, Convert.ToInt64(rows[0]["cnt"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/bit_functions#bit_count
	[Fact]
	public async Task BitCount_NullInput_ReturnsNull()
	{
		var results = await Query("SELECT BIT_COUNT(NULL) AS cnt");
		var rows = results.ToList();
		Assert.Null(rows[0]["cnt"]);
	}

	// ======================================================================
	// NET functions (NET.HOST, NET.PUBLIC_SUFFIX, NET.REG_DOMAIN)
	// ======================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#nethost
	//   "Takes a URL as a STRING value and returns the host."
	[Fact]
	public async Task NetHost_ExtractsHost()
	{
		var results = await Query(
			"SELECT NET.HOST('https://www.google.com/search?q=test') AS h");
		var rows = results.ToList();
		Assert.Equal("www.google.com", (string)rows[0]["h"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#nethost
	[Fact]
	public async Task NetHost_NullInput_ReturnsNull()
	{
		var results = await Query("SELECT NET.HOST(NULL) AS h");
		var rows = results.ToList();
		Assert.Null(rows[0]["h"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netpublic_suffix
	//   "Takes a URL as a STRING and returns the public suffix."
	[Fact]
	public async Task NetPublicSuffix_ReturnsSuffix()
	{
		var results = await Query(
			"SELECT NET.PUBLIC_SUFFIX('https://www.google.com/page') AS s");
		var rows = results.ToList();
		Assert.Equal("com", (string)rows[0]["s"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netreg_domain
	//   "Takes a URL and returns the registered or registerable domain."
	[Fact]
	public async Task NetRegDomain_ReturnsDomain()
	{
		var results = await Query(
			"SELECT NET.REG_DOMAIN('https://www.google.com/page') AS d");
		var rows = results.ToList();
		Assert.Equal("google.com", (string)rows[0]["d"]);
	}

	// ======================================================================
	// Hash functions (MD5, SHA1, SHA256, SHA512, FARM_FINGERPRINT)
	// ======================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#md5
	//   "Computes the hash of the input using the MD5 algorithm."
	[Fact]
	public async Task Md5_ReturnsHash()
	{
		var results = await Query(
			"SELECT TO_HEX(MD5('hello')) AS h");
		var rows = results.ToList();
		Assert.Equal("5d41402abc4b2a76b9719d911017c592", ((string)rows[0]["h"]).ToLower());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#sha256
	//   "Computes the hash of the input using the SHA-256 algorithm."
	[Fact]
	public async Task Sha256_ReturnsHash()
	{
		var results = await Query(
			"SELECT TO_HEX(SHA256('hello')) AS h");
		var rows = results.ToList();
		Assert.Equal("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
			((string)rows[0]["h"]).ToLower());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#sha512
	//   "Computes the hash of the input using the SHA-512 algorithm."
	[Fact]
	public async Task Sha512_ReturnsHash()
	{
		// SHA-512 produces 64 bytes
		var results = await Query(
			"SELECT LENGTH(SHA512('hello')) AS len");
		var rows = results.ToList();
		Assert.Equal(64L, Convert.ToInt64(rows[0]["len"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#farm_fingerprint
	//   "Computes the fingerprint of the STRING or BYTES input using the Fingerprint64 function."
	[Fact]
	public async Task FarmFingerprint_ReturnsInteger()
	{
		var results = await Query(
			"SELECT FARM_FINGERPRINT('hello') AS fp");
		var rows = results.ToList();
		Assert.NotNull(rows[0]["fp"]);
		// Should be a non-zero integer
		Assert.NotEqual(0L, Convert.ToInt64(rows[0]["fp"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#md5
	[Fact]
	public async Task Hash_NullInput_ReturnsNull()
	{
		var results = await Query("SELECT MD5(NULL) AS h");
		var rows = results.ToList();
		Assert.Null(rows[0]["h"]);
	}

	// ======================================================================
	// HLL_COUNT functions
	// ======================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_count_functions#hll_countextract
	//   "Extracts a cardinality estimate of a single HLL++ sketch."
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task HllCountExtract_Merge_ReturnsCount()
	{
		var client = await _fixture.GetClientAsync();
		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "val", Type = "STRING", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "hll_data", schema);
		await client.InsertRowsAsync(_datasetId, "hll_data", new[]
		{
			new BigQueryInsertRow("r1") { ["id"] = 1, ["val"] = "a" },
			new BigQueryInsertRow("r2") { ["id"] = 2, ["val"] = "b" },
			new BigQueryInsertRow("r3") { ["id"] = 3, ["val"] = "a" },
			new BigQueryInsertRow("r4") { ["id"] = 4, ["val"] = "c" },
		});

		// In-memory: HLL functions approximate exact counting
		var results = await client.ExecuteQueryAsync(
			$"SELECT HLL_COUNT.EXTRACT(HLL_COUNT.INIT(val)) AS approx_cnt FROM `{_datasetId}.hll_data`",
			parameters: null);
		var rows = results.ToList();
		Assert.True(rows.Count > 0);
	}
}
