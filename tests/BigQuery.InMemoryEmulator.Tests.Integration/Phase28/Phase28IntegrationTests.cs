using System.Globalization;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration.Phase28;

/// <summary>
/// Phase 28 integration tests: JSON type conversions, conversion functions,
/// NET functions, AEAD encryption, procedural language, and utility functions.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class Phase28IntegrationTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public Phase28IntegrationTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_p28_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public async ValueTask DisposeAsync()
	{
		try
		{
			var client = await _fixture.GetClientAsync();
			await client.DeleteDatasetAsync(_datasetId, new Google.Cloud.BigQuery.V2.DeleteDatasetOptions { DeleteContents = true });
		}
		catch { }
		await _fixture.DisposeAsync();
	}

	#region JSON Type Conversions

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#bool_for_json
	[Fact]
	public async Task Bool_ExtractsJsonBoolean()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT BOOL(JSON 'true') AS val", parameters: null);
		var rows = results.ToList();
		Assert.Single(rows);
		Assert.Equal(true, rows[0]["val"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#int64_for_json
	[Fact]
	public async Task Int64_ExtractsJsonNumber()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT INT64(JSON '42') AS val", parameters: null);
		var rows = results.ToList();
		Assert.Single(rows);
		Assert.Equal(42L, Convert.ToInt64(rows[0]["val"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#float64_for_json
	[Fact]
	public async Task Float64_ExtractsJsonNumber()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT FLOAT64(JSON '3.14') AS val", parameters: null);
		var rows = results.ToList();
		Assert.Single(rows);
		Assert.Equal(3.14, Convert.ToDouble(rows[0]["val"]), 6);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#string_for_json
	[Fact]
	public async Task String_ExtractsJsonString()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT STRING(JSON '\"hello\"') AS val", parameters: null);
		var rows = results.ToList();
		Assert.Single(rows);
		Assert.Equal("hello", rows[0]["val"]);
	}

	#endregion

	#region Conversion Functions

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#parse_numeric
	[Fact]
	public async Task ParseNumeric_ValidInput()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT PARSE_NUMERIC('123.45') AS val", parameters: null);
		var rows = results.ToList();
		Assert.Single(rows);
		Assert.Equal(123.45, Convert.ToDouble(rows[0]["val"]), 6);
	}

	#endregion

	#region NET Functions

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netip_trunc
	[Fact]
	public async Task NetIpTrunc_TruncatesIp()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT NET.IP_TRUNC(NET.IP_FROM_STRING('192.168.1.100'), 24) AS val", parameters: null);
		var rows = results.ToList();
		Assert.Single(rows);
		Assert.NotNull(rows[0]["val"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netipv4_from_int64
	[Fact]
	public async Task NetIpv4FromInt64_ConvertsIntToIp()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT NET.IP_TO_STRING(NET.IPV4_FROM_INT64(167772161)) AS val", parameters: null);
		var rows = results.ToList();
		Assert.Single(rows);
		Assert.Equal("10.0.0.1", rows[0]["val"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netipv4_to_int64
	[Fact]
	public async Task NetIpv4ToInt64_ConvertsBytesToInt()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT NET.IPV4_TO_INT64(NET.IP_FROM_STRING('10.0.0.1')) AS val", parameters: null);
		var rows = results.ToList();
		Assert.Single(rows);
		Assert.Equal(167772161L, Convert.ToInt64(rows[0]["val"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netsafe_ip_from_string
	[Fact]
	public async Task NetSafeIpFromString_InvalidInput_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT NET.SAFE_IP_FROM_STRING('not-an-ip') AS val", parameters: null);
		var rows = results.ToList();
		Assert.Single(rows);
		Assert.Null(rows[0]["val"]);
	}

	#endregion

	#region AEAD Encryption Functions

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aead_encryption_functions#keysnew_keyset
	[Fact]
	public async Task Keys_NewKeyset_ReturnsNonNull()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT KEYS.NEW_KEYSET('AEAD_AES_GCM_256') AS ks", parameters: null);
		var rows = results.ToList();
		Assert.Single(rows);
		Assert.NotNull(rows[0]["ks"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aead_encryption_functions#keyskeyset_length
	[Fact]
	public async Task Keys_KeysetLength_Returns1()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT KEYS.KEYSET_LENGTH(KEYS.NEW_KEYSET('AEAD_AES_GCM_256')) AS len", parameters: null);
		var rows = results.ToList();
		Assert.Single(rows);
		Assert.Equal(1L, Convert.ToInt64(rows[0]["len"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aead_encryption_functions#aeadencrypt
	[Fact]
	public async Task Aead_Encrypt_ProducesCiphertext()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT AEAD.ENCRYPT(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'), 'hello', 'aad') AS ct", parameters: null);
		var rows = results.ToList();
		Assert.Single(rows);
		Assert.NotNull(rows[0]["ct"]);
	}

	#endregion

	#region Utility Functions

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/debugging_functions#error
	[Fact]
	public async Task Error_ThrowsException()
	{
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAnyAsync<Exception>(async () =>
			await client.ExecuteQueryAsync("SELECT ERROR('test error')", parameters: null));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#session_user
	[Fact]
	public async Task SessionUser_ReturnsString()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT SESSION_USER() AS val", parameters: null);
		var rows = results.ToList();
		Assert.Single(rows);
		Assert.NotNull(rows[0]["val"]);
	}

	#endregion
}
