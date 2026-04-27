using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for missing string functions (Phase 24).
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class MissingStringFunctionIntegrationTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public MissingStringFunctionIntegrationTests(BigQuerySession session)
	{
		_session = session;
	}

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_strfn_{Guid.NewGuid():N}"[..30];
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

	#region BYTE_LENGTH / OCTET_LENGTH

	[Fact]
	public async Task ByteLength_ReturnsUtf8ByteCount()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT BYTE_LENGTH('hello') AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("5", rows[0]["result"].ToString());
	}

	[Fact]
	public async Task OctetLength_IsAlias()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT OCTET_LENGTH('hello') AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("5", rows[0]["result"].ToString());
	}

	#endregion

	#region UNICODE

	[Fact]
	public async Task Unicode_ReturnsCodePoint()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT UNICODE('A') AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("65", rows[0]["result"].ToString());
	}

	#endregion

	#region INITCAP

	[Fact]
	public async Task Initcap_CapitalizesWords()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT INITCAP('hello world') AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("Hello World", (string)rows[0]["result"]);
	}

	#endregion

	#region TRANSLATE

	[Fact]
	public async Task Translate_ReplacesCharacters()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT TRANSLATE('abcabc', 'abc', 'xyz') AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("xyzxyz", (string)rows[0]["result"]);
	}

	[Fact]
	public async Task Translate_OmitsWhenTargetShorter()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT TRANSLATE('abcabc', 'abc', 'x') AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("xx", (string)rows[0]["result"]);
	}

	#endregion

	#region SOUNDEX

	[Fact]
	public async Task Soundex_Ashcraft()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT SOUNDEX('Ashcraft') AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("A261", (string)rows[0]["result"]);
	}

	[Fact]
	public async Task Soundex_Robert()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT SOUNDEX('Robert') AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("R163", (string)rows[0]["result"]);
	}

	#endregion

	#region REGEXP_EXTRACT_ALL

	[Fact]
	public async Task RegexpExtractAll_ReturnsAllMatches()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT ARRAY_TO_STRING(REGEXP_EXTRACT_ALL('abc 123 def 456', '\\d+'), ',') AS result",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal("123,456", (string)rows[0]["result"]);
	}

	#endregion

	#region NORMALIZE / NORMALIZE_AND_CASEFOLD

	[Fact]
	public async Task Normalize_ReturnsNormalizedString()
	{
		var client = await _fixture.GetClientAsync();
		// é composed (U+00E9) stays as é after NFC normalization
		var results = await client.ExecuteQueryAsync("SELECT NORMALIZE('\u00e9') AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("\u00e9", (string)rows[0]["result"]);
	}

	[Fact]
	public async Task NormalizeAndCasefold_LowerCases()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT NORMALIZE_AND_CASEFOLD('ABc') AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("abc", (string)rows[0]["result"]);
	}

	#endregion

	#region COLLATE

	[Fact]
	public async Task Collate_ReturnsValue()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT COLLATE('hello', 'und:ci') AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("hello", (string)rows[0]["result"]);
	}

	#endregion

	#region TO_CODE_POINTS / CODE_POINTS_TO_STRING

	[Fact]
	public async Task ToCodePoints_ReturnsArray()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT ARRAY_TO_STRING(TO_CODE_POINTS('ABC'), ',') AS result",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal("65,66,67", (string)rows[0]["result"]);
	}

	[Fact]
	public async Task CodePointsToString_ReturnsString()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT CODE_POINTS_TO_STRING([65, 66, 67]) AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("ABC", (string)rows[0]["result"]);
	}

	#endregion

	#region NULL handling

	[Fact]
	public async Task ByteLength_Null_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT BYTE_LENGTH(NULL) AS result", parameters: null);
		var rows = results.ToList();
		Assert.Null(rows[0]["result"]);
	}

	[Fact]
	public async Task Soundex_Null_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT SOUNDEX(NULL) AS result", parameters: null);
		var rows = results.ToList();
		Assert.Null(rows[0]["result"]);
	}

	#endregion
}
