using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for JSON function patterns including JSON_EXTRACT, JSON_QUERY, JSON_VALUE, TO_JSON_STRING.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class JsonFunctionPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public JsonFunctionPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_json_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.events` (id INT64, payload STRING)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.events` (id, payload) VALUES
			(1, '{{""name"":""click"",""user"":""alice"",""count"":5,""nested"":{{""key"":""val""}}}}'),
			(2, '{{""name"":""view"",""user"":""bob"",""count"":10,""tags"":[""a"",""b"",""c""]}}'),
			(3, '{{""name"":""click"",""user"":""charlie"",""count"":3,""active"":true}}'),
			(4, '{{""name"":""purchase"",""user"":""alice"",""count"":1,""amount"":99.99}}')", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
		return result.ToList();
	}

	// JSON_EXTRACT / JSON_QUERY
	[Fact] public async Task JsonExtract_TopLevel() => Assert.Contains("click", await Scalar(@"SELECT JSON_EXTRACT(payload, '$.name') FROM `{ds}.events` WHERE id = 1") ?? "");
	[Fact] public async Task JsonExtract_Nested() => Assert.Contains("val", await Scalar(@"SELECT JSON_EXTRACT(payload, '$.nested.key') FROM `{ds}.events` WHERE id = 1") ?? "");
	[Fact] public async Task JsonExtract_Array() => Assert.Contains("a", await Scalar(@"SELECT JSON_EXTRACT(payload, '$.tags[0]') FROM `{ds}.events` WHERE id = 2") ?? "");
	[Fact] public async Task JsonExtract_Null() => Assert.Null(await Scalar(@"SELECT JSON_EXTRACT(payload, '$.nonexistent') FROM `{ds}.events` WHERE id = 1"));
	[Fact] public async Task JsonExtract_NullInput() => Assert.Null(await Scalar("SELECT JSON_EXTRACT(NULL, '$.name')"));

	// JSON_EXTRACT_SCALAR / JSON_VALUE  
	[Fact] public async Task JsonExtractScalar_String() => Assert.Equal("click", await Scalar(@"SELECT JSON_EXTRACT_SCALAR(payload, '$.name') FROM `{ds}.events` WHERE id = 1"));
	[Fact] public async Task JsonExtractScalar_Number() => Assert.Equal("5", await Scalar(@"SELECT JSON_EXTRACT_SCALAR(payload, '$.count') FROM `{ds}.events` WHERE id = 1"));
	[Fact] public async Task JsonExtractScalar_Boolean() => Assert.Equal("true", await Scalar(@"SELECT JSON_EXTRACT_SCALAR(payload, '$.active') FROM `{ds}.events` WHERE id = 3"));
	[Fact] public async Task JsonExtractScalar_Nested() => Assert.Equal("val", await Scalar(@"SELECT JSON_EXTRACT_SCALAR(payload, '$.nested.key') FROM `{ds}.events` WHERE id = 1"));
	[Fact] public async Task JsonExtractScalar_Null() => Assert.Null(await Scalar(@"SELECT JSON_EXTRACT_SCALAR(payload, '$.missing') FROM `{ds}.events` WHERE id = 1"));
	[Fact] public async Task JsonExtractScalar_Float() => Assert.Equal("99.99", await Scalar(@"SELECT JSON_EXTRACT_SCALAR(payload, '$.amount') FROM `{ds}.events` WHERE id = 4"));

	// JSON_VALUE (alias for JSON_EXTRACT_SCALAR)
	[Fact] public async Task JsonValue_Basic() => Assert.Equal("alice", await Scalar(@"SELECT JSON_VALUE(payload, '$.user') FROM `{ds}.events` WHERE id = 1"));
	[Fact] public async Task JsonValue_Number() => Assert.Equal("10", await Scalar(@"SELECT JSON_VALUE(payload, '$.count') FROM `{ds}.events` WHERE id = 2"));
	[Fact] public async Task JsonValue_Missing() => Assert.Null(await Scalar(@"SELECT JSON_VALUE(payload, '$.xyz') FROM `{ds}.events` WHERE id = 1"));

	// JSON_EXTRACT_ARRAY
	[Fact] public async Task JsonExtractArray_Basic()
	{
		var result = await Scalar(@"SELECT ARRAY_LENGTH(JSON_EXTRACT_ARRAY(payload, '$.tags')) FROM `{ds}.events` WHERE id = 2");
		Assert.Equal("3", result);
	}

	[Fact] public async Task JsonExtractArray_NonArray() => Assert.Null(await Scalar(@"SELECT JSON_EXTRACT_ARRAY(payload, '$.name') FROM `{ds}.events` WHERE id = 1"));

	// TO_JSON_STRING
	[Fact] public async Task ToJsonString_Int() => Assert.Equal("42", await Scalar("SELECT TO_JSON_STRING(42)"));
	[Fact] public async Task ToJsonString_String() => Assert.Contains("hello", await Scalar("SELECT TO_JSON_STRING('hello')") ?? "");
	[Fact] public async Task ToJsonString_Bool() => Assert.Equal("true", await Scalar("SELECT TO_JSON_STRING(TRUE)"));
	[Fact] public async Task ToJsonString_Null() => Assert.Equal("null", await Scalar("SELECT TO_JSON_STRING(NULL)"));
	[Fact] public async Task ToJsonString_Array() => Assert.Contains("[", await Scalar("SELECT TO_JSON_STRING([1, 2, 3])") ?? "");

	// JSON functions in WHERE
	[Fact] public async Task JsonInWhere_Filter()
	{
		var rows = await Query(@"SELECT id FROM `{ds}.events` WHERE JSON_EXTRACT_SCALAR(payload, '$.name') = 'click' ORDER BY id");
		Assert.Equal(2, rows.Count);
	}

	[Fact] public async Task JsonInWhere_NumericCompare()
	{
		var rows = await Query(@"SELECT id FROM `{ds}.events` WHERE CAST(JSON_EXTRACT_SCALAR(payload, '$.count') AS INT64) > 5 ORDER BY id");
		Assert.Single(rows);
		Assert.Equal("2", rows[0]["id"].ToString());
	}

	// JSON with GROUP BY
	[Fact] public async Task JsonGroupBy()
	{
		var rows = await Query(@"
			SELECT JSON_EXTRACT_SCALAR(payload, '$.name') AS event_name, COUNT(*) AS cnt
			FROM `{ds}.events`
			GROUP BY JSON_EXTRACT_SCALAR(payload, '$.name')
			ORDER BY cnt DESC");
		Assert.Equal("click", rows[0]["event_name"].ToString());
	}

	// Literal JSON operations
	[Fact] public async Task JsonExtract_Literal() => Assert.Equal("world", await Scalar(@"SELECT JSON_EXTRACT_SCALAR('{""hello"":""world""}', '$.hello')"));
	[Fact] public async Task JsonExtract_LiteralNested() => Assert.Equal("42", await Scalar(@"SELECT JSON_EXTRACT_SCALAR('{""a"":{""b"":42}}', '$.a.b')"));
	[Fact] public async Task JsonExtract_LiteralArray() => Assert.Equal("2", await Scalar(@"SELECT JSON_EXTRACT_SCALAR('[1,2,3]', '$[1]')"));
}
