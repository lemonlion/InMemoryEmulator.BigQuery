using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// JSON function tests: JSON_EXTRACT, JSON_EXTRACT_SCALAR, JSON_VALUE, JSON_QUERY, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class JsonFunctionCoverageTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public JsonFunctionCoverageTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_jfc_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.docs` (id INT64, data STRING)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.docs` VALUES
			(1, '{{""name"":""Alice"",""age"":30,""city"":""NYC""}}'),
			(2, '{{""name"":""Bob"",""age"":25,""tags"":[""dev"",""lead""]}}'),
			(3, '{{""name"":""Carol"",""nested"":{{""x"":1,""y"":2}}}}'),
			(4, '{{""name"":""Dave"",""scores"":[90,85,92]}}'),
			(5, '{{""name"":""Eve"",""active"":true,""salary"":75000.50}}')", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- JSON_EXTRACT_SCALAR ----
	[Fact] public async Task JsonExtractScalar_String() => Assert.Equal("Alice", await S("SELECT JSON_EXTRACT_SCALAR(data, '$.name') FROM `{ds}.docs` WHERE id = 1"));
	[Fact] public async Task JsonExtractScalar_Int() => Assert.Equal("30", await S("SELECT JSON_EXTRACT_SCALAR(data, '$.age') FROM `{ds}.docs` WHERE id = 1"));
	[Fact] public async Task JsonExtractScalar_Nested() => Assert.Equal("1", await S("SELECT JSON_EXTRACT_SCALAR(data, '$.nested.x') FROM `{ds}.docs` WHERE id = 3"));
	[Fact] public async Task JsonExtractScalar_Bool() => Assert.Equal("true", await S("SELECT JSON_EXTRACT_SCALAR(data, '$.active') FROM `{ds}.docs` WHERE id = 5"));
	[Fact] public async Task JsonExtractScalar_Missing() => Assert.Null(await S("SELECT JSON_EXTRACT_SCALAR(data, '$.missing') FROM `{ds}.docs` WHERE id = 1"));
	[Fact] public async Task JsonExtractScalar_ArrayElement() => Assert.Equal("90", await S("SELECT JSON_EXTRACT_SCALAR(data, '$.scores[0]') FROM `{ds}.docs` WHERE id = 4"));
	[Fact] public async Task JsonExtractScalar_ArrayElement2() => Assert.Equal("85", await S("SELECT JSON_EXTRACT_SCALAR(data, '$.scores[1]') FROM `{ds}.docs` WHERE id = 4"));
	[Fact] public async Task JsonExtractScalar_NullInput() => Assert.Null(await S("SELECT JSON_EXTRACT_SCALAR(NULL, '$.name')"));
	[Fact] public async Task JsonExtractScalar_Float() => Assert.Equal("75000.50", await S("SELECT JSON_EXTRACT_SCALAR(data, '$.salary') FROM `{ds}.docs` WHERE id = 5"));
	[Fact] public async Task JsonExtractScalar_Literal() => Assert.Equal("hello", await S(@"SELECT JSON_EXTRACT_SCALAR('{""key"":""hello""}', '$.key')"));

	// ---- JSON_EXTRACT ----
	[Fact] public async Task JsonExtract_Object()
	{
		var v = await S("SELECT JSON_EXTRACT(data, '$.nested') FROM `{ds}.docs` WHERE id = 3");
		Assert.NotNull(v);
		Assert.Contains("\"x\"", v);
	}
	[Fact] public async Task JsonExtract_Array()
	{
		var v = await S("SELECT JSON_EXTRACT(data, '$.scores') FROM `{ds}.docs` WHERE id = 4");
		Assert.NotNull(v);
		Assert.Contains("90", v);
	}
	[Fact] public async Task JsonExtract_Scalar()
	{
		var v = await S("SELECT JSON_EXTRACT(data, '$.name') FROM `{ds}.docs` WHERE id = 1");
		Assert.NotNull(v);
		Assert.Contains("Alice", v);
	}
	[Fact] public async Task JsonExtract_Missing() => Assert.Null(await S("SELECT JSON_EXTRACT(data, '$.nope') FROM `{ds}.docs` WHERE id = 1"));
	[Fact] public async Task JsonExtract_NullInput() => Assert.Null(await S("SELECT JSON_EXTRACT(NULL, '$.key')"));

	// ---- JSON_VALUE (synonym for JSON_EXTRACT_SCALAR in standard SQL) ----
	[Fact] public async Task JsonValue_Basic() => Assert.Equal("Alice", await S("SELECT JSON_VALUE(data, '$.name') FROM `{ds}.docs` WHERE id = 1"));
	[Fact] public async Task JsonValue_Nested() => Assert.Equal("2", await S("SELECT JSON_VALUE(data, '$.nested.y') FROM `{ds}.docs` WHERE id = 3"));
	[Fact] public async Task JsonValue_Null() => Assert.Null(await S("SELECT JSON_VALUE(NULL, '$.x')"));
	[Fact] public async Task JsonValue_Missing() => Assert.Null(await S("SELECT JSON_VALUE(data, '$.nosuch') FROM `{ds}.docs` WHERE id = 1"));

	// ---- JSON_QUERY (synonym for JSON_EXTRACT) ----
	[Fact] public async Task JsonQuery_Object()
	{
		var v = await S("SELECT JSON_QUERY(data, '$.nested') FROM `{ds}.docs` WHERE id = 3");
		Assert.NotNull(v);
		Assert.Contains("\"x\"", v);
	}
	[Fact] public async Task JsonQuery_Array()
	{
		var v = await S("SELECT JSON_QUERY(data, '$.tags') FROM `{ds}.docs` WHERE id = 2");
		Assert.NotNull(v);
		Assert.Contains("dev", v);
	}
	[Fact] public async Task JsonQuery_Missing() => Assert.Null(await S("SELECT JSON_QUERY(data, '$.nope') FROM `{ds}.docs` WHERE id = 1"));

	// ---- WHERE with JSON ----
	[Fact] public async Task Where_JsonScalar()
	{
		var rows = await Q("SELECT id FROM `{ds}.docs` WHERE JSON_EXTRACT_SCALAR(data, '$.name') = 'Alice'");
		Assert.Single(rows);
		Assert.Equal("1", rows[0]["id"]?.ToString());
	}
	[Fact] public async Task Where_JsonInt()
	{
		var rows = await Q("SELECT id FROM `{ds}.docs` WHERE CAST(JSON_EXTRACT_SCALAR(data, '$.age') AS INT64) > 26 ORDER BY id");
		Assert.Single(rows); // Alice (30)
		Assert.Equal("1", rows[0]["id"]?.ToString());
	}
	[Fact] public async Task Where_JsonNull()
	{
		var rows = await Q("SELECT id FROM `{ds}.docs` WHERE JSON_EXTRACT_SCALAR(data, '$.city') IS NOT NULL ORDER BY id");
		Assert.Single(rows); // only Alice has city
	}

	// ---- JSON with aggregation ----
	[Fact] public async Task Agg_JsonCount()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.docs` WHERE JSON_EXTRACT_SCALAR(data, '$.age') IS NOT NULL");
		Assert.Equal("2", v); // Alice and Bob have age
	}
	[Fact] public async Task Agg_JsonSum()
	{
		var v = await S("SELECT SUM(CAST(JSON_EXTRACT_SCALAR(data, '$.age') AS INT64)) FROM `{ds}.docs` WHERE JSON_EXTRACT_SCALAR(data, '$.age') IS NOT NULL");
		Assert.Equal("55", v); // 30 + 25
	}

	// ---- Complex ----
	[Fact] public async Task Complex_ConcatJson()
	{
		var v = await S("SELECT CONCAT(JSON_EXTRACT_SCALAR(data, '$.name'), ' from ', COALESCE(JSON_EXTRACT_SCALAR(data, '$.city'), 'Unknown')) FROM `{ds}.docs` WHERE id = 1");
		Assert.Equal("Alice from NYC", v);
	}
	[Fact] public async Task Complex_CaseJson()
	{
		var v = await S("SELECT CASE WHEN JSON_EXTRACT_SCALAR(data, '$.active') = 'true' THEN 'Active' ELSE 'Inactive' END FROM `{ds}.docs` WHERE id = 5");
		Assert.Equal("Active", v);
	}
	[Fact] public async Task Complex_OrderByJson()
	{
		var rows = await Q("SELECT JSON_EXTRACT_SCALAR(data, '$.name') AS n FROM `{ds}.docs` ORDER BY JSON_EXTRACT_SCALAR(data, '$.name') LIMIT 3");
		Assert.Equal("Alice", rows[0]["n"]?.ToString());
		Assert.Equal("Bob", rows[1]["n"]?.ToString());
		Assert.Equal("Carol", rows[2]["n"]?.ToString());
	}
}
