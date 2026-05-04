using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Array function patterns: ARRAY(), ARRAY_LENGTH, ARRAY_TO_STRING, ARRAY_REVERSE, ARRAY_CONCAT, GENERATE_ARRAY.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ArrayFunctionPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ArrayFunctionPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_afp_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- ARRAY_LENGTH ----
	[Fact] public async Task ArrayLength_Basic() => Assert.Equal("3", await S("SELECT ARRAY_LENGTH([1, 2, 3])"));
	[Fact] public async Task ArrayLength_Empty() => Assert.Equal("0", await S("SELECT ARRAY_LENGTH([])"));
	[Fact] public async Task ArrayLength_Strings() => Assert.Equal("2", await S("SELECT ARRAY_LENGTH(['a', 'b'])"));

	// ---- ARRAY_TO_STRING ----
	[Fact] public async Task ArrayToString_Comma() => Assert.Equal("1,2,3", await S("SELECT ARRAY_TO_STRING([1, 2, 3], ',')"));
	[Fact] public async Task ArrayToString_Dash() => Assert.Equal("a-b-c", await S("SELECT ARRAY_TO_STRING(['a', 'b', 'c'], '-')"));
	[Fact] public async Task ArrayToString_Empty() => Assert.Equal("", await S("SELECT ARRAY_TO_STRING(CAST([] AS ARRAY<STRING>), ',')"));

	// ---- ARRAY_REVERSE ----
	[Fact] public async Task ArrayReverse_Basic()
	{
		var v = await S("SELECT ARRAY_TO_STRING(ARRAY_REVERSE([1, 2, 3]), ',')");
		Assert.Equal("3,2,1", v);
	}
	[Fact] public async Task ArrayReverse_Strings()
	{
		var v = await S("SELECT ARRAY_TO_STRING(ARRAY_REVERSE(['a', 'b', 'c']), ',')");
		Assert.Equal("c,b,a", v);
	}

	// ---- ARRAY_CONCAT ----
	[Fact] public async Task ArrayConcat_Basic()
	{
		var v = await S("SELECT ARRAY_TO_STRING(ARRAY_CONCAT([1, 2], [3, 4]), ',')");
		Assert.Equal("1,2,3,4", v);
	}
	[Fact] public async Task ArrayConcat_Strings()
	{
		var v = await S("SELECT ARRAY_TO_STRING(ARRAY_CONCAT(['a'], ['b', 'c']), ',')");
		Assert.Equal("a,b,c", v);
	}

	// ---- GENERATE_ARRAY ----
	[Fact] public async Task GenerateArray_Basic() => Assert.Equal("5", await S("SELECT ARRAY_LENGTH(GENERATE_ARRAY(1, 5))"));
	[Fact] public async Task GenerateArray_WithStep() => Assert.Equal("3", await S("SELECT ARRAY_LENGTH(GENERATE_ARRAY(0, 10, 5))")); // 0,5,10
	[Fact] public async Task GenerateArray_AsString()
	{
		var v = await S("SELECT ARRAY_TO_STRING(GENERATE_ARRAY(1, 3), ',')");
		Assert.Equal("1,2,3", v);
	}

	// ---- UNNEST ----
	[Fact] public async Task Unnest_Basic()
	{
		var rows = await Q("SELECT x FROM UNNEST([10, 20, 30]) AS x ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("10", rows[0]["x"]?.ToString());
	}
	[Fact] public async Task Unnest_WithOffset()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unnest_operator
		var rows = await Q("SELECT x, pos FROM UNNEST(['a', 'b', 'c']) AS x WITH OFFSET AS pos ORDER BY pos");
		Assert.Equal(3, rows.Count);
		Assert.Equal("a", rows[0]["x"]?.ToString());
		Assert.Equal("0", rows[0]["pos"]?.ToString());
		Assert.Equal("c", rows[2]["x"]?.ToString());
		Assert.Equal("2", rows[2]["pos"]?.ToString());
	}
	[Fact] public async Task Unnest_InWhere()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#in_operators
		await Exec("CREATE TABLE `{ds}.ut` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.ut` VALUES (1,'a'),(2,'b'),(3,'c')");
		Assert.Equal("2", await S("SELECT COUNT(*) FROM `{ds}.ut` WHERE name IN UNNEST(['a','c'])"));
	}

	[Fact] public async Task Array_Subquery()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array
		await Exec("CREATE TABLE `{ds}.as1` (val INT64)");
		await Exec("INSERT INTO `{ds}.as1` VALUES (1),(2),(3),(4),(5)");
		var v = await S("SELECT ARRAY_LENGTH(ARRAY(SELECT val FROM `{ds}.as1` WHERE val > 2))");
		Assert.Equal("3", v);
	}

	// ---- ARRAY in table ----
	[Fact] public async Task Array_InTable()
	{
		await Exec("CREATE TABLE `{ds}.at` (id INT64, tags ARRAY<STRING>)");
		await Exec("INSERT INTO `{ds}.at` VALUES (1, ['a','b','c']),(2, ['d','e'])");
		var rows = await Q("SELECT id, ARRAY_LENGTH(tags) AS cnt FROM `{ds}.at` ORDER BY id");
		Assert.Equal("3", rows[0]["cnt"]?.ToString());
		Assert.Equal("2", rows[1]["cnt"]?.ToString());
	}
	[Fact] public async Task Array_Unnest_Join()
	{
		await Exec("CREATE TABLE `{ds}.aj` (id INT64, items ARRAY<STRING>)");
		await Exec("INSERT INTO `{ds}.aj` VALUES (1, ['x','y']),(2, ['z'])");
		var rows = await Q("SELECT id, item FROM `{ds}.aj`, UNNEST(items) item ORDER BY id, item");
		Assert.Equal(3, rows.Count);
	}

	// ---- ARRAY_AGG ----
	[Fact] public async Task ArrayAgg_Basic()
	{
		await Exec("CREATE TABLE `{ds}.aa` (grp STRING, val INT64)");
		await Exec("INSERT INTO `{ds}.aa` VALUES ('A',1),('A',2),('B',3),('B',4),('B',5)");
		var rows = await Q("SELECT grp, ARRAY_LENGTH(ARRAY_AGG(val)) AS cnt FROM `{ds}.aa` GROUP BY grp ORDER BY grp");
		Assert.Equal("2", rows[0]["cnt"]?.ToString());
		Assert.Equal("3", rows[1]["cnt"]?.ToString());
	}

	// ---- ARRAY with DISTINCT via subquery ----
	[Fact] public async Task Array_Distinct()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array
		await Exec("CREATE TABLE `{ds}.ad` (val INT64)");
		await Exec("INSERT INTO `{ds}.ad` VALUES (1),(2),(2),(3),(3),(3)");
		var v = await S("SELECT ARRAY_LENGTH(ARRAY(SELECT DISTINCT val FROM `{ds}.ad`))");
		Assert.Equal("3", v);
	}

	// ---- GENERATE_DATE_ARRAY ----
	[Fact] public async Task GenerateDateArray_Basic()
	{
		var v = await S("SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-05'))");
		Assert.Equal("5", v);
	}
	[Fact] public async Task GenerateDateArray_Monthly()
	{
		var v = await S("SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-06-01', INTERVAL 1 MONTH))");
		Assert.Equal("6", v);
	}

	private async Task Exec(string sql) { var c = await _fixture.GetClientAsync(); await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); }
}
