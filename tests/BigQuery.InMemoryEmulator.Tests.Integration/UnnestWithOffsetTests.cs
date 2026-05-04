using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for UNNEST WITH OFFSET, ARRAY() subquery constructor, IN UNNEST(), and ARRAY_AGG DISTINCT.
/// These are standard BigQuery features that must be properly supported.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unnest_operator
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class UnnestWithOffsetTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public UnnestWithOffsetTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_uwo_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }
	private async Task Exec(string sql) { var c = await _fixture.GetClientAsync(); await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); }

	// ===== UNNEST WITH OFFSET =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unnest_operator
	// "You can also use WITH OFFSET to get the array element index"

	[Fact]
	public async Task Unnest_WithOffset_Basic()
	{
		var rows = await Q("SELECT x, off FROM UNNEST(['a', 'b', 'c']) AS x WITH OFFSET AS off ORDER BY off");
		Assert.Equal(3, rows.Count);
		Assert.Equal("a", rows[0]["x"]?.ToString());
		Assert.Equal("0", rows[0]["off"]?.ToString());
		Assert.Equal("b", rows[1]["x"]?.ToString());
		Assert.Equal("1", rows[1]["off"]?.ToString());
		Assert.Equal("c", rows[2]["x"]?.ToString());
		Assert.Equal("2", rows[2]["off"]?.ToString());
	}

	[Fact]
	public async Task Unnest_WithOffset_DefaultAlias()
	{
		// When no AS alias given, the offset column defaults to "offset"
		var rows = await Q("SELECT x, offset FROM UNNEST([10, 20, 30]) AS x WITH OFFSET ORDER BY offset");
		Assert.Equal(3, rows.Count);
		Assert.Equal("10", rows[0]["x"]?.ToString());
		Assert.Equal("0", rows[0]["offset"]?.ToString());
		Assert.Equal("30", rows[2]["x"]?.ToString());
		Assert.Equal("2", rows[2]["offset"]?.ToString());
	}

	[Fact]
	public async Task Unnest_WithOffset_OrderByOffset()
	{
		var rows = await Q("SELECT x, pos FROM UNNEST([100, 200, 300]) AS x WITH OFFSET AS pos ORDER BY pos DESC");
		Assert.Equal(3, rows.Count);
		Assert.Equal("300", rows[0]["x"]?.ToString());
		Assert.Equal("2", rows[0]["pos"]?.ToString());
		Assert.Equal("100", rows[2]["x"]?.ToString());
		Assert.Equal("0", rows[2]["pos"]?.ToString());
	}

	[Fact]
	public async Task Unnest_WithOffset_FilterByOffset()
	{
		var rows = await Q("SELECT x FROM UNNEST(['a', 'b', 'c', 'd', 'e']) AS x WITH OFFSET AS pos WHERE pos < 3 ORDER BY pos");
		Assert.Equal(3, rows.Count);
		Assert.Equal("a", rows[0]["x"]?.ToString());
		Assert.Equal("c", rows[2]["x"]?.ToString());
	}

	[Fact]
	public async Task Unnest_WithOffset_FromTable()
	{
		await Exec("CREATE TABLE `{ds}.arr_t` (id INT64, items ARRAY<STRING>)");
		await Exec("INSERT INTO `{ds}.arr_t` VALUES (1, ['x','y','z'])");
		var rows = await Q("SELECT id, item, pos FROM `{ds}.arr_t`, UNNEST(items) AS item WITH OFFSET AS pos ORDER BY pos");
		Assert.Equal(3, rows.Count);
		Assert.Equal("x", rows[0]["item"]?.ToString());
		Assert.Equal("0", rows[0]["pos"]?.ToString());
		Assert.Equal("z", rows[2]["item"]?.ToString());
		Assert.Equal("2", rows[2]["pos"]?.ToString());
	}

	// ===== IN UNNEST() =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#in_operators
	// "value IN UNNEST(array_expression)"

	[Fact]
	public async Task InUnnest_LiteralArray()
	{
		await Exec("CREATE TABLE `{ds}.inu1` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.inu1` VALUES (1,'Alice'),(2,'Bob'),(3,'Charlie'),(4,'Dave')");
		var rows = await Q("SELECT name FROM `{ds}.inu1` WHERE name IN UNNEST(['Alice', 'Charlie']) ORDER BY name");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("Charlie", rows[1]["name"]?.ToString());
	}

	[Fact]
	public async Task InUnnest_IntArray()
	{
		await Exec("CREATE TABLE `{ds}.inu2` (val INT64)");
		await Exec("INSERT INTO `{ds}.inu2` VALUES (1),(2),(3),(4),(5)");
		var rows = await Q("SELECT val FROM `{ds}.inu2` WHERE val IN UNNEST([2, 4]) ORDER BY val");
		Assert.Equal(2, rows.Count);
		Assert.Equal("2", rows[0]["val"]?.ToString());
		Assert.Equal("4", rows[1]["val"]?.ToString());
	}

	[Fact]
	public async Task NotInUnnest_LiteralArray()
	{
		await Exec("CREATE TABLE `{ds}.inu3` (val INT64)");
		await Exec("INSERT INTO `{ds}.inu3` VALUES (1),(2),(3),(4),(5)");
		var rows = await Q("SELECT val FROM `{ds}.inu3` WHERE val NOT IN UNNEST([2, 4]) ORDER BY val");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]["val"]?.ToString());
		Assert.Equal("3", rows[1]["val"]?.ToString());
		Assert.Equal("5", rows[2]["val"]?.ToString());
	}

	[Fact]
	public async Task InUnnest_WithColumnArray()
	{
		await Exec("CREATE TABLE `{ds}.inu4` (id INT64, allowed ARRAY<STRING>)");
		await Exec("INSERT INTO `{ds}.inu4` VALUES (1, ['read','write']),(2, ['read'])");
		var rows = await Q("SELECT id FROM `{ds}.inu4` WHERE 'write' IN UNNEST(allowed)");
		Assert.Single(rows);
		Assert.Equal("1", rows[0]["id"]?.ToString());
	}

	// ===== ARRAY() subquery constructor =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array
	// "ARRAY(subquery) - Returns an ARRAY with one element for each row in a subquery"

	[Fact]
	public async Task ArraySubquery_Basic()
	{
		var v = await S("SELECT ARRAY_TO_STRING(ARRAY(SELECT x FROM UNNEST([3,1,2]) AS x ORDER BY x), ',')");
		Assert.Equal("1,2,3", v);
	}

	[Fact]
	public async Task ArraySubquery_FromTable()
	{
		await Exec("CREATE TABLE `{ds}.asq1` (val INT64)");
		await Exec("INSERT INTO `{ds}.asq1` VALUES (5),(3),(1),(4),(2)");
		var v = await S("SELECT ARRAY_TO_STRING(ARRAY(SELECT val FROM `{ds}.asq1` WHERE val > 2 ORDER BY val), ',')");
		Assert.Equal("3,4,5", v);
	}

	[Fact]
	public async Task ArraySubquery_Length()
	{
		await Exec("CREATE TABLE `{ds}.asq2` (val INT64)");
		await Exec("INSERT INTO `{ds}.asq2` VALUES (1),(2),(3),(4),(5)");
		var v = await S("SELECT ARRAY_LENGTH(ARRAY(SELECT val FROM `{ds}.asq2` WHERE val <= 3))");
		Assert.Equal("3", v);
	}

	[Fact]
	public async Task ArraySubquery_Distinct()
	{
		await Exec("CREATE TABLE `{ds}.asq3` (val INT64)");
		await Exec("INSERT INTO `{ds}.asq3` VALUES (1),(2),(2),(3),(3),(3)");
		var v = await S("SELECT ARRAY_LENGTH(ARRAY(SELECT DISTINCT val FROM `{ds}.asq3`))");
		Assert.Equal("3", v);
	}

	// ===== ARRAY_AGG with DISTINCT =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#array_agg
	// "ARRAY_AGG([DISTINCT] expression)"

	[Fact]
	public async Task ArrayAgg_Distinct_Basic()
	{
		await Exec("CREATE TABLE `{ds}.aad1` (grp STRING, val INT64)");
		await Exec("INSERT INTO `{ds}.aad1` VALUES ('A',1),('A',1),('A',2),('B',3),('B',3)");
		var rows = await Q("SELECT grp, ARRAY_LENGTH(ARRAY_AGG(DISTINCT val)) AS cnt FROM `{ds}.aad1` GROUP BY grp ORDER BY grp");
		Assert.Equal("2", rows[0]["cnt"]?.ToString()); // A has distinct values 1,2
		Assert.Equal("1", rows[1]["cnt"]?.ToString()); // B has distinct value 3
	}

	[Fact]
	public async Task ArrayAgg_Distinct_Strings()
	{
		await Exec("CREATE TABLE `{ds}.aad2` (tag STRING)");
		await Exec("INSERT INTO `{ds}.aad2` VALUES ('a'),('b'),('a'),('c'),('b')");
		var v = await S("SELECT ARRAY_LENGTH(ARRAY_AGG(DISTINCT tag)) FROM `{ds}.aad2`");
		Assert.Equal("3", v);
	}
}
