using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for DDL operations: CREATE TABLE, DROP TABLE, ALTER TABLE, CREATE VIEW.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DdlComprehensiveExtTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public DdlComprehensiveExtTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_ddl_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task Exec(string sql) { var c = await _fixture.GetClientAsync(); await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); }
	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- CREATE TABLE ----
	[Fact] public async Task Create_BasicTable()
	{
		await Exec("CREATE TABLE `{ds}.t1` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.t1` VALUES (1, 'test')");
		Assert.Equal("1", await S("SELECT COUNT(*) FROM `{ds}.t1`"));
	}
	[Fact] public async Task Create_AllTypes()
	{
		await Exec("CREATE TABLE `{ds}.t2` (a INT64, b FLOAT64, c STRING, d BOOL, e BYTES, f DATE, g TIMESTAMP, h NUMERIC)");
		await Exec("INSERT INTO `{ds}.t2` (a, b, c, d) VALUES (1, 1.5, 'hello', true)");
		Assert.Equal("1", await S("SELECT COUNT(*) FROM `{ds}.t2`"));
	}
	[Fact] public async Task Create_IfNotExists()
	{
		await Exec("CREATE TABLE `{ds}.t3` (id INT64)");
		await Exec("CREATE TABLE IF NOT EXISTS `{ds}.t3` (id INT64)"); // no error
		await Exec("INSERT INTO `{ds}.t3` VALUES (1)");
		Assert.Equal("1", await S("SELECT COUNT(*) FROM `{ds}.t3`"));
	}
	[Fact] public async Task Create_OrReplace()
	{
		await Exec("CREATE TABLE `{ds}.t4` (id INT64, old_col STRING)");
		await Exec("INSERT INTO `{ds}.t4` VALUES (1, 'test')");
		await Exec("CREATE OR REPLACE TABLE `{ds}.t4` (id INT64, new_col STRING)");
		await Exec("INSERT INTO `{ds}.t4` VALUES (1, 'replaced')");
		Assert.Equal("replaced", await S("SELECT new_col FROM `{ds}.t4`"));
	}
	[Fact] public async Task Create_WithMultipleColumns()
	{
		await Exec("CREATE TABLE `{ds}.t5` (a INT64, b INT64, c INT64, d INT64, e INT64)");
		await Exec("INSERT INTO `{ds}.t5` VALUES (1,2,3,4,5)");
		Assert.Equal("15", await S("SELECT a+b+c+d+e FROM `{ds}.t5`"));
	}

	// ---- CREATE TABLE AS SELECT ----
	[Fact] public async Task Create_AsSelect()
	{
		await Exec("CREATE TABLE `{ds}.src` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.src` VALUES (1,'A'),(2,'B'),(3,'C')");
		await Exec("CREATE TABLE `{ds}.dst` AS SELECT * FROM `{ds}.src` WHERE id <= 2");
		Assert.Equal("2", await S("SELECT COUNT(*) FROM `{ds}.dst`"));
	}
	[Fact] public async Task Create_AsSelectWithExpr()
	{
		await Exec("CREATE TABLE `{ds}.src2` (id INT64, val INT64)");
		await Exec("INSERT INTO `{ds}.src2` VALUES (1,10),(2,20)");
		await Exec("CREATE TABLE `{ds}.dst2` AS SELECT id, val * 2 AS doubled FROM `{ds}.src2`");
		Assert.Equal("60", await S("SELECT SUM(doubled) FROM `{ds}.dst2`")); // 10*2 + 20*2 = 60: id=1 doubled=20, id=2 doubled=40 => SUM=60
	}

	// ---- DROP TABLE ----
	[Fact] public async Task Drop_Basic()
	{
		await Exec("CREATE TABLE `{ds}.drp1` (id INT64)");
		await Exec("INSERT INTO `{ds}.drp1` VALUES (1)");
		await Exec("DROP TABLE `{ds}.drp1`");
		await Assert.ThrowsAnyAsync<Exception>(async () => await S("SELECT COUNT(*) FROM `{ds}.drp1`"));
	}
	[Fact] public async Task Drop_IfExists()
	{
		await Exec("DROP TABLE IF EXISTS `{ds}.nonexistent`"); // no error
	}
	[Fact] public async Task Drop_AndRecreate()
	{
		await Exec("CREATE TABLE `{ds}.drp2` (id INT64)");
		await Exec("INSERT INTO `{ds}.drp2` VALUES (1)");
		await Exec("DROP TABLE `{ds}.drp2`");
		await Exec("CREATE TABLE `{ds}.drp2` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.drp2` VALUES (1, 'new')");
		Assert.Equal("new", await S("SELECT name FROM `{ds}.drp2`"));
	}

	// ---- ALTER TABLE ----
	[Fact] public async Task Alter_AddColumn()
	{
		await Exec("CREATE TABLE `{ds}.alt1` (id INT64)");
		await Exec("ALTER TABLE `{ds}.alt1` ADD COLUMN name STRING");
		await Exec("INSERT INTO `{ds}.alt1` VALUES (1, 'test')");
		Assert.Equal("test", await S("SELECT name FROM `{ds}.alt1`"));
	}
	[Fact] public async Task Alter_DropColumn()
	{
		await Exec("CREATE TABLE `{ds}.alt2` (id INT64, name STRING, val INT64)");
		await Exec("INSERT INTO `{ds}.alt2` VALUES (1, 'test', 42)");
		await Exec("ALTER TABLE `{ds}.alt2` DROP COLUMN name");
		Assert.Equal("42", await S("SELECT val FROM `{ds}.alt2`"));
	}

	// ---- CREATE VIEW ----
	[Fact] public async Task Create_View()
	{
		await Exec("CREATE TABLE `{ds}.vt1` (id INT64, name STRING, val INT64)");
		await Exec("INSERT INTO `{ds}.vt1` VALUES (1,'A',10),(2,'B',20),(3,'C',30)");
		await Exec("CREATE VIEW `{ds}.v1` AS SELECT name, val FROM `{ds}.vt1` WHERE val > 15");
		var rows = await Q("SELECT * FROM `{ds}.v1` ORDER BY name");
		Assert.Equal(2, rows.Count);
	}
	[Fact] public async Task Create_ViewWithAgg()
	{
		await Exec("CREATE TABLE `{ds}.vt2` (id INT64, grp STRING, val INT64)");
		await Exec("INSERT INTO `{ds}.vt2` VALUES (1,'A',10),(2,'A',20),(3,'B',30)");
		await Exec("CREATE VIEW `{ds}.v2` AS SELECT grp, SUM(val) AS total FROM `{ds}.vt2` GROUP BY grp");
		var rows = await Q("SELECT * FROM `{ds}.v2` ORDER BY grp");
		Assert.Equal(2, rows.Count);
		Assert.Equal("30", rows[0]["total"]?.ToString()); // A: 10+20
	}

	// ---- Table operations sequence ----
	[Fact] public async Task Sequence_CreateInsertQuery()
	{
		await Exec("CREATE TABLE `{ds}.seq1` (id INT64, val STRING)");
		for (int i = 0; i < 5; i++) await Exec($"INSERT INTO `{{ds}}.seq1` VALUES ({i}, 'item_{i}')");
		Assert.Equal("5", await S("SELECT COUNT(*) FROM `{ds}.seq1`"));
	}
	[Fact] public async Task Sequence_CreateDropCreate()
	{
		await Exec("CREATE TABLE `{ds}.seq2` (id INT64)");
		await Exec("DROP TABLE `{ds}.seq2`");
		await Exec("CREATE TABLE `{ds}.seq2` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.seq2` VALUES (1, 'test')");
		Assert.Equal("test", await S("SELECT name FROM `{ds}.seq2`"));
	}
}
