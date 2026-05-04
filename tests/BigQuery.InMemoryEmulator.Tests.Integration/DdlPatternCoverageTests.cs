using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// DDL patterns: CREATE TABLE, CREATE OR REPLACE, CREATE TABLE AS SELECT, DROP TABLE, ALTER TABLE.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DdlPatternCoverageTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public DdlPatternCoverageTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_ddlp_{Guid.NewGuid():N}"[..30];
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
	[Fact] public async Task Create_Basic()
	{
		await Exec("CREATE TABLE `{ds}.ct1` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.ct1` VALUES (1, 'test')");
		Assert.Equal("1", await S("SELECT COUNT(*) FROM `{ds}.ct1`"));
	}
	[Fact] public async Task Create_WithTypes()
	{
		await Exec("CREATE TABLE `{ds}.ct2` (a INT64, b FLOAT64, c STRING, d BOOL, e DATE, f TIMESTAMP)");
		await Exec("INSERT INTO `{ds}.ct2` VALUES (1, 3.14, 'hello', true, DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00')");
		Assert.Equal("1", await S("SELECT COUNT(*) FROM `{ds}.ct2`"));
	}
	[Fact] public async Task Create_IfNotExists()
	{
		await Exec("CREATE TABLE `{ds}.ct3` (id INT64)");
		await Exec("CREATE TABLE IF NOT EXISTS `{ds}.ct3` (id INT64)"); // Should not error
		await Exec("INSERT INTO `{ds}.ct3` VALUES (1)");
		Assert.Equal("1", await S("SELECT COUNT(*) FROM `{ds}.ct3`"));
	}

	// ---- CREATE OR REPLACE TABLE ----
	[Fact] public async Task CreateOrReplace()
	{
		await Exec("CREATE TABLE `{ds}.ct4` (id INT64)");
		await Exec("INSERT INTO `{ds}.ct4` VALUES (1)");
		await Exec("CREATE OR REPLACE TABLE `{ds}.ct4` (id INT64, name STRING)");
		Assert.Equal("0", await S("SELECT COUNT(*) FROM `{ds}.ct4`")); // Data cleared
	}

	// ---- CREATE TABLE AS SELECT ----
	[Fact] public async Task Create_AsSelect()
	{
		await Exec("CREATE TABLE `{ds}.src5` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.src5` VALUES (1,'a'),(2,'b'),(3,'c')");
		await Exec("CREATE TABLE `{ds}.ct5` AS SELECT * FROM `{ds}.src5` WHERE id > 1");
		Assert.Equal("2", await S("SELECT COUNT(*) FROM `{ds}.ct5`"));
	}
	[Fact] public async Task Create_AsSelectWithExpr()
	{
		await Exec("CREATE TABLE `{ds}.src6` (id INT64, val INT64)");
		await Exec("INSERT INTO `{ds}.src6` VALUES (1,10),(2,20),(3,30)");
		await Exec("CREATE TABLE `{ds}.ct6` AS SELECT id, val * 2 AS doubled FROM `{ds}.src6`");
		Assert.Equal("120", await S("SELECT SUM(doubled) FROM `{ds}.ct6`")); // 20+40+60
	}

	// ---- DROP TABLE ----
	[Fact] public async Task Drop_Table()
	{
		await Exec("CREATE TABLE `{ds}.dt1` (id INT64)");
		await Exec("DROP TABLE `{ds}.dt1`");
		await Assert.ThrowsAnyAsync<Exception>(async () => await Exec("SELECT * FROM `{ds}.dt1`"));
	}
	[Fact] public async Task Drop_IfExists()
	{
		await Exec("DROP TABLE IF EXISTS `{ds}.nonexistent`"); // Should not error
	}

	// ---- ALTER TABLE ----
	[Fact] public async Task Alter_AddColumn()
	{
		await Exec("CREATE TABLE `{ds}.at1` (id INT64)");
		await Exec("ALTER TABLE `{ds}.at1` ADD COLUMN name STRING");
		await Exec("INSERT INTO `{ds}.at1` VALUES (1, 'test')");
		Assert.Equal("test", await S("SELECT name FROM `{ds}.at1` WHERE id = 1"));
	}
	[Fact] public async Task Alter_DropColumn()
	{
		await Exec("CREATE TABLE `{ds}.at2` (id INT64, name STRING, extra STRING)");
		await Exec("INSERT INTO `{ds}.at2` VALUES (1, 'test', 'drop_me')");
		await Exec("ALTER TABLE `{ds}.at2` DROP COLUMN extra");
		var rows = await Q("SELECT * FROM `{ds}.at2`");
		Assert.Single(rows);
	}

	// ---- Multiple DDL operations ----
	[Fact] public async Task Ddl_Sequence()
	{
		await Exec("CREATE TABLE `{ds}.seq1` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.seq1` VALUES (1,'a'),(2,'b')");
		await Exec("ALTER TABLE `{ds}.seq1` ADD COLUMN val INT64");
		await Exec("INSERT INTO `{ds}.seq1` (id, name, val) VALUES (3, 'c', 100)");
		Assert.Equal("3", await S("SELECT COUNT(*) FROM `{ds}.seq1`"));
	}

	// ---- CREATE TABLE with ARRAY type ----
	[Fact] public async Task Create_WithArray()
	{
		await Exec("CREATE TABLE `{ds}.arr1` (id INT64, tags ARRAY<STRING>)");
		await Exec("INSERT INTO `{ds}.arr1` VALUES (1, ['a','b','c'])");
		Assert.Equal("3", await S("SELECT ARRAY_LENGTH(tags) FROM `{ds}.arr1`"));
	}

	// ---- CREATE TABLE with STRUCT type ----
	[Fact] public async Task Create_WithStruct()
	{
		await Exec("CREATE TABLE `{ds}.st1` (id INT64, info STRUCT<name STRING, age INT64>)");
		await Exec("INSERT INTO `{ds}.st1` VALUES (1, STRUCT('Alice', 30))");
		Assert.Equal("1", await S("SELECT COUNT(*) FROM `{ds}.st1`"));
	}

	// ---- TRUNCATE TABLE ----
	[Fact] public async Task Truncate_Table()
	{
		await Exec("CREATE TABLE `{ds}.tr1` (id INT64)");
		await Exec("INSERT INTO `{ds}.tr1` VALUES (1),(2),(3)");
		await Exec("TRUNCATE TABLE `{ds}.tr1`");
		Assert.Equal("0", await S("SELECT COUNT(*) FROM `{ds}.tr1`"));
	}
}
