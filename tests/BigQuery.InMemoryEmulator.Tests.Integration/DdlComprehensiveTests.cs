using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Comprehensive DDL tests: CREATE TABLE, ALTER TABLE, DROP TABLE, CREATE/DROP VIEW, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DdlComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public DdlComprehensiveTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_ddl_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task Exec(string sql)
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(sql, parameters: null);
	}

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		return result.ToList();
	}

	private async Task<string?> Scalar(string sql)
	{
		var rows = await Query(sql);
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- CREATE TABLE ----
	[Fact] public async Task CreateTable_BasicColumns()
	{
		await Exec($"CREATE TABLE `{_datasetId}.ct1` (id INT64, name STRING, active BOOL)");
		var rows = await Query($"SELECT column_name FROM `{_datasetId}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'ct1' ORDER BY ordinal_position");
		Assert.Equal(3, rows.Count);
		Assert.Equal("id", rows[0]["column_name"]?.ToString());
	}

	[Fact] public async Task CreateTable_IfNotExists_NoError()
	{
		await Exec($"CREATE TABLE `{_datasetId}.ct2` (id INT64)");
		await Exec($"CREATE TABLE IF NOT EXISTS `{_datasetId}.ct2` (id INT64)"); // should not throw
	}

	[Fact] public async Task CreateTable_OrReplace()
	{
		await Exec($"CREATE TABLE `{_datasetId}.ct3` (id INT64)");
		await Exec($"CREATE OR REPLACE TABLE `{_datasetId}.ct3` (id INT64, name STRING)");
		var rows = await Query($"SELECT column_name FROM `{_datasetId}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'ct3' ORDER BY ordinal_position");
		Assert.Equal(2, rows.Count);
	}

	[Fact(Skip = "DDL CREATE TABLE not routed through procedural executor from ExecuteQuery")] public async Task CreateTable_WithNullableAndRequired()
	{
		await Exec($"CREATE TABLE `{_datasetId}.ct4` (id INT64 NOT NULL, name STRING)");
		var rows = await Query($"SELECT column_name, is_nullable FROM `{_datasetId}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'ct4' ORDER BY ordinal_position");
		Assert.Equal("NO", rows[0]["is_nullable"]?.ToString());
		Assert.Equal("YES", rows[1]["is_nullable"]?.ToString());
	}

	[Fact] public async Task CreateTable_WithAllTypes()
	{
		await Exec($"CREATE TABLE `{_datasetId}.ct5` (a INT64, b FLOAT64, c NUMERIC, d BOOL, e STRING, f BYTES, g DATE, h DATETIME, i TIMESTAMP, j TIME)");
		var rows = await Query($"SELECT COUNT(*) FROM `{_datasetId}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'ct5'");
		Assert.Equal("10", rows[0][0]?.ToString());
	}

	// ---- CREATE TABLE AS SELECT (CTAS) ----
	[Fact(Skip = "DDL: CREATE TABLE AS SELECT not implemented")] public async Task CreateTableAsSelect_Basic()
	{
		await Exec($"CREATE TABLE `{_datasetId}.src1` (id INT64, name STRING)");
		await Exec($"INSERT INTO `{_datasetId}.src1` VALUES (1, 'A'), (2, 'B')");
		await Exec($"CREATE TABLE `{_datasetId}.ctas1` AS SELECT * FROM `{_datasetId}.src1`");
		Assert.Equal("2", await Scalar($"SELECT COUNT(*) FROM `{_datasetId}.ctas1`"));
	}

	[Fact(Skip = "DDL: CREATE TABLE AS SELECT not implemented")] public async Task CreateTableAsSelect_WithTransform()
	{
		await Exec($"CREATE TABLE `{_datasetId}.src2` (id INT64, name STRING)");
		await Exec($"INSERT INTO `{_datasetId}.src2` VALUES (1, 'hello')");
		await Exec($"CREATE TABLE `{_datasetId}.ctas2` AS SELECT id * 2 AS double_id, UPPER(name) AS upper_name FROM `{_datasetId}.src2`");
		var rows = await Query($"SELECT * FROM `{_datasetId}.ctas2`");
		Assert.Equal("2", rows[0]["double_id"]?.ToString());
		Assert.Equal("HELLO", rows[0]["upper_name"]?.ToString());
	}

	// ---- DROP TABLE ----
	[Fact] public async Task DropTable_Exists()
	{
		await Exec($"CREATE TABLE `{_datasetId}.dt1` (id INT64)");
		await Exec($"DROP TABLE `{_datasetId}.dt1`");
		var rows = await Query($"SELECT table_name FROM `{_datasetId}.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'dt1'");
		Assert.Empty(rows);
	}

	[Fact] public async Task DropTable_IfExists_NoError()
	{
		await Exec($"DROP TABLE IF EXISTS `{_datasetId}.nonexistent_table`");
	}

	// ---- ALTER TABLE ----
	[Fact] public async Task AlterTable_AddColumn()
	{
		await Exec($"CREATE TABLE `{_datasetId}.at1` (id INT64)");
		await Exec($"ALTER TABLE `{_datasetId}.at1` ADD COLUMN name STRING");
		var rows = await Query($"SELECT column_name FROM `{_datasetId}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'at1' ORDER BY ordinal_position");
		Assert.Equal(2, rows.Count);
		Assert.Equal("name", rows[1]["column_name"]?.ToString());
	}

	[Fact] public async Task AlterTable_DropColumn()
	{
		await Exec($"CREATE TABLE `{_datasetId}.at2` (id INT64, name STRING, value FLOAT64)");
		await Exec($"ALTER TABLE `{_datasetId}.at2` DROP COLUMN value");
		var rows = await Query($"SELECT column_name FROM `{_datasetId}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'at2' ORDER BY ordinal_position");
		Assert.Equal(2, rows.Count);
	}

	[Fact(Skip = "Not yet supported")] public async Task AlterTable_RenameTable()
	{
		await Exec($"CREATE TABLE `{_datasetId}.at3old` (id INT64)");
		await Exec($"ALTER TABLE `{_datasetId}.at3old` RENAME TO at3new");
		var rows = await Query($"SELECT table_name FROM `{_datasetId}.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'at3new'");
		Assert.Single(rows);
	}

	// ---- TRUNCATE TABLE ----
	[Fact(Skip = "Not yet supported")] public async Task TruncateTable_RemovesAllRows()
	{
		await Exec($"CREATE TABLE `{_datasetId}.tt1` (id INT64)");
		await Exec($"INSERT INTO `{_datasetId}.tt1` VALUES (1), (2), (3)");
		await Exec($"TRUNCATE TABLE `{_datasetId}.tt1`");
		Assert.Equal("0", await Scalar($"SELECT COUNT(*) FROM `{_datasetId}.tt1`"));
	}

	// ---- CREATE VIEW ----
	[Fact(Skip = "Not yet supported")] public async Task CreateView_Basic()
	{
		await Exec($"CREATE TABLE `{_datasetId}.vt1` (id INT64, name STRING)");
		await Exec($"INSERT INTO `{_datasetId}.vt1` VALUES (1, 'A'), (2, 'B')");
		await Exec($"CREATE VIEW `{_datasetId}.v1` AS SELECT * FROM `{_datasetId}.vt1`");
		Assert.Equal("2", await Scalar($"SELECT COUNT(*) FROM `{_datasetId}.v1`"));
	}

	[Fact(Skip = "Not yet supported")] public async Task CreateView_WithFilter()
	{
		await Exec($"CREATE TABLE `{_datasetId}.vt2` (id INT64, active BOOL)");
		await Exec($"INSERT INTO `{_datasetId}.vt2` VALUES (1, TRUE), (2, FALSE), (3, TRUE)");
		await Exec($"CREATE VIEW `{_datasetId}.v2` AS SELECT id FROM `{_datasetId}.vt2` WHERE active = TRUE");
		Assert.Equal("2", await Scalar($"SELECT COUNT(*) FROM `{_datasetId}.v2`"));
	}

	[Fact(Skip = "Not yet supported")] public async Task CreateView_OrReplace()
	{
		await Exec($"CREATE TABLE `{_datasetId}.vt3` (id INT64)");
		await Exec($"CREATE VIEW `{_datasetId}.v3` AS SELECT id FROM `{_datasetId}.vt3`");
		await Exec($"CREATE OR REPLACE VIEW `{_datasetId}.v3` AS SELECT id * 2 AS doubled FROM `{_datasetId}.vt3`");
		var cols = await Query($"SELECT column_name FROM `{_datasetId}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'v3'");
		Assert.Equal("doubled", cols[0]["column_name"]?.ToString());
	}

	[Fact(Skip = "Not yet supported")] public async Task DropView_Basic()
	{
		await Exec($"CREATE TABLE `{_datasetId}.vt4` (id INT64)");
		await Exec($"CREATE VIEW `{_datasetId}.v4` AS SELECT * FROM `{_datasetId}.vt4`");
		await Exec($"DROP VIEW `{_datasetId}.v4`");
		var rows = await Query($"SELECT table_name FROM `{_datasetId}.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'v4'");
		Assert.Empty(rows);
	}

	[Fact(Skip = "Not yet supported")] public async Task DropView_IfExists_NoError()
	{
		await Exec($"DROP VIEW IF EXISTS `{_datasetId}.nonexistent_view`");
	}

	// ---- CREATE TEMP TABLE ----
	[Fact(Skip = "Not yet supported")] public async Task CreateTempTable_UsableInSession()
	{
		await Exec($"CREATE TEMP TABLE tmp1 (id INT64, val STRING); INSERT INTO tmp1 VALUES (1, 'x'); SELECT * FROM tmp1;");
		// temp tables are session-scoped; just verifying no error
	}

	// ---- CREATE SCHEMA ----
	[Fact(Skip = "Not yet supported")] public async Task CreateSchema_IfNotExists()
	{
		var client = await _fixture.GetClientAsync();
		var schemaName = $"test_sch_{Guid.NewGuid():N}"[..30];
		await Exec($"CREATE SCHEMA IF NOT EXISTS `{schemaName}`");
		try { await client.DeleteDatasetAsync(schemaName); } catch { }
	}

	// ---- CREATE TABLE with STRUCT ----
	[Fact(Skip = "DDL: STRUCT column type in CREATE TABLE not supported")] public async Task CreateTable_WithStruct()
	{
		await Exec($"CREATE TABLE `{_datasetId}.st1` (id INT64, info STRUCT<name STRING, age INT64>)");
		var rows = await Query($"SELECT column_name FROM `{_datasetId}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'st1'");
		Assert.True(rows.Count >= 1);
	}

	// ---- CREATE TABLE with ARRAY ----
	[Fact(Skip = "DDL: ARRAY column type in CREATE TABLE not supported")] public async Task CreateTable_WithArray()
	{
		await Exec($"CREATE TABLE `{_datasetId}.arr1` (id INT64, tags ARRAY<STRING>)");
		var rows = await Query($"SELECT column_name FROM `{_datasetId}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'arr1'");
		Assert.True(rows.Count >= 1);
	}

	// ---- Views show in INFORMATION_SCHEMA.VIEWS ----
	[Fact(Skip = "Not yet supported")] public async Task View_AppearsInInformationSchemaViews()
	{
		await Exec($"CREATE TABLE `{_datasetId}.vt5` (id INT64)");
		await Exec($"CREATE VIEW `{_datasetId}.v5` AS SELECT * FROM `{_datasetId}.vt5`");
		var rows = await Query($"SELECT table_name FROM `{_datasetId}.INFORMATION_SCHEMA.VIEWS` WHERE table_name = 'v5'");
		Assert.Single(rows);
	}

	// ---- INFORMATION_SCHEMA.TABLES shows correct table_type ----
	[Fact] public async Task Table_HasCorrectTableTypeInInfoSchema()
	{
		await Exec($"CREATE TABLE `{_datasetId}.bt1` (id INT64)");
		var rows = await Query($"SELECT table_type FROM `{_datasetId}.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'bt1'");
		Assert.Equal("BASE TABLE", rows[0]["table_type"]?.ToString());
	}

	[Fact(Skip = "Not yet supported")] public async Task View_HasCorrectTableTypeInInfoSchema()
	{
		await Exec($"CREATE TABLE `{_datasetId}.bt2` (id INT64)");
		await Exec($"CREATE VIEW `{_datasetId}.v6` AS SELECT * FROM `{_datasetId}.bt2`");
		var rows = await Query($"SELECT table_type FROM `{_datasetId}.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'v6'");
		Assert.Equal("VIEW", rows[0]["table_type"]?.ToString());
	}
}
