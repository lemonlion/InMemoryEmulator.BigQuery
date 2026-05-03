using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for advanced DDL operations: CREATE TABLE, CREATE OR REPLACE, CREATE IF NOT EXISTS, ALTER TABLE.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DdlAdvancedOperationTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public DdlAdvancedOperationTests(BigQuerySession session) => _session = session;

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

	private async Task Execute(string sql)
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
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

	// CREATE TABLE
	[Fact] public async Task CreateTable_Basic()
	{
		await Execute("CREATE TABLE `{ds}.basic_table` (id INT64, name STRING)");
		await Execute("INSERT INTO `{ds}.basic_table` (id, name) VALUES (1, 'test')");
		var result = await Scalar("SELECT name FROM `{ds}.basic_table` WHERE id = 1");
		Assert.Equal("test", result);
	}

	[Fact] public async Task CreateTable_AllTypes()
	{
		await Execute(@"CREATE TABLE `{ds}.all_types` (
			col_int INT64, col_float FLOAT64, col_string STRING, col_bool BOOL,
			col_date DATE, col_timestamp TIMESTAMP, col_bytes BYTES)");
		await Execute("INSERT INTO `{ds}.all_types` (col_int, col_float, col_string, col_bool, col_date) VALUES (1, 3.14, 'hello', TRUE, DATE '2024-01-15')");
		var result = await Scalar("SELECT col_string FROM `{ds}.all_types`");
		Assert.Equal("hello", result);
	}

	[Fact] public async Task CreateTable_WithArray()
	{
		await Execute("CREATE TABLE `{ds}.array_table` (id INT64, tags ARRAY<STRING>)");
		await Execute("INSERT INTO `{ds}.array_table` (id, tags) VALUES (1, ['a', 'b', 'c'])");
		var result = await Scalar("SELECT ARRAY_LENGTH(tags) FROM `{ds}.array_table`");
		Assert.Equal("3", result);
	}

	// CREATE TABLE IF NOT EXISTS
	[Fact] public async Task CreateTableIfNotExists_NewTable()
	{
		await Execute("CREATE TABLE IF NOT EXISTS `{ds}.maybe_table` (id INT64)");
		await Execute("INSERT INTO `{ds}.maybe_table` (id) VALUES (1)");
		Assert.Equal("1", await Scalar("SELECT COUNT(*) FROM `{ds}.maybe_table`"));
	}

	[Fact] public async Task CreateTableIfNotExists_AlreadyExists()
	{
		await Execute("CREATE TABLE `{ds}.existing_table` (id INT64)");
		await Execute("INSERT INTO `{ds}.existing_table` (id) VALUES (1)");
		// Should not error
		await Execute("CREATE TABLE IF NOT EXISTS `{ds}.existing_table` (id INT64, name STRING)");
		// Original structure preserved
		Assert.Equal("1", await Scalar("SELECT COUNT(*) FROM `{ds}.existing_table`"));
	}

	// CREATE OR REPLACE TABLE
	[Fact] public async Task CreateOrReplace_NewTable()
	{
		await Execute("CREATE OR REPLACE TABLE `{ds}.replace_table` (id INT64, name STRING)");
		await Execute("INSERT INTO `{ds}.replace_table` (id, name) VALUES (1, 'first')");
		Assert.Equal("first", await Scalar("SELECT name FROM `{ds}.replace_table`"));
	}

	[Fact] public async Task CreateOrReplace_ExistingTable()
	{
		await Execute("CREATE TABLE `{ds}.replace_me` (id INT64, old_col STRING)");
		await Execute("INSERT INTO `{ds}.replace_me` (id, old_col) VALUES (1, 'old')");
		await Execute("CREATE OR REPLACE TABLE `{ds}.replace_me` (id INT64, new_col STRING)");
		await Execute("INSERT INTO `{ds}.replace_me` (id, new_col) VALUES (2, 'new')");
		Assert.Equal("1", await Scalar("SELECT COUNT(*) FROM `{ds}.replace_me`"));
		Assert.Equal("new", await Scalar("SELECT new_col FROM `{ds}.replace_me`"));
	}

	// CREATE TABLE AS SELECT
	[Fact] public async Task CreateTableAsSelect_Basic()
	{
		await Execute("CREATE TABLE `{ds}.source_ctas` (id INT64, val STRING)");
		await Execute("INSERT INTO `{ds}.source_ctas` (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c')");
		await Execute("CREATE TABLE `{ds}.dest_ctas` AS SELECT * FROM `{ds}.source_ctas` WHERE id > 1");
		Assert.Equal("2", await Scalar("SELECT COUNT(*) FROM `{ds}.dest_ctas`"));
	}

	[Fact] public async Task CreateTableAsSelect_WithTransform()
	{
		await Execute("CREATE TABLE `{ds}.src_transform` (id INT64, amount FLOAT64)");
		await Execute("INSERT INTO `{ds}.src_transform` (id, amount) VALUES (1, 100), (2, 200), (3, 300)");
		await Execute("CREATE TABLE `{ds}.dst_transform` AS SELECT id, amount * 2 AS doubled FROM `{ds}.src_transform`");
		Assert.Equal("400", await Scalar("SELECT doubled FROM `{ds}.dst_transform` WHERE id = 2"));
	}

	// DROP TABLE
	[Fact] public async Task DropTable_Basic()
	{
		await Execute("CREATE TABLE `{ds}.to_drop` (id INT64)");
		await Execute("INSERT INTO `{ds}.to_drop` (id) VALUES (1)");
		await Execute("DROP TABLE `{ds}.to_drop`");
		// Should not be able to query
		await Assert.ThrowsAnyAsync<Exception>(async () => await Scalar("SELECT * FROM `{ds}.to_drop`"));
	}

	[Fact] public async Task DropTableIfExists_Exists()
	{
		await Execute("CREATE TABLE `{ds}.maybe_drop` (id INT64)");
		await Execute("DROP TABLE IF EXISTS `{ds}.maybe_drop`");
		// Should not error even though table is gone
	}

	[Fact] public async Task DropTableIfExists_NotExists()
	{
		// Should not error for non-existent table
		await Execute("DROP TABLE IF EXISTS `{ds}.nonexistent_table`");
	}

	// TRUNCATE TABLE
	[Fact] public async Task TruncateTable()
	{
		await Execute("CREATE TABLE `{ds}.to_truncate` (id INT64, name STRING)");
		await Execute("INSERT INTO `{ds}.to_truncate` (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')");
		await Execute("TRUNCATE TABLE `{ds}.to_truncate`");
		Assert.Equal("0", await Scalar("SELECT COUNT(*) FROM `{ds}.to_truncate`"));
	}

	// ALTER TABLE ADD COLUMN
	[Fact] public async Task AlterTable_AddColumn()
	{
		await Execute("CREATE TABLE `{ds}.alter_test` (id INT64)");
		await Execute("ALTER TABLE `{ds}.alter_test` ADD COLUMN name STRING");
		await Execute("INSERT INTO `{ds}.alter_test` (id, name) VALUES (1, 'hello')");
		Assert.Equal("hello", await Scalar("SELECT name FROM `{ds}.alter_test`"));
	}

	[Fact] public async Task AlterTable_AddMultipleColumns()
	{
		await Execute("CREATE TABLE `{ds}.alter_multi` (id INT64)");
		await Execute("ALTER TABLE `{ds}.alter_multi` ADD COLUMN name STRING");
		await Execute("ALTER TABLE `{ds}.alter_multi` ADD COLUMN value FLOAT64");
		await Execute("INSERT INTO `{ds}.alter_multi` (id, name, value) VALUES (1, 'test', 3.14)");
		var rows = await Query("SELECT * FROM `{ds}.alter_multi`");
		Assert.Single(rows);
	}

	// ALTER TABLE DROP COLUMN
	[Fact] public async Task AlterTable_DropColumn()
	{
		await Execute("CREATE TABLE `{ds}.drop_col` (id INT64, name STRING, extra STRING)");
		await Execute("INSERT INTO `{ds}.drop_col` (id, name, extra) VALUES (1, 'test', 'x')");
		await Execute("ALTER TABLE `{ds}.drop_col` DROP COLUMN extra");
		var rows = await Query("SELECT * FROM `{ds}.drop_col`");
		Assert.Single(rows);
	}

	// Table with default values via INSERT
	[Fact] public async Task InsertPartialColumns_NullDefault()
	{
		await Execute("CREATE TABLE `{ds}.partial_insert` (id INT64, name STRING, value FLOAT64)");
		await Execute("INSERT INTO `{ds}.partial_insert` (id, name) VALUES (1, 'test')");
		Assert.Null(await Scalar("SELECT value FROM `{ds}.partial_insert` WHERE id = 1"));
	}
}
