using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for DDL operations (Phase 12): CREATE TABLE, DROP TABLE, ALTER TABLE, CREATE VIEW.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class DdlTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public DdlTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_ddl_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();

		// Seed table for CTAS and view tests
		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "source", schema);
		await client.InsertRowsAsync(_datasetId, "source", new[]
		{
			new BigQueryInsertRow("r1") { ["id"] = 1, ["name"] = "Alice" },
			new BigQueryInsertRow("r2") { ["id"] = 2, ["name"] = "Bob" },
		});
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

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_statement
	//   "Creates a new table."
	[Fact]
	public async Task CreateTable_ViaSql()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"CREATE TABLE `{_datasetId}.new_table` (col1 INT64, col2 STRING)",
			parameters: null);

		var table = await client.GetTableAsync(_datasetId, "new_table");
		Assert.NotNull(table);
	}

	[Fact]
	public async Task CreateTable_IfNotExists_NoError()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"CREATE TABLE `{_datasetId}.existing` (col1 INT64)",
			parameters: null);

		// Should not throw
		await client.ExecuteQueryAsync(
			$"CREATE TABLE IF NOT EXISTS `{_datasetId}.existing` (col1 INT64)",
			parameters: null);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_as_select
	//   "Creates a table from the result of a query."
	[Fact]
	public async Task CreateTableAsSelect_PopulatesFromQuery()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"CREATE TABLE `{_datasetId}.ctas_table` AS SELECT * FROM `{_datasetId}.source`",
			parameters: null);

		var results = await client.ExecuteQueryAsync(
			$"SELECT COUNT(*) AS cnt FROM `{_datasetId}.ctas_table`",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(2L, Convert.ToInt64(rows[0]["cnt"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#drop_table_statement
	//   "Deletes a table."
	[Fact]
	public async Task DropTable_RemovesTable()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"CREATE TABLE `{_datasetId}.to_drop` (col1 INT64)",
			parameters: null);

		await client.ExecuteQueryAsync(
			$"DROP TABLE `{_datasetId}.to_drop`",
			parameters: null);

		await Assert.ThrowsAsync<Google.GoogleApiException>(
			() => client.GetTableAsync(_datasetId, "to_drop"));
	}

	[Fact]
	public async Task DropTable_IfExists_NoError()
	{
		var client = await _fixture.GetClientAsync();
		// Should not throw even though table doesn't exist
		await client.ExecuteQueryAsync(
			$"DROP TABLE IF EXISTS `{_datasetId}.nonexistent`",
			parameters: null);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_table_add_column_statement
	//   "Adds one or more columns."
	[Fact]
	public async Task AlterTable_AddColumn()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"CREATE TABLE `{_datasetId}.alter_tbl` (col1 INT64)",
			parameters: null);

		await client.ExecuteQueryAsync(
			$"ALTER TABLE `{_datasetId}.alter_tbl` ADD COLUMN col2 STRING",
			parameters: null);

		var table = await client.GetTableAsync(_datasetId, "alter_tbl");
		Assert.Equal(2, table.Schema.Fields.Count);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_view_statement
	//   "Creates a new view."
	[Fact]
	public async Task CreateView_ThenQuery()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"CREATE VIEW `{_datasetId}.my_view` AS SELECT name FROM `{_datasetId}.source`",
			parameters: null);

		var results = await client.ExecuteQueryAsync(
			$"SELECT * FROM `{_datasetId}.my_view`",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(2, rows.Count);
	}

	[Fact]
	public async Task DropView_RemovesView()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"CREATE VIEW `{_datasetId}.drop_view` AS SELECT 1 AS x",
			parameters: null);

		await client.ExecuteQueryAsync(
			$"DROP VIEW `{_datasetId}.drop_view`",
			parameters: null);

		// Querying should fail
		await Assert.ThrowsAnyAsync<Exception>(
			() => client.ExecuteQueryAsync(
				$"SELECT * FROM `{_datasetId}.drop_view`", parameters: null));
	}

	[Fact]
	public async Task CreateOrReplace_Table()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"CREATE TABLE `{_datasetId}.replace_tbl` (col1 INT64)",
			parameters: null);

		await client.ExecuteQueryAsync(
			$"CREATE OR REPLACE TABLE `{_datasetId}.replace_tbl` (col1 STRING, col2 FLOAT64)",
			parameters: null);

		var table = await client.GetTableAsync(_datasetId, "replace_tbl");
		Assert.Equal(2, table.Schema.Fields.Count);
	}
}
