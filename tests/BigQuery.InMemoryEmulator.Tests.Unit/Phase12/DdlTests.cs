using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase12;

/// <summary>
/// Unit tests for DDL statements: CREATE TABLE, DROP TABLE, ALTER TABLE, views (Phase 12).
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language
/// </summary>
public class DdlTests
{
	private static (QueryExecutor Executor, InMemoryDataStore Store) CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;

		var schema = new TableSchema
		{
			Fields = [
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
			]
		};
		var table = new InMemoryTable("test_ds", "items", schema);
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 1L, ["name"] = "Alpha" }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 2L, ["name"] = "Beta" }));
		ds.Tables["items"] = table;

		return (new QueryExecutor(store, "test_ds"), store);
	}

	// --- CREATE TABLE ---

	[Fact]
	public void CreateTable_CreatesNewTable()
	{
		var (exec, store) = CreateExecutor();
		exec.Execute("CREATE TABLE new_table (col1 STRING, col2 INTEGER)");

		var ds = store.Datasets["test_ds"];
		Assert.True(ds.Tables.ContainsKey("new_table"));
		Assert.Equal(2, ds.Tables["new_table"].Schema.Fields.Count);
		Assert.Equal("STRING", ds.Tables["new_table"].Schema.Fields[0].Type);
	}

	[Fact]
	public void CreateTable_IfNotExists_DoesNotThrow()
	{
		var (exec, store) = CreateExecutor();
		exec.Execute("CREATE TABLE IF NOT EXISTS items (col1 STRING)");

		// Original table should be unchanged
		Assert.Equal(2, store.Datasets["test_ds"].Tables["items"].Schema.Fields.Count);
	}

	[Fact]
	public void CreateTable_OrReplace_ReplacesExisting()
	{
		var (exec, store) = CreateExecutor();
		exec.Execute("CREATE OR REPLACE TABLE items (col1 STRING)");

		var tbl = store.Datasets["test_ds"].Tables["items"];
		Assert.Single(tbl.Schema.Fields);
		Assert.Equal(0, tbl.RowCount);
	}

	[Fact]
	public void CreateTable_AlreadyExists_Throws()
	{
		var (exec, _) = CreateExecutor();
		Assert.Throws<InvalidOperationException>(() =>
			exec.Execute("CREATE TABLE items (col1 STRING)"));
	}

	// --- CREATE TABLE AS SELECT ---

	[Fact]
	public void CreateTableAsSelect_PopulatesFromQuery()
	{
		var (exec, store) = CreateExecutor();
		exec.Execute("CREATE TABLE copy_table AS SELECT id, name FROM items WHERE id = 1");

		var tbl = store.Datasets["test_ds"].Tables["copy_table"];
		Assert.Equal(2, tbl.Schema.Fields.Count);
		Assert.Equal(1, tbl.RowCount);
	}

	// --- DROP TABLE ---

	[Fact]
	public void DropTable_RemovesTable()
	{
		var (exec, store) = CreateExecutor();
		exec.Execute("DROP TABLE items");

		Assert.False(store.Datasets["test_ds"].Tables.ContainsKey("items"));
	}

	[Fact]
	public void DropTable_IfExists_NoError()
	{
		var (exec, _) = CreateExecutor();
		exec.Execute("DROP TABLE IF EXISTS nonexistent");
	}

	[Fact]
	public void DropTable_NotExists_Throws()
	{
		var (exec, _) = CreateExecutor();
		Assert.Throws<InvalidOperationException>(() =>
			exec.Execute("DROP TABLE nonexistent"));
	}

	// --- ALTER TABLE ---

	[Fact]
	public void AlterTable_AddColumn()
	{
		var (exec, store) = CreateExecutor();
		exec.Execute("ALTER TABLE items ADD COLUMN score INTEGER");

		var tbl = store.Datasets["test_ds"].Tables["items"];
		Assert.Equal(3, tbl.Schema.Fields.Count);
		Assert.Equal("score", tbl.Schema.Fields[2].Name);
	}

	[Fact]
	public void AlterTable_DropColumn()
	{
		var (exec, store) = CreateExecutor();
		exec.Execute("ALTER TABLE items DROP COLUMN name");

		var tbl = store.Datasets["test_ds"].Tables["items"];
		Assert.Single(tbl.Schema.Fields);
		Assert.Equal("id", tbl.Schema.Fields[0].Name);
		// Verify data also removed
		Assert.False(tbl.Rows[0].Fields.ContainsKey("name"));
	}

	[Fact]
	public void AlterTable_RenameTable()
	{
		var (exec, store) = CreateExecutor();
		exec.Execute("ALTER TABLE items RENAME TO renamed_items");

		var ds = store.Datasets["test_ds"];
		Assert.False(ds.Tables.ContainsKey("items"));
		Assert.True(ds.Tables.ContainsKey("renamed_items"));
	}

	// --- CREATE VIEW ---

	[Fact]
	public void CreateView_CreatesView()
	{
		var (exec, store) = CreateExecutor();
		exec.Execute("CREATE VIEW my_view AS SELECT id FROM items");

		Assert.True(store.Datasets["test_ds"].Tables.ContainsKey("my_view"));
		Assert.Single(store.Datasets["test_ds"].Tables["my_view"].Schema.Fields);
	}

	// --- DROP VIEW ---

	[Fact]
	public void DropView_RemovesView()
	{
		var (exec, store) = CreateExecutor();
		exec.Execute("CREATE VIEW my_view AS SELECT id FROM items");
		exec.Execute("DROP VIEW my_view");

		Assert.False(store.Datasets["test_ds"].Tables.ContainsKey("my_view"));
	}

	[Fact]
	public void DropView_IfExists_NoError()
	{
		var (exec, _) = CreateExecutor();
		exec.Execute("DROP VIEW IF EXISTS nonexistent");
	}
}
