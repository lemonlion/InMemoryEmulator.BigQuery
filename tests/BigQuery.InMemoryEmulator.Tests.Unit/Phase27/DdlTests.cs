using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase27;

/// <summary>
/// Phase 27: DDL — ALTER COLUMN variants, CREATE TABLE LIKE/COPY/CLONE,
/// CREATE/DROP SCHEMA, and various stub DDL statements.
/// </summary>
public class DdlTests
{
	private static (QueryExecutor Exec, InMemoryDataStore Store) CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("ds");
		store.Datasets["ds"] = ds;
		return (new QueryExecutor(store, "ds"), store);
	}

	private static void SeedTable(InMemoryDataStore store)
	{
		var ds = store.Datasets["ds"];
		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE",
					DefaultValueExpression = "'unknown'" },
				new TableFieldSchema { Name = "score", Type = "FLOAT", Mode = "NULLABLE" },
			]
		};
		var table = new InMemoryTable("ds", "src", schema);
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 1L, ["name"] = "Alice", ["score"] = 95.5 }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 2L, ["name"] = "Bob", ["score"] = 82.0 }));
		ds.Tables["src"] = table;
	}

	#region ALTER COLUMN SET DATA TYPE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_column_set_data_type
	//   "Changes the data type of a column."
	[Fact]
	public void AlterColumn_SetDataType()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);

		exec.Execute("ALTER TABLE src ALTER COLUMN score SET DATA TYPE STRING");
		var field = store.Datasets["ds"].Tables["src"].Schema.Fields
			.First(f => f.Name == "score");
		Assert.Equal("STRING", field.Type);
	}

	#endregion

	#region ALTER COLUMN SET DEFAULT

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_column_set_default
	//   "Sets or changes the default value expression for a column."
	[Fact]
	public void AlterColumn_SetDefault()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);

		exec.Execute("ALTER TABLE src ALTER COLUMN name SET DEFAULT 'N/A'");
		var field = store.Datasets["ds"].Tables["src"].Schema.Fields
			.First(f => f.Name == "name");
		Assert.Equal("'N/A'", field.DefaultValueExpression);
	}

	#endregion

	#region ALTER COLUMN DROP DEFAULT

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_column_drop_default
	//   "Removes the default value expression from a column."
	[Fact]
	public void AlterColumn_DropDefault()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);

		exec.Execute("ALTER TABLE src ALTER COLUMN name DROP DEFAULT");
		var field = store.Datasets["ds"].Tables["src"].Schema.Fields
			.First(f => f.Name == "name");
		Assert.Null(field.DefaultValueExpression);
	}

	#endregion

	#region ALTER COLUMN DROP NOT NULL

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_column_drop_not_null
	//   "Removes the NOT NULL constraint from a column."
	[Fact]
	public void AlterColumn_DropNotNull()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);

		exec.Execute("ALTER TABLE src ALTER COLUMN id DROP NOT NULL");
		var field = store.Datasets["ds"].Tables["src"].Schema.Fields
			.First(f => f.Name == "id");
		Assert.Equal("NULLABLE", field.Mode);
	}

	#endregion

	#region ALTER TABLE SET OPTIONS

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_table_set_options
	//   "Sets options on a table."
	[Fact]
	public void AlterTable_SetOptions_NoError()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);

		// Should not throw — no-op that stores metadata
		exec.Execute("ALTER TABLE src SET OPTIONS (description = 'test table')");
		Assert.True(store.Datasets["ds"].Tables.ContainsKey("src"));
	}

	#endregion

	#region CREATE TABLE LIKE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_like
	//   "Creates a new table with the same schema as the source table."
	[Fact]
	public void CreateTableLike_CopiesSchema()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);

		exec.Execute("CREATE TABLE dst LIKE src");
		var dst = store.Datasets["ds"].Tables["dst"];
		Assert.Equal(3, dst.Schema.Fields.Count);
		Assert.Empty(dst.Rows); // no data copied
	}

	#endregion

	#region CREATE TABLE COPY

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_copy
	//   "Creates a new table by copying the schema and data from the source table."
	[Fact]
	public void CreateTableCopy_CopiesSchemaAndData()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);

		exec.Execute("CREATE TABLE dst COPY src");
		var dst = store.Datasets["ds"].Tables["dst"];
		Assert.Equal(3, dst.Schema.Fields.Count);
		Assert.Equal(2, dst.Rows.Count);
	}

	#endregion

	#region CREATE TABLE CLONE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_clone
	//   "Creates a table clone."
	[Fact]
	public void CreateTableClone_CopiesSchemaAndData()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);

		exec.Execute("CREATE TABLE dst CLONE src");
		var dst = store.Datasets["ds"].Tables["dst"];
		Assert.Equal(3, dst.Schema.Fields.Count);
		Assert.Equal(2, dst.Rows.Count);
	}

	#endregion

	#region CREATE SNAPSHOT TABLE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_snapshot_table
	//   "Creates a table snapshot."
	[Fact]
	public void CreateSnapshotTable_CopiesSchemaAndData()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);

		exec.Execute("CREATE SNAPSHOT TABLE dst CLONE src");
		var dst = store.Datasets["ds"].Tables["dst"];
		Assert.Equal(3, dst.Schema.Fields.Count);
		Assert.Equal(2, dst.Rows.Count);
	}

	#endregion

	#region CREATE SCHEMA / DROP SCHEMA

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_schema_statement
	//   "Creates a new schema (dataset)."
	[Fact]
	public void CreateSchema_CreatesDataset()
	{
		var (exec, store) = CreateExecutor();

		exec.Execute("CREATE SCHEMA new_ds");
		Assert.True(store.Datasets.ContainsKey("new_ds"));
	}

	[Fact]
	public void CreateSchema_IfNotExists()
	{
		var (exec, store) = CreateExecutor();

		exec.Execute("CREATE SCHEMA IF NOT EXISTS ds");
		Assert.True(store.Datasets.ContainsKey("ds")); // already exists, no error
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#drop_schema_statement
	//   "Drops a schema (dataset)."
	[Fact]
	public void DropSchema_RemovesDataset()
	{
		var (exec, store) = CreateExecutor();
		store.Datasets["to_drop"] = new InMemoryDataset("to_drop");

		exec.Execute("DROP SCHEMA to_drop");
		Assert.False(store.Datasets.ContainsKey("to_drop"));
	}

	[Fact]
	public void DropSchema_IfExists_NoError()
	{
		var (exec, store) = CreateExecutor();

		exec.Execute("DROP SCHEMA IF EXISTS nonexistent");
		// No error
	}

	#endregion

	#region CREATE/DROP PROCEDURE (stub)

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_procedure
	[Fact]
	public void CreateProcedure_NoError()
	{
		var (exec, store) = CreateExecutor();
		// Should parse and not throw
		exec.Execute("CREATE PROCEDURE ds.my_proc() BEGIN SELECT 1; END");
	}

	[Fact]
	public void DropProcedure_NoError()
	{
		var (exec, store) = CreateExecutor();
		exec.Execute("DROP PROCEDURE IF EXISTS ds.my_proc");
	}

	#endregion

	#region CREATE/DROP MATERIALIZED VIEW (stub)

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_materialized_view
	[Fact]
	public void CreateMaterializedView_NoError()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);

		exec.Execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM src");
		Assert.True(store.Datasets["ds"].Tables.ContainsKey("mv"));
	}

	[Fact]
	public void DropMaterializedView_NoError()
	{
		var (exec, store) = CreateExecutor();
		exec.Execute("DROP MATERIALIZED VIEW IF EXISTS mv");
	}

	#endregion

	#region CREATE/DROP EXTERNAL TABLE (stub)

	[Fact]
	public void CreateExternalTable_NoError()
	{
		var (exec, store) = CreateExecutor();
		exec.Execute("CREATE EXTERNAL TABLE ext (x INT64)");
		Assert.True(store.Datasets["ds"].Tables.ContainsKey("ext"));
	}

	[Fact]
	public void DropExternalTable_NoError()
	{
		var (exec, store) = CreateExecutor();
		exec.Execute("DROP EXTERNAL TABLE IF EXISTS ext");
	}

	#endregion

	#region CREATE/DROP TABLE FUNCTION (stub)

	[Fact]
	public void CreateTableFunction_NoError()
	{
		var (exec, store) = CreateExecutor();
		exec.Execute("CREATE TABLE FUNCTION ds.my_tvf() AS SELECT 1 AS x");
	}

	[Fact]
	public void DropTableFunction_NoError()
	{
		var (exec, store) = CreateExecutor();
		exec.Execute("DROP TABLE FUNCTION IF EXISTS ds.my_tvf");
	}

	#endregion

	#region CREATE/DROP ROW ACCESS POLICY (stub)

	[Fact]
	public void CreateRowAccessPolicy_NoError()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);
		exec.Execute("CREATE ROW ACCESS POLICY my_policy ON src GRANT TO ('user@example.com') FILTER USING (TRUE)");
	}

	[Fact]
	public void DropRowAccessPolicy_NoError()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);
		exec.Execute("DROP ROW ACCESS POLICY IF EXISTS my_policy ON src");
	}

	#endregion

	#region CREATE/DROP SEARCH INDEX (stub)

	[Fact]
	public void CreateSearchIndex_NoError()
	{
		var (exec, store) = CreateExecutor();
		SeedTable(store);
		exec.Execute("CREATE SEARCH INDEX my_idx ON src (name)");
	}

	[Fact]
	public void DropSearchIndex_NoError()
	{
		var (exec, store) = CreateExecutor();
		exec.Execute("DROP SEARCH INDEX IF EXISTS my_idx ON src");
	}

	#endregion
}
