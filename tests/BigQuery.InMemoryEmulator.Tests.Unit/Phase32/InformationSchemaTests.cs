using BigQuery.InMemoryEmulator.SqlEngine;
using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase32;

/// <summary>
/// Unit tests for INFORMATION_SCHEMA views.
/// Ref: https://cloud.google.com/bigquery/docs/information-schema-intro
/// </summary>
public class InformationSchemaTests
{
	private static (QueryExecutor Executor, InMemoryDataStore Store) CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;
		return (new QueryExecutor(store, "test_ds"), store);
	}

	private static InMemoryTable AddTable(InMemoryDataset ds, string tableId, TableSchema schema)
	{
		var table = new InMemoryTable("test_ds", tableId, schema);
		ds.Tables[tableId] = table;
		return table;
	}

	/// <summary>A minimal SelectStatement to mark a table as a view.</summary>
	private static SelectStatement DummyViewQuery => new(
		false, [new SelectItem(new LiteralExpr(1), null)],
		null, null, null, null, null, null, null);

	private static readonly TableSchema SimpleSchema = new()
	{
		Fields = [new TableFieldSchema { Name = "id", Type = "INTEGER" }]
	};

	#region TABLES view — table_type fix

	// Ref: https://cloud.google.com/bigquery/docs/information-schema-tables
	//   "table_type: The table type. For example, BASE TABLE for a standard table,
	//    VIEW for a view, MATERIALIZED VIEW, CLONE, SNAPSHOT, EXTERNAL."

	[Fact]
	public void Tables_View_ReturnsViewType()
	{
		var (exec, store) = CreateExecutor();
		var ds = store.Datasets["test_ds"];

		AddTable(ds, "my_table", SimpleSchema);

		// Create a view by setting ViewQuery on a table
		var viewTable = AddTable(ds, "my_view", SimpleSchema);
		viewTable.ViewQuery = DummyViewQuery;

		var (_, rows) = exec.Execute(
			"SELECT table_name, table_type FROM INFORMATION_SCHEMA.TABLES ORDER BY table_name");

		Assert.Equal(2, rows.Count);
		Assert.Equal("my_table", rows[0].F[0].V?.ToString());
		Assert.Equal("BASE TABLE", rows[0].F[1].V?.ToString());
		Assert.Equal("my_view", rows[1].F[0].V?.ToString());
		Assert.Equal("VIEW", rows[1].F[1].V?.ToString());
	}

	#endregion

	#region ROUTINES view

	// Ref: https://cloud.google.com/bigquery/docs/information-schema-routines
	//   "The INFORMATION_SCHEMA.ROUTINES view contains one row for each routine
	//    in a dataset."

	[Fact]
	public void Routines_ReturnsCreatedFunction()
	{
		var (exec, store) = CreateExecutor();
		var ds = store.Datasets["test_ds"];
		ds.Routines["double_it"] = new InMemoryRoutine(
			"test_ds", "double_it", "SCALAR_FUNCTION", "SQL", "x * 2",
			[("x", "INT64")], returnType: "INT64");

		var (_, rows) = exec.Execute(
			"SELECT routine_name, routine_type, routine_body, routine_definition, data_type " +
			"FROM INFORMATION_SCHEMA.ROUTINES");

		Assert.Single(rows);
		Assert.Equal("double_it", rows[0].F[0].V?.ToString());
		Assert.Equal("SCALAR_FUNCTION", rows[0].F[1].V?.ToString());
		Assert.Equal("SQL", rows[0].F[2].V?.ToString());
		Assert.Equal("x * 2", rows[0].F[3].V?.ToString());
		Assert.Equal("INT64", rows[0].F[4].V?.ToString());
	}

	[Fact]
	public void Routines_EmptyDataset_ReturnsNoRows()
	{
		var (exec, _) = CreateExecutor();

		var (_, rows) = exec.Execute(
			"SELECT routine_name FROM INFORMATION_SCHEMA.ROUTINES");

		Assert.Empty(rows);
	}

	[Fact]
	public void Routines_MultipleRoutines_ReturnsAll()
	{
		var (exec, store) = CreateExecutor();
		var ds = store.Datasets["test_ds"];
		ds.Routines["fn_a"] = new InMemoryRoutine(
			"test_ds", "fn_a", "SCALAR_FUNCTION", "SQL", "x + 1",
			[("x", "INT64")], returnType: "INT64");
		ds.Routines["fn_b"] = new InMemoryRoutine(
			"test_ds", "fn_b", "SCALAR_FUNCTION", "SQL", "UPPER(x)",
			[("x", "STRING")], returnType: "STRING");

		var (_, rows) = exec.Execute(
			"SELECT routine_name FROM INFORMATION_SCHEMA.ROUTINES ORDER BY routine_name");

		Assert.Equal(2, rows.Count);
		Assert.Equal("fn_a", rows[0].F[0].V?.ToString());
		Assert.Equal("fn_b", rows[1].F[0].V?.ToString());
	}

	#endregion

	#region VIEWS view

	// Ref: https://cloud.google.com/bigquery/docs/information-schema-views
	//   "The INFORMATION_SCHEMA.VIEWS view contains metadata about views."

	[Fact]
	public void Views_ReturnsCreatedView()
	{
		var (exec, store) = CreateExecutor();
		var ds = store.Datasets["test_ds"];

		AddTable(ds, "base_t", SimpleSchema);

		var viewTable = AddTable(ds, "my_view", SimpleSchema);
		viewTable.ViewQuery = DummyViewQuery;
		viewTable.ViewDefinitionSql = "SELECT id FROM base_t";

		var (_, rows) = exec.Execute(
			"SELECT table_name, view_definition, use_standard_sql " +
			"FROM INFORMATION_SCHEMA.VIEWS");

		Assert.Single(rows);
		Assert.Equal("my_view", rows[0].F[0].V?.ToString());
		Assert.Equal("SELECT id FROM base_t", rows[0].F[1].V?.ToString());
		Assert.Equal("YES", rows[0].F[2].V?.ToString());
	}

	[Fact]
	public void Views_ExcludesRegularTables()
	{
		var (exec, store) = CreateExecutor();
		var ds = store.Datasets["test_ds"];
		AddTable(ds, "regular_table", SimpleSchema);

		var (_, rows) = exec.Execute(
			"SELECT table_name FROM INFORMATION_SCHEMA.VIEWS");

		Assert.Empty(rows);
	}

	#endregion

	#region TABLE_OPTIONS view

	// Ref: https://cloud.google.com/bigquery/docs/information-schema-table-options
	//   "The INFORMATION_SCHEMA.TABLE_OPTIONS view contains one row for each
	//    option, for each table or view in a dataset."

	[Fact]
	public void TableOptions_ReturnsDescriptionOption()
	{
		var (exec, store) = CreateExecutor();
		var ds = store.Datasets["test_ds"];
		var table = AddTable(ds, "described_table", SimpleSchema);
		table.Description = "My test table";

		var (_, rows) = exec.Execute(
			"SELECT table_name, option_name, option_type, option_value " +
			"FROM INFORMATION_SCHEMA.TABLE_OPTIONS " +
			"WHERE option_name = 'description'");

		Assert.Single(rows);
		Assert.Equal("described_table", rows[0].F[0].V?.ToString());
		Assert.Equal("description", rows[0].F[1].V?.ToString());
		Assert.Equal("STRING", rows[0].F[2].V?.ToString());
		Assert.Contains("My test table", rows[0].F[3].V?.ToString());
	}

	[Fact]
	public void TableOptions_ReturnsLabelsOption()
	{
		var (exec, store) = CreateExecutor();
		var ds = store.Datasets["test_ds"];
		var table = AddTable(ds, "labeled_table", SimpleSchema);
		table.Labels = new Dictionary<string, string> { ["env"] = "test" };

		var (_, rows) = exec.Execute(
			"SELECT option_name, option_value FROM INFORMATION_SCHEMA.TABLE_OPTIONS " +
			"WHERE table_name = 'labeled_table' AND option_name = 'labels'");

		Assert.Single(rows);
		Assert.Equal("labels", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region COLUMN_FIELD_PATHS view

	// Ref: https://cloud.google.com/bigquery/docs/information-schema-column-field-paths
	//   "The INFORMATION_SCHEMA.COLUMN_FIELD_PATHS view contains one row for each
	//    column nested within a RECORD (or STRUCT) column."

	[Fact]
	public void ColumnFieldPaths_FlatSchema_ReturnsTopLevelColumns()
	{
		var (exec, store) = CreateExecutor();
		var ds = store.Datasets["test_ds"];
		AddTable(ds, "flat_table", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER" },
				new TableFieldSchema { Name = "name", Type = "STRING" },
			]
		});

		var (_, rows) = exec.Execute(
			"SELECT table_name, column_name, field_path, data_type " +
			"FROM INFORMATION_SCHEMA.COLUMN_FIELD_PATHS " +
			"WHERE table_name = 'flat_table' ORDER BY field_path");

		Assert.Equal(2, rows.Count);
		Assert.Equal("id", rows[0].F[2].V?.ToString());
		Assert.Equal("INT64", rows[0].F[3].V?.ToString());
		Assert.Equal("name", rows[1].F[2].V?.ToString());
	}

	[Fact]
	public void ColumnFieldPaths_NestedRecord_ReturnsNestedPaths()
	{
		var (exec, store) = CreateExecutor();
		var ds = store.Datasets["test_ds"];
		AddTable(ds, "nested_table", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER" },
				new TableFieldSchema
				{
					Name = "address", Type = "RECORD", Mode = "NULLABLE",
					Fields =
					[
						new TableFieldSchema { Name = "city", Type = "STRING" },
						new TableFieldSchema { Name = "zip", Type = "STRING" },
					]
				},
			]
		});

		var (_, rows) = exec.Execute(
			"SELECT column_name, field_path, data_type " +
			"FROM INFORMATION_SCHEMA.COLUMN_FIELD_PATHS " +
			"WHERE table_name = 'nested_table' ORDER BY field_path");

		// Should have: address, address.city, address.zip, id
		Assert.Equal(4, rows.Count);
		var paths = rows.Select(r => r.F[1].V?.ToString()).ToList();
		Assert.Contains("id", paths);
		Assert.Contains("address", paths);
		Assert.Contains("address.city", paths);
		Assert.Contains("address.zip", paths);
	}

	#endregion

	#region PARTITIONS view

	// Ref: https://cloud.google.com/bigquery/docs/information-schema-partitions
	//   "The INFORMATION_SCHEMA.PARTITIONS view provides one row for each
	//    partition of a partitioned table."

	[Fact]
	public void Partitions_NonPartitionedTable_ReturnsNoRows()
	{
		var (exec, store) = CreateExecutor();
		var ds = store.Datasets["test_ds"];
		AddTable(ds, "plain_table", SimpleSchema);

		var (_, rows) = exec.Execute(
			"SELECT table_name FROM INFORMATION_SCHEMA.PARTITIONS");

		Assert.Empty(rows);
	}

	[Fact]
	public void Partitions_PartitionedTable_ReturnsPartitionInfo()
	{
		var (exec, store) = CreateExecutor();
		var ds = store.Datasets["test_ds"];
		var table = AddTable(ds, "partitioned_table", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER" },
				new TableFieldSchema { Name = "created", Type = "DATE" },
			]
		});
		table.TimePartitioning = new TimePartitioning { Type = "DAY", Field = "created" };

		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 1L, ["created"] = "2024-01-15" }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 2L, ["created"] = "2024-01-15" }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 3L, ["created"] = "2024-02-20" }));

		var (_, rows) = exec.Execute(
			"SELECT table_name, partition_id, total_rows " +
			"FROM INFORMATION_SCHEMA.PARTITIONS " +
			"ORDER BY partition_id");

		Assert.True(rows.Count >= 1);
		Assert.Equal("partitioned_table", rows[0].F[0].V?.ToString());
	}

	#endregion
}
