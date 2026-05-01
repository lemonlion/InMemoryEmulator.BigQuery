using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Comprehensive INFORMATION_SCHEMA view tests.
/// Ref: https://cloud.google.com/bigquery/docs/information-schema-intro
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class InformationSchemaComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public InformationSchemaComprehensiveTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_is_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		// Create tables with various types
		await client.CreateTableAsync(_datasetId, "base_table", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING" },
				new TableFieldSchema { Name = "score", Type = "FLOAT" },
				new TableFieldSchema { Name = "active", Type = "BOOL" },
				new TableFieldSchema { Name = "created", Type = "TIMESTAMP" },
			]
		});

		// Create table with nested struct
		await client.CreateTableAsync(_datasetId, "nested_table", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "info", Type = "RECORD", Fields = [
					new TableFieldSchema { Name = "first_name", Type = "STRING" },
					new TableFieldSchema { Name = "last_name", Type = "STRING" },
				] },
			]
		});

		// Create a view
		try { await client.ExecuteQueryAsync($"CREATE VIEW `{_datasetId}.base_view` AS SELECT id, name FROM `{_datasetId}.base_table`", parameters: null); } catch { }

		// Create a function
		try { await client.ExecuteQueryAsync($"CREATE FUNCTION `{_datasetId}.my_func`(x INT64) RETURNS INT64 AS (x * 2)", parameters: null); } catch { }
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
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

	// ==== INFORMATION_SCHEMA.TABLES ====
	[Fact] public async Task Tables_ListsAllTables()
	{
		var rows = await Query($"SELECT table_name, table_type FROM `{_datasetId}.INFORMATION_SCHEMA.TABLES` ORDER BY table_name");
		Assert.True(rows.Count >= 3); // base_table, nested_table, base_view
	}

	[Fact] public async Task Tables_BaseTableType()
	{
		var rows = await Query($"SELECT table_type FROM `{_datasetId}.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'base_table'");
		Assert.Equal("BASE TABLE", rows[0]["table_type"]?.ToString());
	}

	[Fact] public async Task Tables_ViewType()
	{
		var rows = await Query($"SELECT table_type FROM `{_datasetId}.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'base_view'");
		Assert.Equal("VIEW", rows[0]["table_type"]?.ToString());
	}

	[Fact] public async Task Tables_WhereFilter()
	{
		var rows = await Query($"SELECT table_name FROM `{_datasetId}.INFORMATION_SCHEMA.TABLES` WHERE table_name LIKE 'base_%' ORDER BY table_name");
		Assert.Equal(2, rows.Count);
	}

	// ==== INFORMATION_SCHEMA.COLUMNS ====
	[Fact] public async Task Columns_ListsAllColumns()
	{
		var rows = await Query($"SELECT column_name, data_type FROM `{_datasetId}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'base_table' ORDER BY ordinal_position");
		Assert.Equal(5, rows.Count);
		Assert.Equal("id", rows[0]["column_name"]?.ToString());
	}

	[Fact] public async Task Columns_DataTypes()
	{
		var rows = await Query($"SELECT column_name, data_type FROM `{_datasetId}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'base_table' ORDER BY ordinal_position");
		Assert.Equal("INT64", rows[0]["data_type"]?.ToString());
		Assert.Equal("STRING", rows[1]["data_type"]?.ToString());
		Assert.Equal("FLOAT64", rows[2]["data_type"]?.ToString());
	}

	[Fact] public async Task Columns_IsNullable()
	{
		var rows = await Query($"SELECT column_name, is_nullable FROM `{_datasetId}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'base_table' AND column_name = 'id'");
		Assert.Equal("NO", rows[0]["is_nullable"]?.ToString());
	}

	[Fact] public async Task Columns_NullableColumn()
	{
		var rows = await Query($"SELECT column_name, is_nullable FROM `{_datasetId}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'base_table' AND column_name = 'name'");
		Assert.Equal("YES", rows[0]["is_nullable"]?.ToString());
	}

	[Fact] public async Task Columns_OrdinalPosition()
	{
		var rows = await Query($"SELECT column_name, ordinal_position FROM `{_datasetId}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'base_table' ORDER BY ordinal_position");
		Assert.Equal("1", rows[0]["ordinal_position"]?.ToString());
		Assert.Equal("5", rows[4]["ordinal_position"]?.ToString());
	}

	[Fact] public async Task Columns_ForView()
	{
		var rows = await Query($"SELECT column_name FROM `{_datasetId}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'base_view' ORDER BY ordinal_position");
		Assert.Equal(2, rows.Count);
		Assert.Equal("id", rows[0]["column_name"]?.ToString());
		Assert.Equal("name", rows[1]["column_name"]?.ToString());
	}

	// ==== INFORMATION_SCHEMA.COLUMN_FIELD_PATHS ====
	[Fact] public async Task ColumnFieldPaths_FlatTable()
	{
		var rows = await Query($"SELECT column_name, field_path FROM `{_datasetId}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` WHERE table_name = 'base_table' ORDER BY field_path");
		Assert.True(rows.Count >= 5);
	}

	[Fact] public async Task ColumnFieldPaths_NestedTable()
	{
		var rows = await Query($"SELECT field_path, data_type FROM `{_datasetId}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` WHERE table_name = 'nested_table' ORDER BY field_path");
		Assert.True(rows.Count >= 3); // id, info, info.first_name, info.last_name
	}

	// ==== INFORMATION_SCHEMA.SCHEMATA ====
	[Fact] public async Task Schemata_ContainsDataset()
	{
		var rows = await Query($"SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA WHERE schema_name = '{_datasetId}'");
		Assert.Single(rows);
	}

	// ==== INFORMATION_SCHEMA.VIEWS ====
	[Fact] public async Task Views_ListsViews()
	{
		var rows = await Query($"SELECT table_name, view_definition FROM `{_datasetId}.INFORMATION_SCHEMA.VIEWS` WHERE table_name = 'base_view'");
		Assert.Single(rows);
		Assert.Contains("base_table", rows[0]["view_definition"]?.ToString());
	}

	[Fact] public async Task Views_DoesNotListBaseTables()
	{
		var rows = await Query($"SELECT table_name FROM `{_datasetId}.INFORMATION_SCHEMA.VIEWS` WHERE table_name = 'base_table'");
		Assert.Empty(rows);
	}

	// ==== INFORMATION_SCHEMA.ROUTINES ====
	[Fact] public async Task Routines_ListsFunctions()
	{
		var rows = await Query($"SELECT routine_name FROM `{_datasetId}.INFORMATION_SCHEMA.ROUTINES`");
		Assert.Contains(rows, r => r["routine_name"]?.ToString() == "my_func");
	}

	// ==== INFORMATION_SCHEMA.TABLE_OPTIONS ====
	[Fact] public async Task TableOptions_AfterSetDescription()
	{
		var client = await _fixture.GetClientAsync();
		await client.PatchTableAsync(_datasetId, "base_table", new Table { Description = "Test description" });
		var rows = await Query($"SELECT option_name, option_value FROM `{_datasetId}.INFORMATION_SCHEMA.TABLE_OPTIONS` WHERE table_name = 'base_table' AND option_name = 'description'");
		Assert.Single(rows);
		Assert.Contains("Test description", rows[0]["option_value"]?.ToString());
	}

	// ==== INFORMATION_SCHEMA.PARTITIONS ====
	[Fact] public async Task Partitions_NonPartitioned_Empty()
	{
		var rows = await Query($"SELECT * FROM `{_datasetId}.INFORMATION_SCHEMA.PARTITIONS` WHERE table_name = 'base_table'");
		// Non-partitioned tables may return empty or a single __NULL__ partition
		Assert.True(rows.Count <= 1);
	}

	// ==== Cross-view queries ====
	[Fact] public async Task JoinTablesAndColumns()
	{
		var rows = await Query($@"
			SELECT t.table_name, c.column_name
			FROM `{_datasetId}.INFORMATION_SCHEMA.TABLES` t
			JOIN `{_datasetId}.INFORMATION_SCHEMA.COLUMNS` c ON t.table_name = c.table_name
			WHERE t.table_name = 'base_table'
			ORDER BY c.ordinal_position
		");
		Assert.Equal(5, rows.Count);
	}

	[Fact] public async Task CountColumnsPerTable()
	{
		var rows = await Query($@"
			SELECT table_name, COUNT(*) AS col_count
			FROM `{_datasetId}.INFORMATION_SCHEMA.COLUMNS`
			GROUP BY table_name
			ORDER BY table_name
		");
		Assert.True(rows.Count >= 2);
	}
}
