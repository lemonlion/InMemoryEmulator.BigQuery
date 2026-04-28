using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for INFORMATION_SCHEMA views: ROUTINES, VIEWS, TABLE_OPTIONS,
/// COLUMN_FIELD_PATHS, PARTITIONS, and the TABLES table_type fix.
/// Ref: https://cloud.google.com/bigquery/docs/information-schema-intro
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class InformationSchemaViewsTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public InformationSchemaViewsTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_is_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();

		// Base table with nested schema
		await client.CreateTableAsync(_datasetId, "base_table", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING" },
				new TableFieldSchema
				{
					Name = "address", Type = "RECORD",
					Fields =
					[
						new TableFieldSchema { Name = "city", Type = "STRING" },
						new TableFieldSchema { Name = "zip", Type = "STRING" },
					]
				},
			]
		});

		// Create a view via multi-statement script (uses semicolons → ProceduralExecutor)
		await client.ExecuteQueryAsync(
			$"CREATE VIEW `{_datasetId}.my_view` AS SELECT id, name FROM `{_datasetId}.base_table`;",
			parameters: null);
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

	#region TABLES — table_type VIEW fix

	// Ref: https://cloud.google.com/bigquery/docs/information-schema-tables
	//   "table_type: BASE TABLE for a standard table, VIEW for a view."
	[Fact]
	public async Task Tables_ReturnsCorrectTypeForViews()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT table_name, table_type FROM `{_datasetId}.INFORMATION_SCHEMA.TABLES` ORDER BY table_name",
			parameters: null);

		var rows = results.ToList();
		Assert.Equal(2, rows.Count);
		Assert.Equal("base_table", rows[0]["table_name"]);
		Assert.Equal("BASE TABLE", rows[0]["table_type"]);
		Assert.Equal("my_view", rows[1]["table_name"]);
		Assert.Equal("VIEW", rows[1]["table_type"]);
	}

	#endregion

	#region VIEWS

	// Ref: https://cloud.google.com/bigquery/docs/information-schema-views
	[Fact]
	public async Task Views_ReturnsOnlyViews()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT table_name, use_standard_sql FROM `{_datasetId}.INFORMATION_SCHEMA.VIEWS`",
			parameters: null);

		var rows = results.ToList();
		Assert.Single(rows);
		Assert.Equal("my_view", rows[0]["table_name"]);
		Assert.Equal("YES", rows[0]["use_standard_sql"]);
	}

	#endregion

	#region ROUTINES

	// Ref: https://cloud.google.com/bigquery/docs/information-schema-routines
	[Fact]
	public async Task Routines_ReturnsCreatedFunction()
	{
		var client = await _fixture.GetClientAsync();

		// Create a UDF via multi-statement script (no backticks — parser uses regex)
		await client.ExecuteQueryAsync(
			$"CREATE FUNCTION {_datasetId}.triple_it(x INT64) RETURNS INT64 AS (x * 3);",
			parameters: null);

		var results = await client.ExecuteQueryAsync(
			$"SELECT routine_name, routine_type, routine_body FROM `{_datasetId}.INFORMATION_SCHEMA.ROUTINES`",
			parameters: null);

		var rows = results.ToList();
		Assert.Single(rows);
		Assert.Equal("triple_it", rows[0]["routine_name"]);
		Assert.Equal("SQL", rows[0]["routine_body"]);
	}

	#endregion

	#region TABLE_OPTIONS

	// Ref: https://cloud.google.com/bigquery/docs/information-schema-table-options
	[Fact]
	public async Task TableOptions_ReturnsDescriptionWhenSet()
	{
		var client = await _fixture.GetClientAsync();

		// Update table with description
		await client.UpdateTableAsync(
			client.GetTableReference(_datasetId, "base_table"),
			new Table { Description = "Integration test table" });

		var results = await client.ExecuteQueryAsync(
			$"SELECT table_name, option_name, option_type, option_value " +
			$"FROM `{_datasetId}.INFORMATION_SCHEMA.TABLE_OPTIONS` " +
			$"WHERE option_name = 'description' AND table_name = 'base_table'",
			parameters: null);

		var rows = results.ToList();
		Assert.Single(rows);
		Assert.Equal("description", rows[0]["option_name"]);
		Assert.Equal("STRING", rows[0]["option_type"]);
	}

	#endregion

	#region COLUMN_FIELD_PATHS

	// Ref: https://cloud.google.com/bigquery/docs/information-schema-column-field-paths
	[Fact]
	public async Task ColumnFieldPaths_ReturnsNestedPaths()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT column_name, field_path, data_type " +
			$"FROM `{_datasetId}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` " +
			$"WHERE table_name = 'base_table' ORDER BY field_path",
			parameters: null);

		var rows = results.ToList();
		// id, name, address, address.city, address.zip = 5 rows
		Assert.Equal(5, rows.Count);

		var paths = rows.Select(r => (string)r["field_path"]).ToList();
		Assert.Contains("id", paths);
		Assert.Contains("name", paths);
		Assert.Contains("address", paths);
		Assert.Contains("address.city", paths);
		Assert.Contains("address.zip", paths);
	}

	#endregion

	#region PARTITIONS

	// Ref: https://cloud.google.com/bigquery/docs/information-schema-partitions
	[Fact]
	public async Task Partitions_NonPartitionedTable_ReturnsNoRows()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT table_name FROM `{_datasetId}.INFORMATION_SCHEMA.PARTITIONS`",
			parameters: null);

		Assert.Empty(results.ToList());
	}

	#endregion
}
