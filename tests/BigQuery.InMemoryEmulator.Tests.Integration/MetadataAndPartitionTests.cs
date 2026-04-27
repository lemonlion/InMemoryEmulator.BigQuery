using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for INFORMATION_SCHEMA, wildcard tables, partitioning (Phases 13-14).
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class MetadataAndPartitionTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public MetadataAndPartitionTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_meta_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();

		// Create multiple tables for wildcard / INFORMATION_SCHEMA tests
		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "events_2023", schema);
		await client.CreateTableAsync(_datasetId, "events_2024", schema);
		await client.CreateTableAsync(_datasetId, "other_table", schema);

		await client.InsertRowsAsync(_datasetId, "events_2023", new[]
		{
			new BigQueryInsertRow("r1") { ["id"] = 1, ["name"] = "Event A" },
		});
		await client.InsertRowsAsync(_datasetId, "events_2024", new[]
		{
			new BigQueryInsertRow("r2") { ["id"] = 2, ["name"] = "Event B" },
			new BigQueryInsertRow("r3") { ["id"] = 3, ["name"] = "Event C" },
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

	// --- INFORMATION_SCHEMA ---

	// Ref: https://cloud.google.com/bigquery/docs/information-schema-tables
	//   "INFORMATION_SCHEMA.TABLES provides metadata about tables."
	[Fact]
	public async Task InformationSchema_Tables_ListsAllTables()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT table_name FROM `{_datasetId}.INFORMATION_SCHEMA.TABLES` ORDER BY table_name",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(3, rows.Count);
	}

	// Ref: https://cloud.google.com/bigquery/docs/information-schema-columns
	//   "INFORMATION_SCHEMA.COLUMNS provides metadata about columns."
	[Fact]
	public async Task InformationSchema_Columns_ListsColumns()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT column_name FROM `{_datasetId}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'events_2023'",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(2, rows.Count); // id, name
	}

	// Ref: https://cloud.google.com/bigquery/docs/information-schema-datasets
	//   "INFORMATION_SCHEMA.SCHEMATA provides metadata about datasets."
	[Fact]
	public async Task InformationSchema_Schemata_ListsDatasets()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT schema_name FROM `{_datasetId}.INFORMATION_SCHEMA.SCHEMATA`",
			parameters: null);
		var rows = results.ToList();
		Assert.True(rows.Count >= 1);
	}

	// --- Wildcard Tables ---

	// Ref: https://cloud.google.com/bigquery/docs/querying-wildcard-tables
	//   "You can use wildcard table syntax to query multiple tables."
	[Fact]
	public async Task WildcardTable_MatchesMultipleTables()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT * FROM `{_datasetId}.events_*`",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(3, rows.Count); // 1 from 2023 + 2 from 2024
	}

	[Fact]
	public async Task WildcardTable_FilterBySuffix()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT * FROM `{_datasetId}.events_*` WHERE _TABLE_SUFFIX = '2024'",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(2, rows.Count);
	}

	[Fact]
	public async Task WildcardTable_NoMatch_ReturnsEmpty()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT * FROM `{_datasetId}.events_*` WHERE _TABLE_SUFFIX = '2099'",
			parameters: null);
		var rows = results.ToList();
		Assert.Empty(rows);
	}

	// --- Partitioning ---

	// Ref: https://cloud.google.com/bigquery/docs/creating-partitioned-tables
	//   "A partitioned table is divided into segments called partitions."
	[Fact]
	public async Task PartitionedTable_CreateAndQuery()
	{
		var client = await _fixture.GetClientAsync();
		// Create a partitioned table via SQL DDL
		await client.ExecuteQueryAsync(
			$@"CREATE TABLE `{_datasetId}.partitioned`
			(id INT64, event_date DATE, name STRING)
			PARTITION BY event_date",
			parameters: null);

		// Insert data
		await client.ExecuteQueryAsync(
			$@"INSERT INTO `{_datasetId}.partitioned` (id, event_date, name)
			VALUES (1, DATE '2024-01-15', 'Jan Event'),
			       (2, DATE '2024-02-20', 'Feb Event')",
			parameters: null);

		var results = await client.ExecuteQueryAsync(
			$"SELECT * FROM `{_datasetId}.partitioned`",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(2, rows.Count);
	}
}
