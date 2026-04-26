using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase14;

/// <summary>
/// Unit tests for wildcard tables, INFORMATION_SCHEMA, and related features (Phase 14).
/// Ref: https://cloud.google.com/bigquery/docs/information-schema-intro
/// Ref: https://cloud.google.com/bigquery/docs/querying-wildcard-tables
/// </summary>
public class MetadataTests
{
	private static (QueryExecutor Executor, InMemoryDataStore Store) CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;

		var schema1 = new TableSchema
		{
			Fields = [
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
			]
		};

		var t1 = new InMemoryTable("test_ds", "events_2023", schema1);
		t1.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 1L, ["name"] = "A" }));
		ds.Tables["events_2023"] = t1;

		var t2 = new InMemoryTable("test_ds", "events_2024", schema1);
		t2.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 2L, ["name"] = "B" }));
		t2.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 3L, ["name"] = "C" }));
		ds.Tables["events_2024"] = t2;

		var schema2 = new TableSchema
		{
			Fields = [
				new TableFieldSchema { Name = "key", Type = "STRING", Mode = "REQUIRED" },
			]
		};
		var t3 = new InMemoryTable("test_ds", "other", schema2);
		ds.Tables["other"] = t3;

		return (new QueryExecutor(store, "test_ds"), store);
	}

	// --- INFORMATION_SCHEMA.TABLES ---

	[Fact]
	public void InformationSchema_Tables_ListsAllTables()
	{
		var (exec, _) = CreateExecutor();
		var (_, rows) = exec.Execute("SELECT table_name FROM INFORMATION_SCHEMA.TABLES ORDER BY table_name");

		Assert.Equal(3, rows.Count);
		Assert.Equal("events_2023", rows[0].F[0].V?.ToString());
		Assert.Equal("events_2024", rows[1].F[0].V?.ToString());
		Assert.Equal("other", rows[2].F[0].V?.ToString());
	}

	[Fact]
	public void InformationSchema_Tables_HasExpectedColumns()
	{
		var (exec, _) = CreateExecutor();
		var (schema, rows) = exec.Execute("SELECT table_catalog, table_schema, table_name, table_type FROM INFORMATION_SCHEMA.TABLES WHERE table_name = 'other'");

		Assert.Single(rows);
		Assert.Equal("test-project", rows[0].F[0].V?.ToString());
		Assert.Equal("test_ds", rows[0].F[1].V?.ToString());
		Assert.Equal("other", rows[0].F[2].V?.ToString());
		Assert.Equal("BASE TABLE", rows[0].F[3].V?.ToString());
	}

	// --- INFORMATION_SCHEMA.COLUMNS ---

	[Fact]
	public void InformationSchema_Columns_ListsColumns()
	{
		var (exec, _) = CreateExecutor();
		var (_, rows) = exec.Execute("SELECT column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'events_2023' ORDER BY ordinal_position");

		Assert.Equal(2, rows.Count);
		Assert.Equal("id", rows[0].F[0].V?.ToString());
		Assert.Equal("INTEGER", rows[0].F[1].V?.ToString());
		Assert.Equal("name", rows[1].F[0].V?.ToString());
		Assert.Equal("STRING", rows[1].F[1].V?.ToString());
	}

	// --- INFORMATION_SCHEMA.SCHEMATA ---

	[Fact]
	public void InformationSchema_Schemata_ListsDatasets()
	{
		var (exec, _) = CreateExecutor();
		var (_, rows) = exec.Execute("SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA");

		Assert.Single(rows);
		Assert.Equal("test_ds", rows[0].F[0].V?.ToString());
	}

	// --- Wildcard Tables ---

	[Fact]
	public void WildcardTable_MatchesMultipleTables()
	{
		var (exec, _) = CreateExecutor();
		var (_, rows) = exec.Execute("SELECT id, _TABLE_SUFFIX FROM `events_*` ORDER BY id");

		Assert.Equal(3, rows.Count);
	}

	[Fact]
	public void WildcardTable_FilterBySuffix()
	{
		var (exec, _) = CreateExecutor();
		var (_, rows) = exec.Execute("SELECT id FROM `events_*` WHERE _TABLE_SUFFIX = '2024'");

		Assert.Equal(2, rows.Count);
	}

	[Fact]
	public void WildcardTable_NoMatch_ReturnsEmpty()
	{
		var (exec, _) = CreateExecutor();
		var (_, rows) = exec.Execute("SELECT id FROM `nonexistent_*`");

		Assert.Empty(rows);
	}

	[Fact]
	public void WildcardTable_SuffixValueCorrect()
	{
		var (exec, _) = CreateExecutor();
		var (_, rows) = exec.Execute("SELECT _TABLE_SUFFIX FROM `events_*` WHERE id = 1");

		Assert.Single(rows);
		Assert.Equal("2023", rows[0].F[0].V?.ToString());
	}
}
