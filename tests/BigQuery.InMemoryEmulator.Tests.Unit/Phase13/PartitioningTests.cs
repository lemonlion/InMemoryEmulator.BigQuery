using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase13;

/// <summary>
/// Unit tests for time partitioning, clustering, and pseudo-columns (Phase 13).
/// Ref: https://cloud.google.com/bigquery/docs/partitioned-tables
/// </summary>
public class PartitioningTests
{
	private static (QueryExecutor Executor, InMemoryTable Table) CreatePartitionedTable()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;

		var schema = new TableSchema
		{
			Fields = [
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "created", Type = "TIMESTAMP", Mode = "NULLABLE" },
			]
		};
		var table = new InMemoryTable("test_ds", "events", schema)
		{
			// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TimePartitioning
			TimePartitioning = new TimePartitioning { Type = "DAY", Field = "created" },
		};
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?>
		{
			["id"] = 1L, ["name"] = "A", ["created"] = new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.Zero)
		}));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?>
		{
			["id"] = 2L, ["name"] = "B", ["created"] = new DateTimeOffset(2024, 2, 20, 14, 0, 0, TimeSpan.Zero)
		}));
		ds.Tables["events"] = table;

		return (new QueryExecutor(store, "test_ds"), table);
	}

	[Fact]
	public void PartitionedTable_PseudoColumns_Populated()
	{
		var (exec, _) = CreatePartitionedTable();
		var (schema, rows) = exec.Execute("SELECT id, _PARTITIONTIME, _PARTITIONDATE FROM events WHERE id = 1");

		Assert.Single(rows);
		// DAY partitioning truncates to day boundary
		var partTime = rows[0].F[1].V;
		Assert.NotNull(partTime);
		var partDate = rows[0].F[2].V?.ToString();
		Assert.Equal("2024-01-15", partDate);
	}

	[Fact]
	public void PartitionedTable_FilterByPartitionDate()
	{
		var (exec, _) = CreatePartitionedTable();
		var (_, rows) = exec.Execute("SELECT id FROM events WHERE _PARTITIONDATE = '2024-01-15'");

		Assert.Single(rows);
		Assert.Equal("1", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void RequirePartitionFilter_WithoutFilter_Throws()
	{
		var (exec, table) = CreatePartitionedTable();
		table.RequirePartitionFilter = true;

		Assert.Throws<InvalidOperationException>(() =>
			exec.Execute("SELECT id FROM events"));
	}

	[Fact]
	public void RequirePartitionFilter_WithFilter_Succeeds()
	{
		var (exec, table) = CreatePartitionedTable();
		table.RequirePartitionFilter = true;

		var (_, rows) = exec.Execute("SELECT id FROM events WHERE _PARTITIONDATE >= '2024-01-01'");
		Assert.Equal(2, rows.Count);
	}

	[Fact]
	public void MonthPartitioning_TruncatesToMonth()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;

		var schema = new TableSchema
		{
			Fields = [
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "ts", Type = "TIMESTAMP", Mode = "NULLABLE" },
			]
		};
		var table = new InMemoryTable("test_ds", "monthly", schema)
		{
			TimePartitioning = new TimePartitioning { Type = "MONTH", Field = "ts" },
		};
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?>
		{
			["id"] = 1L, ["ts"] = new DateTimeOffset(2024, 3, 15, 10, 0, 0, TimeSpan.Zero)
		}));
		ds.Tables["monthly"] = table;

		var exec = new QueryExecutor(store, "test_ds");
		var (_, rows) = exec.Execute("SELECT _PARTITIONDATE FROM monthly");

		// MONTH partitioning truncates to first of month
		Assert.Equal("2024-03-01", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Clustering_StoredOnTable()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;

		var schema = new TableSchema
		{
			Fields = [
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "category", Type = "STRING", Mode = "NULLABLE" },
			]
		};
		var table = new InMemoryTable("test_ds", "clustered", schema)
		{
			TimePartitioning = new TimePartitioning { Type = "DAY" },
			Clustering = new Clustering { Fields = ["category"] },
		};
		ds.Tables["clustered"] = table;

		// Ref: https://cloud.google.com/bigquery/docs/clustered-tables
		// Clustering is semantic only in the emulator - just verify it's stored
		Assert.NotNull(table.Clustering);
		Assert.Equal("category", table.Clustering.Fields[0]);
	}
}
