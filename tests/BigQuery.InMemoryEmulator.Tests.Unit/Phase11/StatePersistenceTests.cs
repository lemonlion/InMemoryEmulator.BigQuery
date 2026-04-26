using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase11;

/// <summary>
/// Unit tests for state persistence: ExportState, ImportState (Phase 11).
/// </summary>
public class StatePersistenceTests
{
	private static InMemoryDataStore CreateStore()
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
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 1L, ["name"] = "Alice" }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 2L, ["name"] = "Bob" }));
		ds.Tables["items"] = table;

		return store;
	}

	[Fact]
	public void ExportState_ReturnsJson()
	{
		var store = CreateStore();
		var json = store.ExportState();
		Assert.Contains("test_ds", json);
		Assert.Contains("items", json);
		Assert.Contains("Alice", json);
	}

	[Fact]
	public void ImportState_RestoresData()
	{
		var store = CreateStore();
		var json = store.ExportState();

		var store2 = new InMemoryDataStore("test-project");
		store2.ImportState(json);

		Assert.True(store2.Datasets.ContainsKey("test_ds"));
		var table = store2.Datasets["test_ds"].Tables["items"];
		Assert.Equal(2, table.Rows.Count);
	}

	[Fact]
	public void ImportState_ReplacesExistingData()
	{
		var store = CreateStore();
		var json = store.ExportState();

		// Create a different store with different data
		var store2 = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("other_ds");
		store2.Datasets["other_ds"] = ds;

		store2.ImportState(json);

		Assert.False(store2.Datasets.ContainsKey("other_ds"));
		Assert.True(store2.Datasets.ContainsKey("test_ds"));
	}

	[Fact]
	public void ExportStateToFile_And_ImportStateFromFile_RoundTrip()
	{
		var store = CreateStore();
		var tempFile = Path.GetTempFileName();
		try
		{
			store.ExportStateToFile(tempFile);
			var store2 = new InMemoryDataStore("test-project");
			store2.ImportStateFromFile(tempFile);
			Assert.Equal(2, store2.Datasets["test_ds"].Tables["items"].Rows.Count);
		}
		finally
		{
			File.Delete(tempFile);
		}
	}
}
