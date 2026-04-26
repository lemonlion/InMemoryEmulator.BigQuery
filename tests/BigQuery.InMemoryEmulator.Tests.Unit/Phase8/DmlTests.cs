using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase8;

/// <summary>
/// Unit tests for DML statements: INSERT, UPDATE, DELETE, MERGE (Phase 8).
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax
/// </summary>
public class DmlTests
{
	private static (QueryExecutor Executor, InMemoryTable Table) CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;

		var schema = new TableSchema
		{
			Fields = [
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "score", Type = "INTEGER", Mode = "NULLABLE" },
			]
		};
		var table = new InMemoryTable("test_ds", "items", schema);
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 1L, ["name"] = "Alpha", ["score"] = 10L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 2L, ["name"] = "Beta", ["score"] = 20L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 3L, ["name"] = "Gamma", ["score"] = 30L }));
		ds.Tables["items"] = table;

		return (new QueryExecutor(store, "test_ds"), table);
	}

	// --- INSERT INTO ... VALUES ---

	[Fact]
	public void InsertValues_AddsRows()
	{
		var (exec, table) = CreateExecutor();
		exec.Execute("INSERT INTO items (id, name, score) VALUES (4, 'Delta', 40)");
		Assert.Equal(4, table.Rows.Count);
		Assert.Equal("Delta", table.Rows[3].Fields["name"]?.ToString());
	}

	[Fact]
	public void InsertValues_MultipleRows()
	{
		var (exec, table) = CreateExecutor();
		exec.Execute("INSERT INTO items (id, name, score) VALUES (4, 'Delta', 40), (5, 'Epsilon', 50)");
		Assert.Equal(5, table.Rows.Count);
	}

	// --- INSERT INTO ... SELECT ---

	[Fact]
	public void InsertSelect_CopiesRows()
	{
		var (exec, table) = CreateExecutor();
		exec.Execute("INSERT INTO items (id, name, score) SELECT id, name, score FROM items WHERE score > 15");
		Assert.Equal(5, table.Rows.Count); // 3 original + 2 copied
	}

	// --- UPDATE ---

	[Fact]
	public void Update_ModifiesMatchingRows()
	{
		var (exec, table) = CreateExecutor();
		exec.Execute("UPDATE items SET score = 99 WHERE id = 2");
		var row = table.Rows.First(r => (long)r.Fields["id"]! == 2L);
		Assert.Equal(99L, Convert.ToInt64(row.Fields["score"]));
	}

	[Fact]
	public void Update_AffectsMultipleRows()
	{
		var (exec, table) = CreateExecutor();
		exec.Execute("UPDATE items SET score = 0 WHERE score < 25");
		Assert.Equal(2, table.Rows.Count(r => Convert.ToInt64(r.Fields["score"]) == 0));
	}

	[Fact]
	public void Update_NoMatch_ChangesNothing()
	{
		var (exec, table) = CreateExecutor();
		exec.Execute("UPDATE items SET score = 0 WHERE id = 999");
		Assert.True(table.Rows.All(r => Convert.ToInt64(r.Fields["score"]) != 0));
	}

	// --- DELETE ---

	[Fact]
	public void Delete_RemovesMatchingRows()
	{
		var (exec, table) = CreateExecutor();
		exec.Execute("DELETE FROM items WHERE id = 2");
		Assert.Equal(2, table.Rows.Count);
		Assert.DoesNotContain(table.Rows, r => (long)r.Fields["id"]! == 2L);
	}

	[Fact]
	public void Delete_NoMatch_RemovesNothing()
	{
		var (exec, table) = CreateExecutor();
		exec.Execute("DELETE FROM items WHERE id = 999");
		Assert.Equal(3, table.Rows.Count);
	}

	// --- MERGE ---

	[Fact]
	public void Merge_WhenMatched_Updates()
	{
		var (exec, _) = CreateExecutor();

		// Create source table
		var store = (InMemoryDataStore)typeof(QueryExecutor)
			.GetField("_store", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
			.GetValue(exec)!;
		var ds = store.Datasets["test_ds"];
		var srcSchema = new TableSchema
		{
			Fields = [
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "score", Type = "INTEGER", Mode = "NULLABLE" },
			]
		};
		var src = new InMemoryTable("test_ds", "updates", srcSchema);
		src.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 2L, ["name"] = "BetaNew", ["score"] = 200L }));
		ds.Tables["updates"] = src;

		exec.Execute(
			"MERGE INTO items t USING updates s ON t.id = s.id " +
			"WHEN MATCHED THEN UPDATE SET name = s.name, score = s.score");

		var target = ds.Tables["items"];
		var row = target.Rows.First(r => (long)r.Fields["id"]! == 2L);
		Assert.Equal("BetaNew", row.Fields["name"]?.ToString());
		Assert.Equal(200L, Convert.ToInt64(row.Fields["score"]));
	}

	[Fact]
	public void Merge_WhenNotMatched_Inserts()
	{
		var (exec, _) = CreateExecutor();

		var store = (InMemoryDataStore)typeof(QueryExecutor)
			.GetField("_store", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
			.GetValue(exec)!;
		var ds = store.Datasets["test_ds"];
		var srcSchema = new TableSchema
		{
			Fields = [
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "score", Type = "INTEGER", Mode = "NULLABLE" },
			]
		};
		var src = new InMemoryTable("test_ds", "new_items", srcSchema);
		src.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 99L, ["name"] = "New", ["score"] = 999L }));
		ds.Tables["new_items"] = src;

		exec.Execute(
			"MERGE INTO items t USING new_items s ON t.id = s.id " +
			"WHEN NOT MATCHED THEN INSERT (id, name, score) VALUES (s.id, s.name, s.score)");

		var target = ds.Tables["items"];
		Assert.Equal(4, target.Rows.Count);
		Assert.Contains(target.Rows, r => r.Fields["name"]?.ToString() == "New");
	}
}
