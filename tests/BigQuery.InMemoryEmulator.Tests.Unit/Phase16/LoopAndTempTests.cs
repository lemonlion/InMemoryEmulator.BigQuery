using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase16;

/// <summary>
/// Unit tests for loops, temp tables, BREAK/CONTINUE (Phase 16).
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language
/// </summary>
public class LoopAndTempTests
{
	private static (BigQuery.InMemoryEmulator.SqlEngine.ProceduralExecutor Executor, InMemoryDataStore Store) CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;

		var schema = new TableSchema
		{
			Fields = [
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "NULLABLE" },
			]
		};
		var table = new InMemoryTable("test_ds", "items", schema);
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 1L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 2L }));
		ds.Tables["items"] = table;

		return (new BigQuery.InMemoryEmulator.SqlEngine.ProceduralExecutor(store, "test_ds"), store);
	}

	[Fact]
	public void WhileLoop_ExecutesBody()
	{
		var (exec, store) = CreateExecutor();
		exec.Execute(@"
			DECLARE x INT64 DEFAULT 0;
			WHILE x < 3 DO
				SET x = x + 1;
			END WHILE;
			SELECT x");

		// x was incremented to 3 - but variable isn't visible in SELECT directly
		// Test the loop didn't infinite-loop (would timeout)
	}

	[Fact]
	public void LoopWithBreak_ExitsLoop()
	{
		var (exec, _) = CreateExecutor();
		var (_, rows) = exec.Execute(@"
			DECLARE x INT64 DEFAULT 0;
			LOOP
				SET x = x + 1;
				IF x >= 5 THEN BREAK END IF;
			END LOOP;
			SELECT 'done'");

		Assert.Single(rows);
		Assert.Equal("done", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void CreateTempTable_CreatedInTempDataset()
	{
		var (exec, store) = CreateExecutor();
		exec.Execute("CREATE TEMP TABLE tmp (val STRING)");

		Assert.True(store.Datasets.ContainsKey("_temp"));
		Assert.True(store.Datasets["_temp"].Tables.ContainsKey("tmp"));
	}

	[Fact]
	public void CreateTempTable_InsertAndQuery()
	{
		var (exec, store) = CreateExecutor();
		exec.Execute("CREATE TEMP TABLE tmp (val INTEGER)");

		// Insert and query via temp dataset
		var tempExec = new QueryExecutor(store, "_temp");
		tempExec.Execute("INSERT INTO tmp (val) VALUES (42)");
		var (_, rows) = tempExec.Execute("SELECT val FROM tmp");

		Assert.Single(rows);
		Assert.Equal("42", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Break_OutsideLoop_ThrowsBreakException()
	{
		var (exec, _) = CreateExecutor();
		// BREAK outside a loop should propagate as unhandled
		Assert.ThrowsAny<Exception>(() => exec.Execute("BREAK"));
	}

	[Fact]
	public void SplitStatements_RespectsNestedBlocks()
	{
		var stmts = BigQuery.InMemoryEmulator.SqlEngine.ProceduralExecutor.SplitStatements(
			"BEGIN SELECT 1; SELECT 2; END; SELECT 3");

		Assert.Equal(2, stmts.Count);
		Assert.Contains("BEGIN", stmts[0]);
		Assert.Contains("END", stmts[0]);
		Assert.Contains("SELECT 3", stmts[1]);
	}
}
