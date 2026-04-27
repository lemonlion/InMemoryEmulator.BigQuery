using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase25;

/// <summary>
/// Phase 25: UNPIVOT and TABLESAMPLE query clauses.
/// </summary>
public class UnpivotAndTablesampleTests
{
	private static (QueryExecutor Exec, InMemoryDataStore Store) CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;
		return (new QueryExecutor(store, "test_ds"), store);
	}

	private static void SeedScoreTable(InMemoryDataStore store)
	{
		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "student", Type = "STRING" },
				new TableFieldSchema { Name = "math", Type = "INTEGER" },
				new TableFieldSchema { Name = "english", Type = "INTEGER" },
				new TableFieldSchema { Name = "science", Type = "INTEGER" },
			]
		};
		var table = new InMemoryTable("test_ds", "scores", schema);
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["student"] = "Alice", ["math"] = 90L, ["english"] = 85L, ["science"] = 92L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["student"] = "Bob", ["math"] = 80L, ["english"] = 88L, ["science"] = 75L }));
		store.Datasets["test_ds"].Tables["scores"] = table;
	}

	#region UNPIVOT

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unpivot_operator
	//   "The UNPIVOT operator rotates columns into rows."
	[Fact]
	public void Unpivot_BasicColumns()
	{
		var (exec, store) = CreateExecutor();
		SeedScoreTable(store);

		var (schema, rows) = exec.Execute(@"
			SELECT * FROM scores
			UNPIVOT(score FOR subject IN (math, english, science))
			ORDER BY student, subject");

		// Alice has 3 rows, Bob has 3 rows = 6 total
		Assert.Equal(6, rows.Count);

		// Schema: student, score, subject
		Assert.Equal(3, schema.Fields.Count);
		Assert.Equal("student", schema.Fields[0].Name);
		Assert.Equal("score", schema.Fields[1].Name);
		Assert.Equal("subject", schema.Fields[2].Name);

		// First row should be Alice/english (alphabetical by subject)
		Assert.Equal("Alice", rows[0].F![0].V?.ToString());
	}

	[Fact]
	public void Unpivot_PreservesNonPivotColumns()
	{
		var (exec, store) = CreateExecutor();
		SeedScoreTable(store);

		var (_, rows) = exec.Execute(@"
			SELECT student, subject, score FROM scores
			UNPIVOT(score FOR subject IN (math, english))
			WHERE student = 'Alice'
			ORDER BY subject");

		Assert.Equal(2, rows.Count);
		Assert.Equal("Alice", rows[0].F![0].V?.ToString());
		Assert.Equal("english", rows[0].F![1].V?.ToString());
		Assert.Equal("85", rows[0].F![2].V?.ToString());
		Assert.Equal("math", rows[1].F![1].V?.ToString());
		Assert.Equal("90", rows[1].F![2].V?.ToString());
	}

	#endregion

	#region TABLESAMPLE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#tablesample_operator
	//   "TABLESAMPLE returns a random sample of rows from the table."
	[Fact]
	public void Tablesample_100Percent_ReturnsAll()
	{
		var (exec, store) = CreateExecutor();
		SeedScoreTable(store);

		var (_, rows) = exec.Execute(@"
			SELECT * FROM scores TABLESAMPLE SYSTEM (100 PERCENT)");

		Assert.Equal(2, rows.Count);
	}

	[Fact]
	public void Tablesample_0Percent_ReturnsNone()
	{
		var (exec, store) = CreateExecutor();
		SeedScoreTable(store);

		var (_, rows) = exec.Execute(@"
			SELECT * FROM scores TABLESAMPLE SYSTEM (0 PERCENT)");

		Assert.Empty(rows);
	}

	#endregion
}
