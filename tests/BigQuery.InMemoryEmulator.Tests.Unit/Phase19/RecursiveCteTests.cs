using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase19;

/// <summary>
/// Unit tests for recursive CTEs (Phase 19d).
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_clause
///   "WITH RECURSIVE allows CTEs to reference themselves."
/// </summary>
public class RecursiveCteTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;
		return new QueryExecutor(store, "test_ds");
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_clause
	//   "A recursive CTE references itself, and must use UNION ALL."

	[Fact]
	public void RecursiveCte_SimpleCount()
	{
		var exec = CreateExecutor();

		var (schema, rows) = exec.Execute(@"
			WITH RECURSIVE nums AS (
				SELECT 1 AS n
				UNION ALL
				SELECT n + 1 FROM nums WHERE n < 5
			)
			SELECT n FROM nums ORDER BY n");

		Assert.Equal(5, rows.Count);
		Assert.Equal("1", rows[0].F![0].V?.ToString());
		Assert.Equal("5", rows[4].F![0].V?.ToString());
	}

	[Fact]
	public void RecursiveCte_Fibonacci()
	{
		var exec = CreateExecutor();

		var (schema, rows) = exec.Execute(@"
			WITH RECURSIVE fib AS (
				SELECT 1 AS n, 1 AS val, 0 AS prev
				UNION ALL
				SELECT n + 1, val + prev, val FROM fib WHERE n < 8
			)
			SELECT n, val FROM fib ORDER BY n");

		Assert.Equal(8, rows.Count);
		// Fib sequence: 1, 1, 2, 3, 5, 8, 13, 21
		Assert.Equal("1", rows[0].F![1].V?.ToString());
		Assert.Equal("21", rows[7].F![1].V?.ToString());
	}

	[Fact]
	public void RecursiveCte_TerminatesWhenEmpty()
	{
		var exec = CreateExecutor();

		var (schema, rows) = exec.Execute(@"
			WITH RECURSIVE T AS (
				SELECT 1 AS n WHERE FALSE
				UNION ALL
				SELECT n + 1 FROM T WHERE n < 5
			)
			SELECT n FROM T");

		// Base case returns no rows, so recursion never starts
		Assert.Empty(rows);
	}
}
