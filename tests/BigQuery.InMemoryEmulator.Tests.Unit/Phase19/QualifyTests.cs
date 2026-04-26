using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase19;

/// <summary>
/// Unit tests for the QUALIFY clause (Phase 19a).
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#qualify_clause
///   "The QUALIFY clause filters the results of window functions."
/// </summary>
public class QualifyTests
{
	private static (QueryExecutor exec, InMemoryDataStore store) CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;
		return (new QueryExecutor(store, "test_ds"), store);
	}

	private static void SeedSalesTable(InMemoryDataStore store)
	{
		var table = new InMemoryTable("test_ds", "sales", new TableSchema
		{
			Fields = new[]
			{
				new TableFieldSchema { Name = "category", Type = "STRING" },
				new TableFieldSchema { Name = "item", Type = "STRING" },
				new TableFieldSchema { Name = "amount", Type = "INT64" },
			}
		});
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["category"] = "A", ["item"] = "x", ["amount"] = 10L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["category"] = "A", ["item"] = "y", ["amount"] = 20L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["category"] = "A", ["item"] = "z", ["amount"] = 30L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["category"] = "B", ["item"] = "p", ["amount"] = 100L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["category"] = "B", ["item"] = "q", ["amount"] = 50L }));
		store.Datasets["test_ds"].Tables["sales"] = table;
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#qualify_clause
	//   "The QUALIFY clause filters the results of window functions."

	[Fact]
	public void Qualify_WithRowNumber_FiltersTopPerGroup()
	{
		var (exec, store) = CreateExecutor();
		SeedSalesTable(store);

		var (schema, rows) = exec.Execute(@"
			SELECT category, item, amount,
				ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) AS rn
			FROM sales
			QUALIFY rn <= 2");

		// Category A: z(30), y(20) — top 2 by amount DESC
		// Category B: p(100), q(50) — top 2 by amount DESC
		Assert.Equal(4, rows.Count);
	}

	[Fact]
	public void Qualify_WithInlineWindowFunction()
	{
		var (exec, store) = CreateExecutor();
		SeedSalesTable(store);

		var (schema, rows) = exec.Execute(@"
			SELECT category, item, amount
			FROM sales
			QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) = 1");

		// Top 1 per category: z(30) for A, p(100) for B
		Assert.Equal(2, rows.Count);
	}

	[Fact]
	public void Qualify_FilterAllRows_ReturnsEmpty()
	{
		var (exec, store) = CreateExecutor();
		SeedSalesTable(store);

		var (_, rows) = exec.Execute(@"
			SELECT category, item, amount,
				ROW_NUMBER() OVER (ORDER BY amount) AS rn
			FROM sales
			QUALIFY rn > 100");

		Assert.Empty(rows);
	}
}
