using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase19;

/// <summary>
/// Unit tests for GROUP BY ROLLUP (Phase 19b).
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#rollup
///   "GROUP BY ROLLUP generates groups including subtotals."
/// </summary>
public class RollupTests
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
		var schema = new TableSchema
		{
			Fields = new[]
			{
				new TableFieldSchema { Name = "region", Type = "STRING" },
				new TableFieldSchema { Name = "product", Type = "STRING" },
				new TableFieldSchema { Name = "amount", Type = "INT64" },
			}
		};
		var table = new InMemoryTable("test_ds", "sales", schema);
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["region"] = "East", ["product"] = "A", ["amount"] = 10L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["region"] = "East", ["product"] = "B", ["amount"] = 20L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["region"] = "West", ["product"] = "A", ["amount"] = 30L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["region"] = "West", ["product"] = "B", ["amount"] = 40L }));
		store.Datasets["test_ds"].Tables["sales"] = table;
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#rollup
	//   "ROLLUP(a, b) generates grouping sets: (a, b), (a), ()"

	[Fact]
	public void Rollup_TwoColumns_GeneratesSubtotalsAndGrandTotal()
	{
		var (exec, store) = CreateExecutor();
		SeedSalesTable(store);

		var (schema, rows) = exec.Execute(@"
			SELECT region, product, SUM(amount) AS total
			FROM sales
			GROUP BY ROLLUP(region, product)
			ORDER BY region, product");

		// Expected grouping sets:
		// (East, A) = 10, (East, B) = 20, (East, NULL) = 30,
		// (West, A) = 30, (West, B) = 40, (West, NULL) = 70,
		// (NULL, NULL) = 100 (grand total)
		Assert.Equal(7, rows.Count);

		// Grand total row should exist with NULL region and NULL product
		var grandTotal = rows.FirstOrDefault(r =>
			r.F![0].V is null && r.F![1].V is null);
		Assert.NotNull(grandTotal);
		Assert.Equal("100", grandTotal!.F![2].V?.ToString());
	}

	[Fact]
	public void Rollup_SingleColumn_GeneratesGrandTotal()
	{
		var (exec, store) = CreateExecutor();
		SeedSalesTable(store);

		var (schema, rows) = exec.Execute(@"
			SELECT region, SUM(amount) AS total
			FROM sales
			GROUP BY ROLLUP(region)
			ORDER BY region");

		// (East) = 30, (West) = 70, (NULL) = 100
		Assert.Equal(3, rows.Count);

		var grandTotal = rows.FirstOrDefault(r => r.F![0].V is null);
		Assert.NotNull(grandTotal);
		Assert.Equal("100", grandTotal!.F![1].V?.ToString());
	}
}
