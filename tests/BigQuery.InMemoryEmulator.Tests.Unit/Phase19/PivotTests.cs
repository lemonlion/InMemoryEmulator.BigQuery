using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase19;

/// <summary>
/// Unit tests for PIVOT operator (Phase 19c).
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#pivot_operator
///   "The PIVOT operator rotates rows into columns."
/// </summary>
public class PivotTests
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
				new TableFieldSchema { Name = "year", Type = "INT64" },
				new TableFieldSchema { Name = "quarter", Type = "STRING" },
				new TableFieldSchema { Name = "revenue", Type = "INT64" },
			}
		};
		var table = new InMemoryTable("test_ds", "sales", schema);
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["year"] = 2020L, ["quarter"] = "Q1", ["revenue"] = 100L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["year"] = 2020L, ["quarter"] = "Q2", ["revenue"] = 200L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["year"] = 2021L, ["quarter"] = "Q1", ["revenue"] = 150L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["year"] = 2021L, ["quarter"] = "Q2", ["revenue"] = 250L }));
		store.Datasets["test_ds"].Tables["sales"] = table;
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#pivot_operator
	//   "PIVOT(aggregate_function(column) FOR input_column IN (pivot_value [AS alias], ...))"

	[Fact]
	public void Pivot_BasicSumByQuarter()
	{
		var (exec, store) = CreateExecutor();
		SeedSalesTable(store);

		var (schema, rows) = exec.Execute(@"
			SELECT * FROM sales
			PIVOT(SUM(revenue) FOR quarter IN ('Q1', 'Q2'))
			ORDER BY year");

		Assert.Equal(2, rows.Count);
		// 2020: Q1=100, Q2=200
		Assert.Equal("2020", rows[0].F![0].V?.ToString());
		Assert.Equal("100", rows[0].F![1].V?.ToString());
		Assert.Equal("200", rows[0].F![2].V?.ToString());
		// 2021: Q1=150, Q2=250
		Assert.Equal("2021", rows[1].F![0].V?.ToString());
		Assert.Equal("150", rows[1].F![1].V?.ToString());
		Assert.Equal("250", rows[1].F![2].V?.ToString());

		// Schema should have: year, Q1, Q2
		Assert.Equal("year", schema.Fields[0].Name);
		Assert.Equal("Q1", schema.Fields[1].Name);
		Assert.Equal("Q2", schema.Fields[2].Name);
	}

	[Fact]
	public void Pivot_MissingPivotValue_ReturnsNull()
	{
		var (exec, store) = CreateExecutor();
		SeedSalesTable(store);

		var (schema, rows) = exec.Execute(@"
			SELECT * FROM sales
			PIVOT(SUM(revenue) FOR quarter IN ('Q1', 'Q3'))
			ORDER BY year");

		Assert.Equal(2, rows.Count);
		// Q3 doesn't exist, so it should be null
		Assert.Null(rows[0].F![2].V);
		Assert.Null(rows[1].F![2].V);
	}
}
