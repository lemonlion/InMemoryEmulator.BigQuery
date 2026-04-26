using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase6;

/// <summary>
/// Unit tests for aggregate function additions (Phase 6).
/// </summary>
public class AggregateFunctionTests
{
	private static (InMemoryDataStore Store, InMemoryTable Table) CreateTestData()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;

		var schema = new Google.Apis.Bigquery.v2.Data.TableSchema
		{
			Fields =
			[
				new Google.Apis.Bigquery.v2.Data.TableFieldSchema { Name = "category", Type = "STRING", Mode = "NULLABLE" },
				new Google.Apis.Bigquery.v2.Data.TableFieldSchema { Name = "value", Type = "INTEGER", Mode = "NULLABLE" },
				new Google.Apis.Bigquery.v2.Data.TableFieldSchema { Name = "flag", Type = "BOOLEAN", Mode = "NULLABLE" },
			]
		};
		var table = new InMemoryTable("test_ds", "items", schema);
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["category"] = "A", ["value"] = 10L, ["flag"] = true }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["category"] = "B", ["value"] = 20L, ["flag"] = false }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["category"] = "A", ["value"] = 30L, ["flag"] = true }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["category"] = "B", ["value"] = 20L, ["flag"] = true }));
		ds.Tables["items"] = table;

		return (store, table);
	}

	[Fact]
	public void AnyValue_ReturnsOneValue()
	{
		var (store, _) = CreateTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (_, rows) = executor.Execute("SELECT ANY_VALUE(category) FROM items");

		Assert.Single(rows);
		Assert.NotNull(rows[0].F[0].V);
	}

	[Fact]
	public void StringAgg_ConcatenatesValues()
	{
		var (store, _) = CreateTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (_, rows) = executor.Execute("SELECT STRING_AGG(category) FROM items");

		Assert.Single(rows);
		var result = rows[0].F[0].V?.ToString();
		Assert.NotNull(result);
		Assert.Contains("A", result);
		Assert.Contains("B", result);
	}

	[Fact]
	public void ApproxCountDistinct()
	{
		var (store, _) = CreateTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (_, rows) = executor.Execute("SELECT APPROX_COUNT_DISTINCT(category) FROM items");

		Assert.Equal("2", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void CountDistinct_InGroupBy()
	{
		var (store, _) = CreateTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (_, rows) = executor.Execute(
			"SELECT category, COUNT(DISTINCT value) AS unique_vals FROM items GROUP BY category");

		Assert.Equal(2, rows.Count);
	}
}
