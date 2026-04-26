using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase5;

/// <summary>
/// Unit tests for GROUP BY and HAVING (Phase 5).
/// </summary>
public class GroupByTests
{
	private static (InMemoryDataStore Store, InMemoryTable Table) CreateTestData()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "department", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "salary", Type = "INTEGER", Mode = "NULLABLE" },
			]
		};
		var table = new InMemoryTable("test_ds", "employees", schema);
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["department"] = "Engineering", ["name"] = "Alice", ["salary"] = 100000L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["department"] = "Engineering", ["name"] = "Bob", ["salary"] = 90000L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["department"] = "Sales", ["name"] = "Charlie", ["salary"] = 80000L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["department"] = "Sales", ["name"] = "Dave", ["salary"] = 70000L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["department"] = "HR", ["name"] = "Eve", ["salary"] = 75000L }));
		ds.Tables["employees"] = table;

		return (store, table);
	}

	[Fact]
	public void GroupBy_CountByDepartment()
	{
		var (store, _) = CreateTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (schema, rows) = executor.Execute("SELECT department, COUNT(*) AS cnt FROM employees GROUP BY department");

		Assert.Equal(3, rows.Count);
	}

	[Fact]
	public void GroupBy_SumByDepartment()
	{
		var (store, _) = CreateTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (schema, rows) = executor.Execute("SELECT department, SUM(salary) AS total FROM employees GROUP BY department");

		Assert.Equal(3, rows.Count);
		// Find Engineering row
		var engIdx = Enumerable.Range(0, rows.Count).First(i => rows[i].F[0].V?.ToString() == "Engineering");
		Assert.Equal("190000", rows[engIdx].F[1].V?.ToString());
	}

	[Fact]
	public void GroupBy_AvgByDepartment()
	{
		var (store, _) = CreateTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (schema, rows) = executor.Execute("SELECT department, AVG(salary) AS avg_sal FROM employees GROUP BY department");

		Assert.Equal(3, rows.Count);
		var engIdx = Enumerable.Range(0, rows.Count).First(i => rows[i].F[0].V?.ToString() == "Engineering");
		Assert.Equal("95000", rows[engIdx].F[1].V?.ToString());
	}

	[Fact]
	public void GroupBy_Having_FiltersGroups()
	{
		var (store, _) = CreateTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (schema, rows) = executor.Execute(
			"SELECT department, COUNT(*) AS cnt FROM employees GROUP BY department HAVING COUNT(*) > 1");

		Assert.Equal(2, rows.Count); // Engineering (2), Sales (2) — HR (1) filtered out
	}

	[Fact]
	public void GroupBy_MinMax()
	{
		var (store, _) = CreateTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (schema, rows) = executor.Execute(
			"SELECT department, MIN(salary) AS min_sal, MAX(salary) AS max_sal FROM employees GROUP BY department");

		Assert.Equal(3, rows.Count);
		var engIdx = Enumerable.Range(0, rows.Count).First(i => rows[i].F[0].V?.ToString() == "Engineering");
		Assert.Equal("90000", rows[engIdx].F[1].V?.ToString());
		Assert.Equal("100000", rows[engIdx].F[2].V?.ToString());
	}

	[Fact]
	public void GroupBy_CountDistinct()
	{
		var (store, _) = CreateTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (schema, rows) = executor.Execute(
			"SELECT department, COUNT(DISTINCT name) AS unique_names FROM employees GROUP BY department");

		Assert.Equal(3, rows.Count);
		var engIdx = Enumerable.Range(0, rows.Count).First(i => rows[i].F[0].V?.ToString() == "Engineering");
		Assert.Equal("2", rows[engIdx].F[1].V?.ToString());
	}

	[Fact]
	public void GroupBy_WithOrderBy()
	{
		var (store, _) = CreateTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (schema, rows) = executor.Execute(
			"SELECT department, COUNT(*) AS cnt FROM employees GROUP BY department ORDER BY cnt DESC");

		Assert.Equal(3, rows.Count);
		// Engineering and Sales both have 2, HR has 1
		Assert.Equal("1", rows[2].F[1].V?.ToString());
	}

	[Fact]
	public void GroupBy_WithLimit()
	{
		var (store, _) = CreateTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (schema, rows) = executor.Execute(
			"SELECT department, COUNT(*) AS cnt FROM employees GROUP BY department LIMIT 2");

		Assert.Equal(2, rows.Count);
	}

	[Fact]
	public void GroupBy_NullGroupKey()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "category", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "value", Type = "INTEGER", Mode = "NULLABLE" },
			]
		};
		var table = new InMemoryTable("test_ds", "items", schema);
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["category"] = "A", ["value"] = 1L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["category"] = null, ["value"] = 2L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["category"] = null, ["value"] = 3L }));
		ds.Tables["items"] = table;

		var executor = new QueryExecutor(store, "test_ds");
		var (_, rows) = executor.Execute("SELECT category, SUM(value) AS total FROM items GROUP BY category");

		// Should have 2 groups: "A" and NULL
		Assert.Equal(2, rows.Count);
	}
}
