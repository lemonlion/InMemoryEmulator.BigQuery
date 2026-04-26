using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase7;

/// <summary>
/// Unit tests for CTE (WITH clause) queries (Phase 7).
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_clause
/// </summary>
public class CteTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;

		var schema = new TableSchema
		{
			Fields = [
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "dept", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "salary", Type = "INTEGER", Mode = "NULLABLE" },
			]
		};
		var table = new InMemoryTable("test_ds", "employees", schema);
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 1L, ["name"] = "Alice", ["dept"] = "Eng", ["salary"] = 100000L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 2L, ["name"] = "Bob", ["dept"] = "Eng", ["salary"] = 90000L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 3L, ["name"] = "Carol", ["dept"] = "Sales", ["salary"] = 80000L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 4L, ["name"] = "Dave", ["dept"] = "Sales", ["salary"] = 70000L }));
		ds.Tables["employees"] = table;

		return new QueryExecutor(store, "test_ds");
	}

	[Fact]
	public void SimpleCte()
	{
		var exec = CreateExecutor();
		var (_, rows) = exec.Execute("WITH eng AS (SELECT name FROM employees WHERE dept = 'Eng') SELECT name FROM eng");
		Assert.Equal(2, rows.Count);
	}

	[Fact]
	public void CteWithAggregation()
	{
		var exec = CreateExecutor();
		var (_, rows) = exec.Execute(
			"WITH dept_totals AS (SELECT dept, SUM(salary) AS total FROM employees GROUP BY dept) " +
			"SELECT dept, total FROM dept_totals ORDER BY total DESC");
		Assert.Equal(2, rows.Count);
		Assert.Equal("190000", rows[0].F[1].V?.ToString());
	}

	[Fact]
	public void MultipleCtes()
	{
		var exec = CreateExecutor();
		var (_, rows) = exec.Execute(
			"WITH eng AS (SELECT name, salary FROM employees WHERE dept = 'Eng'), " +
			"sales AS (SELECT name, salary FROM employees WHERE dept = 'Sales') " +
			"SELECT COUNT(*) AS cnt FROM eng");
		Assert.Equal("2", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void CteReferencedMultipleTimes()
	{
		var exec = CreateExecutor();
		var (_, rows) = exec.Execute(
			"WITH high_salary AS (SELECT name FROM employees WHERE salary > 85000) " +
			"SELECT COUNT(*) AS cnt FROM high_salary");
		Assert.Equal("2", rows[0].F[0].V?.ToString());
	}
}
