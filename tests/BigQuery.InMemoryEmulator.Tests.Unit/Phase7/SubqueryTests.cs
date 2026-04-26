using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase7;

/// <summary>
/// Unit tests for subqueries: scalar, EXISTS, IN (SELECT ...), subquery in FROM (Phase 7).
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries
/// </summary>
public class SubqueryTests
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
				new TableFieldSchema { Name = "dept_id", Type = "INTEGER", Mode = "NULLABLE" },
			]
		};
		var emp = new InMemoryTable("test_ds", "employees", schema);
		emp.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 1L, ["name"] = "Alice", ["dept_id"] = 10L }));
		emp.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 2L, ["name"] = "Bob", ["dept_id"] = 20L }));
		emp.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 3L, ["name"] = "Carol", ["dept_id"] = 10L }));
		ds.Tables["employees"] = emp;

		var deptSchema = new TableSchema
		{
			Fields = [
				new TableFieldSchema { Name = "dept_id", Type = "INTEGER", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "dept_name", Type = "STRING", Mode = "NULLABLE" },
			]
		};
		var dept = new InMemoryTable("test_ds", "departments", deptSchema);
		dept.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["dept_id"] = 10L, ["dept_name"] = "Engineering" }));
		dept.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["dept_id"] = 20L, ["dept_name"] = "Sales" }));
		ds.Tables["departments"] = dept;

		return new QueryExecutor(store, "test_ds");
	}

	[Fact]
	public void ScalarSubquery_InSelect()
	{
		var exec = CreateExecutor();
		var (_, rows) = exec.Execute("SELECT name, (SELECT COUNT(*) FROM employees) AS total FROM employees WHERE id = 1");
		Assert.Single(rows);
		Assert.Equal("Alice", rows[0].F[0].V?.ToString());
		Assert.Equal("3", rows[0].F[1].V?.ToString());
	}

	[Fact]
	public void ExistsSubquery()
	{
		var exec = CreateExecutor();
		// Non-correlated EXISTS
		var (_, rows) = exec.Execute(
			"SELECT name FROM employees WHERE EXISTS (SELECT 1 FROM departments WHERE dept_name = 'Engineering')");
		Assert.Equal(3, rows.Count);
	}

	[Fact]
	public void ExistsSubquery_NoMatch()
	{
		var exec = CreateExecutor();
		var (_, rows) = exec.Execute(
			"SELECT name FROM employees WHERE EXISTS (SELECT 1 FROM departments WHERE dept_name = 'HR')");
		Assert.Empty(rows);
	}

	[Fact]
	public void InSubquery()
	{
		var exec = CreateExecutor();
		var (_, rows) = exec.Execute(
			"SELECT name FROM employees WHERE dept_id IN (SELECT dept_id FROM departments WHERE dept_name = 'Sales')");
		Assert.Single(rows);
		Assert.Equal("Bob", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void SubqueryInFrom()
	{
		var exec = CreateExecutor();
		var (_, rows) = exec.Execute(
			"SELECT sub.name FROM (SELECT name FROM employees WHERE dept_id = 10) AS sub ORDER BY sub.name");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Alice", rows[0].F[0].V?.ToString());
		Assert.Equal("Carol", rows[1].F[0].V?.ToString());
	}
}