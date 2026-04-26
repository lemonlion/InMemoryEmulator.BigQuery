using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase7;

/// <summary>
/// Unit tests for window functions: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, SUM/COUNT/AVG OVER (Phase 7).
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls
/// </summary>
public class WindowFunctionTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;

		var schema = new TableSchema
		{
			Fields = [
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "dept", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "salary", Type = "INTEGER", Mode = "NULLABLE" },
			]
		};
		var table = new InMemoryTable("test_ds", "employees", schema);
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["name"] = "Alice", ["dept"] = "Eng", ["salary"] = 100L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["name"] = "Bob", ["dept"] = "Eng", ["salary"] = 90L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["name"] = "Carol", ["dept"] = "Eng", ["salary"] = 90L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["name"] = "Dave", ["dept"] = "Sales", ["salary"] = 80L }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["name"] = "Eve", ["dept"] = "Sales", ["salary"] = 70L }));
		ds.Tables["employees"] = table;

		return new QueryExecutor(store, "test_ds");
	}

	[Fact]
	public void RowNumber_OverPartition()
	{
		var exec = CreateExecutor();
		var (_, rows) = exec.Execute(
			"SELECT name, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM employees ORDER BY dept, rn");
		Assert.Equal(5, rows.Count);
		// Eng partition: Alice=1, Bob=2, Carol=3
		Assert.Equal("1", rows[0].F[1].V?.ToString());
		Assert.Equal("2", rows[1].F[1].V?.ToString());
		Assert.Equal("3", rows[2].F[1].V?.ToString());
		// Sales partition: Dave=1, Eve=2
		Assert.Equal("1", rows[3].F[1].V?.ToString());
		Assert.Equal("2", rows[4].F[1].V?.ToString());
	}

	[Fact]
	public void Rank_WithTies()
	{
		var exec = CreateExecutor();
		var (_, rows) = exec.Execute(
			"SELECT name, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk FROM employees WHERE dept = 'Eng' ORDER BY salary DESC, name");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0].F[1].V?.ToString());  // Alice 100
		Assert.Equal("2", rows[1].F[1].V?.ToString());  // Bob 90
		Assert.Equal("2", rows[2].F[1].V?.ToString());  // Carol 90
	}

	[Fact]
	public void DenseRank_WithTies()
	{
		var exec = CreateExecutor();
		var (_, rows) = exec.Execute(
			"SELECT name, DENSE_RANK() OVER (ORDER BY salary DESC) AS dr FROM employees ORDER BY salary DESC, name");
		Assert.Equal(5, rows.Count);
		Assert.Equal("1", rows[0].F[1].V?.ToString());  // Alice 100
		Assert.Equal("2", rows[1].F[1].V?.ToString());  // Bob 90
		Assert.Equal("2", rows[2].F[1].V?.ToString());  // Carol 90
		Assert.Equal("3", rows[3].F[1].V?.ToString());  // Dave 80
		Assert.Equal("4", rows[4].F[1].V?.ToString());  // Eve 70
	}

	[Fact]
	public void SumOver_Partition()
	{
		var exec = CreateExecutor();
		var (_, rows) = exec.Execute(
			"SELECT name, SUM(salary) OVER (PARTITION BY dept) AS dept_total FROM employees WHERE dept = 'Sales' ORDER BY name");
		Assert.Equal(2, rows.Count);
		Assert.Equal("150", rows[0].F[1].V?.ToString());
		Assert.Equal("150", rows[1].F[1].V?.ToString());
	}

	[Fact]
	public void CountOver_NoPartition()
	{
		var exec = CreateExecutor();
		var (_, rows) = exec.Execute(
			"SELECT name, COUNT(*) OVER () AS total FROM employees ORDER BY name LIMIT 1");
		Assert.Equal("5", rows[0].F[1].V?.ToString());
	}
}
