using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for CTEs, subqueries, window functions, and set operations (Phase 7).
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class AdvancedQueryTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public AdvancedQueryTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_adv_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();

		// employees table for window functions and CTEs
		var empSchema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "department", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "salary", Type = "INTEGER", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "employees", empSchema);

		var empRows = new[]
		{
			new BigQueryInsertRow("e1") { ["id"] = 1, ["name"] = "Alice", ["department"] = "Eng", ["salary"] = 100000 },
			new BigQueryInsertRow("e2") { ["id"] = 2, ["name"] = "Bob", ["department"] = "Eng", ["salary"] = 90000 },
			new BigQueryInsertRow("e3") { ["id"] = 3, ["name"] = "Charlie", ["department"] = "Sales", ["salary"] = 80000 },
			new BigQueryInsertRow("e4") { ["id"] = 4, ["name"] = "Dave", ["department"] = "Sales", ["salary"] = 70000 },
			new BigQueryInsertRow("e5") { ["id"] = 5, ["name"] = "Eve", ["department"] = "HR", ["salary"] = 75000 },
		};
		await client.InsertRowsAsync(_datasetId, "employees", empRows);

		// products table for set operations
		var prodSchema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "category", Type = "STRING", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "products_a", prodSchema);
		await client.CreateTableAsync(_datasetId, "products_b", prodSchema);

		await client.InsertRowsAsync(_datasetId, "products_a", new[]
		{
			new BigQueryInsertRow("a1") { ["name"] = "Widget", ["category"] = "A" },
			new BigQueryInsertRow("a2") { ["name"] = "Gadget", ["category"] = "B" },
			new BigQueryInsertRow("a3") { ["name"] = "Doohickey", ["category"] = "A" },
		});
		await client.InsertRowsAsync(_datasetId, "products_b", new[]
		{
			new BigQueryInsertRow("b1") { ["name"] = "Widget", ["category"] = "A" },
			new BigQueryInsertRow("b2") { ["name"] = "Thingamajig", ["category"] = "C" },
		});

		// items table with arrays for UNNEST
		var arraySchema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "tags", Type = "STRING", Mode = "REPEATED" },
			]
		};
		await client.CreateTableAsync(_datasetId, "items", arraySchema);
		await client.InsertRowsAsync(_datasetId, "items", new[]
		{
			new BigQueryInsertRow("i1") { ["id"] = 1, ["tags"] = new[] { "red", "blue" } },
			new BigQueryInsertRow("i2") { ["id"] = 2, ["tags"] = new[] { "green" } },
		});
	}

	public async ValueTask DisposeAsync()
	{
		try
		{
			var client = await _fixture.GetClientAsync();
			await client.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true });
		}
		catch { }
		await _fixture.DisposeAsync();
	}

	// --- CTEs ---

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_clause
	//   "A WITH clause contains one or more common table expressions (CTEs)."
	[Fact]
	public async Task Cte_SimpleCte()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"WITH eng AS (SELECT name FROM `{_datasetId}.employees` WHERE department = 'Eng') SELECT * FROM eng",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(2, rows.Count);
	}

	[Fact]
	public async Task Cte_WithAggregation()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$@"WITH dept_totals AS (
				SELECT department, SUM(salary) AS total
				FROM `{_datasetId}.employees`
				GROUP BY department
			)
			SELECT department, total FROM dept_totals",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(3, rows.Count);
	}

	[Fact]
	public async Task Cte_MultipleCtes()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$@"WITH
				eng AS (SELECT name, salary FROM `{_datasetId}.employees` WHERE department = 'Eng'),
				high_salary AS (SELECT name FROM eng WHERE salary > 95000)
			SELECT * FROM high_salary",
			parameters: null);
		var rows = results.ToList();
		Assert.Single(rows);
		Assert.Equal("Alice", (string)rows[0]["name"]);
	}

	// --- Subqueries ---

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries
	//   "A subquery is a query that appears inside another query statement."
	[Fact]
	public async Task Subquery_ScalarInSelect()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$@"SELECT name, salary,
				(SELECT AVG(salary) FROM `{_datasetId}.employees`) AS avg_salary
			FROM `{_datasetId}.employees`
			WHERE name = 'Alice'",
			parameters: null);
		var rows = results.ToList();
		Assert.Single(rows);
	}

	[Fact]
	public async Task Subquery_InClause()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$@"SELECT name FROM `{_datasetId}.employees`
			WHERE department IN (
				SELECT department FROM `{_datasetId}.employees`
				GROUP BY department HAVING COUNT(*) > 1
			)",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(4, rows.Count); // Eng(2) + Sales(2)
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries#exists_subquery_concepts
	//   "EXISTS returns TRUE if the subquery produces one or more rows."
	[Fact]
	public async Task Subquery_Exists()
	{
		var client = await _fixture.GetClientAsync();
		// Non-correlated EXISTS — check that ANY employees exist
		var results = await client.ExecuteQueryAsync(
			$@"SELECT 'found' AS status WHERE EXISTS (
				SELECT 1 FROM `{_datasetId}.employees`
			)",
			parameters: null);
		var rows = results.ToList();
		Assert.Single(rows);
		Assert.Equal("found", (string)rows[0]["status"]);
	}

	[Fact]
	public async Task Subquery_InFrom()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$@"SELECT department, cnt
			FROM (
				SELECT department, COUNT(*) AS cnt FROM `{_datasetId}.employees` GROUP BY department
			) sub
			WHERE cnt > 1",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(2, rows.Count);
	}

	// --- Window Functions ---

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls
	//   "A window function computes values over a group of rows."
	[Fact]
	public async Task WindowFunction_RowNumber()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$@"SELECT name, department,
				ROW_NUMBER() OVER(PARTITION BY department ORDER BY salary DESC) AS rn
			FROM `{_datasetId}.employees`
			ORDER BY department, rn",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(5, rows.Count);
	}

	[Fact]
	public async Task WindowFunction_SumOver()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$@"SELECT name, department, salary,
				SUM(salary) OVER(PARTITION BY department) AS dept_total
			FROM `{_datasetId}.employees`
			ORDER BY department, name",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(5, rows.Count);
	}

	[Fact]
	public async Task WindowFunction_Rank()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$@"SELECT name, salary,
				RANK() OVER(ORDER BY salary DESC) AS rnk
			FROM `{_datasetId}.employees`",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(5, rows.Count);
	}

	// --- Set Operations ---

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators
	//   "UNION, INTERSECT, and EXCEPT combine the result sets of two or more queries."
	[Fact]
	public async Task UnionAll_IncludesDuplicates()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$@"SELECT name FROM `{_datasetId}.products_a`
			UNION ALL
			SELECT name FROM `{_datasetId}.products_b`",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(5, rows.Count); // 3 + 2
	}

	[Fact]
	public async Task UnionDistinct_RemovesDuplicates()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$@"SELECT name FROM `{_datasetId}.products_a`
			UNION DISTINCT
			SELECT name FROM `{_datasetId}.products_b`",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(4, rows.Count); // Widget appears once
	}

	[Fact]
	public async Task ExceptDistinct_ReturnsOnlyInLeft()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$@"SELECT name FROM `{_datasetId}.products_a`
			EXCEPT DISTINCT
			SELECT name FROM `{_datasetId}.products_b`",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(2, rows.Count); // Gadget, Doohickey
	}

	[Fact]
	public async Task IntersectDistinct_ReturnsCommon()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$@"SELECT name FROM `{_datasetId}.products_a`
			INTERSECT DISTINCT
			SELECT name FROM `{_datasetId}.products_b`",
			parameters: null);
		var rows = results.ToList();
		Assert.Single(rows); // Widget
		Assert.Equal("Widget", (string)rows[0]["name"]);
	}

	// --- UNNEST ---

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unnest
	//   "The UNNEST operator takes an array and returns a table."
	[Fact]
	public async Task Unnest_InlineArray()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT val FROM UNNEST([1, 2, 3]) AS val",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(3, rows.Count);
	}

	// --- DISTINCT ---

	[Fact]
	public async Task Distinct_RemovesDuplicates()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT DISTINCT department FROM `{_datasetId}.employees`",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(3, rows.Count);
	}
}
