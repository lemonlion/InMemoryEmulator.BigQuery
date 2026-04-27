using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for QUALIFY, PIVOT, ROLLUP, recursive CTEs (Phase 19).
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class AdvancedSqlTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public AdvancedSqlTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_asql_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();

		// Sales data for PIVOT/ROLLUP
		var salesSchema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "region", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "product", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "quarter", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "amount", Type = "INTEGER", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "sales", salesSchema);
		await client.InsertRowsAsync(_datasetId, "sales", new[]
		{
			new BigQueryInsertRow("s1") { ["region"] = "East", ["product"] = "A", ["quarter"] = "Q1", ["amount"] = 100 },
			new BigQueryInsertRow("s2") { ["region"] = "East", ["product"] = "A", ["quarter"] = "Q2", ["amount"] = 150 },
			new BigQueryInsertRow("s3") { ["region"] = "East", ["product"] = "B", ["quarter"] = "Q1", ["amount"] = 200 },
			new BigQueryInsertRow("s4") { ["region"] = "West", ["product"] = "A", ["quarter"] = "Q1", ["amount"] = 120 },
			new BigQueryInsertRow("s5") { ["region"] = "West", ["product"] = "B", ["quarter"] = "Q2", ["amount"] = 180 },
		});

		// Employees for QUALIFY
		var empSchema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "department", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "salary", Type = "INTEGER", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "employees", empSchema);
		await client.InsertRowsAsync(_datasetId, "employees", new[]
		{
			new BigQueryInsertRow("e1") { ["name"] = "Alice", ["department"] = "Eng", ["salary"] = 100000 },
			new BigQueryInsertRow("e2") { ["name"] = "Bob", ["department"] = "Eng", ["salary"] = 90000 },
			new BigQueryInsertRow("e3") { ["name"] = "Charlie", ["department"] = "Sales", ["salary"] = 80000 },
			new BigQueryInsertRow("e4") { ["name"] = "Dave", ["department"] = "Sales", ["salary"] = 70000 },
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

	// --- QUALIFY ---

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#qualify_clause
	//   "The QUALIFY clause filters the results of window functions."
	[Fact]
	public async Task Qualify_TopPerGroup()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$@"SELECT name, department, salary
			FROM `{_datasetId}.employees`
			QUALIFY ROW_NUMBER() OVER(PARTITION BY department ORDER BY salary DESC) = 1",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(2, rows.Count); // Top earner per department
	}

	// --- PIVOT ---

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#pivot_operator
	//   "The PIVOT operator rotates rows into columns."
	[Fact]
	public async Task Pivot_BasicSum()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$@"SELECT * FROM (
				SELECT region, quarter, amount FROM `{_datasetId}.sales`
			) PIVOT(SUM(amount) FOR quarter IN ('Q1', 'Q2'))",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(2, rows.Count); // East, West
	}

	// --- ROLLUP ---

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#group_by_rollup
	//   "GROUP BY ROLLUP generates subtotals and a grand total."
	[Fact]
	public async Task Rollup_GeneratesSubtotals()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$@"SELECT region, product, SUM(amount) AS total
			FROM `{_datasetId}.sales`
			GROUP BY ROLLUP(region, product)
			ORDER BY region, product",
			parameters: null);
		var rows = results.ToList();
		// Should include: per-region-product, per-region subtotals, grand total
		Assert.True(rows.Count >= 5); // At least region*product + region subtotals + grand total
	}

	// --- Recursive CTE ---

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_recursive
	//   "WITH RECURSIVE enables recursive queries."
	[Fact]
	public async Task RecursiveCte_SimpleCount()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			@"WITH RECURSIVE cnt AS (
				SELECT 1 AS n
				UNION ALL
				SELECT n + 1 FROM cnt WHERE n < 5
			)
			SELECT * FROM cnt",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(5, rows.Count);
	}

	[Fact]
	public async Task RecursiveCte_Fibonacci()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			@"WITH RECURSIVE fib AS (
				SELECT 1 AS n, 1 AS val, 0 AS prev
				UNION ALL
				SELECT n + 1, val + prev, val FROM fib WHERE n < 8
			)
			SELECT n, val FROM fib ORDER BY n",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(8, rows.Count);
	}
}
