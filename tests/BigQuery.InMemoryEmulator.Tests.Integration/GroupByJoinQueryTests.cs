using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for GROUP BY, HAVING, and JOIN queries (Phase 5).
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class GroupByJoinQueryTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public GroupByJoinQueryTests(BigQuerySession session)
	{
		_session = session;
	}

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_p5_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();

		// employees table
		var empSchema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "department", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "salary", Type = "INTEGER", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "employees", empSchema);

		var empRows = new[]
		{
			new BigQueryInsertRow("e1") { ["department"] = "Engineering", ["name"] = "Alice", ["salary"] = 100000 },
			new BigQueryInsertRow("e2") { ["department"] = "Engineering", ["name"] = "Bob", ["salary"] = 90000 },
			new BigQueryInsertRow("e3") { ["department"] = "Sales", ["name"] = "Charlie", ["salary"] = 80000 },
			new BigQueryInsertRow("e4") { ["department"] = "Sales", ["name"] = "Dave", ["salary"] = 70000 },
			new BigQueryInsertRow("e5") { ["department"] = "HR", ["name"] = "Eve", ["salary"] = 75000 },
		};
		await client.InsertRowsAsync(_datasetId, "employees", empRows);

		// users table (for JOINs)
		var usersSchema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "users", usersSchema);

		var userRows = new[]
		{
			new BigQueryInsertRow("u1") { ["id"] = 1, ["name"] = "Alice" },
			new BigQueryInsertRow("u2") { ["id"] = 2, ["name"] = "Bob" },
			new BigQueryInsertRow("u3") { ["id"] = 3, ["name"] = "Charlie" },
		};
		await client.InsertRowsAsync(_datasetId, "users", userRows);

		// orders table (for JOINs)
		var ordersSchema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "order_id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "user_id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "amount", Type = "FLOAT", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "orders", ordersSchema);

		var orderRows = new[]
		{
			new BigQueryInsertRow("o1") { ["order_id"] = 101, ["user_id"] = 1, ["amount"] = 50.0 },
			new BigQueryInsertRow("o2") { ["order_id"] = 102, ["user_id"] = 1, ["amount"] = 30.0 },
			new BigQueryInsertRow("o3") { ["order_id"] = 103, ["user_id"] = 2, ["amount"] = 75.0 },
		};
		await client.InsertRowsAsync(_datasetId, "orders", orderRows);
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

	[Fact]
	public async Task GroupBy_CountByDepartment()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT department, COUNT(*) AS cnt FROM `{_datasetId}.employees` GROUP BY department",
			parameters: null);

		var rows = results.ToList();
		Assert.Equal(3, rows.Count);
	}

	[Fact]
	public async Task GroupBy_SumSalary()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT department, SUM(salary) AS total FROM `{_datasetId}.employees` GROUP BY department",
			parameters: null);

		var rows = results.ToList();
		Assert.Equal(3, rows.Count);
	}

	[Fact]
	public async Task GroupBy_Having_FilterGroups()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT department, COUNT(*) AS cnt FROM `{_datasetId}.employees` GROUP BY department HAVING COUNT(*) > 1",
			parameters: null);

		var rows = results.ToList();
		Assert.Equal(2, rows.Count); // Engineering(2), Sales(2), HR(1) filtered
	}

	[Fact]
	public async Task InnerJoin_UsersAndOrders()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT u.name, o.amount FROM `{_datasetId}.users` u JOIN `{_datasetId}.orders` o ON u.id = o.user_id",
			parameters: null);

		var rows = results.ToList();
		Assert.Equal(3, rows.Count); // Alice(2), Bob(1)
	}

	[Fact]
	public async Task LeftJoin_IncludesUnmatchedUsers()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT u.name, o.amount FROM `{_datasetId}.users` u LEFT JOIN `{_datasetId}.orders` o ON u.id = o.user_id",
			parameters: null);

		var rows = results.ToList();
		Assert.Equal(4, rows.Count); // Alice(2) + Bob(1) + Charlie(null)
	}

	[Fact]
	public async Task CrossJoin_CartesianProduct()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT u.name, o.order_id FROM `{_datasetId}.users` u CROSS JOIN `{_datasetId}.orders` o",
			parameters: null);

		var rows = results.ToList();
		Assert.Equal(9, rows.Count); // 3 × 3
	}

	[Fact]
	public async Task Join_WithGroupBy_SumPerUser()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT u.name, SUM(o.amount) AS total FROM `{_datasetId}.users` u JOIN `{_datasetId}.orders` o ON u.id = o.user_id GROUP BY u.name",
			parameters: null);

		var rows = results.ToList();
		Assert.Equal(2, rows.Count); // Alice and Bob
	}

	[Fact]
	public async Task CaseExpression_InSelect()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT name, CASE WHEN salary >= 90000 THEN 'high' ELSE 'low' END AS level FROM `{_datasetId}.employees`",
			parameters: null);

		var rows = results.ToList();
		Assert.Equal(5, rows.Count);
	}
}
