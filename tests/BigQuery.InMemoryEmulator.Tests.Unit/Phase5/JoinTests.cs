using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase5;

/// <summary>
/// Unit tests for JOIN operations (Phase 5).
/// </summary>
public class JoinTests
{
	private static InMemoryDataStore CreateJoinTestData()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;

		var usersSchema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
			]
		};
		var users = new InMemoryTable("test_ds", "users", usersSchema);
		users.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 1L, ["name"] = "Alice" }));
		users.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 2L, ["name"] = "Bob" }));
		users.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 3L, ["name"] = "Charlie" }));
		ds.Tables["users"] = users;

		var ordersSchema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "order_id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "user_id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "amount", Type = "FLOAT", Mode = "NULLABLE" },
			]
		};
		var orders = new InMemoryTable("test_ds", "orders", ordersSchema);
		orders.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["order_id"] = 101L, ["user_id"] = 1L, ["amount"] = 50.0 }));
		orders.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["order_id"] = 102L, ["user_id"] = 1L, ["amount"] = 30.0 }));
		orders.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["order_id"] = 103L, ["user_id"] = 2L, ["amount"] = 75.0 }));
		ds.Tables["orders"] = orders;

		return store;
	}

	[Fact]
	public void InnerJoin_MatchingRows()
	{
		var store = CreateJoinTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (schema, rows) = executor.Execute(
			"SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id");

		Assert.Equal(3, rows.Count); // Alice has 2 orders, Bob has 1, Charlie has 0
	}

	[Fact]
	public void InnerJoin_ExplicitInner()
	{
		var store = CreateJoinTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (schema, rows) = executor.Execute(
			"SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id");

		Assert.Equal(3, rows.Count);
	}

	[Fact]
	public void LeftJoin_IncludesUnmatched()
	{
		var store = CreateJoinTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (schema, rows) = executor.Execute(
			"SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id");

		Assert.Equal(4, rows.Count); // Alice(2) + Bob(1) + Charlie(NULL)
	}

	[Fact]
	public void LeftJoin_UnmatchedRowHasNulls()
	{
		var store = CreateJoinTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (schema, rows) = executor.Execute(
			"SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.name");

		// Charlie has no orders — amount should be null
		var charlieRow = rows.Last(); // ORDER BY name, Charlie is last alphabetically
		Assert.Equal("Charlie", charlieRow.F[0].V?.ToString());
		Assert.Null(charlieRow.F[1].V);
	}

	[Fact]
	public void RightJoin_IncludesUnmatched()
	{
		// Add an order for a user that doesn't exist
		var store = CreateJoinTestData();
		var ds = store.Datasets["test_ds"];
		ds.Tables["orders"].Rows.Add(new InMemoryRow(new Dictionary<string, object?>
			{ ["order_id"] = 104L, ["user_id"] = 99L, ["amount"] = 10.0 }));

		var executor = new QueryExecutor(store, "test_ds");
		var (schema, rows) = executor.Execute(
			"SELECT u.name, o.amount FROM users u RIGHT JOIN orders o ON u.id = o.user_id");

		Assert.Equal(4, rows.Count); // 3 matched + 1 unmatched order
	}

	[Fact]
	public void CrossJoin_CartesianProduct()
	{
		var store = CreateJoinTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (schema, rows) = executor.Execute(
			"SELECT u.name, o.order_id FROM users u CROSS JOIN orders o");

		Assert.Equal(9, rows.Count); // 3 users × 3 orders
	}

	[Fact]
	public void FullOuterJoin_IncludesBothSides()
	{
		var store = CreateJoinTestData();
		// Add order for non-existent user
		var ds = store.Datasets["test_ds"];
		ds.Tables["orders"].Rows.Add(new InMemoryRow(new Dictionary<string, object?>
			{ ["order_id"] = 104L, ["user_id"] = 99L, ["amount"] = 10.0 }));

		var executor = new QueryExecutor(store, "test_ds");
		var (schema, rows) = executor.Execute(
			"SELECT u.name, o.amount FROM users u FULL OUTER JOIN orders o ON u.id = o.user_id");

		// Alice(2) + Bob(1) + Charlie(NULL right) + order 104 (NULL left) = 5
		Assert.Equal(5, rows.Count);
	}

	[Fact]
	public void Join_WithGroupBy()
	{
		var store = CreateJoinTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (schema, rows) = executor.Execute(
			"SELECT u.name, SUM(o.amount) AS total FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name");

		Assert.Equal(2, rows.Count); // Alice and Bob have orders
		var aliceIdx = Enumerable.Range(0, rows.Count).First(i => rows[i].F[0].V?.ToString() == "Alice");
		Assert.Equal("80", rows[aliceIdx].F[1].V?.ToString()); // 50 + 30
	}

	[Fact]
	public void Join_WithWhere()
	{
		var store = CreateJoinTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (schema, rows) = executor.Execute(
			"SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 40");

		Assert.Equal(2, rows.Count); // Alice's 50.0 order and Bob's 75.0 order
	}
}
