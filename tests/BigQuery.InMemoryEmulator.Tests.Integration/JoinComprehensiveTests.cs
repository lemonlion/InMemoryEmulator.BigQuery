using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Comprehensive JOIN tests: INNER, LEFT, RIGHT, FULL OUTER, CROSS, self-joins, multiple joins, with aggregation.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#join_types
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class JoinComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public JoinComprehensiveTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_join_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.CreateTableAsync(_datasetId, "users", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING" },
			]
		});
		await client.CreateTableAsync(_datasetId, "orders", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "order_id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "user_id", Type = "INTEGER" },
				new TableFieldSchema { Name = "amount", Type = "FLOAT" },
			]
		});
		await client.CreateTableAsync(_datasetId, "products", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "product_id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "product_name", Type = "STRING" },
			]
		});
		await client.CreateTableAsync(_datasetId, "order_items", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "order_id", Type = "INTEGER" },
				new TableFieldSchema { Name = "product_id", Type = "INTEGER" },
				new TableFieldSchema { Name = "qty", Type = "INTEGER" },
			]
		});

		await client.InsertRowsAsync(_datasetId, "users", new[]
		{
			new BigQueryInsertRow("u1") { ["id"] = 1, ["name"] = "Alice" },
			new BigQueryInsertRow("u2") { ["id"] = 2, ["name"] = "Bob" },
			new BigQueryInsertRow("u3") { ["id"] = 3, ["name"] = "Charlie" },
		});
		await client.InsertRowsAsync(_datasetId, "orders", new[]
		{
			new BigQueryInsertRow("o1") { ["order_id"] = 101, ["user_id"] = 1, ["amount"] = 50.0 },
			new BigQueryInsertRow("o2") { ["order_id"] = 102, ["user_id"] = 1, ["amount"] = 30.0 },
			new BigQueryInsertRow("o3") { ["order_id"] = 103, ["user_id"] = 2, ["amount"] = 75.0 },
			new BigQueryInsertRow("o4") { ["order_id"] = 104, ["user_id"] = 9, ["amount"] = 10.0 }, // orphan
		});
		await client.InsertRowsAsync(_datasetId, "products", new[]
		{
			new BigQueryInsertRow("p1") { ["product_id"] = 1, ["product_name"] = "Widget" },
			new BigQueryInsertRow("p2") { ["product_id"] = 2, ["product_name"] = "Gadget" },
		});
		await client.InsertRowsAsync(_datasetId, "order_items", new[]
		{
			new BigQueryInsertRow("oi1") { ["order_id"] = 101, ["product_id"] = 1, ["qty"] = 2 },
			new BigQueryInsertRow("oi2") { ["order_id"] = 101, ["product_id"] = 2, ["qty"] = 1 },
			new BigQueryInsertRow("oi3") { ["order_id"] = 102, ["product_id"] = 1, ["qty"] = 3 },
			new BigQueryInsertRow("oi4") { ["order_id"] = 103, ["product_id"] = 2, ["qty"] = 5 },
		});
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		return result.ToList();
	}

	// ---- INNER JOIN ----
	[Fact] public async Task InnerJoin_Basic()
	{
		var rows = await Query($"SELECT u.name, o.amount FROM `{_datasetId}.users` u INNER JOIN `{_datasetId}.orders` o ON u.id = o.user_id ORDER BY o.order_id");
		Assert.Equal(3, rows.Count); // Alice 2, Bob 1
	}

	[Fact] public async Task InnerJoin_NoMatch_Empty()
	{
		var rows = await Query($"SELECT u.name FROM `{_datasetId}.users` u INNER JOIN `{_datasetId}.orders` o ON u.id = o.user_id WHERE u.name = 'Charlie'");
		Assert.Empty(rows);
	}

	[Fact] public async Task InnerJoin_WithAlias()
	{
		var rows = await Query($"SELECT u.name, o.amount FROM `{_datasetId}.users` AS u JOIN `{_datasetId}.orders` AS o ON u.id = o.user_id ORDER BY o.order_id");
		Assert.Equal(3, rows.Count);
	}

	// ---- LEFT JOIN ----
	[Fact] public async Task LeftJoin_IncludesUnmatched()
	{
		var rows = await Query($"SELECT u.name, o.order_id FROM `{_datasetId}.users` u LEFT JOIN `{_datasetId}.orders` o ON u.id = o.user_id ORDER BY u.name");
		Assert.True(rows.Count >= 4); // Alice(2), Bob(1), Charlie(1 null)
		var charlie = rows.FirstOrDefault(r => r["name"]?.ToString() == "Charlie");
		Assert.NotNull(charlie);
		Assert.Null(charlie!["order_id"]);
	}

	[Fact] public async Task LeftJoin_AllMatched()
	{
		var rows = await Query($"SELECT u.name, o.amount FROM `{_datasetId}.users` u LEFT JOIN `{_datasetId}.orders` o ON u.id = o.user_id WHERE o.amount IS NOT NULL ORDER BY u.name");
		Assert.Equal(3, rows.Count);
	}

	// ---- RIGHT JOIN ----
	[Fact] public async Task RightJoin_IncludesOrphans()
	{
		var rows = await Query($"SELECT u.name, o.order_id FROM `{_datasetId}.users` u RIGHT JOIN `{_datasetId}.orders` o ON u.id = o.user_id ORDER BY o.order_id");
		Assert.Equal(4, rows.Count);
		var orphan = rows.FirstOrDefault(r => r["order_id"]?.ToString() == "104");
		Assert.NotNull(orphan);
		Assert.Null(orphan!["name"]);
	}

	// ---- FULL OUTER JOIN ----
	[Fact] public async Task FullOuterJoin_Both()
	{
		var rows = await Query($"SELECT u.name, o.order_id FROM `{_datasetId}.users` u FULL OUTER JOIN `{_datasetId}.orders` o ON u.id = o.user_id ORDER BY u.name, o.order_id");
		Assert.True(rows.Count >= 5); // 3 matched + Charlie(null) + orphan(null)
	}

	// ---- CROSS JOIN ----
	[Fact] public async Task CrossJoin_CartesianProduct()
	{
		var rows = await Query($"SELECT u.name, p.product_name FROM `{_datasetId}.users` u CROSS JOIN `{_datasetId}.products` p ORDER BY u.name, p.product_name");
		Assert.Equal(6, rows.Count); // 3 users × 2 products
	}

	[Fact] public async Task CrossJoin_ImplicitComma()
	{
		var rows = await Query($"SELECT u.name, p.product_name FROM `{_datasetId}.users` u, `{_datasetId}.products` p ORDER BY u.name, p.product_name");
		Assert.Equal(6, rows.Count);
	}

	// ---- Multiple JOINs ----
	[Fact] public async Task ThreeTableJoin()
	{
		var rows = await Query($@"
			SELECT u.name, p.product_name, oi.qty 
			FROM `{_datasetId}.users` u
			JOIN `{_datasetId}.orders` o ON u.id = o.user_id
			JOIN `{_datasetId}.order_items` oi ON o.order_id = oi.order_id
			JOIN `{_datasetId}.products` p ON oi.product_id = p.product_id
			ORDER BY u.name, p.product_name
		");
		Assert.Equal(4, rows.Count);
	}

	// ---- Self JOIN ----
	[Fact] public async Task SelfJoin()
	{
		var rows = await Query($"SELECT a.name AS name1, b.name AS name2 FROM `{_datasetId}.users` a CROSS JOIN `{_datasetId}.users` b WHERE a.id < b.id ORDER BY a.id, b.id");
		Assert.Equal(3, rows.Count); // (1,2), (1,3), (2,3)
	}

	// ---- JOIN with GROUP BY ----
	[Fact] public async Task JoinWithGroupBy()
	{
		var rows = await Query($@"
			SELECT u.name, SUM(o.amount) AS total
			FROM `{_datasetId}.users` u
			JOIN `{_datasetId}.orders` o ON u.id = o.user_id
			GROUP BY u.name
			ORDER BY total DESC
		");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal(80.0, double.Parse(rows[0]["total"]?.ToString()!), 0.01);
	}

	// ---- JOIN with WHERE ----
	[Fact] public async Task JoinWithWhere()
	{
		var rows = await Query($"SELECT u.name, o.amount FROM `{_datasetId}.users` u JOIN `{_datasetId}.orders` o ON u.id = o.user_id WHERE o.amount > 40 ORDER BY o.amount");
		Assert.Equal(2, rows.Count);
	}

	// ---- JOIN with HAVING ----
	[Fact] public async Task JoinWithHaving()
	{
		var rows = await Query($@"
			SELECT u.name, COUNT(*) AS order_count
			FROM `{_datasetId}.users` u
			JOIN `{_datasetId}.orders` o ON u.id = o.user_id
			GROUP BY u.name
			HAVING COUNT(*) > 1
		");
		Assert.Single(rows);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}

	// ---- JOIN on multiple conditions ----
	[Fact] public async Task JoinMultipleConditions()
	{
		var rows = await Query($@"
			SELECT u.name, o.amount
			FROM `{_datasetId}.users` u 
			JOIN `{_datasetId}.orders` o ON u.id = o.user_id AND o.amount > 40
			ORDER BY o.amount
		");
		Assert.Equal(2, rows.Count);
	}

	// ---- USING clause ----
	[Fact] public async Task JoinUsing()
	{
		var rows = await Query($@"
			SELECT oi.order_id, p.product_name, oi.qty
			FROM `{_datasetId}.order_items` oi
			JOIN `{_datasetId}.products` p USING (product_id)
			ORDER BY oi.order_id, p.product_name
		");
		Assert.Equal(4, rows.Count);
	}

	// ---- UNNEST as join ----
	[Fact] public async Task UnnestJoin()
	{
		var rows = await Query("SELECT val FROM UNNEST([1, 2, 3]) AS val ORDER BY val");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]["val"]?.ToString());
	}

	[Fact] public async Task UnnestCrossJoin()
	{
		var rows = await Query("SELECT n, v FROM (SELECT 'test' AS n), UNNEST([10, 20, 30]) AS v ORDER BY v");
		Assert.Equal(3, rows.Count);
		Assert.Equal("test", rows[0]["n"]?.ToString());
	}

	// ---- LEFT JOIN with IS NULL pattern (anti-join) ----
	[Fact] public async Task AntiJoin_LeftJoinIsNull()
	{
		var rows = await Query($@"
			SELECT u.name 
			FROM `{_datasetId}.users` u 
			LEFT JOIN `{_datasetId}.orders` o ON u.id = o.user_id 
			WHERE o.order_id IS NULL
		");
		Assert.Single(rows);
		Assert.Equal("Charlie", rows[0]["name"]?.ToString());
	}

	// ---- Join with subquery ----
	[Fact] public async Task JoinWithSubquery()
	{
		var rows = await Query($@"
			SELECT u.name, agg.total
			FROM `{_datasetId}.users` u
			JOIN (SELECT user_id, SUM(amount) AS total FROM `{_datasetId}.orders` GROUP BY user_id) agg ON u.id = agg.user_id
			ORDER BY agg.total DESC
		");
		Assert.Equal(2, rows.Count);
	}
}
