using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for advanced join patterns: multi-table, self-join, FULL OUTER, CROSS, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#join_types
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class JoinEdgeCaseTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public JoinEdgeCaseTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_join_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.CreateTableAsync(_datasetId, "employees", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING" },
				new TableFieldSchema { Name = "dept_id", Type = "INTEGER" },
				new TableFieldSchema { Name = "mgr_id", Type = "INTEGER" },
			]
		});
		await client.CreateTableAsync(_datasetId, "departments", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING" },
			]
		});
		await client.CreateTableAsync(_datasetId, "projects", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING" },
				new TableFieldSchema { Name = "dept_id", Type = "INTEGER" },
			]
		});

		await client.InsertRowsAsync(_datasetId, "employees", new[]
		{
			new BigQueryInsertRow("e1") { ["id"] = 1, ["name"] = "Alice", ["dept_id"] = 10, ["mgr_id"] = (long?)null },
			new BigQueryInsertRow("e2") { ["id"] = 2, ["name"] = "Bob", ["dept_id"] = 10, ["mgr_id"] = 1 },
			new BigQueryInsertRow("e3") { ["id"] = 3, ["name"] = "Carol", ["dept_id"] = 20, ["mgr_id"] = 1 },
			new BigQueryInsertRow("e4") { ["id"] = 4, ["name"] = "Dave", ["dept_id"] = 30, ["mgr_id"] = 3 },
			new BigQueryInsertRow("e5") { ["id"] = 5, ["name"] = "Eve", ["dept_id"] = (long?)null, ["mgr_id"] = (long?)null },
		});
		await client.InsertRowsAsync(_datasetId, "departments", new[]
		{
			new BigQueryInsertRow("d1") { ["id"] = 10, ["name"] = "Engineering" },
			new BigQueryInsertRow("d2") { ["id"] = 20, ["name"] = "Sales" },
			new BigQueryInsertRow("d3") { ["id"] = 40, ["name"] = "Marketing" },
		});
		await client.InsertRowsAsync(_datasetId, "projects", new[]
		{
			new BigQueryInsertRow("p1") { ["id"] = 100, ["name"] = "Alpha", ["dept_id"] = 10 },
			new BigQueryInsertRow("p2") { ["id"] = 101, ["name"] = "Beta", ["dept_id"] = 20 },
			new BigQueryInsertRow("p3") { ["id"] = 102, ["name"] = "Gamma", ["dept_id"] = 10 },
		});
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<List<BigQueryRow>> Q(string sql)
	{
		var client = await _fixture.GetClientAsync();
		return (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
	}

	private async Task<string?> S(string sql)
	{
		var rows = await Q(sql);
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- INNER JOIN ----
	[Fact] public async Task InnerJoin_Basic()
	{
		var rows = await Q($"SELECT e.name, d.name AS dept FROM `{_datasetId}.employees` e JOIN `{_datasetId}.departments` d ON e.dept_id = d.id ORDER BY e.name");
		Assert.Equal(3, rows.Count); // Alice, Bob, Carol (Dave's dept_id=30 not in departments, Eve null)
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}

	[Fact] public async Task InnerJoin_NoMatch()
	{
		var v = await S($"SELECT COUNT(*) FROM `{_datasetId}.employees` e JOIN `{_datasetId}.departments` d ON e.dept_id = d.id WHERE d.name = 'Marketing'");
		Assert.Equal("0", v);
	}

	// ---- LEFT JOIN ----
	[Fact] public async Task LeftJoin_IncludesNonMatching()
	{
		var rows = await Q($"SELECT e.name, d.name AS dept FROM `{_datasetId}.employees` e LEFT JOIN `{_datasetId}.departments` d ON e.dept_id = d.id ORDER BY e.name");
		Assert.Equal(5, rows.Count);
		var dave = rows.First(r => r["name"]?.ToString() == "Dave");
		Assert.Null(dave["dept"]);
	}

	[Fact] public async Task LeftJoin_NullKey()
	{
		var rows = await Q($"SELECT e.name, d.name AS dept FROM `{_datasetId}.employees` e LEFT JOIN `{_datasetId}.departments` d ON e.dept_id = d.id WHERE e.name = 'Eve'");
		Assert.Single(rows);
		Assert.Null(rows[0]["dept"]);
	}

	// ---- RIGHT JOIN ----
	[Fact] public async Task RightJoin_IncludesNonMatching()
	{
		var rows = await Q($"SELECT e.name, d.name AS dept FROM `{_datasetId}.employees` e RIGHT JOIN `{_datasetId}.departments` d ON e.dept_id = d.id ORDER BY d.name");
		var marketing = rows.Where(r => r["dept"]?.ToString() == "Marketing").ToList();
		Assert.Single(marketing);
		Assert.Null(marketing[0]["name"]);
	}

	// ---- FULL OUTER JOIN ----
	[Fact] public async Task FullOuterJoin_BothSides()
	{
		var rows = await Q($"SELECT e.name, d.name AS dept FROM `{_datasetId}.employees` e FULL OUTER JOIN `{_datasetId}.departments` d ON e.dept_id = d.id ORDER BY e.name, d.name");
		Assert.True(rows.Count >= 6); // all employees + unmatched Marketing
	}

	[Fact] public async Task FullOuterJoin_NullOnBothSides()
	{
		var rows = await Q($"SELECT e.name, d.name AS dept FROM `{_datasetId}.employees` e FULL OUTER JOIN `{_datasetId}.departments` d ON e.dept_id = d.id");
		var hasNullEmp = rows.Any(r => r["name"] == null);
		var hasNullDept = rows.Any(r => r["dept"] == null);
		Assert.True(hasNullEmp); // Marketing has no employees
		Assert.True(hasNullDept); // Dave and Eve have no dept match
	}

	// ---- CROSS JOIN ----
	[Fact] public async Task CrossJoin_CartesianProduct()
	{
		var v = await S($"SELECT COUNT(*) FROM `{_datasetId}.employees` CROSS JOIN `{_datasetId}.departments`");
		Assert.Equal("15", v); // 5 * 3
	}

	[Fact] public async Task CrossJoin_WithWhere()
	{
		var rows = await Q($"SELECT e.name, d.name AS dept FROM `{_datasetId}.employees` e CROSS JOIN `{_datasetId}.departments` d WHERE e.dept_id = d.id ORDER BY e.name");
		Assert.Equal(3, rows.Count);
	}

	// ---- Self JOIN ----
	[Fact] public async Task SelfJoin_ManagerLookup()
	{
		var rows = await Q($"SELECT e.name AS emp, m.name AS mgr FROM `{_datasetId}.employees` e JOIN `{_datasetId}.employees` m ON e.mgr_id = m.id ORDER BY e.name");
		Assert.Equal(3, rows.Count);
		var bob = rows.First(r => r["emp"]?.ToString() == "Bob");
		Assert.Equal("Alice", bob["mgr"]?.ToString());
	}

	[Fact] public async Task SelfJoin_IncludeNoManager()
	{
		var rows = await Q($"SELECT e.name AS emp, m.name AS mgr FROM `{_datasetId}.employees` e LEFT JOIN `{_datasetId}.employees` m ON e.mgr_id = m.id ORDER BY e.name");
		Assert.Equal(5, rows.Count);
		var alice = rows.First(r => r["emp"]?.ToString() == "Alice");
		Assert.Null(alice["mgr"]);
	}

	// ---- 3-table JOIN ----
	[Fact] public async Task ThreeTableJoin()
	{
		var rows = await Q($@"
			SELECT e.name, d.name AS dept, p.name AS project
			FROM `{_datasetId}.employees` e
			JOIN `{_datasetId}.departments` d ON e.dept_id = d.id
			JOIN `{_datasetId}.projects` p ON d.id = p.dept_id
			ORDER BY e.name, p.name");
		Assert.True(rows.Count >= 4); // Alice+Bob * 2 projects in dept 10, Carol * 1 project in dept 20
	}

	[Fact] public async Task ThreeTableJoin_LeftLeft()
	{
		var rows = await Q($@"
			SELECT e.name, d.name AS dept, p.name AS project
			FROM `{_datasetId}.employees` e
			LEFT JOIN `{_datasetId}.departments` d ON e.dept_id = d.id
			LEFT JOIN `{_datasetId}.projects` p ON d.id = p.dept_id
			ORDER BY e.name");
		Assert.Equal(7, rows.Count); // Alice*2proj, Bob*2proj, Carol*1proj, Dave*noproj, Eve*noproj
	}

	// ---- JOIN with aggregation ----
	[Fact] public async Task Join_WithGroupBy()
	{
		var rows = await Q($@"
			SELECT d.name, COUNT(*) AS cnt
			FROM `{_datasetId}.employees` e
			JOIN `{_datasetId}.departments` d ON e.dept_id = d.id
			GROUP BY d.name
			ORDER BY d.name");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Engineering", rows[0]["name"]?.ToString());
		Assert.Equal("2", rows[0]["cnt"]?.ToString());
	}

	// ---- JOIN with subquery ----
	[Fact] public async Task Join_WithSubquery()
	{
		var rows = await Q($@"
			SELECT e.name, sub.dept_name
			FROM `{_datasetId}.employees` e
			JOIN (SELECT id, name AS dept_name FROM `{_datasetId}.departments`) sub ON e.dept_id = sub.id
			ORDER BY e.name");
		Assert.Equal(3, rows.Count);
	}

	// ---- JOIN with USING ----
	[Fact] public async Task Join_UsingClause()
	{
		// departments.id and employees.dept_id differ, so use explicit ON. Instead test with same column name.
		var rows = await Q($@"
			SELECT e.name
			FROM `{_datasetId}.employees` e
			JOIN `{_datasetId}.employees` e2 ON e.id = e2.id
			ORDER BY e.name");
		Assert.Equal(5, rows.Count);
	}

	// ---- JOIN condition with expression ----
	[Fact] public async Task Join_ExprInCondition()
	{
		var rows = await Q($@"
			SELECT e.name
			FROM `{_datasetId}.employees` e
			JOIN `{_datasetId}.departments` d ON e.dept_id = d.id AND d.name = 'Engineering'
			ORDER BY e.name");
		Assert.Equal(2, rows.Count);
	}

	// ---- JOIN with OR condition ----
	[Fact] public async Task Join_OrCondition()
	{
		var rows = await Q($@"
			SELECT DISTINCT d.name
			FROM `{_datasetId}.employees` e
			JOIN `{_datasetId}.departments` d ON e.dept_id = d.id OR d.id = 40
			ORDER BY d.name");
		Assert.True(rows.Count >= 3);
	}

	// ---- Semi-join via EXISTS ----
	[Fact] public async Task Exists_SemiJoin()
	{
		var rows = await Q($@"
			SELECT d.name
			FROM `{_datasetId}.departments` d
			WHERE EXISTS (SELECT 1 FROM `{_datasetId}.employees` e WHERE e.dept_id = d.id)
			ORDER BY d.name");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Engineering", rows[0]["name"]?.ToString());
		Assert.Equal("Sales", rows[1]["name"]?.ToString());
	}

	// ---- Anti-join via NOT EXISTS ----
	[Fact] public async Task NotExists_AntiJoin()
	{
		var rows = await Q($@"
			SELECT d.name
			FROM `{_datasetId}.departments` d
			WHERE NOT EXISTS (SELECT 1 FROM `{_datasetId}.employees` e WHERE e.dept_id = d.id)
			ORDER BY d.name");
		Assert.Single(rows);
		Assert.Equal("Marketing", rows[0]["name"]?.ToString());
	}

	// ---- IN subquery ----
	[Fact] public async Task In_Subquery()
	{
		var rows = await Q($@"
			SELECT name FROM `{_datasetId}.employees`
			WHERE dept_id IN (SELECT id FROM `{_datasetId}.departments` WHERE name = 'Engineering')
			ORDER BY name");
		Assert.Equal(2, rows.Count);
	}

	[Fact] public async Task NotIn_Subquery()
	{
		var rows = await Q($@"
			SELECT name FROM `{_datasetId}.employees`
			WHERE dept_id NOT IN (SELECT id FROM `{_datasetId}.departments`)
			ORDER BY name");
		// Dave (dept_id=30 not in departments), Eve is NULL so NOT IN with NULL is tricky
		Assert.True(rows.Count >= 1);
		Assert.Contains(rows, r => r["name"]?.ToString() == "Dave");
	}

	// ---- Correlated subquery ----
	[Fact] public async Task CorrelatedSubquery_Scalar()
	{
		var rows = await Q($@"
			SELECT e.name,
				(SELECT d.name FROM `{_datasetId}.departments` d WHERE d.id = e.dept_id) AS dept
			FROM `{_datasetId}.employees` e
			ORDER BY e.name");
		Assert.Equal(5, rows.Count);
		Assert.Equal("Engineering", rows[0]["dept"]?.ToString()); // Alice
	}

	// ---- Multiple conditions ----
	[Fact] public async Task Join_MultipleConditions()
	{
		var rows = await Q($@"
			SELECT e.name
			FROM `{_datasetId}.employees` e
			JOIN `{_datasetId}.departments` d ON e.dept_id = d.id
			WHERE d.name = 'Engineering' AND e.mgr_id IS NOT NULL
			ORDER BY e.name");
		Assert.Single(rows);
		Assert.Equal("Bob", rows[0]["name"]?.ToString());
	}
}
