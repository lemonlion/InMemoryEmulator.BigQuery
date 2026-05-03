using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for complex query composition: nested subqueries, correlated queries, derived tables.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#subqueries
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ComplexQueryCompositionTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public ComplexQueryCompositionTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_cqc_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.emp` (id INT64, name STRING, dept STRING, salary INT64, mgr_id INT64)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.emp` (id, name, dept, salary, mgr_id) VALUES
			(1, 'Alice', 'Eng', 100000, NULL),
			(2, 'Bob', 'Eng', 90000, 1),
			(3, 'Carol', 'Sales', 80000, 1),
			(4, 'Dave', 'Sales', 85000, 3),
			(5, 'Eve', 'Eng', 95000, 1),
			(6, 'Frank', 'HR', 75000, NULL),
			(7, 'Grace', 'HR', 70000, 6),
			(8, 'Hank', 'Eng', 110000, 1)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
		return result.ToList();
	}

	// Scalar subquery
	[Fact] public async Task ScalarSubquery_InSelect()
	{
		var rows = await Query("SELECT name, (SELECT MAX(salary) FROM `{ds}.emp`) AS max_sal FROM `{ds}.emp` WHERE id = 1");
		Assert.Equal("110000", rows[0]["max_sal"].ToString());
	}

	[Fact] public async Task ScalarSubquery_InWhere()
	{
		var rows = await Query("SELECT name FROM `{ds}.emp` WHERE salary = (SELECT MAX(salary) FROM `{ds}.emp`)");
		Assert.Equal("Hank", rows[0]["name"].ToString());
	}

	// EXISTS subquery
	[Fact] public async Task Exists_True()
	{
		var rows = await Query("SELECT name FROM `{ds}.emp` WHERE EXISTS(SELECT 1 FROM `{ds}.emp` e2 WHERE e2.mgr_id = emp.id) ORDER BY name");
		Assert.True(rows.Count >= 2); // Alice and Carol are managers
	}

	[Fact] public async Task NotExists()
	{
		var rows = await Query("SELECT name FROM `{ds}.emp` WHERE NOT EXISTS(SELECT 1 FROM `{ds}.emp` e2 WHERE e2.mgr_id = emp.id) ORDER BY name");
		Assert.True(rows.Count >= 5); // Non-managers
	}

	// IN subquery
	[Fact] public async Task In_Subquery()
	{
		var rows = await Query("SELECT name FROM `{ds}.emp` WHERE dept IN (SELECT dept FROM `{ds}.emp` WHERE salary > 100000) ORDER BY name");
		Assert.True(rows.Count >= 1); // Eng dept has someone >100k
	}

	[Fact] public async Task NotIn_Subquery()
	{
		var rows = await Query("SELECT name FROM `{ds}.emp` WHERE id NOT IN (SELECT mgr_id FROM `{ds}.emp` WHERE mgr_id IS NOT NULL) ORDER BY name");
		Assert.True(rows.Count >= 5);
	}

	// Derived table (FROM subquery)
	[Fact] public async Task DerivedTable_Basic()
	{
		var rows = await Query(@"
			SELECT dept, avg_sal FROM (
				SELECT dept, CAST(AVG(salary) AS INT64) AS avg_sal FROM `{ds}.emp` GROUP BY dept
			) WHERE avg_sal > 80000 ORDER BY dept");
		Assert.True(rows.Count >= 1);
	}

	[Fact] public async Task DerivedTable_WithAlias()
	{
		var result = await Scalar("SELECT t.cnt FROM (SELECT COUNT(*) AS cnt FROM `{ds}.emp`) AS t");
		Assert.Equal("8", result);
	}

	// Multiple levels of nesting
	[Fact] public async Task Nested_TwoLevels()
	{
		var result = await Scalar(@"
			SELECT MAX(avg_sal) FROM (
				SELECT CAST(AVG(salary) AS INT64) AS avg_sal FROM `{ds}.emp` GROUP BY dept
			)");
		Assert.NotNull(result);
	}

	// CTE + subquery combination
	[Fact] public async Task Cte_WithSubquery()
	{
		var rows = await Query(@"
			WITH dept_stats AS (
				SELECT dept, CAST(AVG(salary) AS INT64) AS avg_sal, COUNT(*) AS cnt
				FROM `{ds}.emp` GROUP BY dept
			)
			SELECT e.name, d.avg_sal
			FROM `{ds}.emp` e
			JOIN dept_stats d ON e.dept = d.dept
			WHERE e.salary > d.avg_sal
			ORDER BY e.name");
		Assert.True(rows.Count >= 1);
	}

	// JOIN + aggregation + subquery
	[Fact] public async Task Join_WithAggregatedSubquery()
	{
		var rows = await Query(@"
			SELECT e.name, e.salary, d.max_sal
			FROM `{ds}.emp` e
			JOIN (SELECT dept, MAX(salary) AS max_sal FROM `{ds}.emp` GROUP BY dept) d
				ON e.dept = d.dept
			WHERE e.salary = d.max_sal
			ORDER BY e.name");
		Assert.True(rows.Count >= 3); // One per dept with max salary
	}

	// UNION in subquery
	[Fact] public async Task Union_InSubquery()
	{
		var result = await Scalar(@"
			SELECT COUNT(*) FROM (
				SELECT name FROM `{ds}.emp` WHERE dept = 'Eng'
				UNION ALL
				SELECT name FROM `{ds}.emp` WHERE dept = 'HR'
			)");
		Assert.Equal("6", result); // 4 Eng + 2 HR
	}

	// Window function with derived table
	[Fact] public async Task Window_InDerivedTable()
	{
		var rows = await Query(@"
			SELECT name, rnk FROM (
				SELECT name, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk
				FROM `{ds}.emp`
			) WHERE rnk = 1 ORDER BY name");
		Assert.Equal(3, rows.Count); // One from each dept
	}

	// GROUP BY with HAVING referencing aggregate
	[Fact] public async Task Having_Complex()
	{
		var rows = await Query("SELECT dept, COUNT(*) AS cnt FROM `{ds}.emp` GROUP BY dept HAVING COUNT(*) > 2 ORDER BY dept");
		Assert.Equal("Eng", rows[0]["dept"].ToString());
	}

	// Multiple JOINs
	[Fact] public async Task MultiJoin()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($"CREATE TABLE `{_datasetId}.dept` (name STRING, budget INT64)", parameters: null);
		await client.ExecuteQueryAsync($"INSERT INTO `{_datasetId}.dept` (name, budget) VALUES ('Eng', 500000), ('Sales', 300000), ('HR', 200000)", parameters: null);
		var rows = await Query(@"
			SELECT e.name, e.salary, d.budget
			FROM `{ds}.emp` e
			JOIN `{ds}.dept` d ON e.dept = d.name
			WHERE e.salary > 90000
			ORDER BY e.name");
		Assert.True(rows.Count >= 3);
	}

	// CASE in aggregate
	[Fact] public async Task Case_InAggregate()
	{
		var result = await Scalar("SELECT SUM(CASE WHEN dept = 'Eng' THEN salary ELSE 0 END) FROM `{ds}.emp`");
		Assert.Equal("395000", result); // 100k+90k+95k+110k = 395k
	}

	// Subquery in CASE
	[Fact] public async Task Subquery_InCase()
	{
		var rows = await Query(@"
			SELECT name,
				CASE WHEN salary > (SELECT AVG(salary) FROM `{ds}.emp`) THEN 'above' ELSE 'below' END AS tier
			FROM `{ds}.emp` WHERE id <= 3 ORDER BY name");
		Assert.True(rows.Count == 3);
	}
}
