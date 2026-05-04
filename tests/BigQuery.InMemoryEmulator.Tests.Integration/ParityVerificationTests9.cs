using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Parity verification tests batch 9: DML operations (UPDATE, DELETE, MERGE),
/// complex JOIN patterns, correlated subqueries, UNNEST, STRUCT operations,
/// and advanced expression evaluation.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests9 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests9(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv9_{Guid.NewGuid():N}"[..28];
		await _fixture.CreateDatasetAsync(_ds);

		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_ds}.employees` (id INT64, name STRING, dept STRING, salary INT64, manager_id INT64)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_ds}.employees` (id, name, dept, salary, manager_id) VALUES
			(1, 'Alice', 'Eng', 100000, NULL),
			(2, 'Bob', 'Eng', 90000, 1),
			(3, 'Charlie', 'Sales', 80000, 1),
			(4, 'Diana', 'Sales', 85000, 3),
			(5, 'Eve', 'Eng', 95000, 1),
			(6, 'Frank', 'HR', 75000, NULL)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try
		{
			var c = await _fixture.GetClientAsync();
			await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true });
		}
		catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null);
		var rows = result.ToList();
		return rows.Count == 0 ? null : rows[0][0]?.ToString();
	}

	private async Task<List<BigQueryRow>> Q(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null);
		return result.ToList();
	}

	private async Task Exec(string sql)
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// UPDATE
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#update_statement
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Update_SingleRow()
	{
		await Exec("UPDATE `{ds}.employees` SET salary = 110000 WHERE id = 1");
		var result = await S("SELECT salary FROM `{ds}.employees` WHERE id = 1");
		Assert.Equal("110000", result);
	}

	[Fact] public async Task Update_MultipleRows()
	{
		await Exec("UPDATE `{ds}.employees` SET salary = salary + 5000 WHERE dept = 'Eng'");
		var result = await S("SELECT SUM(salary) FROM `{ds}.employees` WHERE dept = 'Eng'");
		// Original: 100000 + 90000 + 95000 = 285000 + 15000 = 300000
		Assert.Equal("300000", result);
	}

	[Fact] public async Task Update_MultipleColumns()
	{
		await Exec("UPDATE `{ds}.employees` SET dept = 'Engineering', salary = 120000 WHERE id = 1");
		var rows = await Q("SELECT dept, salary FROM `{ds}.employees` WHERE id = 1");
		Assert.Equal("Engineering", rows[0]["dept"]?.ToString());
		Assert.Equal("120000", rows[0]["salary"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// DELETE
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#delete_statement
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Delete_SingleRow()
	{
		await Exec("DELETE FROM `{ds}.employees` WHERE id = 6");
		var result = await S("SELECT COUNT(*) FROM `{ds}.employees`");
		Assert.Equal("5", result);
	}

	[Fact] public async Task Delete_MultipleRows()
	{
		await Exec("DELETE FROM `{ds}.employees` WHERE dept = 'Sales'");
		var result = await S("SELECT COUNT(*) FROM `{ds}.employees`");
		Assert.Equal("4", result);
	}

	[Fact] public async Task Delete_NoMatch()
	{
		await Exec("DELETE FROM `{ds}.employees` WHERE id = 999");
		var result = await S("SELECT COUNT(*) FROM `{ds}.employees`");
		Assert.Equal("6", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// MERGE
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Merge_InsertWhenNotMatched()
	{
		await Exec(@"
			MERGE `{ds}.employees` AS t
			USING (SELECT 7 AS id, 'Grace' AS name, 'Eng' AS dept, 88000 AS salary, 1 AS manager_id) AS s
			ON t.id = s.id
			WHEN NOT MATCHED THEN INSERT (id, name, dept, salary, manager_id) VALUES (s.id, s.name, s.dept, s.salary, s.manager_id)");
		var result = await S("SELECT name FROM `{ds}.employees` WHERE id = 7");
		Assert.Equal("Grace", result);
	}

	[Fact] public async Task Merge_UpdateWhenMatched()
	{
		await Exec(@"
			MERGE `{ds}.employees` AS t
			USING (SELECT 1 AS id, 115000 AS new_salary) AS s
			ON t.id = s.id
			WHEN MATCHED THEN UPDATE SET salary = s.new_salary");
		var result = await S("SELECT salary FROM `{ds}.employees` WHERE id = 1");
		Assert.Equal("115000", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// JOIN
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#join_types
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task InnerJoin_Self()
	{
		// Self-join to find employees and their managers
		var rows = await Q(@"
			SELECT e.name AS emp, m.name AS mgr
			FROM `{ds}.employees` e
			INNER JOIN `{ds}.employees` m ON e.manager_id = m.id
			ORDER BY e.name");
		Assert.Equal(4, rows.Count); // Bob, Charlie, Diana, Eve have managers
		Assert.Equal("Bob", rows[0]["emp"]?.ToString());
		Assert.Equal("Alice", rows[0]["mgr"]?.ToString());
	}

	[Fact] public async Task LeftJoin()
	{
		var rows = await Q(@"
			SELECT e.name AS emp, m.name AS mgr
			FROM `{ds}.employees` e
			LEFT JOIN `{ds}.employees` m ON e.manager_id = m.id
			ORDER BY e.name");
		Assert.Equal(6, rows.Count); // All employees
		// Alice has no manager
		var alice = rows.First(r => r["emp"]?.ToString() == "Alice");
		Assert.Null(alice["mgr"]);
	}

	[Fact] public async Task CrossJoin()
	{
		var result = await S(@"
			SELECT COUNT(*) FROM `{ds}.employees` a CROSS JOIN `{ds}.employees` b
			WHERE a.dept = 'HR' AND b.dept = 'HR'");
		// HR has 1 person, 1x1 = 1
		Assert.Equal("1", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// UNNEST
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unnest_operator
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Unnest_Basic()
	{
		var result = await S("SELECT SUM(x) FROM UNNEST([1, 2, 3, 4, 5]) AS x");
		Assert.Equal("15", result);
	}

	[Fact] public async Task Unnest_WithOffset()
	{
		var rows = await Q("SELECT x, off FROM UNNEST(['a', 'b', 'c']) AS x WITH OFFSET AS off ORDER BY off");
		Assert.Equal(3, rows.Count);
		Assert.Equal("a", rows[0]["x"]?.ToString());
		Assert.Equal("0", rows[0]["off"]?.ToString());
		Assert.Equal("c", rows[2]["x"]?.ToString());
		Assert.Equal("2", rows[2]["off"]?.ToString());
	}

	[Fact] public async Task Unnest_InWhere()
	{
		var rows = await Q(@"
			SELECT name FROM `{ds}.employees`
			WHERE dept IN UNNEST(['Eng', 'HR'])
			ORDER BY name");
		Assert.Equal(4, rows.Count); // Alice, Bob, Eve (Eng) + Frank (HR)
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Correlated subqueries
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task CorrelatedSubquery_ScalarInSelect()
	{
		var rows = await Q(@"
			SELECT e.name, 
				(SELECT COUNT(*) FROM `{ds}.employees` sub WHERE sub.manager_id = e.id) AS report_count
			FROM `{ds}.employees` e
			WHERE e.id <= 3
			ORDER BY e.id");
		Assert.Equal("3", rows[0]["report_count"]?.ToString()); // Alice has 3 reports
		Assert.Equal("0", rows[1]["report_count"]?.ToString()); // Bob has 0
		Assert.Equal("1", rows[2]["report_count"]?.ToString()); // Charlie has 1
	}

	// ───────────────────────────────────────────────────────────────────────────
	// STRUCT construction and access
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Struct_Literal()
	{
		var result = await S("SELECT STRUCT(1 AS x, 'hello' AS y).x");
		Assert.Equal("1", result);
	}

	[Fact] public async Task Struct_NestedAccess()
	{
		var result = await S("SELECT STRUCT(STRUCT(42 AS inner_val) AS nested).nested.inner_val");
		Assert.Equal("42", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Multiple aggregates in single query
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task MultipleAggregates()
	{
		var rows = await Q(@"
			SELECT dept, COUNT(*) AS cnt, SUM(salary) AS total, AVG(salary) AS avg_sal
			FROM `{ds}.employees`
			GROUP BY dept
			ORDER BY dept");
		Assert.Equal("Eng", rows[0]["dept"]?.ToString());
		Assert.Equal("3", rows[0]["cnt"]?.ToString());
		Assert.Equal("285000", rows[0]["total"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// String functions on table data
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Upper_Lower()
	{
		var rows = await Q("SELECT UPPER(name) AS up, LOWER(name) AS low FROM `{ds}.employees` WHERE id = 1");
		Assert.Equal("ALICE", rows[0]["up"]?.ToString());
		Assert.Equal("alice", rows[0]["low"]?.ToString());
	}

	[Fact] public async Task Trim()
	{
		var result = await S("SELECT TRIM('  hello  ')");
		Assert.Equal("hello", result);
	}

	[Fact] public async Task Format_String()
	{
		var result = await S("SELECT FORMAT('%s has %d reports', 'Alice', 3)");
		Assert.Equal("Alice has 3 reports", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Conditional expressions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Iff_TrueCase()
	{
		var result = await S("SELECT IF(salary > 90000, 'high', 'low') FROM `{ds}.employees` WHERE id = 1");
		Assert.Equal("high", result);
	}

	[Fact] public async Task Coalesce_WithTableColumn()
	{
		// manager_id is NULL for Alice and Frank
		var result = await S("SELECT COALESCE(manager_id, -1) FROM `{ds}.employees` WHERE id = 1");
		Assert.Equal("-1", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// ORDER BY expression
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task OrderBy_Expression()
	{
		var rows = await Q("SELECT name FROM `{ds}.employees` ORDER BY LENGTH(name) DESC, name LIMIT 3");
		Assert.Equal("Charlie", rows[0]["name"]?.ToString()); // 7 chars
	}

	[Fact] public async Task OrderBy_Desc()
	{
		var rows = await Q("SELECT name FROM `{ds}.employees` ORDER BY salary DESC LIMIT 1");
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// EXCEPT and INTERSECT
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Except_Basic()
	{
		var result = await S(@"
			SELECT COUNT(*) FROM (
				SELECT dept FROM `{ds}.employees`
				EXCEPT DISTINCT
				SELECT 'Sales'
			)");
		// All depts (Eng, Sales, HR) except Sales = 2 distinct
		Assert.Equal("2", result);
	}

	[Fact] public async Task Intersect_Basic()
	{
		var result = await S(@"
			SELECT COUNT(*) FROM (
				SELECT dept FROM `{ds}.employees`
				INTERSECT DISTINCT
				SELECT 'Eng'
			)");
		Assert.Equal("1", result);
	}
}
