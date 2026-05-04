using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// ORDER BY advanced patterns: NULLS FIRST/LAST, expression ORDER BY, multi-column, stable sort, aliased.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#order_by_clause
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class OrderByAdvancedPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public OrderByAdvancedPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_oap_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.t` (id INT64, name STRING, dept STRING, salary INT64, score FLOAT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.t` VALUES
			(1,'Alice','Eng',90000,4.5),
			(2,'Bob','Eng',75000,NULL),
			(3,'Carol','Sales',70000,4.2),
			(4,'Dave','Sales',NULL,3.5),
			(5,'Eve','HR',60000,NULL),
			(6,'Frank','HR',58000,3.2),
			(7,'Grace','Eng',85000,4.7),
			(8,'Hank','Sales',72000,3.9)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- ASC / DESC ----
	[Fact] public async Task OrderBy_Asc()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY name ASC LIMIT 3");
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("Bob", rows[1]["name"]?.ToString());
		Assert.Equal("Carol", rows[2]["name"]?.ToString());
	}
	[Fact] public async Task OrderBy_Desc()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY name DESC LIMIT 3");
		Assert.Equal("Hank", rows[0]["name"]?.ToString());
		Assert.Equal("Grace", rows[1]["name"]?.ToString());
		Assert.Equal("Frank", rows[2]["name"]?.ToString());
	}
	[Fact] public async Task OrderBy_DefaultAsc()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY name LIMIT 1");
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}

	// ---- NULLS FIRST / NULLS LAST ----
	[Fact] public async Task NullsFirst_Asc()
	{
		var rows = await Q("SELECT name, salary FROM `{ds}.t` ORDER BY salary ASC NULLS FIRST");
		Assert.Null(rows[0]["salary"]); // Dave (NULL)
	}
	[Fact] public async Task NullsLast_Asc()
	{
		var rows = await Q("SELECT name, salary FROM `{ds}.t` ORDER BY salary ASC NULLS LAST");
		Assert.Null(rows[rows.Count - 1]["salary"]); // NULL at end
		Assert.Equal("Frank", rows[0]["name"]?.ToString()); // 58000 is lowest
	}
	[Fact] public async Task NullsFirst_Desc()
	{
		var rows = await Q("SELECT name, score FROM `{ds}.t` ORDER BY score DESC NULLS FIRST");
		Assert.Null(rows[0]["score"]); // NULLs first
	}
	[Fact] public async Task NullsLast_Desc()
	{
		var rows = await Q("SELECT name, score FROM `{ds}.t` ORDER BY score DESC NULLS LAST");
		// First should be highest non-null score
		Assert.Equal("Grace", rows[0]["name"]?.ToString()); // 4.7
		Assert.Null(rows[rows.Count - 1]["score"]); // NULL at end
	}

	// ---- Multi-column ORDER BY ----
	[Fact] public async Task MultiCol_DeptThenSalary()
	{
		var rows = await Q("SELECT name, dept, salary FROM `{ds}.t` WHERE salary IS NOT NULL ORDER BY dept ASC, salary DESC");
		// Eng: Alice(90k), Grace(85k), Bob(75k)
		Assert.Equal("Eng", rows[0]["dept"]?.ToString());
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("Grace", rows[1]["name"]?.ToString());
		Assert.Equal("Bob", rows[2]["name"]?.ToString());
	}
	[Fact] public async Task MultiCol_DeptDescSalaryAsc()
	{
		var rows = await Q("SELECT name, dept, salary FROM `{ds}.t` WHERE salary IS NOT NULL ORDER BY dept DESC, salary ASC");
		// Sales first (alphabetically last): Carol(70k), Hank(72k)
		Assert.Equal("Sales", rows[0]["dept"]?.ToString());
		Assert.Equal("Carol", rows[0]["name"]?.ToString());
		Assert.Equal("Hank", rows[1]["name"]?.ToString());
	}

	// ---- Expression ORDER BY ----
	[Fact] public async Task Expr_Length()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY LENGTH(name), name LIMIT 3");
		// 3-char: Bob, Eve
		// 4-char: Dave, Hank
		Assert.Equal("Bob", rows[0]["name"]?.ToString());
		Assert.Equal("Eve", rows[1]["name"]?.ToString());
		Assert.Equal("Dave", rows[2]["name"]?.ToString());
	}
	[Fact] public async Task Expr_Arithmetic()
	{
		var rows = await Q("SELECT name, salary FROM `{ds}.t` WHERE salary IS NOT NULL ORDER BY salary * -1 LIMIT 1");
		Assert.Equal("Alice", rows[0]["name"]?.ToString()); // highest salary * -1 = most negative = first
	}
	[Fact] public async Task Expr_Case()
	{
		var rows = await Q("SELECT name, dept FROM `{ds}.t` ORDER BY CASE dept WHEN 'HR' THEN 1 WHEN 'Sales' THEN 2 ELSE 3 END, name");
		Assert.Equal("HR", rows[0]["dept"]?.ToString()); // HR first per CASE
	}
	[Fact] public async Task Expr_Coalesce()
	{
		var rows = await Q("SELECT name, salary FROM `{ds}.t` ORDER BY COALESCE(salary, 0) ASC LIMIT 1");
		Assert.Equal("Dave", rows[0]["name"]?.ToString()); // NULL→0, lowest
	}

	// ---- ORDER BY alias ----
	[Fact] public async Task Alias_InOrderBy()
	{
		var rows = await Q("SELECT name, salary AS pay FROM `{ds}.t` WHERE salary IS NOT NULL ORDER BY pay DESC LIMIT 1");
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task Alias_Expression()
	{
		var rows = await Q("SELECT name, LENGTH(name) AS name_len FROM `{ds}.t` ORDER BY name_len DESC, name LIMIT 1");
		Assert.Equal("Alice", rows[0]["name"]?.ToString()); // 5 chars, first alphabetically among 5-char names
	}

	// ---- ORDER BY ordinal ----
	[Fact] public async Task Ordinal_First()
	{
		var rows = await Q("SELECT name, salary FROM `{ds}.t` WHERE salary IS NOT NULL ORDER BY 2 DESC LIMIT 1");
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task Ordinal_Second()
	{
		var rows = await Q("SELECT dept, name FROM `{ds}.t` ORDER BY 1, 2 LIMIT 1");
		Assert.Equal("Eng", rows[0]["dept"]?.ToString());
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}

	// ---- ORDER BY with LIMIT/OFFSET ----
	[Fact] public async Task LimitOffset_Page1()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY name LIMIT 3 OFFSET 0");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task LimitOffset_Page2()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY name LIMIT 3 OFFSET 3");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Dave", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task LimitOffset_LastPage()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY name LIMIT 3 OFFSET 6");
		Assert.Equal(2, rows.Count); // only 8 total, 6+2=8
		Assert.Equal("Grace", rows[0]["name"]?.ToString());
		Assert.Equal("Hank", rows[1]["name"]?.ToString());
	}

	// ---- ORDER BY with aggregate ----
	[Fact] public async Task OrderBy_Aggregate()
	{
		var rows = await Q("SELECT dept, COUNT(*) AS cnt FROM `{ds}.t` GROUP BY dept ORDER BY cnt DESC");
		Assert.Equal(3, rows.Count);
		// Eng has 3, Sales has 3, HR has 2 → ties possible
		var firstCount = int.Parse(rows[0]["cnt"]!.ToString()!);
		Assert.True(firstCount >= 3);
	}

	// ---- Deterministic ordering ----
	[Fact] public async Task Deterministic_AllRows()
	{
		var rows = await Q("SELECT id, name FROM `{ds}.t` ORDER BY id");
		Assert.Equal(8, rows.Count);
		for (int i = 0; i < 8; i++)
			Assert.Equal((i + 1).ToString(), rows[i]["id"]?.ToString());
	}

	// ---- ORDER BY with WHERE ----
	[Fact] public async Task WhereAndOrder()
	{
		var rows = await Q("SELECT name, salary FROM `{ds}.t` WHERE dept = 'Eng' AND salary IS NOT NULL ORDER BY salary DESC");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("Grace", rows[1]["name"]?.ToString());
		Assert.Equal("Bob", rows[2]["name"]?.ToString());
	}

	// ---- Stable sort verification ----
	[Fact] public async Task StableSort_SameDept()
	{
		var rows = await Q("SELECT name, dept FROM `{ds}.t` ORDER BY dept, name");
		// Within Eng: Alice, Bob, Grace
		var engRows = rows.Where(r => r["dept"]?.ToString() == "Eng").ToList();
		Assert.Equal("Alice", engRows[0]["name"]?.ToString());
		Assert.Equal("Bob", engRows[1]["name"]?.ToString());
		Assert.Equal("Grace", engRows[2]["name"]?.ToString());
	}
}
