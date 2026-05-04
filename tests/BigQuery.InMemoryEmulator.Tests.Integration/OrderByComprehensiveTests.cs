using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for ORDER BY, LIMIT, OFFSET patterns with various data types and expressions.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#order_by_clause
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class OrderByComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public OrderByComprehensiveTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_ob_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.t` (id INT64, name STRING, val FLOAT64, grp STRING, dt STRING)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.t` VALUES
			(1,'Alice',30,'A','2024-01-15'),(2,'Bob',20,'B','2024-01-10'),
			(3,'Carol',30,'A','2024-01-20'),(4,'Dave',10,'B','2024-01-05'),
			(5,'Eve',50,'C','2024-01-25'),(6,'Frank',NULL,'C','2024-01-01'),
			(7,'Grace',20,'A','2024-01-30'),(8,'Hank',40,'B','2024-02-01')", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- Single column ASC/DESC ----
	[Fact] public async Task OrderBy_AscDefault()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY id");
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("Hank", rows[7]["name"]?.ToString());
	}
	[Fact] public async Task OrderBy_AscExplicit()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY id ASC");
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task OrderBy_Desc()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY id DESC");
		Assert.Equal("Hank", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task OrderBy_String()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY name");
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task OrderBy_StringDesc()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY name DESC");
		Assert.Equal("Hank", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task OrderBy_Float()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` WHERE val IS NOT NULL ORDER BY val");
		Assert.Equal("Dave", rows[0]["name"]?.ToString()); // 10
	}

	// ---- Multi-column ORDER BY ----
	[Fact] public async Task OrderBy_TwoColumns()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY grp, name");
		Assert.Equal("Alice", rows[0]["name"]?.ToString()); // grp A, Alice
	}
	[Fact] public async Task OrderBy_MixedDirection()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY grp ASC, val DESC");
		Assert.True(rows.Count == 8);
	}
	[Fact] public async Task OrderBy_ThreeColumns()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY grp, val, name");
		Assert.Equal(8, rows.Count);
	}

	// ---- NULL ordering ----
	[Fact] public async Task OrderBy_NullsFirst()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY val ASC");
		Assert.Equal("Frank", rows[0]["name"]?.ToString()); // NULL first in ASC
	}
	[Fact] public async Task OrderBy_NullsLast()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY val DESC");
		Assert.Equal("Eve", rows[0]["name"]?.ToString()); // 50 first
	}

	// ---- ORDER BY with expression ----
	[Fact] public async Task OrderBy_Expr()
	{
		var rows = await Q("SELECT name, val FROM `{ds}.t` WHERE val IS NOT NULL ORDER BY val * -1");
		Assert.Equal("Eve", rows[0]["name"]?.ToString()); // -50 first
	}
	[Fact] public async Task OrderBy_Function()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY LENGTH(name) DESC, name");
		Assert.True(rows[0]["name"]?.ToString()?.Length >= 5); // longest names first
	}

	// ---- ORDER BY with alias ----
	[Fact] public async Task OrderBy_Alias()
	{
		var rows = await Q("SELECT name, val AS value FROM `{ds}.t` WHERE val IS NOT NULL ORDER BY value");
		Assert.Equal("Dave", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task OrderBy_ColumnNumber()
	{
		var rows = await Q("SELECT name, val FROM `{ds}.t` WHERE val IS NOT NULL ORDER BY 2");
		Assert.Equal("Dave", rows[0]["name"]?.ToString()); // ordered by 2nd column (val)
	}

	// ---- LIMIT ----
	[Fact] public async Task Limit_Basic()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY id LIMIT 3");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task Limit_One()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY val DESC LIMIT 1");
		Assert.Single(rows);
		Assert.Equal("Eve", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task Limit_Zero()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY id LIMIT 0");
		Assert.Empty(rows);
	}
	[Fact] public async Task Limit_GreaterThanRows()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY id LIMIT 100");
		Assert.Equal(8, rows.Count);
	}

	// ---- OFFSET ----
	[Fact] public async Task Offset_Basic()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY id LIMIT 3 OFFSET 2");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Carol", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task Offset_Skip()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY id LIMIT 2 OFFSET 6");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Grace", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task Offset_BeyondRows()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY id LIMIT 10 OFFSET 100");
		Assert.Empty(rows);
	}

	// ---- Pagination pattern ----
	[Fact] public async Task Pagination_Page1()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY id LIMIT 3 OFFSET 0");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task Pagination_Page2()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY id LIMIT 3 OFFSET 3");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Dave", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task Pagination_Page3()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY id LIMIT 3 OFFSET 6");
		Assert.Equal(2, rows.Count); // only 2 remaining
		Assert.Equal("Grace", rows[0]["name"]?.ToString());
	}

	// ---- ORDER BY with DISTINCT ----
	[Fact] public async Task OrderBy_Distinct()
	{
		var rows = await Q("SELECT DISTINCT grp FROM `{ds}.t` ORDER BY grp");
		Assert.Equal(3, rows.Count);
		Assert.Equal("A", rows[0]["grp"]?.ToString());
	}

	// ---- ORDER BY with GROUP BY ----
	[Fact] public async Task OrderBy_GroupBy()
	{
		var rows = await Q("SELECT grp, COUNT(*) AS cnt FROM `{ds}.t` GROUP BY grp ORDER BY cnt DESC");
		Assert.Equal(3, rows.Count);
		Assert.True(int.Parse(rows[0]["cnt"]?.ToString() ?? "0") >= int.Parse(rows[2]["cnt"]?.ToString() ?? "0"));
	}

	// ---- ORDER BY with WHERE ----
	[Fact] public async Task OrderBy_WithWhere()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` WHERE grp = 'A' ORDER BY name");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}

	// ---- ORDER BY with CASE ----
	[Fact] public async Task OrderBy_Case()
	{
		var rows = await Q("SELECT name FROM `{ds}.t` ORDER BY CASE WHEN grp = 'C' THEN 0 WHEN grp = 'A' THEN 1 ELSE 2 END, name");
		Assert.True(rows[0]["name"]?.ToString() == "Eve" || rows[0]["name"]?.ToString() == "Frank"); // C group first
	}

	// ---- ORDER BY with COALESCE ----
	[Fact] public async Task OrderBy_Coalesce()
	{
		var rows = await Q("SELECT name, COALESCE(val, 0) AS safe_val FROM `{ds}.t` ORDER BY COALESCE(val, 0)");
		Assert.Equal("Frank", rows[0]["name"]?.ToString()); // NULL → 0
		Assert.Equal("Dave", rows[1]["name"]?.ToString()); // 10
	}
}
