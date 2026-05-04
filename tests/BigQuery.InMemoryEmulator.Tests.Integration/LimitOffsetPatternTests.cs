using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// LIMIT OFFSET patterns: pagination, with ORDER BY, edge cases, with aggregation, subqueries.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#limit_and_offset_clause
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class LimitOffsetPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public LimitOffsetPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_lop_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.items` (id INT64, name STRING, val INT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.items` VALUES
			(1,'a',10),(2,'b',20),(3,'c',30),(4,'d',40),(5,'e',50),
			(6,'f',60),(7,'g',70),(8,'h',80),(9,'i',90),(10,'j',100),
			(11,'k',110),(12,'l',120),(13,'m',130),(14,'n',140),(15,'o',150)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- LIMIT only ----
	[Fact] public async Task Limit_5()
	{
		var rows = await Q("SELECT id FROM `{ds}.items` ORDER BY id LIMIT 5");
		Assert.Equal(5, rows.Count);
		Assert.Equal("1", rows[0]["id"]?.ToString());
		Assert.Equal("5", rows[4]["id"]?.ToString());
	}
	[Fact] public async Task Limit_1()
	{
		var rows = await Q("SELECT id FROM `{ds}.items` ORDER BY id LIMIT 1");
		Assert.Single(rows);
	}
	[Fact] public async Task Limit_0()
	{
		var rows = await Q("SELECT id FROM `{ds}.items` LIMIT 0");
		Assert.Empty(rows);
	}
	[Fact] public async Task Limit_ExceedsRows()
	{
		var rows = await Q("SELECT id FROM `{ds}.items` LIMIT 100");
		Assert.Equal(15, rows.Count);
	}

	// ---- LIMIT + OFFSET ----
	[Fact] public async Task Offset_5()
	{
		var rows = await Q("SELECT id FROM `{ds}.items` ORDER BY id LIMIT 5 OFFSET 5");
		Assert.Equal(5, rows.Count);
		Assert.Equal("6", rows[0]["id"]?.ToString());
		Assert.Equal("10", rows[4]["id"]?.ToString());
	}
	[Fact] public async Task Offset_10()
	{
		var rows = await Q("SELECT id FROM `{ds}.items` ORDER BY id LIMIT 5 OFFSET 10");
		Assert.Equal(5, rows.Count);
		Assert.Equal("11", rows[0]["id"]?.ToString());
	}
	[Fact] public async Task Offset_BeyondRows()
	{
		var rows = await Q("SELECT id FROM `{ds}.items` ORDER BY id LIMIT 5 OFFSET 20");
		Assert.Empty(rows);
	}
	[Fact] public async Task Offset_Partial()
	{
		var rows = await Q("SELECT id FROM `{ds}.items` ORDER BY id LIMIT 10 OFFSET 10");
		Assert.Equal(5, rows.Count); // only 5 remaining
	}
	[Fact] public async Task Offset_0()
	{
		var rows = await Q("SELECT id FROM `{ds}.items` ORDER BY id LIMIT 5 OFFSET 0");
		Assert.Equal(5, rows.Count);
		Assert.Equal("1", rows[0]["id"]?.ToString());
	}

	// ---- Pagination simulation ----
	[Fact] public async Task Pagination_Page1()
	{
		var rows = await Q("SELECT id, name FROM `{ds}.items` ORDER BY id LIMIT 5 OFFSET 0");
		Assert.Equal(5, rows.Count);
		Assert.Equal("a", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task Pagination_Page2()
	{
		var rows = await Q("SELECT id, name FROM `{ds}.items` ORDER BY id LIMIT 5 OFFSET 5");
		Assert.Equal(5, rows.Count);
		Assert.Equal("f", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task Pagination_Page3()
	{
		var rows = await Q("SELECT id, name FROM `{ds}.items` ORDER BY id LIMIT 5 OFFSET 10");
		Assert.Equal(5, rows.Count);
		Assert.Equal("k", rows[0]["name"]?.ToString());
	}

	// ---- LIMIT with ORDER BY DESC ----
	[Fact] public async Task Limit_OrderByDesc()
	{
		var rows = await Q("SELECT id FROM `{ds}.items` ORDER BY id DESC LIMIT 3");
		Assert.Equal(3, rows.Count);
		Assert.Equal("15", rows[0]["id"]?.ToString());
	}

	// ---- LIMIT with WHERE ----
	[Fact] public async Task Limit_WithWhere()
	{
		var rows = await Q("SELECT id FROM `{ds}.items` WHERE val > 50 ORDER BY id LIMIT 3");
		Assert.Equal(3, rows.Count);
	}

	// ---- LIMIT with aggregation ----
	[Fact] public async Task Limit_WithGroupBy()
	{
		var rows = await Q("SELECT val, COUNT(*) AS cnt FROM `{ds}.items` GROUP BY val ORDER BY val LIMIT 3");
		Assert.Equal(3, rows.Count);
	}

	// ---- LIMIT with DISTINCT ----
	[Fact] public async Task Limit_WithDistinct()
	{
		var rows = await Q("SELECT DISTINCT val FROM `{ds}.items` ORDER BY val LIMIT 5");
		Assert.Equal(5, rows.Count);
	}

	// ---- LIMIT in subquery ----
	[Fact] public async Task Limit_InSubquery()
	{
		var v = await S("SELECT COUNT(*) FROM (SELECT id FROM `{ds}.items` ORDER BY id LIMIT 7)");
		Assert.Equal("7", v);
	}

	// ---- LIMIT with UNION ----
	[Fact] public async Task Limit_WithUnion()
	{
		var rows = await Q("SELECT id FROM `{ds}.items` WHERE id <= 5 UNION ALL SELECT id FROM `{ds}.items` WHERE id > 13 ORDER BY id LIMIT 6");
		Assert.Equal(6, rows.Count);
	}

	// ---- Single row ----
	[Fact] public async Task Limit_SingleRow()
	{
		var v = await S("SELECT name FROM `{ds}.items` ORDER BY val DESC LIMIT 1");
		Assert.Equal("o", v); // highest val = 150
	}

	// ---- LIMIT with expression ORDER BY ----
	[Fact] public async Task Limit_OrderByExpr()
	{
		var rows = await Q("SELECT id, name FROM `{ds}.items` ORDER BY val * -1 LIMIT 3");
		Assert.Equal(3, rows.Count);
	}

	// ---- Top-N ----
	[Fact] public async Task TopN()
	{
		var rows = await Q("SELECT name, val FROM `{ds}.items` ORDER BY val DESC LIMIT 5");
		Assert.Equal(5, rows.Count);
		Assert.Equal("150", rows[0]["val"]?.ToString());
	}

	// ---- Bottom-N ----
	[Fact] public async Task BottomN()
	{
		var rows = await Q("SELECT name, val FROM `{ds}.items` ORDER BY val ASC LIMIT 5");
		Assert.Equal(5, rows.Count);
		Assert.Equal("10", rows[0]["val"]?.ToString());
	}

	// ---- LIMIT with CTE ----
	[Fact] public async Task Limit_WithCte()
	{
		var rows = await Q("WITH top5 AS (SELECT * FROM `{ds}.items` ORDER BY val DESC LIMIT 5) SELECT name FROM top5 ORDER BY name");
		Assert.Equal(5, rows.Count);
	}
}
