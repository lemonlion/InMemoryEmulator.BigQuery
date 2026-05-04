using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// UNION, INTERSECT, EXCEPT set operation patterns.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class SetOperationPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public SetOperationPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_sop_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.a` (id INT64, name STRING)", parameters: null);
		await c.ExecuteQueryAsync($"INSERT INTO `{_ds}.a` VALUES (1,'Alice'),(2,'Bob'),(3,'Carol'),(4,'Dave')", parameters: null);
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.b` (id INT64, name STRING)", parameters: null);
		await c.ExecuteQueryAsync($"INSERT INTO `{_ds}.b` VALUES (3,'Carol'),(4,'Dave'),(5,'Eve'),(6,'Frank')", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- UNION ALL ----
	[Fact] public async Task UnionAll_Basic()
	{
		var rows = await Q("SELECT name FROM `{ds}.a` UNION ALL SELECT name FROM `{ds}.b`");
		Assert.Equal(8, rows.Count); // 4 + 4
	}
	[Fact] public async Task UnionAll_WithOrderBy()
	{
		var rows = await Q("SELECT name FROM `{ds}.a` UNION ALL SELECT name FROM `{ds}.b` ORDER BY name");
		Assert.Equal(8, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}

	// ---- UNION DISTINCT ----
	[Fact] public async Task UnionDistinct()
	{
		var rows = await Q("SELECT name FROM `{ds}.a` UNION DISTINCT SELECT name FROM `{ds}.b` ORDER BY name");
		Assert.Equal(6, rows.Count); // Alice, Bob, Carol, Dave, Eve, Frank
	}

	// ---- INTERSECT DISTINCT ----
	[Fact] public async Task IntersectDistinct()
	{
		var rows = await Q("SELECT name FROM `{ds}.a` INTERSECT DISTINCT SELECT name FROM `{ds}.b` ORDER BY name");
		Assert.Equal(2, rows.Count); // Carol, Dave
	}

	// ---- EXCEPT DISTINCT ----
	[Fact] public async Task ExceptDistinct()
	{
		var rows = await Q("SELECT name FROM `{ds}.a` EXCEPT DISTINCT SELECT name FROM `{ds}.b` ORDER BY name");
		Assert.Equal(2, rows.Count); // Alice, Bob
	}
	[Fact] public async Task ExceptDistinct_Reverse()
	{
		var rows = await Q("SELECT name FROM `{ds}.b` EXCEPT DISTINCT SELECT name FROM `{ds}.a` ORDER BY name");
		Assert.Equal(2, rows.Count); // Eve, Frank
	}

	// ---- Multiple UNION ALL ----
	[Fact] public async Task MultiUnionAll()
	{
		var rows = await Q(@"
			SELECT name FROM `{ds}.a`
			UNION ALL SELECT name FROM `{ds}.b`
			UNION ALL SELECT name FROM `{ds}.a`
			ORDER BY name");
		Assert.Equal(12, rows.Count); // 4 + 4 + 4
	}

	// ---- UNION with different column counts (same schema) ----
	[Fact] public async Task Union_WithColumns()
	{
		var rows = await Q("SELECT id, name FROM `{ds}.a` UNION ALL SELECT id, name FROM `{ds}.b` ORDER BY id");
		Assert.Equal(8, rows.Count);
	}

	// ---- UNION with WHERE ----
	[Fact] public async Task Union_WithWhere()
	{
		var rows = await Q(@"
			SELECT name FROM `{ds}.a` WHERE id <= 2
			UNION ALL
			SELECT name FROM `{ds}.b` WHERE id >= 5
			ORDER BY name");
		Assert.Equal(4, rows.Count); // Alice, Bob, Eve, Frank
	}

	// ---- UNION with aggregate ----
	[Fact] public async Task Union_ThenCount()
	{
		var v = await S("SELECT COUNT(*) FROM (SELECT name FROM `{ds}.a` UNION ALL SELECT name FROM `{ds}.b`)");
		Assert.Equal("8", v);
	}
	[Fact] public async Task Union_ThenDistinctCount()
	{
		var v = await S("SELECT COUNT(DISTINCT name) FROM (SELECT name FROM `{ds}.a` UNION ALL SELECT name FROM `{ds}.b`)");
		Assert.Equal("6", v);
	}

	// ---- UNION with LIMIT ----
	[Fact] public async Task Union_WithLimit()
	{
		var rows = await Q("SELECT name FROM `{ds}.a` UNION ALL SELECT name FROM `{ds}.b` ORDER BY name LIMIT 3");
		Assert.Equal(3, rows.Count);
	}

	// ---- INTERSECT ALL ----
	[Fact] public async Task IntersectAll()
	{
		var rows = await Q("SELECT name FROM `{ds}.a` INTERSECT ALL SELECT name FROM `{ds}.b` ORDER BY name");
		Assert.Equal(2, rows.Count); // Carol, Dave (each once in both)
	}

	// ---- EXCEPT ALL ----
	[Fact] public async Task ExceptAll()
	{
		var rows = await Q("SELECT name FROM `{ds}.a` EXCEPT ALL SELECT name FROM `{ds}.b` ORDER BY name");
		Assert.Equal(2, rows.Count); // Alice, Bob
	}

	// ---- UNION with constants ----
	[Fact] public async Task Union_Constants()
	{
		var rows = await Q("SELECT 1 AS n, 'a' AS s UNION ALL SELECT 2, 'b' UNION ALL SELECT 3, 'c' ORDER BY n");
		Assert.Equal(3, rows.Count);
	}

	// ---- UNION in CTE ----
	[Fact] public async Task Union_InCte()
	{
		var rows = await Q(@"
			WITH combined AS (
				SELECT name FROM `{ds}.a`
				UNION ALL
				SELECT name FROM `{ds}.b`
			)
			SELECT DISTINCT name FROM combined ORDER BY name");
		Assert.Equal(6, rows.Count);
	}

	// ---- EXCEPT then UNION ----
	[Fact] public async Task Except_ThenUnion()
	{
		var rows = await Q(@"
			SELECT name FROM (
				SELECT name FROM `{ds}.a` EXCEPT DISTINCT SELECT name FROM `{ds}.b`
			)
			UNION ALL
			SELECT name FROM (
				SELECT name FROM `{ds}.b` EXCEPT DISTINCT SELECT name FROM `{ds}.a`
			)
			ORDER BY name");
		Assert.Equal(4, rows.Count); // Alice, Bob, Eve, Frank
	}
}
