using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for UNION ALL, UNION DISTINCT, INTERSECT, EXCEPT operations.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class SetOperationComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public SetOperationComprehensiveTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_so_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.a` (id INT64, name STRING)", parameters: null);
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.b` (id INT64, name STRING)", parameters: null);
		await c.ExecuteQueryAsync($"INSERT INTO `{_ds}.a` VALUES (1,'Alice'),(2,'Bob'),(3,'Carol'),(4,'Dave')", parameters: null);
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
		var rows = await Q("SELECT id, name FROM `{ds}.a` UNION ALL SELECT id, name FROM `{ds}.b`");
		Assert.Equal(8, rows.Count); // 4 + 4
	}
	[Fact] public async Task UnionAll_WithDuplicates()
	{
		var v = await S("SELECT COUNT(*) FROM (SELECT id FROM `{ds}.a` UNION ALL SELECT id FROM `{ds}.b`)");
		Assert.Equal("8", v);
	}
	[Fact] public async Task UnionAll_WithOrderBy()
	{
		var rows = await Q("SELECT id, name FROM `{ds}.a` UNION ALL SELECT id, name FROM `{ds}.b` ORDER BY id");
		Assert.Equal(8, rows.Count);
		Assert.Equal("1", rows[0]["id"]?.ToString());
	}
	[Fact] public async Task UnionAll_WithLimit()
	{
		var rows = await Q("SELECT id FROM `{ds}.a` UNION ALL SELECT id FROM `{ds}.b` ORDER BY id LIMIT 3");
		Assert.Equal(3, rows.Count);
	}
	[Fact] public async Task UnionAll_ThreeSets()
	{
		var rows = await Q("SELECT id FROM `{ds}.a` UNION ALL SELECT id FROM `{ds}.b` UNION ALL SELECT id FROM `{ds}.a`");
		Assert.Equal(12, rows.Count); // 4 + 4 + 4
	}

	// ---- UNION DISTINCT ----
	[Fact] public async Task UnionDistinct_Basic()
	{
		var rows = await Q("SELECT id, name FROM `{ds}.a` UNION DISTINCT SELECT id, name FROM `{ds}.b` ORDER BY id");
		Assert.Equal(6, rows.Count); // 6 unique: 1-6
	}
	[Fact] public async Task UnionDistinct_WithOrderBy()
	{
		var rows = await Q("SELECT id, name FROM `{ds}.a` UNION DISTINCT SELECT id, name FROM `{ds}.b` ORDER BY name");
		Assert.Equal(6, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
	}

	// ---- INTERSECT DISTINCT ----
	[Fact] public async Task IntersectDistinct_Basic()
	{
		var rows = await Q("SELECT id, name FROM `{ds}.a` INTERSECT DISTINCT SELECT id, name FROM `{ds}.b` ORDER BY id");
		Assert.Equal(2, rows.Count); // Carol(3), Dave(4)
	}
	[Fact] public async Task IntersectDistinct_Empty()
	{
		var rows = await Q("SELECT id, name FROM `{ds}.a` WHERE id <= 2 INTERSECT DISTINCT SELECT id, name FROM `{ds}.b` WHERE id >= 5");
		Assert.Empty(rows);
	}

	// ---- EXCEPT DISTINCT ----
	[Fact] public async Task ExceptDistinct_AMinusB()
	{
		var rows = await Q("SELECT id, name FROM `{ds}.a` EXCEPT DISTINCT SELECT id, name FROM `{ds}.b` ORDER BY id");
		Assert.Equal(2, rows.Count); // Alice(1), Bob(2)
	}
	[Fact] public async Task ExceptDistinct_BMinusA()
	{
		var rows = await Q("SELECT id, name FROM `{ds}.b` EXCEPT DISTINCT SELECT id, name FROM `{ds}.a` ORDER BY id");
		Assert.Equal(2, rows.Count); // Eve(5), Frank(6)
	}

	// ---- UNION ALL with expressions ----
	[Fact] public async Task UnionAll_WithExpression()
	{
		var rows = await Q(@"
			SELECT name, 'table_a' AS source FROM `{ds}.a`
			UNION ALL
			SELECT name, 'table_b' AS source FROM `{ds}.b`
			ORDER BY name, source");
		Assert.Equal(8, rows.Count);
	}

	// ---- UNION with aggregation ----
	[Fact] public async Task UnionAll_ThenAggregate()
	{
		var v = await S(@"
			SELECT COUNT(DISTINCT id) FROM (
				SELECT id FROM `{ds}.a`
				UNION ALL
				SELECT id FROM `{ds}.b`
			)");
		Assert.Equal("6", v);
	}

	// ---- UNION with different WHERE ----
	[Fact] public async Task UnionAll_DifferentFilters()
	{
		var rows = await Q(@"
			SELECT id, name FROM `{ds}.a` WHERE id <= 2
			UNION ALL
			SELECT id, name FROM `{ds}.b` WHERE id >= 5
			ORDER BY id");
		Assert.Equal(4, rows.Count);
	}

	// ---- UNION ALL with literals ----
	[Fact] public async Task UnionAll_Literals()
	{
		var rows = await Q("SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]["x"]?.ToString());
	}
	[Fact] public async Task UnionAll_StringLiterals()
	{
		var rows = await Q("SELECT 'a' AS s UNION ALL SELECT 'b' UNION ALL SELECT 'c' ORDER BY s");
		Assert.Equal(3, rows.Count);
	}

	// ---- INTERSECT ALL ----
	[Fact] public async Task IntersectAll_Basic()
	{
		var rows = await Q("SELECT id, name FROM `{ds}.a` INTERSECT ALL SELECT id, name FROM `{ds}.b` ORDER BY id");
		Assert.Equal(2, rows.Count);
	}

	// ---- EXCEPT ALL ----
	[Fact] public async Task ExceptAll_Basic()
	{
		var rows = await Q("SELECT id, name FROM `{ds}.a` EXCEPT ALL SELECT id, name FROM `{ds}.b` ORDER BY id");
		Assert.Equal(2, rows.Count);
	}

	// ---- Complex set operations ----
	[Fact] public async Task Complex_UnionThenIntersect()
	{
		// Parenthesized set operations not supported; use subquery wrapping instead
		var rows = await Q(@"
			SELECT id FROM (
				SELECT id FROM `{ds}.a` UNION ALL SELECT id FROM `{ds}.b`
			) INTERSECT DISTINCT
			SELECT id FROM `{ds}.a`
			ORDER BY id");
		Assert.Equal(4, rows.Count); // IDs 1-4 are in a
	}

	// ---- UNION ALL + subquery ----
	[Fact] public async Task UnionAll_InSubquery()
	{
		var rows = await Q(@"
			SELECT * FROM (
				SELECT id, name FROM `{ds}.a`
				UNION ALL
				SELECT id, name FROM `{ds}.b`
			) WHERE id > 3 ORDER BY id");
		Assert.True(rows.Count >= 4);
	}

	// ---- UNION with GROUP BY ----
	[Fact] public async Task UnionAll_ThenGroupBy()
	{
		var rows = await Q(@"
			SELECT id, COUNT(*) AS cnt FROM (
				SELECT id FROM `{ds}.a`
				UNION ALL
				SELECT id FROM `{ds}.b`
			) GROUP BY id ORDER BY id");
		Assert.Equal(6, rows.Count);
		// IDs 3 and 4 appear twice
		var id3 = rows.First(r => r["id"]?.ToString() == "3");
		Assert.Equal("2", id3["cnt"]?.ToString());
	}
}
