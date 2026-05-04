using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for STRUCT construction, access, and operations.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class StructOperationTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public StructOperationTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql, parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Query(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql, parameters: null); return r.ToList(); }

	// ---- STRUCT construction ----
	[Fact] public async Task Struct_NamedFields() => Assert.Equal("1", await Scalar("SELECT s.x FROM (SELECT STRUCT(1 AS x, 'hello' AS y) AS s)"));
	[Fact] public async Task Struct_FieldAccess() => Assert.Equal("1", await Scalar("SELECT s.x FROM (SELECT STRUCT(1 AS x, 2 AS y) AS s)"));
	[Fact] public async Task Struct_FieldAccess_String() => Assert.Equal("hello", await Scalar("SELECT s.y FROM (SELECT STRUCT(1 AS x, 'hello' AS y) AS s)"));
	[Fact] public async Task Struct_NestedAccess() => Assert.Equal("3", await Scalar("SELECT s.inner_s.z FROM (SELECT STRUCT(STRUCT(3 AS z) AS inner_s) AS s)"));
	[Fact] public async Task Struct_InSelect() { var v = await Scalar("SELECT s.a FROM (SELECT STRUCT(10 AS a, 20 AS b) AS s)"); Assert.Equal("10", v); }

	// ---- STRUCT comparison ----
	[Fact] public async Task Struct_Equality() => Assert.Equal("True", await Scalar("SELECT STRUCT(1, 2) = STRUCT(1, 2)"));
	[Fact] public async Task Struct_Inequality() => Assert.Equal("True", await Scalar("SELECT STRUCT(1, 2) != STRUCT(1, 3)"));

	// ---- STRUCT with NULL fields ----
	[Fact] public async Task Struct_NullField() => Assert.Equal("1", await Scalar("SELECT s.x FROM (SELECT STRUCT(1 AS x, NULL AS y) AS s)"));

	// ---- STRUCT in UNNEST ----
	[Fact] public async Task Struct_Unnest()
	{
		var rows = await Query("SELECT s.x, s.y FROM UNNEST([STRUCT(1 AS x, 'a' AS y), STRUCT(2, 'b'), STRUCT(3, 'c')]) AS s ORDER BY s.x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]["x"]?.ToString());
		Assert.Equal("c", rows[2]["y"]?.ToString());
	}

	// ---- Multiple struct fields ----
	[Fact] public async Task Struct_MultipleFields()
	{
		var v = await Scalar("SELECT s.c FROM (SELECT STRUCT(1 AS a, 2 AS b, 3 AS c, 4 AS d) AS s)");
		Assert.Equal("3", v);
	}

	// ---- STRUCT with different data types ----
	[Fact] public async Task Struct_MixedTypes()
	{
		var v = await Scalar("SELECT s.b FROM (SELECT STRUCT(1 AS a, 3.14 AS b, true AS c, 'hello' AS d) AS s)");
		Assert.Equal("3.14", v);
	}

	// ---- Array of structs ----
	[Fact] public async Task Struct_ArrayOfStructs()
	{
		var rows = await Query("SELECT item.name, item.value FROM UNNEST([STRUCT('a' AS name, 1 AS value), STRUCT('b', 2), STRUCT('c', 3)]) AS item ORDER BY item.value");
		Assert.Equal(3, rows.Count);
		Assert.Equal("a", rows[0]["name"]?.ToString());
		Assert.Equal("3", rows[2]["value"]?.ToString());
	}

	// ---- STRUCT field in WHERE ----
	[Fact] public async Task Struct_WhereOnField()
	{
		var rows = await Query("SELECT s.x FROM UNNEST([STRUCT(1 AS x, 'a' AS y), STRUCT(2, 'b'), STRUCT(3, 'c')]) AS s WHERE s.x > 1 ORDER BY s.x");
		Assert.Equal(2, rows.Count);
		Assert.Equal("2", rows[0]["x"]?.ToString());
	}

	// ---- STRUCT field in ORDER BY ----
	[Fact] public async Task Struct_OrderByField()
	{
		var rows = await Query("SELECT s.x, s.y FROM UNNEST([STRUCT(3 AS x, 'c' AS y), STRUCT(1, 'a'), STRUCT(2, 'b')]) AS s ORDER BY s.x");
		Assert.Equal("a", rows[0]["y"]?.ToString());
		Assert.Equal("c", rows[2]["y"]?.ToString());
	}

	// ---- STRUCT field in GROUP BY ----
	[Fact] public async Task Struct_GroupByField()
	{
		var rows = await Query("SELECT s.cat, COUNT(*) AS cnt FROM UNNEST([STRUCT('A' AS cat, 1 AS val), STRUCT('B', 2), STRUCT('A', 3)]) AS s GROUP BY s.cat ORDER BY s.cat");
		Assert.Equal(2, rows.Count);
		Assert.Equal("2", rows[0]["cnt"]?.ToString()); // A: 2 items
		Assert.Equal("1", rows[1]["cnt"]?.ToString()); // B: 1 item
	}

	// ---- STRUCT aggregate on field ----
	[Fact] public async Task Struct_AggregateOnField()
	{
		var v = await Scalar("SELECT SUM(s.val) FROM UNNEST([STRUCT(1 AS val), STRUCT(2), STRUCT(3)]) AS s");
		Assert.Equal("6", v);
	}

	// ---- Struct without named fields ----
	[Fact] public async Task Struct_Unnamed()
	{
		// Test unnamed struct fields accessed via generated _field_N names
		var v = await Scalar("SELECT s._field_1 FROM (SELECT STRUCT(1, 'hello', true) AS s)");
		Assert.NotNull(v); // Verifies unnamed struct field access works
	}

	// ---- Struct field with reserved name ----
	[Fact] public async Task Struct_ReservedFieldName()
	{
		var v = await Scalar("SELECT s.`select` FROM (SELECT STRUCT(42 AS `select`) AS s)");
		Assert.Equal("42", v);
	}
}
