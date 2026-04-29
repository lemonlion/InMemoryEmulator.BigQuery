using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// UNNEST, ARRAY operations, STRUCT operations, and complex type handling.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unnest
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class UnnestAndStructTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public UnnestAndStructTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_uns_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
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

	private async Task<string?> Scalar(string sql)
	{
		var rows = await Query(sql);
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- Basic UNNEST ----
	[Fact(Skip = "Not yet supported")] public async Task Unnest_IntegerArray()
	{
		var rows = await Query("SELECT x FROM UNNEST([1, 2, 3]) AS x ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]["x"]?.ToString());
	}

	[Fact(Skip = "Not yet supported")] public async Task Unnest_StringArray()
	{
		var rows = await Query("SELECT s FROM UNNEST(['a', 'b', 'c']) AS s ORDER BY s");
		Assert.Equal(3, rows.Count);
		Assert.Equal("a", rows[0]["s"]?.ToString());
	}

	[Fact(Skip = "Not yet supported")] public async Task Unnest_EmptyArray()
	{
		var rows = await Query("SELECT x FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x");
		Assert.Empty(rows);
	}

	[Fact(Skip = "Not yet supported")] public async Task Unnest_WithOffset()
	{
		var rows = await Query("SELECT x, off FROM UNNEST([10, 20, 30]) AS x WITH OFFSET AS off ORDER BY off");
		Assert.Equal(3, rows.Count);
		Assert.Equal("0", rows[0]["off"]?.ToString());
		Assert.Equal("10", rows[0]["x"]?.ToString());
	}

	// ---- UNNEST with cross join ----
	[Fact(Skip = "Not yet supported")] public async Task Unnest_CrossJoinInline()
	{
		var rows = await Query("SELECT n, v FROM (SELECT 'row' AS n), UNNEST([1, 2, 3]) AS v ORDER BY v");
		Assert.Equal(3, rows.Count);
		Assert.Equal("row", rows[0]["n"]?.ToString());
	}

	// ---- UNNEST from table column ----
	[Fact(Skip = "Not yet supported")] public async Task Unnest_FromTableArrayColumn()
	{
		var client = await _fixture.GetClientAsync();
		await client.CreateTableAsync(_datasetId, "arr_tab", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "tags", Type = "STRING", Mode = "REPEATED" },
			]
		});
		await client.InsertRowsAsync(_datasetId, "arr_tab", new[]
		{
			new BigQueryInsertRow("r1") { ["id"] = 1, ["tags"] = new[] { "red", "blue" } },
			new BigQueryInsertRow("r2") { ["id"] = 2, ["tags"] = new[] { "green" } },
		});
		var rows = await Query($"SELECT id, tag FROM `{_datasetId}.arr_tab`, UNNEST(tags) AS tag ORDER BY id, tag");
		Assert.Equal(3, rows.Count);
	}

	// ---- UNNEST with WHERE ----
	[Fact(Skip = "Not yet supported")] public async Task Unnest_WithWhere()
	{
		var rows = await Query("SELECT x FROM UNNEST([1, 2, 3, 4, 5]) AS x WHERE x > 3 ORDER BY x");
		Assert.Equal(2, rows.Count);
	}

	// ---- UNNEST in subquery ----
	[Fact(Skip = "Not yet supported")] public async Task Unnest_InExists()
	{
		var v = await Scalar("SELECT EXISTS(SELECT 1 FROM UNNEST([5, 10, 15]) AS x WHERE x = 10)");
		Assert.Equal("True", v);
	}

	// ---- STRUCT construction ----
	[Fact] public async Task Struct_Basic()
	{
		var rows = await Query("SELECT STRUCT(1 AS x, 'hello' AS y) AS s");
		Assert.Single(rows);
	}

	[Fact(Skip = "Not yet supported")] public async Task Struct_FieldAccess()
	{
		var v = await Scalar("SELECT (STRUCT(42 AS val)).val");
		Assert.Equal("42", v);
	}

	[Fact(Skip = "Not yet supported")] public async Task Struct_NestedFieldAccess()
	{
		var v = await Scalar("SELECT (STRUCT(STRUCT('inner' AS msg) AS nested)).nested.msg");
		Assert.Equal("inner", v);
	}

	[Fact(Skip = "Not yet supported")] public async Task Struct_InArray()
	{
		var rows = await Query("SELECT s.x, s.y FROM UNNEST([STRUCT(1 AS x, 'a' AS y), STRUCT(2, 'b')]) AS s ORDER BY s.x");
		Assert.Equal(2, rows.Count);
		Assert.Equal("1", rows[0]["x"]?.ToString());
	}

	// ---- ARRAY literal ----
	[Fact] public async Task ArrayLiteral_Length() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH([1, 2, 3])"));
	[Fact] public async Task ArrayLiteral_Empty() => Assert.Equal("0", await Scalar("SELECT ARRAY_LENGTH([])"));
	[Fact] public async Task ArrayLiteral_Strings() => Assert.Equal("2", await Scalar("SELECT ARRAY_LENGTH(['a', 'b'])"));

	// ---- ARRAY subscript: OFFSET, ORDINAL, SAFE_OFFSET, SAFE_ORDINAL ----
	[Fact(Skip = "Not yet supported")] public async Task Array_Offset() => Assert.Equal("b", await Scalar("SELECT ['a', 'b', 'c'][OFFSET(1)]"));
	[Fact(Skip = "Not yet supported")] public async Task Array_Ordinal() => Assert.Equal("a", await Scalar("SELECT ['a', 'b', 'c'][ORDINAL(1)]"));
	[Fact(Skip = "Not yet supported")] public async Task Array_SafeOffset_InBounds() => Assert.Equal("c", await Scalar("SELECT ['a', 'b', 'c'][SAFE_OFFSET(2)]"));
	[Fact(Skip = "Not yet supported")] public async Task Array_SafeOffset_OutOfBounds() => Assert.Null(await Scalar("SELECT ['a', 'b', 'c'][SAFE_OFFSET(10)]"));
	[Fact(Skip = "Not yet supported")] public async Task Array_SafeOrdinal_InBounds() => Assert.Equal("b", await Scalar("SELECT ['a', 'b', 'c'][SAFE_ORDINAL(2)]"));
	[Fact(Skip = "Not yet supported")] public async Task Array_SafeOrdinal_OutOfBounds() => Assert.Null(await Scalar("SELECT ['a', 'b', 'c'][SAFE_ORDINAL(10)]"));

	// ---- GENERATE_ARRAY ----
	[Fact] public async Task GenerateArray_Basic() => Assert.Equal("5", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(1, 5))"));
	[Fact] public async Task GenerateArray_WithStep() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(0, 10, 5))"));
	[Fact] public async Task GenerateArray_Descending() => Assert.Equal("5", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(5, 1, -1))"));

	// ---- ARRAY subquery ----
	[Fact(Skip = "ARRAY subquery format differs")] public async Task ArraySubquery_FromUnnest() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST([10, 20, 30]) AS x))"));
	[Fact(Skip = "ARRAY subquery format differs")] public async Task ArraySubquery_WithFilter() => Assert.Equal("2", await Scalar("SELECT ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST([1, 2, 3, 4, 5]) AS x WHERE x > 3))"));

	// ---- Nested arrays (via STRUCT) ----
	[Fact(Skip = "Not yet supported")] public async Task NestedArraysViaStruct()
	{
		var rows = await Query("SELECT s.items FROM UNNEST([STRUCT([1,2] AS items), STRUCT([3,4])]) AS s");
		Assert.Equal(2, rows.Count);
	}

	// ---- UNNEST + aggregation ----
	[Fact(Skip = "Not yet supported")] public async Task Unnest_WithAggregation()
	{
		var v = await Scalar("SELECT SUM(x) FROM UNNEST([1, 2, 3, 4, 5]) AS x");
		Assert.Equal("15", v);
	}

	[Fact(Skip = "Not yet supported")] public async Task Unnest_CountDistinct()
	{
		var v = await Scalar("SELECT COUNT(DISTINCT x) FROM UNNEST([1, 1, 2, 2, 3]) AS x");
		Assert.Equal("3", v);
	}
}
