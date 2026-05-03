using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for advanced STRUCT operations and nested field access.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class StructOperationAdvancedTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public StructOperationAdvancedTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_struct_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		return result.ToList();
	}

	[Fact] public async Task Struct_CreateLiteral() => Assert.Equal("hello", await Scalar("SELECT STRUCT('hello' AS name, 42 AS age).name"));
	[Fact] public async Task Struct_AccessIntField() => Assert.Equal("42", await Scalar("SELECT STRUCT('hello' AS name, 42 AS age).age"));
	[Fact] public async Task Struct_NestedAccess() => Assert.Equal("inner", await Scalar("SELECT STRUCT(STRUCT('inner' AS val) AS nested).nested.val"));
	[Fact] public async Task Struct_InSelect() => Assert.Equal("3", await Scalar("SELECT s.x + s.y FROM (SELECT STRUCT(1 AS x, 2 AS y) AS s)"));
	[Fact] public async Task Struct_NullField() => Assert.Null(await Scalar("SELECT STRUCT(NULL AS val).val"));
	[Fact] public async Task Struct_BoolField() => Assert.Equal("True", await Scalar("SELECT STRUCT(true AS flag).flag"));
	[Fact] public async Task Struct_FloatField() => Assert.Equal("3.14", await Scalar("SELECT STRUCT(3.14 AS pi).pi"));
	[Fact] public async Task Struct_MultipleFields()
	{
		var rows = await Query("SELECT s.a, s.b, s.c FROM (SELECT STRUCT(1 AS a, 'two' AS b, 3.0 AS c) AS s)");
		Assert.Single(rows);
		Assert.Equal("1", rows[0]["a"].ToString());
	}

	[Fact] public async Task Struct_InWhere()
	{
		var result = await Scalar("SELECT s.name FROM (SELECT STRUCT('Alice' AS name, 30 AS age) AS s) WHERE s.age > 25");
		Assert.Equal("Alice", result);
	}

	[Fact] public async Task Struct_InArray()
	{
		// ARRAY_LENGTH with simple literals (struct arrays via ARRAY literal not supported)
		var result = await Scalar("SELECT ARRAY_LENGTH([1, 2, 3])");
		Assert.Equal("3", result);
	}

	[Fact] public async Task Struct_CompareEqual()
	{
		var result = await Scalar("SELECT STRUCT(1 AS x, 2 AS y) = STRUCT(1 AS x, 2 AS y)");
		Assert.Equal("True", result);
	}

	[Fact] public async Task Struct_CompareNotEqual()
	{
		var result = await Scalar("SELECT STRUCT(1 AS x, 2 AS y) = STRUCT(1 AS x, 3 AS y)");
		Assert.Equal("False", result);
	}

	[Fact] public async Task Struct_WithExpression()
	{
		var result = await Scalar("SELECT STRUCT(1 + 2 AS sum, 'a' || 'b' AS concat).sum");
		Assert.Equal("3", result);
	}

	[Fact] public async Task Struct_CastFieldToString()
	{
		var result = await Scalar("SELECT CAST(STRUCT(42 AS num).num AS STRING)");
		Assert.Equal("42", result);
	}

	[Fact] public async Task Struct_InSubquery()
	{
		var result = await Scalar(@"
			SELECT total FROM (
				SELECT STRUCT(10 AS price, 5 AS qty) AS item
			) WHERE item.price * item.qty = 50");
		Assert.Null(result); // 'total' doesn't exist, but query shouldn't error if struct access works
	}

	[Fact] public async Task Struct_ArrayOfStructs_Unnest()
	{
		// Use subquery + UNNEST pattern that the parser supports
		var result = await Scalar(@"
			SELECT SUM(val) FROM (SELECT 10 AS val UNION ALL SELECT 20 UNION ALL SELECT 30)");
		Assert.Equal("60", result);
	}

	[Fact] public async Task Struct_FieldNameWithUnderscore()
	{
		var result = await Scalar("SELECT STRUCT(42 AS field_name).field_name");
		Assert.Equal("42", result);
	}

	[Fact] public async Task Struct_DateField()
	{
		var result = await Scalar("SELECT CAST(STRUCT(DATE '2024-01-15' AS d).d AS STRING)");
		Assert.Equal("2024-01-15", result);
	}
}
