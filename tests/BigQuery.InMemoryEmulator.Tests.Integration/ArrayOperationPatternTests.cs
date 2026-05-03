using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for array operations: ARRAY literals, ARRAY_LENGTH, ARRAY_AGG, GENERATE_ARRAY, array access.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ArrayOperationPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public ArrayOperationPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_arr_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.items` (id INT64, name STRING, tags ARRAY<STRING>, scores ARRAY<INT64>)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.items` (id, name, tags, scores) VALUES
			(1, 'Alice', ['smart', 'kind', 'brave'], [85, 90, 95]),
			(2, 'Bob', ['strong', 'fast'], [70, 80, 75]),
			(3, 'Carol', ['wise'], [100]),
			(4, 'Dave', [], [])", parameters: null);
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

	// ARRAY_LENGTH
	[Fact] public async Task ArrayLength_Literal() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH([1, 2, 3])"));
	[Fact] public async Task ArrayLength_Empty() => Assert.Equal("0", await Scalar("SELECT ARRAY_LENGTH([])"));
	[Fact] public async Task ArrayLength_FromTable()
	{
		var rows = await Query("SELECT name, ARRAY_LENGTH(tags) AS tag_count FROM `{ds}.items` ORDER BY name");
		Assert.Equal("3", rows[0]["tag_count"].ToString()); // Alice: 3 tags
		Assert.Equal("0", rows[3]["tag_count"].ToString()); // Dave: 0 tags
	}

	// GENERATE_ARRAY
	[Fact] public async Task GenerateArray_Basic() => Assert.Equal("5", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(1, 5))"));
	[Fact] public async Task GenerateArray_Step() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(0, 10, 5))")); // 0,5,10
	[Fact] public async Task GenerateArray_Empty() => Assert.Equal("0", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(10, 5))"));

	// Array subscript (OFFSET and ORDINAL)
	[Fact] public async Task Array_Offset_Zero() => Assert.Equal("1", await Scalar("SELECT [1,2,3][OFFSET(0)]"));
	[Fact] public async Task Array_Offset_Last() => Assert.Equal("3", await Scalar("SELECT [1,2,3][OFFSET(2)]"));
	[Fact] public async Task Array_Ordinal_First() => Assert.Equal("1", await Scalar("SELECT [1,2,3][ORDINAL(1)]"));
	[Fact] public async Task Array_Ordinal_Last() => Assert.Equal("3", await Scalar("SELECT [1,2,3][ORDINAL(3)]"));
	[Fact] public async Task Array_SafeOffset_OutOfBounds() => Assert.Null(await Scalar("SELECT [1,2,3][SAFE_OFFSET(10)]"));
	[Fact] public async Task Array_SafeOrdinal_OutOfBounds() => Assert.Null(await Scalar("SELECT [1,2,3][SAFE_ORDINAL(10)]"));

	// ARRAY_CONCAT
	[Fact] public async Task ArrayConcat_TwoArrays() => Assert.Equal("5", await Scalar("SELECT ARRAY_LENGTH(ARRAY_CONCAT([1,2], [3,4,5]))"));
	[Fact] public async Task ArrayConcat_WithEmpty() => Assert.Equal("3", await Scalar("SELECT ARRAY_LENGTH(ARRAY_CONCAT([1,2,3], []))"));

	// ARRAY_REVERSE
	[Fact] public async Task ArrayReverse() => Assert.Equal("3", await Scalar("SELECT ARRAY_REVERSE([1,2,3])[OFFSET(0)]"));

	// ARRAY_TO_STRING
	[Fact] public async Task ArrayToString_Basic() => Assert.Equal("a,b,c", await Scalar("SELECT ARRAY_TO_STRING(['a', 'b', 'c'], ',')"));
	[Fact] public async Task ArrayToString_Delimiter() => Assert.Equal("a-b-c", await Scalar("SELECT ARRAY_TO_STRING(['a', 'b', 'c'], '-')"));

	// UNNEST with ARRAY_AGG
	[Fact] public async Task Unnest_Count()
	{
		var rows = await Query("SELECT name, COUNT(t) AS tc FROM `{ds}.items`, UNNEST(tags) AS t GROUP BY name ORDER BY name");
		Assert.Equal("3", rows[0]["tc"].ToString()); // Alice: 3 tags
	}

	[Fact] public async Task Unnest_WithOffset()
	{
		var rows = await Query("SELECT t, off FROM UNNEST(['x', 'y', 'z']) AS t WITH OFFSET AS off ORDER BY off");
		Assert.Equal("0", rows[0]["off"].ToString());
		Assert.Equal("x", rows[0]["t"].ToString());
	}

	// ARRAY in WHERE (using UNNEST + EXISTS)
	[Fact] public async Task Array_ExistsPattern()
	{
		var rows = await Query("SELECT name FROM `{ds}.items` WHERE EXISTS(SELECT 1 FROM UNNEST(tags) AS t WHERE t = 'smart')");
		Assert.Single(rows);
		Assert.Equal("Alice", rows[0]["name"].ToString());
	}

	// Nested array operations
	[Fact] public async Task Array_AggFromTable()
	{
		var result = await Scalar("SELECT ARRAY_LENGTH(ARRAY_AGG(name)) FROM `{ds}.items`");
		Assert.Equal("4", result);
	}

	// ARRAY with subquery
	[Fact] public async Task Array_SubqueryPattern()
	{
		var result = await Scalar("SELECT ARRAY_LENGTH(ARRAY(SELECT name FROM `{ds}.items` WHERE id <= 3))");
		Assert.Equal("3", result);
	}

	// ARRAY_AGG with GROUP BY
	[Fact] public async Task ArrayAgg_PerGroup()
	{
		var rows = await Query(@"
			SELECT ARRAY_LENGTH(ARRAY_AGG(name)) AS cnt
			FROM `{ds}.items`
			WHERE ARRAY_LENGTH(tags) > 0
			GROUP BY ARRAY_LENGTH(tags)
			ORDER BY cnt");
		Assert.True(rows.Count >= 2); // groups by tag count: 1, 2, 3
	}

	// NULL in arrays
	[Fact] public async Task ArrayLength_Null() => Assert.Null(await Scalar("SELECT ARRAY_LENGTH(NULL)"));
}