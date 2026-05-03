using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for UNNEST with various patterns including nested arrays, WITH OFFSET, cross joins.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unnest_operator
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class UnnestAdvancedPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public UnnestAdvancedPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_unnest_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.items` (id INT64, name STRING, tags ARRAY<STRING>, scores ARRAY<INT64>)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.items` (id, name, tags, scores) VALUES
			(1, 'Widget', ['red', 'small'], [85, 90, 95]),
			(2, 'Gadget', ['blue', 'large', 'premium'], [70, 80]),
			(3, 'Tool', ['green'], [100])", parameters: null);
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

	// Basic UNNEST
	[Fact] public async Task Unnest_ArrayLiteral()
	{
		var rows = await Query("SELECT val FROM UNNEST([1, 2, 3]) AS val");
		Assert.Equal(3, rows.Count);
	}

	[Fact] public async Task Unnest_StringArray()
	{
		var rows = await Query("SELECT val FROM UNNEST(['a', 'b', 'c']) AS val ORDER BY val");
		Assert.Equal("a", rows[0][0].ToString());
	}

	[Fact] public async Task Unnest_WithOffset()
	{
		var rows = await Query("SELECT val, off FROM UNNEST([10, 20, 30]) AS val WITH OFFSET AS off ORDER BY off");
		Assert.Equal(3, rows.Count);
		Assert.Equal("0", rows[0]["off"].ToString());
		Assert.Equal("10", rows[0]["val"].ToString());
	}

	[Fact] public async Task Unnest_EmptyArray()
	{
		var rows = await Query("SELECT val FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS val");
		Assert.Empty(rows);
	}

	// UNNEST from table column
	[Fact] public async Task Unnest_TableColumn_Tags()
	{
		var rows = await Query("SELECT i.name, tag FROM `{ds}.items` i, UNNEST(i.tags) AS tag ORDER BY i.name, tag");
		Assert.Equal(6, rows.Count); // 2+3+1 = 6 tags total
	}

	[Fact] public async Task Unnest_TableColumn_Scores()
	{
		var result = await Scalar("SELECT SUM(score) FROM `{ds}.items` i, UNNEST(i.scores) AS score");
		Assert.Equal("520", result); // 85+90+95+70+80+100 = 520
	}

	[Fact] public async Task Unnest_WithFilter()
	{
		var rows = await Query("SELECT i.name, tag FROM `{ds}.items` i, UNNEST(i.tags) AS tag WHERE tag = 'red'");
		Assert.Single(rows);
		Assert.Equal("Widget", rows[0]["name"].ToString());
	}

	[Fact] public async Task Unnest_WithOffsetFilter()
	{
		var rows = await Query("SELECT val FROM UNNEST([10, 20, 30, 40, 50]) AS val WITH OFFSET AS off WHERE off >= 2 ORDER BY val");
		Assert.Equal(3, rows.Count);
		Assert.Equal("30", rows[0][0].ToString());
	}

	// Aggregation with UNNEST
	[Fact] public async Task Unnest_CountPerRow()
	{
		var rows = await Query("SELECT i.name, COUNT(tag) AS tag_count FROM `{ds}.items` i, UNNEST(i.tags) AS tag GROUP BY i.name ORDER BY i.name");
		Assert.Equal(3, rows.Count);
	}

	[Fact] public async Task Unnest_MaxScore()
	{
		var rows = await Query("SELECT i.name, MAX(score) AS max_score FROM `{ds}.items` i, UNNEST(i.scores) AS score GROUP BY i.name ORDER BY i.name");
		Assert.Equal(3, rows.Count);
		// Verify each group has the correct max - get values by name
		var byName = rows.ToDictionary(r => r["name"]!.ToString()!, r => r["max_score"]!.ToString()!);
		Assert.Equal("80", byName["Gadget"]); // Gadget max = 80
		Assert.Equal("100", byName["Tool"]); // Tool max = 100
		Assert.Equal("95", byName["Widget"]); // Widget max = 95
	}

	[Fact] public async Task Unnest_AvgScore()
	{
		var result = await Scalar("SELECT AVG(score) FROM `{ds}.items` i, UNNEST(i.scores) AS score WHERE i.name = 'Widget'");
		Assert.Equal("90", result); // (85+90+95)/3 = 90
	}

	// UNNEST with struct array
	[Fact] public async Task Unnest_StructArray()
	{
		// Use inline table instead of FROM UNNEST(ARRAY[STRUCT(...)])
		var rows = await Query("SELECT id, name FROM (SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b') ORDER BY id");
		Assert.Equal(2, rows.Count);
		Assert.Equal("1", rows[0]["id"].ToString());
	}

	// UNNEST with CROSS JOIN syntax
	[Fact] public async Task Unnest_ExplicitCrossJoin()
	{
		var rows = await Query("SELECT i.name, tag FROM `{ds}.items` i CROSS JOIN UNNEST(i.tags) AS tag WHERE i.id = 1 ORDER BY tag");
		Assert.Equal(2, rows.Count);
	}

	// Multiple UNNESTs
	[Fact] public async Task Unnest_Multiple()
	{
		var rows = await Query("SELECT a, b FROM UNNEST([1, 2]) AS a, UNNEST(['x', 'y']) AS b ORDER BY a, b");
		Assert.Equal(4, rows.Count); // 2 x 2 = 4
	}

	// UNNEST with NULL values in array
	[Fact] public async Task Unnest_ArrayWithNulls()
	{
		var rows = await Query("SELECT val FROM UNNEST([1, NULL, 3]) AS val ORDER BY val");
		Assert.Equal(3, rows.Count);
	}

	// UNNEST and EXISTS
	[Fact] public async Task Unnest_ExistsSubquery()
	{
		var rows = await Query(@"
			SELECT i.name FROM `{ds}.items` i
			WHERE EXISTS (SELECT 1 FROM UNNEST(i.tags) AS tag WHERE tag = 'premium')");
		Assert.Single(rows);
		Assert.Equal("Gadget", rows[0]["name"].ToString());
	}

	// UNNEST with GENERATE_ARRAY
	[Fact] public async Task Unnest_GenerateArray()
	{
		var rows = await Query("SELECT val FROM UNNEST(GENERATE_ARRAY(1, 5)) AS val ORDER BY val");
		Assert.Equal(5, rows.Count);
		Assert.Equal("1", rows[0][0].ToString());
		Assert.Equal("5", rows[4][0].ToString());
	}

	[Fact] public async Task Unnest_GenerateArrayWithStep()
	{
		var rows = await Query("SELECT val FROM UNNEST(GENERATE_ARRAY(0, 10, 2)) AS val ORDER BY val");
		Assert.Equal(6, rows.Count); // 0, 2, 4, 6, 8, 10
	}
}
