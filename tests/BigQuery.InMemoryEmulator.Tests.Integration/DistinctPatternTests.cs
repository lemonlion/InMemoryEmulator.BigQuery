using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for DISTINCT keyword in various contexts.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_distinct
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DistinctPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public DistinctPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_dist_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.data` (id INT64, category STRING, value INT64, tag STRING)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.data` (id, category, value, tag) VALUES
			(1, 'A', 10, 'x'), (2, 'A', 20, 'y'), (3, 'A', 10, 'x'),
			(4, 'B', 30, 'x'), (5, 'B', 30, 'y'), (6, 'B', 40, 'z'),
			(7, 'C', 50, 'x'), (8, 'C', 50, 'x'), (9, 'C', 60, 'y')", parameters: null);
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

	// SELECT DISTINCT
	[Fact] public async Task Distinct_SingleColumn()
	{
		var rows = await Query("SELECT DISTINCT category FROM `{ds}.data` ORDER BY category");
		Assert.Equal(3, rows.Count);
	}

	[Fact] public async Task Distinct_MultipleColumns()
	{
		var rows = await Query("SELECT DISTINCT category, value FROM `{ds}.data` ORDER BY category, value");
		Assert.Equal(6, rows.Count); // A:10, A:20, B:30, B:40, C:50, C:60
	}

	[Fact] public async Task Distinct_AllColumns()
	{
		var rows = await Query("SELECT DISTINCT category, value, tag FROM `{ds}.data` ORDER BY category, value, tag");
		Assert.Equal(7, rows.Count); // Row 3=(A,10,x) duplicates row 1, row 8=(C,50,x) duplicates row 7
	}

	[Fact] public async Task Distinct_WithNull()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($"INSERT INTO `{_datasetId}.data` (id, category, value, tag) VALUES (10, NULL, 10, 'x'), (11, NULL, 10, 'x')", parameters: null);
		var rows = await Query("SELECT DISTINCT category FROM `{ds}.data` ORDER BY category");
		Assert.Equal(4, rows.Count); // A, B, C, NULL
	}

	// COUNT(DISTINCT ...)
	[Fact] public async Task CountDistinct_Single() => Assert.Equal("3", await Scalar("SELECT COUNT(DISTINCT category) FROM `{ds}.data`"));
	[Fact] public async Task CountDistinct_Value() => Assert.Equal("6", await Scalar("SELECT COUNT(DISTINCT value) FROM `{ds}.data`")); // 10,20,30,40,50,60
	[Fact] public async Task CountDistinct_Tag() => Assert.Equal("3", await Scalar("SELECT COUNT(DISTINCT tag) FROM `{ds}.data`"));
	[Fact] public async Task CountDistinct_PerGroup()
	{
		var rows = await Query("SELECT category, COUNT(DISTINCT value) AS dv FROM `{ds}.data` GROUP BY category ORDER BY category");
		Assert.Equal("2", rows[0]["dv"].ToString()); // A: 10, 20
	}

	// DISTINCT with ORDER BY
	[Fact] public async Task Distinct_WithOrderByAsc()
	{
		var rows = await Query("SELECT DISTINCT value FROM `{ds}.data` ORDER BY value ASC");
		Assert.Equal("10", rows[0]["value"].ToString());
	}

	[Fact] public async Task Distinct_WithOrderByDesc()
	{
		var rows = await Query("SELECT DISTINCT value FROM `{ds}.data` ORDER BY value DESC");
		Assert.Equal("60", rows[0]["value"].ToString());
	}

	// DISTINCT with LIMIT
	[Fact] public async Task Distinct_WithLimit()
	{
		var rows = await Query("SELECT DISTINCT category FROM `{ds}.data` ORDER BY category LIMIT 2");
		Assert.Equal(2, rows.Count);
	}

	// DISTINCT with expressions
	[Fact] public async Task Distinct_Expression()
	{
		var rows = await Query("SELECT DISTINCT value * 2 AS doubled FROM `{ds}.data` ORDER BY doubled");
		Assert.True(rows.Count >= 5);
	}

	// DISTINCT with functions
	[Fact] public async Task Distinct_WithFunction()
	{
		var rows = await Query("SELECT DISTINCT UPPER(category) AS upper_cat FROM `{ds}.data` ORDER BY upper_cat");
		Assert.Equal(3, rows.Count);
	}

	// DISTINCT in subquery
	[Fact] public async Task Distinct_InSubquery()
	{
		var result = await Scalar("SELECT COUNT(*) FROM (SELECT DISTINCT category, value FROM `{ds}.data`)");
		Assert.Equal("6", result);
	}

	// DISTINCT with HAVING (via COUNT)
	[Fact] public async Task Distinct_InHaving()
	{
		var rows = await Query("SELECT category FROM `{ds}.data` GROUP BY category HAVING COUNT(DISTINCT tag) > 2 ORDER BY category");
		Assert.Equal("B", rows[0]["category"].ToString()); // B has 3 distinct tags
	}

	// STRING_AGG with DISTINCT (use subquery to get distinct values first)
	[Fact] public async Task StringAgg_Distinct()
	{
		var result = await Scalar("SELECT STRING_AGG(category, ',' ORDER BY category) FROM (SELECT DISTINCT category FROM `{ds}.data`)");
		Assert.Equal("A,B,C", result);
	}

	// ARRAY_AGG with DISTINCT
	[Fact] public async Task ArrayAgg_Distinct()
	{
		var result = await Scalar("SELECT ARRAY_LENGTH(ARRAY_AGG(DISTINCT tag)) FROM `{ds}.data`");
		Assert.Equal("3", result);
	}
}
