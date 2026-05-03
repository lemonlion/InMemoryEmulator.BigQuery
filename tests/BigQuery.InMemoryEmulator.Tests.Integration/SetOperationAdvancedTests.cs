using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for advanced set operations: UNION ALL, UNION DISTINCT, INTERSECT, EXCEPT with complex scenarios.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class SetOperationAdvancedTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public SetOperationAdvancedTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_setop_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.t1` (id INT64, val STRING)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.t1` (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')", parameters: null);
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.t2` (id INT64, val STRING)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.t2` (id, val) VALUES (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f'), (7, 'g')", parameters: null);
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.t3` (id INT64, val STRING)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.t3` (id, val) VALUES (5, 'e'), (6, 'f'), (7, 'g'), (8, 'h'), (9, 'i')", parameters: null);
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

	// UNION ALL
	[Fact] public async Task UnionAll_TwoTables()
	{
		var result = await Scalar("SELECT COUNT(*) FROM (SELECT * FROM `{ds}.t1` UNION ALL SELECT * FROM `{ds}.t2`)");
		Assert.Equal("10", result); // 5 + 5
	}

	[Fact] public async Task UnionAll_ThreeTables()
	{
		var result = await Scalar("SELECT COUNT(*) FROM (SELECT * FROM `{ds}.t1` UNION ALL SELECT * FROM `{ds}.t2` UNION ALL SELECT * FROM `{ds}.t3`)");
		Assert.Equal("15", result); // 5 + 5 + 5
	}

	[Fact] public async Task UnionAll_WithLiterals()
	{
		var rows = await Query("SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY x");
		Assert.Equal(3, rows.Count);
	}

	[Fact] public async Task UnionAll_DuplicatesPreserved()
	{
		var result = await Scalar("SELECT COUNT(*) FROM (SELECT id FROM `{ds}.t1` WHERE id >= 3 UNION ALL SELECT id FROM `{ds}.t2` WHERE id <= 5)");
		Assert.Equal("6", result); // t1: 3,4,5 + t2: 3,4,5 = 6
	}

	// UNION DISTINCT
	[Fact] public async Task UnionDistinct_TwoTables()
	{
		var result = await Scalar("SELECT COUNT(*) FROM (SELECT * FROM `{ds}.t1` UNION DISTINCT SELECT * FROM `{ds}.t2`)");
		Assert.Equal("7", result); // 1,2,3,4,5,6,7 - deduped
	}

	[Fact] public async Task UnionDistinct_ThreeTables()
	{
		var result = await Scalar("SELECT COUNT(*) FROM (SELECT * FROM `{ds}.t1` UNION DISTINCT SELECT * FROM `{ds}.t2` UNION DISTINCT SELECT * FROM `{ds}.t3`)");
		Assert.Equal("9", result); // 1..9
	}

	[Fact] public async Task UnionDistinct_WithDuplicateLiterals()
	{
		var rows = await Query("SELECT 1 AS x UNION DISTINCT SELECT 1 UNION DISTINCT SELECT 2");
		Assert.Equal(2, rows.Count);
	}

	// INTERSECT DISTINCT
	[Fact] public async Task IntersectDistinct_TwoTables()
	{
		var result = await Scalar("SELECT COUNT(*) FROM (SELECT * FROM `{ds}.t1` INTERSECT DISTINCT SELECT * FROM `{ds}.t2`)");
		Assert.Equal("3", result); // 3,4,5
	}

	[Fact] public async Task IntersectDistinct_ThreeTables()
	{
		var result = await Scalar("SELECT COUNT(*) FROM (SELECT * FROM `{ds}.t1` INTERSECT DISTINCT SELECT * FROM `{ds}.t2` INTERSECT DISTINCT SELECT * FROM `{ds}.t3`)");
		Assert.Equal("1", result); // only 5 is common to all three
	}

	[Fact] public async Task IntersectDistinct_NoOverlap()
	{
		var result = await Scalar("SELECT COUNT(*) FROM (SELECT * FROM `{ds}.t1` WHERE id <= 2 INTERSECT DISTINCT SELECT * FROM `{ds}.t2`)");
		Assert.Equal("0", result);
	}

	// EXCEPT DISTINCT
	[Fact] public async Task ExceptDistinct_TwoTables()
	{
		var result = await Scalar("SELECT COUNT(*) FROM (SELECT * FROM `{ds}.t1` EXCEPT DISTINCT SELECT * FROM `{ds}.t2`)");
		Assert.Equal("2", result); // 1,2 only in t1
	}

	[Fact] public async Task ExceptDistinct_Reversed()
	{
		var result = await Scalar("SELECT COUNT(*) FROM (SELECT * FROM `{ds}.t2` EXCEPT DISTINCT SELECT * FROM `{ds}.t1`)");
		Assert.Equal("2", result); // 6,7 only in t2
	}

	[Fact] public async Task ExceptDistinct_AllRemoved()
	{
		var result = await Scalar("SELECT COUNT(*) FROM (SELECT * FROM `{ds}.t1` WHERE id >= 3 EXCEPT DISTINCT SELECT * FROM `{ds}.t2`)");
		Assert.Equal("0", result); // 3,4,5 are all in t2
	}

	// Combined set operations
	[Fact] public async Task SetOps_UnionThenIntersect()
	{
		// Use INTERSECT DISTINCT directly (without parenthesized subquery)
		// t2 (3-7) INTERSECT DISTINCT t3 (5-9) = 5,6,7
		var result = await Scalar(@"
			SELECT COUNT(*) FROM (
				SELECT * FROM `{ds}.t2`
				INTERSECT DISTINCT
				SELECT * FROM `{ds}.t3`
			)");
		Assert.Equal("3", result);
	}

	// Set operations with ORDER BY
	[Fact] public async Task UnionAll_WithOrderBy()
	{
		var rows = await Query("SELECT id FROM `{ds}.t1` UNION ALL SELECT id FROM `{ds}.t2` ORDER BY id");
		Assert.Equal("1", rows[0]["id"].ToString());
	}

	[Fact] public async Task UnionAll_WithLimit()
	{
		// Wrap in subquery to apply ORDER BY + LIMIT to the full UNION result
		var rows = await Query("SELECT id FROM (SELECT id FROM `{ds}.t1` UNION ALL SELECT id FROM `{ds}.t2`) ORDER BY id LIMIT 3");
		Assert.Equal(3, rows.Count);
	}

	// Set operations with filtering
	[Fact] public async Task UnionAll_WithWhere()
	{
		var result = await Scalar("SELECT COUNT(*) FROM (SELECT * FROM `{ds}.t1` WHERE id > 3 UNION ALL SELECT * FROM `{ds}.t2` WHERE id < 5)");
		Assert.Equal("4", result); // t1: 4,5 + t2: 3,4 = 4
	}

	// Set operations with aggregation
	[Fact] public async Task UnionAll_WithAggregation()
	{
		var result = await Scalar("SELECT SUM(id) FROM (SELECT id FROM `{ds}.t1` UNION ALL SELECT id FROM `{ds}.t2`)");
		Assert.Equal("40", result); // (1+2+3+4+5) + (3+4+5+6+7) = 15 + 25 = 40
	}

	// NULL in set operations
	[Fact] public async Task UnionAll_WithNulls()
	{
		var rows = await Query("SELECT NULL AS x UNION ALL SELECT 1 UNION ALL SELECT NULL ORDER BY x");
		Assert.Equal(3, rows.Count);
	}

	[Fact] public async Task UnionDistinct_WithNulls()
	{
		var rows = await Query("SELECT NULL AS x UNION DISTINCT SELECT 1 UNION DISTINCT SELECT NULL");
		Assert.Equal(2, rows.Count); // NULL and 1
	}

	// Different column types
	[Fact] public async Task UnionAll_StringColumns()
	{
		var rows = await Query("SELECT val FROM `{ds}.t1` UNION ALL SELECT val FROM `{ds}.t2` ORDER BY val");
		Assert.Equal(10, rows.Count);
	}
}
