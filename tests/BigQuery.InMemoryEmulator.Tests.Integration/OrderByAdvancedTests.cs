using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for advanced ORDER BY patterns including expressions, NULLS FIRST/LAST.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#order_by_clause
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class OrderByAdvancedTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public OrderByAdvancedTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_ord_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.data` (id INT64, name STRING, value FLOAT64, category STRING, created DATE)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.data` (id, name, value, category, created) VALUES
			(1, 'Alpha', 10.5, 'A', DATE '2024-01-15'),
			(2, 'Beta', NULL, 'B', DATE '2024-02-20'),
			(3, 'Gamma', 30.0, 'A', DATE '2024-03-10'),
			(4, 'Delta', 20.0, NULL, DATE '2024-01-25'),
			(5, 'Epsilon', 15.0, 'B', NULL),
			(6, 'Zeta', 25.0, 'A', DATE '2024-04-01'),
			(7, 'Eta', NULL, 'C', DATE '2024-02-28'),
			(8, 'Theta', 5.0, NULL, DATE '2024-05-15')", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
		return result.ToList();
	}

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// Basic ordering
	[Fact] public async Task OrderBy_Ascending()
	{
		var rows = await Query("SELECT name FROM `{ds}.data` ORDER BY name ASC");
		Assert.Equal("Alpha", rows[0]["name"].ToString());
	}

	[Fact] public async Task OrderBy_Descending()
	{
		var rows = await Query("SELECT name FROM `{ds}.data` ORDER BY name DESC");
		Assert.Equal("Zeta", rows[0]["name"].ToString());
	}

	[Fact] public async Task OrderBy_MultipleColumns()
	{
		var rows = await Query("SELECT name, category FROM `{ds}.data` WHERE category IS NOT NULL ORDER BY category ASC, name ASC");
		Assert.Equal("Alpha", rows[0]["name"].ToString()); // category A first
	}

	[Fact] public async Task OrderBy_MixedDirection()
	{
		var rows = await Query("SELECT name, category FROM `{ds}.data` WHERE category IS NOT NULL ORDER BY category ASC, value DESC");
		Assert.Equal("Gamma", rows[0]["name"].ToString()); // A category, highest value
	}

	// NULL ordering
	[Fact] public async Task OrderBy_NullsLast_Asc()
	{
		var rows = await Query("SELECT name, value FROM `{ds}.data` ORDER BY value ASC NULLS LAST");
		Assert.Equal("Theta", rows[0]["name"].ToString()); // 5.0 is smallest
		Assert.Null(rows[^1]["value"]); // NULL last
	}

	[Fact] public async Task OrderBy_NullsFirst_Asc()
	{
		var rows = await Query("SELECT name, value FROM `{ds}.data` ORDER BY value ASC NULLS FIRST");
		Assert.Null(rows[0]["value"]); // NULL first
	}

	[Fact] public async Task OrderBy_NullsLast_Desc()
	{
		var rows = await Query("SELECT name, value FROM `{ds}.data` ORDER BY value DESC NULLS LAST");
		Assert.Equal("Gamma", rows[0]["name"].ToString()); // 30.0 is largest
		Assert.Null(rows[^1]["value"]);
	}

	[Fact] public async Task OrderBy_NullsFirst_Desc()
	{
		var rows = await Query("SELECT name, value FROM `{ds}.data` ORDER BY value DESC NULLS FIRST");
		Assert.Null(rows[0]["value"]);
	}

	// ORDER BY with expressions
	[Fact] public async Task OrderBy_Expression()
	{
		var rows = await Query("SELECT name, value FROM `{ds}.data` WHERE value IS NOT NULL ORDER BY value * 2 ASC");
		Assert.Equal("Theta", rows[0]["name"].ToString()); // 5.0*2=10 is smallest
	}

	[Fact] public async Task OrderBy_CaseExpression()
	{
		var rows = await Query(@"
			SELECT name, category FROM `{ds}.data` WHERE category IS NOT NULL
			ORDER BY CASE category WHEN 'C' THEN 1 WHEN 'B' THEN 2 WHEN 'A' THEN 3 END");
		Assert.Equal("C", rows[0]["category"].ToString());
	}

	[Fact] public async Task OrderBy_FunctionResult()
	{
		var rows = await Query("SELECT name FROM `{ds}.data` ORDER BY LENGTH(name) ASC");
		Assert.Equal("Eta", rows[0]["name"].ToString()); // 3 chars
	}

	[Fact] public async Task OrderBy_Alias()
	{
		var rows = await Query("SELECT name, LENGTH(name) AS name_len FROM `{ds}.data` ORDER BY name_len ASC");
		Assert.Equal("Eta", rows[0]["name"].ToString());
	}

	// ORDER BY with LIMIT/OFFSET
	[Fact] public async Task OrderBy_WithLimit()
	{
		var rows = await Query("SELECT name FROM `{ds}.data` ORDER BY name ASC LIMIT 3");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Alpha", rows[0]["name"].ToString());
	}

	[Fact] public async Task OrderBy_WithOffset()
	{
		var rows = await Query("SELECT name FROM `{ds}.data` ORDER BY name ASC LIMIT 2 OFFSET 2");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Delta", rows[0]["name"].ToString());
	}

	[Fact] public async Task OrderBy_OffsetBeyondRows()
	{
		var rows = await Query("SELECT name FROM `{ds}.data` ORDER BY name ASC LIMIT 10 OFFSET 100");
		Assert.Empty(rows);
	}

	// ORDER BY with GROUP BY
	[Fact] public async Task OrderBy_AfterGroupBy()
	{
		var rows = await Query("SELECT category, COUNT(*) AS cnt FROM `{ds}.data` WHERE category IS NOT NULL GROUP BY category ORDER BY cnt DESC");
		Assert.Equal("A", rows[0]["category"].ToString()); // A has 3 rows
	}

	[Fact] public async Task OrderBy_AggregateExpression()
	{
		var rows = await Query("SELECT category, SUM(value) AS total FROM `{ds}.data` WHERE category IS NOT NULL AND value IS NOT NULL GROUP BY category ORDER BY SUM(value) DESC");
		Assert.True(rows.Count >= 2);
	}

	// ORDER BY ordinal
	[Fact] public async Task OrderBy_Ordinal()
	{
		var rows = await Query("SELECT name, value FROM `{ds}.data` WHERE value IS NOT NULL ORDER BY 2 ASC");
		Assert.Equal("Theta", rows[0]["name"].ToString()); // value 5.0
	}

	// ORDER BY with DISTINCT
	[Fact] public async Task OrderBy_WithDistinct()
	{
		var rows = await Query("SELECT DISTINCT category FROM `{ds}.data` WHERE category IS NOT NULL ORDER BY category");
		Assert.Equal(3, rows.Count);
		Assert.Equal("A", rows[0]["category"].ToString());
	}

	// ORDER BY with subquery
	[Fact] public async Task OrderBy_InSubquery()
	{
		var rows = await Query(@"
			SELECT name FROM (
				SELECT name, value FROM `{ds}.data` WHERE value IS NOT NULL ORDER BY value DESC LIMIT 3
			) ORDER BY name");
		Assert.Equal(3, rows.Count);
	}

	// Stable sort
	[Fact] public async Task OrderBy_StableWithTies()
	{
		var rows = await Query("SELECT name, category FROM `{ds}.data` WHERE category = 'A' ORDER BY category");
		Assert.Equal(3, rows.Count); // All have same category
	}
}
