using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Comprehensive tests for UNNEST, CROSS JOIN, and array flattening.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unnest_operator
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class UnnestComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public UnnestComprehensiveTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_unn_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.orders` (id INT64, customer STRING, items ARRAY<STRING>, amounts ARRAY<FLOAT64>)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.orders` (id, customer, items, amounts) VALUES
			(1, 'Alice', ['apple','banana','cherry'], [1.5, 2.0, 3.0]),
			(2, 'Bob', ['date','elderberry'], [4.0, 5.5]),
			(3, 'Carol', ['fig'], [6.0])", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> Scalar(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Query(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- Basic UNNEST of array literal ----
	[Fact] public async Task Unnest_IntArray()
	{
		var rows = await Query("SELECT x FROM UNNEST([1,2,3]) AS x ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("3", rows[2][0]?.ToString());
	}

	[Fact] public async Task Unnest_StringArray()
	{
		var rows = await Query("SELECT x FROM UNNEST(['a','b','c']) AS x ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("a", rows[0][0]?.ToString());
	}

	[Fact] public async Task Unnest_EmptyArray()
	{
		var rows = await Query("SELECT x FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x");
		Assert.Empty(rows);
	}

	// ---- UNNEST with WHERE ----
	[Fact] public async Task Unnest_WithWhere()
	{
		var rows = await Query("SELECT x FROM UNNEST([10,20,30,40,50]) AS x WHERE x > 25 ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("30", rows[0][0]?.ToString());
	}

	// ---- UNNEST with aggregation ----
	[Fact] public async Task Unnest_Sum()
	{
		Assert.Equal("15", await Scalar("SELECT SUM(x) FROM UNNEST([1,2,3,4,5]) AS x"));
	}
	[Fact] public async Task Unnest_Count()
	{
		Assert.Equal("5", await Scalar("SELECT COUNT(*) FROM UNNEST([1,2,3,4,5]) AS x"));
	}
	[Fact] public async Task Unnest_Avg()
	{
		Assert.Equal("3", await Scalar("SELECT AVG(x) FROM UNNEST([1,2,3,4,5]) AS x"));
	}
	[Fact] public async Task Unnest_MinMax()
	{
		Assert.Equal("1", await Scalar("SELECT MIN(x) FROM UNNEST([3,1,4,1,5]) AS x"));
		Assert.Equal("5", await Scalar("SELECT MAX(x) FROM UNNEST([3,1,4,1,5]) AS x"));
	}

	// ---- UNNEST with OFFSET ----
	[Fact] public async Task Unnest_WithOffset()
	{
		var rows = await Query("SELECT x, off FROM UNNEST(['a','b','c']) AS x WITH OFFSET AS off ORDER BY off");
		Assert.Equal(3, rows.Count);
		Assert.Equal("0", rows[0]["off"]?.ToString());
		Assert.Equal("a", rows[0]["x"]?.ToString());
		Assert.Equal("2", rows[2]["off"]?.ToString());
	}

	// ---- UNNEST from table column ----
	[Fact] public async Task Unnest_FromTableColumn()
	{
		var rows = await Query("SELECT o.customer, item FROM `{ds}.orders` AS o, UNNEST(o.items) AS item ORDER BY o.customer, item");
		Assert.Equal(6, rows.Count);
	}

	// ---- UNNEST CROSS JOIN from table ----
	[Fact] public async Task Unnest_CrossJoinTable()
	{
		var rows = await Query("SELECT o.id, item FROM `{ds}.orders` AS o CROSS JOIN UNNEST(o.items) AS item ORDER BY o.id");
		Assert.Equal(6, rows.Count);
		Assert.Equal("1", rows[0]["id"]?.ToString());
	}

	// ---- UNNEST count per parent ----
	[Fact] public async Task Unnest_CountPerParent()
	{
		var rows = await Query("SELECT o.customer, COUNT(item) AS cnt FROM `{ds}.orders` AS o, UNNEST(o.items) AS item GROUP BY o.customer ORDER BY o.customer");
		Assert.Equal(3, rows.Count);
		Assert.Equal("3", rows[0]["cnt"]?.ToString()); // Alice: 3 items
		Assert.Equal("2", rows[1]["cnt"]?.ToString()); // Bob: 2 items
		Assert.Equal("1", rows[2]["cnt"]?.ToString()); // Carol: 1 item
	}

	// ---- UNNEST with LIMIT ----
	[Fact] public async Task Unnest_Limit()
	{
		var rows = await Query("SELECT x FROM UNNEST([1,2,3,4,5]) AS x ORDER BY x LIMIT 3");
		Assert.Equal(3, rows.Count);
	}

	// ---- UNNEST with ORDER BY + LIMIT ----
	[Fact] public async Task Unnest_OrderLimit()
	{
		var rows = await Query("SELECT x FROM UNNEST([5,3,1,4,2]) AS x ORDER BY x DESC LIMIT 2");
		Assert.Equal(2, rows.Count);
		Assert.Equal("5", rows[0][0]?.ToString());
		Assert.Equal("4", rows[1][0]?.ToString());
	}

	// ---- UNNEST with DISTINCT ----
	[Fact] public async Task Unnest_Distinct()
	{
		var rows = await Query("SELECT DISTINCT x FROM UNNEST([1,2,2,3,3,3]) AS x ORDER BY x");
		Assert.Equal(3, rows.Count);
	}

	// ---- UNNEST with expression ----
	[Fact] public async Task Unnest_WithExpression()
	{
		var rows = await Query("SELECT x * 2 AS doubled FROM UNNEST([1,2,3]) AS x ORDER BY x");
		Assert.Equal("2", rows[0][0]?.ToString());
		Assert.Equal("6", rows[2][0]?.ToString());
	}

	// ---- UNNEST multiple arrays (CROSS JOIN) ----
	[Fact] public async Task Unnest_CrossMultiple()
	{
		var rows = await Query("SELECT a, b FROM UNNEST([1,2]) AS a, UNNEST(['x','y','z']) AS b ORDER BY a, b");
		Assert.Equal(6, rows.Count); // 2 * 3 = 6
	}

	// ---- UNNEST with GENERATE_ARRAY ----
	[Fact] public async Task Unnest_GenerateArray()
	{
		var rows = await Query("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x ORDER BY x");
		Assert.Equal(5, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("5", rows[4][0]?.ToString());
	}

	// ---- UNNEST with NULL elements ----
	[Fact] public async Task Unnest_WithNulls()
	{
		var rows = await Query("SELECT x FROM UNNEST([1, NULL, 3]) AS x ORDER BY x");
		Assert.Equal(3, rows.Count);
	}

	// ---- UNNEST as subquery ----
	[Fact] public async Task Unnest_InSubquery()
	{
		var v = await Scalar("SELECT (SELECT SUM(x) FROM UNNEST([1,2,3,4,5]) AS x)");
		Assert.Equal("15", v);
	}

	// ---- UNNEST with string functions ----
	[Fact] public async Task Unnest_StringFunctions()
	{
		var rows = await Query("SELECT UPPER(x) AS u FROM UNNEST(['hello','world']) AS x ORDER BY u");
		Assert.Equal("HELLO", rows[0]["u"]?.ToString());
		Assert.Equal("WORLD", rows[1]["u"]?.ToString());
	}

	// ---- UNNEST with GROUP BY and HAVING ----
	[Fact] public async Task Unnest_GroupByHaving()
	{
		var rows = await Query("SELECT x, COUNT(*) AS cnt FROM UNNEST([1,1,2,2,2,3]) AS x GROUP BY x HAVING COUNT(*) > 1 ORDER BY x");
		Assert.Equal(2, rows.Count); // 1 (2 times) and 2 (3 times)
	}

	// ---- UNNEST float array from table ----
	[Fact] public async Task Unnest_FloatFromTable()
	{
		var v = await Scalar("SELECT SUM(amt) FROM `{ds}.orders` AS o, UNNEST(o.amounts) AS amt");
		Assert.Equal("22", v);
	}

	// ---- Array function ARRAY_LENGTH on table column ----
	[Fact] public async Task ArrayLength_TableColumn()
	{
		var rows = await Query("SELECT customer, ARRAY_LENGTH(items) AS len FROM `{ds}.orders` ORDER BY customer");
		Assert.Equal("3", rows[0]["len"]?.ToString()); // Alice: 3
		Assert.Equal("2", rows[1]["len"]?.ToString()); // Bob: 2
		Assert.Equal("1", rows[2]["len"]?.ToString()); // Carol: 1
	}

	// ---- UNNEST with JOIN ----
	[Fact] public async Task Unnest_WithJoin()
	{
		var rows = await Query(@"
			SELECT o.customer, item FROM `{ds}.orders` AS o, UNNEST(o.items) AS item
			WHERE o.customer = 'Alice' ORDER BY item");
		Assert.Equal(3, rows.Count);
		Assert.Equal("apple", rows[0]["item"]?.ToString());
	}

	// ---- Nested UNNEST ----
	[Fact] public async Task Unnest_Nested()
	{
		var rows = await Query("SELECT x, y FROM UNNEST([1,2]) AS x CROSS JOIN UNNEST([10,20]) AS y ORDER BY x, y");
		Assert.Equal(4, rows.Count);
		Assert.Equal("1", rows[0]["x"]?.ToString());
		Assert.Equal("10", rows[0]["y"]?.ToString());
	}
}
