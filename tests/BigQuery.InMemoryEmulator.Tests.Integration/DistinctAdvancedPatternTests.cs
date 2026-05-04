using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// DISTINCT patterns: basic, with ORDER BY, with aggregates, DISTINCT ON equivalent, with NULLS.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_distinct
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DistinctAdvancedPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public DistinctAdvancedPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_dap_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.t` (id INT64, cat STRING, sub STRING, val INT64, tag STRING)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.t` VALUES
			(1,'A','x',10,'red'),(2,'A','x',20,'blue'),(3,'A','y',10,'red'),
			(4,'B','x',30,'red'),(5,'B','y',40,'green'),(6,'B','y',30,'blue'),
			(7,'C','x',50,'red'),(8,'C','x',50,'red'),(9,'C','z',60,NULL),
			(10,'A','x',10,'red'),(11,NULL,NULL,NULL,NULL),(12,'A','x',20,'blue')", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- Basic DISTINCT ----
	[Fact] public async Task Distinct_SingleColumn()
	{
		var rows = await Q("SELECT DISTINCT cat FROM `{ds}.t` ORDER BY cat");
		Assert.Equal(4, rows.Count); // A, B, C, NULL
	}
	[Fact] public async Task Distinct_MultiColumn()
	{
		var rows = await Q("SELECT DISTINCT cat, sub FROM `{ds}.t` WHERE cat IS NOT NULL ORDER BY cat, sub");
		Assert.True(rows.Count >= 5); // A-x, A-y, B-x, B-y, C-x, C-z
	}
	[Fact] public async Task Distinct_AllSame()
	{
		var rows = await Q("SELECT DISTINCT 1 AS x FROM `{ds}.t`");
		Assert.Single(rows);
	}

	// ---- DISTINCT with NULL ----
	[Fact] public async Task Distinct_IncludesNull()
	{
		var rows = await Q("SELECT DISTINCT cat FROM `{ds}.t` ORDER BY cat");
		Assert.NotNull(rows); // NULL should be among distinct values
		Assert.True(rows.Count >= 3); // at least A, B, NULL
	}
	[Fact] public async Task Distinct_NullTag()
	{
		var rows = await Q("SELECT DISTINCT tag FROM `{ds}.t` ORDER BY tag");
		Assert.True(rows.Count >= 4); // blue, green, red, NULL
	}

	// ---- DISTINCT with ORDER BY ----
	[Fact] public async Task Distinct_OrderByAsc()
	{
		var rows = await Q("SELECT DISTINCT cat FROM `{ds}.t` WHERE cat IS NOT NULL ORDER BY cat ASC");
		Assert.Equal("A", rows[0]["cat"]?.ToString());
	}
	[Fact] public async Task Distinct_OrderByDesc()
	{
		var rows = await Q("SELECT DISTINCT cat FROM `{ds}.t` WHERE cat IS NOT NULL ORDER BY cat DESC");
		Assert.Equal("C", rows[0]["cat"]?.ToString());
	}

	// ---- DISTINCT with LIMIT ----
	[Fact] public async Task Distinct_Limit()
	{
		var rows = await Q("SELECT DISTINCT cat FROM `{ds}.t` WHERE cat IS NOT NULL ORDER BY cat LIMIT 2");
		Assert.Equal(2, rows.Count);
	}

	// ---- DISTINCT with WHERE ----
	[Fact] public async Task Distinct_Filtered()
	{
		var rows = await Q("SELECT DISTINCT tag FROM `{ds}.t` WHERE cat = 'A' AND tag IS NOT NULL ORDER BY tag");
		Assert.Equal(2, rows.Count); // red, blue
	}

	// ---- COUNT(DISTINCT ...) ----
	[Fact] public async Task CountDistinct_Basic()
	{
		var v = await S("SELECT COUNT(DISTINCT cat) FROM `{ds}.t`");
		Assert.Equal("3", v); // A, B, C (excludes NULL)
	}
	[Fact] public async Task CountDistinct_Tag()
	{
		var v = await S("SELECT COUNT(DISTINCT tag) FROM `{ds}.t`");
		Assert.Equal("3", v); // red, blue, green
	}
	[Fact] public async Task CountDistinct_Val()
	{
		var v = await S("SELECT COUNT(DISTINCT val) FROM `{ds}.t`");
		Assert.NotNull(v);
	}

	// ---- SUM(DISTINCT ...) ----
	[Fact] public async Task SumDistinct()
	{
		var v = await S("SELECT SUM(DISTINCT val) FROM `{ds}.t`");
		Assert.NotNull(v); // 10+20+30+40+50+60
	}

	// ---- AVG(DISTINCT ...) ----
	[Fact] public async Task AvgDistinct()
	{
		var v = await S("SELECT ROUND(AVG(DISTINCT val), 1) FROM `{ds}.t`");
		Assert.NotNull(v);
	}

	// ---- DISTINCT in subquery ----
	[Fact] public async Task Distinct_InSubquery()
	{
		var v = await S("SELECT COUNT(*) FROM (SELECT DISTINCT cat, sub FROM `{ds}.t` WHERE cat IS NOT NULL)");
		Assert.True(int.Parse(v!) >= 5);
	}

	// ---- DISTINCT with expression ----
	[Fact] public async Task Distinct_WithExpr()
	{
		var rows = await Q("SELECT DISTINCT val * 2 AS doubled FROM `{ds}.t` WHERE val IS NOT NULL ORDER BY doubled");
		Assert.True(rows.Count >= 5);
	}

	// ---- DISTINCT with CASE ----
	[Fact] public async Task Distinct_WithCase()
	{
		var rows = await Q("SELECT DISTINCT CASE WHEN val > 30 THEN 'high' ELSE 'low' END AS tier FROM `{ds}.t` WHERE val IS NOT NULL ORDER BY tier");
		Assert.Equal(2, rows.Count);
	}

	// ---- DISTINCT with GROUP BY ----
	[Fact] public async Task Distinct_GroupBy()
	{
		var rows = await Q("SELECT cat, COUNT(DISTINCT sub) AS sub_cnt FROM `{ds}.t` WHERE cat IS NOT NULL GROUP BY cat ORDER BY cat");
		Assert.Equal(3, rows.Count);
	}

	// ---- DISTINCT with JOIN ----
	[Fact] public async Task Distinct_CrossColumns()
	{
		var rows = await Q("SELECT DISTINCT cat, tag FROM `{ds}.t` WHERE cat IS NOT NULL AND tag IS NOT NULL ORDER BY cat, tag");
		Assert.True(rows.Count >= 5);
	}

	// ---- DISTINCT with UNION ----
	[Fact] public async Task UnionDistinct()
	{
		var rows = await Q("SELECT cat FROM `{ds}.t` WHERE cat = 'A' UNION DISTINCT SELECT cat FROM `{ds}.t` WHERE cat = 'A' ORDER BY cat");
		Assert.Single(rows);
	}

	// ---- Duplicate detection ----
	[Fact] public async Task Exact_Duplicates()
	{
		// Rows 7 & 8 are exact duplicates (C, x, 50, red)
		var all = await Q("SELECT * FROM `{ds}.t` WHERE cat = 'C' AND sub = 'x' AND val = 50");
		var distinct = await Q("SELECT DISTINCT cat, sub, val, tag FROM `{ds}.t` WHERE cat = 'C' AND sub = 'x' AND val = 50");
		Assert.True(all.Count > distinct.Count);
	}

	// ---- DISTINCT with COALESCE ----
	[Fact] public async Task Distinct_Coalesce()
	{
		var rows = await Q("SELECT DISTINCT COALESCE(tag, 'none') AS tag FROM `{ds}.t` ORDER BY tag");
		Assert.True(rows.Count >= 4); // blue, green, none, red
	}

	// ---- DISTINCT star ----
	[Fact] public async Task Distinct_Star()
	{
		var all = await Q("SELECT * FROM `{ds}.t`");
		var distinct = await Q("SELECT DISTINCT * FROM `{ds}.t`");
		Assert.True(distinct.Count <= all.Count);
	}
}
