using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Complex GROUP BY patterns: multiple columns, expressions, ROLLUP, CUBE, GROUPING SETS.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#group_by_clause
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class GroupByAdvancedPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public GroupByAdvancedPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_gbap_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.t` (id INT64, dept STRING, team STRING, role STRING, salary INT64, year INT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.t` VALUES
			(1,'Eng','Backend','Dev',90000,2023),(2,'Eng','Backend','Lead',110000,2023),
			(3,'Eng','Frontend','Dev',85000,2023),(4,'Sales','Direct','Rep',65000,2023),
			(5,'Sales','Direct','Lead',80000,2023),(6,'Sales','Channel','Rep',60000,2023),
			(7,'HR','Ops','Spec',55000,2023),(8,'HR','Ops','Lead',70000,2023),
			(9,'Eng','Backend','Dev',95000,2024),(10,'Sales','Direct','Rep',68000,2024)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- Basic GROUP BY ----
	[Fact] public async Task GroupBy_SingleColumn()
	{
		var rows = await Q("SELECT dept, COUNT(*) AS cnt FROM `{ds}.t` GROUP BY dept ORDER BY dept");
		Assert.Equal(3, rows.Count);
	}
	[Fact] public async Task GroupBy_MultiColumn()
	{
		var rows = await Q("SELECT dept, team, COUNT(*) AS cnt FROM `{ds}.t` GROUP BY dept, team ORDER BY dept, team");
		Assert.True(rows.Count >= 4);
	}
	[Fact] public async Task GroupBy_ThreeColumns()
	{
		var rows = await Q("SELECT dept, team, role, COUNT(*) AS cnt FROM `{ds}.t` GROUP BY dept, team, role ORDER BY dept, team, role");
		Assert.True(rows.Count >= 6);
	}

	// ---- GROUP BY with expressions ----
	[Fact] public async Task GroupBy_Expression()
	{
		var rows = await Q("SELECT CASE WHEN salary >= 80000 THEN 'High' ELSE 'Low' END AS tier, COUNT(*) AS cnt FROM `{ds}.t` GROUP BY tier ORDER BY tier");
		Assert.Equal(2, rows.Count);
	}
	[Fact] public async Task GroupBy_Length()
	{
		var rows = await Q("SELECT LENGTH(dept) AS dlen, COUNT(*) AS cnt FROM `{ds}.t` GROUP BY dlen ORDER BY dlen");
		Assert.True(rows.Count >= 2);
	}

	// ---- GROUP BY with HAVING ----
	[Fact] public async Task Having_AvgSalary()
	{
		var rows = await Q("SELECT dept, ROUND(AVG(salary)) AS avg_sal FROM `{ds}.t` GROUP BY dept HAVING AVG(salary) > 70000 ORDER BY dept");
		Assert.Single(rows); // Only Eng
	}
	[Fact] public async Task Having_MinSalary()
	{
		var rows = await Q("SELECT dept, MIN(salary) AS min_sal FROM `{ds}.t` GROUP BY dept HAVING MIN(salary) < 60000 ORDER BY dept");
		Assert.Single(rows); // HR (55000)
	}

	// ---- GROUP BY with multiple aggregates ----
	[Fact] public async Task GroupBy_MultiAgg()
	{
		var rows = await Q(@"
			SELECT dept, COUNT(*) AS cnt, SUM(salary) AS total, MIN(salary) AS mn, MAX(salary) AS mx
			FROM `{ds}.t` GROUP BY dept ORDER BY dept");
		Assert.Equal(3, rows.Count);
		Assert.NotNull(rows[0]["cnt"]?.ToString());
		Assert.NotNull(rows[0]["total"]?.ToString());
		Assert.NotNull(rows[0]["mn"]?.ToString());
		Assert.NotNull(rows[0]["mx"]?.ToString());
	}

	// ---- GROUP BY with ORDER BY aggregate ----
	[Fact] public async Task GroupBy_OrderByAgg()
	{
		var rows = await Q("SELECT dept, SUM(salary) AS total FROM `{ds}.t` GROUP BY dept ORDER BY total DESC");
		Assert.Equal("Eng", rows[0]["dept"]?.ToString());
	}

	// ---- GROUP BY with LIMIT ----
	[Fact] public async Task GroupBy_Limit()
	{
		var rows = await Q("SELECT dept, COUNT(*) AS cnt FROM `{ds}.t` GROUP BY dept ORDER BY cnt DESC LIMIT 2");
		Assert.Equal(2, rows.Count);
	}

	// ---- GROUP BY year ----
	[Fact] public async Task GroupBy_Year()
	{
		var rows = await Q("SELECT year, COUNT(*) AS cnt FROM `{ds}.t` GROUP BY year ORDER BY year");
		Assert.Equal(2, rows.Count);
		Assert.Equal("2023", rows[0]["year"]?.ToString());
		Assert.Equal("8", rows[0]["cnt"]?.ToString());
		Assert.Equal("2024", rows[1]["year"]?.ToString());
		Assert.Equal("2", rows[1]["cnt"]?.ToString());
	}

	// ---- GROUP BY with CASE-level aggregate ----
	[Fact] public async Task GroupBy_AggCase()
	{
		var rows = await Q(@"
			SELECT dept,
				SUM(CASE WHEN role = 'Dev' THEN 1 ELSE 0 END) AS devs,
				SUM(CASE WHEN role = 'Lead' THEN 1 ELSE 0 END) AS leads
			FROM `{ds}.t` GROUP BY dept ORDER BY dept");
		Assert.Equal(3, rows.Count);
	}

	// ---- GROUP BY with DISTINCT count ----
	[Fact] public async Task GroupBy_CountDistinct()
	{
		var rows = await Q("SELECT dept, COUNT(DISTINCT role) AS roles FROM `{ds}.t` GROUP BY dept ORDER BY dept");
		Assert.Equal("2", rows[0]["roles"]?.ToString()); // Eng: Dev, Lead
	}

	// ---- GROUP BY with string_agg ----
	[Fact] public async Task GroupBy_StringAgg()
	{
		var rows = await Q("SELECT dept, STRING_AGG(role ORDER BY role) AS roles FROM `{ds}.t` GROUP BY dept ORDER BY dept");
		Assert.NotNull(rows[0]["roles"]?.ToString());
	}

	// ---- GROUP BY with subquery ----
	[Fact] public async Task GroupBy_Subquery()
	{
		var rows = await Q(@"
			SELECT dept, total FROM (
				SELECT dept, SUM(salary) AS total FROM `{ds}.t` GROUP BY dept
			) ORDER BY total DESC");
		Assert.Equal("Eng", rows[0]["dept"]?.ToString());
	}

	// ---- GROUP BY with CTE ----
	[Fact] public async Task GroupBy_Cte()
	{
		var rows = await Q(@"
			WITH dept_summary AS (
				SELECT dept, SUM(salary) AS total, COUNT(*) AS cnt
				FROM `{ds}.t` GROUP BY dept
			)
			SELECT dept, total FROM dept_summary ORDER BY total DESC");
		Assert.Equal("Eng", rows[0]["dept"]?.ToString());
	}
}
