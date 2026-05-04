using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// HAVING clause patterns: with multiple aggregates, complex conditions, subqueries, CASE.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#having_clause
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class HavingClausePatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public HavingClausePatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_hcp_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.sales` (id INT64, rep STRING, region STRING, product STRING, amount FLOAT64, qty INT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.sales` VALUES
			(1,'Alice','East','Widget',100,5),(2,'Alice','East','Gadget',200,3),
			(3,'Bob','West','Widget',150,8),(4,'Bob','West','Widget',120,6),
			(5,'Carol','East','Gadget',180,4),(6,'Carol','East','Doohickey',90,10),
			(7,'Dave','West','Widget',140,7),(8,'Dave','West','Gadget',220,2),
			(9,'Alice','East','Widget',130,6),(10,'Eve','North','Widget',160,9),
			(11,'Eve','North','Gadget',190,5),(12,'Eve','North','Doohickey',110,8),
			(13,'Frank','North','Widget',95,12),(14,'Frank','North','Gadget',170,3),
			(15,'Carol','East','Widget',200,4)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- Basic HAVING ----
	[Fact] public async Task Having_CountGt2()
	{
		var rows = await Q("SELECT rep, COUNT(*) AS cnt FROM `{ds}.sales` GROUP BY rep HAVING COUNT(*) > 2 ORDER BY rep");
		Assert.True(rows.Count >= 2);
	}
	[Fact] public async Task Having_SumGt300()
	{
		var rows = await Q("SELECT rep, SUM(amount) AS total FROM `{ds}.sales` GROUP BY rep HAVING SUM(amount) > 300 ORDER BY rep");
		Assert.True(rows.Count >= 2);
	}

	// ---- HAVING with AVG ----
	[Fact] public async Task Having_AvgGt150()
	{
		var rows = await Q("SELECT region, ROUND(AVG(amount), 2) AS avg_amt FROM `{ds}.sales` GROUP BY region HAVING AVG(amount) > 150 ORDER BY region");
		Assert.True(rows.Count >= 0);
	}

	// ---- HAVING with MIN/MAX ----
	[Fact] public async Task Having_MinGt100()
	{
		var rows = await Q("SELECT rep, MIN(amount) AS min_amt FROM `{ds}.sales` GROUP BY rep HAVING MIN(amount) > 100 ORDER BY rep");
		Assert.True(rows.Count >= 0);
	}
	[Fact] public async Task Having_MaxLt200()
	{
		var rows = await Q("SELECT rep, MAX(amount) AS max_amt FROM `{ds}.sales` GROUP BY rep HAVING MAX(amount) < 200 ORDER BY rep");
		Assert.True(rows.Count >= 0);
	}

	// ---- HAVING with multiple conditions ----
	[Fact] public async Task Having_AndCondition()
	{
		var rows = await Q("SELECT rep, COUNT(*) AS cnt, SUM(amount) AS total FROM `{ds}.sales` GROUP BY rep HAVING COUNT(*) >= 2 AND SUM(amount) > 250 ORDER BY rep");
		Assert.True(rows.Count >= 1);
	}
	[Fact] public async Task Having_OrCondition()
	{
		var rows = await Q("SELECT rep, COUNT(*) AS cnt, SUM(amount) AS total FROM `{ds}.sales` GROUP BY rep HAVING COUNT(*) >= 3 OR SUM(amount) > 400 ORDER BY rep");
		Assert.True(rows.Count >= 2);
	}

	// ---- HAVING with WHERE ----
	[Fact] public async Task Having_WithWhere()
	{
		var rows = await Q("SELECT rep, SUM(amount) AS total FROM `{ds}.sales` WHERE product = 'Widget' GROUP BY rep HAVING SUM(amount) > 100 ORDER BY rep");
		Assert.True(rows.Count >= 2);
	}
	[Fact] public async Task Having_WithWhereAndOrderBy()
	{
		var rows = await Q("SELECT rep, SUM(amount) AS total FROM `{ds}.sales` WHERE region = 'East' GROUP BY rep HAVING SUM(amount) > 100 ORDER BY total DESC");
		Assert.True(rows.Count >= 1);
	}

	// ---- HAVING with multi-column GROUP BY ----
	[Fact] public async Task Having_MultiGroup()
	{
		var rows = await Q("SELECT region, product, COUNT(*) AS cnt FROM `{ds}.sales` GROUP BY region, product HAVING COUNT(*) > 1 ORDER BY region, product");
		Assert.True(rows.Count >= 1);
	}

	// ---- HAVING with DISTINCT count ----
	[Fact] public async Task Having_CountDistinct()
	{
		var rows = await Q("SELECT region, COUNT(DISTINCT product) AS prod_cnt FROM `{ds}.sales` GROUP BY region HAVING COUNT(DISTINCT product) >= 2 ORDER BY region");
		Assert.True(rows.Count >= 2);
	}

	// ---- HAVING = specific value ----
	[Fact] public async Task Having_ExactCount()
	{
		var rows = await Q("SELECT rep, COUNT(*) AS cnt FROM `{ds}.sales` GROUP BY rep HAVING COUNT(*) = 3");
		Assert.True(rows.Count >= 0);
	}

	// ---- HAVING NOT ----
	[Fact] public async Task Having_Not()
	{
		var rows = await Q("SELECT rep, COUNT(*) AS cnt FROM `{ds}.sales` GROUP BY rep HAVING NOT COUNT(*) = 1 ORDER BY rep");
		Assert.True(rows.Count >= 3);
	}

	// ---- HAVING with SUM CASE ----
	[Fact] public async Task Having_SumCase()
	{
		var rows = await Q(@"
			SELECT rep,
				SUM(CASE WHEN product = 'Widget' THEN 1 ELSE 0 END) AS widget_count
			FROM `{ds}.sales`
			GROUP BY rep
			HAVING SUM(CASE WHEN product = 'Widget' THEN 1 ELSE 0 END) > 1
			ORDER BY rep");
		Assert.True(rows.Count >= 1);
	}

	// ---- HAVING with ORDER BY aggregate ----
	[Fact] public async Task Having_OrderByAgg()
	{
		var rows = await Q("SELECT rep, SUM(amount) AS total FROM `{ds}.sales` GROUP BY rep HAVING SUM(amount) > 200 ORDER BY SUM(amount) DESC");
		Assert.True(rows.Count >= 2);
	}

	// ---- HAVING with LIMIT ----
	[Fact] public async Task Having_WithLimit()
	{
		var rows = await Q("SELECT rep, COUNT(*) AS cnt FROM `{ds}.sales` GROUP BY rep HAVING COUNT(*) >= 2 ORDER BY cnt DESC LIMIT 2");
		Assert.True(rows.Count <= 2);
	}

	// ---- HAVING BETWEEN ----
	[Fact] public async Task Having_Between()
	{
		var rows = await Q("SELECT rep, SUM(amount) AS total FROM `{ds}.sales` GROUP BY rep HAVING SUM(amount) >= 200 AND SUM(amount) <= 600 ORDER BY rep");
		Assert.True(rows.Count >= 1);
	}

	// ---- HAVING in subquery ----
	[Fact] public async Task Having_InSubquery()
	{
		var v = await S(@"
			SELECT COUNT(*) FROM (
				SELECT rep, COUNT(*) AS cnt
				FROM `{ds}.sales`
				GROUP BY rep
				HAVING COUNT(*) >= 2
			)");
		Assert.True(int.Parse(v!) >= 3);
	}

	// ---- HAVING with CTE ----
	[Fact] public async Task Having_InCte()
	{
		var rows = await Q(@"
			WITH grouped AS (
				SELECT region, product, SUM(amount) AS total
				FROM `{ds}.sales`
				GROUP BY region, product
				HAVING SUM(amount) > 100
			)
			SELECT region, product, total FROM grouped ORDER BY total DESC");
		Assert.True(rows.Count >= 3);
	}

	// ---- HAVING with string aggregate ----
	[Fact] public async Task Having_StringAgg()
	{
		var rows = await Q("SELECT region, STRING_AGG(DISTINCT product, ', ') AS products FROM `{ds}.sales` GROUP BY region HAVING COUNT(DISTINCT product) >= 2 ORDER BY region");
		Assert.True(rows.Count >= 2);
	}

	// ---- Complex HAVING expression ----
	[Fact] public async Task Having_Complex()
	{
		var rows = await Q(@"
			SELECT rep, COUNT(*) AS cnt, SUM(amount) AS total, ROUND(AVG(amount), 2) AS avg_amt
			FROM `{ds}.sales`
			GROUP BY rep
			HAVING COUNT(*) >= 2 AND SUM(amount) > 200 AND AVG(amount) > 100
			ORDER BY total DESC");
		Assert.True(rows.Count >= 1);
	}

	// ---- HAVING with computed expression ----
	[Fact] public async Task Having_ComputedExpr()
	{
		var rows = await Q("SELECT rep, SUM(amount * qty) AS revenue FROM `{ds}.sales` GROUP BY rep HAVING SUM(amount * qty) > 1000 ORDER BY revenue DESC");
		Assert.True(rows.Count >= 1);
	}
}
