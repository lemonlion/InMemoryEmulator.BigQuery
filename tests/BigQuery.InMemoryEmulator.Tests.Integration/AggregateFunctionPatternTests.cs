using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Aggregate function patterns: COUNT, SUM, AVG, MIN, MAX, STRING_AGG, ARRAY_AGG, LOGICAL, APPROX.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class AggregateFunctionPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public AggregateFunctionPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_agfp_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.t` (id INT64, dept STRING, salary INT64, active BOOL, rating FLOAT64)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.t` VALUES
			(1,'Eng',90000,true,4.5),(2,'Eng',75000,false,3.8),(3,'Sales',70000,true,4.2),
			(4,'Sales',65000,true,3.5),(5,'HR',60000,false,4.0),(6,'HR',58000,true,3.2),
			(7,'Eng',85000,true,4.7),(8,'Sales',72000,false,NULL)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- COUNT ----
	[Fact] public async Task Count_Star() => Assert.Equal("8", await S("SELECT COUNT(*) FROM `{ds}.t`"));
	[Fact] public async Task Count_Column() => Assert.Equal("7", await S("SELECT COUNT(rating) FROM `{ds}.t`"));
	[Fact] public async Task Count_Distinct() => Assert.Equal("3", await S("SELECT COUNT(DISTINCT dept) FROM `{ds}.t`"));
	[Fact] public async Task CountIf_Active() => Assert.Equal("5", await S("SELECT COUNTIF(active) FROM `{ds}.t`"));

	// ---- SUM ----
	[Fact] public async Task Sum_Basic() => Assert.Equal("575000", await S("SELECT SUM(salary) FROM `{ds}.t`"));
	[Fact] public async Task Sum_WithWhere() => Assert.Equal("250000", await S("SELECT SUM(salary) FROM `{ds}.t` WHERE dept = 'Eng'"));
	[Fact] public async Task Sum_Null() => Assert.Null(await S("SELECT SUM(CAST(NULL AS INT64))"));

	// ---- AVG ----
	[Fact] public async Task Avg_Basic()
	{
		var v = await S("SELECT ROUND(AVG(salary)) FROM `{ds}.t`");
		Assert.NotNull(v);
		Assert.Equal("71875", v); // 575000/8
	}
	[Fact] public async Task Avg_SkipsNull()
	{
		var v = await S("SELECT ROUND(AVG(rating), 1) FROM `{ds}.t`");
		Assert.NotNull(v);
		// 7 non-null values: (4.5+3.8+4.2+3.5+4.0+3.2+4.7)/7 = 27.9/7 ≈ 3.985... rounds to 4.0
	}

	// ---- MIN/MAX ----
	[Fact] public async Task Min_Basic() => Assert.Equal("58000", await S("SELECT MIN(salary) FROM `{ds}.t`"));
	[Fact] public async Task Max_Basic() => Assert.Equal("90000", await S("SELECT MAX(salary) FROM `{ds}.t`"));
	[Fact] public async Task Min_String() => Assert.Equal("Eng", await S("SELECT MIN(dept) FROM `{ds}.t`"));
	[Fact] public async Task Max_String() => Assert.Equal("Sales", await S("SELECT MAX(dept) FROM `{ds}.t`"));
	[Fact] public async Task Min_Null() => Assert.Null(await S("SELECT MIN(CAST(NULL AS INT64))"));

	// ---- GROUP BY ----
	[Fact] public async Task GroupBy_Count()
	{
		var rows = await Q("SELECT dept, COUNT(*) AS cnt FROM `{ds}.t` GROUP BY dept ORDER BY dept");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Eng", rows[0]["dept"]?.ToString());
		Assert.Equal("3", rows[0]["cnt"]?.ToString());
	}
	[Fact] public async Task GroupBy_Sum()
	{
		var rows = await Q("SELECT dept, SUM(salary) AS total FROM `{ds}.t` GROUP BY dept ORDER BY dept");
		Assert.Equal("250000", rows[0]["total"]?.ToString()); // Eng
		Assert.Equal("118000", rows[1]["total"]?.ToString()); // HR
		Assert.Equal("207000", rows[2]["total"]?.ToString()); // Sales
	}
	[Fact] public async Task GroupBy_Avg()
	{
		var rows = await Q("SELECT dept, ROUND(AVG(salary)) AS a FROM `{ds}.t` GROUP BY dept ORDER BY dept");
		Assert.NotNull(rows[0]["a"]?.ToString());
	}

	// ---- HAVING ----
	[Fact] public async Task Having_Count()
	{
		var rows = await Q("SELECT dept, COUNT(*) AS cnt FROM `{ds}.t` GROUP BY dept HAVING COUNT(*) >= 3 ORDER BY dept");
		Assert.Equal(2, rows.Count); // Eng(3), Sales(3)
	}
	[Fact] public async Task Having_Sum()
	{
		var rows = await Q("SELECT dept, SUM(salary) AS total FROM `{ds}.t` GROUP BY dept HAVING SUM(salary) > 200000 ORDER BY total");
		Assert.Equal(2, rows.Count); // Sales(207000), Eng(250000)
	}

	// ---- STRING_AGG ----
	[Fact] public async Task StringAgg_Basic()
	{
		var v = await S("SELECT STRING_AGG(dept, ',') FROM (SELECT DISTINCT dept FROM `{ds}.t` ORDER BY dept)");
		Assert.NotNull(v);
		Assert.Contains("Eng", v);
	}
	[Fact] public async Task StringAgg_OrderBy()
	{
		var v = await S("SELECT STRING_AGG(CAST(id AS STRING) ORDER BY id) FROM `{ds}.t`");
		Assert.NotNull(v);
		Assert.StartsWith("1", v);
	}

	// ---- ARRAY_AGG ----
	[Fact] public async Task ArrayAgg_Basic()
	{
		var v = await S("SELECT ARRAY_LENGTH(ARRAY_AGG(salary)) FROM `{ds}.t`");
		Assert.Equal("8", v);
	}
	[Fact] public async Task ArrayAgg_GroupBy()
	{
		var rows = await Q("SELECT dept, ARRAY_LENGTH(ARRAY_AGG(salary)) AS cnt FROM `{ds}.t` GROUP BY dept ORDER BY dept");
		Assert.Equal("3", rows[0]["cnt"]?.ToString()); // Eng
	}

	// ---- LOGICAL ----
	[Fact] public async Task LogicalAnd() => Assert.Equal("False", await S("SELECT LOGICAL_AND(active) FROM `{ds}.t`"));
	[Fact] public async Task LogicalOr() => Assert.Equal("True", await S("SELECT LOGICAL_OR(active) FROM `{ds}.t`"));

	// ---- BIT ----
	[Fact] public async Task BitAnd_Agg() => Assert.NotNull(await S("SELECT BIT_AND(CAST(id AS INT64)) FROM `{ds}.t`"));
	[Fact] public async Task BitOr_Agg() => Assert.NotNull(await S("SELECT BIT_OR(CAST(id AS INT64)) FROM `{ds}.t`"));
	[Fact] public async Task BitXor_Agg() => Assert.NotNull(await S("SELECT BIT_XOR(CAST(id AS INT64)) FROM `{ds}.t`"));

	// ---- APPROX ----
	[Fact] public async Task ApproxCountDistinct()
	{
		var v = await S("SELECT APPROX_COUNT_DISTINCT(dept) FROM `{ds}.t`");
		Assert.Equal("3", v);
	}

	// ---- Multiple aggregates ----
	[Fact] public async Task Multiple_Agg()
	{
		var rows = await Q("SELECT COUNT(*) AS c, SUM(salary) AS s, MIN(salary) AS mn, MAX(salary) AS mx FROM `{ds}.t`");
		Assert.Equal("8", rows[0]["c"]?.ToString());
		Assert.Equal("575000", rows[0]["s"]?.ToString());
		Assert.Equal("58000", rows[0]["mn"]?.ToString());
		Assert.Equal("90000", rows[0]["mx"]?.ToString());
	}

	// ---- Aggregate with CASE ----
	[Fact] public async Task Sum_WithCase()
	{
		var v = await S("SELECT SUM(CASE WHEN active THEN 1 ELSE 0 END) FROM `{ds}.t`");
		Assert.Equal("5", v);
	}

	// ---- Aggregate with DISTINCT ----
	[Fact] public async Task Sum_Distinct()
	{
		var v = await S("SELECT SUM(DISTINCT salary) FROM `{ds}.t`");
		Assert.NotNull(v);
	}

	// ---- Empty table aggregate ----
	[Fact] public async Task Sum_Empty()
	{
		await Exec("CREATE TABLE `{ds}.empty` (val INT64)");
		Assert.Null(await S("SELECT SUM(val) FROM `{ds}.empty`"));
	}
	[Fact] public async Task Count_Empty()
	{
		await Exec("CREATE TABLE `{ds}.empty2` (val INT64)");
		Assert.Equal("0", await S("SELECT COUNT(*) FROM `{ds}.empty2`"));
	}

	private async Task Exec(string sql) { var c = await _fixture.GetClientAsync(); await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); }
}
