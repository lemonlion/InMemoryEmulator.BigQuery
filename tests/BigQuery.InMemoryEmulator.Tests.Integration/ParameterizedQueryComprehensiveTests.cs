using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for parameterized queries using query parameters.
/// Ref: https://cloud.google.com/bigquery/docs/parameterized-queries
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ParameterizedQueryComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParameterizedQueryComprehensiveTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_pq_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.items` (id INT64, name STRING, price FLOAT64, category STRING, active BOOL)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.items` VALUES
			(1,'Apple',1.5,'fruit',true),(2,'Banana',0.75,'fruit',true),
			(3,'Carrot',2.0,'vegetable',true),(4,'Donut',3.5,'pastry',false),
			(5,'Egg',4.0,'dairy',true),(6,'Fig',5.5,'fruit',false),
			(7,'Grape',2.25,'fruit',true),(8,'Ham',8.0,'meat',true)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql, params BigQueryParameter[] p) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), p); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql, params BigQueryParameter[] p) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), p); return r.ToList(); }

	// ---- String parameter ----
	[Fact] public async Task Param_String()
	{
		var rows = await Q("SELECT name FROM `{ds}.items` WHERE category = @cat ORDER BY name",
			new BigQueryParameter("cat", BigQueryDbType.String, "fruit"));
		Assert.Equal(4, rows.Count);
	}
	[Fact] public async Task Param_StringName()
	{
		var rows = await Q("SELECT id FROM `{ds}.items` WHERE name = @name",
			new BigQueryParameter("name", BigQueryDbType.String, "Apple"));
		Assert.Single(rows);
		Assert.Equal("1", rows[0]["id"]?.ToString());
	}

	// ---- Int parameter ----
	[Fact] public async Task Param_Int()
	{
		var rows = await Q("SELECT name FROM `{ds}.items` WHERE id = @id",
			new BigQueryParameter("id", BigQueryDbType.Int64, 3));
		Assert.Single(rows);
		Assert.Equal("Carrot", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task Param_IntComparison()
	{
		var rows = await Q("SELECT name FROM `{ds}.items` WHERE id > @min_id ORDER BY id",
			new BigQueryParameter("min_id", BigQueryDbType.Int64, 6));
		Assert.Equal(2, rows.Count);
	}

	// ---- Float parameter ----
	[Fact] public async Task Param_Float()
	{
		var rows = await Q("SELECT name FROM `{ds}.items` WHERE price > @min_price ORDER BY price",
			new BigQueryParameter("min_price", BigQueryDbType.Float64, 3.0));
		Assert.Equal(4, rows.Count); // Donut(3.5), Egg(4.0), Fig(5.5), Ham(8.0) -> > 3.0
	}
	[Fact] public async Task Param_FloatBetween()
	{
		var rows = await Q("SELECT name FROM `{ds}.items` WHERE price BETWEEN @lo AND @hi ORDER BY name",
			new BigQueryParameter("lo", BigQueryDbType.Float64, 2.0),
			new BigQueryParameter("hi", BigQueryDbType.Float64, 4.0));
		Assert.True(rows.Count >= 3);
	}

	// ---- Bool parameter ----
	[Fact] public async Task Param_Bool()
	{
		var rows = await Q("SELECT name FROM `{ds}.items` WHERE active = @flag ORDER BY name",
			new BigQueryParameter("flag", BigQueryDbType.Bool, true));
		Assert.Equal(6, rows.Count);
	}

	// ---- Multiple parameters ----
	[Fact] public async Task Param_MultipleParams()
	{
		var rows = await Q("SELECT name FROM `{ds}.items` WHERE category = @cat AND price < @max_price ORDER BY name",
			new BigQueryParameter("cat", BigQueryDbType.String, "fruit"),
			new BigQueryParameter("max_price", BigQueryDbType.Float64, 2.0));
		Assert.Equal(2, rows.Count); // Apple(1.5), Banana(0.75)
	}

	// ---- Parameter in SELECT ----
	[Fact] public async Task Param_InSelect()
	{
		var v = await S("SELECT @val + 10",
			new BigQueryParameter("val", BigQueryDbType.Int64, 5));
		Assert.Equal("15", v);
	}

	// ---- Parameter in expression ----
	[Fact] public async Task Param_InExpression()
	{
		var rows = await Q("SELECT name, ROUND(price * @factor, 2) AS adjusted FROM `{ds}.items` WHERE id = 1",
			new BigQueryParameter("factor", BigQueryDbType.Float64, 1.1));
		Assert.Equal("1.65", rows[0]["adjusted"]?.ToString());
	}

	// ---- Parameter in LIKE ----
	[Fact] public async Task Param_InLike()
	{
		var rows = await Q("SELECT name FROM `{ds}.items` WHERE name LIKE @pattern ORDER BY name",
			new BigQueryParameter("pattern", BigQueryDbType.String, "%a%"));
		Assert.True(rows.Count >= 1);
	}

	// ---- Parameter reuse ----
	[Fact] public async Task Param_Reuse()
	{
		var rows = await Q("SELECT name FROM `{ds}.items` WHERE price > @val AND id > @val ORDER BY name",
			new BigQueryParameter("val", BigQueryDbType.Float64, 3));
		Assert.True(rows.Count >= 2); // price > 3 AND id > 3
	}

	// ---- Count with parameter ----
	[Fact] public async Task Param_CountWhere()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.items` WHERE category = @cat",
			new BigQueryParameter("cat", BigQueryDbType.String, "fruit"));
		Assert.Equal("4", v);
	}

	// ---- Sum with parameter ----
	[Fact] public async Task Param_SumFilter()
	{
		var v = await S("SELECT SUM(price) FROM `{ds}.items` WHERE active = @flag",
			new BigQueryParameter("flag", BigQueryDbType.Bool, true));
		Assert.NotNull(v);
	}

	// ---- Parameter in CASE ----
	[Fact] public async Task Param_InCase()
	{
		var rows = await Q("SELECT name, CASE WHEN price > @threshold THEN 'expensive' ELSE 'cheap' END AS tier FROM `{ds}.items` ORDER BY name",
			new BigQueryParameter("threshold", BigQueryDbType.Float64, 3.0));
		var apple = rows.First(r => r["name"]?.ToString() == "Apple");
		Assert.Equal("cheap", apple["tier"]?.ToString());
	}

	// ---- Parameter with ORDER BY and LIMIT ----
	[Fact] public async Task Param_WithLimit()
	{
		var rows = await Q("SELECT name FROM `{ds}.items` WHERE active = @flag ORDER BY price DESC LIMIT 3",
			new BigQueryParameter("flag", BigQueryDbType.Bool, true));
		Assert.Equal(3, rows.Count);
	}

	// ---- Null parameter ----
	[Fact] public async Task Param_NullValue()
	{
		var v = await S("SELECT COALESCE(@val, 'default')",
			new BigQueryParameter("val", BigQueryDbType.String, null));
		Assert.Equal("default", v);
	}
}
