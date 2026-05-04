using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Comprehensive GROUP BY and aggregation patterns: HAVING, multi-column GROUP BY,
/// aggregation with expressions, COUNTIF, LOGICAL_AND, LOGICAL_OR, ANY_VALUE.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class GroupByAggregationCoverageTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public GroupByAggregationCoverageTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_gba_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.orders` (id INT64, customer STRING, product STRING, category STRING, amount FLOAT64, qty INT64, is_returned BOOL)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.orders` VALUES
			(1,'Alice','Widget','A',10.50,3,false),(2,'Alice','Gadget','B',25.00,1,false),
			(3,'Bob','Widget','A',10.50,5,true),(4,'Bob','Thingamajig','C',50.00,1,false),
			(5,'Carol','Gadget','B',25.00,2,false),(6,'Carol','Widget','A',10.50,4,false),
			(7,'Dave','Thingamajig','C',50.00,2,true),(8,'Dave','Widget','A',10.50,1,false),
			(9,'Alice','Thingamajig','C',50.00,1,false),(10,'Bob','Gadget','B',25.00,3,false),
			(11,'Eve','Widget','A',10.50,2,false),(12,'Eve','Gadget','B',25.00,1,true)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- Basic GROUP BY ----
	[Fact] public async Task GroupBy_Count()
	{
		var rows = await Q("SELECT customer, COUNT(*) AS cnt FROM `{ds}.orders` GROUP BY customer ORDER BY customer");
		Assert.Equal(5, rows.Count);
		Assert.Equal("Alice", rows[0]["customer"]?.ToString());
		Assert.Equal("3", rows[0]["cnt"]?.ToString());
	}
	[Fact] public async Task GroupBy_Sum()
	{
		var rows = await Q("SELECT category, SUM(amount * qty) AS total FROM `{ds}.orders` GROUP BY category ORDER BY category");
		Assert.Equal(3, rows.Count); // A, B, C
	}
	[Fact] public async Task GroupBy_Avg()
	{
		var rows = await Q("SELECT category, AVG(amount) AS avg_price FROM `{ds}.orders` GROUP BY category ORDER BY category");
		Assert.Equal(3, rows.Count);
		Assert.Equal("10.5", rows[0]["avg_price"]?.ToString()); // category A all have amount=10.50
	}
	[Fact] public async Task GroupBy_Min()
	{
		var rows = await Q("SELECT customer, MIN(amount) AS min_amt FROM `{ds}.orders` GROUP BY customer ORDER BY customer");
		Assert.Equal("10.5", rows[0]["min_amt"]?.ToString()); // Alice min is 10.50
	}
	[Fact] public async Task GroupBy_Max()
	{
		var rows = await Q("SELECT customer, MAX(amount) AS max_amt FROM `{ds}.orders` GROUP BY customer ORDER BY customer");
		Assert.Equal("50", rows[0]["max_amt"]?.ToString()); // Alice max is 50
	}

	// ---- Multi-column GROUP BY ----
	[Fact] public async Task GroupBy_MultiCol()
	{
		var rows = await Q("SELECT customer, category, COUNT(*) AS cnt FROM `{ds}.orders` GROUP BY customer, category ORDER BY customer, category");
		Assert.True(rows.Count > 5);
	}
	[Fact] public async Task GroupBy_MultiCol_Sum()
	{
		var rows = await Q("SELECT customer, category, SUM(qty) AS total_qty FROM `{ds}.orders` GROUP BY customer, category HAVING SUM(qty) > 2 ORDER BY customer, category");
		Assert.True(rows.Count > 0);
	}

	// ---- HAVING ----
	[Fact] public async Task Having_Count()
	{
		var rows = await Q("SELECT customer, COUNT(*) AS cnt FROM `{ds}.orders` GROUP BY customer HAVING COUNT(*) >= 3 ORDER BY customer");
		Assert.Equal(2, rows.Count); // Alice(3), Bob(3)
	}
	[Fact] public async Task Having_Sum()
	{
		var rows = await Q("SELECT category, SUM(qty) AS total_qty FROM `{ds}.orders` GROUP BY category HAVING SUM(qty) >= 10 ORDER BY category");
		Assert.True(rows.Count > 0);
	}
	[Fact] public async Task Having_Avg()
	{
		var rows = await Q("SELECT customer, AVG(amount) AS avg_amt FROM `{ds}.orders` GROUP BY customer HAVING AVG(amount) > 20 ORDER BY customer");
		Assert.True(rows.Count > 0);
	}

	// ---- COUNTIF ----
	[Fact] public async Task CountIf_Basic()
	{
		var v = await S("SELECT COUNTIF(is_returned) FROM `{ds}.orders`");
		Assert.Equal("3", v); // orders 3, 7, 12
	}
	[Fact] public async Task CountIf_PerCustomer()
	{
		var rows = await Q("SELECT customer, COUNTIF(is_returned) AS returns FROM `{ds}.orders` GROUP BY customer ORDER BY customer");
		// Alice: 0, Bob: 1, Carol: 0, Dave: 1, Eve: 1
		Assert.Equal("0", rows[0]["returns"]?.ToString()); // Alice
		Assert.Equal("1", rows[1]["returns"]?.ToString()); // Bob
	}
	[Fact] public async Task CountIf_Condition()
	{
		var v = await S("SELECT COUNTIF(amount > 20) FROM `{ds}.orders`");
		Assert.Equal("7", v); // Gadgets(25, 3 orders) + Thingamajigs(50, 3 orders) + Eve's Gadget = 25*4 + 50*3 = wait no, count by row: orders 2,5,10,12(amount=25) + 4,7,9(amount=50) = 7
	}

	// ---- LOGICAL_AND / LOGICAL_OR ----
	[Fact] public async Task LogicalAnd_AllReturned()
	{
		var rows = await Q("SELECT customer, LOGICAL_AND(is_returned) AS all_ret FROM `{ds}.orders` GROUP BY customer ORDER BY customer");
		// No customer has all orders returned
		Assert.Equal("False", rows[0]["all_ret"]?.ToString()); // Alice: all false
	}
	[Fact] public async Task LogicalOr_AnyReturned()
	{
		var rows = await Q("SELECT customer, LOGICAL_OR(is_returned) AS any_ret FROM `{ds}.orders` GROUP BY customer ORDER BY customer");
		Assert.Equal("False", rows[0]["any_ret"]?.ToString()); // Alice: no returns
		Assert.Equal("True", rows[1]["any_ret"]?.ToString()); // Bob: has return
	}

	// ---- ANY_VALUE ----
	[Fact] public async Task AnyValue_Basic()
	{
		var rows = await Q("SELECT category, ANY_VALUE(product) AS sample_product FROM `{ds}.orders` GROUP BY category ORDER BY category");
		Assert.Equal(3, rows.Count);
		Assert.NotNull(rows[0][1]); // has a value
	}

	// ---- STRING_AGG ----
	[Fact] public async Task StringAgg_Basic()
	{
		var rows = await Q("SELECT customer, STRING_AGG(product, ',') AS products FROM `{ds}.orders` GROUP BY customer ORDER BY customer LIMIT 1");
		Assert.NotNull(rows[0]["products"]);
		Assert.Contains("Widget", rows[0]["products"]!.ToString()!);
	}
	[Fact] public async Task StringAgg_OrderBy()
	{
		var rows = await Q("SELECT customer, STRING_AGG(product, ',' ORDER BY product) AS products FROM `{ds}.orders` GROUP BY customer ORDER BY customer LIMIT 1");
		// Alice: Gadget, Thingamajig, Widget (alphabetical)
		Assert.StartsWith("Gadget", rows[0]["products"]!.ToString()!);
	}

	// ---- ARRAY_AGG ----
	[Fact] public async Task ArrayAgg_Basic()
	{
		var v = await S("SELECT ARRAY_LENGTH(ARRAY_AGG(product)) FROM `{ds}.orders` WHERE customer = 'Alice'");
		Assert.Equal("3", v);
	}
	[Fact] public async Task ArrayAgg_ToString()
	{
		var v = await S("SELECT ARRAY_TO_STRING(ARRAY_AGG(category ORDER BY category), ',') FROM (SELECT DISTINCT category FROM `{ds}.orders`)");
		Assert.Equal("A,B,C", v);
	}

	// ---- Expressions in aggregation ----
	[Fact] public async Task Agg_SumExpr()
	{
		var v = await S("SELECT SUM(amount * qty) FROM `{ds}.orders`");
		Assert.NotNull(v);
		var total = double.Parse(v!);
		Assert.True(total > 0);
	}
	[Fact] public async Task Agg_CountDistinct()
	{
		var v = await S("SELECT COUNT(DISTINCT customer) FROM `{ds}.orders`");
		Assert.Equal("5", v);
	}
	[Fact] public async Task Agg_CountDistinctProduct()
	{
		var v = await S("SELECT COUNT(DISTINCT product) FROM `{ds}.orders`");
		Assert.Equal("3", v); // Widget, Gadget, Thingamajig
	}

	// ---- GROUP BY with CASE ----
	[Fact] public async Task GroupBy_CaseExpr()
	{
		var rows = await Q("SELECT CASE WHEN amount < 15 THEN 'Low' WHEN amount < 40 THEN 'Mid' ELSE 'High' END AS tier, COUNT(*) AS cnt FROM `{ds}.orders` GROUP BY tier ORDER BY tier");
		Assert.Equal(3, rows.Count);
	}

	// ---- Aggregate in WHERE not allowed (expect it in HAVING) ----
	[Fact] public async Task Having_WithoutAlias()
	{
		var rows = await Q("SELECT customer FROM `{ds}.orders` GROUP BY customer HAVING COUNT(*) > 2 ORDER BY customer");
		Assert.Equal(2, rows.Count); // Alice(3), Bob(3)
	}

	// ---- GROUP BY with NULL values ----
	[Fact] public async Task GroupBy_WithNull()
	{
		var rows = await Q(@"
			WITH data AS (
				SELECT 'a' AS grp, 1 AS val UNION ALL
				SELECT 'a', 2 UNION ALL
				SELECT NULL, 3 UNION ALL
				SELECT NULL, 4
			)
			SELECT grp, SUM(val) AS total FROM data GROUP BY grp ORDER BY grp NULLS LAST");
		Assert.Equal(2, rows.Count);
		Assert.Equal("a", rows[0]["grp"]?.ToString());
		Assert.Equal("3", rows[0]["total"]?.ToString());
		Assert.Null(rows[1]["grp"]);
		Assert.Equal("7", rows[1]["total"]?.ToString());
	}

	// ---- Aggregate over empty set ----
	[Fact] public async Task Agg_EmptySet_Count()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.orders` WHERE 1 = 0");
		Assert.Equal("0", v);
	}
	[Fact] public async Task Agg_EmptySet_Sum()
	{
		var v = await S("SELECT SUM(amount) FROM `{ds}.orders` WHERE 1 = 0");
		Assert.Null(v);
	}
	[Fact] public async Task Agg_EmptySet_Avg()
	{
		var v = await S("SELECT AVG(amount) FROM `{ds}.orders` WHERE 1 = 0");
		Assert.Null(v);
	}
}
