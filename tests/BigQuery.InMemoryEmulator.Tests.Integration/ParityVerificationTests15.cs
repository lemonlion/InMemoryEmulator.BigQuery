using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Parity verification tests batch 15: Complex query patterns that stress the emulator -
/// nested window functions, multi-level CTEs with aggregates, ARRAY_AGG of STRUCTs,
/// TIMESTAMP precision handling, complex JOIN conditions, LATERAL joins, GROUP BY 
/// ordinal references, TABLESAMPLE (if supported), and complex MERGE patterns.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests15 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests15(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv15_{Guid.NewGuid():N}"[..28];
		await _fixture.CreateDatasetAsync(_ds);

		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_ds}.sales` (id INT64, product STRING, category STRING, amount FLOAT64, sale_date DATE)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_ds}.sales` (id, product, category, amount, sale_date) VALUES
			(1, 'Widget', 'Hardware', 10.00, DATE '2024-01-01'),
			(2, 'Gadget', 'Hardware', 25.00, DATE '2024-01-15'),
			(3, 'Widget', 'Hardware', 10.00, DATE '2024-02-01'),
			(4, 'Service', 'Software', 100.00, DATE '2024-02-15'),
			(5, 'License', 'Software', 50.00, DATE '2024-03-01'),
			(6, 'Widget', 'Hardware', 10.00, DATE '2024-03-15'),
			(7, 'Gadget', 'Hardware', 25.00, DATE '2024-03-20'),
			(8, 'Service', 'Software', 100.00, DATE '2024-03-25')", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try
		{
			var c = await _fixture.GetClientAsync();
			await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true });
		}
		catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null);
		var rows = result.ToList();
		return rows.Count == 0 ? null : rows[0][0]?.ToString();
	}

	private async Task<List<BigQueryRow>> Q(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null);
		return result.ToList();
	}

	// ───────────────────────────────────────────────────────────────────────────
	// GROUP BY ordinal reference
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#group_by_clause
	//   "You can use column references (1, 2, etc.) in the GROUP BY clause."
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task GroupBy_Ordinal()
	{
		var rows = await Q(@"
			SELECT category, COUNT(*) AS cnt
			FROM `{ds}.sales`
			GROUP BY 1
			ORDER BY 1");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Hardware", rows[0]["category"]?.ToString());
		Assert.Equal("5", rows[0]["cnt"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Window function with PARTITION BY + ORDER BY
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Window_PartitionByOrderBy()
	{
		var rows = await Q(@"
			SELECT product, category, amount,
				ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) AS rn
			FROM `{ds}.sales`
			ORDER BY category, rn");
		// Hardware first (alphabetically): Gadget=25 is rn=1
		Assert.Equal("Gadget", rows[0]["product"]?.ToString());
		Assert.Equal("1", rows[0]["rn"]?.ToString());
	}

	[Fact] public async Task Window_Lag_Partitioned()
	{
		var rows = await Q(@"
			SELECT product, sale_date, 
				LAG(CAST(sale_date AS STRING)) OVER (PARTITION BY product ORDER BY sale_date) AS prev_sale
			FROM `{ds}.sales`
			WHERE product = 'Widget'
			ORDER BY sale_date");
		Assert.Equal(3, rows.Count);
		Assert.Null(rows[0]["prev_sale"]); // First widget sale has no previous
		Assert.Equal("2024-01-01", rows[1]["prev_sale"]?.ToString());
		Assert.Equal("2024-02-01", rows[2]["prev_sale"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// CTE with aggregate then JOIN back
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Cte_AggThenJoin()
	{
		var rows = await Q(@"
			WITH totals AS (
				SELECT category, SUM(amount) AS total
				FROM `{ds}.sales`
				GROUP BY category
			)
			SELECT s.product, s.amount, t.total
			FROM `{ds}.sales` s
			JOIN totals t ON s.category = t.category
			WHERE s.id = 4
			ORDER BY s.id");
		Assert.Equal("Service", rows[0]["product"]?.ToString());
		Assert.Equal("250", rows[0]["total"]?.ToString()); // Software total: 100+50+100=250
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Cumulative/running totals
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task RunningTotal_ByCategory()
	{
		var rows = await Q(@"
			SELECT id, category, amount,
				CAST(SUM(amount) OVER (PARTITION BY category ORDER BY id) AS STRING) AS cumulative
			FROM `{ds}.sales`
			WHERE category = 'Software'
			ORDER BY id");
		Assert.Equal("100.0", rows[0]["cumulative"]?.ToString()); // id=4: 100
		Assert.Equal("150.0", rows[1]["cumulative"]?.ToString()); // id=5: 100+50=150
		Assert.Equal("250.0", rows[2]["cumulative"]?.ToString()); // id=8: 100+50+100=250
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Subquery in SELECT (scalar subquery)
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ScalarSubquery_Max()
	{
		var rows = await Q(@"
			SELECT product, amount, 
				(SELECT MAX(amount) FROM `{ds}.sales`) AS max_amount
			FROM `{ds}.sales`
			WHERE id = 1");
		Assert.Equal("100", rows[0]["max_amount"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// CASE with aggregation
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Case_PivotLike()
	{
		var rows = await Q(@"
			SELECT 
				product,
				SUM(CASE WHEN EXTRACT(MONTH FROM sale_date) = 1 THEN amount ELSE 0 END) AS jan_total,
				SUM(CASE WHEN EXTRACT(MONTH FROM sale_date) = 2 THEN amount ELSE 0 END) AS feb_total,
				SUM(CASE WHEN EXTRACT(MONTH FROM sale_date) = 3 THEN amount ELSE 0 END) AS mar_total
			FROM `{ds}.sales`
			GROUP BY product
			HAVING SUM(amount) > 20
			ORDER BY product");
		// Gadget: Jan=25, Feb=0, Mar=25
		Assert.Equal("Gadget", rows[0]["product"]?.ToString());
		Assert.Equal("25", rows[0]["jan_total"]?.ToString());
		Assert.Equal("0", rows[0]["feb_total"]?.ToString());
		Assert.Equal("25", rows[0]["mar_total"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Nested aggregation via subquery
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task NestedAggregation()
	{
		var result = await S(@"
			SELECT CAST(AVG(cat_total) AS STRING) FROM (
				SELECT category, SUM(amount) AS cat_total
				FROM `{ds}.sales`
				GROUP BY category
			)");
		// Hardware: 10+25+10+10+25=80; Software: 100+50+100=250; AVG=(80+250)/2=165
		Assert.Equal("165.0", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// DISTINCT with multiple columns
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Distinct_MultiColumn()
	{
		var result = await S(@"
			SELECT COUNT(*) FROM (
				SELECT DISTINCT category, product FROM `{ds}.sales`
			)");
		// Hardware/Widget, Hardware/Gadget, Software/Service, Software/License = 4
		Assert.Equal("4", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// ARRAY_AGG of complex expressions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ArrayAgg_Expression()
	{
		var result = await S(@"
			SELECT ARRAY_TO_STRING(ARRAY_AGG(CONCAT(product, ':', CAST(CAST(amount AS INT64) AS STRING)) ORDER BY product, amount), '; ')
			FROM `{ds}.sales`
			WHERE category = 'Software'");
		Assert.Equal("License:50; Service:100; Service:100", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Multiple ORDER BY columns with mixed ASC/DESC
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task OrderBy_MixedAscDesc()
	{
		var rows = await Q(@"
			SELECT product, CAST(amount AS STRING) AS amt
			FROM `{ds}.sales`
			ORDER BY amount DESC, product ASC
			LIMIT 3");
		// 100 (Service), 100 (Service), 50 (License)
		Assert.Equal("Service", rows[0]["product"]?.ToString());
		Assert.Equal("100.0", rows[0]["amt"]?.ToString());
		Assert.Equal("License", rows[2]["product"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// GROUP BY with expression
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task GroupBy_Expression()
	{
		var rows = await Q(@"
			SELECT EXTRACT(MONTH FROM sale_date) AS month, SUM(amount) AS total
			FROM `{ds}.sales`
			GROUP BY EXTRACT(MONTH FROM sale_date)
			ORDER BY month");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]["month"]?.ToString());
		Assert.Equal("35", rows[0]["total"]?.ToString()); // Jan: 10+25=35
	}

	// ───────────────────────────────────────────────────────────────────────────
	// HAVING referencing alias (BigQuery supports this)
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#having_clause
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Having_ReferencingAlias()
	{
		var rows = await Q(@"
			SELECT product, COUNT(*) AS cnt
			FROM `{ds}.sales`
			GROUP BY product
			HAVING cnt >= 3
			ORDER BY product");
		Assert.Single(rows);
		Assert.Equal("Widget", rows[0]["product"]?.ToString());
		Assert.Equal("3", rows[0]["cnt"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Complex CONCAT in GROUP BY key
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task GroupBy_ConcatKey()
	{
		var rows = await Q(@"
			SELECT CONCAT(category, '-', product) AS key, SUM(amount) AS total
			FROM `{ds}.sales`
			GROUP BY key
			ORDER BY key
			LIMIT 3");
		Assert.Equal("Hardware-Gadget", rows[0]["key"]?.ToString());
		Assert.Equal("50", rows[0]["total"]?.ToString()); // 25+25
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Window function: frame with CURRENT ROW to UNBOUNDED FOLLOWING
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Window_RemainingSum()
	{
		var rows = await Q(@"
			SELECT val, SUM(val) OVER (ORDER BY val ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS remaining
			FROM UNNEST([10, 20, 30, 40]) AS val
			ORDER BY val");
		Assert.Equal("100", rows[0]["remaining"]?.ToString()); // 10+20+30+40
		Assert.Equal("90", rows[1]["remaining"]?.ToString()); // 20+30+40
		Assert.Equal("70", rows[2]["remaining"]?.ToString()); // 30+40
		Assert.Equal("40", rows[3]["remaining"]?.ToString()); // 40
	}

	// ───────────────────────────────────────────────────────────────────────────
	// COUNT(*) vs COUNT(col) with NULLs
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Count_Star_vs_Column()
	{
		var rows = await Q(@"
			SELECT COUNT(*) AS cnt_star, COUNT(x) AS cnt_x
			FROM UNNEST([CAST(1 AS INT64), NULL, 3, NULL, 5]) AS x");
		Assert.Equal("5", rows[0]["cnt_star"]?.ToString());
		Assert.Equal("3", rows[0]["cnt_x"]?.ToString());
	}
}
