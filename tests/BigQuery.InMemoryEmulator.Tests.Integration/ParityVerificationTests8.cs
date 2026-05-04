using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Parity verification tests batch 8: Complex expressions, NULL propagation in aggregates,
/// TIMESTAMP formatting, subqueries, CTE edge cases, HAVING, window frames,
/// type coercion, and CASE expression edge cases.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests8 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests8(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv8_{Guid.NewGuid():N}"[..28];
		await _fixture.CreateDatasetAsync(_ds);

		var client = await _fixture.GetClientAsync();
		// Create test table for aggregate/window tests
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_ds}.items` (id INT64, category STRING, price FLOAT64, qty INT64, created DATE)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_ds}.items` (id, category, price, qty, created) VALUES
			(1, 'A', 10.5, 5, DATE '2024-01-01'),
			(2, 'A', 20.0, 3, DATE '2024-01-15'),
			(3, 'B', 15.0, 8, DATE '2024-02-01'),
			(4, 'B', 25.5, 2, DATE '2024-02-15'),
			(5, 'A', 30.0, 1, DATE '2024-03-01'),
			(6, 'C', NULL, 4, DATE '2024-03-15'),
			(7, 'C', 5.0, NULL, DATE '2024-04-01')", parameters: null);
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

	private async Task<List<string?>> Col(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null);
		return result.ToList().Select(r => r[0]?.ToString()).ToList();
	}

	private async Task<List<BigQueryRow>> Q(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null);
		return result.ToList();
	}

	// ───────────────────────────────────────────────────────────────────────────
	// NULL handling in aggregates
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions
	//   "Most aggregate functions ignore NULL values."
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Sum_IgnoresNulls()
	{
		// price has NULL for id=6, SUM should ignore it
		var result = await S("SELECT CAST(SUM(price) AS STRING) FROM `{ds}.items`");
		Assert.Equal("106.0", result);
	}

	[Fact] public async Task Avg_IgnoresNulls()
	{
		// 6 non-null prices: (10.5+20+15+25.5+30+5)/6 = 17.666...
		var result = await S("SELECT ROUND(AVG(price), 2) FROM `{ds}.items`");
		Assert.Equal("17.67", result);
	}

	[Fact] public async Task Count_Star_IncludesNulls()
	{
		var result = await S("SELECT COUNT(*) FROM `{ds}.items`");
		Assert.Equal("7", result);
	}

	[Fact] public async Task Count_Column_ExcludesNulls()
	{
		var result = await S("SELECT COUNT(price) FROM `{ds}.items`");
		Assert.Equal("6", result);
	}

	[Fact] public async Task Count_NullColumn()
	{
		var result = await S("SELECT COUNT(qty) FROM `{ds}.items`");
		Assert.Equal("6", result);
	}

	[Fact] public async Task Min_IgnoresNulls()
	{
		var result = await S("SELECT MIN(price) FROM `{ds}.items`");
		Assert.Equal("5", result);
	}

	[Fact] public async Task Max_IgnoresNulls()
	{
		var result = await S("SELECT MAX(price) FROM `{ds}.items`");
		Assert.Equal("30", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// GROUP BY with aggregates
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task GroupBy_CountPerCategory()
	{
		var rows = await Q("SELECT category, COUNT(*) AS cnt FROM `{ds}.items` GROUP BY category ORDER BY category");
		Assert.Equal(3, rows.Count);
		Assert.Equal("A", rows[0]["category"]?.ToString());
		Assert.Equal("3", rows[0]["cnt"]?.ToString());
		Assert.Equal("B", rows[1]["category"]?.ToString());
		Assert.Equal("2", rows[1]["cnt"]?.ToString());
	}

	[Fact] public async Task GroupBy_SumPerCategory()
	{
		var result = await S("SELECT CAST(SUM(price) AS STRING) FROM `{ds}.items` WHERE category = 'A'");
		Assert.Equal("60.5", result);
	}

	[Fact] public async Task Having_FilterGroups()
	{
		var rows = await Q("SELECT category, COUNT(*) AS cnt FROM `{ds}.items` GROUP BY category HAVING COUNT(*) >= 3 ORDER BY category");
		Assert.Single(rows);
		Assert.Equal("A", rows[0]["category"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Window functions with frames
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls#def_window_frame
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Window_RunningSum()
	{
		var rows = await Q(@"
			SELECT id, CAST(SUM(price) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS STRING) AS running
			FROM `{ds}.items` WHERE category = 'A' ORDER BY id");
		Assert.Equal("10.5", rows[0]["running"]?.ToString());
		Assert.Equal("30.5", rows[1]["running"]?.ToString());
		Assert.Equal("60.5", rows[2]["running"]?.ToString());
	}

	[Fact] public async Task Window_RowNumber()
	{
		var rows = await Q("SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM `{ds}.items` ORDER BY id");
		Assert.Equal("1", rows[0]["rn"]?.ToString());
		Assert.Equal("7", rows[6]["rn"]?.ToString());
	}

	[Fact] public async Task Window_Rank_WithTies()
	{
		// All items in same category get same rank
		var rows = await Q(@"
			SELECT id, category, RANK() OVER (ORDER BY category) AS rnk 
			FROM `{ds}.items` ORDER BY category, id");
		// 'A' items get rank 1, 'B' get rank 4, 'C' get rank 6
		Assert.Equal("1", rows[0]["rnk"]?.ToString());
		Assert.Equal("1", rows[2]["rnk"]?.ToString());
		Assert.Equal("4", rows[3]["rnk"]?.ToString());
	}

	[Fact] public async Task Window_DenseRank()
	{
		var rows = await Q(@"
			SELECT id, category, DENSE_RANK() OVER (ORDER BY category) AS drnk 
			FROM `{ds}.items` ORDER BY category, id");
		Assert.Equal("1", rows[0]["drnk"]?.ToString());
		Assert.Equal("2", rows[3]["drnk"]?.ToString());
		Assert.Equal("3", rows[5]["drnk"]?.ToString());
	}

	[Fact] public async Task Window_PartitionBy()
	{
		var rows = await Q(@"
			SELECT id, category, ROW_NUMBER() OVER (PARTITION BY category ORDER BY id) AS rn 
			FROM `{ds}.items` ORDER BY category, id");
		// First item in each category gets rn=1
		Assert.Equal("1", rows[0]["rn"]?.ToString()); // A, id=1
		Assert.Equal("2", rows[1]["rn"]?.ToString()); // A, id=2
		Assert.Equal("1", rows[3]["rn"]?.ToString()); // B, id=3
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Subqueries
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ScalarSubquery()
	{
		var result = await S("SELECT (SELECT MAX(price) FROM `{ds}.items`)");
		Assert.Equal("30", result);
	}

	[Fact] public async Task InSubquery()
	{
		var rows = await Q(@"
			SELECT id FROM `{ds}.items` 
			WHERE category IN (SELECT category FROM `{ds}.items` WHERE price > 20) 
			ORDER BY id");
		// Categories with price > 20: A (30.0), B (25.5)
		Assert.Equal(5, rows.Count); // ids 1,2,3,4,5
	}

	[Fact] public async Task ExistsSubquery()
	{
		var rows = await Q(@"
			SELECT id FROM `{ds}.items` i
			WHERE EXISTS (SELECT 1 FROM `{ds}.items` i2 WHERE i2.category = i.category AND i2.price > 20)
			ORDER BY id");
		Assert.Equal(5, rows.Count);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// CTEs (Common Table Expressions)
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_clause
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Cte_Basic()
	{
		var result = await S(@"
			WITH expensive AS (SELECT * FROM `{ds}.items` WHERE price > 20)
			SELECT COUNT(*) FROM expensive");
		Assert.Equal("2", result);
	}

	[Fact] public async Task Cte_Multiple()
	{
		var result = await S(@"
			WITH 
				cat_a AS (SELECT * FROM `{ds}.items` WHERE category = 'A'),
				cat_b AS (SELECT * FROM `{ds}.items` WHERE category = 'B')
			SELECT (SELECT COUNT(*) FROM cat_a) + (SELECT COUNT(*) FROM cat_b)");
		Assert.Equal("5", result);
	}

	[Fact] public async Task Cte_ReferencedMultipleTimes()
	{
		var result = await S(@"
			WITH data AS (SELECT price FROM `{ds}.items` WHERE price IS NOT NULL)
			SELECT CAST((SELECT MAX(price) FROM data) - (SELECT MIN(price) FROM data) AS STRING)");
		Assert.Equal("25.0", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// CASE expression edge cases
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Case_NoElse_ReturnsNull()
	{
		var result = await S("SELECT CASE WHEN 1 > 2 THEN 'yes' END");
		Assert.Null(result);
	}

	[Fact] public async Task Case_MultipleWhens()
	{
		var result = await S(@"
			SELECT CASE 
				WHEN price > 25 THEN 'expensive'
				WHEN price > 15 THEN 'moderate'
				ELSE 'cheap'
			END FROM `{ds}.items` WHERE id = 5");
		Assert.Equal("expensive", result);
	}

	[Fact] public async Task Case_SimpleForm()
	{
		var result = await S("SELECT CASE category WHEN 'A' THEN 'Alpha' WHEN 'B' THEN 'Beta' ELSE 'Other' END FROM `{ds}.items` WHERE id = 1");
		Assert.Equal("Alpha", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Type coercion in expressions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task IntPlusFloat_ProducesFloat()
	{
		// INT64 + FLOAT64 → FLOAT64
		var result = await S("SELECT CAST(qty + price AS STRING) FROM `{ds}.items` WHERE id = 1");
		Assert.Equal("15.5", result);
	}

	[Fact] public async Task IntMultiplyInt_ProducesInt()
	{
		var result = await S("SELECT qty * 10 FROM `{ds}.items` WHERE id = 1");
		Assert.Equal("50", result);
	}

	[Fact] public async Task DivisionOfInts_ProducesFloat()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#division_operator
		//   "Division by / always produces FLOAT64 result"
		var result = await S("SELECT 10 / 4");
		Assert.Equal("2.5", result);
	}

	[Fact] public async Task GenerateArray_FloatStep()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_array
		//   "GENERATE_ARRAY supports FLOAT64 step values"
		var result = await S("SELECT ARRAY_TO_STRING(GENERATE_ARRAY(0, 1.0, 0.5), ',')");
		Assert.Equal("0.0,0.5,1.0", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// String expressions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task StringConcat_Operator()
	{
		// || operator for string concatenation
		var result = await S("SELECT 'hello' || ' ' || 'world'");
		Assert.Equal("hello world", result);
	}

	[Fact] public async Task Like_Wildcard()
	{
		var rows = await Q("SELECT id FROM `{ds}.items` WHERE category LIKE 'A' ORDER BY id");
		Assert.Equal(3, rows.Count);
	}

	[Fact] public async Task Like_Percent()
	{
		// All categories start with a letter, LIKE with % should match
		var rows = await Q("SELECT id FROM `{ds}.items` WHERE category LIKE '%' ORDER BY id");
		Assert.Equal(7, rows.Count);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// BETWEEN
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Between_Inclusive()
	{
		var rows = await Q("SELECT id FROM `{ds}.items` WHERE price BETWEEN 15 AND 25.5 ORDER BY id");
		// prices: 10.5, 20.0, 15.0, 25.5, 30.0, NULL, 5.0
		// matches: 20.0(id=2), 15.0(id=3), 25.5(id=4)
		Assert.Equal(3, rows.Count);
	}

	[Fact] public async Task NotBetween()
	{
		var rows = await Q("SELECT id FROM `{ds}.items` WHERE price NOT BETWEEN 10 AND 20 ORDER BY id");
		// NOT BETWEEN: 25.5(id=4), 30.0(id=5), 5.0(id=7) — NULL excluded
		Assert.Equal(3, rows.Count);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// IN with literals
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task In_StringList()
	{
		var rows = await Q("SELECT id FROM `{ds}.items` WHERE category IN ('A', 'C') ORDER BY id");
		Assert.Equal(5, rows.Count); // A: 1,2,5; C: 6,7
	}

	[Fact] public async Task NotIn_StringList()
	{
		var rows = await Q("SELECT id FROM `{ds}.items` WHERE category NOT IN ('A', 'C') ORDER BY id");
		Assert.Equal(2, rows.Count); // B: 3,4
	}

	// ───────────────────────────────────────────────────────────────────────────
	// DISTINCT
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task SelectDistinct()
	{
		var rows = await Col("SELECT DISTINCT category FROM `{ds}.items` ORDER BY category");
		Assert.Equal(3, rows.Count);
		Assert.Equal("A", rows[0]);
		Assert.Equal("B", rows[1]);
		Assert.Equal("C", rows[2]);
	}

	[Fact] public async Task CountDistinct()
	{
		var result = await S("SELECT COUNT(DISTINCT category) FROM `{ds}.items`");
		Assert.Equal("3", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// UNION ALL / UNION DISTINCT
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task UnionAll()
	{
		var result = await S(@"
			SELECT COUNT(*) FROM (
				SELECT id FROM `{ds}.items` WHERE category = 'A'
				UNION ALL
				SELECT id FROM `{ds}.items` WHERE category = 'B'
			)");
		Assert.Equal("5", result);
	}

	[Fact] public async Task UnionDistinct()
	{
		var result = await S(@"
			SELECT COUNT(*) FROM (
				SELECT category FROM `{ds}.items`
				UNION DISTINCT
				SELECT category FROM `{ds}.items`
			)");
		Assert.Equal("3", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// ORDER BY + LIMIT + OFFSET
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Limit()
	{
		var rows = await Q("SELECT id FROM `{ds}.items` ORDER BY id LIMIT 3");
		Assert.Equal(3, rows.Count);
		Assert.Equal("3", rows[2]["id"]?.ToString());
	}

	[Fact] public async Task LimitOffset()
	{
		var rows = await Q("SELECT id FROM `{ds}.items` ORDER BY id LIMIT 2 OFFSET 3");
		Assert.Equal(2, rows.Count);
		Assert.Equal("4", rows[0]["id"]?.ToString());
		Assert.Equal("5", rows[1]["id"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// IS NULL / IS NOT NULL
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task IsNull()
	{
		var rows = await Q("SELECT id FROM `{ds}.items` WHERE price IS NULL");
		Assert.Single(rows);
		Assert.Equal("6", rows[0]["id"]?.ToString());
	}

	[Fact] public async Task IsNotNull()
	{
		var rows = await Q("SELECT id FROM `{ds}.items` WHERE price IS NOT NULL ORDER BY id");
		Assert.Equal(6, rows.Count);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Arithmetic with NULL
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators
	//   "Any operation with NULL produces NULL"
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Null_Plus_Int_IsNull()
	{
		var result = await S("SELECT NULL + 5");
		Assert.Null(result);
	}

	[Fact] public async Task Null_Multiply_IsNull()
	{
		var result = await S("SELECT price * qty FROM `{ds}.items` WHERE id = 6");
		Assert.Null(result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Multiple columns with aliases
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task MultiColumn_WithAlias()
	{
		var rows = await Q("SELECT id AS item_id, category AS cat, price AS cost FROM `{ds}.items` WHERE id = 1");
		Assert.Single(rows);
		Assert.Equal("1", rows[0]["item_id"]?.ToString());
		Assert.Equal("A", rows[0]["cat"]?.ToString());
		Assert.Equal("10.5", rows[0]["cost"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Expression in SELECT
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ComputedColumn()
	{
		var result = await S("SELECT CAST(price * qty AS STRING) FROM `{ds}.items` WHERE id = 1");
		Assert.Equal("52.5", result);
	}

	[Fact] public async Task RoundExpression()
	{
		var result = await S("SELECT ROUND(price * 1.1, 1) FROM `{ds}.items` WHERE id = 1");
		Assert.Equal("11.6", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// CAST TIMESTAMP edge cases
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task CastTimestamp_WithFractional()
	{
		// Fractional seconds should be included when non-zero
		var result = await S("SELECT CAST(TIMESTAMP '2024-01-01 12:30:45.123456 UTC' AS STRING)");
		Assert.Equal("2024-01-01 12:30:45.123456+00", result);
	}

	[Fact] public async Task CastTimestamp_NoFractional()
	{
		var result = await S("SELECT CAST(TIMESTAMP '2024-01-01 12:30:45 UTC' AS STRING)");
		Assert.Equal("2024-01-01 12:30:45+00", result);
	}

	[Fact] public async Task CastTimestamp_TrailingZerosTrimmed()
	{
		// .100000 should be trimmed to .1
		var result = await S("SELECT CAST(TIMESTAMP '2024-01-01 12:30:45.100000 UTC' AS STRING)");
		Assert.Equal("2024-01-01 12:30:45.1+00", result);
	}
}
