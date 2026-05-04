using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Parity verification tests batch 13: Date/time arithmetic edge cases,
/// complex window functions, multi-table operations, QUALIFY, ROLLUP/CUBE,
/// array subqueries, OFFSET/ORDINAL access, and more expression edge cases.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests13 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests13(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv13_{Guid.NewGuid():N}"[..28];
		await _fixture.CreateDatasetAsync(_ds);

		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_ds}.orders` (id INT64, customer STRING, amount FLOAT64, order_date DATE)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_ds}.orders` (id, customer, amount, order_date) VALUES
			(1, 'Alice', 100.50, DATE '2024-01-15'),
			(2, 'Bob', 200.75, DATE '2024-01-16'),
			(3, 'Alice', 50.25, DATE '2024-01-17'),
			(4, 'Charlie', 300.00, DATE '2024-02-01'),
			(5, 'Bob', 150.00, DATE '2024-02-15'),
			(6, 'Alice', 75.00, DATE '2024-03-01')", parameters: null);
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
	// DATE_DIFF
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_diff
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task DateDiff_Days()
	{
		var result = await S("SELECT DATE_DIFF(DATE '2024-03-01', DATE '2024-01-15', DAY)");
		Assert.Equal("46", result);
	}

	[Fact] public async Task DateDiff_Months()
	{
		var result = await S("SELECT DATE_DIFF(DATE '2024-03-15', DATE '2024-01-15', MONTH)");
		Assert.Equal("2", result);
	}

	[Fact] public async Task DateDiff_Weeks()
	{
		// DATE_DIFF counts week boundaries (Sundays) crossed
		var result = await S("SELECT DATE_DIFF(DATE '2024-01-22', DATE '2024-01-15', WEEK)");
		Assert.Equal("1", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// TIMESTAMP_DIFF
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_diff
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task TimestampDiff_Hours()
	{
		var result = await S("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-02 10:00:00 UTC', TIMESTAMP '2024-01-01 10:00:00 UTC', HOUR)");
		Assert.Equal("24", result);
	}

	[Fact] public async Task TimestampDiff_Minutes()
	{
		var result = await S("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-01 10:30:00 UTC', TIMESTAMP '2024-01-01 10:00:00 UTC', MINUTE)");
		Assert.Equal("30", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// QUALIFY
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#qualify_clause
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Qualify_RowNumber()
	{
		var rows = await Q(@"
			SELECT customer, CAST(amount AS STRING) AS amount
			FROM `{ds}.orders`
			QUALIFY ROW_NUMBER() OVER (PARTITION BY customer ORDER BY amount DESC) = 1
			ORDER BY customer");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Alice", rows[0]["customer"]?.ToString());
		Assert.Equal("100.5", rows[0]["amount"]?.ToString()); // Alice's highest
		Assert.Equal("Bob", rows[1]["customer"]?.ToString());
		Assert.Equal("200.75", rows[1]["amount"]?.ToString()); // Bob's highest
	}

	[Fact] public async Task Qualify_Rank()
	{
		var rows = await Q(@"
			SELECT customer, id
			FROM `{ds}.orders`
			QUALIFY RANK() OVER (ORDER BY amount DESC) <= 2
			ORDER BY id");
		Assert.Equal(2, rows.Count); // Top 2 by amount: Charlie(300), Bob(200.75)
		Assert.Equal("Bob", rows[0]["customer"]?.ToString());
		Assert.Equal("Charlie", rows[1]["customer"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// ROLLUP in GROUP BY
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#group_by_rollup
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task GroupBy_Rollup()
	{
		var rows = await Q(@"
			SELECT customer, COUNT(*) AS cnt
			FROM `{ds}.orders`
			GROUP BY ROLLUP(customer)
			ORDER BY customer NULLS LAST");
		// Should include per-customer groups + grand total (NULL customer)
		Assert.True(rows.Count >= 4); // Alice, Bob, Charlie + grand total
		var grandTotal = rows.Last();
		Assert.Null(grandTotal["customer"]);
		Assert.Equal("6", grandTotal["cnt"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Array subscript access
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_subscript_operator
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ArrayOffset_Basic()
	{
		var result = await S("SELECT ['a', 'b', 'c'][OFFSET(1)]");
		Assert.Equal("b", result); // 0-based
	}

	[Fact] public async Task ArrayOrdinal_Basic()
	{
		var result = await S("SELECT ['a', 'b', 'c'][ORDINAL(1)]");
		Assert.Equal("a", result); // 1-based
	}

	[Fact] public async Task ArraySafeOffset_OutOfBounds()
	{
		var result = await S("SELECT ['a', 'b', 'c'][SAFE_OFFSET(10)]");
		Assert.Null(result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// LAG/LEAD with offset and default
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#lag
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Lag_WithDefault()
	{
		var rows = await Q(@"
			SELECT val, LAG(val, 1, -1) OVER (ORDER BY val) AS prev
			FROM UNNEST([10, 20, 30]) AS val
			ORDER BY val");
		Assert.Equal("-1", rows[0]["prev"]?.ToString()); // No preceding row, default -1
		Assert.Equal("10", rows[1]["prev"]?.ToString());
	}

	[Fact] public async Task Lead_WithDefault()
	{
		var rows = await Q(@"
			SELECT val, LEAD(val, 1, 999) OVER (ORDER BY val) AS nxt
			FROM UNNEST([10, 20, 30]) AS val
			ORDER BY val");
		Assert.Equal("20", rows[0]["nxt"]?.ToString());
		Assert.Equal("999", rows[2]["nxt"]?.ToString()); // No following row, default 999
	}

	[Fact] public async Task Lag_Offset2()
	{
		var rows = await Q(@"
			SELECT val, LAG(val, 2) OVER (ORDER BY val) AS prev2
			FROM UNNEST([10, 20, 30, 40]) AS val
			ORDER BY val");
		Assert.Null(rows[0]["prev2"]); // no 2 rows before
		Assert.Null(rows[1]["prev2"]); // no 2 rows before
		Assert.Equal("10", rows[2]["prev2"]?.ToString()); // 30's lag(2) = 10
	}

	// ───────────────────────────────────────────────────────────────────────────
	// ARRAY subquery
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ArraySubquery()
	{
		var result = await S(@"
			SELECT ARRAY_TO_STRING(ARRAY(SELECT x * 2 FROM UNNEST([1, 2, 3]) AS x), ',')");
		Assert.Equal("2,4,6", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// COALESCE with multiple args
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Coalesce_SkipsMultipleNulls()
	{
		var result = await S("SELECT COALESCE(NULL, NULL, NULL, 'found', 'ignored')");
		Assert.Equal("found", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Complex expressions: nested functions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task NestedFunctions()
	{
		var result = await S("SELECT UPPER(CONCAT(LEFT('hello', 3), RIGHT('world', 3)))");
		Assert.Equal("HELRLD", result);
	}

	[Fact] public async Task Concat_Number_Cast()
	{
		var result = await S("SELECT CONCAT('Order #', CAST(42 AS STRING), ' total: $', CAST(99.99 AS STRING))");
		Assert.Equal("Order #42 total: $99.99", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// IN with subquery
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task In_Subquery()
	{
		var rows = await Q(@"
			SELECT customer FROM `{ds}.orders`
			WHERE customer IN (SELECT x FROM UNNEST(['Alice', 'Charlie']) AS x)
			GROUP BY customer
			ORDER BY customer");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Alice", rows[0]["customer"]?.ToString());
		Assert.Equal("Charlie", rows[1]["customer"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// EXISTS subquery
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Exists_Subquery()
	{
		var result = await S(@"
			SELECT COUNT(*) FROM `{ds}.orders` o
			WHERE EXISTS (SELECT 1 FROM `{ds}.orders` o2 WHERE o2.customer = o.customer AND o2.id != o.id)");
		// Customers with multiple orders: Alice (3), Bob (2) = 5 rows
		Assert.Equal("5", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// TIMESTAMP_ADD / TIMESTAMP_SUB
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_add
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task TimestampAdd_Days()
	{
		var result = await S("SELECT CAST(TIMESTAMP_ADD(TIMESTAMP '2024-01-15 10:00:00 UTC', INTERVAL 3 DAY) AS STRING)");
		Assert.Equal("2024-01-18 10:00:00+00", result);
	}

	[Fact] public async Task TimestampSub_Hours()
	{
		var result = await S("SELECT CAST(TIMESTAMP_SUB(TIMESTAMP '2024-01-15 10:00:00 UTC', INTERVAL 5 HOUR) AS STRING)");
		Assert.Equal("2024-01-15 05:00:00+00", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// ROUND, TRUNC with precision
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#round
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Round_WithPrecision()
	{
		var result = await S("SELECT CAST(ROUND(3.14159, 2) AS STRING)");
		Assert.Equal("3.14", result);
	}

	[Fact] public async Task Trunc_WithPrecision()
	{
		var result = await S("SELECT CAST(TRUNC(3.14159, 2) AS STRING)");
		Assert.Equal("3.14", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// SAFE_OFFSET / SAFE_ORDINAL
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task SafeOrdinal_OutOfBounds()
	{
		var result = await S("SELECT ['a', 'b', 'c'][SAFE_ORDINAL(10)]");
		Assert.Null(result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// STRING functions on NULL
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Concat_WithNull()
	{
		var result = await S("SELECT CONCAT('hello', NULL, 'world')");
		Assert.Null(result);
	}

	[Fact] public async Task Length_Null()
	{
		var result = await S("SELECT LENGTH(NULL)");
		Assert.Null(result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// BETWEEN with dates
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Between_Dates()
	{
		var result = await S(@"
			SELECT COUNT(*) FROM `{ds}.orders`
			WHERE order_date BETWEEN DATE '2024-01-15' AND DATE '2024-01-31'");
		Assert.Equal("3", result); // orders 1, 2, 3
	}
}
