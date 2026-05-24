using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 29: Advanced aggregation edge cases,
/// STRUCT construction, ARRAY subqueries, window function combinations,
/// and complex type operations.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests29 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests29(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv29_{Guid.NewGuid():N}"[..28];
		await _fixture.CreateDatasetAsync(_ds);
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

	private async Task Exec(string sql)
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// STRUCT construction and access
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Struct_Construction()
	{
		var result = await S("SELECT STRUCT(1 AS a, 'hello' AS b).a");
		Assert.Equal("1", result);
	}

	[Fact] public async Task Struct_StringAccess()
	{
		var result = await S("SELECT STRUCT(1 AS a, 'hello' AS b).b");
		Assert.Equal("hello", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// ARRAY subquery
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Array_Subquery_Basic()
	{
		var result = await S("SELECT ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST([1,2,3,4,5]) AS x WHERE x > 2))");
		Assert.Equal("3", result); // [3, 4, 5]
	}

	[Fact] public async Task Array_Subquery_OrderBy()
	{
		var result = await S("SELECT ARRAY_TO_STRING(ARRAY(SELECT CAST(x AS STRING) FROM UNNEST([3,1,4,1,5]) AS x ORDER BY x), ',')");
		Assert.Equal("1,1,3,4,5", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// CROSS JOIN
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task CrossJoin_Basic()
	{
		var rows = await Q(@"
			SELECT a.x, b.y
			FROM (SELECT 1 AS x UNION ALL SELECT 2) a
			CROSS JOIN (SELECT 'a' AS y UNION ALL SELECT 'b') b
			ORDER BY a.x, b.y");
		Assert.Equal(4, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("a", rows[0][1]?.ToString());
		Assert.Equal("1", rows[1][0]?.ToString());
		Assert.Equal("b", rows[1][1]?.ToString());
		Assert.Equal("2", rows[2][0]?.ToString());
		Assert.Equal("a", rows[2][1]?.ToString());
		Assert.Equal("2", rows[3][0]?.ToString());
		Assert.Equal("b", rows[3][1]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// FULL OUTER JOIN
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task FullOuterJoin_Basic()
	{
		var rows = await Q(@"
			WITH left_t AS (SELECT 1 AS id, 'a' AS val UNION ALL SELECT 2, 'b'),
			     right_t AS (SELECT 2 AS id, 'x' AS val UNION ALL SELECT 3, 'y')
			SELECT l.id, l.val, r.id, r.val
			FROM left_t l FULL OUTER JOIN right_t r ON l.id = r.id
			ORDER BY COALESCE(l.id, r.id)");
		Assert.Equal(3, rows.Count);
		// id=1: only in left
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("a", rows[0][1]?.ToString());
		Assert.Null(rows[0][2]);
		Assert.Null(rows[0][3]);
		// id=2: in both
		Assert.Equal("2", rows[1][0]?.ToString());
		Assert.Equal("b", rows[1][1]?.ToString());
		Assert.Equal("2", rows[1][2]?.ToString());
		Assert.Equal("x", rows[1][3]?.ToString());
		// id=3: only in right
		Assert.Null(rows[2][0]);
		Assert.Null(rows[2][1]);
		Assert.Equal("3", rows[2][2]?.ToString());
		Assert.Equal("y", rows[2][3]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// ANY_VALUE aggregate
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task AnyValue_Aggregate()
	{
		// ANY_VALUE returns an arbitrary value from the group (non-NULL if possible)
		var rows = await Q(@"
			SELECT grp, ANY_VALUE(val) AS any_v
			FROM (
				SELECT 'A' AS grp, 10 AS val UNION ALL
				SELECT 'A', 20 UNION ALL
				SELECT 'B', 30
			)
			GROUP BY grp
			ORDER BY grp");
		Assert.Equal(2, rows.Count);
		Assert.Equal("A", rows[0][0]?.ToString());
		// ANY_VALUE can return 10 or 20 for group A - just check it's not null
		Assert.NotNull(rows[0][1]);
		Assert.Equal("B", rows[1][0]?.ToString());
		Assert.Equal("30", rows[1][1]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// FARM_FINGERPRINT
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task FarmFingerprint_Consistent()
	{
		// Same input should produce same output
		var rows = await Q("SELECT FARM_FINGERPRINT('hello') AS f1, FARM_FINGERPRINT('hello') AS f2");
		Assert.Equal(rows[0][0]?.ToString(), rows[0][1]?.ToString());
	}

	[Fact] public async Task FarmFingerprint_DifferentInputs()
	{
		var rows = await Q("SELECT FARM_FINGERPRINT('hello') AS f1, FARM_FINGERPRINT('world') AS f2");
		Assert.NotEqual(rows[0][0]?.ToString(), rows[0][1]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// BIT_COUNT
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task BitCount_Basic()
	{
		var result = await S("SELECT BIT_COUNT(7)"); // 7 = 111 in binary, 3 set bits
		Assert.Equal("3", result);
	}

	[Fact] public async Task BitCount_PowerOfTwo()
	{
		var result = await S("SELECT BIT_COUNT(8)"); // 8 = 1000, 1 set bit
		Assert.Equal("1", result);
	}

	[Fact] public async Task BitCount_Zero()
	{
		var result = await S("SELECT BIT_COUNT(0)");
		Assert.Equal("0", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// GENERATE_TIMESTAMP_ARRAY
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task GenerateTimestampArray_Hours()
	{
		var result = await S(@"
			SELECT ARRAY_LENGTH(GENERATE_TIMESTAMP_ARRAY(
				TIMESTAMP '2024-01-01 00:00:00 UTC',
				TIMESTAMP '2024-01-01 12:00:00 UTC',
				INTERVAL 3 HOUR))");
		Assert.Equal("5", result); // 00:00, 03:00, 06:00, 09:00, 12:00
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Window DENSE_RANK with ties
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task DenseRank_WithTies()
	{
		var rows = await Q(@"
			SELECT x, DENSE_RANK() OVER (ORDER BY x) AS dr
			FROM UNNEST([1, 2, 2, 3, 3, 3, 4]) AS x
			ORDER BY x");
		Assert.Equal(7, rows.Count);
		Assert.Equal("1", rows[0][1]?.ToString()); // 1 → rank 1
		Assert.Equal("2", rows[1][1]?.ToString()); // 2 → rank 2
		Assert.Equal("2", rows[2][1]?.ToString()); // 2 → rank 2
		Assert.Equal("3", rows[3][1]?.ToString()); // 3 → rank 3
		Assert.Equal("4", rows[6][1]?.ToString()); // 4 → rank 4
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Window RANK with ties (gaps)
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Rank_WithGaps()
	{
		var rows = await Q(@"
			SELECT x, RANK() OVER (ORDER BY x) AS rk
			FROM UNNEST([1, 2, 2, 3]) AS x
			ORDER BY x");
		Assert.Equal(4, rows.Count);
		Assert.Equal("1", rows[0][1]?.ToString()); // rank 1
		Assert.Equal("2", rows[1][1]?.ToString()); // rank 2 (two 2s)
		Assert.Equal("2", rows[2][1]?.ToString()); // rank 2
		Assert.Equal("4", rows[3][1]?.ToString()); // rank 4 (gap: no rank 3)
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Multi-level CTE with aggregation at each level
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task CTE_MultiLevel_Aggregation()
	{
		var rows = await Q(@"
			WITH raw_data AS (
				SELECT 'A' AS dept, 'Alice' AS name, 100 AS salary UNION ALL
				SELECT 'A', 'Bob', 120 UNION ALL
				SELECT 'B', 'Carol', 90 UNION ALL
				SELECT 'B', 'Dave', 110
			),
			dept_stats AS (
				SELECT dept, AVG(salary) AS avg_sal, COUNT(*) AS cnt
				FROM raw_data
				GROUP BY dept
			)
			SELECT dept, CAST(avg_sal AS STRING) AS avg,
				CASE WHEN avg_sal > 100 THEN 'high' ELSE 'normal' END AS category
			FROM dept_stats
			ORDER BY dept");
		Assert.Equal(2, rows.Count);
		Assert.Equal("A", rows[0][0]?.ToString());
		Assert.Equal("110", rows[0][1]?.ToString()); // CAST(110.0 AS STRING) = "110"
		Assert.Equal("high", rows[0][2]?.ToString());
		Assert.Equal("B", rows[1][0]?.ToString());
		Assert.Equal("100", rows[1][1]?.ToString()); // CAST(100.0 AS STRING) = "100"
		Assert.Equal("normal", rows[1][2]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// EXISTS vs NOT EXISTS
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task NotExists_Basic()
	{
		await Exec("CREATE TABLE `{ds}.t1` (id INT64, name STRING)");
		await Exec("CREATE TABLE `{ds}.t2` (id INT64, t1_id INT64)");
		await Exec("INSERT INTO `{ds}.t1` (id, name) VALUES (1, 'A'), (2, 'B'), (3, 'C')");
		await Exec("INSERT INTO `{ds}.t2` (id, t1_id) VALUES (1, 1), (2, 2)");

		var rows = await Q(@"
			SELECT t.name FROM `{ds}.t1` t
			WHERE NOT EXISTS (SELECT 1 FROM `{ds}.t2` x WHERE x.t1_id = t.id)
			ORDER BY t.name");
		Assert.Single(rows);
		Assert.Equal("C", rows[0][0]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// CASE with aggregate inside
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Case_WithAggregate()
	{
		var rows = await Q(@"
			SELECT grp,
				CASE WHEN SUM(val) > 10 THEN 'high' ELSE 'low' END AS category
			FROM (
				SELECT 'A' AS grp, 8 AS val UNION ALL
				SELECT 'A', 5 UNION ALL
				SELECT 'B', 3 UNION ALL
				SELECT 'B', 4
			)
			GROUP BY grp
			ORDER BY grp");
		Assert.Equal(2, rows.Count);
		Assert.Equal("high", rows[0][1]?.ToString()); // A: 8+5=13 > 10
		Assert.Equal("low", rows[1][1]?.ToString()); // B: 3+4=7 <= 10
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Subquery as column expression 
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ScalarSubquery_InSelect()
	{
		var rows = await Q(@"
			SELECT x, (SELECT SUM(y) FROM UNNEST([1,2,3,4,5]) AS y WHERE y <= x) AS cum
			FROM UNNEST([1, 3, 5]) AS x
			ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("1", rows[0][1]?.ToString()); // sum(1)
		Assert.Equal("3", rows[1][0]?.ToString());
		Assert.Equal("6", rows[1][1]?.ToString()); // sum(1,2,3)
		Assert.Equal("5", rows[2][0]?.ToString());
		Assert.Equal("15", rows[2][1]?.ToString()); // sum(1,2,3,4,5)
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Complex multi-table operations
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task MultiTable_AggregateJoin()
	{
		await Exec("CREATE TABLE `{ds}.sales` (id INT64, product STRING, amount FLOAT64)");
		await Exec("INSERT INTO `{ds}.sales` (id, product, amount) VALUES (1,'Widget',100), (2,'Widget',150), (3,'Gadget',200), (4,'Gadget',50)");

		var rows = await Q(@"
			WITH product_totals AS (
				SELECT product, SUM(amount) AS total, COUNT(*) AS cnt
				FROM `{ds}.sales`
				GROUP BY product
			)
			SELECT product, total,
				ROUND(total / (SELECT SUM(total) FROM product_totals) * 100, 1) AS pct
			FROM product_totals
			ORDER BY total DESC, product ASC");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Gadget", rows[0][0]?.ToString());
		Assert.Equal("250", rows[0][1]?.ToString());
		Assert.Equal("50", rows[0][2]?.ToString()); // ROUND returns FLOAT64, SDK renders 50.0 as "50"
		Assert.Equal("Widget", rows[1][0]?.ToString());
		Assert.Equal("250", rows[1][1]?.ToString());
		Assert.Equal("50", rows[1][2]?.ToString());
	}
}

