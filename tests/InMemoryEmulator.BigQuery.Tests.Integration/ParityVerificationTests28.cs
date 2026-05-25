using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 28: Exercise rare/complex scenarios that commonly
/// have bugs in emulators: multiple JOINs, self-joins, correlated subqueries,
/// window functions with RANGE frames, complex GROUP BY with expressions,
/// ARRAY_AGG DISTINCT, COUNTIF with complex expressions, FORMAT with edge values.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests28 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests28(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv28_{Guid.NewGuid():N}"[..28];
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
	// Self-join
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task SelfJoin_EmployeeManager()
	{
		await Exec("CREATE TABLE `{ds}.emp` (id INT64, name STRING, mgr_id INT64)");
		await Exec("INSERT INTO `{ds}.emp` (id, name, mgr_id) VALUES (1, 'Alice', NULL), (2, 'Bob', 1), (3, 'Carol', 1), (4, 'Dave', 2)");

		var rows = await Q(@"
			SELECT e.name AS employee, m.name AS manager
			FROM `{ds}.emp` e LEFT JOIN `{ds}.emp` m ON e.mgr_id = m.id
			ORDER BY e.name");
		Assert.Equal(4, rows.Count);
		Assert.Equal("Alice", rows[0][0]?.ToString());
		Assert.Null(rows[0][1]); // Alice has no manager
		Assert.Equal("Bob", rows[1][0]?.ToString());
		Assert.Equal("Alice", rows[1][1]?.ToString());
		Assert.Equal("Dave", rows[3][0]?.ToString());
		Assert.Equal("Bob", rows[3][1]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Multiple JOINs
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ThreeTableJoin()
	{
		await Exec("CREATE TABLE `{ds}.orders` (id INT64, cust_id INT64, prod_id INT64, qty INT64)");
		await Exec("CREATE TABLE `{ds}.customers` (id INT64, name STRING)");
		await Exec("CREATE TABLE `{ds}.products` (id INT64, name STRING, price FLOAT64)");
		await Exec("INSERT INTO `{ds}.customers` (id, name) VALUES (1, 'Alice'), (2, 'Bob')");
		await Exec("INSERT INTO `{ds}.products` (id, name, price) VALUES (10, 'Widget', 5.0), (20, 'Gadget', 10.0)");
		await Exec("INSERT INTO `{ds}.orders` (id, cust_id, prod_id, qty) VALUES (1, 1, 10, 3), (2, 1, 20, 1), (3, 2, 10, 5)");

		var rows = await Q(@"
			SELECT c.name AS customer, p.name AS product, o.qty * p.price AS total
			FROM `{ds}.orders` o
			JOIN `{ds}.customers` c ON o.cust_id = c.id
			JOIN `{ds}.products` p ON o.prod_id = p.id
			ORDER BY c.name, p.name");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Alice", rows[0][0]?.ToString());
		Assert.Equal("Gadget", rows[0][1]?.ToString());
		Assert.Equal("10", rows[0][2]?.ToString());
		Assert.Equal("Alice", rows[1][0]?.ToString());
		Assert.Equal("Widget", rows[1][1]?.ToString());
		Assert.Equal("15", rows[1][2]?.ToString());
		Assert.Equal("Bob", rows[2][0]?.ToString());
		Assert.Equal("Widget", rows[2][1]?.ToString());
		Assert.Equal("25", rows[2][2]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Correlated subquery: EXISTS with reference to outer query
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task CorrelatedSubquery_Exists()
	{
		await Exec("CREATE TABLE `{ds}.dept` (id INT64, name STRING)");
		await Exec("CREATE TABLE `{ds}.staff` (id INT64, dept_id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.dept` (id, name) VALUES (1, 'Engineering'), (2, 'Marketing'), (3, 'Empty')");
		await Exec("INSERT INTO `{ds}.staff` (id, dept_id, name) VALUES (1, 1, 'Alice'), (2, 1, 'Bob'), (3, 2, 'Carol')");

		var rows = await Q(@"
			SELECT d.name
			FROM `{ds}.dept` d
			WHERE EXISTS (SELECT 1 FROM `{ds}.staff` s WHERE s.dept_id = d.id)
			ORDER BY d.name");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Engineering", rows[0][0]?.ToString());
		Assert.Equal("Marketing", rows[1][0]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// GROUP BY expression (not just column name)
	// ───────────────────────────────────────────────────────────────────────────

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact] public async Task GroupBy_Expression()
	{
		var rows = await Q(@"
			SELECT MOD(x, 3) AS bucket, COUNT(*) AS cnt
			FROM UNNEST([1,2,3,4,5,6,7,8,9]) AS x
			GROUP BY MOD(x, 3)
			ORDER BY bucket");
		Assert.Equal(3, rows.Count);
		Assert.Equal("0", rows[0][0]?.ToString()); // 3,6,9
		Assert.Equal("3", rows[0][1]?.ToString());
		Assert.Equal("1", rows[1][0]?.ToString()); // 1,4,7
		Assert.Equal("3", rows[1][1]?.ToString());
		Assert.Equal("2", rows[2][0]?.ToString()); // 2,5,8
		Assert.Equal("3", rows[2][1]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// ARRAY_AGG DISTINCT
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ArrayAgg_Distinct()
	{
		var result = await S(@"
			SELECT ARRAY_LENGTH(ARRAY_AGG(DISTINCT x))
			FROM UNNEST([1, 2, 2, 3, 3, 3]) AS x");
		Assert.Equal("3", result); // [1, 2, 3]
	}

	// ───────────────────────────────────────────────────────────────────────────
	// COUNTIF with complex expressions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task CountIf_Complex()
	{
		var result = await S(@"
			SELECT COUNTIF(x > 5 AND x < 9)
			FROM UNNEST([1,3,5,6,7,8,9,10]) AS x");
		Assert.Equal("3", result); // 6, 7, 8
	}

	// ───────────────────────────────────────────────────────────────────────────
	// SUM/AVG with DISTINCT
	// ───────────────────────────────────────────────────────────────────────────

	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact] public async Task Sum_Distinct()
	{
		var result = await S("SELECT SUM(DISTINCT x) FROM UNNEST([1, 2, 2, 3, 3, 3]) AS x");
		Assert.Equal("6", result); // 1 + 2 + 3 = 6
	}

	[Fact] public async Task Avg_Distinct()
	{
		var result = await S("SELECT AVG(DISTINCT x) FROM UNNEST([1, 2, 2, 3, 3, 3]) AS x");
		Assert.Equal("2", result); // (1 + 2 + 3) / 3 = 2.0, SDK renders as "2"
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Window ROWS BETWEEN N PRECEDING AND N FOLLOWING
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Window_RowsBetween_Symmetric()
	{
		var rows = await Q(@"
			SELECT x,
				SUM(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_sum
			FROM UNNEST([10, 20, 30, 40, 50]) AS x
			ORDER BY x");
		Assert.Equal("30", rows[0][1]?.ToString()); // 10+20
		Assert.Equal("60", rows[1][1]?.ToString()); // 10+20+30
		Assert.Equal("90", rows[2][1]?.ToString()); // 20+30+40
		Assert.Equal("120", rows[3][1]?.ToString()); // 30+40+50
		Assert.Equal("90", rows[4][1]?.ToString()); // 40+50
	}

	// ───────────────────────────────────────────────────────────────────────────
	// DISTINCT with ORDER BY
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Distinct_OrderBy()
	{
		var rows = await Q(@"
			SELECT DISTINCT x FROM (
				SELECT 3 AS x UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL
				SELECT 1 UNION ALL SELECT 3 UNION ALL SELECT 2
			)
			ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("2", rows[1][0]?.ToString());
		Assert.Equal("3", rows[2][0]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// IF function (expression form)
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task If_Function()
	{
		var result = await S("SELECT IF(2 > 1, 'yes', 'no')");
		Assert.Equal("yes", result);
	}

	[Fact] public async Task If_FunctionFalse()
	{
		var result = await S("SELECT IF(1 > 2, 'yes', 'no')");
		Assert.Equal("no", result);
	}

	[Fact] public async Task If_FunctionNull()
	{
		var result = await S("SELECT IF(NULL, 'yes', 'no')");
		Assert.Equal("no", result); // NULL condition treated as false
	}

	// ───────────────────────────────────────────────────────────────────────────
	// IIF (not supported in BigQuery - this tests that IF works correctly)
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Nullif_Basic()
	{
		var result = await S("SELECT NULLIF(5, 5)");
		Assert.Null(result); // returns NULL when args are equal
	}

	[Fact] public async Task Nullif_Different()
	{
		var result = await S("SELECT NULLIF(5, 3)");
		Assert.Equal("5", result); // returns first arg when different
	}

	// ───────────────────────────────────────────────────────────────────────────
	// ARRAY_REVERSE
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ArrayReverse()
	{
		var result = await S("SELECT ARRAY_TO_STRING(ARRAY(SELECT CAST(x AS STRING) FROM UNNEST(ARRAY_REVERSE([1,2,3,4,5])) AS x), ',')");
		Assert.Equal("5,4,3,2,1", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// TIMESTAMP_TRUNC
	// ───────────────────────────────────────────────────────────────────────────

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact] public async Task TimestampTrunc_Hour()
	{
		var result = await S("SELECT CAST(TIMESTAMP_TRUNC(TIMESTAMP '2024-06-15 14:35:22 UTC', HOUR) AS STRING)");
		Assert.Equal("2024-06-15 14:00:00+00", result);
	}

	[Fact] public async Task TimestampTrunc_Day()
	{
		var result = await S("SELECT CAST(TIMESTAMP_TRUNC(TIMESTAMP '2024-06-15 14:35:22 UTC', DAY) AS STRING)");
		Assert.Equal("2024-06-15 00:00:00+00", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Multiple ORDER BY columns
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task OrderBy_Multiple()
	{
		var rows = await Q(@"
			WITH data AS (
				SELECT 'A' AS grp, 2 AS val UNION ALL
				SELECT 'B', 1 UNION ALL
				SELECT 'A', 1 UNION ALL
				SELECT 'B', 2
			)
			SELECT grp, val FROM data ORDER BY grp ASC, val DESC");
		Assert.Equal("A", rows[0][0]?.ToString());
		Assert.Equal("2", rows[0][1]?.ToString());
		Assert.Equal("A", rows[1][0]?.ToString());
		Assert.Equal("1", rows[1][1]?.ToString());
		Assert.Equal("B", rows[2][0]?.ToString());
		Assert.Equal("2", rows[2][1]?.ToString());
		Assert.Equal("B", rows[3][0]?.ToString());
		Assert.Equal("1", rows[3][1]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// LOGICAL_OR / LOGICAL_AND as aggregates
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task LogicalOr_Aggregate()
	{
		var result = await S("SELECT LOGICAL_OR(x > 5) FROM UNNEST([1, 2, 3, 7]) AS x");
		Assert.Equal("True", result);
	}

	[Fact] public async Task LogicalAnd_Aggregate()
	{
		var result = await S("SELECT LOGICAL_AND(x > 0) FROM UNNEST([1, 2, 3, 7]) AS x");
		Assert.Equal("True", result);
	}

	[Fact] public async Task LogicalAnd_AggregateFalse()
	{
		var result = await S("SELECT LOGICAL_AND(x > 5) FROM UNNEST([1, 2, 3, 7]) AS x");
		Assert.Equal("False", result); // Not all > 5
	}
}
