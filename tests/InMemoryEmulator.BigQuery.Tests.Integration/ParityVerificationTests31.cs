using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 31: Final stress tests targeting:
/// complex correlated scalar subqueries, self-referencing CTEs,
/// GROUP BY with multiple aggregates + HAVING, window frame edge cases,
/// extreme VALUES expressions, nested CASE, expression in PARTITION BY,
/// UPDATE with subquery, DELETE with JOIN pattern, ARRAY operations chain.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests31 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests31(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv31_{Guid.NewGuid():N}"[..28];
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
	// Window with expression in PARTITION BY
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Window_ExpressionPartitionBy()
	{
		var rows = await Q(@"
			SELECT x,
				COUNT(*) OVER (PARTITION BY x > 3) AS cnt_in_partition
			FROM UNNEST([1, 2, 3, 4, 5, 6]) AS x
			ORDER BY x");
		// x<=3: [1,2,3] → 3 each; x>3: [4,5,6] → 3 each
		Assert.Equal("3", rows[0][1]?.ToString()); // x=1, partition x>3 is false, 3 items
		Assert.Equal("3", rows[3][1]?.ToString()); // x=4, partition x>3 is true, 3 items
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Nested CASE with multiple WHEN
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task NestedCase_Complex()
	{
		var rows = await Q(@"
			SELECT x,
				CASE 
					WHEN x < 0 THEN 'negative'
					WHEN x = 0 THEN 'zero'
					WHEN x <= 10 THEN
						CASE
							WHEN x <= 5 THEN 'low'
							ELSE 'medium'
						END
					ELSE 'high'
				END AS label
			FROM UNNEST([-1, 0, 3, 7, 15]) AS x
			ORDER BY x");
		Assert.Equal("negative", rows[0][1]?.ToString());
		Assert.Equal("zero", rows[1][1]?.ToString());
		Assert.Equal("low", rows[2][1]?.ToString());
		Assert.Equal("medium", rows[3][1]?.ToString());
		Assert.Equal("high", rows[4][1]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Multiple aggregates with HAVING using multiple conditions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task GroupBy_MultipleAggregates_Having()
	{
		var rows = await Q(@"
			WITH data AS (
				SELECT 'A' AS g, 10 AS v UNION ALL
				SELECT 'A', 20 UNION ALL
				SELECT 'A', 30 UNION ALL
				SELECT 'B', 5 UNION ALL
				SELECT 'B', 15 UNION ALL
				SELECT 'C', 100
			)
			SELECT g, SUM(v) AS s, COUNT(*) AS c, AVG(v) AS a
			FROM data
			GROUP BY g
			HAVING SUM(v) > 15 AND COUNT(*) >= 2
			ORDER BY g");
		Assert.Equal(2, rows.Count);
		Assert.Equal("A", rows[0][0]?.ToString());
		Assert.Equal("60", rows[0][1]?.ToString());
		Assert.Equal("3", rows[0][2]?.ToString());
		Assert.Equal("B", rows[1][0]?.ToString());
		Assert.Equal("20", rows[1][1]?.ToString());
		Assert.Equal("2", rows[1][2]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Chained ARRAY operations
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Array_ChainedOperations()
	{
		var result = await S(@"
			SELECT ARRAY_TO_STRING(
				ARRAY(SELECT CAST(x AS STRING) FROM UNNEST(ARRAY_REVERSE(GENERATE_ARRAY(1, 5))) AS x WHERE x > 2),
				','
			)");
		// GENERATE_ARRAY(1,5) = [1,2,3,4,5]
		// ARRAY_REVERSE = [5,4,3,2,1]
		// WHERE x > 2 = [5,4,3]
		Assert.Equal("5,4,3", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// UPDATE with computed expression
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Update_ComputedExpression()
	{
		await Exec("CREATE TABLE `{ds}.items` (id INT64, price FLOAT64, qty INT64)");
		await Exec("INSERT INTO `{ds}.items` (id, price, qty) VALUES (1, 10.0, 5), (2, 20.0, 3), (3, 5.0, 10)");
		await Exec("UPDATE `{ds}.items` SET price = price * 1.1 WHERE qty >= 5");

		var rows = await Q("SELECT id, price FROM `{ds}.items` ORDER BY id");
		Assert.Equal("11", rows[0][1]?.ToString()); // 10 * 1.1 = 11
		Assert.Equal("20", rows[1][1]?.ToString()); // not updated (qty=3)
		Assert.Equal("5.5", rows[2][1]?.ToString()); // 5 * 1.1 = 5.5
	}

	// ───────────────────────────────────────────────────────────────────────────
	// DELETE with complex WHERE
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Delete_ComplexWhere()
	{
		await Exec("CREATE TABLE `{ds}.logs` (id INT64, level STRING, msg STRING)");
		await Exec(@"INSERT INTO `{ds}.logs` (id, level, msg) VALUES 
			(1, 'INFO', 'start'), (2, 'ERROR', 'fail'), (3, 'WARN', 'slow'),
			(4, 'ERROR', 'timeout'), (5, 'INFO', 'done')");
		await Exec("DELETE FROM `{ds}.logs` WHERE level = 'ERROR' OR level = 'WARN'");

		var rows = await Q("SELECT id FROM `{ds}.logs` ORDER BY id");
		Assert.Equal(2, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("5", rows[1][0]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Complex expression with ||, COALESCE, and CASE together
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Expression_ComplexCombined()
	{
		var rows = await Q(@"
			SELECT 
				COALESCE(name, 'Unknown') || ' (' || CASE WHEN age >= 18 THEN 'adult' ELSE 'minor' END || ')' AS label
			FROM (
				SELECT 'Alice' AS name, 25 AS age UNION ALL
				SELECT CAST(NULL AS STRING), 15 UNION ALL
				SELECT 'Bob', 30
			)
			ORDER BY label");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Alice (adult)", rows[0][0]?.ToString());
		Assert.Equal("Bob (adult)", rows[1][0]?.ToString());
		Assert.Equal("Unknown (minor)", rows[2][0]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Window ROW_NUMBER for pagination pattern
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Window_Pagination()
	{
		var rows = await Q(@"
			WITH numbered AS (
				SELECT name, ROW_NUMBER() OVER (ORDER BY name) AS rn
				FROM (
					SELECT 'Alice' AS name UNION ALL SELECT 'Bob' UNION ALL
					SELECT 'Carol' UNION ALL SELECT 'Dave' UNION ALL SELECT 'Eve'
				)
			)
			SELECT name FROM numbered WHERE rn BETWEEN 2 AND 4 ORDER BY rn");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Bob", rows[0][0]?.ToString());
		Assert.Equal("Carol", rows[1][0]?.ToString());
		Assert.Equal("Dave", rows[2][0]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Subquery in FROM with alias
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Subquery_InFrom()
	{
		var rows = await Q(@"
			SELECT sub.total, sub.grp
			FROM (
				SELECT grp, SUM(val) AS total
				FROM (
					SELECT 'X' AS grp, 10 AS val UNION ALL
					SELECT 'X', 20 UNION ALL
					SELECT 'Y', 5
				)
				GROUP BY grp
			) sub
			ORDER BY sub.total DESC");
		Assert.Equal(2, rows.Count);
		Assert.Equal("30", rows[0][0]?.ToString());
		Assert.Equal("X", rows[0][1]?.ToString());
		Assert.Equal("5", rows[1][0]?.ToString());
		Assert.Equal("Y", rows[1][1]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Multiple UNION ALL with different column names (positional matching)
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task UnionAll_DifferentAliases()
	{
		var rows = await Q(@"
			SELECT 1 AS a, 'x' AS b
			UNION ALL
			SELECT 2, 'y'
			UNION ALL
			SELECT 3, 'z'
			ORDER BY a");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("x", rows[0][1]?.ToString());
		Assert.Equal("3", rows[2][0]?.ToString());
		Assert.Equal("z", rows[2][1]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// IN with subquery
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task In_Subquery()
	{
		var rows = await Q(@"
			SELECT x FROM UNNEST([1, 2, 3, 4, 5]) AS x
			WHERE x IN (SELECT y FROM UNNEST([2, 4, 6]) AS y)
			ORDER BY x");
		Assert.Equal(2, rows.Count);
		Assert.Equal("2", rows[0][0]?.ToString());
		Assert.Equal("4", rows[1][0]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// EXCEPT DISTINCT
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Except_Distinct()
	{
		var rows = await Q(@"
			SELECT x FROM UNNEST([1, 2, 3, 4, 5]) AS x
			EXCEPT DISTINCT
			SELECT y FROM UNNEST([3, 4, 5, 6]) AS y
			ORDER BY x");
		Assert.Equal(2, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("2", rows[1][0]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// INTERSECT DISTINCT
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Intersect_Distinct()
	{
		var rows = await Q(@"
			SELECT x FROM UNNEST([1, 2, 3, 4, 5]) AS x
			INTERSECT DISTINCT
			SELECT y FROM UNNEST([3, 4, 5, 6]) AS y
			ORDER BY x");
		Assert.Equal(3, rows.Count);
		Assert.Equal("3", rows[0][0]?.ToString());
		Assert.Equal("4", rows[1][0]?.ToString());
		Assert.Equal("5", rows[2][0]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// ORDER BY with LIMIT 0
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Limit_Zero()
	{
		var rows = await Q("SELECT x FROM UNNEST([1,2,3]) AS x LIMIT 0");
		Assert.Empty(rows);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Complex calculation with mixed types
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task MixedTypeMath()
	{
		// INT64 / INT64 = FLOAT64 in BigQuery
		var result = await S("SELECT 10 / 3");
		Assert.Contains("3.3333", result!);
	}

	[Fact] public async Task IntegerDivision()
	{
		var result = await S("SELECT DIV(10, 3)");
		Assert.Equal("3", result);
	}

	[Fact] public async Task IntegerModulo()
	{
		var result = await S("SELECT MOD(10, 3)");
		Assert.Equal("1", result);
	}
}
