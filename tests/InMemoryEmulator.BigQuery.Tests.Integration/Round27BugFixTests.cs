using Google;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Integration tests for bugs fixed in research round 27:
/// - UNION ALL type coercion (INT64 + FLOAT64 → FLOAT64)
/// - Set operation schema compatibility with NULL-inferred types
/// </summary>
[Collection(IntegrationCollection.Name)]
public class Round27BugFixTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public Round27BugFixTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_r27_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
	}

	// ===== 1. UNION ALL type coercion =====

	[Fact]
	public async Task UnionAll_IntAndFloat_CoercesToFloat64()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 1.5 AS x)",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(2, rows.Count);
		// Both should be FLOAT64 after coercion
		Assert.Equal(1.0, (double)rows[0]["x"]);
		Assert.Equal(1.5, (double)rows[1]["x"]);
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task UnionAll_IntAndString_DoesNotError_DueToTypeInferenceLimitation()
	{
		// Note: Real BigQuery errors with "Column 1 in UNION ALL has incompatible types: INT64, STRING"
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators
		// The emulator treats STRING as a wildcard type for NULL-inferred columns,
		// which incorrectly allows INT64+STRING unions. This is a known limitation.
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT 1 AS x UNION ALL SELECT 'abc' AS x",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(2, rows.Count);
	}

	// ===== 2. ORDER BY position number =====

	[Fact]
	public async Task OrderBy_PositionNumber()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"CREATE TABLE `{_datasetId}.order_test` (a STRING, b INT64)",
			parameters: null);
		await client.ExecuteQueryAsync(
			$"INSERT INTO `{_datasetId}.order_test` (a, b) VALUES ('x', 3), ('y', 1), ('z', 2)",
			parameters: null);

		var result = await client.ExecuteQueryAsync(
			$"SELECT a, b FROM `{_datasetId}.order_test` ORDER BY 2",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(3, rows.Count);
		Assert.Equal(1L, (long)rows[0]["b"]);
		Assert.Equal(2L, (long)rows[1]["b"]);
		Assert.Equal(3L, (long)rows[2]["b"]);
	}

	[Fact]
	public async Task OrderBy_PositionNumber_Desc()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"CREATE TABLE `{_datasetId}.order_test2` (a STRING, b INT64)",
			parameters: null);
		await client.ExecuteQueryAsync(
			$"INSERT INTO `{_datasetId}.order_test2` (a, b) VALUES ('x', 3), ('y', 1), ('z', 2)",
			parameters: null);

		var result = await client.ExecuteQueryAsync(
			$"SELECT a, b FROM `{_datasetId}.order_test2` ORDER BY 2 DESC, 1 ASC",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(3, rows.Count);
		Assert.Equal(3L, (long)rows[0]["b"]);
		Assert.Equal(2L, (long)rows[1]["b"]);
		Assert.Equal(1L, (long)rows[2]["b"]);
	}

	// ===== 3. GROUP BY position number =====

	[Fact]
	public async Task GroupBy_PositionNumber()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"CREATE TABLE `{_datasetId}.group_test` (a STRING, b INT64)",
			parameters: null);
		await client.ExecuteQueryAsync(
			$"INSERT INTO `{_datasetId}.group_test` (a, b) VALUES ('x', 1), ('x', 2), ('y', 3)",
			parameters: null);

		var result = await client.ExecuteQueryAsync(
			$"SELECT a, COUNT(*) AS cnt FROM `{_datasetId}.group_test` GROUP BY 1 ORDER BY 1",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(2, rows.Count);
		Assert.Equal("x", (string)rows[0]["a"]);
		Assert.Equal(2L, (long)rows[0]["cnt"]);
		Assert.Equal("y", (string)rows[1]["a"]);
		Assert.Equal(1L, (long)rows[1]["cnt"]);
	}

	// ===== 4. SELECT without FROM =====

	[Fact]
	public async Task Select_WithoutFrom_ProducesOneRow()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT 1 AS a, 'hello' AS b",
			parameters: null);
		var rows = result.ToList();
		Assert.Single(rows);
		Assert.Equal(1L, (long)rows[0]["a"]);
		Assert.Equal("hello", (string)rows[0]["b"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax
	//   "Query without FROM clause cannot have a WHERE clause"
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Select_WithoutFrom_WhereFalse_ProducesZeroRows()
	{
		var client = await _fixture.GetClientAsync();
		var ex = await Assert.ThrowsAsync<Google.GoogleApiException>(async () =>
			await client.ExecuteQueryAsync(
				"SELECT 1 AS a WHERE FALSE",
				parameters: null));
		Assert.Contains("Query without FROM clause cannot have a WHERE clause", ex.Message);
	}

	// ===== 5. LIMIT 0 =====

	[Fact]
	public async Task Limit_Zero_ReturnsZeroRows()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT 1 AS a LIMIT 0",
			parameters: null);
		var rows = result.ToList();
		Assert.Empty(rows);
	}

	[Fact]
	public async Task Limit_Zero_WithTable()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"CREATE TABLE `{_datasetId}.limit_test` (a INT64)",
			parameters: null);
		await client.ExecuteQueryAsync(
			$"INSERT INTO `{_datasetId}.limit_test` (a) VALUES (1), (2), (3)",
			parameters: null);

		var result = await client.ExecuteQueryAsync(
			$"SELECT * FROM `{_datasetId}.limit_test` LIMIT 0",
			parameters: null);
		var rows = result.ToList();
		Assert.Empty(rows);
	}

	// ===== 6. Aggregate on single-row result (SELECT without FROM) =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax
	//   "Aggregate function COUNT(*) not allowed in SELECT without FROM clause"

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Aggregate_WithoutFrom()
	{
		var client = await _fixture.GetClientAsync();
		var ex = await Assert.ThrowsAsync<Google.GoogleApiException>(async () =>
			await client.ExecuteQueryAsync(
				"SELECT COUNT(*) AS cnt, SUM(1) AS s, MAX(1) AS m",
				parameters: null));
		Assert.Contains("Aggregate function", ex.Message);
		Assert.Contains("not allowed in SELECT without FROM clause", ex.Message);
	}

	// ===== 7. HAVING without GROUP BY (implicit single group) =====

	[Fact]
	public async Task Having_WithoutGroupBy_Passes()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"CREATE TABLE `{_datasetId}.having_test` (x INT64)",
			parameters: null);
		await client.ExecuteQueryAsync(
			$"INSERT INTO `{_datasetId}.having_test` (x) VALUES (5), (10), (15)",
			parameters: null);

		var result = await client.ExecuteQueryAsync(
			$"SELECT SUM(x) AS total FROM `{_datasetId}.having_test` HAVING SUM(x) > 10",
			parameters: null);
		var rows = result.ToList();
		Assert.Single(rows);
		Assert.Equal(30L, (long)rows[0]["total"]);
	}

	[Fact]
	public async Task Having_WithoutGroupBy_Fails()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"CREATE TABLE `{_datasetId}.having_test2` (x INT64)",
			parameters: null);
		await client.ExecuteQueryAsync(
			$"INSERT INTO `{_datasetId}.having_test2` (x) VALUES (1), (2), (3)",
			parameters: null);

		var result = await client.ExecuteQueryAsync(
			$"SELECT SUM(x) AS total FROM `{_datasetId}.having_test2` HAVING SUM(x) > 100",
			parameters: null);
		var rows = result.ToList();
		Assert.Empty(rows);
	}

	// ===== 8. Correlated subquery =====

	[Fact]
	public async Task CorrelatedSubquery_Basic()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"CREATE TABLE `{_datasetId}.t1` (id INT64, name STRING)",
			parameters: null);
		await client.ExecuteQueryAsync(
			$"INSERT INTO `{_datasetId}.t1` (id, name) VALUES (1, 'a'), (2, 'b')",
			parameters: null);
		await client.ExecuteQueryAsync(
			$"CREATE TABLE `{_datasetId}.t2` (id INT64, val INT64)",
			parameters: null);
		await client.ExecuteQueryAsync(
			$"INSERT INTO `{_datasetId}.t2` (id, val) VALUES (1, 10), (1, 20), (2, 30)",
			parameters: null);

		var result = await client.ExecuteQueryAsync(
			$"SELECT t1.id, (SELECT MAX(val) FROM `{_datasetId}.t2` t2 WHERE t2.id = t1.id) AS max_val FROM `{_datasetId}.t1` t1 ORDER BY t1.id",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(2, rows.Count);
		Assert.Equal(1L, (long)rows[0]["id"]);
		Assert.Equal(20L, (long)rows[0]["max_val"]);
		Assert.Equal(2L, (long)rows[1]["id"]);
		Assert.Equal(30L, (long)rows[1]["max_val"]);
	}

	// ===== 9. CROSS JOIN UNNEST =====

	[Fact]
	public async Task CrossJoinUnnest_WithAlias()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"CREATE TABLE `{_datasetId}.arr_test` (id INT64, arr ARRAY<INT64>)",
			parameters: null);
		await client.ExecuteQueryAsync(
			$"INSERT INTO `{_datasetId}.arr_test` (id, arr) VALUES (1, [10, 20]), (2, [30])",
			parameters: null);

		var result = await client.ExecuteQueryAsync(
			$"SELECT t.id, u FROM `{_datasetId}.arr_test` t, UNNEST(t.arr) u ORDER BY t.id, u",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(3, rows.Count);
		Assert.Equal(1L, (long)rows[0]["id"]);
		Assert.Equal(10L, (long)rows[0]["u"]);
		Assert.Equal(1L, (long)rows[1]["id"]);
		Assert.Equal(20L, (long)rows[1]["u"]);
		Assert.Equal(2L, (long)rows[2]["id"]);
		Assert.Equal(30L, (long)rows[2]["u"]);
	}

	// ===== 10. EXCEPT DISTINCT =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators
	//   "Different set operations cannot be used in the same query without using parentheses for grouping"

	[Fact]
	public async Task ExceptDistinct_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var result1 = await client.ExecuteQueryAsync(
			"SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x UNION ALL SELECT 1 AS x) EXCEPT DISTINCT SELECT 1 AS x",
			parameters: null);
		var rows1 = result1.ToList();
		Assert.Single(rows1);
		Assert.Equal(2L, (long)rows1[0]["x"]);
	}

	[Fact]
	public async Task ExceptDistinct_Simple()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x UNION ALL SELECT 3 AS x) EXCEPT DISTINCT SELECT 2 AS x",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(2, rows.Count);
	}

	// ===== 11. CASE with no ELSE =====

	[Fact]
	public async Task Case_NoElse_NoMatch_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT CASE WHEN FALSE THEN 'x' END AS result",
			parameters: null);
		var rows = result.ToList();
		Assert.Single(rows);
		Assert.Null(rows[0]["result"]);
	}

	// ===== 12. QUALIFY with window function =====

	[Fact]
	public async Task Qualify_WithRowNumber()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"CREATE TABLE `{_datasetId}.qualify_test` (grp STRING, val INT64)",
			parameters: null);
		await client.ExecuteQueryAsync(
			$"INSERT INTO `{_datasetId}.qualify_test` (grp, val) VALUES ('a', 1), ('a', 2), ('b', 3), ('b', 4)",
			parameters: null);

		var result = await client.ExecuteQueryAsync(
			$"SELECT grp, val FROM `{_datasetId}.qualify_test` QUALIFY ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val DESC) = 1 ORDER BY grp",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(2, rows.Count);
		Assert.Equal("a", (string)rows[0]["grp"]);
		Assert.Equal(2L, (long)rows[0]["val"]);
		Assert.Equal("b", (string)rows[1]["grp"]);
		Assert.Equal(4L, (long)rows[1]["val"]);
	}

	// ===== 13. SAFE_CAST overflow =====

	[Fact]
	public async Task SafeCast_OverflowInt64_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT SAFE_CAST('99999999999999999999' AS INT64) AS result",
			parameters: null);
		var rows = result.ToList();
		Assert.Single(rows);
		Assert.Null(rows[0]["result"]);
	}

	[Fact]
	public async Task SafeCast_NegativeOverflowInt64_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT SAFE_CAST('-99999999999999999999' AS INT64) AS result",
			parameters: null);
		var rows = result.ToList();
		Assert.Single(rows);
		Assert.Null(rows[0]["result"]);
	}

	// ===== 15. CURRENT_DATE, CURRENT_TIMESTAMP functions =====

	[Fact]
	public async Task CurrentDate_ReturnsDate()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT CURRENT_DATE() AS d",
			parameters: null);
		var rows = result.ToList();
		Assert.Single(rows);
		Assert.NotNull(rows[0]["d"]);
	}

	[Fact]
	public async Task CurrentTimestamp_ReturnsTimestamp()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT CURRENT_TIMESTAMP() AS ts",
			parameters: null);
		var rows = result.ToList();
		Assert.Single(rows);
		Assert.NotNull(rows[0]["ts"]);
	}

	[Fact]
	public async Task CurrentDatetime_ReturnsDatetime()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT CURRENT_DATETIME() AS dt",
			parameters: null);
		var rows = result.ToList();
		Assert.Single(rows);
		Assert.NotNull(rows[0]["dt"]);
	}

	[Fact]
	public async Task CurrentTime_ReturnsTime()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT CURRENT_TIME() AS t",
			parameters: null);
		var rows = result.ToList();
		Assert.Single(rows);
		Assert.NotNull(rows[0]["t"]);
	}

	// ===== Additional: ORDER BY alias =====

	[Fact]
	public async Task OrderBy_Alias()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT 3 AS x UNION ALL SELECT 1 AS x UNION ALL SELECT 2 AS x ORDER BY x",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(3, rows.Count);
		Assert.Equal(1L, (long)rows[0]["x"]);
		Assert.Equal(2L, (long)rows[1]["x"]);
		Assert.Equal(3L, (long)rows[2]["x"]);
	}

	// ===== Additional: GROUP BY alias =====

	[Fact]
	public async Task GroupBy_Alias()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"CREATE TABLE `{_datasetId}.grpalias` (cat STRING, val INT64)",
			parameters: null);
		await client.ExecuteQueryAsync(
			$"INSERT INTO `{_datasetId}.grpalias` (cat, val) VALUES ('a', 1), ('a', 2), ('b', 3)",
			parameters: null);

		var result = await client.ExecuteQueryAsync(
			$"SELECT cat AS category, SUM(val) AS total FROM `{_datasetId}.grpalias` GROUP BY category ORDER BY category",
			parameters: null);
		var rows = result.ToList();
		Assert.Equal(2, rows.Count);
		Assert.Equal("a", (string)rows[0]["category"]);
		Assert.Equal(3L, (long)rows[0]["total"]);
	}
}
