using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 22: Edge cases that commonly diverge from BigQuery behavior.
/// CAST precision, division semantics, NULL propagation in functions, ARRAY_AGG with ORDER BY 
/// and LIMIT combined, COUNTIF NULL, IN with subquery, EXISTS, complex STRUCTs, 
/// multiple window functions in single query, OFFSET/LIMIT edge cases.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests22 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests22(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv22_{Guid.NewGuid():N}"[..28];
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

	// ───────────────────────────────────────────────────────────────────────────
	// Division precision
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#division_operator
	//   "Division of two INT64 values returns FLOAT64."
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Division_IntByInt_ReturnsFloat()
	{
		// 1/3 should return 0.333...
		var result = await S("SELECT 1/3");
		Assert.StartsWith("0.333", result);
	}

	[Fact] public async Task Division_ExactResult()
	{
		var result = await S("SELECT 10/4");
		Assert.Equal("2.5", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// CAST string to numeric types
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Cast_FloatToInt_Truncates()
	{
		var result = await S("SELECT CAST(3.7 AS INT64)");
		Assert.Equal("4", result); // BigQuery rounds to nearest (not truncates)
	}

	[Fact] public async Task Cast_NegativeFloatToInt()
	{
		var result = await S("SELECT CAST(-3.7 AS INT64)");
		Assert.Equal("-4", result); // rounds toward nearest
	}

	// ───────────────────────────────────────────────────────────────────────────
	// ARRAY_AGG with ORDER BY + LIMIT combined
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#array_agg
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ArrayAgg_OrderByLimit()
	{
		var result = await S(@"
			SELECT ARRAY_TO_STRING(ARRAY_AGG(CAST(x AS STRING) ORDER BY x LIMIT 3), ',')
			FROM UNNEST([5, 3, 1, 4, 2]) AS x");
		Assert.Equal("1,2,3", result);
	}

	[Fact] public async Task ArrayAgg_OrderByDescLimit()
	{
		var result = await S(@"
			SELECT ARRAY_TO_STRING(ARRAY_AGG(CAST(x AS STRING) ORDER BY x DESC LIMIT 2), ',')
			FROM UNNEST([5, 3, 1, 4, 2]) AS x");
		Assert.Equal("5,4", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// IN with subquery
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#in_operators
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task InSubquery_Found()
	{
		var result = await S(@"
			SELECT 3 IN (SELECT x FROM UNNEST([1, 2, 3, 4, 5]) AS x)");
		Assert.Equal("True", result);
	}

	[Fact] public async Task InSubquery_NotFound()
	{
		var result = await S(@"
			SELECT 10 IN (SELECT x FROM UNNEST([1, 2, 3, 4, 5]) AS x)");
		Assert.Equal("False", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// EXISTS
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#exists_operator
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Exists_True()
	{
		var result = await S(@"
			SELECT EXISTS(SELECT 1 FROM UNNEST([1, 2, 3]) AS x WHERE x > 2)");
		Assert.Equal("True", result);
	}

	[Fact] public async Task Exists_False()
	{
		var result = await S(@"
			SELECT EXISTS(SELECT 1 FROM UNNEST([1, 2, 3]) AS x WHERE x > 10)");
		Assert.Equal("False", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// OFFSET / LIMIT
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#limit_and_offset_clause
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task OffsetLimit()
	{
		var rows = await Q(@"
			SELECT x FROM UNNEST([1, 2, 3, 4, 5]) AS x ORDER BY x LIMIT 2 OFFSET 2");
		Assert.Equal(2, rows.Count);
		Assert.Equal("3", rows[0][0]?.ToString());
		Assert.Equal("4", rows[1][0]?.ToString());
	}

	[Fact] public async Task Limit_Zero()
	{
		var rows = await Q(@"
			SELECT x FROM UNNEST([1, 2, 3]) AS x LIMIT 0");
		Assert.Empty(rows);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Multiple window functions in one query
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task MultipleWindowFunctions()
	{
		var rows = await Q(@"
			SELECT x,
				ROW_NUMBER() OVER (ORDER BY x) AS rn,
				SUM(x) OVER (ORDER BY x) AS running_sum,
				COUNT(*) OVER () AS total
			FROM UNNEST([10, 20, 30]) AS x
			ORDER BY x");
		Assert.Equal("1", rows[0][1]?.ToString());
		Assert.Equal("10", rows[0][2]?.ToString());
		Assert.Equal("3", rows[0][3]?.ToString());
		Assert.Equal("2", rows[1][1]?.ToString());
		Assert.Equal("30", rows[1][2]?.ToString());
		Assert.Equal("3", rows[1][3]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// COUNTIF with NULL
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#countif
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task CountIf_NullCondition()
	{
		// COUNTIF only counts TRUE (NULL conditions don't count)
		var result = await S("SELECT COUNTIF(x > 2) FROM UNNEST([1, CAST(NULL AS INT64), 3]) AS x");
		Assert.Equal("1", result); // only 3 > 2 is TRUE; NULL > 2 is NULL (not counted)
	}

	// ───────────────────────────────────────────────────────────────────────────
	// STRUCT with UNNEST
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Unnest_StructArray()
	{
		var rows = await Q(@"
			SELECT s.name, s.age
			FROM UNNEST([
				STRUCT('Alice' AS name, 30 AS age),
				STRUCT('Bob' AS name, 25 AS age),
				STRUCT('Charlie' AS name, 35 AS age)
			]) AS s
			ORDER BY s.age");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Bob", rows[0][0]?.ToString());
		Assert.Equal("25", rows[0][1]?.ToString());
		Assert.Equal("Alice", rows[1][0]?.ToString());
		Assert.Equal("Charlie", rows[2][0]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// String || concatenation operator
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#concatenation_operator
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task StringConcat_Operator()
	{
		var result = await S("SELECT 'hello' || ' ' || 'world'");
		Assert.Equal("hello world", result);
	}

	[Fact] public async Task StringConcat_WithNull()
	{
		// || with NULL returns NULL
		var result = await S("SELECT 'hello' || CAST(NULL AS STRING)");
		Assert.Null(result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// ARRAY subquery
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_subquery
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ArraySubquery_Basic()
	{
		var result = await S(@"
			SELECT ARRAY_TO_STRING(ARRAY(SELECT CAST(x * 2 AS STRING) FROM UNNEST([1, 2, 3]) AS x), ',')");
		Assert.Equal("2,4,6", result);
	}

	[Fact] public async Task ArraySubquery_WithFilter()
	{
		var result = await S(@"
			SELECT ARRAY_TO_STRING(ARRAY(SELECT CAST(x AS STRING) FROM UNNEST([1, 2, 3, 4, 5]) AS x WHERE x > 3 ORDER BY x), ',')");
		Assert.Equal("4,5", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// NTILE window function
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions#ntile
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Ntile_EvenDistribution()
	{
		var rows = await Q(@"
			SELECT x, NTILE(3) OVER (ORDER BY x) AS bucket
			FROM UNNEST([1, 2, 3, 4, 5, 6]) AS x
			ORDER BY x");
		Assert.Equal("1", rows[0][1]?.ToString()); // 1,2 in bucket 1
		Assert.Equal("1", rows[1][1]?.ToString());
		Assert.Equal("2", rows[2][1]?.ToString()); // 3,4 in bucket 2
		Assert.Equal("2", rows[3][1]?.ToString());
		Assert.Equal("3", rows[4][1]?.ToString()); // 5,6 in bucket 3
		Assert.Equal("3", rows[5][1]?.ToString());
	}

	[Fact] public async Task Ntile_UnevenDistribution()
	{
		var rows = await Q(@"
			SELECT x, NTILE(3) OVER (ORDER BY x) AS bucket
			FROM UNNEST([1, 2, 3, 4, 5]) AS x
			ORDER BY x");
		// 5 items in 3 buckets: 2,2,1 distribution
		Assert.Equal("1", rows[0][1]?.ToString()); // 1,2 in bucket 1
		Assert.Equal("1", rows[1][1]?.ToString());
		Assert.Equal("2", rows[2][1]?.ToString()); // 3,4 in bucket 2
		Assert.Equal("2", rows[3][1]?.ToString());
		Assert.Equal("3", rows[4][1]?.ToString()); // 5 in bucket 3
	}

	// ───────────────────────────────────────────────────────────────────────────
	// PERCENT_RANK / CUME_DIST
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task PercentRank()
	{
		var rows = await Q(@"
			SELECT x, PERCENT_RANK() OVER (ORDER BY x) AS prank
			FROM UNNEST([10, 20, 30, 40]) AS x
			ORDER BY x");
		Assert.Equal("0", rows[0][1]?.ToString()); // rank 0/(4-1)
		Assert.StartsWith("0.333", rows[1][1]?.ToString()); // rank 1/3
	}

	[Fact] public async Task CumeDist()
	{
		var rows = await Q(@"
			SELECT x, CUME_DIST() OVER (ORDER BY x) AS cd
			FROM UNNEST([10, 20, 30, 40]) AS x
			ORDER BY x");
		Assert.Equal("0.25", rows[0][1]?.ToString()); // 1/4
		Assert.Equal("0.5", rows[1][1]?.ToString()); // 2/4
		Assert.Equal("1", rows[3][1]?.ToString()); // 4/4
	}

	// ───────────────────────────────────────────────────────────────────────────
	// NULL-safe comparison (IS NOT DISTINCT FROM as equal)
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task IsDistinctFrom_InWhere()
	{
		var rows = await Q(@"
			SELECT x FROM UNNEST([1, 2, CAST(NULL AS INT64), 3]) AS x
			WHERE x IS DISTINCT FROM 2
			ORDER BY x NULLS LAST");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Equal("3", rows[1][0]?.ToString());
		Assert.Null(rows[2][0]);
	}
}
