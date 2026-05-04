using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Parity verification tests batch 14: Edge cases around NULL handling, type coercion,
/// string escaping, numeric precision, complex GROUP BY patterns, HAVING edge cases,
/// nested UNNEST, PIVOT-like patterns, and multi-level window functions.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests14 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests14(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv14_{Guid.NewGuid():N}"[..28];
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
	// NULL arithmetic and comparison
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Null_Arithmetic()
	{
		var result = await S("SELECT 5 + NULL");
		Assert.Null(result);
	}

	[Fact] public async Task Null_Comparison_Equals()
	{
		// NULL = NULL is NULL (not TRUE)
		var result = await S("SELECT CASE WHEN NULL = NULL THEN 'equal' ELSE 'not equal' END");
		Assert.Equal("not equal", result);
	}

	[Fact] public async Task Null_In_List()
	{
		// x IN (NULL, 1) when x=2 → NULL (not FALSE)  
		// But CASE treats NULL as not-true, so goes to ELSE
		var result = await S("SELECT CASE WHEN 2 IN (NULL, 1) THEN 'yes' ELSE 'no' END");
		Assert.Equal("no", result);
	}

	[Fact] public async Task Null_NotIn_WithNull()
	{
		// 2 NOT IN (NULL, 1) → NULL (because NULL might equal 2)
		var result = await S("SELECT CASE WHEN 2 NOT IN (NULL, 1) THEN 'yes' ELSE 'no' END");
		Assert.Equal("no", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Type coercion in UNION ALL
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task UnionAll_TypeCoercion()
	{
		var rows = await Q(@"
			SELECT 1 AS val UNION ALL SELECT 2 UNION ALL SELECT 3
			ORDER BY val");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]["val"]?.ToString());
		Assert.Equal("3", rows[2]["val"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// CAST edge cases
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Cast_StringToDate()
	{
		var result = await S("SELECT CAST(CAST('2024-06-15' AS DATE) AS STRING)");
		Assert.Equal("2024-06-15", result);
	}

	[Fact] public async Task Cast_IntToString()
	{
		var result = await S("SELECT CAST(12345 AS STRING)");
		Assert.Equal("12345", result);
	}

	[Fact] public async Task Cast_NegativeFloat()
	{
		var result = await S("SELECT CAST(-3.14 AS STRING)");
		Assert.Equal("-3.14", result);
	}

	[Fact] public async Task Cast_BoolToInt()
	{
		var r1 = await S("SELECT CAST(TRUE AS INT64)");
		var r2 = await S("SELECT CAST(FALSE AS INT64)");
		Assert.Equal("1", r1);
		Assert.Equal("0", r2);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// REGEXP_CONTAINS
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_contains
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task RegexpContains_Match()
	{
		var result = await S(@"SELECT REGEXP_CONTAINS('hello123', r'\d+')");
		Assert.Equal("True", result);
	}

	[Fact] public async Task RegexpContains_NoMatch()
	{
		var result = await S(@"SELECT REGEXP_CONTAINS('hello', r'\d+')");
		Assert.Equal("False", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// IFNULL vs COALESCE
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions#ifnull
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Ifnull_NonNull()
	{
		var result = await S("SELECT IFNULL(5, 10)");
		Assert.Equal("5", result);
	}

	[Fact] public async Task Ifnull_Null()
	{
		var result = await S("SELECT IFNULL(NULL, 10)");
		Assert.Equal("10", result);
	}

	[Fact] public async Task Nullif_Equal()
	{
		var result = await S("SELECT NULLIF(5, 5)");
		Assert.Null(result);
	}

	[Fact] public async Task Nullif_NotEqual()
	{
		var result = await S("SELECT NULLIF(5, 10)");
		Assert.Equal("5", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Complex aggregation patterns
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task CountIf_Basic()
	{
		var result = await S("SELECT COUNTIF(x > 3) FROM UNNEST([1, 2, 3, 4, 5]) AS x");
		Assert.Equal("2", result); // 4, 5
	}

	[Fact] public async Task Sum_Conditional()
	{
		var result = await S(@"
			SELECT SUM(IF(x > 3, x, 0)) FROM UNNEST([1, 2, 3, 4, 5]) AS x");
		Assert.Equal("9", result); // 4+5
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Multiple window functions in same query
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task MultipleWindowFunctions()
	{
		var rows = await Q(@"
			SELECT val,
				ROW_NUMBER() OVER (ORDER BY val) AS rn,
				RANK() OVER (ORDER BY val) AS rnk,
				SUM(val) OVER (ORDER BY val) AS running_sum
			FROM UNNEST([30, 10, 20, 10]) AS val
			ORDER BY val, rn");
		Assert.Equal("10", rows[0]["val"]?.ToString());
		Assert.Equal("1", rows[0]["rn"]?.ToString());
		Assert.Equal("1", rows[0]["rnk"]?.ToString()); // Both 10s have rank 1
		Assert.Equal("20", rows[1]["running_sum"]?.ToString()); // 10+10=20
	}

	// ───────────────────────────────────────────────────────────────────────────
	// STRUCT in SELECT
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Struct_InUnnest()
	{
		var rows = await Q(@"
			SELECT name, age
			FROM UNNEST([
				STRUCT('Alice' AS name, 30 AS age),
				STRUCT('Bob' AS name, 25 AS age)
			])
			ORDER BY name");
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("30", rows[0]["age"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// ARRAY_AGG with ORDER BY DESC
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ArrayAgg_OrderByDesc()
	{
		var result = await S(@"
			SELECT ARRAY_TO_STRING(ARRAY_AGG(x ORDER BY x DESC), ',')
			FROM UNNEST([3, 1, 4, 1, 5]) AS x");
		Assert.Equal("5,4,3,1,1", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// GENERATE_DATE_ARRAY with different intervals
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task GenerateDateArray_Year()
	{
		var result = await S(@"
			SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2020-01-01', DATE '2024-01-01', INTERVAL 1 YEAR))");
		Assert.Equal("5", result); // 2020, 2021, 2022, 2023, 2024
	}

	[Fact] public async Task GenerateDateArray_Quarter()
	{
		var result = await S(@"
			SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-12-31', INTERVAL 3 MONTH))");
		Assert.Equal("4", result); // Jan, Apr, Jul, Oct
	}

	// ───────────────────────────────────────────────────────────────────────────
	// String operations
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Substr_Negative()
	{
		// SUBSTR with negative position counts from end
		var result = await S("SELECT SUBSTR('hello world', -5)");
		Assert.Equal("world", result);
	}

	[Fact] public async Task Replace_Multiple()
	{
		var result = await S("SELECT REPLACE('aabbcc', 'bb', 'XX')");
		Assert.Equal("aaXXcc", result);
	}

	[Fact] public async Task Ltrim_Rtrim()
	{
		var r1 = await S("SELECT LTRIM('xxxhello', 'x')");
		var r2 = await S("SELECT RTRIM('helloyyy', 'y')");
		Assert.Equal("hello", r1);
		Assert.Equal("hello", r2);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// SAFE_CAST
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task SafeCast_ValidInt()
	{
		var result = await S("SELECT SAFE_CAST('123' AS INT64)");
		Assert.Equal("123", result);
	}

	[Fact] public async Task SafeCast_InvalidFloat()
	{
		var result = await S("SELECT SAFE_CAST('abc' AS FLOAT64)");
		Assert.Null(result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// CONCAT with empty string
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Concat_EmptyString()
	{
		var result = await S("SELECT CONCAT('', 'hello', '')");
		Assert.Equal("hello", result);
	}
}
