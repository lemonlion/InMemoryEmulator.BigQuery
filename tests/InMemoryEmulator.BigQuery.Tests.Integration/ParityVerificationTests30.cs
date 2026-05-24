using Google;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 30: Tests targeting likely bug areas:
/// HAVING with alias, GROUP BY ordinal, window LAG with NULL values,
/// multiple UNNEST, CONCAT with various types, ORDER BY NULL handling,
/// aggregate over empty set, various CAST edge cases, expression evaluation order.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests30 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests30(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv30_{Guid.NewGuid():N}"[..28];
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
	// GROUP BY ordinal position
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task GroupBy_Ordinal()
	{
		var rows = await Q(@"
			SELECT grp, COUNT(*) AS cnt
			FROM (SELECT 'A' AS grp UNION ALL SELECT 'A' UNION ALL SELECT 'B')
			GROUP BY 1
			ORDER BY 1");
		Assert.Equal(2, rows.Count);
		Assert.Equal("A", rows[0][0]?.ToString());
		Assert.Equal("2", rows[0][1]?.ToString());
		Assert.Equal("B", rows[1][0]?.ToString());
		Assert.Equal("1", rows[1][1]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// ORDER BY ordinal position
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task OrderBy_Ordinal()
	{
		var rows = await Q(@"SELECT x, x * 2 AS doubled FROM UNNEST([3, 1, 2]) AS x ORDER BY 2");
		Assert.Equal("1", rows[0][0]?.ToString()); // doubled=2
		Assert.Equal("2", rows[1][0]?.ToString()); // doubled=4
		Assert.Equal("3", rows[2][0]?.ToString()); // doubled=6
	}

	// ───────────────────────────────────────────────────────────────────────────
	// HAVING with alias reference
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Having_WithAlias()
	{
		var rows = await Q(@"
			SELECT grp, COUNT(*) AS cnt
			FROM (
				SELECT 'A' AS grp UNION ALL SELECT 'A' UNION ALL SELECT 'A' UNION ALL
				SELECT 'B' UNION ALL SELECT 'B' UNION ALL
				SELECT 'C'
			)
			GROUP BY grp
			HAVING cnt >= 2
			ORDER BY grp");
		Assert.Equal(2, rows.Count);
		Assert.Equal("A", rows[0][0]?.ToString());
		Assert.Equal("3", rows[0][1]?.ToString());
		Assert.Equal("B", rows[1][0]?.ToString());
		Assert.Equal("2", rows[1][1]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Aggregate over empty set
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Aggregate_EmptySet_Count()
	{
		var result = await S("SELECT COUNT(*) FROM UNNEST(CAST([] AS ARRAY<INT64>))");
		Assert.Equal("0", result);
	}

	[Fact] public async Task Aggregate_EmptySet_Sum()
	{
		var result = await S("SELECT SUM(x) FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x");
		Assert.Null(result); // SUM over empty set = NULL
	}

	[Fact] public async Task Aggregate_EmptySet_Avg()
	{
		var result = await S("SELECT AVG(x) FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x");
		Assert.Null(result); // AVG over empty set = NULL
	}

	[Fact] public async Task Aggregate_EmptySet_Max()
	{
		var result = await S("SELECT MAX(x) FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x");
		Assert.Null(result); // MAX over empty set = NULL
	}

	// ───────────────────────────────────────────────────────────────────────────
	// LAG/LEAD with NULL values in data
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Lag_NullValues()
	{
		var rows = await Q(@"
			SELECT x, LAG(x) OVER (ORDER BY CASE WHEN x IS NULL THEN 999 ELSE x END) AS prev
			FROM UNNEST([1, CAST(NULL AS INT64), 3]) AS x
			ORDER BY CASE WHEN x IS NULL THEN 999 ELSE x END");
		// Order: 1, 3, NULL
		Assert.Equal("1", rows[0][0]?.ToString());
		Assert.Null(rows[0][1]); // no previous
		Assert.Equal("3", rows[1][0]?.ToString());
		Assert.Equal("1", rows[1][1]?.ToString()); // lag = 1
		Assert.Null(rows[2][0]); // NULL value
		Assert.Equal("3", rows[2][1]?.ToString()); // lag = 3
	}

	// ───────────────────────────────────────────────────────────────────────────
	// CAST edge cases
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Cast_BoolToInt()
	{
		var result = await S("SELECT CAST(TRUE AS INT64)");
		Assert.Equal("1", result);
	}

	[Fact] public async Task Cast_IntToBool_Zero()
	{
		var result = await S("SELECT CAST(0 AS BOOL)");
		Assert.Equal("False", result);
	}

	[Fact] public async Task Cast_IntToBool_NonZero()
	{
		var result = await S("SELECT CAST(42 AS BOOL)");
		Assert.Equal("True", result);
	}

	[Fact] public async Task Cast_FloatToInt_Truncates()
	{
		var result = await S("SELECT CAST(3.7 AS INT64)");
		Assert.Equal("4", result); // BigQuery rounds to nearest even for .5, rounds 3.7 to 4
	}

	[Fact] public async Task Cast_NegativeFloatToInt()
	{
		var result = await S("SELECT CAST(-2.3 AS INT64)");
		Assert.Equal("-2", result); // rounds toward zero for .3
	}

	// ───────────────────────────────────────────────────────────────────────────
	// CONCAT with non-string types (should auto-cast)
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Concat_MixedTypes()
	{
		var result = await S("SELECT CONCAT('value: ', CAST(42 AS STRING))");
		Assert.Equal("value: 42", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// NULL-safe comparison operations
	// ───────────────────────────────────────────────────────────────────────────

	[Fact]
	public async Task NullComparison_Equals()
	{
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAsync<GoogleApiException>(
			() => client.ExecuteQueryAsync("SELECT CASE WHEN NULL = NULL THEN 'true' ELSE 'false' END", parameters: null));
	}

	[Fact]
	public async Task NullComparison_NotEquals()
	{
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAsync<GoogleApiException>(
			() => client.ExecuteQueryAsync("SELECT CASE WHEN NULL != NULL THEN 'true' ELSE 'false' END", parameters: null));
	}

	[Fact] public async Task NullComparison_InWhere()
	{
		var rows = await Q("SELECT x FROM UNNEST([1, CAST(NULL AS INT64), 3]) AS x WHERE x != 1 ORDER BY x");
		Assert.Single(rows); // Only 3; NULL != 1 is NULL (not TRUE)
		Assert.Equal("3", rows[0][0]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// String comparison operators
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task StringComparison_LessThan()
	{
		var result = await S("SELECT 'apple' < 'banana'");
		Assert.Equal("True", result);
	}

	[Fact] public async Task StringComparison_GreaterThan()
	{
		var result = await S("SELECT 'zebra' > 'apple'");
		Assert.Equal("True", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// REPLACE function
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Replace_Multiple()
	{
		var result = await S("SELECT REPLACE('hello world hello', 'hello', 'hi')");
		Assert.Equal("hi world hi", result);
	}

	[Fact] public async Task Replace_NoMatch()
	{
		var result = await S("SELECT REPLACE('hello', 'xyz', 'abc')");
		Assert.Equal("hello", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// SPLIT function
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Split_Basic()
	{
		var result = await S("SELECT ARRAY_LENGTH(SPLIT('a,b,c,d', ','))");
		Assert.Equal("4", result);
	}

	[Fact] public async Task Split_EmptyDelimiter()
	{
		var result = await S("SELECT ARRAY_LENGTH(SPLIT('abc', ''))");
		Assert.Equal("3", result); // each character
	}

	// ───────────────────────────────────────────────────────────────────────────
	// SUBSTR with negative start
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Substr_NegativeStart()
	{
		// BigQuery: SUBSTR(str, -n) counts from end
		var result = await S("SELECT SUBSTR('Hello World', -5)");
		Assert.Equal("World", result);
	}

	[Fact] public async Task Substr_NegativeStart_WithLength()
	{
		var result = await S("SELECT SUBSTR('Hello World', -5, 3)");
		Assert.Equal("Wor", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Window function without ORDER BY (whole partition)
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Window_SumWithoutOrderBy()
	{
		// Without ORDER BY, SUM is over the whole partition
		var rows = await Q(@"
			SELECT x, SUM(x) OVER () AS total
			FROM UNNEST([10, 20, 30]) AS x
			ORDER BY x");
		Assert.Equal("60", rows[0][1]?.ToString());
		Assert.Equal("60", rows[1][1]?.ToString());
		Assert.Equal("60", rows[2][1]?.ToString());
	}

	[Fact] public async Task Window_CountWithPartition()
	{
		var rows = await Q(@"
			WITH data AS (
				SELECT 'A' AS grp, 1 AS val UNION ALL
				SELECT 'A', 2 UNION ALL
				SELECT 'B', 3
			)
			SELECT grp, val, COUNT(*) OVER (PARTITION BY grp) AS grp_cnt
			FROM data
			ORDER BY grp, val");
		Assert.Equal("2", rows[0][2]?.ToString()); // A has 2
		Assert.Equal("2", rows[1][2]?.ToString()); // A has 2
		Assert.Equal("1", rows[2][2]?.ToString()); // B has 1
	}
}
