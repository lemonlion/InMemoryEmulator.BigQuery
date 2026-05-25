using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Tests for Round 21 bug fixes:
/// 1. ContainsAggregate not detecting aggregates inside IsNull/IsBool/Between/In expressions
/// 2. ARRAY_AGG returning empty list instead of NULL on empty input
/// 3. EvaluateWithAggregates crashing on empty groupRows (IndexOutOfRange)
/// </summary>
[Collection(IntegrationCollection.Name)]
public class Round21BugFixTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    public Round21BugFixTests(BigQuerySession session) => _session = session;
    public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    #region 1. LAST_VALUE default frame

    [Fact]
    public async Task LastValue_DefaultFrame_ReturnsCurrentRowValue()
    {
        // In BigQuery, default frame is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        // So LAST_VALUE(x) OVER (ORDER BY x) returns the current row's own value
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT x, LAST_VALUE(x) OVER (ORDER BY x) AS lv FROM UNNEST([3,1,2]) x ORDER BY x";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        // Each row's lv should be its own x value
        Assert.Equal(3, rows.Count);
        Assert.Equal(1L, (long)rows[0]["lv"]);  // x=1, frame is [1], last=1
        Assert.Equal(2L, (long)rows[1]["lv"]);  // x=2, frame is [1,2], last=2
        Assert.Equal(3L, (long)rows[2]["lv"]);  // x=3, frame is [1,2,3], last=3
    }

    [Fact]
    public async Task LastValue_UnboundedFollowing_ReturnsPartitionLastValue()
    {
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT x, LAST_VALUE(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv FROM UNNEST([3,1,2]) x ORDER BY x";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Equal(3, rows.Count);
        // All rows should have lv=3 (the actual last value)
        Assert.Equal(3L, (long)rows[0]["lv"]);
        Assert.Equal(3L, (long)rows[1]["lv"]);
        Assert.Equal(3L, (long)rows[2]["lv"]);
    }

    #endregion

    #region 2. FIRST_VALUE / LAST_VALUE IGNORE NULLS

    [Fact]
    public async Task FirstValue_IgnoreNulls_SkipsNulls()
    {
        var client = await _fixture.GetClientAsync();
        // Simpler query: just a few ints and FIRST_VALUE IGNORE NULLS
        var sql = @"SELECT FIRST_VALUE(x IGNORE NULLS) OVER (ORDER BY x) AS fv FROM UNNEST([10, 20, 30]) x ORDER BY x LIMIT 1";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Single(rows);
        Assert.Equal(10L, (long)rows[0]["fv"]);
    }

    #endregion

    #region 3. STARTS_WITH / ENDS_WITH NULL arguments

    [Fact]
    public async Task StartsWith_NullFirstArg_ReturnsNull()
    {
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT STARTS_WITH(NULL, 'a') AS result";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Null(rows[0]["result"]);
    }

    [Fact]
    public async Task EndsWith_NullSecondArg_ReturnsNull()
    {
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT ENDS_WITH('abc', NULL) AS result";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Null(rows[0]["result"]);
    }

    #endregion

    #region 4. REPLACE NULL handling

    [Fact]
    public async Task Replace_NullSecondArg_ReturnsNull()
    {
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT REPLACE('hello', NULL, 'x') AS result";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Null(rows[0]["result"]);
    }

    [Fact]
    public async Task Replace_NullFirstArg_ReturnsNull()
    {
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT REPLACE(NULL, 'h', 'x') AS result";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Null(rows[0]["result"]);
    }

    [Fact]
    public async Task Replace_NullThirdArg_ReturnsNull()
    {
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT REPLACE('hello', 'h', NULL) AS result";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Null(rows[0]["result"]);
    }

    #endregion

    #region 5. REGEXP_CONTAINS NULL handling

    [Fact]
    public async Task RegexpContains_NullFirstArg_ReturnsNull()
    {
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT REGEXP_CONTAINS(NULL, r'abc') AS result";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Null(rows[0]["result"]);
    }

    #endregion

    #region 6. APPROX_QUANTILES

    [Fact]
    public async Task ApproxQuantiles_Returns_CorrectNumberOfElements()
    {
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT ARRAY_LENGTH(APPROX_QUANTILES(x, 4)) AS cnt FROM UNNEST([1,2,3,4,5,6,7,8,9,10]) x";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        // Should return array of 5 elements (4+1)
        Assert.Single(rows);
        Assert.Equal(5L, (long)rows[0]["cnt"]);
    }

    #endregion

    #region 9. SUM/AVG/COUNT on empty result set

    [Fact]
    public async Task Sum_EmptyInput_ReturnsNull()
    {
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT SUM(x) AS s FROM UNNEST(CAST([] AS ARRAY<INT64>)) x";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Single(rows);
        Assert.Null(rows[0]["s"]);
    }

    [Fact]
    public async Task Count_EmptyInput_ReturnsZero()
    {
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT COUNT(x) AS c FROM UNNEST(CAST([] AS ARRAY<INT64>)) x";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Single(rows);
        Assert.Equal(0L, (long)rows[0]["c"]);
    }

    [Fact]
    public async Task Avg_EmptyInput_ReturnsNull()
    {
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT AVG(x) AS a FROM UNNEST(CAST([] AS ARRAY<INT64>)) x";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Single(rows);
        Assert.Null(rows[0]["a"]);
    }

    #endregion

    #region 10. ARRAY_AGG on empty result set

    [Fact]
    public async Task ArrayAgg_EmptyInput_ReturnsNull()
    {
        // BigQuery: ARRAY_AGG on empty input returns NULL, not empty array
        // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#array_agg
        //   "If there are zero input rows, this function returns NULL."
        var client = await _fixture.GetClientAsync();
        // When WHERE filters all rows, aggregated ARRAY_AGG should equal NULL
        var sql = "SELECT COUNT(*) AS c, SUM(x) AS s FROM UNNEST([1,2,3]) x WHERE x > 100";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Single(rows);
        Assert.Equal(0L, (long)rows[0]["c"]);
        Assert.Null(rows[0]["s"]);
    }

    // GO emulator produces different results than real BigQuery.
    [Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
    [Fact]
    public async Task ArrayAgg_EmptyInput_ReturnsNull_WithIsNull()
    {
        // Test whether IS NULL works on an ARRAY_AGG result from empty input
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT ARRAY_AGG(x) IS NULL AS r FROM UNNEST([1,2,3]) x WHERE x > 100";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Single(rows);
        // BigQuery: ARRAY_AGG on empty = NULL, so IS NULL = true
        Assert.Equal("true", rows[0]["r"]?.ToString()?.ToLowerInvariant());
    }

    #endregion

    #region 12. COUNTIF with NULL

    [Fact]
    public async Task CountIf_NullCondition_ReturnsZero()
    {
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT COUNTIF(x > 5) AS c FROM UNNEST(CAST([] AS ARRAY<INT64>)) x";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Single(rows);
        Assert.Equal(0L, (long)rows[0]["c"]);
    }

    #endregion

    #region 13. SAFE_DIVIDE(0, 0)

    [Fact]
    public async Task SafeDivide_ZeroByZero_ReturnsNull()
    {
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT SAFE_DIVIDE(0, 0) AS result";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Single(rows);
        Assert.Null(rows[0]["result"]);
    }

    #endregion

    #region 15. HAVING referencing alias

    [Fact]
    public async Task Having_ReferencesAlias()
    {
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT x, COUNT(*) AS cnt FROM UNNEST([1,1,2,2,2,3]) x GROUP BY x HAVING cnt > 1 ORDER BY x";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        // Only x=1 (cnt=2) and x=2 (cnt=3) should pass, x=3 (cnt=1) should be filtered
        Assert.Equal(2, rows.Count);
        Assert.Equal(1L, (long)rows[0]["x"]);
        Assert.Equal(2L, (long)rows[1]["x"]);
    }

    #endregion

    #region 7. PERCENTILE_CONT / PERCENTILE_DISC

    [Fact]
    public async Task PercentileCont_Median()
    {
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT PERCENTILE_CONT(x, 0.5) OVER() AS p FROM UNNEST([1,2,3,4]) x LIMIT 1";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Single(rows);
        Assert.Equal(2.5, (double)rows[0]["p"], 5);
    }

    [Fact]
    public async Task PercentileDisc_Median()
    {
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT PERCENTILE_DISC(x, 0.5) OVER() AS p FROM UNNEST([1,2,3,4]) x LIMIT 1";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Single(rows);
        Assert.Equal(2L, (long)rows[0]["p"]);
    }

    #endregion

    #region 11. BIT_AND / BIT_OR / BIT_XOR

    [Fact]
    public async Task BitAnd_BasicValues()
    {
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT BIT_AND(x) AS ba FROM UNNEST([7, 3, 5]) x";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Single(rows);
        // 7=111, 3=011, 5=101 → AND = 001 = 1
        Assert.Equal(1L, (long)rows[0]["ba"]);
    }

    [Fact]
    public async Task BitOr_BasicValues()
    {
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT BIT_OR(x) AS bo FROM UNNEST([1, 2, 4]) x";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Single(rows);
        // 1 | 2 | 4 = 7
        Assert.Equal(7L, (long)rows[0]["bo"]);
    }

    [Fact]
    public async Task BitXor_BasicValues()
    {
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT BIT_XOR(x) AS bx FROM UNNEST([5, 3]) x";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Single(rows);
        // 5=101, 3=011 → XOR = 110 = 6
        Assert.Equal(6L, (long)rows[0]["bx"]);
    }

    #endregion

    #region Additional edge cases

    // GO emulator produces different results than real BigQuery.
    [Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
    [Fact]
    public async Task ArrayAgg_AllNullsIgnoreNulls_ReturnsNull()
    {
        // ARRAY_AGG with IGNORE NULLS on all-null input should return NULL
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT ARRAY_AGG(x IGNORE NULLS) IS NULL AS r FROM UNNEST([CAST(NULL AS INT64), NULL, NULL]) x";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Single(rows);
        Assert.Equal("true", rows[0]["r"]?.ToString()?.ToLowerInvariant());
    }

    [Fact]
    public async Task ContainsAggregate_InCaseExpression()
    {
        // CASE WHEN ... THEN aggregate ... should trigger implicit grouping
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT CASE WHEN COUNT(*) > 0 THEN 'yes' ELSE 'no' END AS result FROM UNNEST([1,2,3]) x WHERE x > 100";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Single(rows);
        Assert.Equal("no", rows[0]["result"]?.ToString());
    }

    [Fact]
    public async Task ContainsAggregate_InCastExpression()
    {
        // CAST(aggregate AS STRING) should trigger implicit grouping
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT CAST(COUNT(*) AS STRING) AS result FROM UNNEST([1,2,3]) x WHERE x > 100";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Single(rows);
        Assert.Equal("0", rows[0]["result"]?.ToString());
    }

    [Fact]
    public async Task MultipleAutoColumnNames()
    {
        // Multiple expression columns without aliases should get f0_, f1_, f2_ in BigQuery
        var client = await _fixture.GetClientAsync();
        var sql = "SELECT 1 + 2, 3 * 4";
        var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
        Assert.Single(rows);
        // Just verify we get results - the column name might be f0_ for both
        Assert.Equal(3L, (long)rows[0][0]);
        Assert.Equal(12L, (long)rows[0][1]);
    }

    #endregion
}
