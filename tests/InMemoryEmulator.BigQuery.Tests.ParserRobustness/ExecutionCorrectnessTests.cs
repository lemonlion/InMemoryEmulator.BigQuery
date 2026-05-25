using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.ParserRobustness;

/// <summary>
/// Tests for query execution correctness bugs found in real-world usage.
/// Ref: https://github.com/lemonlion/InMemoryEmulator.BigQuery/issues
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ExecutionCorrectnessTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _ds = null!;

    public ExecutionCorrectnessTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _ds = $"ec_{Guid.NewGuid():N}"[..25];
        await _fixture.CreateDatasetAsync(_ds);
        var client = await _fixture.GetClientAsync();

        await client.ExecuteQueryAsync(
            $"CREATE TABLE `{_ds}.txns` (id INT64, location_id STRING, transaction_period DATE, comparison_period DATE, amount NUMERIC, amount_lag NUMERIC, weekly_or_monthly STRING)",
            parameters: null);
        await client.ExecuteQueryAsync(
            $@"INSERT INTO `{_ds}.txns` (id, location_id, transaction_period, comparison_period, amount, amount_lag, weekly_or_monthly) VALUES
            (1, 'LOC1', '2025-11-10', '2025-10-13', 100.50, 95.25, 'Weekly'),
            (2, 'LOC1', '2025-11-17', '2025-10-20', 200.75, 180.00, 'Weekly'),
            (3, 'LOC1', '2025-10-13', '2025-09-15', 95.25, 88.00, 'Weekly'),
            (4, 'LOC2', '2025-11-10', '2025-10-13', 50.00, 60.00, 'Weekly')",
            parameters: null);
    }

    public async ValueTask DisposeAsync()
    {
        try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
        await _fixture.DisposeAsync();
    }

    private async Task<BigQueryResults> Raw(string sql, IEnumerable<BigQueryParameter>? parameters = null)
    {
        var client = await _fixture.GetClientAsync();
        return await client.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters);
    }

    private async Task<List<BigQueryRow>> Q(string sql, IEnumerable<BigQueryParameter>? parameters = null)
    {
        var result = await Raw(sql, parameters);
        return result.ToList();
    }

    // ===================================================================
    // Bug 1: DATE columns returned as DATETIME in schema
    // ===================================================================

    [Fact]
    public async Task Bug1_DateColumn_SchemaType_ShouldBeDate()
    {
        var result = await Raw("SELECT transaction_period FROM `{ds}.txns` LIMIT 1");
        var field = result.Schema.Fields.First(f => f.Name == "transaction_period");
        Assert.Equal("DATE", field.Type);
    }

    [Fact]
    public async Task Bug1_DateColumn_AliasedInSelect_ShouldBeDate()
    {
        var result = await Raw("SELECT transaction_period AS report_date FROM `{ds}.txns` LIMIT 1");
        var field = result.Schema.Fields.First(f => f.Name == "report_date");
        Assert.Equal("DATE", field.Type);
    }

    [Fact]
    public async Task Bug1_DateColumn_InDistinct_ShouldBeDate()
    {
        var result = await Raw("SELECT DISTINCT transaction_period AS report_date FROM `{ds}.txns`");
        var field = result.Schema.Fields.First(f => f.Name == "report_date");
        Assert.Equal("DATE", field.Type);
    }

    [Fact]
    public async Task Bug1_DateColumn_ValueFormat_ShouldBeYyyyMmDd()
    {
        var rows = await Q("SELECT CAST(transaction_period AS STRING) AS d FROM `{ds}.txns` WHERE id = 1");
        Assert.Equal("2025-11-10", rows[0]["d"]?.ToString());
    }

    // ===================================================================
    // Bug 5: MIN/MAX on DATE columns
    // ===================================================================

    [Fact]
    public async Task Bug5_MinMaxDate_ReturnsCorrectBoundaries()
    {
        var rows = await Q(
            "SELECT CAST(MIN(transaction_period) AS STRING) AS min_d, CAST(MAX(transaction_period) AS STRING) AS max_d FROM `{ds}.txns` WHERE location_id = 'LOC1'");
        Assert.Equal("2025-10-13", rows[0]["min_d"]?.ToString());
        Assert.Equal("2025-11-17", rows[0]["max_d"]?.ToString());
    }

    [Fact]
    public async Task Bug5_MinMaxDate_SchemaType_ShouldBeDate()
    {
        var result = await Raw(
            "SELECT MIN(transaction_period) AS first_date, MAX(transaction_period) AS last_date FROM `{ds}.txns`");
        Assert.Equal("DATE", result.Schema.Fields.First(f => f.Name == "first_date").Type);
        Assert.Equal("DATE", result.Schema.Fields.First(f => f.Name == "last_date").Type);
    }

    // ===================================================================
    // Bug 2: CTE + SUM(IF()) + UNION ALL returns 0 rows
    // ===================================================================

    [Fact]
    public async Task Bug2_SumIf_ConditionalAggregation_ReturnsRows()
    {
        var rows = await Q($@"
            WITH aggregated AS (
              SELECT
                location_id,
                SUM(IF(transaction_period >= @rd AND transaction_period <= DATE_ADD(@rd, INTERVAL 3 WEEK), amount, NULL)) AS cur_amount,
                SUM(IF(transaction_period >= @cd AND transaction_period <= DATE_ADD(@cd, INTERVAL 3 WEEK), amount, NULL)) AS cmp_amount
              FROM `{{ds}}.txns`
              WHERE location_id = 'LOC1'
              GROUP BY location_id
            )
            SELECT location_id, cur_amount, cmp_amount FROM aggregated",
            new[] {
                new BigQueryParameter("rd", BigQueryDbType.Date, new DateTime(2025, 11, 10)),
                new BigQueryParameter("cd", BigQueryDbType.Date, new DateTime(2025, 10, 13)),
            });
        Assert.Single(rows);
        Assert.NotNull(rows[0]["cur_amount"]);
    }

    [Fact]
    public async Task Bug2_SumIf_WithUnionAll_ReturnsTwoRows()
    {
        var rows = await Q($@"
            WITH aggregated AS (
              SELECT
                location_id,
                SUM(IF(transaction_period >= @rd AND transaction_period <= DATE_ADD(@rd, INTERVAL 3 WEEK), amount, NULL)) AS cur_amount,
                SUM(IF(transaction_period >= @cd AND transaction_period <= DATE_ADD(@cd, INTERVAL 3 WEEK), amount, NULL)) AS cmp_amount
              FROM `{{ds}}.txns`
              WHERE location_id = 'LOC1'
              GROUP BY location_id
            )
            SELECT location_id, CAST(@rd AS DATE) AS report_date, cur_amount AS amount FROM aggregated
            UNION ALL
            SELECT location_id, CAST(@cd AS DATE) AS report_date, cmp_amount AS amount FROM aggregated",
            new[] {
                new BigQueryParameter("rd", BigQueryDbType.Date, new DateTime(2025, 11, 10)),
                new BigQueryParameter("cd", BigQueryDbType.Date, new DateTime(2025, 10, 13)),
            });
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task Bug2_SumIf_WithSafeDiv_AndWindowFunction()
    {
        var rows = await Q($@"
            WITH aggregated AS (
              SELECT
                location_id,
                SUM(IF(transaction_period >= @rd AND transaction_period <= DATE_ADD(@rd, INTERVAL 3 WEEK), amount, NULL)) AS cur_amount,
                SUM(IF(transaction_period >= @cd AND transaction_period <= DATE_ADD(@cd, INTERVAL 3 WEEK), amount, NULL)) AS cmp_amount
              FROM `{{ds}}.txns`
              WHERE location_id = 'LOC1'
              GROUP BY location_id
            )
            SELECT
              location_id,
              cur_amount,
              SAFE_DIVIDE(cur_amount - cmp_amount, cmp_amount) AS change_ratio,
              SUM(cur_amount) OVER () AS total_cur
            FROM aggregated",
            new[] {
                new BigQueryParameter("rd", BigQueryDbType.Date, new DateTime(2025, 11, 10)),
                new BigQueryParameter("cd", BigQueryDbType.Date, new DateTime(2025, 10, 13)),
            });
        Assert.Single(rows);
        Assert.NotNull(rows[0]["change_ratio"]);
    }

    [Fact]
    public async Task Bug1_StreamingInsert_DateColumn_SchemaPreserved()
    {
        var client = await _fixture.GetClientAsync();
        // Create table and insert via streaming (InsertRowsAsync)
        await client.ExecuteQueryAsync(
            $"CREATE TABLE `{_ds}.streamed` (id INT64, event_date DATE)", parameters: null);
        var tableRef = client.GetTableReference(_ds, "streamed");
        var table = await client.GetTableAsync(tableRef);
        await table.InsertRowsAsync(new[] {
            new BigQueryInsertRow { ["id"] = 1, ["event_date"] = "2025-06-15" }
        });
        var result = await Raw($"SELECT event_date FROM `{_ds}.streamed`");
        var field = result.Schema.Fields.First(f => f.Name == "event_date");
        Assert.Equal("DATE", field.Type);
    }

    // ===================================================================
    // Bug 3: NUMERIC precision
    // ===================================================================

    // Ref: NUMERIC precision bug report — exact reproduction from minimal SQL
    //   SAFE_DIVIDE(NUMERIC, INT64) should return NUMERIC (9 decimal places)
    [Fact]
    public async Task Bug3_SafeDiv_NumericOverInt_Returns9DecimalPlaces()
    {
        // Seed via streaming insert with decimal value
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync(
            $"CREATE TABLE `{_ds}.numeric_test` (amount NUMERIC, cnt INT64)", parameters: null);
        await client.ExecuteQueryAsync(
            $"INSERT INTO `{_ds}.numeric_test` (amount, cnt) VALUES (71863.43, 6996)", parameters: null);

        var rows = await Q(
            "SELECT CAST(SAFE_DIVIDE(amount, cnt) AS STRING) AS ratio FROM `{ds}.numeric_test`");
        // Real BigQuery: 10.272074042 (exactly 9 decimal places)
        Assert.Equal("10.272074042", rows[0]["ratio"]!.ToString());
    }

    [Fact]
    public async Task Bug3_SafeDiv_NestedNumeric_ChainedPrecision()
    {
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync(
            $"CREATE TABLE `{_ds}.numeric_chain` (spend NUMERIC, visits INT64, spend_lag NUMERIC, visits_lag INT64)",
            parameters: null);
        await client.ExecuteQueryAsync(
            $"INSERT INTO `{_ds}.numeric_chain` (spend, visits, spend_lag, visits_lag) VALUES (71863.43, 6996, 62892.59, 6078)",
            parameters: null);

        // Chained SAFE_DIVIDE: (spend/visits - spend_lag/visits_lag) / (spend_lag/visits_lag)
        var rows = await Q(@"
            SELECT CAST(
              SAFE_DIVIDE(
                SAFE_DIVIDE(spend, visits) - SAFE_DIVIDE(spend_lag, visits_lag),
                SAFE_DIVIDE(spend_lag, visits_lag)
              ) AS STRING) AS change
            FROM `{ds}.numeric_chain`");
        // Real BigQuery: -0.007296948 (9 decimal places, NUMERIC precision)
        Assert.Equal("-0.007296948", rows[0]["change"]!.ToString());
    }
}
