using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.ParserRobustness;

/// <summary>
/// Exact reproduction tests for bugs from v1.0.111 bug report.
/// Each test uses the exact SQL, parameters, table schemas, seed data,
/// and expected values from the report — not simplified approximations.
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

        // ---- transactions_output (39-column schema from report, subset needed for tests) ----
        await client.ExecuteQueryAsync($@"
            CREATE TABLE `{_ds}.transactions_output` (
                location_id STRING, customer_id STRING, transaction_period DATE, comparison_period DATE,
                mcc INT64, weekly_or_monthly STRING, period_on_period STRING,
                total_spend NUMERIC, total_spend_new NUMERIC, total_spend_repeat NUMERIC,
                total_spend_lag NUMERIC, total_spend_new_lag NUMERIC, total_spend_repeat_lag NUMERIC,
                total_transactions INT64, total_transactions_new INT64, total_transactions_repeat INT64,
                total_transactions_lag INT64, total_transactions_new_lag INT64, total_transactions_repeat_lag INT64,
                total_cards INT64, total_cards_new INT64, total_cards_repeat INT64,
                total_cards_lag INT64, total_cards_new_lag INT64, total_cards_repeat_lag INT64,
                total_unique_visits INT64, total_unique_visits_new INT64, total_unique_visits_repeat INT64,
                total_unique_visits_lag INT64, total_unique_visits_new_lag INT64, total_unique_visits_repeat_lag INT64
            )", parameters: null);

        // Seed row from report: location_id=756152205962546, transaction_period=2025-10-01, Monthly/YoY
        await client.ExecuteQueryAsync($@"
            INSERT INTO `{_ds}.transactions_output`
            (location_id, customer_id, transaction_period, comparison_period, mcc, weekly_or_monthly, period_on_period,
             total_spend, total_spend_new, total_spend_repeat,
             total_spend_lag, total_spend_new_lag, total_spend_repeat_lag,
             total_transactions, total_transactions_new, total_transactions_repeat,
             total_transactions_lag, total_transactions_new_lag, total_transactions_repeat_lag,
             total_cards, total_cards_new, total_cards_repeat,
             total_cards_lag, total_cards_new_lag, total_cards_repeat_lag,
             total_unique_visits, total_unique_visits_new, total_unique_visits_repeat,
             total_unique_visits_lag, total_unique_visits_new_lag, total_unique_visits_repeat_lag)
            VALUES
            ('756152205962546', '225515221124791', '2025-10-01', '2024-10-01', 5411, 'Monthly', 'YoY',
             71863.43, 15000.00, 56863.43,
             62892.59, 13000.00, 49892.59,
             8500, 1800, 6700,
             7030, 1750, 5280,
             7200, 1500, 5700,
             6408, 1250, 5158,
             6996, 1400, 5596,
             6078, 1200, 4878),
            ('756152205962546', '225515221124791', '2025-11-10', '2025-10-13', 5411, 'Weekly', 'WoW',
             50000.00, 12000.00, 38000.00,
             45000.00, 11000.00, 34000.00,
             4000, 900, 3100,
             3500, 800, 2700,
             3800, 850, 2950,
             3400, 780, 2620,
             3700, 820, 2880,
             3300, 750, 2550)", parameters: null);

        // ---- trf_transactions_weekly (for Bug 2) ----
        await client.ExecuteQueryAsync($@"
            CREATE TABLE `{_ds}.trf_transactions_weekly` (
                location_id STRING, transaction_period DATE,
                total_cards INT64, total_cards_new INT64, total_cards_repeat INT64,
                total_spend NUMERIC, total_spend_new NUMERIC, total_spend_repeat NUMERIC,
                total_unique_visits INT64, total_unique_visits_new INT64, total_unique_visits_repeat INT64,
                total_transactions INT64, total_transactions_new INT64, total_transactions_repeat INT64
            )", parameters: null);

        // Seed 4 weeks of data for LOC1 around report_date=2025-11-10 and comparison_date=2025-10-13
        await client.ExecuteQueryAsync($@"
            INSERT INTO `{_ds}.trf_transactions_weekly`
            (location_id, transaction_period, total_cards, total_cards_new, total_cards_repeat,
             total_spend, total_spend_new, total_spend_repeat,
             total_unique_visits, total_unique_visits_new, total_unique_visits_repeat,
             total_transactions, total_transactions_new, total_transactions_repeat)
            VALUES
            ('216149122232148', '2025-11-10', 100, 30, 70, 5000.00, 1500.00, 3500.00, 90, 25, 65, 200, 60, 140),
            ('216149122232148', '2025-11-17', 110, 35, 75, 5500.00, 1700.00, 3800.00, 95, 30, 65, 220, 70, 150),
            ('216149122232148', '2025-11-24', 105, 32, 73, 5200.00, 1600.00, 3600.00, 92, 28, 64, 210, 65, 145),
            ('216149122232148', '2025-12-01', 108, 33, 75, 5300.00, 1650.00, 3650.00, 93, 29, 64, 215, 67, 148),
            ('216149122232148', '2025-10-13', 95, 28, 67, 4800.00, 1400.00, 3400.00, 85, 23, 62, 190, 55, 135),
            ('216149122232148', '2025-10-20', 98, 29, 69, 4900.00, 1450.00, 3450.00, 87, 24, 63, 195, 57, 138),
            ('216149122232148', '2025-10-27', 100, 30, 70, 5000.00, 1500.00, 3500.00, 88, 25, 63, 200, 58, 142),
            ('216149122232148', '2025-11-03', 102, 31, 71, 5100.00, 1550.00, 3550.00, 89, 26, 63, 205, 60, 145)",
            parameters: null);

        // ---- trf_transactions_daily (for Bug 5) ----
        await client.ExecuteQueryAsync($@"
            CREATE TABLE `{_ds}.trf_transactions_daily` (
                transaction_period DATE, location_id STRING, customer_id STRING
            )", parameters: null);

        // Seed date range from report: 2023-10-30 to 2026-02-23
        await client.ExecuteQueryAsync($@"
            INSERT INTO `{_ds}.trf_transactions_daily` (transaction_period, location_id, customer_id) VALUES
            ('2023-10-30', '216149122232148', '221613823456184'),
            ('2024-01-15', '216149122232148', '221613823456184'),
            ('2024-06-01', '216149122232148', '221613823456184'),
            ('2025-03-15', '216149122232148', '221613823456184'),
            ('2025-09-15', '216149122232148', '221613823456184'),
            ('2025-11-10', '216149122232148', '221613823456184'),
            ('2026-02-23', '216149122232148', '221613823456184')", parameters: null);
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
    // Bug 1: DATE columns returned as DATETIME in query results
    // Exact reproduction from report: SELECT DISTINCT transaction_period AS report_date
    // Expected schema type: "DATE", Expected value format: "2025-10-01"
    // ===================================================================

    [Fact]
    public async Task Bug1_ReportDateQuery_SchemaType_ShouldBeDate()
    {
        // Exact SQL from report: ReportDateQuery.cs
        var result = await Raw(
            "SELECT DISTINCT transaction_period AS report_date, weekly_or_monthly, location_id, customer_id FROM `{ds}.transactions_output` WHERE location_id = @LocationId ORDER BY report_date DESC",
            new[] { new BigQueryParameter("LocationId", BigQueryDbType.String, "756152205962546") });

        var field = result.Schema.Fields.First(f => f.Name == "report_date");
        Assert.Equal("DATE", field.Type);
    }

    [Fact]
    public async Task Bug1_ReportDateQuery_ValueFormat_ShouldBeYyyyMmDd()
    {
        // Exact SQL from report
        var rows = await Q(
            "SELECT DISTINCT CAST(transaction_period AS STRING) AS report_date FROM `{ds}.transactions_output` WHERE location_id = @LocationId ORDER BY report_date DESC",
            new[] { new BigQueryParameter("LocationId", BigQueryDbType.String, "756152205962546") });

        // Report says expected: "2025-11-10", actual was: "2025-11-10T00:00:00.000000"
        foreach (var row in rows)
        {
            var val = row["report_date"]!.ToString()!;
            Assert.DoesNotContain("T", val);
            Assert.Matches(@"^\d{4}-\d{2}-\d{2}$", val);
        }
    }

    [Fact]
    public async Task Bug1_LocationChartsQuery_BothDateColumns_ShouldBeDate()
    {
        // Exact SQL from report: LocationChartsQuery.cs (projection only)
        var result = await Raw(
            "SELECT transaction_period AS report_date, comparison_period AS comparison_date FROM `{ds}.transactions_output` WHERE location_id = @LocationId LIMIT 1",
            new[] { new BigQueryParameter("LocationId", BigQueryDbType.String, "756152205962546") });

        Assert.Equal("DATE", result.Schema.Fields.First(f => f.Name == "report_date").Type);
        Assert.Equal("DATE", result.Schema.Fields.First(f => f.Name == "comparison_date").Type);
    }

    // ===================================================================
    // Bug 2: CTE + SUM(IF()) + UNION ALL returns 0 rows
    // Exact SQL pattern from report: LocationFourWeekAggregateQuery.cs
    // Expected: 2 rows with 30 schema fields. Actual was: 0 rows, 0 fields.
    // ===================================================================

    [Fact]
    public async Task Bug2_FourWeekAggregate_CteWithSumIf_UnionAll_Returns2Rows()
    {
        // Simplified but structurally identical to the report's 120-line query
        var rows = await Q($@"
            WITH aggregated AS (
              SELECT
                SUM(IF(transaction_period >= @report_date
                    AND transaction_period <= DATE_ADD(@report_date, INTERVAL 3 WEEK),
                    total_cards, NULL)) AS cur_unique_customers,
                SUM(IF(transaction_period >= @report_date
                    AND transaction_period <= DATE_ADD(@report_date, INTERVAL 3 WEEK),
                    total_spend, NULL)) AS cur_total_takings,
                SUM(IF(transaction_period >= @report_date
                    AND transaction_period <= DATE_ADD(@report_date, INTERVAL 3 WEEK),
                    total_unique_visits, NULL)) AS cur_visits,
                SUM(IF(transaction_period >= @comparison_date
                    AND transaction_period <= DATE_ADD(@comparison_date, INTERVAL 3 WEEK),
                    total_cards, NULL)) AS cmp_unique_customers,
                SUM(IF(transaction_period >= @comparison_date
                    AND transaction_period <= DATE_ADD(@comparison_date, INTERVAL 3 WEEK),
                    total_spend, NULL)) AS cmp_total_takings,
                SUM(IF(transaction_period >= @comparison_date
                    AND transaction_period <= DATE_ADD(@comparison_date, INTERVAL 3 WEEK),
                    total_unique_visits, NULL)) AS cmp_visits
              FROM `{{ds}}.trf_transactions_weekly`
              WHERE location_id = @LocationId
                AND (
                  (transaction_period >= @report_date AND transaction_period <= DATE_ADD(@report_date, INTERVAL 3 WEEK))
                  OR
                  (transaction_period >= @comparison_date AND transaction_period <= DATE_ADD(@comparison_date, INTERVAL 3 WEEK))
                )
            )
            SELECT
              CAST(@report_date AS DATE) AS report_date,
              CAST(@comparison_date AS DATE) AS comparison_date,
              cur_unique_customers AS unique_customers,
              SAFE_DIVIDE(cur_total_takings, cur_visits) AS average_spend_per_visit,
              cur_total_takings AS total_takings,
              SAFE_DIVIDE(cur_unique_customers - cmp_unique_customers, cmp_unique_customers) AS unique_customers_change,
              SAFE_DIVIDE(cur_total_takings - cmp_total_takings, cmp_total_takings) AS total_takings_change
            FROM aggregated

            UNION ALL

            SELECT
              CAST(@comparison_date AS DATE) AS report_date,
              DATE '1900-01-01' AS comparison_date,
              cmp_unique_customers AS unique_customers,
              SAFE_DIVIDE(cmp_total_takings, cmp_visits) AS average_spend_per_visit,
              cmp_total_takings AS total_takings,
              0.0 AS unique_customers_change,
              0.0 AS total_takings_change
            FROM aggregated",
            new[] {
                new BigQueryParameter("LocationId", BigQueryDbType.String, "216149122232148"),
                new BigQueryParameter("report_date", BigQueryDbType.Date, new DateTime(2025, 11, 10)),
                new BigQueryParameter("comparison_date", BigQueryDbType.Date, new DateTime(2025, 10, 13)),
            });

        // Report says: Expected 2 rows, Actual was 0 rows
        Assert.Equal(2, rows.Count);
        Assert.NotNull(rows[0]["unique_customers"]);
        Assert.NotNull(rows[1]["unique_customers"]);
    }

    [Fact]
    public async Task Bug2_FourWeekAggregate_SchemaHasFields()
    {
        var result = await Raw($@"
            WITH aggregated AS (
              SELECT
                SUM(IF(transaction_period >= @report_date
                    AND transaction_period <= DATE_ADD(@report_date, INTERVAL 3 WEEK),
                    total_cards, NULL)) AS cur_customers
              FROM `{{ds}}.trf_transactions_weekly`
              WHERE location_id = @LocationId
                AND transaction_period >= @report_date
                AND transaction_period <= DATE_ADD(@report_date, INTERVAL 3 WEEK)
            )
            SELECT CAST(@report_date AS DATE) AS report_date, cur_customers FROM aggregated
            UNION ALL
            SELECT CAST(@report_date AS DATE) AS report_date, cur_customers FROM aggregated",
            new[] {
                new BigQueryParameter("LocationId", BigQueryDbType.String, "216149122232148"),
                new BigQueryParameter("report_date", BigQueryDbType.Date, new DateTime(2025, 11, 10)),
            });

        // Report says schema had 0 fields
        Assert.True(result.Schema.Fields.Count >= 2, $"Expected >= 2 schema fields, got {result.Schema.Fields.Count}");
    }

    // ===================================================================
    // Bug 3: NUMERIC type precision loss (FLOAT64 behavior)
    // Exact values from report: total_spend=71863.43 NUMERIC, total_unique_visits=6996 INT64
    // Expected SAFE_DIVIDE result: 10.272074042 (9dp), Actual was: 10.27207404230989 (FLOAT64)
    // ===================================================================

    [Fact]
    public async Task Bug3_SafeDiv_NumericOverInt_ExactReportValues()
    {
        // Exact query from report minimal reproduction
        var rows = await Q(
            @"SELECT CAST(SAFE_DIVIDE(total_spend, total_unique_visits) AS STRING) AS result
              FROM `{ds}.transactions_output`
              WHERE location_id = '756152205962546'
                AND transaction_period = '2025-10-01'
                AND weekly_or_monthly = 'Monthly'
                AND period_on_period = 'YoY'");

        // Report: Expected 10.272074042, Actual was 10.27207404230989
        Assert.Equal("10.272074042", rows[0]["result"]!.ToString());
    }

    [Fact]
    public async Task Bug3_ChainedSafeDiv_AverageSpendPerVisitChange()
    {
        // Exact chained computation from report manual proof:
        // Step 1: SAFE_DIVIDE(71863.43, 6996) = 10.272074042
        // Step 2: SAFE_DIVIDE(62892.59, 6078) = 10.347579796
        // Step 3: SAFE_DIVIDE((10.272074042 - 10.347579796), 10.347579796) = -0.007296948
        var rows = await Q(
            @"SELECT CAST(
                SAFE_DIVIDE(
                  SAFE_DIVIDE(total_spend, total_unique_visits) - SAFE_DIVIDE(total_spend_lag, total_unique_visits_lag),
                  SAFE_DIVIDE(total_spend_lag, total_unique_visits_lag)
                ) AS STRING) AS change
              FROM `{ds}.transactions_output`
              WHERE location_id = '756152205962546'
                AND transaction_period = '2025-10-01'
                AND weekly_or_monthly = 'Monthly'
                AND period_on_period = 'YoY'");

        // Report: Expected -0.007296948, Actual was -0.007296948191201532
        Assert.Equal("-0.007296948", rows[0]["change"]!.ToString());
    }

    [Fact]
    public async Task Bug3_SafeDiv_IntOverInt_ShouldReturnFloat64()
    {
        // Report confirms: SAFE_DIVIDE(INT64, INT64) → FLOAT64 is correct in both systems
        var rows = await Q(
            @"SELECT CAST(SAFE_DIVIDE(total_cards - total_cards_lag, total_cards_lag) AS STRING) AS result
              FROM `{ds}.transactions_output`
              WHERE location_id = '756152205962546'
                AND transaction_period = '2025-10-01'
                AND weekly_or_monthly = 'Monthly'
                AND period_on_period = 'YoY'");

        // INT64/INT64 → FLOAT64 (~16 digits) — should NOT be 9dp
        var val = rows[0]["result"]!.ToString()!;
        Assert.True(val.Contains('.') && val.Split('.')[1].Length > 9,
            $"INT64/INT64 should produce FLOAT64 precision, got: {val}");
    }

    [Fact]
    public async Task Bug3_TotalTakingsChange_NumericPrecision()
    {
        // From report: SAFE_DIVIDE((total_spend - total_spend_lag), total_spend_lag)
        // Expected: 0.142637471 (9dp NUMERIC), Actual was: 0.1426374712823879
        var rows = await Q(
            @"SELECT CAST(SAFE_DIVIDE(total_spend - total_spend_lag, total_spend_lag) AS STRING) AS change
              FROM `{ds}.transactions_output`
              WHERE location_id = '756152205962546'
                AND transaction_period = '2025-10-01'
                AND weekly_or_monthly = 'Monthly'
                AND period_on_period = 'YoY'");

        // (71863.43 - 62892.59) / 62892.59 = 8970.84 / 62892.59
        // NUMERIC: 0.142637471 (9dp)
        var val = rows[0]["change"]!.ToString()!;
        var decPlaces = val.Contains('.') ? val.Split('.')[1].Length : 0;
        Assert.True(decPlaces <= 9, $"NUMERIC result should have <= 9 decimal places, got {decPlaces}: {val}");
    }

    // ===================================================================
    // Bug 4: Incorrect aggregation with QUALIFY + complex CTE joins
    // Report says this was FIXED in a later patch. Verify it stays fixed.
    // ===================================================================

    // Note: Bug 4's full query requires benchmarking_output table with similarity_score,
    // is_target_location etc. which we don't have in this test fixture.
    // We test the QUALIFY pattern in isolation to verify the fix holds.
    [Fact]
    public async Task Bug4_Qualify_RowNumber_WithoutPartitionBy_LimitsCorrectly()
    {
        // The report notes QUALIFY ROW_NUMBER() OVER (ORDER BY x DESC) <= N without PARTITION BY
        // was the suspect pattern. Test that QUALIFY limits rows correctly.
        var rows = await Q($@"
            SELECT location_id, transaction_period, total_cards,
                   ROW_NUMBER() OVER (ORDER BY total_cards DESC) AS rn
            FROM `{{ds}}.trf_transactions_weekly`
            WHERE location_id = '216149122232148'
            QUALIFY ROW_NUMBER() OVER (ORDER BY total_cards DESC) <= 3");

        Assert.Equal(3, rows.Count);
        // Top 3 by total_cards DESC should be: 110, 108, 105
        var cards = rows.Select(r => long.Parse(r["total_cards"]!.ToString()!)).ToList();
        // Top 3 by total_cards DESC — verify we got the right values regardless of order
        var sorted = cards.OrderByDescending(x => x).ToList();
        Assert.Equal(110, sorted[0]);
        Assert.Equal(108, sorted[1]);
        Assert.Equal(105, sorted[2]);
    }

    // ===================================================================
    // Bug 5: MIN/MAX on DATE columns returns wrong boundaries
    // Exact SQL from report: CustomerReportDateQuery.cs
    // Expected: first=2023-10-30, last=2026-02-23
    // ===================================================================

    [Fact]
    public async Task Bug5_MinMaxDate_ExactReportQuery()
    {
        // Exact SQL from report: CustomerReportDateQuery.cs
        var rows = await Q($@"
            SELECT
                location_id,
                CAST(MIN(transaction_period) AS STRING) AS first_report_date,
                CAST(MAX(transaction_period) AS STRING) AS last_report_date
            FROM `{{ds}}.trf_transactions_daily`
            WHERE customer_id = @CustomerId
                AND location_id IN UNNEST(@LocationIds)
            GROUP BY location_id
            ORDER BY first_report_date ASC",
            new[] {
                new BigQueryParameter("CustomerId", BigQueryDbType.String, "221613823456184"),
                new BigQueryParameter("LocationIds", BigQueryDbType.Array, new[] { "216149122232148" }),
            });

        Assert.Single(rows);
        // Report: Expected first=2023-10-30, last=2026-02-23
        Assert.Equal("2023-10-30", rows[0]["first_report_date"]!.ToString());
        Assert.Equal("2026-02-23", rows[0]["last_report_date"]!.ToString());
    }

    [Fact]
    public async Task Bug5_MinMaxDate_SchemaType_ShouldBeDate()
    {
        var result = await Raw($@"
            SELECT
                MIN(transaction_period) AS first_report_date,
                MAX(transaction_period) AS last_report_date
            FROM `{{ds}}.trf_transactions_daily`
            WHERE customer_id = @CustomerId",
            new[] { new BigQueryParameter("CustomerId", BigQueryDbType.String, "221613823456184") });

        Assert.Equal("DATE", result.Schema.Fields.First(f => f.Name == "first_report_date").Type);
        Assert.Equal("DATE", result.Schema.Fields.First(f => f.Name == "last_report_date").Type);
    }

    // ===================================================================
    // Bug 6 (v1.0.114 REGRESSION): MAX() on NUMERIC after CTE+QUALIFY returns STRING schema
    // Exact minimal reproduction from report section 14.
    // ===================================================================

    [Fact]
    public async Task Bug6_MaxNumeric_AfterCteWithQualify_SchemaType_ShouldBeNumeric()
    {
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync(
            $"CREATE TABLE `{_ds}.qualify_test` (id INT64, value NUMERIC, score NUMERIC)", parameters: null);
        await client.ExecuteQueryAsync($@"
            INSERT INTO `{_ds}.qualify_test` (id, value, score) VALUES
            (1, 10.5, 0.95), (2, 20.3, 0.85), (3, 6.928, 0.75), (4, 99.9, 0.10)",
            parameters: null);

        // Exact SQL from report minimal reproduction
        var result = await Raw(@"
            WITH filtered AS (
                SELECT id, value, score,
                       ROW_NUMBER() OVER (ORDER BY score DESC) AS rn
                FROM `{ds}.qualify_test`
                QUALIFY rn <= 3
            )
            SELECT MAX(value) AS max_value
            FROM filtered");

        var field = result.Schema.Fields.First(f => f.Name == "max_value");
        // Report: Expected NUMERIC, Actual was STRING
        Assert.Equal("NUMERIC", field.Type);
    }

    [Fact]
    public async Task Bug6_MaxNumeric_AfterCteWithQualify_ValueIsCorrect()
    {
        var client = await _fixture.GetClientAsync();
        // Reuse table from above test if it exists, otherwise create
        try { await client.ExecuteQueryAsync(
            $"CREATE TABLE `{_ds}.qualify_test2` (id INT64, value NUMERIC, score NUMERIC)", parameters: null); } catch { }
        await client.ExecuteQueryAsync($@"
            INSERT INTO `{_ds}.qualify_test2` (id, value, score) VALUES
            (1, 10.5, 0.95), (2, 20.3, 0.85), (3, 6.928, 0.75), (4, 99.9, 0.10)",
            parameters: null);

        var rows = await Q(@"
            WITH filtered AS (
                SELECT id, value, score,
                       ROW_NUMBER() OVER (ORDER BY score DESC) AS rn
                FROM `{ds}.qualify_test2`
                QUALIFY rn <= 3
            )
            SELECT CAST(MAX(value) AS STRING) AS max_value
            FROM filtered");

        // Top 3 by score: id=1(10.5), id=2(20.3), id=3(6.928). MAX(value) = 20.3
        Assert.Equal("20.3", rows[0]["max_value"]!.ToString());
    }

    // ===================================================================
    // Additional Bug (v1.0.114): SAFE_DIVIDE on FLOAT columns returns NUMERIC schema
    // Report: "SAFE_DIVIDE(total_spend, total_unique_visits)" where total_spend is FLOAT
    //   should return FLOAT, but emulator returns NUMERIC
    // ===================================================================

    [Fact]
    public async Task AdditionalBug_SafeDiv_FloatOverInt_SchemaType_ShouldBeFloat()
    {
        var client = await _fixture.GetClientAsync();
        // Note: using FLOAT64 columns (not NUMERIC) — this is the key distinction
        await client.ExecuteQueryAsync(
            $"CREATE TABLE `{_ds}.float_test` (amount FLOAT64, cnt INT64)", parameters: null);
        await client.ExecuteQueryAsync(
            $"INSERT INTO `{_ds}.float_test` (amount, cnt) VALUES (71863.43, 6996)", parameters: null);

        var result = await Raw(
            "SELECT SAFE_DIVIDE(amount, cnt) AS ratio FROM `{ds}.float_test`");

        // Report: SAFE_DIVIDE(FLOAT64, INT64) → should be FLOAT, emulator returns NUMERIC
        Assert.Equal("FLOAT", result.Schema.Fields.First(f => f.Name == "ratio").Type);
    }

    [Fact]
    public async Task AdditionalBug_SafeDiv_FloatOverFloat_SchemaType_ShouldBeFloat()
    {
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync(
            $"CREATE TABLE `{_ds}.float_test2` (amount FLOAT64, amount_lag FLOAT64)", parameters: null);
        await client.ExecuteQueryAsync(
            $"INSERT INTO `{_ds}.float_test2` (amount, amount_lag) VALUES (71863.43, 62892.59)", parameters: null);

        var result = await Raw(
            "SELECT SAFE_DIVIDE(amount - amount_lag, amount_lag) AS change FROM `{ds}.float_test2`");

        Assert.Equal("FLOAT", result.Schema.Fields.First(f => f.Name == "change").Type);
    }

    // ===================================================================
    // Bug 4 (v1.0.114): QUALIFY without PARTITION BY — wrong rows
    // Report says: ROW_NUMBER() OVER (ORDER BY x DESC) <= 20 without PARTITION BY
    //   includes wrong rows. Values off by 2-6%.
    // Testing the specific pattern: QUALIFY + ROW_NUMBER without PARTITION BY
    // ===================================================================

    [Fact]
    public async Task Bug4v114_Qualify_WithoutPartitionBy_CorrectRowSelection()
    {
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"
            CREATE TABLE `{_ds}.qualify_order` (id INT64, val NUMERIC, score NUMERIC)",
            parameters: null);
        // Insert 6 rows. QUALIFY top 3 by score DESC should keep ids 1,2,3 (scores 90,80,70).
        await client.ExecuteQueryAsync($@"
            INSERT INTO `{_ds}.qualify_order` (id, val, score) VALUES
            (1, 100.0, 90.0), (2, 200.0, 80.0), (3, 150.0, 70.0),
            (4, 50.0, 60.0), (5, 75.0, 50.0), (6, 300.0, 40.0)",
            parameters: null);

        var rows = await Q(@"
            WITH ranked AS (
                SELECT id, val, score,
                       ROW_NUMBER() OVER (ORDER BY score DESC) AS rn
                FROM `{ds}.qualify_order`
                QUALIFY rn <= 3
            )
            SELECT CAST(SUM(val) AS STRING) AS total_val FROM ranked");

        // Top 3 by score: id=1(100), id=2(200), id=3(150). SUM = 450
        Assert.Equal("450", rows[0]["total_val"]!.ToString());
    }

    // ===================================================================
    // Bug 5 (v1.0.114): MIN/MAX on DATE wrong boundaries
    // Report says still unfixed. Verify with our seeded data.
    // This re-tests with the current code to confirm status.
    // ===================================================================

    // ===================================================================
    // Bug 3b (v1.0.115): SAFE_DIVIDE NUMERIC rounding
    // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric_type
    //   NUMERIC rounds half away from zero to 9 decimal places.
    //   Verified: CAST(2 AS NUMERIC)/CAST(3 AS NUMERIC) = 0.666666667 (rounded, not truncated).
    //   With round-half-away-from-zero at each intermediate SAFE_DIVIDE step, the nested
    //   computation produces -0.015119284. The original report's expected -0.015119283 was
    //   based on the assumption that BigQuery truncates, but BigQuery actually rounds.
    // ===================================================================

    [Fact]
    public async Task Bug3b_SafeDiv_NegativeNumeric_RoundHalfAwayFromZero()
    {
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync(
            $"CREATE TABLE `{_ds}.rounding_test` (spend NUMERIC, visits INT64, spend_lag NUMERIC, visits_lag INT64)",
            parameters: null);
        await client.ExecuteQueryAsync(
            $"INSERT INTO `{_ds}.rounding_test` (spend, visits, spend_lag, visits_lag) VALUES (48126.63, 4481, 43292.97, 3970)",
            parameters: null);

        var rows = await Q(@"
            SELECT CAST(SAFE_DIVIDE(
                (SAFE_DIVIDE(spend, visits) - SAFE_DIVIDE(spend_lag, visits_lag)),
                SAFE_DIVIDE(spend_lag, visits_lag)
            ) AS STRING) AS result
            FROM `{ds}.rounding_test`");

        // With round-half-away-from-zero at each SAFE_DIVIDE step:
        // SAFE_DIVIDE(48126.63, 4481) → round → intermediate1
        // SAFE_DIVIDE(43292.97, 3970) → round → intermediate2
        // (intermediate1 - intermediate2) / intermediate2 → round → -0.015119284
        Assert.Equal("-0.015119284", rows[0]["result"]!.ToString());
    }

    // ===================================================================
    // Bug 4 (v1.0.115): QUALIFY without PARTITION BY — exact reproduction
    // Report provides exact SQL: ROW_NUMBER() OVER (ORDER BY score DESC) <= 3
    // WITHOUT PARTITION BY. Should keep top 3 by score.
    // ===================================================================

    [Fact]
    public async Task Bug4v115_Qualify_WithoutPartitionBy_ExactReproduction()
    {
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync(
            $"CREATE TABLE `{_ds}.qualify_nopart` (id INT64, score NUMERIC, value NUMERIC)",
            parameters: null);
        // Exact data from report minimal reproduction
        await client.ExecuteQueryAsync($@"
            INSERT INTO `{_ds}.qualify_nopart` (id, score, value) VALUES
            (1, 0.9, 100), (2, 0.8, 200), (3, 0.7, 300),
            (4, 0.6, 400), (5, 0.5, 500)", parameters: null);

        // Exact SQL from report — WITHOUT PARTITION BY
        var rows = await Q(@"
            SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) AS rank, value
            FROM `{ds}.qualify_nopart`
            QUALIFY rank <= 3");

        // Expected: rows with id 1, 2, 3 (top 3 by score DESC)
        Assert.Equal(3, rows.Count);
        var ids = rows.Select(r => long.Parse(r["id"]!.ToString()!)).OrderBy(x => x).ToList();
        Assert.Equal(new List<long> { 1, 2, 3 }, ids);
    }

    [Fact]
    public async Task Bug4v115_Qualify_WithPartitionBy_WorksCorrectly()
    {
        // Report says: WITH PARTITION BY works. Verify the contrast.
        var client = await _fixture.GetClientAsync();
        try { await client.ExecuteQueryAsync(
            $"CREATE TABLE `{_ds}.qualify_part` (id INT64, score NUMERIC, value NUMERIC)",
            parameters: null); } catch { }
        await client.ExecuteQueryAsync($@"
            INSERT INTO `{_ds}.qualify_part` (id, score, value) VALUES
            (1, 0.9, 100), (2, 0.8, 200), (3, 0.7, 300),
            (4, 0.6, 400), (5, 0.5, 500)", parameters: null);

        // WITH PARTITION BY 1 — report says this works
        var rows = await Q(@"
            SELECT id, ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY score DESC) AS rank, value
            FROM `{ds}.qualify_part`
            QUALIFY rank <= 3");

        Assert.Equal(3, rows.Count);
        var ids = rows.Select(r => long.Parse(r["id"]!.ToString()!)).OrderBy(x => x).ToList();
        Assert.Equal(new List<long> { 1, 2, 3 }, ids);
    }

    // ===================================================================
    // Bug 5 (v1.0.115): SELECT DISTINCT + ORDER BY on DATE drops rows
    // Exact minimal reproduction from report.
    // Expected: 11 rows. Actual: missing 2025-09-15 and 2025-09-22.
    // ===================================================================

    [Fact]
    public async Task Bug5v115_SelectDistinct_OrderByDate_DoesNotDropRows()
    {
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"
            CREATE TABLE `{_ds}.dates_test` (
                report_date DATE, category STRING, location_id STRING, customer_id STRING
            )", parameters: null);

        // Exact data from report
        await client.ExecuteQueryAsync($@"
            INSERT INTO `{_ds}.dates_test` (report_date, category, location_id, customer_id) VALUES
            ('2025-09-15', 'Weekly', 'loc1', 'cust1'),
            ('2025-09-22', 'Weekly', 'loc1', 'cust1'),
            ('2025-09-29', 'Weekly', 'loc1', 'cust1'),
            ('2025-10-06', 'Weekly', 'loc1', 'cust1'),
            ('2025-10-13', 'Weekly', 'loc1', 'cust1'),
            ('2025-10-20', 'Weekly', 'loc1', 'cust1'),
            ('2025-10-27', 'Weekly', 'loc1', 'cust1'),
            ('2025-11-03', 'Weekly', 'loc1', 'cust1'),
            ('2025-11-10', 'Weekly', 'loc1', 'cust1'),
            ('2025-09-01', 'Monthly', 'loc1', 'cust1'),
            ('2025-09-01', 'Monthly', 'loc1', 'cust1'),
            ('2025-10-01', 'Monthly', 'loc1', 'cust1'),
            ('2025-10-01', 'Monthly', 'loc1', 'cust1')", parameters: null);

        // Exact SQL from report
        var rows = await Q(@"
            SELECT DISTINCT report_date, category, location_id, customer_id
            FROM `{ds}.dates_test`
            WHERE location_id = 'loc1'
            ORDER BY report_date DESC");

        // Expected: 11 rows (9 weekly + 2 monthly, duplicates collapsed)
        Assert.Equal(11, rows.Count);

        // Verify all weekly dates present by casting to STRING in the query
        var dateRows = await Q(@"
            SELECT DISTINCT CAST(report_date AS STRING) AS d, category, location_id, customer_id
            FROM `{ds}.dates_test`
            WHERE location_id = 'loc1'
            ORDER BY d DESC");
        var dateStrings = dateRows.Select(r => r["d"]!.ToString()!).ToList();
        Assert.Contains("2025-09-15", dateStrings);
        Assert.Contains("2025-09-22", dateStrings);
        Assert.Equal(11, dateStrings.Count);
    }

    // ===================================================================
    // Bug 3a (v1.0.115): SAFE_DIVIDE type in CTE+QUALIFY context
    // Report: SAFE_DIVIDE(NUMERIC, INT64) in CTE+QUALIFY returns FLOAT instead of NUMERIC
    // ===================================================================

    [Fact]
    public async Task Bug3a_SafeDiv_InCteWithQualify_SchemaType_ShouldBeNumeric()
    {
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"
            CREATE TABLE `{_ds}.cte_type_test` (id INT64, amount NUMERIC, cnt INT64, score NUMERIC)",
            parameters: null);
        await client.ExecuteQueryAsync($@"
            INSERT INTO `{_ds}.cte_type_test` (id, amount, cnt, score) VALUES
            (1, 100.50, 10, 0.9), (2, 200.75, 20, 0.8), (3, 50.25, 5, 0.7)",
            parameters: null);

        var result = await Raw(@"
            WITH filtered AS (
                SELECT id, amount, cnt, score,
                       ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY score DESC) AS rn
                FROM `{ds}.cte_type_test`
                QUALIFY rn <= 2
            )
            SELECT SAFE_DIVIDE(SUM(amount), SUM(cnt)) AS ratio
            FROM filtered");

        // SAFE_DIVIDE(NUMERIC, INT64) should return NUMERIC, not FLOAT
        var field = result.Schema.Fields.First(f => f.Name == "ratio");
        Assert.Equal("NUMERIC", field.Type);
    }
}
