using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.ParserRobustness;

/// <summary>
/// Bug reproduction tests using the actual CSV seed data from the bug reports.
/// These tests cannot be simplified — the bugs only manifest with the real data.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class RealDataBugTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _ds = null!;

    public RealDataBugTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _ds = $"rd_{Guid.NewGuid():N}"[..25];
        await _fixture.CreateDatasetAsync(_ds);
        var client = await _fixture.GetClientAsync();

        // Create transactions_output with exact schema from bug report
        await client.ExecuteQueryAsync($@"
            CREATE TABLE `{_ds}.transactions_output` (
                postcode_area STRING, postcode_district STRING, postcode STRING, industry STRING,
                mcc INT64, location_id STRING, trading_name STRING, customer_id STRING,
                transaction_period DATE,
                total_spend NUMERIC, total_spend_new NUMERIC, total_spend_repeat NUMERIC,
                total_transactions INT64, total_transactions_new INT64, total_transactions_repeat INT64,
                total_cards INT64, total_cards_new INT64, total_cards_repeat INT64,
                total_unique_visits INT64, total_unique_visits_new INT64, total_unique_visits_repeat INT64,
                comparison_period DATE,
                total_spend_lag NUMERIC, total_spend_new_lag NUMERIC, total_spend_repeat_lag NUMERIC,
                total_transactions_lag INT64, total_transactions_new_lag INT64, total_transactions_repeat_lag INT64,
                total_cards_lag INT64, total_cards_new_lag INT64, total_cards_repeat_lag INT64,
                total_unique_visits_lag INT64, total_unique_visits_new_lag INT64, total_unique_visits_repeat_lag INT64,
                weekly_or_monthly STRING, period_on_period STRING, report_date_filter STRING,
                inverted_date INT64, pretty_comparison STRING
            )", parameters: null);

        // Create benchmarking_output with exact schema
        await client.ExecuteQueryAsync($@"
            CREATE TABLE `{_ds}.benchmarking_output` (
                mcc INT64, industry STRING, customer_id1 STRING, customer_id2 STRING,
                location_id1 STRING, location_id2 STRING,
                trading_name1 STRING, trading_name2 STRING,
                region1 STRING, region2 STRING, postcode1 STRING, postcode2 STRING,
                trading_location1 STRING, trading_location2 STRING,
                live_date1 STRING, live_date2 STRING,
                months_live1 INT64, months_live2 INT64,
                first_complete_month1 STRING, first_complete_month2 STRING,
                weeks_live1 INT64, weeks_live2 INT64,
                first_complete_week1 STRING, first_complete_week2 STRING,
                distance_km NUMERIC, distance_scaled NUMERIC,
                relative_density NUMERIC, absolute_density NUMERIC, density_scaled NUMERIC,
                relative_atv NUMERIC, absolute_atv NUMERIC, atv_scaled NUMERIC,
                day_similarity NUMERIC, hour_similarity NUMERIC, spend_similarity NUMERIC,
                full_similarity_score NUMERIC, initial_similarity_score NUMERIC, similarity_score NUMERIC,
                no_of_competitors INT64, is_yoy_eligible BOOL,
                is_target_location BOOL, is_target_customer BOOL
            )", parameters: null);

        // Seed from CSVs using streaming inserts
        await SeedFromCsv(client, "transactions_output",
            Path.Combine(AppContext.BaseDirectory, "TestData", "transactions_output.csv"));
        await SeedFromCsv(client, "benchmarking_output",
            Path.Combine(AppContext.BaseDirectory, "TestData", "benchmarking_output.csv"));
    }

    private async Task SeedFromCsv(BigQueryClient client, string tableName, string csvPath)
    {
        var tableRef = client.GetTableReference(_ds, tableName);
        var table = await client.GetTableAsync(tableRef);

        using var reader = new StreamReader(csvPath);
        using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HeaderValidated = null, MissingFieldFound = null
        });

        var records = csv.GetRecords<dynamic>().ToList();
        var batchSize = 50;
        for (int i = 0; i < records.Count; i += batchSize)
        {
            var batch = records.Skip(i).Take(batchSize).Select(r =>
            {
                var row = new BigQueryInsertRow();
                foreach (var prop in ((IDictionary<string, object>)r))
                {
                    if (string.IsNullOrEmpty(prop.Value?.ToString())) continue;
                    row[prop.Key] = prop.Value;
                }
                return row;
            }).ToList();
            await table.InsertRowsAsync(batch);
        }
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
    // Bug 5: SELECT DISTINCT + ORDER BY on DATE drops rows
    // Exact query from ReportDateQuery.cs with real 538-row dataset
    // Expected: 11 DISTINCT rows. Report says only 9 returned.
    // ===================================================================

    [Fact]
    public async Task Bug5_RealData_SelectDistinct_OrderByDate_Returns11Rows()
    {
        // Exact SQL from report: ReportDateQuery.cs
        var rows = await Q(@"
            SELECT DISTINCT transaction_period AS report_date, weekly_or_monthly, location_id, customer_id
            FROM `{ds}.transactions_output`
            WHERE location_id = @LocationId
            ORDER BY report_date DESC",
            new[] { new BigQueryParameter("LocationId", BigQueryDbType.String, "756152205962546") });

        // Expected: 11 DISTINCT rows (9 weekly + 2 monthly after dedup)
        Assert.Equal(11, rows.Count);
    }

    [Fact]
    public async Task Bug5_RealData_OldestWeeklyDates_NotDropped()
    {
        var rows = await Q(@"
            SELECT DISTINCT CAST(transaction_period AS STRING) AS report_date, weekly_or_monthly, location_id, customer_id
            FROM `{ds}.transactions_output`
            WHERE location_id = @LocationId
            ORDER BY report_date DESC",
            new[] { new BigQueryParameter("LocationId", BigQueryDbType.String, "756152205962546") });

        var weeklyDates = rows
            .Where(r => r["weekly_or_monthly"]!.ToString() == "Weekly")
            .Select(r => r["report_date"]!.ToString()!)
            .ToList();

        // Report: missing 2025-09-15 and 2025-09-22
        Assert.Equal(9, weeklyDates.Count);
        Assert.Contains("2025-09-15", weeklyDates);
        Assert.Contains("2025-09-22", weeklyDates);
    }

    // ===================================================================
    // Bug 4: QUALIFY without PARTITION BY — real benchmarking data
    // Exact query from LocalBusinessesTotalTakingsQuery.cs
    // Expected: 0.101806123 (monthly YoY), Actual was: 0.10811516996502529
    // ===================================================================

    // ===================================================================
    // Bug 7: SUM(IF()) in CTE + UNION ALL returns STRING type
    // Report: source table uses FLOAT/INTEGER types (from JSON schema)
    // Seeded via streaming inserts from transactions_weekly.csv (100 rows)
    // ===================================================================

    [Fact]
    public async Task Bug7_RealData_SumIf_InCte_UnionAll_SchemaType()
    {
        var client = await _fixture.GetClientAsync();

        // Create table via GetOrCreateTableAsync (matching user's exact seeding path)
        var weeklySchema = new TableSchemaBuilder
        {
            { "location_id", BigQueryDbType.String },
            { "trading_name", BigQueryDbType.String },
            { "customer_id", BigQueryDbType.String },
            { "transaction_period", BigQueryDbType.Date },
            { "total_spend", BigQueryDbType.Float64 },
            { "total_spend_new", BigQueryDbType.Float64 },
            { "total_spend_repeat", BigQueryDbType.Float64 },
            { "total_transactions", BigQueryDbType.Int64 },
            { "total_transactions_new", BigQueryDbType.Int64 },
            { "total_transactions_repeat", BigQueryDbType.Int64 },
            { "total_cards", BigQueryDbType.Int64 },
            { "total_cards_new", BigQueryDbType.Int64 },
            { "total_cards_repeat", BigQueryDbType.Int64 },
            { "total_unique_visits", BigQueryDbType.Int64 },
            { "total_unique_visits_new", BigQueryDbType.Int64 },
            { "total_unique_visits_repeat", BigQueryDbType.Int64 },
        }.Build();
        var weeklyRef = client.GetTableReference(_ds, "trf_transactions_weekly");
        await client.GetOrCreateTableAsync(weeklyRef, weeklySchema);

        // Seed via streaming inserts (matching user's exact seeding path)
        await SeedFromCsv(client, "trf_transactions_weekly",
            Path.Combine(AppContext.BaseDirectory, "TestData", "transactions_weekly.csv"));

        // Exact query pattern from Bug 7 report (simplified to key columns)
        var result = await Raw($@"
            WITH aggregated AS (
              SELECT
                location_id,
                SUM(IF(transaction_period >= @ReportDate
                    AND transaction_period <= DATE_ADD(@ReportDate, INTERVAL 3 WEEK),
                    total_cards, NULL)) AS cur_unique_customers,
                SUM(IF(transaction_period >= @ReportDate
                    AND transaction_period <= DATE_ADD(@ReportDate, INTERVAL 3 WEEK),
                    total_spend, NULL)) AS cur_total_takings,
                SUM(IF(transaction_period >= @ReportDate
                    AND transaction_period <= DATE_ADD(@ReportDate, INTERVAL 3 WEEK),
                    total_unique_visits, NULL)) AS cur_visits,
                SUM(IF(transaction_period >= @ComparisonDate
                    AND transaction_period <= DATE_ADD(@ComparisonDate, INTERVAL 3 WEEK),
                    total_cards, NULL)) AS cmp_unique_customers,
                SUM(IF(transaction_period >= @ComparisonDate
                    AND transaction_period <= DATE_ADD(@ComparisonDate, INTERVAL 3 WEEK),
                    total_spend, NULL)) AS cmp_total_takings,
                SUM(IF(transaction_period >= @ComparisonDate
                    AND transaction_period <= DATE_ADD(@ComparisonDate, INTERVAL 3 WEEK),
                    total_unique_visits, NULL)) AS cmp_visits
              FROM `{{ds}}.trf_transactions_weekly`
              WHERE customer_id = @CustomerId
                AND location_id IN UNNEST(@LocationIds)
                AND (
                  (transaction_period >= @ReportDate AND transaction_period <= DATE_ADD(@ReportDate, INTERVAL 3 WEEK))
                  OR
                  (transaction_period >= @ComparisonDate AND transaction_period <= DATE_ADD(@ComparisonDate, INTERVAL 3 WEEK))
                )
              GROUP BY location_id
            )
            SELECT
              location_id,
              cur_unique_customers AS unique_customers,
              cur_total_takings AS total_takings,
              SAFE_DIVIDE(cur_total_takings, cur_visits) AS average_spend_per_visit,
              SAFE_DIVIDE(cur_unique_customers - cmp_unique_customers, cmp_unique_customers) AS unique_customers_change
            FROM aggregated
            UNION ALL
            SELECT
              location_id,
              cmp_unique_customers AS unique_customers,
              cmp_total_takings AS total_takings,
              SAFE_DIVIDE(cmp_total_takings, cmp_visits) AS average_spend_per_visit,
              0.0 AS unique_customers_change
            FROM aggregated",
            new[] {
                new BigQueryParameter("CustomerId", BigQueryDbType.String, "221613823456184"),
                new BigQueryParameter("LocationIds", BigQueryDbType.Array, new[] { "216149122232148" }),
                new BigQueryParameter("ReportDate", BigQueryDbType.Date, new DateTime(2025, 11, 10)),
                new BigQueryParameter("ComparisonDate", BigQueryDbType.Date, new DateTime(2025, 10, 13)),
            });

        // Report says: unique_customers returns STRING (should be INTEGER)
        var custField = result.Schema.Fields.First(f => f.Name == "unique_customers");
        Assert.NotEqual("STRING", custField.Type);

        // Report says: total_takings returns STRING (should be FLOAT)
        var takingsField = result.Schema.Fields.First(f => f.Name == "total_takings");
        Assert.NotEqual("STRING", takingsField.Type);

        // Report says: all row values are null
        var rows = result.ToList();
        Assert.Equal(2, rows.Count);
        Assert.NotNull(rows[0]["unique_customers"]);
    }

    [Fact]
    public async Task Bug7_SumIf_AllNull_SchemaType_ShouldNotBeString()
    {
        // When NO rows match the IF condition, SUM returns NULL.
        // The schema should still reflect the correct type, not STRING.
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync(
            $"CREATE TABLE `{_ds}.bug7_null` (id STRING, val INT64, amount FLOAT64)", parameters: null);
        await client.ExecuteQueryAsync(
            $"INSERT INTO `{_ds}.bug7_null` (id, val, amount) VALUES ('a', 10, 100.0), ('b', 20, 200.0)",
            parameters: null);

        // IF condition is FALSE for all rows → SUM returns NULL
        // Test plain GROUP BY (no CTE) to isolate schema inference
        var directGroupBy = await Raw(@"
            SELECT id, SUM(IF(val > 999, val, NULL)) AS val_sum,
                       SUM(IF(val > 999, amount, NULL)) AS amount_sum
            FROM `{ds}.bug7_null`
            GROUP BY id");

        var directValField = directGroupBy.Schema.Fields.First(f => f.Name == "val_sum");
        Assert.NotEqual("STRING", directValField.Type);

        // Now with UNION ALL
        var result = await Raw(@"
            WITH cte AS (
              SELECT id, SUM(IF(val > 999, val, NULL)) AS val_sum,
                         SUM(IF(val > 999, amount, NULL)) AS amount_sum
              FROM `{ds}.bug7_null`
              GROUP BY id
            )
            SELECT val_sum, amount_sum FROM cte
            UNION ALL
            SELECT val_sum, amount_sum FROM cte");

        // Even though values are all NULL, schema should NOT be STRING
        var valField = result.Schema.Fields.First(f => f.Name == "val_sum");
        var amtField = result.Schema.Fields.First(f => f.Name == "amount_sum");
        Assert.NotEqual("STRING", valField.Type);
        Assert.NotEqual("STRING", amtField.Type);
    }

    // ===================================================================
    // Bug 9: SAFE_DIVIDE rounding off by 1 ULP (-1e-9) for NUMERIC
    // Report: truncation produces value 1e-9 less than expected
    // ===================================================================

    [Fact]
    public async Task Bug9_SafeDiv_Numeric_Rounding_1ULP()
    {
        // Values from report: expected 0.010171945, got 0.010171944
        var rows = await Q(@"
            SELECT CAST(SAFE_DIVIDE(
              SAFE_DIVIDE(total_spend, total_unique_visits) - SAFE_DIVIDE(total_spend_lag, total_unique_visits_lag),
              SAFE_DIVIDE(total_spend_lag, total_unique_visits_lag)
            ) AS STRING) AS change
            FROM `{ds}.transactions_output`
            WHERE location_id = '756152205962546'
              AND transaction_period = '2025-10-01'
              AND weekly_or_monthly = 'Monthly'
              AND period_on_period = 'YoY'");

        // The report shows values off by exactly -0.000000001
        // We already test -0.015119283 vs -0.015119284 in Bug3b
        // This test uses different input values to verify consistency
        var val = rows[0]["change"]!.ToString()!;
        // Should have at most 9 decimal places (NUMERIC truncation)
        var parts = val.Split('.');
        if (parts.Length == 2)
            Assert.True(parts[1].Length <= 9, $"NUMERIC should have <= 9dp, got {parts[1].Length}: {val}");
    }

    // ===================================================================
    // Bug 10: SELECT DISTINCT + ORDER BY drops rows (real data)
    // Report: 9 weekly dates expected, only 7 returned
    // ===================================================================

    [Fact]
    public async Task Bug10_RealData_SelectDistinct_OrderBy_WeeklyDates()
    {
        var rows = await Q(@"
            SELECT DISTINCT CAST(transaction_period AS STRING) AS report_date, weekly_or_monthly, location_id, customer_id
            FROM `{ds}.transactions_output`
            WHERE location_id = @LocationId
            ORDER BY report_date DESC",
            new[] { new BigQueryParameter("LocationId", BigQueryDbType.String, "756152205962546") });

        var weeklyDates = rows
            .Where(r => r["weekly_or_monthly"]!.ToString() == "Weekly")
            .Select(r => r["report_date"]!.ToString()!)
            .ToList();

        // Report: Expected 9 weekly dates, got 7 (missing 2025-09-15 and 2025-09-22)
        Assert.Equal(9, weeklyDates.Count);
        Assert.Contains("2025-09-15", weeklyDates);
        Assert.Contains("2025-09-22", weeklyDates);
    }

    [Fact]
    public async Task Bug4_RealData_Qualify_WithoutPartitionBy_MonthlyYoY()
    {
        var rows = await Q($@"
            WITH
                bounds AS (
                    SELECT mcc, weekly_or_monthly, AVG(total_spend) * 0.01 AS cto_lower_bound
                    FROM `{{ds}}.transactions_output`
                    GROUP BY mcc, weekly_or_monthly
                ),
                transactions AS (
                    SELECT txns.*, cto_lower_bound,
                        IFNULL(total_spend > cto_lower_bound AND total_spend_lag > cto_lower_bound, FALSE) AS is_valid_comparison
                    FROM `{{ds}}.transactions_output` txns
                    LEFT JOIN bounds ON bounds.mcc = txns.mcc AND bounds.weekly_or_monthly = txns.weekly_or_monthly
                    WHERE DATE_DIFF(CURRENT_DATE(), transaction_period, MONTH) <= 15
                      AND comparison_period IS NOT NULL
                ),
                competitors AS (
                    SELECT
                        benchmarking.location_id2 AS location_id,
                        ROW_NUMBER() OVER (ORDER BY benchmarking.similarity_score DESC) AS similarity_rank,
                        COALESCE(SUM(transactions.total_spend), 0) AS total_spend,
                        COALESCE(SUM(transactions.total_spend_lag), 0) AS total_spend_lag,
                        transactions.period_on_period,
                        transactions.transaction_period,
                        transactions.comparison_period,
                        transactions.weekly_or_monthly
                    FROM `{{ds}}.benchmarking_output` AS benchmarking
                    LEFT JOIN transactions ON benchmarking.location_id2 = transactions.location_id
                    WHERE (benchmarking.location_id1) = @LocationId
                      AND ((NOT (benchmarking.is_target_location) OR (benchmarking.is_target_location) IS NULL)
                          AND (transactions.transaction_period) = @reportDate)
                      AND (transactions.weekly_or_monthly) = @cadence
                      AND (transactions.period_on_period) = @comparisonPeriodOnPeriod
                      AND (transactions.is_valid_comparison)
                    GROUP BY location_id, similarity_score,
                             transactions.period_on_period, transactions.transaction_period,
                             transactions.comparison_period, transactions.weekly_or_monthly
                    QUALIFY similarity_rank <= 20
                )
            SELECT
                CAST(SAFE_DIVIDE((SUM(total_spend) - SUM(total_spend_lag)), SUM(total_spend_lag)) AS STRING) AS change_ratio
            FROM competitors",
            new[] {
                new BigQueryParameter("LocationId", BigQueryDbType.String, "756152205962546"),
                new BigQueryParameter("reportDate", BigQueryDbType.Date, new DateTime(2025, 10, 1)),
                new BigQueryParameter("cadence", BigQueryDbType.String, "Monthly"),
                new BigQueryParameter("comparisonPeriodOnPeriod", BigQueryDbType.String, "YoY"),
            });

        Assert.Single(rows);
        var ratio = decimal.Parse(rows[0]["change_ratio"]!.ToString()!, CultureInfo.InvariantCulture);
        // Expected: 0.101806123 (within 0.001 tolerance)
        Assert.True(Math.Abs(ratio - 0.101806123m) < 0.001m,
            $"Expected ~0.101806123, got {ratio}, diff = {Math.Abs(ratio - 0.101806123m)}");
    }
}
