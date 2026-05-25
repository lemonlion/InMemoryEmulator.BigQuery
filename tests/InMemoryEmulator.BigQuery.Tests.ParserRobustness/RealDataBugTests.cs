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
