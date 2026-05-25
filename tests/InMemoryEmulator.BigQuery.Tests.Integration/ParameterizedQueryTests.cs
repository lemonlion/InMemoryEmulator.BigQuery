using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

[Collection(IntegrationCollection.Name)]
public class ParameterizedQueryTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public ParameterizedQueryTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_prm_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.items` (id INT64, name STRING, price FLOAT64, active BOOL)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.items` (id, name, price, active) VALUES
            (1,'Widget',10.0,TRUE),(2,'Gadget',20.0,FALSE),(3,'Doohickey',30.0,TRUE),(4,'Thingamajig',40.0,TRUE),(5,'Gizmo',50.0,FALSE)", parameters: null);
    }

    public async ValueTask DisposeAsync()
    {
        try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
        await _fixture.DisposeAsync();
    }

    private async Task<List<BigQueryRow>> Query(string sql, IEnumerable<BigQueryParameter> parameters)
    {
        var client = await _fixture.GetClientAsync();
        var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters);
        return result.ToList();
    }

    private async Task<string?> Scalar(string sql, IEnumerable<BigQueryParameter> parameters)
    {
        var rows = await Query(sql, parameters);
        return rows.Count > 0 ? rows[0][0]?.ToString() : null;
    }

    // String parameter
    [Fact] public async Task Param_String()
    {
        var result = await Scalar("SELECT name FROM `{ds}.items` WHERE name = @name",
            new[] { new BigQueryParameter("name", BigQueryDbType.String, "Widget") });
        Assert.Equal("Widget", result);
    }

    // Int parameter
    [Fact] public async Task Param_Int()
    {
        var result = await Scalar("SELECT name FROM `{ds}.items` WHERE id = @id",
            new[] { new BigQueryParameter("id", BigQueryDbType.Int64, 3) });
        Assert.Equal("Doohickey", result);
    }

    // Float parameter
    [Fact] public async Task Param_Float()
    {
        var result = await Scalar("SELECT name FROM `{ds}.items` WHERE price > @minPrice ORDER BY price LIMIT 1",
            new[] { new BigQueryParameter("minPrice", BigQueryDbType.Float64, 25.0) });
        Assert.Equal("Doohickey", result);
    }

    // Bool parameter
    [Fact] public async Task Param_Bool()
    {
        var rows = await Query("SELECT name FROM `{ds}.items` WHERE active = @active ORDER BY name",
            new[] { new BigQueryParameter("active", BigQueryDbType.Bool, true) });
        Assert.Equal(3, rows.Count);
    }

    // Multiple parameters
    [Fact] public async Task Param_Multiple()
    {
        var rows = await Query("SELECT name FROM `{ds}.items` WHERE price >= @min AND price <= @max ORDER BY price",
            new[] {
                new BigQueryParameter("min", BigQueryDbType.Float64, 20.0),
                new BigQueryParameter("max", BigQueryDbType.Float64, 40.0)
            });
        Assert.Equal(3, rows.Count);
    }

    // Parameter in expression
    [Fact] public async Task Param_InExpression()
    {
        var result = await Scalar("SELECT CAST(price * @multiplier AS INT64) FROM `{ds}.items` WHERE id = 1",
            new[] { new BigQueryParameter("multiplier", BigQueryDbType.Float64, 2.0) });
        Assert.Equal("20", result);
    }

    // NULL parameter
    [Fact] public async Task Param_Null()
    {
        var rows = await Query("SELECT name FROM `{ds}.items` WHERE @val IS NULL",
            new[] { new BigQueryParameter("val", BigQueryDbType.String, null) });
        Assert.Equal(5, rows.Count);
    }

    // Parameter in SELECT
    [Fact] public async Task Param_InSelect()
    {
        var result = await Scalar("SELECT @greeting",
            new[] { new BigQueryParameter("greeting", BigQueryDbType.String, "Hello World") });
        Assert.Equal("Hello World", result);
    }

    // Parameter comparison operators
    [Fact] public async Task Param_LessThan()
    {
        var rows = await Query("SELECT name FROM `{ds}.items` WHERE price < @max ORDER BY price",
            new[] { new BigQueryParameter("max", BigQueryDbType.Float64, 30.0) });
        Assert.Equal(2, rows.Count);
    }

    // Parameter with LIKE
    [Fact] public async Task Param_Like()
    {
        var rows = await Query("SELECT name FROM `{ds}.items` WHERE name LIKE @pattern",
            new[] { new BigQueryParameter("pattern", BigQueryDbType.String, "G%") });
        Assert.Equal(2, rows.Count); // Gadget, Gizmo
    }

    // Parameter with IN (not directly supported as array in all implementations, use scalar)
    [Fact] public async Task Param_InWhere()
    {
        var result = await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE id = @id1 OR id = @id2",
            new[] {
                new BigQueryParameter("id1", BigQueryDbType.Int64, 1),
                new BigQueryParameter("id2", BigQueryDbType.Int64, 3)
            });
        Assert.Equal("2", result);
    }

    // Parameter with aggregation
    [Fact] public async Task Param_WithAgg()
    {
        var result = await Scalar("SELECT COUNT(*) FROM `{ds}.items` WHERE price > @threshold",
            new[] { new BigQueryParameter("threshold", BigQueryDbType.Float64, 35.0) });
        Assert.Equal("2", result);
    }

    // Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
    //   "parameterType.type: DATE" — value is a date string "YYYY-MM-DD"
    [Fact] public async Task Param_Date_Comparison()
    {
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync(
            $"CREATE TABLE `{_datasetId}.events` (event_date DATE, description STRING)", parameters: null);
        await client.ExecuteQueryAsync(
            $"INSERT INTO `{_datasetId}.events` (event_date, description) VALUES ('2025-01-15', 'A'), ('2025-03-20', 'B'), ('2025-06-01', 'C')",
            parameters: null);
        var rows = await Query(
            "SELECT description FROM `{ds}.events` WHERE event_date >= @start_date ORDER BY event_date",
            new[] { new BigQueryParameter("start_date", BigQueryDbType.Date, new DateTime(2025, 3, 1)) });
        Assert.Equal(2, rows.Count);
        Assert.Equal("B", rows[0]["description"]?.ToString());
        Assert.Equal("C", rows[1]["description"]?.ToString());
    }

    // Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
    //   "parameterType.type: TIMESTAMP" — value is an ISO 8601 timestamp string
    [Fact] public async Task Param_Timestamp_Comparison()
    {
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync(
            $"CREATE TABLE `{_datasetId}.logs` (ts TIMESTAMP, msg STRING)", parameters: null);
        await client.ExecuteQueryAsync(
            $"INSERT INTO `{_datasetId}.logs` (ts, msg) VALUES (TIMESTAMP '2025-01-15 10:00:00 UTC', 'X'), (TIMESTAMP '2025-06-01 12:00:00 UTC', 'Y')",
            parameters: null);
        var result = await Scalar(
            "SELECT msg FROM `{ds}.logs` WHERE ts > @cutoff ORDER BY ts LIMIT 1",
            new[] { new BigQueryParameter("cutoff", BigQueryDbType.Timestamp, new DateTime(2025, 3, 1, 0, 0, 0, DateTimeKind.Utc)) });
        Assert.Equal("Y", result);
    }

    // Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
    //   "parameterType.type: DATETIME" — value is a datetime string
    [Fact] public async Task Param_Datetime_Comparison()
    {
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync(
            $"CREATE TABLE `{_datasetId}.meetings` (dt DATETIME, title STRING)", parameters: null);
        await client.ExecuteQueryAsync(
            $"INSERT INTO `{_datasetId}.meetings` (dt, title) VALUES (DATETIME '2025-01-15 09:00:00', 'M1'), (DATETIME '2025-06-01 14:00:00', 'M2')",
            parameters: null);
        var result = await Scalar(
            "SELECT title FROM `{ds}.meetings` WHERE dt > @cutoff ORDER BY dt LIMIT 1",
            new[] { new BigQueryParameter("cutoff", BigQueryDbType.DateTime, new DateTime(2025, 3, 1)) });
        Assert.Equal("M2", result);
    }

    // Ref: https://github.com/lemonlion/InMemoryEmulator.BigQuery/issues/2
    //   DATE parameter with DATE_ADD — the exact scenario from the bug report
    [Fact] public async Task Param_Date_WithDateAdd()
    {
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync(
            $"CREATE TABLE `{_datasetId}.txns` (transaction_period DATE, location_id STRING)", parameters: null);
        await client.ExecuteQueryAsync(
            $"INSERT INTO `{_datasetId}.txns` (transaction_period, location_id) VALUES ('2025-11-10', 'LOC1'), ('2025-11-20', 'LOC2'), ('2025-12-15', 'LOC3')",
            parameters: null);
        var rows = await Query(
            "SELECT location_id FROM `{ds}.txns` WHERE transaction_period >= @ReportDate AND transaction_period <= DATE_ADD(@ReportDate, INTERVAL 3 WEEK) ORDER BY location_id",
            new[] { new BigQueryParameter("ReportDate", BigQueryDbType.Date, new DateTime(2025, 11, 10)) });
        Assert.Equal(2, rows.Count);
        Assert.Equal("LOC1", rows[0]["location_id"]?.ToString());
        Assert.Equal("LOC2", rows[1]["location_id"]?.ToString());
    }

    // Ref: https://github.com/lemonlion/InMemoryEmulator.BigQuery/issues/3 — Reproduction case 1
    //   EXACT SQL from the bug report
    [Fact] public async Task Issue3_Cte_ExactReproduction()
    {
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync(
            $"CREATE TABLE `{_datasetId}.transactions_output` (mcc STRING, weekly_or_monthly STRING, transaction_period DATE, location_id STRING)",
            parameters: null);
        await client.ExecuteQueryAsync(
            $"INSERT INTO `{_datasetId}.transactions_output` (mcc, weekly_or_monthly, transaction_period, location_id) VALUES ('5411', 'weekly', '2025-11-10', 'LOC1'), ('5411', 'monthly', '2025-11-10', 'LOC1')",
            parameters: null);
        var sql = $@"WITH bounds AS (
  SELECT
    mcc,
    weekly_or_monthly,
    transaction_period AS report_date
  FROM `{_datasetId}.transactions_output`
  WHERE location_id = @LocationId
    AND transaction_period = @reportDate
)
SELECT * FROM bounds";
        var rows = await Query(sql.Replace($"`{_datasetId}.", $"`{{ds}}."),
            new[] {
                new BigQueryParameter("LocationId", BigQueryDbType.String, "LOC1"),
                new BigQueryParameter("reportDate", BigQueryDbType.Date, new DateTime(2025, 11, 10))
            });
        Assert.Equal(2, rows.Count);
    }

    // Ref: https://github.com/lemonlion/InMemoryEmulator.BigQuery/issues/3 — Reproduction case 2
    //   EXACT SQL from the bug report
    [Fact] public async Task Issue3_DateLiteral_ExactReproduction()
    {
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync(
            $"CREATE TABLE `{_datasetId}.tbl` (id INT64)", parameters: null);
        await client.ExecuteQueryAsync(
            $"INSERT INTO `{_datasetId}.tbl` (id) VALUES (1)", parameters: null);
        var result = await Scalar(
            $"SELECT CAST(CAST(@comparison_date AS DATE) AS STRING) AS report_date FROM `{{ds}}.tbl` WHERE id = 1",
            new[] { new BigQueryParameter("comparison_date", BigQueryDbType.Date, new DateTime(2025, 11, 10)) });
        Assert.Equal("2025-11-10", result);
    }

    // Ref: https://github.com/lemonlion/InMemoryEmulator.BigQuery/issues/3 — Reproduction case 2b
    //   DATE literal sentinel value
    [Fact] public async Task Issue3_DateLiteralSentinel()
    {
        var result = await Scalar("SELECT CAST(DATE '1900-01-01' AS STRING)", Array.Empty<BigQueryParameter>());
        Assert.Equal("1900-01-01", result);
    }

    // Ref: https://github.com/lemonlion/InMemoryEmulator.BigQuery/issues/3 — Reproduction case 3
    //   EXACT SQL from the bug report (trailing semicolon)
    [Fact] public async Task Issue3_TrailingSemicolon_ExactReproduction()
    {
        var result = await Scalar(
            "SELECT name FROM `{ds}.items` WHERE id = @id\n;",
            new[] { new BigQueryParameter("id", BigQueryDbType.Int64, 1) });
        Assert.Equal("Widget", result);
    }

    // Ref: https://github.com/lemonlion/InMemoryEmulator.BigQuery/issues/3
    //   CTE with parameters — simplified version
    [Fact] public async Task Param_Cte_WithParameters()
    {
        var rows = await Query(
            "WITH filtered AS (SELECT name, price FROM `{ds}.items` WHERE price > @minPrice) SELECT name FROM filtered ORDER BY name",
            new[] { new BigQueryParameter("minPrice", BigQueryDbType.Float64, 25.0) });
        Assert.Equal(3, rows.Count);
    }

    // Ref: https://github.com/lemonlion/InMemoryEmulator.BigQuery/issues/3
    //   Trailing semicolons should not cause parse errors
    [Fact] public async Task Query_TrailingSemicolon()
    {
        var result = await Scalar("SELECT name FROM `{ds}.items` WHERE id = @id ;",
            new[] { new BigQueryParameter("id", BigQueryDbType.Int64, 1) });
        Assert.Equal("Widget", result);
    }

    // Ref: https://github.com/lemonlion/InMemoryEmulator.BigQuery/issues/3
    //   SQL line comments should be stripped before parsing
    [Fact] public async Task Query_LineComments()
    {
        var result = await Scalar(
            "SELECT name -- get the name\nFROM `{ds}.items` -- from items\nWHERE id = 1",
            Array.Empty<BigQueryParameter>());
        Assert.Equal("Widget", result);
    }

    // Ref: https://github.com/lemonlion/InMemoryEmulator.BigQuery/issues/3
    //   SQL block comments should be stripped before parsing
    [Fact] public async Task Query_BlockComments()
    {
        var result = await Scalar(
            "SELECT /* the name */ name FROM `{ds}.items` WHERE id = 1",
            Array.Empty<BigQueryParameter>());
        Assert.Equal("Widget", result);
    }

    // Ref: https://github.com/lemonlion/InMemoryEmulator.BigQuery/issues/3
    //   CTE with trailing semicolon and parameters
    [Fact] public async Task Param_Cte_TrailingSemicolon()
    {
        var rows = await Query(
            "WITH top_items AS (SELECT name FROM `{ds}.items` WHERE price > @min) SELECT name FROM top_items ORDER BY name;",
            new[] { new BigQueryParameter("min", BigQueryDbType.Float64, 35.0) });
        Assert.Equal(2, rows.Count);
    }

    // Ref: https://github.com/lemonlion/InMemoryEmulator.BigQuery/issues/3 — Issue 2
    //   CTE with leading-comma separator style: `) \n , name AS (`
    [Fact] public async Task Issue3_Cte_LeadingComma()
    {
        var sql = @"WITH a AS (SELECT name FROM `{ds}.items` WHERE id = 1)
, b AS (SELECT name FROM `{ds}.items` WHERE id = 2)
SELECT * FROM a UNION ALL SELECT * FROM b";
        var rows = await Query(sql, Array.Empty<BigQueryParameter>());
        Assert.Equal(2, rows.Count);
    }

    // Ref: https://github.com/lemonlion/InMemoryEmulator.BigQuery/issues/3 — Issue 4
    //   Trailing comma before FROM — BigQuery allows SELECT a, b, FROM table
    [Fact] public async Task Issue3_TrailingCommaBeforeFrom()
    {
        var result = await Scalar(
            "SELECT id, name, FROM `{ds}.items` WHERE id = 1",
            Array.Empty<BigQueryParameter>());
        Assert.Equal("1", result);
    }

    // Ref: https://github.com/lemonlion/InMemoryEmulator.BigQuery/issues/3
    //   DATE literal with comment on same line
    [Fact] public async Task Query_DateLiteralWithComment()
    {
        var result = await Scalar(
            "SELECT CAST(DATE '1900-01-01' AS STRING) -- sentinel date",
            Array.Empty<BigQueryParameter>());
        Assert.Equal("1900-01-01", result);
    }
}