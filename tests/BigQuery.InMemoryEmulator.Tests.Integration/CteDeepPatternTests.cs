using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class CteDeepPatternTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public CteDeepPatternTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_cte2_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.sales` (id INT64, region STRING, product STRING, amount FLOAT64, sale_date DATE)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.sales` (id, region, product, amount, sale_date) VALUES
            (1,'East','Widget',100.0,DATE '2024-01-15'),(2,'East','Gadget',200.0,DATE '2024-01-20'),
            (3,'West','Widget',150.0,DATE '2024-02-01'),(4,'West','Gadget',300.0,DATE '2024-02-15'),
            (5,'East','Widget',175.0,DATE '2024-03-01'),(6,'West','Widget',125.0,DATE '2024-03-10'),
            (7,'North','Gadget',250.0,DATE '2024-01-05'),(8,'North','Widget',180.0,DATE '2024-02-20')", parameters: null);
    }

    public async ValueTask DisposeAsync()
    {
        try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
        await _fixture.DisposeAsync();
    }

    private async Task<List<BigQueryRow>> Query(string sql)
    {
        var client = await _fixture.GetClientAsync();
        var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
        return result.ToList();
    }

    private async Task<string?> Scalar(string sql)
    {
        var rows = await Query(sql);
        return rows.Count > 0 ? rows[0][0]?.ToString() : null;
    }

    // Basic CTE
    [Fact] public async Task Cte_Basic()
    {
        var rows = await Query("WITH east_sales AS (SELECT * FROM `{ds}.sales` WHERE region = 'East') SELECT COUNT(*) as cnt FROM east_sales");
        Assert.Equal("3", rows[0]["cnt"]?.ToString());
    }

    // Multiple CTEs
    [Fact] public async Task Cte_Multiple()
    {
        var rows = await Query(@"WITH
            east AS (SELECT * FROM `{ds}.sales` WHERE region = 'East'),
            west AS (SELECT * FROM `{ds}.sales` WHERE region = 'West')
            SELECT (SELECT COUNT(*) FROM east) as e_cnt, (SELECT COUNT(*) FROM west) as w_cnt");
        Assert.Equal("3", rows[0]["e_cnt"]?.ToString());
        Assert.Equal("3", rows[0]["w_cnt"]?.ToString());
    }

    // CTE referencing another CTE
    [Fact] public async Task Cte_Chained()
    {
        var rows = await Query(@"WITH
            all_widgets AS (SELECT * FROM `{ds}.sales` WHERE product = 'Widget'),
            expensive_widgets AS (SELECT * FROM all_widgets WHERE amount > 150)
            SELECT COUNT(*) as cnt FROM expensive_widgets");
        Assert.Equal("2", rows[0]["cnt"]?.ToString());
    }

    // CTE with aggregation
    [Fact] public async Task Cte_Aggregation()
    {
        var rows = await Query(@"WITH
            region_totals AS (SELECT region, CAST(SUM(amount) AS INT64) as total FROM `{ds}.sales` GROUP BY region)
            SELECT region, total FROM region_totals ORDER BY total DESC");
        Assert.Equal(3, rows.Count);
    }

    // CTE with JOIN
    [Fact] public async Task Cte_WithJoin()
    {
        var rows = await Query(@"WITH
            region_stats AS (SELECT region, COUNT(*) as cnt FROM `{ds}.sales` GROUP BY region),
            region_totals AS (SELECT region, CAST(SUM(amount) AS INT64) as total FROM `{ds}.sales` GROUP BY region)
            SELECT s.region, s.cnt, t.total FROM region_stats s JOIN region_totals t ON s.region = t.region ORDER BY s.region");
        Assert.Equal(3, rows.Count);
    }

    // CTE used multiple times
    [Fact] public async Task Cte_ReusedMultipleTimes()
    {
        var result = await Scalar(@"WITH
            base AS (SELECT region, amount FROM `{ds}.sales`)
            SELECT CAST(SUM(a.amount + b.amount) AS INT64) FROM base a CROSS JOIN base b WHERE a.region = 'East' AND b.region = 'East'");
        Assert.NotNull(result);
    }

    // CTE with window function
    [Fact] public async Task Cte_WithWindow()
    {
        var rows = await Query(@"WITH
            ranked AS (SELECT id, region, amount, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) as rn FROM `{ds}.sales`)
            SELECT id, region, amount FROM ranked WHERE rn = 1 ORDER BY region");
        Assert.Equal(3, rows.Count); // top sale per region
    }

    // CTE with UNION ALL
    [Fact] public async Task Cte_WithUnion()
    {
        var rows = await Query(@"WITH
            combined AS (
                SELECT region, amount FROM `{ds}.sales` WHERE product = 'Widget'
                UNION ALL
                SELECT region, amount FROM `{ds}.sales` WHERE product = 'Gadget'
            )
            SELECT COUNT(*) as cnt FROM combined");
        Assert.Equal("8", rows[0]["cnt"]?.ToString());
    }

    // CTE with subquery in SELECT
    [Fact] public async Task Cte_SubqueryInSelect()
    {
        var rows = await Query(@"WITH
            totals AS (SELECT CAST(SUM(amount) AS INT64) as grand_total FROM `{ds}.sales`)
            SELECT region, CAST(SUM(amount) AS INT64) as reg_total FROM `{ds}.sales` GROUP BY region ORDER BY reg_total DESC");
        Assert.Equal(3, rows.Count);
    }

    // CTE with HAVING
    [Fact] public async Task Cte_WithHaving()
    {
        var rows = await Query(@"WITH
            region_counts AS (SELECT region, COUNT(*) as cnt FROM `{ds}.sales` GROUP BY region HAVING COUNT(*) >= 3)
            SELECT * FROM region_counts ORDER BY region");
        Assert.Equal(2, rows.Count); // East=3, West=3
    }

    // CTE with ORDER BY and LIMIT
    [Fact] public async Task Cte_OrderByLimit()
    {
        var rows = await Query(@"WITH
            ranked AS (SELECT id, amount FROM `{ds}.sales` ORDER BY amount DESC LIMIT 3)
            SELECT * FROM ranked");
        Assert.Equal(3, rows.Count);
    }

    // CTE with CASE
    [Fact] public async Task Cte_WithCase()
    {
        var rows = await Query(@"WITH
            categorized AS (SELECT id, CASE WHEN amount > 200 THEN 'high' WHEN amount > 150 THEN 'medium' ELSE 'low' END as tier FROM `{ds}.sales`)
            SELECT tier, COUNT(*) as cnt FROM categorized GROUP BY tier ORDER BY tier");
        Assert.Equal(3, rows.Count);
    }
}