using Google;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

[Collection(IntegrationCollection.Name)]
public class UnionIntersectExceptTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public UnionIntersectExceptTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_uie_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.a` (id INT64, val STRING)", parameters: null);
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.b` (id INT64, val STRING)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.a` (id, val) VALUES (1,'x'),(2,'y'),(3,'z'),(4,'x')", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.b` (id, val) VALUES (2,'y'),(3,'z'),(5,'w'),(6,'z')", parameters: null);
    }

    public async ValueTask DisposeAsync()
    {
        try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
        await _fixture.DisposeAsync();
    }

    private async Task<string?> Scalar(string sql)
    {
        var client = await _fixture.GetClientAsync();
        var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
        var rows = result.ToList();
        return rows.Count > 0 ? rows[0][0]?.ToString() : null;
    }

    private async Task<List<BigQueryRow>> Query(string sql)
    {
        var client = await _fixture.GetClientAsync();
        var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
        return result.ToList();
    }

    // UNION ALL
    [Fact] public async Task UnionAll_Basic() => Assert.Equal("8", await Scalar("SELECT COUNT(*) FROM (SELECT id, val FROM `{ds}.a` UNION ALL SELECT id, val FROM `{ds}.b`)"));
    [Fact] public async Task UnionAll_OrderBy()
    {
        var rows = await Query("SELECT id, val FROM `{ds}.a` UNION ALL SELECT id, val FROM `{ds}.b` ORDER BY id");
        Assert.Equal(8, rows.Count);
    }

    // UNION DISTINCT
    [Fact] public async Task UnionDistinct_Basic()
    {
        var count = int.Parse((await Scalar("SELECT COUNT(*) FROM (SELECT id, val FROM `{ds}.a` UNION DISTINCT SELECT id, val FROM `{ds}.b`)"))!);
        Assert.True(count < 8); // Should remove duplicates
    }

    // INTERSECT ALL - not supported in BigQuery
    // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators
    //   "INTERSECT ALL is not supported. Only INTERSECT DISTINCT is supported."
    [Fact]
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    public async Task IntersectAll_Basic()
    {
        var client = await _fixture.GetClientAsync();
        await Assert.ThrowsAsync<GoogleApiException>(async () =>
            await client.ExecuteQueryAsync($"SELECT id, val FROM `{_datasetId}.a` INTERSECT ALL SELECT id, val FROM `{_datasetId}.b`", parameters: null));
    }

    // INTERSECT DISTINCT
    [Fact] public async Task IntersectDistinct_Basic()
    {
        var count = int.Parse((await Scalar("SELECT COUNT(*) FROM (SELECT id, val FROM `{ds}.a` INTERSECT DISTINCT SELECT id, val FROM `{ds}.b`)"))!);
        Assert.True(count >= 1);
    }

    // EXCEPT ALL - not supported in BigQuery
    // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators
    //   "EXCEPT ALL is not supported. Only EXCEPT DISTINCT is supported."
    [Fact]
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    public async Task ExceptAll_Basic()
    {
        var client = await _fixture.GetClientAsync();
        await Assert.ThrowsAsync<GoogleApiException>(async () =>
            await client.ExecuteQueryAsync($"SELECT id, val FROM `{_datasetId}.a` EXCEPT ALL SELECT id, val FROM `{_datasetId}.b`", parameters: null));
    }

    // EXCEPT DISTINCT
    [Fact] public async Task ExceptDistinct_Basic()
    {
        var count = int.Parse((await Scalar("SELECT COUNT(*) FROM (SELECT id, val FROM `{ds}.a` EXCEPT DISTINCT SELECT id, val FROM `{ds}.b`)"))!);
        Assert.True(count >= 1);
    }

    // UNION ALL with different expressions
    [Fact] public async Task UnionAll_Expressions()
    {
        var rows = await Query("SELECT val, 'a' as src FROM `{ds}.a` UNION ALL SELECT val, 'b' as src FROM `{ds}.b` ORDER BY src, val");
        Assert.Equal(8, rows.Count);
    }

    // UNION ALL with LIMIT (wrapped in subquery since ORDER BY/LIMIT attaches to last SELECT otherwise)
    [Fact] public async Task UnionAll_Limit()
    {
        var rows = await Query("SELECT id, val FROM `{ds}.a` UNION ALL SELECT id, val FROM `{ds}.b` ORDER BY id LIMIT 3");
        Assert.Equal(3, rows.Count);
    }

    // Chain UNIONs
    [Fact] public async Task UnionAll_Chain()
    {
        var count = await Scalar("SELECT COUNT(*) FROM (SELECT id FROM `{ds}.a` UNION ALL SELECT id FROM `{ds}.b` UNION ALL SELECT id FROM `{ds}.a`)");
        Assert.Equal("12", count);
    }

    // UNION with aggregate
    [Fact] public async Task UnionAll_Aggregate()
    {
        var rows = await Query("SELECT 'a' as tbl, COUNT(*) as cnt FROM `{ds}.a` UNION ALL SELECT 'b' as tbl, COUNT(*) as cnt FROM `{ds}.b` ORDER BY tbl");
        Assert.Equal(2, rows.Count);
        Assert.Equal("4", rows[0]["cnt"]?.ToString());
    }

    // Bug 1 regression: CTE + SUM(IF()) + UNION ALL + Window functions should return correct types
    // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_clause
    [Fact]
    public async Task UnionAll_CTE_SumIf_WindowFunctions_ReturnsCorrectTypes()
    {
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.sales` (
            loc STRING, period DATE, spend NUMERIC, cards INT64, visits INT64
        )", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.sales` (loc, period, spend, cards, visits) VALUES
            ('L1', DATE '2026-02-16', 1000.50, 50, 60),
            ('L1', DATE '2026-02-23', 2000.75, 100, 120),
            ('L1', DATE '2026-01-19', 900.25, 45, 55),
            ('L1', DATE '2026-01-26', 1800.00, 90, 110),
            ('L2', DATE '2026-02-16', 500.00, 25, 30),
            ('L2', DATE '2026-01-19', 450.00, 22, 28)", parameters: null);

        var sql = $@"
            WITH aggregated AS (
                SELECT loc,
                    SUM(IF(period >= DATE '2026-02-16', cards, NULL)) AS cur_cards,
                    SUM(IF(period >= DATE '2026-02-16', spend, NULL)) AS cur_spend,
                    SUM(IF(period >= DATE '2026-02-16', visits, NULL)) AS cur_visits,
                    SUM(IF(period < DATE '2026-02-16', cards, NULL)) AS cmp_cards,
                    SUM(IF(period < DATE '2026-02-16', spend, NULL)) AS cmp_spend,
                    SUM(IF(period < DATE '2026-02-16', visits, NULL)) AS cmp_visits
                FROM `{_datasetId}.sales`
                GROUP BY loc
            )
            SELECT loc, cur_cards, cur_spend,
                SAFE_DIVIDE(cur_spend, cur_visits) AS avg_spend,
                SAFE_DIVIDE(cur_cards - cmp_cards, cmp_cards) AS cards_change,
                SUM(cur_cards) OVER () AS agg_cards,
                SAFE_DIVIDE(SUM(cur_spend) OVER (), SUM(cur_visits) OVER ()) AS agg_avg_spend
            FROM aggregated
            UNION ALL
            SELECT loc, cmp_cards, cmp_spend,
                SAFE_DIVIDE(cmp_spend, cmp_visits) AS avg_spend,
                0.0 AS cards_change,
                SUM(cmp_cards) OVER () AS agg_cards,
                SAFE_DIVIDE(SUM(cmp_spend) OVER (), SUM(cmp_visits) OVER ()) AS agg_avg_spend
            FROM aggregated";

        var result = await client.ExecuteQueryAsync(sql, parameters: null);
        var rows = result.ToList();

        Assert.Equal(4, rows.Count);

        var curCardsField = result.Schema.Fields.First(f => f.Name == "cur_cards");
        var curSpendField = result.Schema.Fields.First(f => f.Name == "cur_spend");
        var avgSpendField = result.Schema.Fields.First(f => f.Name == "avg_spend");
        var aggCardsField = result.Schema.Fields.First(f => f.Name == "agg_cards");

        Assert.NotEqual("STRING", curCardsField.Type);
        Assert.NotEqual("STRING", curSpendField.Type);
        Assert.NotEqual("STRING", avgSpendField.Type);
        Assert.NotEqual("STRING", aggCardsField.Type);

        Assert.NotNull(rows[0]["cur_cards"]);
        Assert.NotNull(rows[0]["cur_spend"]);
        Assert.NotNull(rows[0]["agg_cards"]);
    }
}