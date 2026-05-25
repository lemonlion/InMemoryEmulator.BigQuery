using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.ParserRobustness;

/// <summary>
/// Tests that the SQL preprocessing pipeline handles real-world SQL formatting.
/// These go through the full SDK → HTTP handler → preprocessor → parser pipeline,
/// because preprocessing bugs only surface when SQL arrives from real applications.
/// </summary>
public class SqlPreprocessingTests : IAsyncLifetime
{
    private BigQueryClient _client = null!;
    private string _ds = null!;

    public async ValueTask InitializeAsync()
    {
        var bq = InMemoryBigQuery.Create();
        _client = bq.Client;
        _ds = $"ds_{Guid.NewGuid():N}"[..20];
        await _client.CreateDatasetAsync(_ds);
        await _client.ExecuteQueryAsync(
            $"CREATE TABLE `{_ds}.t` (id INT64, name STRING, created_date DATE, amount NUMERIC)", parameters: null);
        await _client.ExecuteQueryAsync(
            $"INSERT INTO `{_ds}.t` (id, name, created_date, amount) VALUES (1, 'Alice', '2025-01-15', 100.5), (2, 'Bob', '2025-03-20', 200.75), (3, 'Carol', '2025-06-01', 50.0)",
            parameters: null);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private async Task<List<BigQueryRow>> Q(string sql, IEnumerable<BigQueryParameter>? parameters = null)
    {
        var result = await _client.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters);
        return result.ToList();
    }

    private async Task<string?> S(string sql, IEnumerable<BigQueryParameter>? parameters = null)
    {
        var rows = await Q(sql, parameters);
        return rows.Count > 0 ? rows[0][0]?.ToString() : null;
    }

    // ===================================================================
    // Line comments (--)
    // ===================================================================

    [Fact] public async Task LineComment_EndOfLine()
    {
        var rows = await Q("SELECT id, name FROM `{ds}.t` -- get all rows\nORDER BY id");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task LineComment_BeforeFrom()
    {
        var rows = await Q("SELECT id -- the primary key\n, name -- the name\nFROM `{ds}.t` ORDER BY id");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task LineComment_OnItsOwnLine()
    {
        var rows = await Q("SELECT id, name\n-- this is a comment on its own line\nFROM `{ds}.t`");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task LineComment_AtStartOfQuery()
    {
        var rows = await Q("-- fetch all rows\nSELECT id FROM `{ds}.t`");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task LineComment_WithColons()
    {
        var rows = await Q("SELECT id FROM `{ds}.t` -- time: 10:30:00");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task LineComment_WithSpecialChars()
    {
        var rows = await Q("SELECT id FROM `{ds}.t` -- note: @param isn't real; DROP TABLE x;");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task LineComment_MultipleConsecutive()
    {
        var rows = await Q("-- comment 1\n-- comment 2\n-- comment 3\nSELECT id FROM `{ds}.t`");
        Assert.Equal(3, rows.Count);
    }

    // ===================================================================
    // Block comments (/* */)
    // ===================================================================

    [Fact] public async Task BlockComment_Inline()
    {
        var rows = await Q("SELECT /* all columns */ id, name FROM `{ds}.t`");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task BlockComment_MultiLine()
    {
        var rows = await Q("SELECT id, name\n/* this comment\n   spans multiple\n   lines */\nFROM `{ds}.t`");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task BlockComment_BeforeQuery()
    {
        var rows = await Q("/* setup query */ SELECT id FROM `{ds}.t`");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task BlockComment_WithSqlKeywords()
    {
        var rows = await Q("SELECT id /* SELECT FROM WHERE DROP */ FROM `{ds}.t`");
        Assert.Equal(3, rows.Count);
    }

    // ===================================================================
    // Hash comments (#) — BigQuery also supports these
    // ===================================================================

    [Fact] public async Task HashComment_EndOfLine()
    {
        var rows = await Q("SELECT id FROM `{ds}.t` # hash comment");
        Assert.Equal(3, rows.Count);
    }

    // ===================================================================
    // Trailing semicolons
    // ===================================================================

    [Fact] public async Task TrailingSemicolon_Simple()
    {
        var rows = await Q("SELECT id FROM `{ds}.t`;");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task TrailingSemicolon_WithWhitespace()
    {
        var rows = await Q("SELECT id FROM `{ds}.t`  ;  ");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task TrailingSemicolon_WithNewline()
    {
        var rows = await Q("SELECT id FROM `{ds}.t`\n;");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task TrailingSemicolon_WithParameters()
    {
        var rows = await Q("SELECT id FROM `{ds}.t` WHERE id = @id ;",
            new[] { new BigQueryParameter("id", BigQueryDbType.Int64, 1) });
        Assert.Single(rows);
    }

    // ===================================================================
    // Trailing commas in SELECT (BigQuery-specific leniency)
    // ===================================================================

    [Fact] public async Task TrailingComma_BeforeFrom()
    {
        var rows = await Q("SELECT id, name, FROM `{ds}.t`");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task TrailingComma_WithNewline()
    {
        var rows = await Q("SELECT\n  id,\n  name,\nFROM `{ds}.t`");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task TrailingComma_WithWhitespace()
    {
        var rows = await Q("SELECT id, name,    FROM `{ds}.t`");
        Assert.Equal(3, rows.Count);
    }

    // ===================================================================
    // CTE formatting variations
    // ===================================================================

    [Fact] public async Task Cte_TrailingCommaStyle()
    {
        var rows = await Q(@"
            WITH a AS (SELECT id, name FROM `{ds}.t` WHERE id = 1),
            b AS (SELECT id, name FROM `{ds}.t` WHERE id = 2)
            SELECT * FROM a UNION ALL SELECT * FROM b");
        Assert.Equal(2, rows.Count);
    }

    [Fact] public async Task Cte_LeadingCommaStyle()
    {
        var rows = await Q(@"
            WITH a AS (SELECT id, name FROM `{ds}.t` WHERE id = 1)
            , b AS (SELECT id, name FROM `{ds}.t` WHERE id = 2)
            SELECT * FROM a UNION ALL SELECT * FROM b");
        Assert.Equal(2, rows.Count);
    }

    [Fact] public async Task Cte_LeadingCommaStyle_WithBlankLines()
    {
        var rows = await Q("WITH a AS (SELECT id, name FROM `{ds}.t` WHERE id = 1)\n\n, b AS (SELECT id, name FROM `{ds}.t` WHERE id = 2)\nSELECT * FROM a UNION ALL SELECT * FROM b");
        Assert.Equal(2, rows.Count);
    }

    [Fact] public async Task Cte_WithComments()
    {
        var rows = await Q(@"
            WITH
            -- first CTE: get Alice
            a AS (SELECT id, name FROM `{ds}.t` WHERE id = 1),
            -- second CTE: get Bob
            b AS (SELECT id, name FROM `{ds}.t` WHERE id = 2)
            SELECT * FROM a UNION ALL SELECT * FROM b");
        Assert.Equal(2, rows.Count);
    }

    [Fact] public async Task Cte_SingleCte_WithComments()
    {
        var rows = await Q(@"
            WITH filtered AS (
                -- only high-value rows
                SELECT id, name FROM `{ds}.t`
                WHERE amount > 100 -- threshold
            )
            SELECT * FROM filtered");
        Assert.Equal(2, rows.Count);
    }

    // ===================================================================
    // DATE / TIMESTAMP / DATETIME / TIME typed literals
    // ===================================================================

    [Fact] public async Task DateLiteral_InSelect()
    {
        var v = await S("SELECT CAST(DATE '2025-01-01' AS STRING)");
        Assert.Equal("2025-01-01", v);
    }

    [Fact] public async Task DateLiteral_InWhere()
    {
        var rows = await Q("SELECT id FROM `{ds}.t` WHERE created_date >= DATE '2025-03-01' ORDER BY id");
        Assert.Equal(2, rows.Count);
    }

    [Fact] public async Task DateLiteral_WithComment()
    {
        var v = await S("SELECT CAST(DATE '1900-01-01' AS STRING) -- sentinel date");
        Assert.Equal("1900-01-01", v);
    }

    [Fact] public async Task TimestampLiteral_InSelect()
    {
        var v = await S("SELECT CAST(TIMESTAMP '2025-01-15 10:30:00 UTC' AS STRING)");
        Assert.Contains("2025-01-15", v);
    }

    [Fact] public async Task DatetimeLiteral_InSelect()
    {
        var v = await S("SELECT CAST(DATETIME '2025-01-15 09:00:00' AS STRING)");
        Assert.Contains("2025-01-15", v);
    }

    [Fact] public async Task TimeLiteral_InSelect()
    {
        var v = await S("SELECT CAST(TIME '14:30:00' AS STRING)");
        Assert.Equal("14:30:00", v);
    }

    // ===================================================================
    // Parameters with date/time types
    // ===================================================================

    [Fact] public async Task Param_Date_InWhereComparison()
    {
        var rows = await Q("SELECT id FROM `{ds}.t` WHERE created_date >= @d ORDER BY id",
            new[] { new BigQueryParameter("d", BigQueryDbType.Date, new DateTime(2025, 3, 1)) });
        Assert.Equal(2, rows.Count);
    }

    [Fact] public async Task Param_Date_InDateAdd()
    {
        var rows = await Q("SELECT id FROM `{ds}.t` WHERE created_date >= @d AND created_date <= DATE_ADD(@d, INTERVAL 90 DAY) ORDER BY id",
            new[] { new BigQueryParameter("d", BigQueryDbType.Date, new DateTime(2025, 1, 1)) });
        Assert.Equal(2, rows.Count);
    }

    [Fact] public async Task Param_Date_InCte()
    {
        var rows = await Q(@"
            WITH filtered AS (
                SELECT id, name FROM `{ds}.t`
                WHERE created_date >= @start_date
            )
            SELECT * FROM filtered ORDER BY id",
            new[] { new BigQueryParameter("start_date", BigQueryDbType.Date, new DateTime(2025, 3, 1)) });
        Assert.Equal(2, rows.Count);
    }

    // ===================================================================
    // Combined real-world patterns
    // ===================================================================

    [Fact] public async Task RealWorld_CteWithCommentsAndTrailingSemicolon()
    {
        var rows = await Q(@"
            -- Main query: filter by date
            WITH recent AS (
                SELECT id, name, amount
                FROM `{ds}.t`
                WHERE created_date >= DATE '2025-01-01' -- only 2025 data
            )
            SELECT name, amount FROM recent ORDER BY amount DESC
            ;");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task RealWorld_TrailingCommaWithComment()
    {
        var rows = await Q(@"
            SELECT
                id,
                name, -- customer name
                amount,
            FROM `{ds}.t`
            ORDER BY id");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task RealWorld_MultiCteDateParamsCommentsAndSemicolon()
    {
        var rows = await Q(@"
            -- Aggregation query
            WITH current_period AS (
                SELECT SUM(amount) AS total
                FROM `{ds}.t`
                WHERE created_date >= @start_date -- start of period
            )
            , comparison_period AS (
                SELECT SUM(amount) AS total
                FROM `{ds}.t`
                WHERE created_date < @start_date -- before period
            )
            SELECT
                cur.total AS current_total,
                cmp.total AS comparison_total,
            FROM current_period cur
            CROSS JOIN comparison_period cmp
            ;",
            new[] { new BigQueryParameter("start_date", BigQueryDbType.Date, new DateTime(2025, 3, 1)) });
        Assert.Single(rows);
    }

    [Fact] public async Task RealWorld_DateLiteralAsSentinelInUnionAll()
    {
        var rows = await Q(@"
            SELECT name, CAST(created_date AS STRING) AS dt FROM `{ds}.t` WHERE id = 1
            UNION ALL
            SELECT name, CAST(DATE '1900-01-01' AS STRING) AS dt -- sentinel row
            FROM `{ds}.t` WHERE id = 2");
        Assert.Equal(2, rows.Count);
    }

    [Fact] public async Task RealWorld_HeavilyCommentedQuery()
    {
        var rows = await Q(@"
            /* ===========================
               Revenue Report Query
               Author: analytics team
               Last modified: 2025-06-01
               =========================== */
            SELECT
                id,              -- primary key
                name,            -- customer name
                amount           -- transaction amount
                -- note: created_date excluded intentionally
            FROM `{ds}.t`       -- main transactions table
            WHERE amount > 50   -- minimum threshold
            -- TODO: add date filter later
            ORDER BY amount DESC -- highest first
        ");
        Assert.Equal(2, rows.Count);
    }

    // ===================================================================
    // Whitespace edge cases
    // ===================================================================

    [Fact] public async Task Whitespace_LeadingAndTrailing()
    {
        var rows = await Q("   \n\n  SELECT id FROM `{ds}.t`  \n\n  ");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task Whitespace_TabIndentation()
    {
        var rows = await Q("\tSELECT\n\t\tid,\n\t\tname\n\tFROM `{ds}.t`");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task Whitespace_WindowsLineEndings()
    {
        var rows = await Q("SELECT id\r\nFROM `{ds}.t`\r\nWHERE id = 1");
        Assert.Single(rows);
    }

    // ===================================================================
    // Strings that contain comment-like content (should NOT be stripped)
    // ===================================================================

    [Fact] public async Task StringLiteral_ContainsDashDash()
    {
        var v = await S("SELECT 'hello -- world'");
        Assert.Equal("hello -- world", v);
    }

    [Fact] public async Task StringLiteral_ContainsSlashStar()
    {
        var v = await S("SELECT 'hello /* world */ bye'");
        Assert.Equal("hello /* world */ bye", v);
    }

    [Fact] public async Task StringLiteral_ContainsSemicolon()
    {
        var v = await S("SELECT 'hello; world'");
        Assert.Equal("hello; world", v);
    }
}
