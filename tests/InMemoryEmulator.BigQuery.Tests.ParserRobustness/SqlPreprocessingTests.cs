using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.ParserRobustness;

/// <summary>
/// Tests that the SQL preprocessing pipeline handles real-world SQL formatting.
/// These go through the full SDK → HTTP handler → preprocessor → parser pipeline,
/// and run against in-memory, GO emulator, and Cloud BigQuery via BIGQUERY_TEST_TARGET.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class SqlPreprocessingTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _ds = null!;

    public SqlPreprocessingTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _ds = $"pr_{Guid.NewGuid():N}"[..25];
        await _fixture.CreateDatasetAsync(_ds);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync(
            $"CREATE TABLE `{_ds}.t` (id INT64, name STRING, created_date DATE, amount NUMERIC)", parameters: null);
        await client.ExecuteQueryAsync(
            $"INSERT INTO `{_ds}.t` (id, name, created_date, amount) VALUES (1, 'Alice', '2025-01-15', 100.5), (2, 'Bob', '2025-03-20', 200.75), (3, 'Carol', '2025-06-01', 50.0)",
            parameters: null);
    }

    public async ValueTask DisposeAsync()
    {
        try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
        await _fixture.DisposeAsync();
    }

    private async Task<List<BigQueryRow>> Q(string sql, IEnumerable<BigQueryParameter>? parameters = null)
    {
        var client = await _fixture.GetClientAsync();
        var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters);
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

    [Fact] public async Task LineComment_EmptyComment()
    {
        var rows = await Q("SELECT id FROM `{ds}.t` --\nORDER BY id");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task LineComment_AfterTrailingComma()
    {
        var rows = await Q("SELECT\n  id, -- pk\n  name, -- customer\nFROM `{ds}.t`");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task LineComment_BetweenUnionAll()
    {
        var rows = await Q(
            "SELECT id, name FROM `{ds}.t` WHERE id = 1\n-- union with second row\nUNION ALL\nSELECT id, name FROM `{ds}.t` WHERE id = 2");
        Assert.Equal(2, rows.Count);
    }

    [Fact] public async Task LineComment_ContainingUrl()
    {
        var rows = await Q("SELECT id FROM `{ds}.t` -- see https://example.com/docs#section");
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

    [Fact] public async Task BlockComment_Empty()
    {
        var rows = await Q("SELECT id /**/ FROM `{ds}.t`");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task BlockComment_BetweenCteDefinitions()
    {
        var rows = await Q(@"
            WITH a AS (SELECT id, name FROM `{ds}.t` WHERE id = 1),
            /* second CTE */
            b AS (SELECT id, name FROM `{ds}.t` WHERE id = 2)
            SELECT * FROM a UNION ALL SELECT * FROM b");
        Assert.Equal(2, rows.Count);
    }

    // ===================================================================
    // Hash comments (#) — BigQuery also supports these
    // ===================================================================

    [Fact] public async Task HashComment_EndOfLine()
    {
        var rows = await Q("SELECT id FROM `{ds}.t` # hash comment");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task HashComment_AtStart()
    {
        var rows = await Q("# initial comment\nSELECT id FROM `{ds}.t`");
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

    [Fact] public async Task TrailingSemicolon_WithNewlineAfter()
    {
        var rows = await Q("SELECT id FROM `{ds}.t`;\n");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task TrailingSemicolon_WithParameters()
    {
        var rows = await Q("SELECT id FROM `{ds}.t` WHERE id = @id ;",
            new[] { new BigQueryParameter("id", BigQueryDbType.Int64, 1) });
        Assert.Single(rows);
    }

    [Fact] public async Task TrailingSemicolon_MultipleSemicolons()
    {
        var rows = await Q("SELECT id FROM `{ds}.t`;;");
        Assert.Equal(3, rows.Count);
    }

    // ===================================================================
    // Trailing commas in SELECT (BigQuery-specific leniency)
    // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_list
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

    [Fact] public async Task TrailingComma_SingleColumn()
    {
        var rows = await Q("SELECT id, FROM `{ds}.t`");
        Assert.Equal(3, rows.Count);
    }

    [Fact] public async Task TrailingComma_WithCommentAfter()
    {
        var rows = await Q("SELECT\n  id,\n  name, -- last column\nFROM `{ds}.t`");
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

    [Fact] public async Task Cte_WithLineComments()
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

    [Fact] public async Task Cte_WithBlockComments()
    {
        var rows = await Q(@"
            WITH
            /* first CTE */ a AS (SELECT id, name FROM `{ds}.t` WHERE id = 1),
            /* second CTE */ b AS (SELECT id, name FROM `{ds}.t` WHERE id = 2)
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

    [Fact] public async Task Cte_ThreeCtes_MixedCommaStyle()
    {
        var rows = await Q(@"
            WITH a AS (SELECT id, name FROM `{ds}.t` WHERE id = 1),
            b AS (SELECT id, name FROM `{ds}.t` WHERE id = 2)
            , c AS (SELECT id, name FROM `{ds}.t` WHERE id = 3)
            SELECT a.id, a.name FROM a
            INNER JOIN b ON b.id = b.id
            INNER JOIN c ON c.id = c.id");
        Assert.Single(rows);
    }

    // ===================================================================
    // DATE / TIMESTAMP / DATETIME / TIME typed literals
    // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#typed_literals
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

    [Fact] public async Task DateLiteral_InDateDiff()
    {
        var v = await S("SELECT CAST(DATE_DIFF(DATE '2025-03-01', DATE '2025-01-01', DAY) AS STRING)");
        Assert.Equal("59", v);
    }

    [Fact] public async Task TimestampLiteral_InSelect()
    {
        var v = await S("SELECT CAST(TIMESTAMP '2025-01-15 10:30:00 UTC' AS STRING)");
        Assert.Contains("2025-01-15", v);
    }

    [Fact] public async Task TimestampLiteral_WithTimezone()
    {
        var v = await S("SELECT CAST(TIMESTAMP '2025-06-15 14:00:00+05:30' AS STRING)");
        Assert.NotNull(v);
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

    [Fact] public async Task MultipleDateLiterals_SameQuery()
    {
        var v = await S("SELECT CAST(DATE_DIFF(DATE '2025-12-31', DATE '2025-01-01', DAY) AS STRING)");
        Assert.Equal("364", v);
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

    [Fact] public async Task Param_Date_InCaseExpression()
    {
        var v = await S("SELECT CASE WHEN @d >= DATE '2025-01-01' THEN 'recent' ELSE 'old' END",
            new[] { new BigQueryParameter("d", BigQueryDbType.Date, new DateTime(2025, 6, 1)) });
        Assert.Equal("recent", v);
    }

    [Fact] public async Task Param_MultipleTypes_SameQuery()
    {
        var rows = await Q(
            "SELECT id FROM `{ds}.t` WHERE name = @name AND created_date >= @d AND amount > @min ORDER BY id",
            new[] {
                new BigQueryParameter("name", BigQueryDbType.String, "Bob"),
                new BigQueryParameter("d", BigQueryDbType.Date, new DateTime(2025, 1, 1)),
                new BigQueryParameter("min", BigQueryDbType.Float64, 100.0),
            });
        Assert.Single(rows);
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

    [Fact] public async Task RealWorld_QueryFromBigQueryEditor()
    {
        // Typical formatting from the BigQuery web console: extra leading whitespace, trailing semicolon
        var rows = await Q("    SELECT\n      id,\n      name\n    FROM\n      `{ds}.t`\n    WHERE\n      id = 1\n    ;");
        Assert.Single(rows);
    }

    [Fact] public async Task RealWorld_OrmGeneratedMultilineWithParams()
    {
        // ORMs often generate SQL with redundant parentheses and verbose formatting
        var rows = await Q(
            "SELECT\n  id,\n  name\nFROM\n  `{ds}.t`\nWHERE\n  ((created_date) >= (@start))\n  AND ((amount) > (@min))\nORDER BY\n  id ASC",
            new[] {
                new BigQueryParameter("start", BigQueryDbType.Date, new DateTime(2025, 1, 1)),
                new BigQueryParameter("min", BigQueryDbType.Float64, 50.0),
            });
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

    [Fact] public async Task Whitespace_MixedLineEndings()
    {
        var rows = await Q("SELECT id\nFROM `{ds}.t`\r\nWHERE id = 1\rORDER BY id");
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

    [Fact] public async Task StringLiteral_ContainsHash()
    {
        var v = await S("SELECT 'color #FF0000'");
        Assert.Equal("color #FF0000", v);
    }

    [Fact] public async Task BacktickIdentifier_WithHyphens()
    {
        // Project IDs often have hyphens: `my-project.dataset.table`
        // This uses the real dataset — just verifies backticks with hyphens parse
        var rows = await Q("SELECT id FROM `{ds}.t` WHERE id = 1");
        Assert.Single(rows);
    }
}
