using BigQuery.InMemoryEmulator.SqlEngine;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit;

/// <summary>
/// Unit tests for the SQL parser (Phase 4 scope).
/// </summary>
public class SqlParserTests
{
	[Fact]
	public void Parser_SelectStar_Parses()
	{
		var stmt = SqlParser.ParseSql("SELECT * FROM dataset.my_table");

		var select = Assert.IsType<SelectStatement>(stmt);
		Assert.False(select.Distinct);
		Assert.Single(select.Columns);
		Assert.IsType<StarExpr>(select.Columns[0].Expr);

		var from = Assert.IsType<TableRef>(select.From);
		Assert.Equal("dataset", from.DatasetId);
		Assert.Equal("my_table", from.TableId);
	}

	[Fact]
	public void Parser_SelectColumns_Parses()
	{
		var stmt = SqlParser.ParseSql("SELECT id, name AS n FROM tbl");

		var select = Assert.IsType<SelectStatement>(stmt);
		Assert.Equal(2, select.Columns.Count);

		var col0 = select.Columns[0];
		var colRef0 = Assert.IsType<ColumnRef>(col0.Expr);
		Assert.Equal("id", colRef0.ColumnName);
		Assert.Null(col0.Alias);

		var col1 = select.Columns[1];
		var colRef1 = Assert.IsType<ColumnRef>(col1.Expr);
		Assert.Equal("name", colRef1.ColumnName);
		Assert.Equal("n", col1.Alias);
	}

	[Fact]
	public void Parser_WhereClause_Parses()
	{
		var stmt = SqlParser.ParseSql("SELECT * FROM t WHERE x = 42");

		var select = Assert.IsType<SelectStatement>(stmt);
		Assert.NotNull(select.Where);
		var bin = Assert.IsType<BinaryExpr>(select.Where);
		Assert.Equal(BinaryOp.Eq, bin.Op);
		var left = Assert.IsType<ColumnRef>(bin.Left);
		Assert.Equal("x", left.ColumnName);
		var right = Assert.IsType<LiteralExpr>(bin.Right);
		Assert.Equal(42L, right.Value);
	}

	[Fact]
	public void Parser_OrderBy_Parses()
	{
		var stmt = SqlParser.ParseSql("SELECT name FROM t ORDER BY name DESC");

		var select = Assert.IsType<SelectStatement>(stmt);
		Assert.NotNull(select.OrderBy);
		Assert.Single(select.OrderBy);
		var orderItem = select.OrderBy[0];
		Assert.True(orderItem.Descending);
		var colRef = Assert.IsType<ColumnRef>(orderItem.Expr);
		Assert.Equal("name", colRef.ColumnName);
	}

	[Fact]
	public void Parser_Limit_Parses()
	{
		var stmt = SqlParser.ParseSql("SELECT * FROM t LIMIT 10 OFFSET 5");

		var select = Assert.IsType<SelectStatement>(stmt);
		Assert.Equal(10, select.Limit);
		Assert.Equal(5, select.Offset);
	}

	[Fact]
	public void Parser_WhereAndOr_Parses()
	{
		var stmt = SqlParser.ParseSql("SELECT * FROM t WHERE a = 1 AND b = 2 OR c = 3");

		var select = Assert.IsType<SelectStatement>(stmt);
		// OR has lower precedence: (a=1 AND b=2) OR c=3
		var or = Assert.IsType<BinaryExpr>(select.Where);
		Assert.Equal(BinaryOp.Or, or.Op);
		var and = Assert.IsType<BinaryExpr>(or.Left);
		Assert.Equal(BinaryOp.And, and.Op);
	}

	[Fact]
	public void Parser_Parameters_Parses()
	{
		var stmt = SqlParser.ParseSql("SELECT * FROM t WHERE id = @id");

		var select = Assert.IsType<SelectStatement>(stmt);
		var bin = Assert.IsType<BinaryExpr>(select.Where);
		var param = Assert.IsType<ParameterRef>(bin.Right);
		Assert.Equal("id", param.Name);
	}

	[Fact]
	public void Parser_FunctionCall_Parses()
	{
		var stmt = SqlParser.ParseSql("SELECT CONCAT(a, b) FROM t");

		var select = Assert.IsType<SelectStatement>(stmt);
		var func = Assert.IsType<FunctionCall>(select.Columns[0].Expr);
		Assert.Equal("CONCAT", func.FunctionName);
		Assert.Equal(2, func.Args.Count);
	}

	[Fact]
	public void Parser_AggregateCall_Parses()
	{
		var stmt = SqlParser.ParseSql("SELECT COUNT(*) FROM t");

		var select = Assert.IsType<SelectStatement>(stmt);
		var agg = Assert.IsType<AggregateCall>(select.Columns[0].Expr);
		Assert.Equal("COUNT", agg.FunctionName);
		Assert.IsType<StarExpr>(agg.Arg);
		Assert.False(agg.Distinct);
	}

	[Fact]
	public void Parser_IsNull_Parses()
	{
		var stmt = SqlParser.ParseSql("SELECT * FROM t WHERE x IS NOT NULL");

		var select = Assert.IsType<SelectStatement>(stmt);
		var isNull = Assert.IsType<IsNullExpr>(select.Where);
		Assert.True(isNull.IsNot);
	}

	[Fact]
	public void Parser_Between_Parses()
	{
		var stmt = SqlParser.ParseSql("SELECT * FROM t WHERE x BETWEEN 1 AND 10");

		var select = Assert.IsType<SelectStatement>(stmt);
		var between = Assert.IsType<BetweenExpr>(select.Where);
		Assert.IsType<ColumnRef>(between.Expr);
	}

	[Fact]
	public void Parser_InExpression_Parses()
	{
		var stmt = SqlParser.ParseSql("SELECT * FROM t WHERE x IN (1, 2, 3)");

		var select = Assert.IsType<SelectStatement>(stmt);
		var inExpr = Assert.IsType<InExpr>(select.Where);
		Assert.Equal(3, inExpr.Values.Count);
	}

	[Fact]
	public void Parser_BacktickTableRef_Parses()
	{
		var stmt = SqlParser.ParseSql("SELECT * FROM `project.dataset.table`");

		var select = Assert.IsType<SelectStatement>(stmt);
		var from = Assert.IsType<TableRef>(select.From);
		Assert.Equal("dataset", from.DatasetId);
		Assert.Equal("table", from.TableId);
	}

	[Fact]
	public void Parser_SelectWithoutFrom_Parses()
	{
		var stmt = SqlParser.ParseSql("SELECT 1, 'hello'");

		var select = Assert.IsType<SelectStatement>(stmt);
		Assert.Null(select.From);
		Assert.Equal(2, select.Columns.Count);
	}
}
