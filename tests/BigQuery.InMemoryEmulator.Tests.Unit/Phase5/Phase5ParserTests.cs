using BigQuery.InMemoryEmulator.SqlEngine;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase5;

/// <summary>
/// Unit tests for Phase 5 parser additions: JOINs, GROUP BY, HAVING, CASE.
/// </summary>
public class Phase5ParserTests
{
	[Fact]
	public void Parse_InnerJoin()
	{
		var stmt = SqlParser.ParseSql("SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id");
		var select = Assert.IsType<SelectStatement>(stmt);
		var join = Assert.IsType<JoinClause>(select.From);
		Assert.Equal(JoinType.Inner, join.Type);
		Assert.IsType<TableRef>(join.Left);
		Assert.IsType<TableRef>(join.Right);
		Assert.NotNull(join.On);
	}

	[Fact]
	public void Parse_LeftJoin()
	{
		var stmt = SqlParser.ParseSql("SELECT * FROM a LEFT JOIN b ON a.id = b.id");
		var select = Assert.IsType<SelectStatement>(stmt);
		var join = Assert.IsType<JoinClause>(select.From);
		Assert.Equal(JoinType.Left, join.Type);
	}

	[Fact]
	public void Parse_LeftOuterJoin()
	{
		var stmt = SqlParser.ParseSql("SELECT * FROM a LEFT OUTER JOIN b ON a.id = b.id");
		var select = Assert.IsType<SelectStatement>(stmt);
		var join = Assert.IsType<JoinClause>(select.From);
		Assert.Equal(JoinType.Left, join.Type);
	}

	[Fact]
	public void Parse_RightJoin()
	{
		var stmt = SqlParser.ParseSql("SELECT * FROM a RIGHT JOIN b ON a.id = b.id");
		var select = Assert.IsType<SelectStatement>(stmt);
		var join = Assert.IsType<JoinClause>(select.From);
		Assert.Equal(JoinType.Right, join.Type);
	}

	[Fact]
	public void Parse_FullOuterJoin()
	{
		var stmt = SqlParser.ParseSql("SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id");
		var select = Assert.IsType<SelectStatement>(stmt);
		var join = Assert.IsType<JoinClause>(select.From);
		Assert.Equal(JoinType.Full, join.Type);
	}

	[Fact]
	public void Parse_CrossJoin()
	{
		var stmt = SqlParser.ParseSql("SELECT * FROM a CROSS JOIN b");
		var select = Assert.IsType<SelectStatement>(stmt);
		var join = Assert.IsType<JoinClause>(select.From);
		Assert.Equal(JoinType.Cross, join.Type);
		Assert.Null(join.On);
	}

	[Fact]
	public void Parse_GroupBy()
	{
		var stmt = SqlParser.ParseSql("SELECT department, COUNT(*) FROM employees GROUP BY department");
		var select = Assert.IsType<SelectStatement>(stmt);
		Assert.NotNull(select.GroupBy);
		Assert.Single(select.GroupBy!);
	}

	[Fact]
	public void Parse_GroupByMultipleColumns()
	{
		var stmt = SqlParser.ParseSql("SELECT a, b, COUNT(*) FROM t GROUP BY a, b");
		var select = Assert.IsType<SelectStatement>(stmt);
		Assert.Equal(2, select.GroupBy!.Count);
	}

	[Fact]
	public void Parse_GroupByHaving()
	{
		var stmt = SqlParser.ParseSql("SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 1");
		var select = Assert.IsType<SelectStatement>(stmt);
		Assert.NotNull(select.GroupBy);
		Assert.NotNull(select.Having);
	}

	[Fact]
	public void Parse_CaseExpression()
	{
		var stmt = SqlParser.ParseSql("SELECT CASE WHEN score > 90 THEN 'A' WHEN score > 80 THEN 'B' ELSE 'C' END AS grade FROM students");
		var select = Assert.IsType<SelectStatement>(stmt);
		var caseExpr = Assert.IsType<CaseExpr>(select.Columns[0].Expr);
		Assert.Equal(2, caseExpr.Branches.Count);
		Assert.NotNull(caseExpr.Else);
	}

	[Fact]
	public void Parse_CaseWithoutElse()
	{
		var stmt = SqlParser.ParseSql("SELECT CASE WHEN x > 0 THEN 'positive' END FROM t");
		var select = Assert.IsType<SelectStatement>(stmt);
		var caseExpr = Assert.IsType<CaseExpr>(select.Columns[0].Expr);
		Assert.Single(caseExpr.Branches);
		Assert.Null(caseExpr.Else);
	}
}
