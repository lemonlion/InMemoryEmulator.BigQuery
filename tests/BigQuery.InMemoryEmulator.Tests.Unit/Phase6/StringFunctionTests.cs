using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase6;

/// <summary>
/// Unit tests for built-in string functions (Phase 6).
/// </summary>
public class StringFunctionTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;

		var schema = new TableSchema
		{
			Fields = [new TableFieldSchema { Name = "val", Type = "STRING", Mode = "NULLABLE" }]
		};
		var table = new InMemoryTable("test_ds", "t", schema);
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["val"] = "Hello World" }));
		ds.Tables["t"] = table;

		return new QueryExecutor(store, "test_ds");
	}

	[Theory]
	[InlineData("SELECT SUBSTR('abcdef', 2, 3)", "bcd")]
	[InlineData("SELECT SUBSTR('abcdef', 2)", "bcdef")]
	[InlineData("SELECT SUBSTR('abcdef', -2)", "ef")]
	public void Substr(string sql, string expected)
	{
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal(expected, rows[0].F[0].V?.ToString());
	}

	[Theory]
	[InlineData("SELECT REPLACE('hello world', 'world', 'there')", "hello there")]
	[InlineData("SELECT REPLACE('aaa', 'a', 'bb')", "bbbbbb")]
	public void Replace(string sql, string expected)
	{
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal(expected, rows[0].F[0].V?.ToString());
	}

	[Theory]
	[InlineData("SELECT STARTS_WITH('hello', 'he')", "true")]
	[InlineData("SELECT STARTS_WITH('hello', 'lo')", "false")]
	public void StartsWith(string sql, string expected)
	{
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal(expected, rows[0].F[0].V?.ToString());
	}

	[Theory]
	[InlineData("SELECT ENDS_WITH('hello', 'lo')", "true")]
	[InlineData("SELECT ENDS_WITH('hello', 'he')", "false")]
	public void EndsWith(string sql, string expected)
	{
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal(expected, rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Reverse()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT REVERSE('hello')");
		Assert.Equal("olleh", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Repeat()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT REPEAT('ab', 3)");
		Assert.Equal("ababab", rows[0].F[0].V?.ToString());
	}

	[Theory]
	[InlineData("SELECT STRPOS('hello world', 'world')", "7")]
	[InlineData("SELECT STRPOS('hello', 'xyz')", "0")]
	public void Strpos(string sql, string expected)
	{
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal(expected, rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Lpad()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT LPAD('hi', 5, '0')");
		Assert.Equal("000hi", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Rpad()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT RPAD('hi', 5, '0')");
		Assert.Equal("hi000", rows[0].F[0].V?.ToString());
	}

	[Theory]
	[InlineData("SELECT REGEXP_CONTAINS('hello123', '[0-9]+')", "true")]
	[InlineData("SELECT REGEXP_CONTAINS('hello', '[0-9]+')", "false")]
	public void RegexpContains(string sql, string expected)
	{
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal(expected, rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void RegexpReplace()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT REGEXP_REPLACE('hello 123 world', '[0-9]+', 'NUM')");
		Assert.Equal("hello NUM world", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Left()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT LEFT('hello', 3)");
		Assert.Equal("hel", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Right()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT RIGHT('hello', 3)");
		Assert.Equal("llo", rows[0].F[0].V?.ToString());
	}

	[Theory]
	[InlineData("SELECT LTRIM('  hello  ')", "hello  ")]
	[InlineData("SELECT RTRIM('  hello  ')", "  hello")]
	public void LtrimRtrim(string sql, string expected)
	{
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal(expected, rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void ContainsSubstr()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT CONTAINS_SUBSTR('Hello World', 'hello')");
		Assert.Equal("true", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Ascii()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ASCII('A')");
		Assert.Equal("65", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Chr()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT CHR(65)");
		Assert.Equal("A", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void NullPropagation()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT SUBSTR(NULL, 1, 2)");
		Assert.Null(rows[0].F[0].V);
	}
}
