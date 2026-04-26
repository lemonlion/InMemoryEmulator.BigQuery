using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase6;

/// <summary>
/// Unit tests for built-in math functions (Phase 6).
/// </summary>
public class MathFunctionTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		return new QueryExecutor(store);
	}

	[Theory]
	[InlineData("SELECT ABS(-5)", "5")]
	[InlineData("SELECT ABS(5)", "5")]
	[InlineData("SELECT ABS(-3.14)", "3.14")]
	public void Abs(string sql, string expected)
	{
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal(expected, rows[0].F[0].V?.ToString());
	}

	[Theory]
	[InlineData("SELECT CEIL(4.1)", "5")]
	[InlineData("SELECT FLOOR(4.9)", "4")]
	public void CeilFloor(string sql, string expected)
	{
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal(expected, rows[0].F[0].V?.ToString());
	}

	[Theory]
	[InlineData("SELECT ROUND(4.5)", "5")]
	[InlineData("SELECT ROUND(4.456, 2)", "4.46")]
	public void Round(string sql, string expected)
	{
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal(expected, rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Trunc()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT TRUNC(4.9)");
		Assert.Equal("4", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Mod()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT MOD(10, 3)");
		Assert.Equal("1", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Div()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT DIV(10, 3)");
		Assert.Equal("3", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Pow()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT POW(2, 10)");
		Assert.Equal("1024", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Sqrt()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT SQRT(16)");
		Assert.Equal("4", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Ln()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT LN(1)");
		Assert.Equal("0", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Log10()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT LOG10(100)");
		Assert.Equal("2", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Sign()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT SIGN(-42)");
		Assert.Equal("-1", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Greatest()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT GREATEST(1, 5, 3)");
		Assert.Equal("5", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Least()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT LEAST(1, 5, 3)");
		Assert.Equal("1", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void SafeDivide_ByZero_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT SAFE_DIVIDE(10, 0)");
		Assert.Null(rows[0].F[0].V);
	}

	[Fact]
	public void SafeDivide_Normal()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT SAFE_DIVIDE(10, 4)");
		Assert.Equal("2.5", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void IeeeDivide_ByZero_ReturnsInfinity()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT IEEE_DIVIDE(10, 0)");
		Assert.Equal("Infinity", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void NullPropagation()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ABS(NULL)");
		Assert.Null(rows[0].F[0].V);
	}
}
