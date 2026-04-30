using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for complex expressions: nested calls, computed columns, aliases, various literals.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/expressions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ExpressionCombinationTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public ExpressionCombinationTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		return result.ToList();
	}

	// ---- Nested function calls ----
	[Fact] public async Task Nested_UpperConcat() => Assert.Equal("HELLO WORLD", await Scalar("SELECT UPPER(CONCAT('hello', ' ', 'world'))"));
	[Fact] public async Task Nested_LowerTrim() => Assert.Equal("hello", await Scalar("SELECT LOWER(TRIM('  HELLO  '))"));
	[Fact] public async Task Nested_AbsFloor() { var v = double.Parse(await Scalar("SELECT ABS(FLOOR(-3.7))") ?? "0"); Assert.Equal(4.0, v); }
	[Fact] public async Task Nested_CeilSqrt() { var v = double.Parse(await Scalar("SELECT CEIL(SQRT(10))") ?? "0"); Assert.Equal(4.0, v); }
	[Fact] public async Task Nested_LengthReverse() => Assert.Equal("5", await Scalar("SELECT LENGTH(REVERSE('hello'))"));
	[Fact] public async Task Nested_RoundPow() { var v = double.Parse(await Scalar("SELECT ROUND(POW(2.1, 3), 1)") ?? "0"); Assert.Equal(9.3, v, 1); }
	[Fact] public async Task Nested_SubstrUpper() => Assert.Equal("HEL", await Scalar("SELECT UPPER(SUBSTR('hello', 1, 3))"));
	[Fact] public async Task Nested_ConcatLength() => Assert.Equal("10", await Scalar("SELECT LENGTH(CONCAT('hello', 'world'))"));
	[Fact] public async Task Nested_AbsCastString() => Assert.Equal("42", await Scalar("SELECT CAST(ABS(-42) AS STRING)"));
	[Fact] public async Task Nested_TripleReverse() => Assert.Equal("hello", await Scalar("SELECT REVERSE(REVERSE(REVERSE(REVERSE('hello'))))"));

	// ---- Arithmetic expression combos ----
	[Fact] public async Task Expr_OrderOfOps1() => Assert.Equal("14", await Scalar("SELECT 2 + 3 * 4"));
	[Fact] public async Task Expr_OrderOfOps2() => Assert.Equal("20", await Scalar("SELECT (2 + 3) * 4"));
	[Fact] public async Task Expr_Complex1() => Assert.Equal("7", await Scalar("SELECT 1 + 2 * 3"));
	[Fact] public async Task Expr_Complex2() => Assert.Equal("10", await Scalar("SELECT 2 * 3 + 4"));
	[Fact] public async Task Expr_MultipleSub() => Assert.Equal("0", await Scalar("SELECT 10 - 3 - 3 - 4"));
	[Fact] public async Task Expr_NestedParens() => Assert.Equal("36", await Scalar("SELECT ((2 + 4) * (3 + 3))"));
	[Fact] public async Task Expr_DivMul() { var v = double.Parse(await Scalar("SELECT 100.0 / 4 * 2") ?? "0"); Assert.Equal(50.0, v); }
	[Fact] public async Task Expr_UnaryChain() => Assert.Equal("-5", await Scalar("SELECT -(-(-5))"));

	// ---- Aliases ----
	[Fact]
	public async Task Alias_Simple()
	{
		var rows = await Query("SELECT 42 AS answer");
		Assert.Equal("42", rows[0]["answer"]?.ToString());
	}

	[Fact]
	public async Task Alias_Expression()
	{
		var rows = await Query("SELECT 2 + 3 AS result");
		Assert.Equal("5", rows[0]["result"]?.ToString());
	}

	[Fact]
	public async Task Alias_StringConcat()
	{
		var rows = await Query("SELECT CONCAT('hello', ' ', 'world') AS greeting");
		Assert.Equal("hello world", rows[0]["greeting"]?.ToString());
	}

	[Fact]
	public async Task Alias_Multiple()
	{
		var rows = await Query("SELECT 1 AS a, 2 AS b, 3 AS c");
		Assert.Equal("1", rows[0]["a"]?.ToString());
		Assert.Equal("2", rows[0]["b"]?.ToString());
		Assert.Equal("3", rows[0]["c"]?.ToString());
	}

	// ---- Multiple columns ----
	[Fact]
	public async Task MultiCol_Types()
	{
		var rows = await Query("SELECT 42 AS int_val, 3.14 AS float_val, 'hello' AS str_val, TRUE AS bool_val");
		Assert.Equal("42", rows[0]["int_val"]?.ToString());
		Assert.Contains("3.14", rows[0]["float_val"]?.ToString());
		Assert.Equal("hello", rows[0]["str_val"]?.ToString());
		Assert.Equal("True", rows[0]["bool_val"]?.ToString());
	}

	[Fact]
	public async Task MultiCol_Computed()
	{
		var rows = await Query("SELECT 2 + 3 AS sum, 2 * 3 AS product, ABS(-7) AS abs_val");
		Assert.Equal("5", rows[0]["sum"]?.ToString());
		Assert.Equal("6", rows[0]["product"]?.ToString());
		Assert.Equal("7", rows[0]["abs_val"]?.ToString());
	}

	// ---- Literal types ----
	[Fact] public async Task Literal_Int() => Assert.Equal("42", await Scalar("SELECT 42"));
	[Fact] public async Task Literal_NegInt() => Assert.Equal("-42", await Scalar("SELECT -42"));
	[Fact] public async Task Literal_Zero() => Assert.Equal("0", await Scalar("SELECT 0"));
	[Fact] public async Task Literal_LargeInt() => Assert.Equal("9999999999", await Scalar("SELECT 9999999999"));
	[Fact] public async Task Literal_Float() { var v = double.Parse(await Scalar("SELECT 3.14") ?? "0"); Assert.Equal(3.14, v, 2); }
	[Fact] public async Task Literal_NegFloat() { var v = double.Parse(await Scalar("SELECT -3.14") ?? "0"); Assert.Equal(-3.14, v, 2); }
	[Fact] public async Task Literal_String() => Assert.Equal("hello", await Scalar("SELECT 'hello'"));
	[Fact] public async Task Literal_EmptyString() => Assert.Equal("", await Scalar("SELECT ''"));
	[Fact] public async Task Literal_StringWithSpaces() => Assert.Equal("hello world", await Scalar("SELECT 'hello world'"));
	[Fact(Skip = "Tokenizer does not support backslash-escaped quotes in string literals")] public async Task Literal_StringWithEscape() => Assert.Equal("it's", await Scalar("SELECT 'it\\'s'"));
	[Fact] public async Task Literal_BoolTrue() => Assert.Equal("True", await Scalar("SELECT TRUE"));
	[Fact] public async Task Literal_BoolFalse() => Assert.Equal("False", await Scalar("SELECT FALSE"));
	[Fact] public async Task Literal_Null() => Assert.Null(await Scalar("SELECT NULL"));
	[Fact] public async Task Literal_Bytes() { var v = await Scalar("SELECT CAST(b'hello' AS STRING)"); Assert.Equal("hello", v); }

	// ---- IF expression ----
	[Fact] public async Task If_True() => Assert.Equal("yes", await Scalar("SELECT IF(TRUE, 'yes', 'no')"));
	[Fact] public async Task If_False() => Assert.Equal("no", await Scalar("SELECT IF(FALSE, 'yes', 'no')"));
	[Fact] public async Task If_Condition() => Assert.Equal("big", await Scalar("SELECT IF(10 > 5, 'big', 'small')"));
	[Fact] public async Task If_Nested() => Assert.Equal("c", await Scalar("SELECT IF(1 > 2, 'a', IF(3 > 2, 'c', 'd'))"));
	[Fact] public async Task If_WithFunction() => Assert.Equal("5", await Scalar("SELECT IF(LENGTH('hello') > 3, CAST(LENGTH('hello') AS STRING), 'short')"));

	// ---- Ternary in expressions ----
	[Fact] public async Task CaseInConcat() => Assert.Equal("hello world", await Scalar("SELECT CONCAT('hello', CASE WHEN TRUE THEN ' world' ELSE '' END)"));
	[Fact] public async Task IfInConcat() => Assert.Equal("hello there", await Scalar("SELECT CONCAT('hello', IF(TRUE, ' there', ''))"));

	// ---- String concatenation with || ----
	[Fact] public async Task StringConcat_Pipe() => Assert.Equal("helloworld", await Scalar("SELECT 'hello' || 'world'"));
	[Fact] public async Task StringConcat_PipeMulti() => Assert.Equal("abc", await Scalar("SELECT 'a' || 'b' || 'c'"));
	[Fact] public async Task StringConcat_PipeEmpty() => Assert.Equal("hello", await Scalar("SELECT 'hello' || ''"));

	// ---- Complex mixed expressions ----
	[Fact] public async Task Complex_CastIfLength() => Assert.Equal("5", await Scalar("SELECT CAST(IF(LENGTH('hello') > 0, LENGTH('hello'), 0) AS STRING)"));
	[Fact] public async Task Complex_CoalesceConcat() => Assert.Equal("hello", await Scalar("SELECT COALESCE(CONCAT('hel', 'lo'), 'default')"));
	[Fact] public async Task Complex_NestedCase()
	{
		var v = await Scalar(@"
			SELECT CASE
				WHEN LENGTH('abc') > 5 THEN 'long'
				WHEN LENGTH('abc') > 2 THEN 'medium'
				ELSE 'short'
			END");
		Assert.Equal("medium", v);
	}

	// ---- Null coalesce operator (??) ----
	[Fact] public async Task NullCoalesce_NotNull() => Assert.Equal("5", await Scalar("SELECT IFNULL(5, 10)"));
	[Fact] public async Task NullCoalesce_Null() => Assert.Equal("10", await Scalar("SELECT IFNULL(CAST(NULL AS INT64), 10)"));

	// ---- STRUCT construction ----
	[Fact(Skip = "STRUCT type not supported")]
	public async Task Struct_Access()
	{
		var rows = await Query("SELECT t.a, t.b FROM (SELECT STRUCT(1 AS a, 'hello' AS b) AS t) AS s");
		Assert.Equal("1", rows[0]["a"]?.ToString());
		Assert.Equal("hello", rows[0]["b"]?.ToString());
	}

	[Fact(Skip = "STRUCT type not supported")]
	public async Task Struct_InUnnest()
	{
		var rows = await Query("SELECT t.name, t.age FROM UNNEST([STRUCT('Alice' AS name, 30 AS age), STRUCT('Bob', 25)]) AS t ORDER BY t.age");
		Assert.Equal("Bob", rows[0]["name"]?.ToString());
		Assert.Equal("25", rows[0]["age"]?.ToString());
	}
}
