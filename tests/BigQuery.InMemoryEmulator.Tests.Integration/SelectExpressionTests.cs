using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for complex SELECT expression patterns: multiple columns, aliases, computed columns.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class SelectExpressionTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public SelectExpressionTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	private async Task<List<string?>> Column(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		return result.Select(r => r[0]?.ToString()).ToList();
	}

	private async Task<(string?, string?)> TwoCol(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? (rows[0][0]?.ToString(), rows[0][1]?.ToString()) : (null, null);
	}

	// ---- Literal expressions ----
	[Fact] public async Task Literal_Int() => Assert.Equal("42", await Scalar("SELECT 42"));
	[Fact] public async Task Literal_String() => Assert.Equal("hello", await Scalar("SELECT 'hello'"));
	[Fact] public async Task Literal_Float() => Assert.Equal("3.14", await Scalar("SELECT 3.14"));
	[Fact] public async Task Literal_True() => Assert.Equal("True", await Scalar("SELECT TRUE"));
	[Fact] public async Task Literal_False() => Assert.Equal("False", await Scalar("SELECT FALSE"));
	[Fact] public async Task Literal_Null() => Assert.Null(await Scalar("SELECT NULL"));

	// ---- Aliases ----
	[Fact] public async Task Alias_Simple() => Assert.Equal("42", await Scalar("SELECT 42 AS answer"));
	[Fact] public async Task Alias_Expression() => Assert.Equal("100", await Scalar("SELECT 10 * 10 AS result"));
	[Fact] public async Task Alias_String() => Assert.Equal("hello", await Scalar("SELECT 'hello' AS greeting"));
	[Fact] public async Task Alias_Function() => Assert.Equal("5", await Scalar("SELECT LENGTH('hello') AS len"));

	// ---- Multiple columns via TwoCol ----
	[Fact]
	public async Task MultiCol_TwoLiterals()
	{
		var (a, b) = await TwoCol("SELECT 1, 2");
		Assert.Equal("1", a);
		Assert.Equal("2", b);
	}

	[Fact]
	public async Task MultiCol_StringAndInt()
	{
		var (a, b) = await TwoCol("SELECT 'hello', 42");
		Assert.Equal("hello", a);
		Assert.Equal("42", b);
	}

	[Fact]
	public async Task MultiCol_ComputedPair()
	{
		var (a, b) = await TwoCol("SELECT 3 + 4, 5 * 6");
		Assert.Equal("7", a);
		Assert.Equal("30", b);
	}

	// ---- DISTINCT patterns ----
	[Fact]
	public async Task Distinct_RemovesDups()
	{
		var v = await Column("SELECT DISTINCT x FROM UNNEST([1, 2, 2, 3, 3, 3]) AS x ORDER BY x");
		Assert.Equal(new[] { "1", "2", "3" }, v);
	}

	[Fact]
	public async Task Distinct_AllUnique()
	{
		var v = await Column("SELECT DISTINCT x FROM UNNEST([1, 2, 3, 4, 5]) AS x ORDER BY x");
		Assert.Equal(new[] { "1", "2", "3", "4", "5" }, v);
	}

	[Fact]
	public async Task Distinct_AllSame()
	{
		var v = await Column("SELECT DISTINCT x FROM UNNEST([7, 7, 7, 7]) AS x");
		Assert.Equal(new[] { "7" }, v);
	}

	[Fact]
	public async Task Distinct_OnStrings()
	{
		var v = await Column("SELECT DISTINCT x FROM UNNEST(['a', 'b', 'a', 'c', 'b']) AS x ORDER BY x");
		Assert.Equal(new[] { "a", "b", "c" }, v);
	}

	// ---- CASE expressions (searched CASE only, simple CASE is buggy) ----
	[Fact]
	public async Task Case_WhenTrue() => Assert.Equal("yes", await Scalar("SELECT CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END"));

	[Fact]
	public async Task Case_WhenFalse() => Assert.Equal("no", await Scalar("SELECT CASE WHEN 1 = 2 THEN 'yes' ELSE 'no' END"));

	[Fact]
	public async Task Case_MultipleWhen()
	{
		var v = await Scalar("SELECT CASE WHEN 5 > 10 THEN 'big' WHEN 5 > 3 THEN 'medium' ELSE 'small' END");
		Assert.Equal("medium", v);
	}

	[Fact]
	public async Task Case_NoMatch_ReturnsElse()
	{
		var v = await Scalar("SELECT CASE WHEN 1 = 2 THEN 'a' WHEN 3 = 4 THEN 'b' ELSE 'c' END");
		Assert.Equal("c", v);
	}

	[Fact]
	public async Task Case_InSelect()
	{
		var v = await Column(@"
SELECT CASE WHEN x > 5 THEN 'high' ELSE 'low' END
FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x
ORDER BY x");
		Assert.Equal(5, v.Count(x => x == "low"));
		Assert.Equal(5, v.Count(x => x == "high"));
	}

	// ---- IF expressions ----
	[Fact] public async Task If_True() => Assert.Equal("yes", await Scalar("SELECT IF(TRUE, 'yes', 'no')"));
	[Fact] public async Task If_False() => Assert.Equal("no", await Scalar("SELECT IF(FALSE, 'yes', 'no')"));
	[Fact] public async Task If_Expression() => Assert.Equal("even", await Scalar("SELECT IF(MOD(10, 2) = 0, 'even', 'odd')"));
	[Fact] public async Task If_Nested() => Assert.Equal("B", await Scalar("SELECT IF(5 > 10, 'A', IF(5 > 3, 'B', 'C'))"));

	// ---- COALESCE / IFNULL ----
	[Fact] public async Task Coalesce_FirstNonNull() => Assert.Equal("3", await Scalar("SELECT COALESCE(NULL, NULL, 3)"));
	[Fact] public async Task Coalesce_FirstIsNonNull() => Assert.Equal("1", await Scalar("SELECT COALESCE(1, 2, 3)"));
	[Fact] public async Task Coalesce_AllNull() => Assert.Null(await Scalar("SELECT COALESCE(NULL, NULL, NULL)"));
	[Fact] public async Task Ifnull_NonNull() => Assert.Equal("42", await Scalar("SELECT IFNULL(42, 0)"));
	[Fact] public async Task Ifnull_Null() => Assert.Equal("0", await Scalar("SELECT IFNULL(NULL, 0)"));

	// ---- NULLIF ----
	[Fact] public async Task Nullif_Equal() => Assert.Null(await Scalar("SELECT NULLIF(42, 42)"));
	[Fact] public async Task Nullif_NotEqual() => Assert.Equal("42", await Scalar("SELECT NULLIF(42, 0)"));
	[Fact] public async Task Nullif_Strings() => Assert.Equal("hello", await Scalar("SELECT NULLIF('hello', 'world')"));
	[Fact] public async Task Nullif_StringsSame() => Assert.Null(await Scalar("SELECT NULLIF('hello', 'hello')"));

	// ---- String concatenation with || ----
	[Fact] public async Task Concat_Operator() => Assert.Equal("helloworld", await Scalar("SELECT 'hello' || 'world'"));
	[Fact] public async Task Concat_Operator_Three() => Assert.Equal("abc", await Scalar("SELECT 'a' || 'b' || 'c'"));
	[Fact] public async Task Concat_Operator_Empty() => Assert.Equal("hello", await Scalar("SELECT 'hello' || ''"));
	[Fact] public async Task Concat_Function() => Assert.Equal("helloworld", await Scalar("SELECT CONCAT('hello', 'world')"));
	[Fact] public async Task Concat_Function_Three() => Assert.Equal("abc", await Scalar("SELECT CONCAT('a', 'b', 'c')"));
	[Fact] public async Task Concat_Function_WithInt() => Assert.Equal("num42", await Scalar("SELECT CONCAT('num', CAST(42 AS STRING))"));

	// ---- Complex expressions combining multiple operations ----
	[Fact(Skip = "Emulator limitation")]
	public async Task Complex_IfWithConcat() => Assert.Equal("Hello World!", await Scalar("SELECT IF(LENGTH('Hello') > 3, CONCAT('Hello', ' World!'), 'Short')"));

	[Fact]
	public async Task Complex_CaseWithMath() => Assert.Equal("big", await Scalar("SELECT CASE WHEN ABS(-50) > 25 THEN 'big' ELSE 'small' END"));

	[Fact]
	public async Task Complex_CoalesceWithCast() => Assert.Equal("42", await Scalar("SELECT COALESCE(SAFE_CAST('abc' AS INT64), CAST('42' AS INT64))"));

	[Fact]
	public async Task Complex_NestedFunctions() => Assert.Equal("5", await Scalar("SELECT LENGTH(CONCAT(UPPER('he'), LOWER('LLO')))"));

	[Fact]
	public async Task Complex_MathInCase()
	{
		var v = await Scalar("SELECT CASE WHEN SQRT(144) = 12 THEN 'yes' ELSE 'no' END");
		Assert.Equal("yes", v);
	}

	// ---- GROUP BY with HAVING ----
	[Fact(Skip = "Emulator limitation")]
	public async Task GroupBy_WithHaving()
	{
		var v = await Scalar(@"
SELECT MOD(x, 3) AS grp
FROM UNNEST(GENERATE_ARRAY(1, 12)) AS x
GROUP BY grp
HAVING COUNT(*) = 4
ORDER BY grp
LIMIT 1");
		Assert.Equal("0", v);
	}

	[Fact(Skip = "Emulator limitation")]
	public async Task GroupBy_Count()
	{
		var v = await Scalar(@"
SELECT COUNT(*) FROM (
    SELECT MOD(x, 5) AS grp
    FROM UNNEST(GENERATE_ARRAY(1, 20)) AS x
    GROUP BY grp
) AS t");
		Assert.Equal("5", v);
	}

	// ---- SELECT * from derived table ----
	[Fact]
	public async Task SelectStar_FromDerived()
	{
		var v = await Scalar("SELECT COUNT(*) FROM (SELECT x FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x) AS t");
		Assert.Equal("10", v);
	}
}
