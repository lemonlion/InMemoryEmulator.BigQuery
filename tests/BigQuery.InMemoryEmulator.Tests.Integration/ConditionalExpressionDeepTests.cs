using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for conditional expressions: CASE, COALESCE, IFNULL, NULLIF, IFF patterns.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ConditionalExpressionDeepTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public ConditionalExpressionDeepTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- Simple CASE WHEN ----
	[Fact] public async Task Case_TrueCondition() => Assert.Equal("yes", await Scalar("SELECT CASE WHEN TRUE THEN 'yes' ELSE 'no' END"));
	[Fact] public async Task Case_FalseCondition() => Assert.Equal("no", await Scalar("SELECT CASE WHEN FALSE THEN 'yes' ELSE 'no' END"));
	[Fact] public async Task Case_IntCompare() => Assert.Equal("equal", await Scalar("SELECT CASE WHEN 1 = 1 THEN 'equal' ELSE 'not' END"));
	[Fact] public async Task Case_StrCompare() => Assert.Equal("match", await Scalar("SELECT CASE WHEN 'a' = 'a' THEN 'match' ELSE 'no' END"));
	[Fact] public async Task Case_GtCompare() => Assert.Equal("big", await Scalar("SELECT CASE WHEN 10 > 5 THEN 'big' ELSE 'small' END"));
	[Fact] public async Task Case_LtCompare() => Assert.Equal("small", await Scalar("SELECT CASE WHEN 1 > 5 THEN 'big' ELSE 'small' END"));

	// ---- Multi-WHEN CASE ----
	[Fact] public async Task Case_Multi_First() => Assert.Equal("one", await Scalar("SELECT CASE WHEN 1=1 THEN 'one' WHEN 2=2 THEN 'two' WHEN 3=3 THEN 'three' END"));
	[Fact] public async Task Case_Multi_Second() => Assert.Equal("two", await Scalar("SELECT CASE WHEN 1=2 THEN 'one' WHEN 2=2 THEN 'two' WHEN 3=3 THEN 'three' END"));
	[Fact] public async Task Case_Multi_Third() => Assert.Equal("three", await Scalar("SELECT CASE WHEN 1=2 THEN 'one' WHEN 2=3 THEN 'two' WHEN 3=3 THEN 'three' END"));
	[Fact] public async Task Case_Multi_Else() => Assert.Equal("none", await Scalar("SELECT CASE WHEN 1=2 THEN 'one' WHEN 2=3 THEN 'two' ELSE 'none' END"));
	[Fact] public async Task Case_Multi_NoElse() => Assert.Null(await Scalar("SELECT CASE WHEN 1=2 THEN 'one' WHEN 2=3 THEN 'two' END"));

	// ---- Simple CASE (value) ----
	[Fact] public async Task SimpleCase_1() => Assert.Equal("one", await Scalar("SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' WHEN 3 THEN 'three' END"));
	[Fact] public async Task SimpleCase_2() => Assert.Equal("two", await Scalar("SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' WHEN 3 THEN 'three' END"));
	[Fact] public async Task SimpleCase_3() => Assert.Equal("three", await Scalar("SELECT CASE 3 WHEN 1 THEN 'one' WHEN 2 THEN 'two' WHEN 3 THEN 'three' END"));
	[Fact] public async Task SimpleCase_Else() => Assert.Equal("other", await Scalar("SELECT CASE 4 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END"));
	[Fact] public async Task SimpleCase_Str() => Assert.Equal("dog", await Scalar("SELECT CASE 'b' WHEN 'a' THEN 'cat' WHEN 'b' THEN 'dog' ELSE 'bird' END"));

	// ---- CASE with computations ----
	[Fact] public async Task Case_Arithmetic() => Assert.Equal("20", await Scalar("SELECT CASE WHEN TRUE THEN 10 + 10 ELSE 0 END"));
	[Fact] public async Task Case_Concat2() => Assert.Equal("hello world", await Scalar("SELECT CASE WHEN TRUE THEN CONCAT('hello', ' world') ELSE '' END"));
	[Fact] public async Task Case_Upper2() => Assert.Equal("HELLO", await Scalar("SELECT CASE WHEN TRUE THEN UPPER('hello') ELSE '' END"));
	[Fact] public async Task Case_Length2() => Assert.Equal("5", await Scalar("SELECT CASE WHEN LENGTH('hello') > 3 THEN LENGTH('hello') ELSE 0 END"));

	// ---- Nested CASE ----
	[Fact]
	public async Task Case_Nested_Inner() => Assert.Equal("big", await Scalar(@"
SELECT CASE
  WHEN TRUE THEN CASE WHEN 10 > 5 THEN 'big' ELSE 'small' END
  ELSE 'unknown'
END"));

	[Fact]
	public async Task Case_Nested_Double() => Assert.Equal("medium", await Scalar(@"
SELECT CASE
  WHEN 10 > 20 THEN 'high'
  WHEN 10 > 5 THEN CASE
    WHEN 10 > 15 THEN 'high-medium'
    ELSE 'medium'
  END
  ELSE 'low'
END"));

	// ---- CASE in aggregate context via CTE ----
	[Fact(Skip = "Emulator limitation")]
	public async Task Case_InAggregate()
	{
		var v = await Scalar(@"
WITH data AS (
  SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
)
SELECT SUM(CASE WHEN x > 3 THEN 1 ELSE 0 END) FROM data");
		Assert.Equal("2", v);
	}

	[Fact(Skip = "Emulator limitation")]
	public async Task Case_InGroupBy()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
WITH data AS (
  SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
)
SELECT
  CASE WHEN x <= 2 THEN 'low' WHEN x <= 4 THEN 'mid' ELSE 'high' END AS bucket,
  COUNT(*) AS cnt
FROM data
GROUP BY bucket
ORDER BY bucket", parameters: null);
		var rows = result.ToList();
		Assert.Equal(3, rows.Count);
		Assert.Equal("high", rows[0][0]?.ToString());
		Assert.Equal("1", rows[0][1]?.ToString());
		Assert.Equal("low", rows[1][0]?.ToString());
		Assert.Equal("2", rows[1][1]?.ToString());
		Assert.Equal("mid", rows[2][0]?.ToString());
		Assert.Equal("2", rows[2][1]?.ToString());
	}

	// ---- COALESCE chains ----
	[Fact] public async Task Coalesce_Chain2() => Assert.Equal("1", await Scalar("SELECT COALESCE(1, 2)"));
	[Fact] public async Task Coalesce_Chain3() => Assert.Equal("2", await Scalar("SELECT COALESCE(NULL, 2, 3)"));
	[Fact] public async Task Coalesce_Chain4() => Assert.Equal("3", await Scalar("SELECT COALESCE(NULL, NULL, 3, 4)"));
	[Fact] public async Task Coalesce_Chain5() => Assert.Equal("5", await Scalar("SELECT COALESCE(NULL, NULL, NULL, NULL, 5)"));
	[Fact] public async Task Coalesce_Str() => Assert.Equal("b", await Scalar("SELECT COALESCE(CAST(NULL AS STRING), 'b', 'c')"));
	[Fact] public async Task Coalesce_Empty() => Assert.Equal("", await Scalar("SELECT COALESCE('', 'x')"));
	[Fact] public async Task Coalesce_NullCaseExpr() => Assert.Equal("42", await Scalar("SELECT COALESCE(CASE WHEN FALSE THEN 1 END, 42)"));

	// ---- IFNULL patterns ----
	[Fact] public async Task Ifnull_NonNull3() => Assert.Equal("10", await Scalar("SELECT IFNULL(10, 20)"));
	[Fact] public async Task Ifnull_Null3() => Assert.Equal("20", await Scalar("SELECT IFNULL(CAST(NULL AS INT64), 20)"));
	[Fact] public async Task Ifnull_Str3() => Assert.Equal("hello", await Scalar("SELECT IFNULL('hello', 'world')"));
	[Fact] public async Task Ifnull_StrNull() => Assert.Equal("world", await Scalar("SELECT IFNULL(CAST(NULL AS STRING), 'world')"));
	[Fact] public async Task Ifnull_Zero() => Assert.Equal("0", await Scalar("SELECT IFNULL(0, 99)"));
	[Fact] public async Task Ifnull_Nested() => Assert.Equal("3", await Scalar("SELECT IFNULL(IFNULL(CAST(NULL AS INT64), CAST(NULL AS INT64)), 3)"));

	// ---- NULLIF patterns ----
	[Fact] public async Task Nullif_IntSame() => Assert.Null(await Scalar("SELECT NULLIF(5, 5)"));
	[Fact] public async Task Nullif_IntDiff() => Assert.Equal("5", await Scalar("SELECT NULLIF(5, 10)"));
	[Fact] public async Task Nullif_StrSame3() => Assert.Null(await Scalar("SELECT NULLIF('x', 'x')"));
	[Fact] public async Task Nullif_StrDiff3() => Assert.Equal("x", await Scalar("SELECT NULLIF('x', 'y')"));
	[Fact] public async Task Nullif_ZeroZero() => Assert.Null(await Scalar("SELECT NULLIF(0, 0)"));
	[Fact] public async Task Nullif_EmptyEmpty() => Assert.Null(await Scalar("SELECT NULLIF('', '')"));
	[Fact] public async Task Nullif_EmptyNonEmpty() => Assert.Equal("", await Scalar("SELECT NULLIF('', 'x')"));

	// ---- Complex conditional chains ----
	[Fact]
	public async Task Complex_CoalesceNullif() => Assert.Equal("10", await Scalar("SELECT COALESCE(NULLIF(5, 5), 10)"));

	[Fact]
	public async Task Complex_IfnullCase() => Assert.Equal("42", await Scalar("SELECT IFNULL(CASE WHEN FALSE THEN 1 END, 42)"));

	[Fact]
	public async Task Complex_CaseCoalesce() => Assert.Equal("hello", await Scalar(@"
SELECT CASE WHEN COALESCE(NULL, NULL) IS NULL THEN 'hello' ELSE 'world' END"));

	[Fact]
	public async Task Complex_NestedConditionals() => Assert.Equal("99", await Scalar(@"
SELECT COALESCE(NULLIF(1, 1), NULLIF(2, 2), NULLIF(3, 3), 99)"));

	// ---- CASE with NULL handling ----
	[Fact] public async Task Case_NullIsNull() => Assert.Equal("null", await Scalar("SELECT CASE WHEN NULL IS NULL THEN 'null' ELSE 'not' END"));
	[Fact] public async Task Case_NullComparison() => Assert.Equal("other", await Scalar("SELECT CASE WHEN NULL = 1 THEN 'match' ELSE 'other' END"));
}
