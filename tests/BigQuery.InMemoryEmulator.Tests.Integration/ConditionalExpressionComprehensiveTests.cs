using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Comprehensive tests for CASE expressions, IF, IIF, COALESCE, NULLIF, IFNULL, GREATEST, LEAST.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ConditionalExpressionComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public ConditionalExpressionComprehensiveTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql, parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }

	// ---- CASE WHEN ... ----
	[Fact] public async Task Case_SingleWhen() => Assert.Equal("one", await S("SELECT CASE WHEN 1 = 1 THEN 'one' END"));
	[Fact] public async Task Case_MultiWhen() => Assert.Equal("two", await S("SELECT CASE WHEN 1 = 2 THEN 'one' WHEN 2 = 2 THEN 'two' ELSE 'other' END"));
	[Fact] public async Task Case_Else() => Assert.Equal("other", await S("SELECT CASE WHEN 1 = 2 THEN 'one' ELSE 'other' END"));
	[Fact] public async Task Case_NoElse() => Assert.Null(await S("SELECT CASE WHEN 1 = 2 THEN 'one' END"));
	[Fact] public async Task Case_SimpleForm() => Assert.Equal("yes", await S("SELECT CASE 1 WHEN 1 THEN 'yes' WHEN 2 THEN 'no' END"));
	[Fact] public async Task Case_SimpleElse() => Assert.Equal("other", await S("SELECT CASE 3 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END"));
	[Fact] public async Task Case_Nested() => Assert.Equal("deep", await S("SELECT CASE WHEN 1=1 THEN CASE WHEN 2=2 THEN 'deep' ELSE 'shallow' END END"));
	[Fact] public async Task Case_WithExpr() => Assert.Equal("big", await S("SELECT CASE WHEN 100 > 50 THEN 'big' ELSE 'small' END"));
	[Fact] public async Task Case_InSelect() => Assert.Equal("3", await S("SELECT CASE WHEN 1=1 THEN 1+2 ELSE 0 END"));
	[Fact] public async Task Case_WithNull() => Assert.Equal("null_val", await S("SELECT CASE WHEN NULL IS NULL THEN 'null_val' ELSE 'not_null' END"));

	// ---- IF ----
	[Fact] public async Task If_True() => Assert.Equal("yes", await S("SELECT IF(true, 'yes', 'no')"));
	[Fact] public async Task If_False() => Assert.Equal("no", await S("SELECT IF(false, 'yes', 'no')"));
	[Fact] public async Task If_Expr() => Assert.Equal("big", await S("SELECT IF(10 > 5, 'big', 'small')"));
	[Fact] public async Task If_NullCondition() => Assert.Equal("no", await S("SELECT IF(NULL, 'yes', 'no')"));
	[Fact] public async Task If_Nested() => Assert.Equal("c", await S("SELECT IF(false, 'a', IF(false, 'b', 'c'))"));
	[Fact] public async Task If_WithCalc() => Assert.Equal("6", await S("SELECT IF(true, 2*3, 0)"));

	// ---- IIF ----
	[Fact] public async Task Iif_True() => Assert.Equal("yes", await S("SELECT IIF(true, 'yes', 'no')"));
	[Fact] public async Task Iif_False() => Assert.Equal("no", await S("SELECT IIF(false, 'yes', 'no')"));

	// ---- COALESCE ----
	[Fact] public async Task Coalesce_FirstNonNull() => Assert.Equal("a", await S("SELECT COALESCE('a', 'b', 'c')"));
	[Fact] public async Task Coalesce_SkipNull() => Assert.Equal("b", await S("SELECT COALESCE(NULL, 'b', 'c')"));
	[Fact] public async Task Coalesce_AllNull() => Assert.Null(await S("SELECT COALESCE(CAST(NULL AS STRING), CAST(NULL AS STRING))"));
	[Fact] public async Task Coalesce_SingleArg() => Assert.Equal("x", await S("SELECT COALESCE('x')"));
	[Fact] public async Task Coalesce_ThreeNulls() => Assert.Equal("d", await S("SELECT COALESCE(NULL, NULL, NULL, 'd')"));

	// ---- IFNULL ----
	[Fact] public async Task Ifnull_NonNull() => Assert.Equal("a", await S("SELECT IFNULL('a', 'b')"));
	[Fact] public async Task Ifnull_Null() => Assert.Equal("b", await S("SELECT IFNULL(CAST(NULL AS STRING), 'b')"));
	[Fact] public async Task Ifnull_BothNull() => Assert.Null(await S("SELECT IFNULL(CAST(NULL AS STRING), CAST(NULL AS STRING))"));

	// ---- NULLIF ----
	[Fact] public async Task Nullif_Equal() => Assert.Null(await S("SELECT NULLIF(1, 1)"));
	[Fact] public async Task Nullif_NotEqual() => Assert.Equal("1", await S("SELECT NULLIF(1, 2)"));
	[Fact] public async Task Nullif_String() => Assert.Null(await S("SELECT NULLIF('a', 'a')"));
	[Fact] public async Task Nullif_StringDiff() => Assert.Equal("a", await S("SELECT NULLIF('a', 'b')"));

	// ---- GREATEST / LEAST ----
	[Fact] public async Task Greatest_Ints() => Assert.Equal("5", await S("SELECT GREATEST(1, 3, 5, 2, 4)"));
	[Fact] public async Task Least_Ints() => Assert.Equal("1", await S("SELECT LEAST(1, 3, 5, 2, 4)"));
	[Fact] public async Task Greatest_Strings() => Assert.Equal("c", await S("SELECT GREATEST('a', 'c', 'b')"));
	[Fact] public async Task Least_Strings() => Assert.Equal("a", await S("SELECT LEAST('a', 'c', 'b')"));
	[Fact] public async Task Greatest_WithNull() => Assert.Null(await S("SELECT GREATEST(1, NULL, 3)"));
	[Fact] public async Task Least_WithNull() => Assert.Null(await S("SELECT LEAST(1, NULL, 3)"));
	[Fact] public async Task Greatest_Two() => Assert.Equal("10", await S("SELECT GREATEST(5, 10)"));
	[Fact] public async Task Least_Two() => Assert.Equal("5", await S("SELECT LEAST(5, 10)"));
	[Fact] public async Task Greatest_Negative() => Assert.Equal("3", await S("SELECT GREATEST(-1, 3, -5)"));
	[Fact] public async Task Least_Negative() => Assert.Equal("-5", await S("SELECT LEAST(-1, 3, -5)"));

	// ---- Combined expressions ----
	[Fact] public async Task Combined_IfCoalesce() => Assert.Equal("default", await S("SELECT IF(COALESCE(NULL, NULL) IS NULL, 'default', 'value')"));
	[Fact] public async Task Combined_CaseIf() => Assert.Equal("yes", await S("SELECT CASE WHEN IF(true, 1, 0) = 1 THEN 'yes' ELSE 'no' END"));
	[Fact] public async Task Combined_CoalesceNullif() => Assert.Equal("b", await S("SELECT COALESCE(NULLIF('a', 'a'), 'b')"));
	[Fact] public async Task Combined_IfnullGreatest() => Assert.Equal("10", await S("SELECT IFNULL(GREATEST(5, 10), 0)"));
	[Fact] public async Task Combined_NullIfLeast() => Assert.Null(await S("SELECT NULLIF(LEAST(5, 5), 5)"));
	[Fact] public async Task Combined_NestedCoalesce() => Assert.Equal("x", await S("SELECT COALESCE(COALESCE(NULL, NULL), COALESCE(NULL, 'x'))"));
}
