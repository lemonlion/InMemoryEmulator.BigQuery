using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for logical expressions, conditional functions, and boolean evaluation patterns.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ConditionalExpressionPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public ConditionalExpressionPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_cond_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.orders` (id INT64, status STRING, amount FLOAT64, discount FLOAT64)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.orders` (id, status, amount, discount) VALUES
			(1, 'completed', 100.0, 10.0),
			(2, 'pending', 200.0, NULL),
			(3, 'cancelled', 50.0, 5.0),
			(4, 'completed', 300.0, 30.0),
			(5, 'pending', 150.0, 15.0),
			(6, 'completed', NULL, NULL),
			(7, 'shipped', 250.0, 25.0)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
		return result.ToList();
	}

	// IF expressions
	[Fact] public async Task If_TrueCondition() => Assert.Equal("yes", await Scalar("SELECT IF(1 = 1, 'yes', 'no')"));
	[Fact] public async Task If_FalseCondition() => Assert.Equal("no", await Scalar("SELECT IF(1 = 2, 'yes', 'no')"));
	[Fact] public async Task If_NullCondition() => Assert.Equal("no", await Scalar("SELECT IF(NULL, 'yes', 'no')"));
	[Fact] public async Task If_WithExpressions() => Assert.Equal("big", await Scalar("SELECT IF(100 > 50, 'big', 'small')"));
	[Fact] public async Task If_InSelect()
	{
		var rows = await Query("SELECT id, IF(status = 'completed', 'done', 'active') AS state FROM `{ds}.orders` WHERE id = 1");
		Assert.Equal("done", rows[0]["state"].ToString());
	}

	// IIF pattern (nested IF)
	[Fact] public async Task If_Nested() => Assert.Equal("medium", await Scalar("SELECT IF(10 > 20, 'high', IF(10 > 5, 'medium', 'low'))"));

	// COALESCE
	[Fact] public async Task Coalesce_FirstNonNull() => Assert.Equal("hello", await Scalar("SELECT COALESCE(NULL, 'hello', 'world')"));
	[Fact] public async Task Coalesce_AllNull() => Assert.Null(await Scalar("SELECT COALESCE(NULL, NULL)"));
	[Fact] public async Task Coalesce_InQuery()
	{
		var rows = await Query("SELECT id, CAST(COALESCE(discount, 0) AS INT64) AS disc FROM `{ds}.orders` WHERE id = 2");
		Assert.Equal("0", rows[0]["disc"].ToString());
	}

	// IFNULL
	[Fact] public async Task IfNull_NullValue() => Assert.Equal("default", await Scalar("SELECT IFNULL(NULL, 'default')"));
	[Fact] public async Task IfNull_NonNull() => Assert.Equal("value", await Scalar("SELECT IFNULL('value', 'default')"));
	[Fact] public async Task IfNull_InExpression()
	{
		var result = await Scalar("SELECT CAST(SUM(IFNULL(discount, 0)) AS INT64) FROM `{ds}.orders`");
		Assert.Equal("85", result); // 10+0+5+30+15+0+25 = 85
	}

	// NULLIF
	[Fact] public async Task NullIf_Equal() => Assert.Null(await Scalar("SELECT NULLIF(5, 5)"));
	[Fact] public async Task NullIf_Different() => Assert.Equal("5", await Scalar("SELECT NULLIF(5, 3)"));
	[Fact] public async Task NullIf_String() => Assert.Null(await Scalar("SELECT NULLIF('abc', 'abc')"));

	// CASE WHEN (simple)
	[Fact] public async Task Case_Simple_Match() => Assert.Equal("one", await Scalar("SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' END"));
	[Fact] public async Task Case_Simple_NoMatch() => Assert.Null(await Scalar("SELECT CASE 5 WHEN 1 THEN 'one' WHEN 2 THEN 'two' END"));
	[Fact] public async Task Case_Simple_Else() => Assert.Equal("other", await Scalar("SELECT CASE 5 WHEN 1 THEN 'one' ELSE 'other' END"));

	// CASE WHEN (searched)
	[Fact] public async Task Case_Searched_FirstMatch() => Assert.Equal("big", await Scalar("SELECT CASE WHEN 100 > 50 THEN 'big' WHEN 100 > 0 THEN 'pos' ELSE 'neg' END"));
	[Fact] public async Task Case_Searched_SecondMatch() => Assert.Equal("pos", await Scalar("SELECT CASE WHEN 30 > 50 THEN 'big' WHEN 30 > 0 THEN 'pos' ELSE 'neg' END"));
	[Fact] public async Task Case_Searched_Else() => Assert.Equal("neg", await Scalar("SELECT CASE WHEN -5 > 50 THEN 'big' WHEN -5 > 0 THEN 'pos' ELSE 'neg' END"));

	// CASE in GROUP BY/aggregation
	[Fact] public async Task Case_InGroupBy()
	{
		var rows = await Query(@"
			SELECT CASE WHEN status = 'completed' THEN 'done' ELSE 'active' END AS grp,
				COUNT(*) AS cnt
			FROM `{ds}.orders` GROUP BY grp ORDER BY grp");
		Assert.Equal(2, rows.Count);
	}

	// CASE with NULL
	[Fact] public async Task Case_NullHandling() => Assert.Equal("null", await Scalar("SELECT CASE WHEN NULL THEN 'yes' ELSE 'null' END"));

	// Boolean in WHERE with AND/OR/NOT
	[Fact] public async Task Where_And()
	{
		var rows = await Query("SELECT id FROM `{ds}.orders` WHERE status = 'completed' AND amount > 100 ORDER BY id");
		Assert.Equal("4", rows[0]["id"].ToString());
	}

	[Fact] public async Task Where_Or()
	{
		var rows = await Query("SELECT id FROM `{ds}.orders` WHERE status = 'cancelled' OR status = 'shipped' ORDER BY id");
		Assert.Equal(2, rows.Count);
	}

	[Fact] public async Task Where_Not()
	{
		var rows = await Query("SELECT id FROM `{ds}.orders` WHERE NOT (status = 'completed') ORDER BY id");
		Assert.Equal(4, rows.Count);
	}

	[Fact] public async Task Where_Complex()
	{
		var rows = await Query("SELECT id FROM `{ds}.orders` WHERE (status = 'completed' OR status = 'shipped') AND amount > 200 ORDER BY id");
		Assert.Equal(2, rows.Count); // id 4 (300) and id 7 (250)
	}

	// Ternary null coalesce (??)
	[Fact] public async Task NullCoalesce_Op() => Assert.Equal("5", await Scalar("SELECT COALESCE(NULL, 5)"));

	// Boolean type handling
	[Fact] public async Task Bool_True() => Assert.Equal("True", await Scalar("SELECT TRUE"));
	[Fact] public async Task Bool_False() => Assert.Equal("False", await Scalar("SELECT FALSE"));
	[Fact] public async Task Bool_AndNull() => Assert.Null(await Scalar("SELECT TRUE AND NULL"));
	[Fact] public async Task Bool_OrNull() => Assert.Equal("True", await Scalar("SELECT TRUE OR NULL"));
	[Fact] public async Task Bool_FalseAndNull() => Assert.Equal("False", await Scalar("SELECT FALSE AND NULL"));
}
