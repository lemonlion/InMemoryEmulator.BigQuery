using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for comparison operators and predicates: =, !=, <, >, <=, >=, BETWEEN, IN, LIKE, IS NULL.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#comparison_operators
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ComparisonOperatorPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public ComparisonOperatorPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_cmp_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.data` (id INT64, name STRING, value FLOAT64, category STRING, active BOOL)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.data` (id, name, value, category, active) VALUES
			(1, 'Alpha', 10.5, 'A', TRUE),
			(2, 'Beta', 20.0, 'B', FALSE),
			(3, 'Gamma', 30.0, 'A', TRUE),
			(4, 'Delta', NULL, 'B', NULL),
			(5, 'Epsilon', 15.0, 'C', TRUE),
			(6, 'Zeta', 25.0, 'A', FALSE)", parameters: null);
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

	// Equality
	[Fact] public async Task Eq_Int() => Assert.Equal("True", await Scalar("SELECT 5 = 5"));
	[Fact] public async Task Eq_String() => Assert.Equal("True", await Scalar("SELECT 'abc' = 'abc'"));
	[Fact] public async Task Eq_Null() => Assert.Null(await Scalar("SELECT NULL = NULL")); // NULL = NULL → NULL
	[Fact] public async Task Neq_Int() => Assert.Equal("True", await Scalar("SELECT 5 != 3"));
	[Fact] public async Task Neq_IntFalse() => Assert.Equal("False", await Scalar("SELECT 5 != 5"));

	// Less/Greater
	[Fact] public async Task Lt_Int() => Assert.Equal("True", await Scalar("SELECT 3 < 5"));
	[Fact] public async Task Gt_Int() => Assert.Equal("True", await Scalar("SELECT 5 > 3"));
	[Fact] public async Task Lte_Equal() => Assert.Equal("True", await Scalar("SELECT 5 <= 5"));
	[Fact] public async Task Gte_Equal() => Assert.Equal("True", await Scalar("SELECT 5 >= 5"));
	[Fact] public async Task Lt_String() => Assert.Equal("True", await Scalar("SELECT 'abc' < 'abd'"));
	[Fact] public async Task Gt_String() => Assert.Equal("True", await Scalar("SELECT 'z' > 'a'"));

	// NULL comparisons
	[Fact] public async Task Lt_Null() => Assert.Null(await Scalar("SELECT 5 < NULL"));
	[Fact] public async Task Gt_Null() => Assert.Null(await Scalar("SELECT NULL > 3"));
	[Fact] public async Task Eq_NullValue() => Assert.Null(await Scalar("SELECT 5 = NULL"));

	// BETWEEN
	[Fact] public async Task Between_True() => Assert.Equal("True", await Scalar("SELECT 5 BETWEEN 3 AND 7"));
	[Fact] public async Task Between_False() => Assert.Equal("False", await Scalar("SELECT 10 BETWEEN 3 AND 7"));
	[Fact] public async Task Between_Inclusive() => Assert.Equal("True", await Scalar("SELECT 3 BETWEEN 3 AND 7"));
	[Fact] public async Task Between_Strings() => Assert.Equal("True", await Scalar("SELECT 'b' BETWEEN 'a' AND 'c'"));
	[Fact] public async Task NotBetween() => Assert.Equal("True", await Scalar("SELECT 10 NOT BETWEEN 3 AND 7"));
	[Fact] public async Task Between_InWhere()
	{
		var rows = await Query("SELECT name FROM `{ds}.data` WHERE value BETWEEN 15 AND 25 ORDER BY name");
		Assert.Equal(3, rows.Count); // Epsilon(15), Beta(20), Zeta(25)
	}

	// IN
	[Fact] public async Task In_IntTrue() => Assert.Equal("True", await Scalar("SELECT 5 IN (1, 3, 5, 7)"));
	[Fact] public async Task In_IntFalse() => Assert.Equal("False", await Scalar("SELECT 6 IN (1, 3, 5, 7)"));
	[Fact] public async Task In_String() => Assert.Equal("True", await Scalar("SELECT 'b' IN ('a', 'b', 'c')"));
	[Fact] public async Task NotIn() => Assert.Equal("True", await Scalar("SELECT 6 NOT IN (1, 3, 5, 7)"));
	[Fact] public async Task In_WithNull() => Assert.Null(await Scalar("SELECT 6 IN (1, NULL, 5)"));
	[Fact] public async Task In_InWhere()
	{
		var rows = await Query("SELECT name FROM `{ds}.data` WHERE category IN ('A', 'C') ORDER BY name");
		Assert.Equal(4, rows.Count); // Alpha, Epsilon, Gamma, Zeta
	}

	// LIKE
	[Fact] public async Task Like_Percent() => Assert.Equal("True", await Scalar("SELECT 'hello world' LIKE '%world'"));
	[Fact] public async Task Like_PercentBoth() => Assert.Equal("True", await Scalar("SELECT 'hello world' LIKE '%llo wo%'"));
	[Fact] public async Task Like_Underscore() => Assert.Equal("True", await Scalar("SELECT 'abc' LIKE 'a_c'"));
	[Fact] public async Task Like_Exact() => Assert.Equal("True", await Scalar("SELECT 'abc' LIKE 'abc'"));
	[Fact] public async Task Like_False() => Assert.Equal("False", await Scalar("SELECT 'abc' LIKE 'xyz'"));
	[Fact] public async Task Like_CaseSensitive() => Assert.Equal("False", await Scalar("SELECT 'ABC' LIKE 'abc'"));
	[Fact] public async Task NotLike() => Assert.Equal("True", await Scalar("SELECT 'abc' NOT LIKE 'xyz'"));
	[Fact] public async Task Like_InWhere()
	{
		var rows = await Query("SELECT name FROM `{ds}.data` WHERE name LIKE '%a'");
		Assert.Equal(5, rows.Count); // Alpha, Beta, Gamma, Delta, Zeta (end with 'a')
	}

	// IS NULL / IS NOT NULL
	[Fact] public async Task IsNull_True() => Assert.Equal("True", await Scalar("SELECT NULL IS NULL"));
	[Fact] public async Task IsNull_False() => Assert.Equal("False", await Scalar("SELECT 5 IS NULL"));
	[Fact] public async Task IsNotNull_True() => Assert.Equal("True", await Scalar("SELECT 5 IS NOT NULL"));
	[Fact] public async Task IsNotNull_False() => Assert.Equal("False", await Scalar("SELECT NULL IS NOT NULL"));
	[Fact] public async Task IsNull_InWhere()
	{
		var rows = await Query("SELECT name FROM `{ds}.data` WHERE value IS NULL");
		Assert.Single(rows);
		Assert.Equal("Delta", rows[0]["name"].ToString());
	}
	[Fact] public async Task IsNotNull_InWhere()
	{
		var rows = await Query("SELECT name FROM `{ds}.data` WHERE value IS NOT NULL ORDER BY name");
		Assert.Equal(5, rows.Count);
	}

	// Mixed type comparisons
	[Fact] public async Task Compare_IntFloat() => Assert.Equal("True", await Scalar("SELECT 5 = 5.0"));
	[Fact] public async Task Compare_IntFloat_Lt() => Assert.Equal("True", await Scalar("SELECT 3 < 3.5"));

	// Boolean operations
	[Fact] public async Task And_True() => Assert.Equal("True", await Scalar("SELECT TRUE AND TRUE"));
	[Fact] public async Task And_False() => Assert.Equal("False", await Scalar("SELECT TRUE AND FALSE"));
	[Fact] public async Task Or_True() => Assert.Equal("True", await Scalar("SELECT FALSE OR TRUE"));
	[Fact] public async Task Or_False() => Assert.Equal("False", await Scalar("SELECT FALSE OR FALSE"));
	[Fact] public async Task Not_True() => Assert.Equal("False", await Scalar("SELECT NOT TRUE"));
	[Fact] public async Task Not_False() => Assert.Equal("True", await Scalar("SELECT NOT FALSE"));

	// Ternary (null coalescing)
	[Fact] public async Task Coalesce_First() => Assert.Equal("5", await Scalar("SELECT COALESCE(5, 10)"));
	[Fact] public async Task Coalesce_Second() => Assert.Equal("10", await Scalar("SELECT COALESCE(NULL, 10)"));
	[Fact] public async Task Coalesce_Third() => Assert.Equal("15", await Scalar("SELECT COALESCE(NULL, NULL, 15)"));
	[Fact] public async Task Coalesce_AllNull() => Assert.Null(await Scalar("SELECT COALESCE(NULL, NULL, NULL)"));
	[Fact] public async Task IfNull_NonNull() => Assert.Equal("5", await Scalar("SELECT IFNULL(5, 10)"));
	[Fact] public async Task IfNull_Null() => Assert.Equal("10", await Scalar("SELECT IFNULL(NULL, 10)"));
	[Fact] public async Task NullIf_Equal() => Assert.Null(await Scalar("SELECT NULLIF(5, 5)"));
	[Fact] public async Task NullIf_NotEqual() => Assert.Equal("5", await Scalar("SELECT NULLIF(5, 3)"));

	// IF function
	[Fact] public async Task If_True() => Assert.Equal("yes", await Scalar("SELECT IF(TRUE, 'yes', 'no')"));
	[Fact] public async Task If_False() => Assert.Equal("no", await Scalar("SELECT IF(FALSE, 'yes', 'no')"));
	[Fact] public async Task If_Expression() => Assert.Equal("big", await Scalar("SELECT IF(10 > 5, 'big', 'small')"));
}