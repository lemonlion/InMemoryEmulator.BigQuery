using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for CASE expressions: simple, searched, nested, with aggregation.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions#case
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class CaseExpressionAdvancedTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public CaseExpressionAdvancedTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_case_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.orders` (id INT64, status STRING, amount FLOAT64, priority INT64, customer STRING)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.orders` (id, status, amount, priority, customer) VALUES
			(1, 'completed', 150.0, 1, 'Alice'),
			(2, 'pending', 200.0, 2, 'Bob'),
			(3, 'cancelled', 50.0, 3, 'Charlie'),
			(4, 'completed', 300.0, 1, 'Alice'),
			(5, 'pending', 75.0, 2, 'Diana'),
			(6, 'shipped', 125.0, 1, 'Bob'),
			(7, 'completed', 500.0, 1, 'Eve'),
			(8, 'cancelled', 25.0, 3, 'Charlie'),
			(9, 'shipped', 400.0, 2, 'Alice'),
			(10, 'pending', 100.0, 3, 'Frank')", parameters: null);
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

	// Simple CASE
	[Fact] public async Task SimpleCase_Match() => Assert.Equal("Done", await Scalar("SELECT CASE 'completed' WHEN 'completed' THEN 'Done' WHEN 'pending' THEN 'Waiting' ELSE 'Other' END"));
	[Fact] public async Task SimpleCase_NoMatch() => Assert.Equal("Other", await Scalar("SELECT CASE 'unknown' WHEN 'completed' THEN 'Done' WHEN 'pending' THEN 'Waiting' ELSE 'Other' END"));
	[Fact] public async Task SimpleCase_NullInput() => Assert.Equal("Unknown", await Scalar("SELECT CASE NULL WHEN 'a' THEN 'Found' ELSE 'Unknown' END"));
	[Fact] public async Task SimpleCase_MultipleMatches() => Assert.Equal("First", await Scalar("SELECT CASE 1 WHEN 1 THEN 'First' WHEN 1 THEN 'Second' ELSE 'None' END"));
	[Fact] public async Task SimpleCase_NoElse() => Assert.Null(await Scalar("SELECT CASE 'x' WHEN 'a' THEN 1 WHEN 'b' THEN 2 END"));

	// Searched CASE
	[Fact] public async Task SearchedCase_FirstTrue() => Assert.Equal("High", await Scalar("SELECT CASE WHEN 500 > 200 THEN 'High' WHEN 500 > 100 THEN 'Med' ELSE 'Low' END"));
	[Fact] public async Task SearchedCase_SecondTrue() => Assert.Equal("Med", await Scalar("SELECT CASE WHEN 150 > 200 THEN 'High' WHEN 150 > 100 THEN 'Med' ELSE 'Low' END"));
	[Fact] public async Task SearchedCase_Else() => Assert.Equal("Low", await Scalar("SELECT CASE WHEN 50 > 200 THEN 'High' WHEN 50 > 100 THEN 'Med' ELSE 'Low' END"));
	[Fact] public async Task SearchedCase_WithNull() => Assert.Equal("yes", await Scalar("SELECT CASE WHEN NULL THEN 'no' WHEN TRUE THEN 'yes' END"));
	[Fact] public async Task SearchedCase_IsNull() => Assert.Equal("null", await Scalar("SELECT CASE WHEN NULL IS NULL THEN 'null' ELSE 'not null' END"));

	// CASE in SELECT with table
	[Fact] public async Task Case_InSelect()
	{
		var rows = await Query(@"
			SELECT id, CASE status
				WHEN 'completed' THEN 'Done'
				WHEN 'pending' THEN 'Waiting'
				WHEN 'shipped' THEN 'In Transit'
				ELSE 'Other'
			END AS status_label
			FROM `{ds}.orders` ORDER BY id");
		Assert.Equal("Done", rows[0]["status_label"].ToString());
		Assert.Equal("Waiting", rows[1]["status_label"].ToString());
		Assert.Equal("Other", rows[2]["status_label"].ToString());
	}

	// CASE in WHERE
	[Fact] public async Task Case_InWhere()
	{
		var result = await Scalar(@"
			SELECT COUNT(*) FROM `{ds}.orders`
			WHERE CASE WHEN priority = 1 THEN TRUE ELSE FALSE END = TRUE");
		Assert.Equal("4", result);
	}

	// CASE in ORDER BY
	[Fact] public async Task Case_InOrderBy()
	{
		var rows = await Query(@"
			SELECT id, status FROM `{ds}.orders`
			ORDER BY CASE status WHEN 'pending' THEN 1 WHEN 'shipped' THEN 2 WHEN 'completed' THEN 3 ELSE 4 END, id
			LIMIT 3");
		Assert.Equal("pending", rows[0]["status"].ToString());
	}

	// CASE with aggregation
	[Fact] public async Task Case_WithCountIf()
	{
		var result = await Scalar(@"
			SELECT SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) FROM `{ds}.orders`");
		Assert.Equal("3", result);
	}

	[Fact] public async Task Case_CondInSum()
	{
		var result = await Scalar(@"
			SELECT SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) FROM `{ds}.orders`");
		Assert.Equal("950", result); // 150 + 300 + 500
	}

	[Fact] public async Task Case_InGroupBy()
	{
		var rows = await Query(@"
			SELECT CASE WHEN amount > 200 THEN 'High' ELSE 'Low' END AS tier, COUNT(*) AS cnt
			FROM `{ds}.orders`
			GROUP BY CASE WHEN amount > 200 THEN 'High' ELSE 'Low' END
			ORDER BY tier");
		Assert.Equal(2, rows.Count);
	}

	// Nested CASE
	[Fact] public async Task Case_Nested()
	{
		var result = await Scalar(@"
			SELECT CASE
				WHEN status = 'completed' THEN
					CASE WHEN amount > 200 THEN 'BigWin' ELSE 'SmallWin' END
				ELSE 'NotDone'
			END FROM `{ds}.orders` WHERE id = 7");
		Assert.Equal("BigWin", result);
	}

	[Fact] public async Task Case_NestedInElse()
	{
		var result = await Scalar(@"
			SELECT CASE
				WHEN status = 'completed' THEN 'Done'
				ELSE CASE WHEN status = 'pending' THEN 'Wait' ELSE 'Other' END
			END FROM `{ds}.orders` WHERE id = 2");
		Assert.Equal("Wait", result);
	}

	// CASE with functions
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_string
	//   CAST(FLOAT64 AS STRING) preserves decimal notation: 300.0 → "300.0"
	[Fact] public async Task Case_WithConcat()
	{
		var result = await Scalar(@"
			SELECT CASE WHEN amount > 200 THEN CONCAT('HIGH:', CAST(amount AS STRING)) ELSE 'low' END
			FROM `{ds}.orders` WHERE id = 4");
		Assert.Equal("HIGH:300.0", result);
	}

	// CASE with comparison operators
	[Fact] public async Task Case_WithBetween()
	{
		var result = await Scalar(@"
			SELECT CASE WHEN amount BETWEEN 100 AND 200 THEN 'Med' ELSE 'Other' END FROM `{ds}.orders` WHERE id = 1");
		Assert.Equal("Med", result);
	}

	[Fact] public async Task Case_WithIn()
	{
		var result = await Scalar(@"
			SELECT CASE WHEN status IN ('completed', 'shipped') THEN 'Fulfilled' ELSE 'Pending' END FROM `{ds}.orders` WHERE id = 6");
		Assert.Equal("Fulfilled", result);
	}

	[Fact] public async Task Case_WithLike()
	{
		var result = await Scalar(@"
			SELECT CASE WHEN customer LIKE 'A%' THEN 'A-team' ELSE 'Other' END FROM `{ds}.orders` WHERE id = 1");
		Assert.Equal("A-team", result);
	}

	// CASE returning different types
	[Fact] public async Task Case_ReturnInt() => Assert.Equal("1", await Scalar("SELECT CASE WHEN TRUE THEN 1 ELSE 0 END"));
	[Fact] public async Task Case_ReturnFloat() => Assert.Equal("3.14", await Scalar("SELECT CASE WHEN TRUE THEN 3.14 ELSE 0.0 END"));
	[Fact] public async Task Case_ReturnNull() => Assert.Null(await Scalar("SELECT CASE WHEN FALSE THEN 'x' END"));
}
