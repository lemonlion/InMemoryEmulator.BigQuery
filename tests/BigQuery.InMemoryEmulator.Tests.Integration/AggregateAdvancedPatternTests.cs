using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for aggregate functions including SUM, AVG, COUNT, MIN, MAX, and advanced aggregation patterns.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class AggregateAdvancedPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public AggregateAdvancedPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_agg_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.sales` (id INT64, region STRING, product STRING, amount FLOAT64, qty INT64)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.sales` (id, region, product, amount, qty) VALUES
			(1, 'East', 'Widget', 100.0, 10),
			(2, 'East', 'Gadget', 200.0, 5),
			(3, 'West', 'Widget', 150.0, 8),
			(4, 'West', 'Gadget', 300.0, 12),
			(5, 'East', 'Widget', 175.0, 15),
			(6, 'West', 'Widget', 125.0, 6),
			(7, 'North', 'Gadget', 250.0, 9),
			(8, 'North', 'Widget', 180.0, 11),
			(9, 'East', 'Gadget', 220.0, 7),
			(10, 'North', 'Gadget', NULL, 4)", parameters: null);
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

	// Basic aggregates
	[Fact] public async Task Sum_All() => Assert.Equal("1700", await Scalar("SELECT CAST(SUM(amount) AS INT64) FROM `{ds}.sales`"));
	[Fact] public async Task Avg_All()
	{
		var v = double.Parse((await Scalar("SELECT AVG(amount) FROM `{ds}.sales`"))!);
		Assert.True(v > 188 && v < 190); // 1700/9 ≈ 188.89
	}
	[Fact] public async Task Count_All() => Assert.Equal("10", await Scalar("SELECT COUNT(*) FROM `{ds}.sales`"));
	[Fact] public async Task Count_Column() => Assert.Equal("9", await Scalar("SELECT COUNT(amount) FROM `{ds}.sales`")); // excludes NULL
	[Fact] public async Task Min_Amount() => Assert.Equal("100", await Scalar("SELECT CAST(MIN(amount) AS INT64) FROM `{ds}.sales`"));
	[Fact] public async Task Max_Amount() => Assert.Equal("300", await Scalar("SELECT CAST(MAX(amount) AS INT64) FROM `{ds}.sales`"));
	[Fact] public async Task Min_String() => Assert.Equal("East", await Scalar("SELECT MIN(region) FROM `{ds}.sales`"));
	[Fact] public async Task Max_String() => Assert.Equal("West", await Scalar("SELECT MAX(region) FROM `{ds}.sales`"));
	[Fact] public async Task Sum_WithNull() => Assert.Equal("1700", await Scalar("SELECT CAST(SUM(amount) AS INT64) FROM `{ds}.sales`")); // NULL ignored

	// GROUP BY patterns
	[Fact] public async Task GroupBy_Count()
	{
		var rows = await Query("SELECT region, COUNT(*) AS cnt FROM `{ds}.sales` GROUP BY region ORDER BY region");
		Assert.Equal(3, rows.Count);
		Assert.Equal("4", rows[0]["cnt"].ToString()); // East: 4 rows
	}

	[Fact] public async Task GroupBy_Sum()
	{
		var rows = await Query("SELECT product, CAST(SUM(amount) AS INT64) AS total FROM `{ds}.sales` GROUP BY product ORDER BY product");
		Assert.Equal("Gadget", rows[0]["product"].ToString());
	}

	[Fact] public async Task GroupBy_MultiColumn()
	{
		var rows = await Query("SELECT region, product, COUNT(*) AS cnt FROM `{ds}.sales` GROUP BY region, product ORDER BY region, product");
		Assert.True(rows.Count >= 5);
	}

	[Fact] public async Task GroupBy_Having()
	{
		var rows = await Query("SELECT region, COUNT(*) AS cnt FROM `{ds}.sales` GROUP BY region HAVING COUNT(*) >= 4 ORDER BY region");
		Assert.Equal("East", rows[0]["region"].ToString());
	}

	[Fact] public async Task GroupBy_HavingSum()
	{
		var rows = await Query("SELECT product, CAST(SUM(amount) AS INT64) AS total FROM `{ds}.sales` GROUP BY product HAVING SUM(amount) > 700 ORDER BY product");
		Assert.True(rows.Count >= 1);
	}

	// COUNT DISTINCT
	[Fact] public async Task CountDistinct_Region() => Assert.Equal("3", await Scalar("SELECT COUNT(DISTINCT region) FROM `{ds}.sales`"));
	[Fact] public async Task CountDistinct_Product() => Assert.Equal("2", await Scalar("SELECT COUNT(DISTINCT product) FROM `{ds}.sales`"));
	[Fact] public async Task CountDistinct_PerGroup()
	{
		var rows = await Query("SELECT region, COUNT(DISTINCT product) AS dp FROM `{ds}.sales` GROUP BY region ORDER BY region");
		Assert.Equal("2", rows[0]["dp"].ToString()); // East has Widget and Gadget
	}

	// ANY_VALUE
	[Fact] public async Task AnyValue_ReturnsAValue()
	{
		var result = await Scalar("SELECT ANY_VALUE(region) FROM `{ds}.sales`");
		Assert.NotNull(result);
		Assert.Contains(result!, new[] { "East", "West", "North" });
	}

	// COUNTIF
	[Fact] public async Task CountIf_Basic() => Assert.Equal("4", await Scalar("SELECT COUNTIF(region = 'East') FROM `{ds}.sales`"));
	[Fact] public async Task CountIf_Expression() => Assert.Equal("6", await Scalar("SELECT COUNTIF(amount > 150) FROM `{ds}.sales`"));

	// ARRAY_AGG
	[Fact] public async Task ArrayAgg_Basic()
	{
		var result = await Scalar("SELECT ARRAY_LENGTH(ARRAY_AGG(region)) FROM `{ds}.sales`");
		Assert.Equal("10", result);
	}

	// SUM/AVG with expressions
	[Fact] public async Task Sum_Expression() => Assert.Equal("87", await Scalar("SELECT SUM(qty) FROM `{ds}.sales`"));
	[Fact] public async Task Sum_Multiply()
	{
		var result = await Scalar("SELECT CAST(SUM(amount * qty) AS INT64) FROM `{ds}.sales`");
		Assert.NotNull(result);
	}

	// Aggregate with CASE
	[Fact] public async Task Sum_WithCase()
	{
		var result = await Scalar("SELECT CAST(SUM(CASE WHEN region = 'East' THEN amount ELSE 0 END) AS INT64) FROM `{ds}.sales`");
		Assert.Equal("695", result);
	}

	// Aggregate with WHERE
	[Fact] public async Task Sum_WithWhere() => Assert.Equal("695", await Scalar("SELECT CAST(SUM(amount) AS INT64) FROM `{ds}.sales` WHERE region = 'East'"));
	[Fact] public async Task Count_WithWhere() => Assert.Equal("4", await Scalar("SELECT COUNT(*) FROM `{ds}.sales` WHERE region = 'East'"));
	[Fact] public async Task Avg_WithWhere()
	{
		var v = double.Parse((await Scalar("SELECT AVG(amount) FROM `{ds}.sales` WHERE product = 'Widget'"))!);
		Assert.True(v > 145 && v < 147); // (100+150+175+125+180)/5 = 146
	}

	// Aggregate with NULL
	[Fact] public async Task Sum_NullExcluded() => Assert.Equal("1700", await Scalar("SELECT CAST(SUM(amount) AS INT64) FROM `{ds}.sales`"));
	[Fact] public async Task Avg_NullExcluded()
	{
		var v = double.Parse((await Scalar("SELECT AVG(amount) FROM `{ds}.sales`"))!);
		Assert.True(v > 188 && v < 190); // 1700/9 (null excluded)
	}

	// Empty result aggregates
	[Fact] public async Task Sum_Empty() => Assert.Null(await Scalar("SELECT SUM(amount) FROM `{ds}.sales` WHERE region = 'South'"));
	[Fact] public async Task Count_Empty() => Assert.Equal("0", await Scalar("SELECT COUNT(*) FROM `{ds}.sales` WHERE region = 'South'"));
	[Fact] public async Task Avg_Empty() => Assert.Null(await Scalar("SELECT AVG(amount) FROM `{ds}.sales` WHERE region = 'South'"));
	[Fact] public async Task Min_Empty() => Assert.Null(await Scalar("SELECT MIN(amount) FROM `{ds}.sales` WHERE region = 'South'"));
	[Fact] public async Task Max_Empty() => Assert.Null(await Scalar("SELECT MAX(amount) FROM `{ds}.sales` WHERE region = 'South'"));

	// LOGICAL_AND / LOGICAL_OR
	[Fact] public async Task LogicalAnd_True() => Assert.Equal("True", await Scalar("SELECT LOGICAL_AND(qty > 0) FROM `{ds}.sales`"));
	[Fact] public async Task LogicalAnd_False() => Assert.Equal("False", await Scalar("SELECT LOGICAL_AND(qty > 10) FROM `{ds}.sales`"));
	[Fact] public async Task LogicalOr_True() => Assert.Equal("True", await Scalar("SELECT LOGICAL_OR(qty > 14) FROM `{ds}.sales`"));

	// STRING_AGG
	[Fact] public async Task StringAgg_Basic()
	{
		var result = await Scalar("SELECT STRING_AGG(product, ',' ORDER BY product) FROM `{ds}.sales`");
		Assert.Contains("Widget", result!);
	}

	// Aggregates with ORDER BY
	[Fact] public async Task GroupBy_OrderByAggregate()
	{
		var rows = await Query("SELECT region, CAST(SUM(amount) AS INT64) AS total FROM `{ds}.sales` GROUP BY region ORDER BY total DESC");
		Assert.True(rows.Count == 3);
	}

	// Nested aggregates in HAVING
	[Fact] public async Task Having_AvgCondition()
	{
		var rows = await Query("SELECT region FROM `{ds}.sales` GROUP BY region HAVING AVG(amount) > 200 ORDER BY region");
		Assert.True(rows.Count >= 1);
	}

	// APPROX_COUNT_DISTINCT
	[Fact] public async Task ApproxCountDistinct()
	{
		var result = await Scalar("SELECT APPROX_COUNT_DISTINCT(region) FROM `{ds}.sales`");
		Assert.Equal("3", result);
	}
}