using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Deep integration tests for statistical aggregate functions.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class StatisticalAggregateDeepTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public StatisticalAggregateDeepTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_stat_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();

		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.measurements` (
				id INT64,
				category STRING,
				x FLOAT64,
				y FLOAT64
			)", parameters: null);

		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.measurements` (id, category, x, y) VALUES
			(1, 'A', 1.0, 2.0),
			(2, 'A', 2.0, 4.0),
			(3, 'A', 3.0, 5.0),
			(4, 'A', 4.0, 4.0),
			(5, 'A', 5.0, 5.0),
			(6, 'B', 10.0, 20.0),
			(7, 'B', 20.0, 25.0),
			(8, 'B', 30.0, 35.0),
			(9, 'B', 40.0, 45.0),
			(10, 'B', 50.0, 55.0)", parameters: null);
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

	// ============================================================
	// Basic aggregates on dataset
	// ============================================================

	[Fact] public async Task Count_All() => Assert.Equal("10", await Scalar("SELECT COUNT(*) FROM `{ds}.measurements`"));
	[Fact] public async Task Count_Column() => Assert.Equal("10", await Scalar("SELECT COUNT(x) FROM `{ds}.measurements`"));
	[Fact] public async Task Count_Distinct() => Assert.Equal("2", await Scalar("SELECT COUNT(DISTINCT category) FROM `{ds}.measurements`"));
	[Fact] public async Task Sum_All() => Assert.Equal("165", await Scalar("SELECT CAST(SUM(x) AS INT64) FROM `{ds}.measurements`"));
	[Fact] public async Task Avg_All() => Assert.Equal("16.5", await Scalar("SELECT AVG(x) FROM `{ds}.measurements`"));
	[Fact] public async Task Min_All() => Assert.Equal("1", await Scalar("SELECT MIN(x) FROM `{ds}.measurements`"));
	[Fact] public async Task Max_All() => Assert.Equal("50", await Scalar("SELECT MAX(x) FROM `{ds}.measurements`"));

	// ============================================================
	// Variance and Standard Deviation
	// ============================================================

	[Fact]
	public async Task VarPop()
	{
		var result = await Scalar("SELECT ROUND(VAR_POP(x), 2) FROM `{ds}.measurements` WHERE category = 'A'");
		Assert.Equal("2", result);
	}

	[Fact]
	public async Task VarSamp()
	{
		var result = await Scalar("SELECT ROUND(VAR_SAMP(x), 2) FROM `{ds}.measurements` WHERE category = 'A'");
		Assert.Equal("2.5", result);
	}

	[Fact]
	public async Task StddevPop()
	{
		var result = await Scalar("SELECT ROUND(STDDEV_POP(x), 4) FROM `{ds}.measurements` WHERE category = 'A'");
		Assert.Equal("1.4142", result);
	}

	[Fact]
	public async Task StddevSamp()
	{
		var result = await Scalar("SELECT ROUND(STDDEV_SAMP(x), 4) FROM `{ds}.measurements` WHERE category = 'A'");
		Assert.Equal("1.5811", result);
	}

	[Fact]
	public async Task Variance_SingleValue()
	{
		var result = await Scalar("SELECT VAR_SAMP(x) FROM UNNEST([5.0]) AS x");
		Assert.Null(result); // variance of single value is NULL
	}

	[Fact]
	public async Task Stddev_SingleValue()
	{
		var result = await Scalar("SELECT STDDEV_SAMP(x) FROM UNNEST([5.0]) AS x");
		Assert.Null(result);
	}

	// ============================================================
	// Covariance and Correlation
	// ============================================================

	[Fact]
	public async Task CovarPop()
	{
		var result = await Scalar("SELECT ROUND(COVAR_POP(x, y), 2) FROM `{ds}.measurements` WHERE category = 'B'");
		Assert.NotNull(result);
		var val = double.Parse(result!);
		Assert.True(val > 0);
	}

	[Fact]
	public async Task CovarSamp()
	{
		var result = await Scalar("SELECT ROUND(COVAR_SAMP(x, y), 2) FROM `{ds}.measurements` WHERE category = 'B'");
		Assert.NotNull(result);
		var val = double.Parse(result!);
		Assert.True(val > 0);
	}

	[Fact]
	public async Task Corr_PerfectPositive()
	{
		// x and y perfectly correlated: (1,1),(2,2),(3,3)
		var result = await Scalar("SELECT ROUND(CORR(x, y), 1) FROM (SELECT x, x AS y FROM UNNEST([1.0, 2.0, 3.0]) AS x)");
		Assert.Equal("1", result);
	}

	[Fact]
	public async Task Corr_PerfectNegative()
	{
		var result = await Scalar("SELECT ROUND(CORR(x, y), 1) FROM (SELECT 1.0 AS x, 3.0 AS y UNION ALL SELECT 2.0, 2.0 UNION ALL SELECT 3.0, 1.0)");
		Assert.Equal("-1", result);
	}

	[Fact]
	public async Task Corr_PartialCorrelation()
	{
		var result = await Scalar("SELECT ROUND(CORR(x, y), 2) FROM `{ds}.measurements` WHERE category = 'A'");
		Assert.NotNull(result);
		var val = double.Parse(result!);
		Assert.True(val > 0 && val < 1);
	}

	// ============================================================
	// APPROX functions
	// ============================================================

	[Fact]
	public async Task ApproxCountDistinct()
	{
		var result = await Scalar("SELECT APPROX_COUNT_DISTINCT(category) FROM `{ds}.measurements`");
		Assert.Equal("2", result);
	}

	[Fact]
	public async Task ApproxQuantiles()
	{
		var result = await Scalar("SELECT (APPROX_QUANTILES(x, 4))[OFFSET(2)] FROM `{ds}.measurements`");
		Assert.NotNull(result);
	}

	[Fact]
	public async Task ApproxTopCount()
	{
		var rows = await Query("SELECT APPROX_TOP_COUNT(category, 2) AS top_items FROM `{ds}.measurements`");
		Assert.Single(rows);
	}

	[Fact]
	public async Task ApproxTopSum()
	{
		var rows = await Query("SELECT APPROX_TOP_SUM(category, CAST(x AS INT64), 2) AS top_items FROM `{ds}.measurements`");
		Assert.Single(rows);
	}

	// ============================================================
	// BIT aggregates
	// ============================================================

	[Fact] public async Task BitAnd() => Assert.Equal("0", await Scalar("SELECT BIT_AND(x) FROM UNNEST([6, 4, 2]) AS x"));
	[Fact] public async Task BitOr() => Assert.Equal("7", await Scalar("SELECT BIT_OR(x) FROM UNNEST([1, 2, 4]) AS x"));
	[Fact] public async Task BitXor() => Assert.Equal("7", await Scalar("SELECT BIT_XOR(x) FROM UNNEST([5, 2]) AS x"));

	// ============================================================
	// LOGICAL aggregates
	// ============================================================

	[Fact] public async Task LogicalAnd_AllTrue() => Assert.Equal("True", await Scalar("SELECT LOGICAL_AND(x) FROM UNNEST([TRUE, TRUE, TRUE]) AS x"));
	[Fact] public async Task LogicalAnd_SomeFalse() => Assert.Equal("False", await Scalar("SELECT LOGICAL_AND(x) FROM UNNEST([TRUE, FALSE, TRUE]) AS x"));
	[Fact] public async Task LogicalOr_AllFalse() => Assert.Equal("False", await Scalar("SELECT LOGICAL_OR(x) FROM UNNEST([FALSE, FALSE, FALSE]) AS x"));
	[Fact] public async Task LogicalOr_SomeTrue() => Assert.Equal("True", await Scalar("SELECT LOGICAL_OR(x) FROM UNNEST([FALSE, TRUE, FALSE]) AS x"));

	// ============================================================
	// COUNTIF
	// ============================================================

	[Fact] public async Task CountIf_All() => Assert.Equal("5", await Scalar("SELECT COUNTIF(category = 'A') FROM `{ds}.measurements`"));
	[Fact] public async Task CountIf_None() => Assert.Equal("0", await Scalar("SELECT COUNTIF(category = 'C') FROM `{ds}.measurements`"));
	[Fact] public async Task CountIf_WithCondition() => Assert.Equal("3", await Scalar("SELECT COUNTIF(x > 20) FROM `{ds}.measurements`"));

	// ============================================================
	// STRING_AGG
	// ============================================================

	[Fact]
	public async Task StringAgg_Default()
	{
		var result = await Scalar("SELECT STRING_AGG(CAST(CAST(x AS INT64) AS STRING), ',' ORDER BY x) FROM `{ds}.measurements` WHERE category = 'A'");
		Assert.Equal("1,2,3,4,5", result);
	}

	[Fact]
	public async Task StringAgg_CustomSeparator()
	{
		var result = await Scalar("SELECT STRING_AGG(category, ' | ' ORDER BY category) FROM (SELECT DISTINCT category FROM `{ds}.measurements`)");
		Assert.Equal("A | B", result);
	}

	// ============================================================
	// ARRAY_AGG
	// ============================================================

	[Fact]
	public async Task ArrayAgg_OrderBy()
	{
		var result = await Scalar("SELECT ARRAY_LENGTH(ARRAY_AGG(x ORDER BY x)) FROM `{ds}.measurements` WHERE category = 'A'");
		Assert.Equal("5", result);
	}

	[Fact]
	public async Task ArrayAgg_Distinct()
	{
		var result = await Scalar("SELECT ARRAY_LENGTH(ARRAY_AGG(DISTINCT category)) FROM `{ds}.measurements`");
		Assert.Equal("2", result);
	}

	[Fact]
	public async Task ArrayAgg_IgnoreNulls()
	{
		var result = await Scalar("SELECT ARRAY_LENGTH(ARRAY_AGG(x IGNORE NULLS)) FROM UNNEST([1.0, NULL, 3.0, NULL, 5.0]) AS x");
		Assert.Equal("3", result);
	}

	// ============================================================
	// Grouped aggregates
	// ============================================================

	[Fact]
	public async Task GroupBy_MultipleAggregates()
	{
		var rows = await Query(@"
			SELECT category,
				COUNT(*) AS cnt,
				ROUND(AVG(x), 1) AS avg_x,
				ROUND(STDDEV_POP(x), 2) AS std_x
			FROM `{ds}.measurements`
			GROUP BY category
			ORDER BY category");
		Assert.Equal(2, rows.Count);
		Assert.Equal("A", rows[0]["category"]?.ToString());
		Assert.Equal("5", rows[0]["cnt"]?.ToString());
	}

	[Fact]
	public async Task GroupBy_Having_WithAggregate()
	{
		var rows = await Query(@"
			SELECT category, SUM(x) AS total
			FROM `{ds}.measurements`
			GROUP BY category
			HAVING SUM(x) > 50");
		Assert.Single(rows);
		Assert.Equal("B", rows[0]["category"]?.ToString());
	}

	// ============================================================
	// Aggregate with DISTINCT
	// ============================================================

	[Fact] public async Task SumDistinct() => Assert.Equal("6", await Scalar("SELECT SUM(DISTINCT x) FROM UNNEST([1.0, 2.0, 3.0, 1.0, 2.0]) AS x"));
	[Fact] public async Task AvgDistinct() => Assert.Equal("2", await Scalar("SELECT CAST(AVG(DISTINCT x) AS INT64) FROM UNNEST([1.0, 2.0, 3.0, 1.0, 2.0]) AS x"));

	// ============================================================
	// Aggregates with NULL handling
	// ============================================================

	[Fact] public async Task Avg_IgnoresNull() => Assert.Equal("2", await Scalar("SELECT CAST(AVG(x) AS INT64) FROM UNNEST([1.0, NULL, 3.0]) AS x"));
	[Fact] public async Task Sum_IgnoresNull() => Assert.Equal("4", await Scalar("SELECT CAST(SUM(x) AS INT64) FROM UNNEST([1.0, NULL, 3.0]) AS x"));
	[Fact] public async Task Count_ExcludesNull() => Assert.Equal("2", await Scalar("SELECT COUNT(x) FROM UNNEST([1.0, NULL, 3.0]) AS x"));
	[Fact] public async Task Count_Star_IncludesNull() => Assert.Equal("3", await Scalar("SELECT COUNT(*) FROM UNNEST([1.0, NULL, 3.0]) AS x"));
	[Fact] public async Task Max_WithNulls() => Assert.Equal("3", await Scalar("SELECT CAST(MAX(x) AS INT64) FROM UNNEST([1.0, NULL, 3.0]) AS x"));
	[Fact] public async Task Min_WithNulls() => Assert.Equal("1", await Scalar("SELECT CAST(MIN(x) AS INT64) FROM UNNEST([1.0, NULL, 3.0]) AS x"));

	// ============================================================
	// ANY_VALUE
	// ============================================================

	[Fact]
	public async Task AnyValue()
	{
		var result = await Scalar("SELECT ANY_VALUE(category) FROM `{ds}.measurements`");
		Assert.True(result == "A" || result == "B");
	}

	// ============================================================
	// Aggregate in window context (non-windowed aggregate syntax)
	// ============================================================

	[Fact]
	public async Task Aggregate_EmptySet_Count() => Assert.Equal("0", await Scalar("SELECT COUNT(*) FROM `{ds}.measurements` WHERE 1=0"));
	[Fact] public async Task Aggregate_EmptySet_Sum() => Assert.Null(await Scalar("SELECT SUM(x) FROM `{ds}.measurements` WHERE 1=0"));
	[Fact] public async Task Aggregate_EmptySet_Avg() => Assert.Null(await Scalar("SELECT AVG(x) FROM `{ds}.measurements` WHERE 1=0"));
}
