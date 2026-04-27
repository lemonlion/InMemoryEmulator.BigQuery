using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration.Phase26;

/// <summary>
/// Integration tests for Phase 26: Aggregate functions.
/// BIT_AND/OR/XOR, CORR, COVAR_POP, COVAR_SAMP, APPROX_QUANTILES, APPROX_TOP_COUNT, APPROX_TOP_SUM.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class AggregateFunctionIntegrationTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public AggregateFunctionIntegrationTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_p26a_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();

		// pairs table for CORR / COVAR tests
		var pairsSchema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "x", Type = "INTEGER", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "y", Type = "INTEGER", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "pairs", pairsSchema);
		await client.InsertRowsAsync(_datasetId, "pairs", new[]
		{
			new BigQueryInsertRow("r1") { ["x"] = 1, ["y"] = 2 },
			new BigQueryInsertRow("r2") { ["x"] = 2, ["y"] = 4 },
			new BigQueryInsertRow("r3") { ["x"] = 3, ["y"] = 6 },
		});

		// weighted table for APPROX_TOP_SUM
		var weightedSchema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "label", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "w", Type = "INTEGER", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "weighted", weightedSchema);
		await client.InsertRowsAsync(_datasetId, "weighted", new[]
		{
			new BigQueryInsertRow("r1") { ["label"] = "a", ["w"] = 10 },
			new BigQueryInsertRow("r2") { ["label"] = "b", ["w"] = 20 },
			new BigQueryInsertRow("r3") { ["label"] = "a", ["w"] = 30 },
			new BigQueryInsertRow("r4") { ["label"] = "c", ["w"] = 5 },
		});
	}

	public async ValueTask DisposeAsync()
	{
		try
		{
			var client = await _fixture.GetClientAsync();
			await client.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true });
		}
		catch { }
		await _fixture.DisposeAsync();
	}

	#region BIT_AND / BIT_OR / BIT_XOR

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#bit_and
	[Fact]
	public async Task BitAnd_AllBitsSet()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT BIT_AND(x) AS result FROM UNNEST([7, 5, 3]) AS x", parameters: null);
		Assert.Equal(1L, Convert.ToInt64(results.ToList()[0]["result"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#bit_or
	[Fact]
	public async Task BitOr_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT BIT_OR(x) AS result FROM UNNEST([61441, 161]) AS x", parameters: null);
		Assert.Equal(61601L, Convert.ToInt64(results.ToList()[0]["result"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#bit_xor
	[Fact]
	public async Task BitXor_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT BIT_XOR(x) AS result FROM UNNEST([5678, 1234]) AS x", parameters: null);
		Assert.Equal(4860L, Convert.ToInt64(results.ToList()[0]["result"]));
	}

	#endregion

	#region CORR

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#corr
	[Fact]
	public async Task Corr_PerfectPositive()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT CORR(x, y) AS result FROM `{_datasetId}.pairs`", parameters: null);
		Assert.Equal(1.0, Convert.ToDouble(results.ToList()[0]["result"]), 5);
	}

	#endregion

	#region COVAR_POP / COVAR_SAMP

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#covar_pop
	[Fact]
	public async Task CovarPop_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT COVAR_POP(x, y) AS result FROM `{_datasetId}.pairs`", parameters: null);
		Assert.Equal(4.0 / 3, Convert.ToDouble(results.ToList()[0]["result"]), 5);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#covar_samp
	[Fact]
	public async Task CovarSamp_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT COVAR_SAMP(x, y) AS result FROM `{_datasetId}.pairs`", parameters: null);
		Assert.Equal(2.0, Convert.ToDouble(results.ToList()[0]["result"]), 5);
	}

	#endregion

	#region APPROX_QUANTILES

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/approximate_aggregate_functions#approx_quantiles
	[Fact]
	public async Task ApproxQuantiles_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT ARRAY_LENGTH(APPROX_QUANTILES(x, 2)) AS len FROM UNNEST([1, 2, 3, 4, 5]) AS x",
			parameters: null);
		// 2 quantiles → 3 boundaries
		Assert.Equal(3L, Convert.ToInt64(results.ToList()[0]["len"]));
	}

	#endregion

	#region APPROX_TOP_COUNT

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/approximate_aggregate_functions#approx_top_count
	[Fact]
	public async Task ApproxTopCount_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT ARRAY_LENGTH(APPROX_TOP_COUNT(x, 2)) AS len FROM UNNEST(['apple', 'apple', 'pear', 'pear', 'pear', 'banana']) AS x",
			parameters: null);
		Assert.Equal(2L, Convert.ToInt64(results.ToList()[0]["len"]));
	}

	#endregion

	#region APPROX_TOP_SUM

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/approximate_aggregate_functions#approx_top_sum
	[Fact]
	public async Task ApproxTopSum_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT ARRAY_LENGTH(APPROX_TOP_SUM(label, w, 2)) AS len FROM `{_datasetId}.weighted`",
			parameters: null);
		Assert.Equal(2L, Convert.ToInt64(results.ToList()[0]["len"]));
	}

	#endregion
}
