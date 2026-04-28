using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Phase 30 integration tests: Vector/distance functions (DOT_PRODUCT, APPROX_*),
/// DCL stubs (GRANT/REVOKE), statement stubs (EXPORT DATA, LOAD DATA),
/// BEGIN...EXCEPTION...END, templated UDF args (ANY TYPE).
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class Phase30Tests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public Phase30Tests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_p30_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
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

	#region DOT_PRODUCT

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/search_functions
	//   DOT_PRODUCT computes the inner product of two vectors.
	[Fact]
	public async Task DotProduct_KnownValues()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT DOT_PRODUCT([1.0, 2.0, 3.0], [4.0, 5.0, 6.0]) AS d", null);
		var val = Convert.ToDouble(result.First()["d"]);
		Assert.Equal(32.0, val, 1);
	}

	[Fact]
	public async Task DotProduct_OrthogonalVectors_ReturnsZero()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT DOT_PRODUCT([1.0, 0.0], [0.0, 1.0]) AS d", null);
		var val = Convert.ToDouble(result.First()["d"]);
		Assert.Equal(0.0, val, 10);
	}

	#endregion

	#region APPROX vector functions

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/search_functions
	//   APPROX_COSINE_DISTANCE approximates cosine distance between vectors.
	[Fact]
	public async Task ApproxCosineDistance_IdenticalVectors_ReturnsZero()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT APPROX_COSINE_DISTANCE([1.0, 0.0], [1.0, 0.0]) AS d", null);
		var val = Convert.ToDouble(result.First()["d"]);
		Assert.Equal(0.0, val, 5);
	}

	// Ref: APPROX_EUCLIDEAN_DISTANCE approximates Euclidean distance.
	[Fact]
	public async Task ApproxEuclideanDistance_KnownValues()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT APPROX_EUCLIDEAN_DISTANCE([0.0, 0.0], [3.0, 4.0]) AS d", null);
		var val = Convert.ToDouble(result.First()["d"]);
		Assert.Equal(5.0, val, 5);
	}

	// Ref: APPROX_DOT_PRODUCT approximates dot product.
	[Fact]
	public async Task ApproxDotProduct_KnownValues()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT APPROX_DOT_PRODUCT([1.0, 2.0, 3.0], [4.0, 5.0, 6.0]) AS d", null);
		var val = Convert.ToDouble(result.First()["d"]);
		Assert.Equal(32.0, val, 1);
	}

	#endregion

	#region DCL: GRANT / REVOKE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-control-language
	//   "GRANT statement grants permissions to users, groups, or service accounts."
	[Fact]
	public async Task Grant_NoOp_DoesNotThrow()
	{
		var client = await _fixture.GetClientAsync();
		// Should not throw
		await client.ExecuteQueryAsync(
			$"GRANT `roles/bigquery.dataViewer` ON SCHEMA `{_datasetId}` TO 'user:test@example.com'",
			null);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-control-language
	//   "REVOKE statement revokes previously granted permissions."
	[Fact]
	public async Task Revoke_NoOp_DoesNotThrow()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"REVOKE `roles/bigquery.dataViewer` ON SCHEMA `{_datasetId}` FROM 'user:test@example.com'",
			null);
	}

	#endregion

	#region EXPORT DATA / LOAD DATA

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/other-statements#export_data_statement
	//   "EXPORT DATA statement exports the results of a query to an external data source."
	[Fact]
	public async Task ExportData_NoOp_DoesNotThrow()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			"EXPORT DATA OPTIONS(uri='gs://bucket/path/*', format='CSV') AS SELECT 1 AS x",
			null);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/other-statements#load_data_statement
	//   "LOAD DATA statement loads data from external sources."
	[Fact]
	public async Task LoadData_NoOp_DoesNotThrow()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"LOAD DATA INTO `{_datasetId}.target_table` FROM FILES(format='CSV', uris=['gs://bucket/file.csv'])",
			null);
	}

	#endregion

	#region BEGIN...EXCEPTION...END

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#beginexceptionend
	//   "Catches errors raised in the BEGIN block and executes the EXCEPTION block."
	[Fact]
	public async Task BeginExceptionEnd_CatchesError()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
DECLARE x INT64 DEFAULT 0;
BEGIN
  SELECT ERROR('test error');
EXCEPTION WHEN ERROR THEN
  SET x = -1;
END;
SELECT x AS result;
", null);
		Assert.Equal("-1", result.First()["result"]?.ToString());
	}

	[Fact]
	public async Task BeginExceptionEnd_ErrorMessage_Available()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
DECLARE msg STRING DEFAULT '';
BEGIN
  SELECT ERROR('custom error');
EXCEPTION WHEN ERROR THEN
  SET msg = @@error.message;
END;
SELECT msg AS result;
", null);
		Assert.Contains("custom error", result.First()["result"]?.ToString()!);
	}

	#endregion

	#region Templated UDF (ANY TYPE)

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions#templated-sql-udf-parameters
	//   "A templated parameter can match more than one argument type at function call time."
	[Fact]
	public async Task AnyType_SqlUdf_WithInt64()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
CREATE TEMP FUNCTION identity(x ANY TYPE) AS (x);
SELECT identity(42) AS result;
", null);
		Assert.Equal("42", result.First()["result"]?.ToString());
	}

	[Fact]
	public async Task AnyType_SqlUdf_WithString()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
CREATE TEMP FUNCTION identity(x ANY TYPE) AS (x);
SELECT identity('hello') AS result;
", null);
		Assert.Equal("hello", result.First()["result"]?.ToString());
	}

	#endregion
}
