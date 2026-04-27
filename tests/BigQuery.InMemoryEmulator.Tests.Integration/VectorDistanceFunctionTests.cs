using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for vector distance functions — COSINE_DISTANCE, EUCLIDEAN_DISTANCE.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class VectorDistanceFunctionTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public VectorDistanceFunctionTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_vec_{Guid.NewGuid():N}"[..30];
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

	private async Task<BigQueryResults> Query(string sql) =>
		await (await _fixture.GetClientAsync()).ExecuteQueryAsync(sql, parameters: null);

	// ======================================================================
	// COSINE_DISTANCE
	// ======================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#cosine_distance
	//   COSINE_DISTANCE([1.0, 2.0], [3.0, 4.0]) => 0.016130...
	[Fact]
	public async Task CosineDistance_KnownValues()
	{
		var results = await Query("SELECT COSINE_DISTANCE([1.0, 2.0], [3.0, 4.0]) AS d");
		var rows = results.ToList();
		Assert.Single(rows);
		var val = Convert.ToDouble(rows[0]["d"]);
		Assert.InRange(val, 0.016, 0.017);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#cosine_distance
	//   Identical vectors have cosine distance 0.
	[Fact]
	public async Task CosineDistance_IdenticalVectors_ReturnsZero()
	{
		var results = await Query("SELECT COSINE_DISTANCE([1.0, 2.0, 3.0], [1.0, 2.0, 3.0]) AS d");
		var rows = results.ToList();
		var val = Convert.ToDouble(rows[0]["d"]);
		Assert.Equal(0.0, val, 10);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#cosine_distance
	//   Opposite vectors have cosine distance 2.
	[Fact]
	public async Task CosineDistance_OppositeVectors_ReturnsTwo()
	{
		var results = await Query("SELECT COSINE_DISTANCE([1.0, 0.0], [-1.0, 0.0]) AS d");
		var rows = results.ToList();
		var val = Convert.ToDouble(rows[0]["d"]);
		Assert.Equal(2.0, val, 10);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#cosine_distance
	//   Orthogonal vectors have cosine distance 1.
	[Fact]
	public async Task CosineDistance_OrthogonalVectors_ReturnsOne()
	{
		var results = await Query("SELECT COSINE_DISTANCE([1.0, 0.0], [0.0, 1.0]) AS d");
		var rows = results.ToList();
		var val = Convert.ToDouble(rows[0]["d"]);
		Assert.Equal(1.0, val, 10);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#cosine_distance
	//   "If a vector is NULL, NULL is returned."
	[Fact]
	public async Task CosineDistance_NullVector_ReturnsNull()
	{
		var results = await Query("SELECT COSINE_DISTANCE(NULL, [1.0, 2.0]) AS d");
		var rows = results.ToList();
		Assert.Null(rows[0]["d"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#cosine_distance
	//   Zero vector → error. In emulator we return NULL.
	[Fact]
	public async Task CosineDistance_ZeroVector_ReturnsNull()
	{
		var results = await Query("SELECT COSINE_DISTANCE([0.0, 0.0], [1.0, 2.0]) AS d");
		var rows = results.ToList();
		Assert.Null(rows[0]["d"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#cosine_distance
	//   Mismatched dimensions → error. In emulator we return NULL.
	[Fact]
	public async Task CosineDistance_MismatchedDimensions_ReturnsNull()
	{
		var results = await Query("SELECT COSINE_DISTANCE([1.0, 2.0], [3.0, 4.0, 5.0]) AS d");
		var rows = results.ToList();
		Assert.Null(rows[0]["d"]);
	}

	// ======================================================================
	// EUCLIDEAN_DISTANCE
	// ======================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#euclidean_distance
	//   EUCLIDEAN_DISTANCE([1.0, 2.0], [3.0, 4.0]) => sqrt(8) = 2.828...
	[Fact]
	public async Task EuclideanDistance_KnownValues()
	{
		var results = await Query("SELECT EUCLIDEAN_DISTANCE([1.0, 2.0], [3.0, 4.0]) AS d");
		var rows = results.ToList();
		var val = Convert.ToDouble(rows[0]["d"]);
		Assert.InRange(val, 2.828, 2.829);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#euclidean_distance
	//   Identical vectors → distance 0.
	[Fact]
	public async Task EuclideanDistance_IdenticalVectors_ReturnsZero()
	{
		var results = await Query("SELECT EUCLIDEAN_DISTANCE([5.0, 10.0], [5.0, 10.0]) AS d");
		var rows = results.ToList();
		var val = Convert.ToDouble(rows[0]["d"]);
		Assert.Equal(0.0, val, 10);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#euclidean_distance
	//   3-4-5 triangle: EUCLIDEAN_DISTANCE([0.0, 0.0], [3.0, 4.0]) => 5.0
	[Fact]
	public async Task EuclideanDistance_345Triangle()
	{
		var results = await Query("SELECT EUCLIDEAN_DISTANCE([0.0, 0.0], [3.0, 4.0]) AS d");
		var rows = results.ToList();
		var val = Convert.ToDouble(rows[0]["d"]);
		Assert.Equal(5.0, val, 10);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#euclidean_distance
	//   "If a vector is NULL, NULL is returned."
	[Fact]
	public async Task EuclideanDistance_NullVector_ReturnsNull()
	{
		var results = await Query("SELECT EUCLIDEAN_DISTANCE(NULL, [1.0, 2.0]) AS d");
		var rows = results.ToList();
		Assert.Null(rows[0]["d"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#euclidean_distance
	//   Mismatched dimensions → error. In emulator we return NULL.
	[Fact]
	public async Task EuclideanDistance_MismatchedDimensions_ReturnsNull()
	{
		var results = await Query("SELECT EUCLIDEAN_DISTANCE([1.0, 2.0], [3.0, 4.0, 5.0]) AS d");
		var rows = results.ToList();
		Assert.Null(rows[0]["d"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#euclidean_distance
	//   4D vector
	[Fact]
	public async Task EuclideanDistance_FourDimensional()
	{
		var results = await Query(
			"SELECT EUCLIDEAN_DISTANCE([1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]) AS d");
		var rows = results.ToList();
		var val = Convert.ToDouble(rows[0]["d"]);
		// sqrt(16+16+16+16) = sqrt(64) = 8.0
		Assert.Equal(8.0, val, 10);
	}

	// ======================================================================
	// Edge Cases
	// ======================================================================

	// Both functions should work with GENERATE_ARRAY-produced arrays
	[Fact]
	public async Task EuclideanDistance_WithGenerateArray()
	{
		var results = await Query(
			"SELECT EUCLIDEAN_DISTANCE(GENERATE_ARRAY(1.0, 3.0, 1.0), GENERATE_ARRAY(4.0, 6.0, 1.0)) AS d");
		var rows = results.ToList();
		var val = Convert.ToDouble(rows[0]["d"]);
		// [1,2,3] vs [4,5,6] → sqrt(9+9+9) = sqrt(27) = 5.196...
		Assert.InRange(val, 5.196, 5.197);
	}

	// Both functions with negative values
	[Fact]
	public async Task EuclideanDistance_NegativeValues()
	{
		var results = await Query("SELECT EUCLIDEAN_DISTANCE([-1.0, -2.0], [1.0, 2.0]) AS d");
		var rows = results.ToList();
		var val = Convert.ToDouble(rows[0]["d"]);
		// sqrt(4+16) = sqrt(20) = 4.472...
		Assert.InRange(val, 4.472, 4.473);
	}
}
