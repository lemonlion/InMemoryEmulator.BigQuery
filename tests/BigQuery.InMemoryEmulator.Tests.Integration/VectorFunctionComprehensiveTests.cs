using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Vector and distance function tests.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/distance_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class VectorFunctionComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public VectorFunctionComprehensiveTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
	}

	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- COSINE_DISTANCE ----
	[Fact] public async Task CosineDistance_Identical_Zero() { var v = double.Parse(await Scalar("SELECT COSINE_DISTANCE([1.0, 0.0], [1.0, 0.0])") ?? "1"); Assert.True(Math.Abs(v) < 0.001); }
	[Fact] public async Task CosineDistance_Orthogonal_One() { var v = double.Parse(await Scalar("SELECT COSINE_DISTANCE([1.0, 0.0], [0.0, 1.0])") ?? "0"); Assert.True(Math.Abs(v - 1.0) < 0.001); }
	[Fact] public async Task CosineDistance_Opposite_Two() { var v = double.Parse(await Scalar("SELECT COSINE_DISTANCE([1.0, 0.0], [-1.0, 0.0])") ?? "0"); Assert.True(Math.Abs(v - 2.0) < 0.001); }
	[Fact] public async Task CosineDistance_3D() { var v = await Scalar("SELECT COSINE_DISTANCE([1.0, 2.0, 3.0], [4.0, 5.0, 6.0])"); Assert.NotNull(v); double.Parse(v!); }

	// ---- EUCLIDEAN_DISTANCE ----
	[Fact] public async Task EuclideanDistance_SameVector_Zero() { var v = double.Parse(await Scalar("SELECT EUCLIDEAN_DISTANCE([1.0, 2.0], [1.0, 2.0])") ?? "1"); Assert.True(Math.Abs(v) < 0.001); }
	[Fact] public async Task EuclideanDistance_UnitDistance() { var v = double.Parse(await Scalar("SELECT EUCLIDEAN_DISTANCE([0.0, 0.0], [3.0, 4.0])") ?? "0"); Assert.True(Math.Abs(v - 5.0) < 0.001); }
	[Fact] public async Task EuclideanDistance_3D() { var v = double.Parse(await Scalar("SELECT EUCLIDEAN_DISTANCE([1.0, 2.0, 3.0], [4.0, 6.0, 3.0])") ?? "0"); Assert.True(Math.Abs(v - 5.0) < 0.001); }

	// ---- DOT_PRODUCT ----
	[Fact] public async Task DotProduct_Orthogonal_Zero() { var v = double.Parse(await Scalar("SELECT DOT_PRODUCT([1.0, 0.0], [0.0, 1.0])") ?? "1"); Assert.True(Math.Abs(v) < 0.001); }
	[Fact] public async Task DotProduct_Parallel() { var v = double.Parse(await Scalar("SELECT DOT_PRODUCT([2.0, 3.0], [4.0, 5.0])") ?? "0"); Assert.True(Math.Abs(v - 23.0) < 0.001); }
	[Fact] public async Task DotProduct_Negative() { var v = double.Parse(await Scalar("SELECT DOT_PRODUCT([1.0, 0.0], [-1.0, 0.0])") ?? "0"); Assert.True(Math.Abs(v - (-1.0)) < 0.001); }

	// ---- APPROX variants ----
	[Fact] public async Task ApproxCosineDistance_Basic() { var v = await Scalar("SELECT APPROX_COSINE_DISTANCE([1.0, 0.0], [1.0, 0.0])"); Assert.NotNull(v); }
	[Fact] public async Task ApproxEuclideanDistance_Basic() { var v = await Scalar("SELECT APPROX_EUCLIDEAN_DISTANCE([0.0, 0.0], [3.0, 4.0])"); Assert.NotNull(v); }
	[Fact] public async Task ApproxDotProduct_Basic() { var v = await Scalar("SELECT APPROX_DOT_PRODUCT([1.0, 2.0], [3.0, 4.0])"); Assert.NotNull(v); }

	// ---- With FLOAT64 arrays ----
	[Fact] public async Task CosineDistance_LargeVector()
	{
		var v = await Scalar("SELECT COSINE_DISTANCE([1.0, 2.0, 3.0, 4.0, 5.0], [5.0, 4.0, 3.0, 2.0, 1.0])");
		Assert.NotNull(v);
		double.Parse(v!);
	}

	// ---- Edge cases ----
	[Fact] public async Task EuclideanDistance_SingleDimension() { var v = double.Parse(await Scalar("SELECT EUCLIDEAN_DISTANCE([3.0], [7.0])") ?? "0"); Assert.True(Math.Abs(v - 4.0) < 0.001); }
}
