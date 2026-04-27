using BigQuery.InMemoryEmulator.SqlEngine;
using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase23;

/// <summary>
/// Phase 23: Vector distance functions — COSINE_DISTANCE, EUCLIDEAN_DISTANCE.
/// </summary>
public class VectorDistanceFunctionTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
			]
		};
		var table = new InMemoryTable("test_ds", "dummy", schema);
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 1L }));
		ds.Tables["dummy"] = table;

		return new QueryExecutor(store, "test_ds");
	}

	#region COSINE_DISTANCE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#cosine_distance
	//   "Computes the cosine distance between two vectors."
	//   COSINE_DISTANCE([1.0, 2.0], [3.0, 4.0]) => 0.016130...
	[Fact]
	public void CosineDistance_KnownValues()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT COSINE_DISTANCE([1.0, 2.0], [3.0, 4.0]) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		Assert.InRange(val, 0.016, 0.017);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#cosine_distance
	//   Identical vectors have cosine distance 0.
	[Fact]
	public void CosineDistance_IdenticalVectors_ReturnsZero()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT COSINE_DISTANCE([1.0, 2.0, 3.0], [1.0, 2.0, 3.0]) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		Assert.Equal(0.0, val, 10);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#cosine_distance
	//   Opposite vectors have cosine distance 2 (cosine similarity = -1, distance = 1 - (-1) = 2).
	[Fact]
	public void CosineDistance_OppositeVectors_ReturnsTwo()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT COSINE_DISTANCE([1.0, 0.0], [-1.0, 0.0]) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		Assert.Equal(2.0, val, 10);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#cosine_distance
	//   Orthogonal vectors have cosine distance 1 (cosine similarity = 0).
	[Fact]
	public void CosineDistance_OrthogonalVectors_ReturnsOne()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT COSINE_DISTANCE([1.0, 0.0], [0.0, 1.0]) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		Assert.Equal(1.0, val, 10);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#cosine_distance
	//   "If a vector is NULL, NULL is returned."
	[Fact]
	public void CosineDistance_NullVector_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT COSINE_DISTANCE(NULL, [1.0, 2.0]) AS d");
		Assert.Null(rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#cosine_distance
	//   "A vector can't be a zero vector... If a zero vector is encountered, an error is produced."
	//   In the emulator we return NULL instead of throwing to be lenient.
	[Fact]
	public void CosineDistance_ZeroVector_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT COSINE_DISTANCE([0.0, 0.0], [1.0, 2.0]) AS d");
		Assert.Null(rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#cosine_distance
	//   "Both non-sparse vectors in this function must share the same dimensions, and if they don't, an error is produced."
	//   In the emulator we return NULL instead of throwing to be lenient.
	[Fact]
	public void CosineDistance_MismatchedDimensions_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT COSINE_DISTANCE([1.0, 2.0], [3.0, 4.0, 5.0]) AS d");
		Assert.Null(rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#cosine_distance
	//   3-4-5 triangle style: COSINE_DISTANCE([3.0, 4.0], [4.0, 3.0])
	[Fact]
	public void CosineDistance_ThreeDimensional()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT COSINE_DISTANCE([1.0, 0.0, 0.0], [0.0, 1.0, 0.0]) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		Assert.Equal(1.0, val, 10); // orthogonal in 3D
	}

	#endregion

	#region EUCLIDEAN_DISTANCE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#euclidean_distance
	//   "Computes the Euclidean distance between two vectors."
	//   EUCLIDEAN_DISTANCE([1.0, 2.0], [3.0, 4.0]) => 2.828...
	[Fact]
	public void EuclideanDistance_KnownValues()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT EUCLIDEAN_DISTANCE([1.0, 2.0], [3.0, 4.0]) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		// sqrt((3-1)^2 + (4-2)^2) = sqrt(4+4) = sqrt(8) = 2.8284...
		Assert.InRange(val, 2.828, 2.829);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#euclidean_distance
	//   Identical vectors → distance 0.
	[Fact]
	public void EuclideanDistance_IdenticalVectors_ReturnsZero()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT EUCLIDEAN_DISTANCE([5.0, 10.0], [5.0, 10.0]) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		Assert.Equal(0.0, val, 10);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#euclidean_distance
	//   3-4-5 triangle: EUCLIDEAN_DISTANCE([0.0, 0.0], [3.0, 4.0]) => 5.0
	[Fact]
	public void EuclideanDistance_345Triangle()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT EUCLIDEAN_DISTANCE([0.0, 0.0], [3.0, 4.0]) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		Assert.Equal(5.0, val, 10);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#euclidean_distance
	//   "A vector can be a zero vector."
	[Fact]
	public void EuclideanDistance_ZeroVector_Works()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT EUCLIDEAN_DISTANCE([0.0, 0.0], [3.0, 4.0]) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		Assert.Equal(5.0, val, 10);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#euclidean_distance
	//   "If a vector is NULL, NULL is returned."
	[Fact]
	public void EuclideanDistance_NullVector_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT EUCLIDEAN_DISTANCE(NULL, [1.0, 2.0]) AS d");
		Assert.Null(rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#euclidean_distance
	//   Mismatched dimensions → error. In emulator we return NULL.
	[Fact]
	public void EuclideanDistance_MismatchedDimensions_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT EUCLIDEAN_DISTANCE([1.0, 2.0], [3.0, 4.0, 5.0]) AS d");
		Assert.Null(rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#euclidean_distance
	//   High-dimensional vector: 4D
	[Fact]
	public void EuclideanDistance_FourDimensional()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT EUCLIDEAN_DISTANCE([1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		// sqrt(16+16+16+16) = sqrt(64) = 8.0
		Assert.Equal(8.0, val, 10);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#euclidean_distance
	//   1D case
	[Fact]
	public void EuclideanDistance_OneDimensional()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT EUCLIDEAN_DISTANCE([3.0], [7.0]) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		Assert.Equal(4.0, val, 10);
	}

	#endregion

	#region Edge Cases

	// Both functions should work with GENERATE_ARRAY-produced arrays
	[Fact]
	public void EuclideanDistance_WithGenerateArray()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT EUCLIDEAN_DISTANCE(GENERATE_ARRAY(1.0, 3.0, 1.0), GENERATE_ARRAY(4.0, 6.0, 1.0)) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		// [1,2,3] vs [4,5,6] → sqrt(9+9+9) = sqrt(27) = 5.196...
		Assert.InRange(val, 5.196, 5.197);
	}

	// Both functions with single-element arrays
	[Fact]
	public void CosineDistance_SingleElement()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT COSINE_DISTANCE([5.0], [5.0]) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		Assert.Equal(0.0, val, 10);
	}

	// Both functions with negative values
	[Fact]
	public void EuclideanDistance_NegativeValues()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT EUCLIDEAN_DISTANCE([-1.0, -2.0], [1.0, 2.0]) AS d");
		var val = Convert.ToDouble(rows[0].F[0].V);
		// sqrt(4+16) = sqrt(20) = 4.472...
		Assert.InRange(val, 4.472, 4.473);
	}

	#endregion
}
