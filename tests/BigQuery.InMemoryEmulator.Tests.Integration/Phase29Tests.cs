using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Phase 29 integration tests: Array functions (ARRAY_IS_DISTINCT, ARRAY_FILTER, ARRAY_TRANSFORM),
/// JSON functions (LAX_BOOL, LAX_INT64, LAX_FLOAT64, LAX_STRING, JSON_ARRAY_APPEND, JSON_ARRAY_INSERT, JSON_CONTAINS),
/// Geography functions (ST_ASBINARY, ST_GEOGFROMWKB, ST_ISCOLLECTION, ST_BOUNDARY, ST_COVEREDBY, ST_COVERS,
/// ST_TOUCHES, ST_CLOSESTPOINT, ST_CONVEXHULL, ST_DIFFERENCE, ST_INTERSECTION, ST_UNION, ST_BUFFER,
/// ST_SIMPLIFY, ST_DUMP, ST_CENTROID_AGG, ST_UNION_AGG).
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class Phase29Tests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public Phase29Tests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_p29_{Guid.NewGuid():N}"[..30];
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

	#region Array Functions

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_is_distinct
	[Fact]
	public async Task ArrayIsDistinct_DistinctElements_ReturnsTrue()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync("SELECT ARRAY_IS_DISTINCT([1, 2, 3]) AS result", null);
		Assert.Equal("true", result.First()["result"]?.ToString(), ignoreCase: true);
	}

	[Fact]
	public async Task ArrayIsDistinct_DuplicateElements_ReturnsFalse()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync("SELECT ARRAY_IS_DISTINCT([1, 2, 1]) AS result", null);
		Assert.Equal("false", result.First()["result"]?.ToString(), ignoreCase: true);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_filter
	[Fact]
	public async Task ArrayFilter_FilterEvenNumbers()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT ARRAY_LENGTH(ARRAY_FILTER([1, 2, 3, 4, 5, 6], e -> MOD(e, 2) = 0)) AS cnt", null);
		Assert.Equal("3", result.First()["cnt"]?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_transform
	[Fact]
	public async Task ArrayTransform_DoubleValues()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT ARRAY_LENGTH(ARRAY_TRANSFORM([1, 2, 3], e -> e * 2)) AS cnt", null);
		Assert.Equal("3", result.First()["cnt"]?.ToString());
	}

	#endregion

	#region JSON Functions

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#lax_bool
	[Fact]
	public async Task LaxBool_TrueValue_ReturnsTrue()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync("SELECT LAX_BOOL(PARSE_JSON('true')) AS result", null);
		Assert.Equal("true", result.First()["result"]?.ToString(), ignoreCase: true);
	}

	[Fact]
	public async Task LaxBool_NumberZero_ReturnsFalse()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync("SELECT LAX_BOOL(PARSE_JSON('0')) AS result", null);
		Assert.Equal("false", result.First()["result"]?.ToString(), ignoreCase: true);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#lax_int64
	[Fact]
	public async Task LaxInt64_NumberValue_ReturnsInt()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync("SELECT LAX_INT64(PARSE_JSON('42')) AS result", null);
		Assert.Equal("42", result.First()["result"]?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#lax_float64
	[Fact]
	public async Task LaxFloat64_NumberValue_ReturnsFloat()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync("SELECT LAX_FLOAT64(PARSE_JSON('3.14')) AS result", null);
		var val = double.Parse(result.First()["result"]!.ToString()!);
		Assert.Equal(3.14, val, 2);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#lax_string
	[Fact]
	public async Task LaxString_StringValue_ReturnsString()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"SELECT LAX_STRING(PARSE_JSON('""hello""')) AS result", null);
		Assert.Equal("hello", result.First()["result"]?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_contains
	[Fact]
	public async Task JsonContains_ValuePresent_ReturnsTrue()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			@"SELECT JSON_CONTAINS(PARSE_JSON('[1, 2, 3]'), PARSE_JSON('2')) AS result", null);
		Assert.Equal("true", result.First()["result"]?.ToString(), ignoreCase: true);
	}

	#endregion

	#region Geography Functions

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_iscollection
	[Fact]
	public async Task StIsCollection_Point_ReturnsFalse()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync("SELECT ST_ISCOLLECTION(ST_GEOGPOINT(0, 0)) AS result", null);
		Assert.Equal("false", result.First()["result"]?.ToString(), ignoreCase: true);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_boundary
	[Fact]
	public async Task StBoundary_Point_ReturnsEmpty()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync("SELECT ST_ISEMPTY(ST_BOUNDARY(ST_GEOGPOINT(1, 2))) AS result", null);
		Assert.Equal("true", result.First()["result"]?.ToString(), ignoreCase: true);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_convexhull
	[Fact]
	public async Task StConvexHull_Point_ReturnsSamePoint()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT ST_ASTEXT(ST_CONVEXHULL(ST_GEOGPOINT(1, 2))) AS result", null);
		Assert.Contains("POINT", result.First()["result"]?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_buffer
	[Fact]
	public async Task StBuffer_PointWithDistance_ReturnsPolygon()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT ST_GEOMETRYTYPE(ST_BUFFER(ST_GEOGPOINT(0, 0), 1000)) AS result", null);
		Assert.Equal("ST_Polygon", result.First()["result"]?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_dump
	[Fact]
	public async Task StDump_Point_ReturnsSingleElementArray()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(
			"SELECT ARRAY_LENGTH(ST_DUMP(ST_GEOGPOINT(1, 2))) AS result", null);
		Assert.Equal("1", result.First()["result"]?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_centroid_agg
	[Fact]
	public async Task StCentroidAgg_MultiplePoints_ReturnsCentroid()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
			SELECT ST_X(ST_CENTROID_AGG(point)) AS x
			FROM UNNEST([ST_GEOGPOINT(0, 0), ST_GEOGPOINT(2, 0)]) AS point", null);
		var x = double.Parse(result.First()["x"]!.ToString()!);
		Assert.Equal(1.0, x, 1);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_union_agg
	[Fact]
	public async Task StUnionAgg_MultiplePoints_ReturnsGeography()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(@"
			SELECT ST_ASTEXT(ST_UNION_AGG(point)) AS result
			FROM UNNEST([ST_GEOGPOINT(0, 0), ST_GEOGPOINT(2, 0)]) AS point", null);
		Assert.NotNull(result.First()["result"]);
	}

	#endregion
}
