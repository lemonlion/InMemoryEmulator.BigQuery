using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for geography functions (Phase 18).
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class GeographyTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public GeographyTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_geo_{Guid.NewGuid():N}"[..30];
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

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_geogpoint
	//   "Creates a GEOGRAPHY with a single point."
	[Fact]
	public async Task StGeogPoint_CreatesPoint()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT ST_ASTEXT(ST_GEOGPOINT(-122.35, 47.62)) AS wkt",
			parameters: null);
		var rows = results.ToList();
		Assert.Contains("POINT", (string)rows[0]["wkt"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_distance
	//   "Returns the shortest distance in meters between two non-empty GEOGRAPHY values."
	[Fact]
	public async Task StDistance_KnownPoints()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT ST_DISTANCE(ST_GEOGPOINT(0, 0), ST_GEOGPOINT(0, 1)) AS meters",
			parameters: null);
		var rows = results.ToList();
		var meters = Convert.ToDouble(rows[0]["meters"]);
		// 1 degree latitude ≈ 111km
		Assert.InRange(meters, 100000, 120000);
	}

	[Fact]
	public async Task StDistance_SamePoint_ReturnsZero()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT ST_DISTANCE(ST_GEOGPOINT(1, 1), ST_GEOGPOINT(1, 1)) AS meters",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(0.0, Convert.ToDouble(rows[0]["meters"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_contains
	//   "Returns TRUE if the second GEOGRAPHY is completely contained by the first."
	[Fact]
	public async Task StContains_PointInsidePolygon()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			@"SELECT ST_CONTAINS(
				ST_GEOGFROMTEXT('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'),
				ST_GEOGPOINT(5, 5)
			) AS inside",
			parameters: null);
		var rows = results.ToList();
		Assert.True((bool)rows[0]["inside"]);
	}

	[Fact]
	public async Task StContains_PointOutsidePolygon()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			@"SELECT ST_CONTAINS(
				ST_GEOGFROMTEXT('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'),
				ST_GEOGPOINT(15, 15)
			) AS inside",
			parameters: null);
		var rows = results.ToList();
		Assert.False((bool)rows[0]["inside"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_area
	//   "Returns the area in square meters of the polygons in the input GEOGRAPHY."
	[Fact]
	public async Task StArea_Polygon()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT ST_AREA(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')) AS area",
			parameters: null);
		var rows = results.ToList();
		var area = Convert.ToDouble(rows[0]["area"]);
		Assert.True(area > 0);
	}

	[Fact]
	public async Task StX_StY_ReturnCoordinates()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT ST_X(ST_GEOGPOINT(-122.35, 47.62)) AS x, ST_Y(ST_GEOGPOINT(-122.35, 47.62)) AS y",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(-122.35, Convert.ToDouble(rows[0]["x"]), 2);
		Assert.Equal(47.62, Convert.ToDouble(rows[0]["y"]), 2);
	}

	[Fact]
	public async Task StGeogFromText_Linestring()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT ST_ASTEXT(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 1, 2 0)')) AS wkt",
			parameters: null);
		var rows = results.ToList();
		Assert.Contains("LINESTRING", (string)rows[0]["wkt"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_dwithin
	//   "Returns TRUE if the distance between two GEOGRAPHY values is at most the given distance."
	[Fact]
	public async Task StDWithin_ClosePoints()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT ST_DWITHIN(ST_GEOGPOINT(0, 0), ST_GEOGPOINT(0, 0.001), 200) AS close",
			parameters: null);
		var rows = results.ToList();
		Assert.True((bool)rows[0]["close"]);
	}

	[Fact]
	public async Task StLength_Linestring()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT ST_LENGTH(ST_GEOGFROMTEXT('LINESTRING(0 0, 0 1)')) AS len",
			parameters: null);
		var rows = results.ToList();
		var len = Convert.ToDouble(rows[0]["len"]);
		Assert.InRange(len, 100000, 120000); // ~111km per degree
	}
}
