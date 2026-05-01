using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Comprehensive geography/geospatial function tests.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class GeographyComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public GeographyComprehensiveTests(BigQuerySession session) => _session = session;

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

	// ---- ST_GEOGPOINT ----
	[Fact] public async Task StGeogPoint_Basic() => Assert.NotNull(await Scalar("SELECT ST_GEOGPOINT(0, 0)"));
	[Fact] public async Task StGeogPoint_London() => Assert.NotNull(await Scalar("SELECT ST_GEOGPOINT(-0.1276, 51.5074)"));
	[Fact] public async Task StGeogPoint_Null() => Assert.Null(await Scalar("SELECT ST_GEOGPOINT(NULL, 0)"));

	// ---- ST_GEOGFROMTEXT / ST_GEOGFROMWKT ----
	[Fact] public async Task StGeogFromText_Point() => Assert.NotNull(await Scalar("SELECT ST_GEOGFROMTEXT('POINT(0 0)')"));
	[Fact] public async Task StGeogFromText_Linestring() => Assert.NotNull(await Scalar("SELECT ST_GEOGFROMTEXT('LINESTRING(0 0, 1 1, 2 2)')"));
	[Fact] public async Task StGeogFromText_Polygon() => Assert.NotNull(await Scalar("SELECT ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')"));
	[Fact] public async Task StGeogFromText_Null() => Assert.Null(await Scalar("SELECT ST_GEOGFROMTEXT(NULL)"));

	// ---- ST_GEOGFROMGEOJSON ----
	[Fact] public async Task StGeogFromGeoJson_Point() => Assert.NotNull(await Scalar("SELECT ST_GEOGFROMGEOJSON('{\"type\":\"Point\",\"coordinates\":[0,0]}')"));
	[Fact] public async Task StGeogFromGeoJson_Null() => Assert.Null(await Scalar("SELECT ST_GEOGFROMGEOJSON(NULL)"));

	// ---- ST_ASTEXT ----
	[Fact] public async Task StAsText_Point() { var v = await Scalar("SELECT ST_ASTEXT(ST_GEOGPOINT(1, 2))"); Assert.Contains("POINT", v!); }
	[Fact] public async Task StAsText_Linestring() { var v = await Scalar("SELECT ST_ASTEXT(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 1)'))"); Assert.Contains("LINESTRING", v!); }
	[Fact] public async Task StAsText_Null() => Assert.Null(await Scalar("SELECT ST_ASTEXT(NULL)"));

	// ---- ST_ASGEOJSON ----
	[Fact] public async Task StAsGeoJson_Point() { var v = await Scalar("SELECT ST_ASGEOJSON(ST_GEOGPOINT(1, 2))"); Assert.Contains("Point", v!); }
	[Fact] public async Task StAsGeoJson_Null() => Assert.Null(await Scalar("SELECT ST_ASGEOJSON(NULL)"));

	// ---- ST_ASBINARY ----
	[Fact] public async Task StAsBinary_Point() => Assert.NotNull(await Scalar("SELECT ST_ASBINARY(ST_GEOGPOINT(0, 0))"));

	// ---- ST_X / ST_Y ----
	[Fact] public async Task StX_ReturnsLongitude() => Assert.Equal("10", await Scalar("SELECT CAST(ST_X(ST_GEOGPOINT(10, 20)) AS INT64)"));
	[Fact] public async Task StY_ReturnsLatitude() => Assert.Equal("20", await Scalar("SELECT CAST(ST_Y(ST_GEOGPOINT(10, 20)) AS INT64)"));
	[Fact] public async Task StX_Null() => Assert.Null(await Scalar("SELECT ST_X(NULL)"));
	[Fact] public async Task StY_Null() => Assert.Null(await Scalar("SELECT ST_Y(NULL)"));

	// ---- ST_DISTANCE ----
	[Fact] public async Task StDistance_SamePoint_Zero() { var v = double.Parse(await Scalar("SELECT ST_DISTANCE(ST_GEOGPOINT(0,0), ST_GEOGPOINT(0,0))") ?? "1"); Assert.Equal(0.0, v); }
	[Fact] public async Task StDistance_DifferentPoints_Positive() { var v = double.Parse(await Scalar("SELECT ST_DISTANCE(ST_GEOGPOINT(0,0), ST_GEOGPOINT(1,1))") ?? "0"); Assert.True(v > 0); }
	[Fact] public async Task StDistance_Null() => Assert.Null(await Scalar("SELECT ST_DISTANCE(NULL, ST_GEOGPOINT(0,0))"));

	// ---- ST_DWITHIN ----
	[Fact] public async Task StDWithin_Close_True() => Assert.Equal("True", await Scalar("SELECT ST_DWITHIN(ST_GEOGPOINT(0,0), ST_GEOGPOINT(0,0.001), 1000)"));
	[Fact] public async Task StDWithin_Far_False() => Assert.Equal("False", await Scalar("SELECT ST_DWITHIN(ST_GEOGPOINT(0,0), ST_GEOGPOINT(10,10), 100)"));

	// ---- ST_LENGTH ----
	[Fact] public async Task StLength_Linestring() { var v = double.Parse(await Scalar("SELECT ST_LENGTH(ST_GEOGFROMTEXT('LINESTRING(0 0, 0 1)'))") ?? "0"); Assert.True(v > 0); }
	[Fact] public async Task StLength_Point_Zero() { var v = double.Parse(await Scalar("SELECT ST_LENGTH(ST_GEOGPOINT(0,0))") ?? "1"); Assert.Equal(0.0, v); }

	// ---- ST_AREA ----
	[Fact] public async Task StArea_Polygon() { var v = double.Parse(await Scalar("SELECT ST_AREA(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'))") ?? "0"); Assert.True(v > 0); }
	[Fact] public async Task StArea_Point_Zero() { var v = double.Parse(await Scalar("SELECT ST_AREA(ST_GEOGPOINT(0,0))") ?? "1"); Assert.Equal(0.0, v); }

	// ---- ST_PERIMETER ----
	[Fact] public async Task StPerimeter_Polygon() { var v = double.Parse(await Scalar("SELECT ST_PERIMETER(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'))") ?? "0"); Assert.True(v > 0); }

	// ---- ST_CONTAINS ----
	[Fact] public async Task StContains_PointInPolygon_True() => Assert.Equal("True", await Scalar("SELECT ST_CONTAINS(ST_GEOGFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'), ST_GEOGPOINT(1, 1))"));
	[Fact] public async Task StContains_PointOutsidePolygon_False() => Assert.Equal("False", await Scalar("SELECT ST_CONTAINS(ST_GEOGFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'), ST_GEOGPOINT(5, 5))"));

	// ---- ST_WITHIN ----
	[Fact] public async Task StWithin_PointInPolygon_True() => Assert.Equal("True", await Scalar("SELECT ST_WITHIN(ST_GEOGPOINT(1, 1), ST_GEOGFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'))"));

	// ---- ST_INTERSECTS ----
	[Fact] public async Task StIntersects_Overlapping_True() => Assert.Equal("True", await Scalar("SELECT ST_INTERSECTS(ST_GEOGFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'), ST_GEOGFROMTEXT('POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))'))"));
	[Fact] public async Task StIntersects_NonOverlapping_False() => Assert.Equal("False", await Scalar("SELECT ST_INTERSECTS(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'), ST_GEOGFROMTEXT('POLYGON((5 5, 6 5, 6 6, 5 6, 5 5))'))"));

	// ---- ST_COVERS / ST_COVEREDBY ----
	[Fact] public async Task StCovers_PointInPolygon() => Assert.Equal("True", await Scalar("SELECT ST_COVERS(ST_GEOGFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'), ST_GEOGPOINT(1, 1))"));
	[Fact] public async Task StCoveredBy_PointInPolygon() => Assert.Equal("True", await Scalar("SELECT ST_COVEREDBY(ST_GEOGPOINT(1, 1), ST_GEOGFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'))"));

	// ---- ST_DISJOINT ----
	[Fact] public async Task StDisjoint_NonOverlapping_True() => Assert.Equal("True", await Scalar("SELECT ST_DISJOINT(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'), ST_GEOGFROMTEXT('POLYGON((5 5, 6 5, 6 6, 5 6, 5 5))'))"));

	// ---- ST_TOUCHES ----
	[Fact] public async Task StTouches_SharedEdge() => Assert.Equal("True", await Scalar("SELECT ST_TOUCHES(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'), ST_GEOGFROMTEXT('POLYGON((1 0, 2 0, 2 1, 1 1, 1 0))'))"));

	// ---- ST_EQUALS ----
	[Fact] public async Task StEquals_SameGeometry_True() => Assert.Equal("True", await Scalar("SELECT ST_EQUALS(ST_GEOGPOINT(1,1), ST_GEOGPOINT(1,1))"));
	[Fact] public async Task StEquals_DifferentGeometry_False() => Assert.Equal("False", await Scalar("SELECT ST_EQUALS(ST_GEOGPOINT(1,1), ST_GEOGPOINT(2,2))"));

	// ---- ST_INTERSECTION ----
	[Fact] public async Task StIntersection_Overlapping() => Assert.NotNull(await Scalar("SELECT ST_ASTEXT(ST_INTERSECTION(ST_GEOGFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'), ST_GEOGFROMTEXT('POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))')))"));

	// ---- ST_UNION ----
	[Fact] public async Task StUnion_TwoGeometries() => Assert.NotNull(await Scalar("SELECT ST_ASTEXT(ST_UNION(ST_GEOGPOINT(0,0), ST_GEOGPOINT(1,1)))"));

	// ---- ST_DIFFERENCE ----
	[Fact] public async Task StDifference_TwoPolygons() => Assert.NotNull(await Scalar("SELECT ST_ASTEXT(ST_DIFFERENCE(ST_GEOGFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'), ST_GEOGFROMTEXT('POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))')))"));

	// ---- ST_BUFFER ----
	[Fact] public async Task StBuffer_Point() => Assert.NotNull(await Scalar("SELECT ST_ASTEXT(ST_BUFFER(ST_GEOGPOINT(0,0), 1000))"));

	// ---- ST_CENTROID ----
	[Fact] public async Task StCentroid_Polygon() => Assert.NotNull(await Scalar("SELECT ST_ASTEXT(ST_CENTROID(ST_GEOGFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')))"));

	// ---- ST_CLOSESTPOINT ----
	[Fact] public async Task StClosestPoint_PointOnLine() => Assert.NotNull(await Scalar("SELECT ST_ASTEXT(ST_CLOSESTPOINT(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 1)'), ST_GEOGPOINT(1, 0)))"));

	// ---- ST_BOUNDARY ----
	[Fact] public async Task StBoundary_Polygon() => Assert.NotNull(await Scalar("SELECT ST_ASTEXT(ST_BOUNDARY(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')))"));

	// ---- ST_CONVEXHULL ----
	[Fact] public async Task StConvexHull_MultiPoint() => Assert.NotNull(await Scalar("SELECT ST_ASTEXT(ST_CONVEXHULL(ST_GEOGFROMTEXT('MULTIPOINT(0 0, 1 0, 0 1, 1 1)')))"));

	// ---- ST_SIMPLIFY ----
	[Fact] public async Task StSimplify_Linestring() => Assert.NotNull(await Scalar("SELECT ST_ASTEXT(ST_SIMPLIFY(ST_GEOGFROMTEXT('LINESTRING(0 0, 0.5 0.01, 1 0)'), 100))"));

	// ---- ST_MAKELINE ----
	[Fact] public async Task StMakeLine_TwoPoints() { var v = await Scalar("SELECT ST_ASTEXT(ST_MAKELINE(ST_GEOGPOINT(0,0), ST_GEOGPOINT(1,1)))"); Assert.Contains("LINESTRING", v!); }

	// ---- ST_DIMENSION ----
	[Fact] public async Task StDimension_Point() => Assert.Equal("0", await Scalar("SELECT ST_DIMENSION(ST_GEOGPOINT(0,0))"));
	[Fact] public async Task StDimension_Line() => Assert.Equal("1", await Scalar("SELECT ST_DIMENSION(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 1)'))"));
	[Fact] public async Task StDimension_Polygon() => Assert.Equal("2", await Scalar("SELECT ST_DIMENSION(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'))"));

	// ---- ST_NUMPOINTS / ST_NPOINTS ----
	[Fact] public async Task StNumPoints_Line() { var v = await Scalar("SELECT ST_NUMPOINTS(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 1, 2 2)'))"); Assert.Equal("3", v); }

	// ---- ST_ISEMPTY ----
	[Fact] public async Task StIsEmpty_Point_False() => Assert.Equal("False", await Scalar("SELECT ST_ISEMPTY(ST_GEOGPOINT(0,0))"));

	// ---- ST_ISCOLLECTION ----
	[Fact] public async Task StIsCollection_Point_False() => Assert.Equal("False", await Scalar("SELECT ST_ISCOLLECTION(ST_GEOGPOINT(0,0))"));
	[Fact] public async Task StIsCollection_MultiPoint_True() => Assert.Equal("True", await Scalar("SELECT ST_ISCOLLECTION(ST_GEOGFROMTEXT('MULTIPOINT(0 0, 1 1)'))"));

	// ---- ST_GEOMETRYTYPE ----
	[Fact] public async Task StGeometryType_Point() { var v = await Scalar("SELECT ST_GEOMETRYTYPE(ST_GEOGPOINT(0,0))"); Assert.Contains("Point", v!); }
	[Fact] public async Task StGeometryType_Line() { var v = await Scalar("SELECT ST_GEOMETRYTYPE(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 1)'))"); Assert.Contains("Line", v!); }

	// ---- ST_DUMP ----
	[Fact] public async Task StDump_MultiPoint()
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync("SELECT geom FROM UNNEST(ST_DUMP(ST_GEOGFROMTEXT('MULTIPOINT(0 0, 1 1)'))) AS geom", parameters: null);
		var rows = result.ToList();
		Assert.Equal(2, rows.Count);
	}

	// ---- ST_GEOGFROMWKB ----
	[Fact] public async Task StGeogFromWkb_RoundTrip() => Assert.NotNull(await Scalar("SELECT ST_ASTEXT(ST_GEOGFROMWKB(ST_ASBINARY(ST_GEOGPOINT(1,2))))"));
}
