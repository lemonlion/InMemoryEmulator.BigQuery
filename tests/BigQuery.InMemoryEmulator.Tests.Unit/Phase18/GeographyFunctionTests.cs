using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase18;

/// <summary>
/// Unit tests for geography functions (Phase 18).
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions
/// </summary>
public class GeographyFunctionTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;
		return new QueryExecutor(store, "test_ds");
	}

	// --- ST_GEOGPOINT ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_geogpoint
	//   "Creates a GEOGRAPHY with a single point."

	[Fact]
	public void StGeogPoint_CreatesPoint()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ST_ASTEXT(ST_GEOGPOINT(-122.35, 47.62))");
		Assert.Equal("POINT(-122.35 47.62)", rows[0].F![0].V?.ToString());
	}

	// --- ST_X / ST_Y ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_x
	//   "Returns the longitude in degrees of the single-point input GEOGRAPHY."

	[Fact]
	public void StX_ReturnsLongitude()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ST_X(ST_GEOGPOINT(-122.35, 47.62))");
		Assert.Equal(-122.35, double.Parse(rows[0].F![0].V!.ToString()!));
	}

	[Fact]
	public void StY_ReturnsLatitude()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ST_Y(ST_GEOGPOINT(-122.35, 47.62))");
		Assert.Equal(47.62, double.Parse(rows[0].F![0].V!.ToString()!));
	}

	// --- ST_ASTEXT ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_astext
	//   "Converts a GEOGRAPHY value to a STRING WKT geography value."

	[Fact]
	public void StAsText_Point()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ST_ASTEXT(ST_GEOGPOINT(1, 2))");
		Assert.Equal("POINT(1 2)", rows[0].F![0].V?.ToString());
	}

	// --- ST_ASGEOJSON ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_asgeojson
	//   "Converts a GEOGRAPHY value to a STRING GeoJSON geography value."

	[Fact]
	public void StAsGeoJson_Point()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ST_ASGEOJSON(ST_GEOGPOINT(1, 2))");
		var json = rows[0].F![0].V?.ToString();
		Assert.Contains("\"type\":\"Point\"", json);
		Assert.Contains("[1,2]", json);
	}

	// --- ST_GEOGFROMTEXT ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_geogfromtext
	//   "Converts a STRING WKT geometry value into a GEOGRAPHY value."

	[Fact]
	public void StGeogFromText_Point()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ST_X(ST_GEOGFROMTEXT('POINT(10 20)'))");
		Assert.Equal(10.0, double.Parse(rows[0].F![0].V!.ToString()!));
	}

	[Fact]
	public void StGeogFromText_Linestring()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ST_ASTEXT(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 1, 2 0)'))");
		Assert.Equal("LINESTRING(0 0, 1 1, 2 0)", rows[0].F![0].V?.ToString());
	}

	[Fact]
	public void StGeogFromText_Polygon()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ST_ASTEXT(ST_GEOGFROMTEXT('POLYGON((0 0, 10 0, 10 10, 0 0))'))");
		Assert.Equal("POLYGON((0 0, 10 0, 10 10, 0 0))", rows[0].F![0].V?.ToString());
	}

	[Fact]
	public void StGeogFromText_Empty()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ST_ISEMPTY(ST_GEOGFROMTEXT('POINT EMPTY'))");
		Assert.Equal("true", rows[0].F![0].V?.ToString());
	}

	// --- ST_GEOGFROMGEOJSON ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_geogfromgeojson
	//   "Returns a GEOGRAPHY value that corresponds to the input GeoJSON representation."

	[Fact]
	public void StGeogFromGeoJson_Point()
	{
		var (_, rows) = CreateExecutor().Execute(
			"""SELECT ST_X(ST_GEOGFROMGEOJSON('{"type":"Point","coordinates":[10,20]}'))""");
		Assert.Equal(10.0, double.Parse(rows[0].F![0].V!.ToString()!));
	}

	// --- ST_DISTANCE ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_distance
	//   "Returns the shortest distance in meters between two non-empty GEOGRAPHYs."

	[Fact]
	public void StDistance_SamePoint_ReturnsZero()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_DISTANCE(ST_GEOGPOINT(0, 0), ST_GEOGPOINT(0, 0))");
		Assert.Equal(0.0, double.Parse(rows[0].F![0].V!.ToString()!));
	}

	[Fact]
	public void StDistance_KnownPoints_ReturnsApproximateMeters()
	{
		// London (51.5074, -0.1278) to Paris (48.8566, 2.3522) ~ 343 km
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_DISTANCE(ST_GEOGPOINT(-0.1278, 51.5074), ST_GEOGPOINT(2.3522, 48.8566))");
		var distance = double.Parse(rows[0].F![0].V!.ToString()!);
		Assert.InRange(distance, 300_000, 400_000); // roughly 343 km
	}

	[Fact]
	public void StDistance_EmptyGeography_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_DISTANCE(ST_GEOGPOINT(0, 0), ST_GEOGFROMTEXT('POINT EMPTY'))");
		Assert.Null(rows[0].F![0].V);
	}

	// --- ST_DWITHIN ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_dwithin
	//   "Returns TRUE if the distance between at least one point in geography_1 and one point
	//    in geography_2 is less than or equal to the distance given by the distance argument."

	[Fact]
	public void StDWithin_ClosePoints_ReturnsTrue()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_DWITHIN(ST_GEOGPOINT(0, 0), ST_GEOGPOINT(0.001, 0), 1000)");
		Assert.Equal("true", rows[0].F![0].V?.ToString());
	}

	[Fact]
	public void StDWithin_FarPoints_ReturnsFalse()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_DWITHIN(ST_GEOGPOINT(0, 0), ST_GEOGPOINT(10, 10), 1000)");
		Assert.Equal("false", rows[0].F![0].V?.ToString());
	}

	// --- ST_CONTAINS ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_contains
	//   "Returns TRUE if no point of geography_2 is outside geography_1, and the interiors intersect."

	[Fact]
	public void StContains_PointInsidePolygon()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_CONTAINS(ST_GEOGFROMTEXT('POLYGON((0 0, 20 0, 10 20, 0 0))'), ST_GEOGPOINT(10, 10))");
		Assert.Equal("true", rows[0].F![0].V?.ToString());
	}

	[Fact]
	public void StContains_PointOutsidePolygon()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_CONTAINS(ST_GEOGFROMTEXT('POLYGON((0 0, 20 0, 10 20, 0 0))'), ST_GEOGPOINT(30, 30))");
		// Point on boundary: BigQuery returns FALSE for ST_CONTAINS (boundary not included in interior)
		Assert.Equal("false", rows[0].F![0].V?.ToString());
	}

	// --- ST_WITHIN ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_within
	//   "Returns TRUE if no point of geography_1 is outside of geography_2."

	[Fact]
	public void StWithin_PointInPolygon()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_WITHIN(ST_GEOGPOINT(10, 10), ST_GEOGFROMTEXT('POLYGON((0 0, 20 0, 10 20, 0 0))'))");
		Assert.Equal("true", rows[0].F![0].V?.ToString());
	}

	// --- ST_INTERSECTS ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_intersects
	//   "Returns TRUE if the point set intersection of geography_1 and geography_2 is non-empty."

	[Fact]
	public void StIntersects_OverlappingPolygons()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_INTERSECTS(ST_GEOGFROMTEXT('POLYGON((0 0, 10 0, 10 10, 0 0))'), ST_GEOGFROMTEXT('POLYGON((5 5, 15 5, 15 15, 5 5))'))");
		Assert.Equal("true", rows[0].F![0].V?.ToString());
	}

	[Fact]
	public void StIntersects_DisjointPolygons()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_INTERSECTS(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 0))'), ST_GEOGFROMTEXT('POLYGON((10 10, 11 10, 11 11, 10 10))'))");
		Assert.Equal("false", rows[0].F![0].V?.ToString());
	}

	// --- ST_DISJOINT ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_disjoint
	//   "Returns TRUE if the intersection of geography_1 and geography_2 is empty."

	[Fact]
	public void StDisjoint_NoOverlap_ReturnsTrue()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_DISJOINT(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 0))'), ST_GEOGFROMTEXT('POLYGON((10 10, 11 10, 11 11, 10 10))'))");
		Assert.Equal("true", rows[0].F![0].V?.ToString());
	}

	// --- ST_EQUALS ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_equals
	//   "Checks if two GEOGRAPHY values represent the same GEOGRAPHY value."

	[Fact]
	public void StEquals_SamePoints()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_EQUALS(ST_GEOGPOINT(1, 2), ST_GEOGPOINT(1, 2))");
		Assert.Equal("true", rows[0].F![0].V?.ToString());
	}

	[Fact]
	public void StEquals_DifferentPoints()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_EQUALS(ST_GEOGPOINT(1, 2), ST_GEOGPOINT(3, 4))");
		Assert.Equal("false", rows[0].F![0].V?.ToString());
	}

	// --- ST_AREA ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_area
	//   "Returns the area in square meters covered by the polygons in the input GEOGRAPHY."

	[Fact]
	public void StArea_Point_ReturnsZero()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ST_AREA(ST_GEOGPOINT(30, 30))");
		Assert.Equal("0", rows[0].F![0].V?.ToString());
	}

	[Fact]
	public void StArea_Polygon_ReturnsPositiveValue()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_AREA(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'))");
		var area = double.Parse(rows[0].F![0].V!.ToString()!);
		Assert.True(area > 0, "Area should be positive");
		// ~12,308 km² for 1°x1° at the equator
		Assert.InRange(area, 1e9, 2e10);
	}

	// --- ST_LENGTH ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_length
	//   "Returns the total length in meters of the lines in the input GEOGRAPHY."

	[Fact]
	public void StLength_Point_ReturnsZero()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ST_LENGTH(ST_GEOGPOINT(30, 30))");
		Assert.Equal("0", rows[0].F![0].V?.ToString());
	}

	[Fact]
	public void StLength_Linestring_ReturnsMeters()
	{
		// ~111 km for 1° of latitude along a meridian
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_LENGTH(ST_GEOGFROMTEXT('LINESTRING(0 0, 0 1)'))");
		var length = double.Parse(rows[0].F![0].V!.ToString()!);
		Assert.InRange(length, 100_000, 120_000);
	}

	// --- ST_PERIMETER ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_perimeter
	//   "Returns the length in meters of the boundary of the polygons."

	[Fact]
	public void StPerimeter_Polygon()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_PERIMETER(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'))");
		var perim = double.Parse(rows[0].F![0].V!.ToString()!);
		// ~4 * 111 km for 1° sides at equator
		Assert.True(perim > 300_000);
	}

	// --- ST_NUMPOINTS ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_numpoints
	//   "Returns the number of vertices in the input GEOGRAPHY."

	[Fact]
	public void StNumPoints_Point()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ST_NUMPOINTS(ST_GEOGPOINT(1, 2))");
		Assert.Equal("1", rows[0].F![0].V?.ToString());
	}

	[Fact]
	public void StNumPoints_Linestring()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_NUMPOINTS(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 1, 2 0)'))");
		Assert.Equal("3", rows[0].F![0].V?.ToString());
	}

	// --- ST_DIMENSION ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_dimension
	//   "Returns the dimension of the highest-dimensional element."

	[Fact]
	public void StDimension_Point()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ST_DIMENSION(ST_GEOGPOINT(1, 2))");
		Assert.Equal("0", rows[0].F![0].V?.ToString());
	}

	[Fact]
	public void StDimension_Linestring()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_DIMENSION(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 1)'))");
		Assert.Equal("1", rows[0].F![0].V?.ToString());
	}

	[Fact]
	public void StDimension_Polygon()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_DIMENSION(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 0))'))");
		Assert.Equal("2", rows[0].F![0].V?.ToString());
	}

	[Fact]
	public void StDimension_Empty()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_DIMENSION(ST_GEOGFROMTEXT('POINT EMPTY'))");
		Assert.Equal("-1", rows[0].F![0].V?.ToString());
	}

	// --- ST_ISEMPTY ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_isempty
	//   "Returns TRUE if the given GEOGRAPHY is empty."

	[Fact]
	public void StIsEmpty_Point_ReturnsFalse()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ST_ISEMPTY(ST_GEOGPOINT(1, 2))");
		Assert.Equal("false", rows[0].F![0].V?.ToString());
	}

	// --- ST_GEOMETRYTYPE ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_geometrytype
	//   "Returns the OGC geometry type with the ST_ prefix."

	[Fact]
	public void StGeometryType_Point()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ST_GEOMETRYTYPE(ST_GEOGPOINT(1, 2))");
		Assert.Equal("ST_Point", rows[0].F![0].V?.ToString());
	}

	[Fact]
	public void StGeometryType_Linestring()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_GEOMETRYTYPE(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 1)'))");
		Assert.Equal("ST_LineString", rows[0].F![0].V?.ToString());
	}

	[Fact]
	public void StGeometryType_Polygon()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_GEOMETRYTYPE(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 0))'))");
		Assert.Equal("ST_Polygon", rows[0].F![0].V?.ToString());
	}

	// --- ST_MAKELINE ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_makeline
	//   "Creates a GEOGRAPHY with a single linestring."

	[Fact]
	public void StMakeLine_TwoPoints()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_ASTEXT(ST_MAKELINE(ST_GEOGPOINT(1, 2), ST_GEOGPOINT(3, 4)))");
		Assert.Equal("LINESTRING(1 2, 3 4)", rows[0].F![0].V?.ToString());
	}

	// --- ST_CENTROID ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_centroid
	//   "Returns the centroid of the input GEOGRAPHY as a single point GEOGRAPHY."

	[Fact]
	public void StCentroid_Point_ReturnsSamePoint()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_ASTEXT(ST_CENTROID(ST_GEOGPOINT(5, 10)))");
		Assert.Equal("POINT(5 10)", rows[0].F![0].V?.ToString());
	}

	[Fact]
	public void StCentroid_Linestring()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ST_ASTEXT(ST_CENTROID(ST_GEOGFROMTEXT('LINESTRING(0 0, 2 0)')))");
		Assert.Equal("POINT(1 0)", rows[0].F![0].V?.ToString());
	}

	// --- NULL handling ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions
	//   "All GoogleSQL geography functions return NULL if any input argument is NULL."

	[Fact]
	public void GeographyFunctions_NullInput_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ST_DISTANCE(NULL, ST_GEOGPOINT(30, 30))");
		Assert.Null(rows[0].F![0].V);
	}
}
