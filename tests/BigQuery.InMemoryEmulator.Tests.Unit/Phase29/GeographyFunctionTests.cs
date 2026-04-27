using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase29;

/// <summary>
/// Phase 29: Geography functions — ST_ASBINARY, ST_BOUNDARY, ST_BUFFER, ST_CENTROID_AGG,
/// ST_CLOSESTPOINT, ST_CONVEXHULL, ST_COVEREDBY, ST_COVERS, ST_DIFFERENCE, ST_DUMP,
/// ST_GEOGFROMWKB, ST_INTERSECTION, ST_ISCOLLECTION, ST_SIMPLIFY, ST_TOUCHES,
/// ST_UNION, ST_UNION_AGG.
/// </summary>
public class GeographyFunctionTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		return new QueryExecutor(store);
	}

	#region ST_ASBINARY

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_asbinary
	//   "Returns the WKB representation of an input GEOGRAPHY."

	[Fact]
	public void StAsBinary_Point_ReturnsBytes()
	{
		var sql = "SELECT ST_ASBINARY(ST_GEOGPOINT(1.0, 2.0)) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.NotNull(rows[0].F[0].V);
	}

	[Fact]
	public void StAsBinary_NullInput_ReturnsNull()
	{
		var sql = "SELECT ST_ASBINARY(NULL) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region ST_GEOGFROMWKB

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_geogfromwkb
	//   "Converts a WKB BYTES value into a GEOGRAPHY value."

	[Fact]
	public void StGeogFromWkb_RoundTrip_Point()
	{
		var sql = "SELECT ST_ASTEXT(ST_GEOGFROMWKB(ST_ASBINARY(ST_GEOGPOINT(1.0, 2.0)))) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		var result = rows[0].F[0].V?.ToString();
		Assert.Contains("1", result);
		Assert.Contains("2", result);
	}

	[Fact]
	public void StGeogFromWkb_NullInput_ReturnsNull()
	{
		var sql = "SELECT ST_GEOGFROMWKB(NULL) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region ST_ISCOLLECTION

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_iscollection
	//   "Returns TRUE if the total number of geometries is greater than one."

	[Fact]
	public void StIsCollection_SinglePoint_ReturnsFalse()
	{
		var sql = "SELECT ST_ISCOLLECTION(ST_GEOGPOINT(1, 2)) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("false", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void StIsCollection_NullInput_ReturnsNull()
	{
		var sql = "SELECT ST_ISCOLLECTION(NULL) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region ST_BOUNDARY

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_boundary
	//   "Returns a single GEOGRAPHY that contains the union of boundaries of each component."

	[Fact]
	public void StBoundary_Point_ReturnsEmpty()
	{
		var sql = "SELECT ST_ISEMPTY(ST_BOUNDARY(ST_GEOGPOINT(1, 2))) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("true", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void StBoundary_NullInput_ReturnsNull()
	{
		var sql = "SELECT ST_BOUNDARY(NULL) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region ST_COVEREDBY

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_coveredby
	//   "Returns TRUE if no point of geography_1 is outside geography_2."

	[Fact]
	public void StCoveredBy_PointInPolygon_ReturnsTrue()
	{
		var sql = "SELECT ST_COVEREDBY(ST_GEOGPOINT(0.5, 0.5), ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("true", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void StCoveredBy_PointOutsidePolygon_ReturnsFalse()
	{
		var sql = "SELECT ST_COVEREDBY(ST_GEOGPOINT(5, 5), ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("false", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void StCoveredBy_NullInput_ReturnsNull()
	{
		var sql = "SELECT ST_COVEREDBY(NULL, ST_GEOGPOINT(1, 2)) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region ST_COVERS

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_covers
	//   "Returns TRUE if no point of geography_2 is outside geography_1."

	[Fact]
	public void StCovers_PolygonCoversPoint_ReturnsTrue()
	{
		var sql = "SELECT ST_COVERS(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'), ST_GEOGPOINT(0.5, 0.5)) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("true", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void StCovers_PolygonDoesNotCoverPoint_ReturnsFalse()
	{
		var sql = "SELECT ST_COVERS(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'), ST_GEOGPOINT(5, 5)) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("false", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region ST_TOUCHES

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_touches
	//   "Returns TRUE if the two geometries touch but do not intersect interiors."

	[Fact]
	public void StTouches_SamePoint_ReturnsFalse()
	{
		var sql = "SELECT ST_TOUCHES(ST_GEOGPOINT(1, 2), ST_GEOGPOINT(1, 2)) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		// Same points share interiors, so they don't "touch" (touch == boundary-only intersection)
		Assert.Equal("false", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void StTouches_NullInput_ReturnsNull()
	{
		var sql = "SELECT ST_TOUCHES(NULL, ST_GEOGPOINT(1, 2)) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region ST_CLOSESTPOINT

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_closestpoint
	//   "Returns the point on geography_1 that is closest to geography_2."

	[Fact]
	public void StClosestPoint_PointToPoint_ReturnsSamePoint()
	{
		var sql = "SELECT ST_ASTEXT(ST_CLOSESTPOINT(ST_GEOGPOINT(1, 2), ST_GEOGPOINT(3, 4))) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		var result = rows[0].F[0].V?.ToString();
		Assert.Contains("1", result);
		Assert.Contains("2", result);
	}

	[Fact]
	public void StClosestPoint_NullInput_ReturnsNull()
	{
		var sql = "SELECT ST_CLOSESTPOINT(NULL, ST_GEOGPOINT(1, 2)) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region ST_CONVEXHULL

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_convexhull
	//   "Returns the convex hull of the input GEOGRAPHY."

	[Fact]
	public void StConvexHull_Point_ReturnsPoint()
	{
		var sql = "SELECT ST_ASTEXT(ST_CONVEXHULL(ST_GEOGPOINT(1, 2))) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		var result = rows[0].F[0].V?.ToString();
		Assert.Contains("POINT", result);
	}

	[Fact]
	public void StConvexHull_NullInput_ReturnsNull()
	{
		var sql = "SELECT ST_CONVEXHULL(NULL) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region ST_DIFFERENCE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_difference
	//   "Returns a GEOGRAPHY that represents the point set difference of two geographies."

	[Fact]
	public void StDifference_NonOverlapping_ReturnsSelf()
	{
		var sql = "SELECT ST_ASTEXT(ST_DIFFERENCE(ST_GEOGPOINT(1, 2), ST_GEOGPOINT(3, 4))) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.NotNull(rows[0].F[0].V);
	}

	[Fact]
	public void StDifference_SameGeometry_ReturnsEmpty()
	{
		var sql = "SELECT ST_ISEMPTY(ST_DIFFERENCE(ST_GEOGPOINT(1, 2), ST_GEOGPOINT(1, 2))) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("true", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void StDifference_NullInput_ReturnsNull()
	{
		var sql = "SELECT ST_DIFFERENCE(NULL, ST_GEOGPOINT(1, 2)) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region ST_INTERSECTION

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_intersection
	//   "Returns a GEOGRAPHY that represents the point set intersection of two geographies."

	[Fact]
	public void StIntersection_SamePoint_ReturnsPoint()
	{
		var sql = "SELECT ST_ASTEXT(ST_INTERSECTION(ST_GEOGPOINT(1, 2), ST_GEOGPOINT(1, 2))) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		var result = rows[0].F[0].V?.ToString();
		Assert.Contains("POINT", result);
		Assert.Contains("1", result);
	}

	[Fact]
	public void StIntersection_NonOverlapping_ReturnsEmpty()
	{
		var sql = "SELECT ST_ISEMPTY(ST_INTERSECTION(ST_GEOGPOINT(1, 2), ST_GEOGPOINT(3, 4))) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("true", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void StIntersection_NullInput_ReturnsNull()
	{
		var sql = "SELECT ST_INTERSECTION(NULL, ST_GEOGPOINT(1, 2)) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region ST_UNION

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_union
	//   "Returns a GEOGRAPHY that represents the point set union of two geographies."

	[Fact]
	public void StUnion_TwoPoints_ReturnsGeography()
	{
		var sql = "SELECT ST_ASTEXT(ST_UNION(ST_GEOGPOINT(1, 2), ST_GEOGPOINT(3, 4))) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.NotNull(rows[0].F[0].V);
	}

	[Fact]
	public void StUnion_SamePoint_ReturnsPoint()
	{
		var sql = "SELECT ST_ASTEXT(ST_UNION(ST_GEOGPOINT(1, 2), ST_GEOGPOINT(1, 2))) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		var result = rows[0].F[0].V?.ToString();
		Assert.Contains("POINT", result);
	}

	[Fact]
	public void StUnion_NullInput_ReturnsNull()
	{
		var sql = "SELECT ST_UNION(NULL, ST_GEOGPOINT(1, 2)) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region ST_BUFFER

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_buffer
	//   "Returns a GEOGRAPHY that represents the buffer around the input GEOGRAPHY."

	[Fact]
	public void StBuffer_Point_ReturnsPolygon()
	{
		var sql = "SELECT ST_GEOMETRYTYPE(ST_BUFFER(ST_GEOGPOINT(0, 0), 100)) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("ST_Polygon", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void StBuffer_Point_HasArea()
	{
		var sql = "SELECT ST_AREA(ST_BUFFER(ST_GEOGPOINT(0, 0), 1000)) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		var area = Convert.ToDouble(rows[0].F[0].V);
		Assert.True(area > 0);
	}

	[Fact]
	public void StBuffer_NullInput_ReturnsNull()
	{
		var sql = "SELECT ST_BUFFER(NULL, 100) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region ST_SIMPLIFY

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_simplify
	//   "Returns a simplified version of geography."

	[Fact]
	public void StSimplify_Point_ReturnsSamePoint()
	{
		var sql = "SELECT ST_ASTEXT(ST_SIMPLIFY(ST_GEOGPOINT(1, 2), 100)) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		var result = rows[0].F[0].V?.ToString();
		Assert.Contains("POINT", result);
	}

	[Fact]
	public void StSimplify_NullInput_ReturnsNull()
	{
		var sql = "SELECT ST_SIMPLIFY(NULL, 100) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region ST_DUMP

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_dump
	//   "Returns an ARRAY of GEOGRAPHY elements from the input GEOGRAPHY."

	[Fact]
	public void StDump_SinglePoint_ReturnsSingleElement()
	{
		var sql = "SELECT ARRAY_LENGTH(ST_DUMP(ST_GEOGPOINT(1, 2))) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal("1", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void StDump_NullInput_ReturnsNull()
	{
		var sql = "SELECT ST_DUMP(NULL) AS result";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region ST_CENTROID_AGG

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_centroid_agg
	//   "Computes the centroid of the union of the input GEOGRAPHY values."

	[Fact]
	public void StCentroidAgg_MultiplePoints()
	{
		var sql = @"
			SELECT ST_X(ST_CENTROID_AGG(point)) AS x, ST_Y(ST_CENTROID_AGG(point)) AS y
			FROM UNNEST([ST_GEOGPOINT(0, 0), ST_GEOGPOINT(2, 0), ST_GEOGPOINT(0, 2)]) AS point";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.NotNull(rows[0].F[0].V);
		Assert.NotNull(rows[0].F[1].V);
	}

	#endregion

	#region ST_UNION_AGG

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_union_agg
	//   "Returns the point set union of all input GEOGRAPHYs."

	[Fact]
	public void StUnionAgg_MultiplePoints()
	{
		var sql = @"
			SELECT ST_ASTEXT(ST_UNION_AGG(point)) AS result
			FROM UNNEST([ST_GEOGPOINT(0, 0), ST_GEOGPOINT(2, 0)]) AS point";
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.NotNull(rows[0].F[0].V);
	}

	#endregion
}
