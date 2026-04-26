using System.Globalization;
using System.Text.RegularExpressions;

namespace BigQuery.InMemoryEmulator.SqlEngine;

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions
//   "GoogleSQL for BigQuery supports geography functions. Geography functions operate
//    on or generate GoogleSQL GEOGRAPHY values."

/// <summary>Base type for in-memory GEOGRAPHY values.</summary>
internal abstract record GeoValue
{
	public abstract string ToWkt();
	public abstract string ToGeoJson();
	public abstract bool IsEmpty { get; }
	public abstract int Dimension { get; }
	public abstract int NumPoints { get; }
	public abstract string GeometryType { get; }
}

/// <summary>A point GEOGRAPHY: ST_GEOGPOINT(longitude, latitude).</summary>
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_geogpoint
//   "Creates a GEOGRAPHY with a single point."
internal record GeoPoint(double Longitude, double Latitude) : GeoValue
{
	public override string ToWkt() => $"POINT({Longitude.ToString(CultureInfo.InvariantCulture)} {Latitude.ToString(CultureInfo.InvariantCulture)})";
	public override string ToGeoJson() => $$"""{"type":"Point","coordinates":[{{Longitude.ToString(CultureInfo.InvariantCulture)}},{{Latitude.ToString(CultureInfo.InvariantCulture)}}]}""";
	public override bool IsEmpty => false;
	public override int Dimension => 0;
	public override int NumPoints => 1;
	public override string GeometryType => "ST_Point";
	public override string ToString() => ToWkt();
}

/// <summary>A linestring GEOGRAPHY.</summary>
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_makeline
internal record GeoLineString(IReadOnlyList<(double Lon, double Lat)> Points) : GeoValue
{
	public override string ToWkt()
	{
		var pts = string.Join(", ", Points.Select(p =>
			$"{p.Lon.ToString(CultureInfo.InvariantCulture)} {p.Lat.ToString(CultureInfo.InvariantCulture)}"));
		return $"LINESTRING({pts})";
	}

	public override string ToGeoJson()
	{
		var coords = string.Join(",", Points.Select(p =>
			$"[{p.Lon.ToString(CultureInfo.InvariantCulture)},{p.Lat.ToString(CultureInfo.InvariantCulture)}]"));
		return $$"""{"type":"LineString","coordinates":[{{coords}}]}""";
	}

	public override bool IsEmpty => Points.Count == 0;
	public override int Dimension => 1;
	public override int NumPoints => Points.Count;
	public override string GeometryType => "ST_LineString";
	public override string ToString() => ToWkt();
}

/// <summary>A polygon GEOGRAPHY.</summary>
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_makepolygon
internal record GeoPolygon(IReadOnlyList<IReadOnlyList<(double Lon, double Lat)>> Rings) : GeoValue
{
	public override string ToWkt()
	{
		var rings = string.Join(", ", Rings.Select(ring =>
		{
			var pts = string.Join(", ", ring.Select(p =>
				$"{p.Lon.ToString(CultureInfo.InvariantCulture)} {p.Lat.ToString(CultureInfo.InvariantCulture)}"));
			return $"({pts})";
		}));
		return $"POLYGON({rings})";
	}

	public override string ToGeoJson()
	{
		var rings = string.Join(",", Rings.Select(ring =>
		{
			var coords = string.Join(",", ring.Select(p =>
				$"[{p.Lon.ToString(CultureInfo.InvariantCulture)},{p.Lat.ToString(CultureInfo.InvariantCulture)}]"));
			return $"[{coords}]";
		}));
		return $$"""{"type":"Polygon","coordinates":[{{rings}}]}""";
	}

	public override bool IsEmpty => Rings.Count == 0;
	public override int Dimension => 2;
	public override int NumPoints => Rings.Sum(r => r.Count);
	public override string GeometryType => "ST_Polygon";
	public override string ToString() => ToWkt();
}

/// <summary>An empty GEOGRAPHY.</summary>
internal record GeoEmpty : GeoValue
{
	public static GeoEmpty Instance { get; } = new();
	public override string ToWkt() => "GEOMETRYCOLLECTION EMPTY";
	public override string ToGeoJson() => """{"type":"GeometryCollection","geometries":[]}""";
	public override bool IsEmpty => true;
	public override int Dimension => -1;
	public override int NumPoints => 0;
	public override string GeometryType => "ST_GeometryCollection";
	public override string ToString() => ToWkt();
}

/// <summary>
/// Geography computation helpers: distance (Haversine), point-in-polygon (ray casting), area, length.
/// </summary>
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_distance
//   "Returns the shortest distance in meters between two non-empty GEOGRAPHYs."
internal static class GeoComputation
{
	private const double EarthRadiusMeters = 6371008.8; // Mean Earth radius in meters

	/// <summary>Haversine distance in meters between two lat/lon points.</summary>
	public static double DistanceMeters(double lon1, double lat1, double lon2, double lat2)
	{
		var dLat = DegreesToRadians(lat2 - lat1);
		var dLon = DegreesToRadians(lon2 - lon1);
		var lat1Rad = DegreesToRadians(lat1);
		var lat2Rad = DegreesToRadians(lat2);

		var a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2) +
				Math.Sin(dLon / 2) * Math.Sin(dLon / 2) *
				Math.Cos(lat1Rad) * Math.Cos(lat2Rad);
		var c = 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
		return EarthRadiusMeters * c;
	}

	/// <summary>Distance between two GeoValues in meters.</summary>
	public static double? Distance(GeoValue a, GeoValue b)
	{
		if (a.IsEmpty || b.IsEmpty) return null;

		return (a, b) switch
		{
			(GeoPoint p1, GeoPoint p2) => DistanceMeters(p1.Longitude, p1.Latitude, p2.Longitude, p2.Latitude),
			(GeoPoint p, GeoLineString l) => MinDistancePointToLine(p, l),
			(GeoLineString l, GeoPoint p) => MinDistancePointToLine(p, l),
			(GeoPoint p, GeoPolygon poly) => PointInPolygon(p, poly) ? 0.0 : MinDistancePointToPolygonBoundary(p, poly),
			(GeoPolygon poly, GeoPoint p) => PointInPolygon(p, poly) ? 0.0 : MinDistancePointToPolygonBoundary(p, poly),
			_ => DistanceMeters(GetRepresentativePoint(a).Longitude, GetRepresentativePoint(a).Latitude,
				GetRepresentativePoint(b).Longitude, GetRepresentativePoint(b).Latitude)
		};
	}

	private static double MinDistancePointToLine(GeoPoint p, GeoLineString line)
	{
		var min = double.MaxValue;
		foreach (var pt in line.Points)
		{
			var d = DistanceMeters(p.Longitude, p.Latitude, pt.Lon, pt.Lat);
			if (d < min) min = d;
		}
		return min;
	}

	private static double MinDistancePointToPolygonBoundary(GeoPoint p, GeoPolygon poly)
	{
		var min = double.MaxValue;
		foreach (var ring in poly.Rings)
		{
			foreach (var pt in ring)
			{
				var d = DistanceMeters(p.Longitude, p.Latitude, pt.Lon, pt.Lat);
				if (d < min) min = d;
			}
		}
		return min;
	}

	/// <summary>Ray casting point-in-polygon test.</summary>
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_contains
	//   "Returns TRUE if no point of geography_2 is outside geography_1, and the interiors intersect."
	public static bool PointInPolygon(GeoPoint point, GeoPolygon polygon)
	{
		if (polygon.Rings.Count == 0) return false;
		var shell = polygon.Rings[0];
		if (!RayCast(point.Longitude, point.Latitude, shell)) return false;

		// Check holes — if point is in a hole, it's outside the polygon
		for (int i = 1; i < polygon.Rings.Count; i++)
		{
			if (RayCast(point.Longitude, point.Latitude, polygon.Rings[i]))
				return false;
		}
		return true;
	}

	private static bool RayCast(double x, double y, IReadOnlyList<(double Lon, double Lat)> ring)
	{
		bool inside = false;
		int n = ring.Count;
		for (int i = 0, j = n - 1; i < n; j = i++)
		{
			var xi = ring[i].Lon; var yi = ring[i].Lat;
			var xj = ring[j].Lon; var yj = ring[j].Lat;

			if ((yi > y) != (yj > y) &&
				x < (xj - xi) * (y - yi) / (yj - yi) + xi)
			{
				inside = !inside;
			}
		}
		return inside;
	}

	/// <summary>Approximate area of a polygon on a sphere in square meters.</summary>
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_area
	//   "Returns the area in square meters covered by the polygons in the input GEOGRAPHY."
	public static double Area(GeoPolygon polygon)
	{
		if (polygon.Rings.Count == 0) return 0.0;
		var shellArea = Math.Abs(SphericalExcess(polygon.Rings[0])) * EarthRadiusMeters * EarthRadiusMeters;
		for (int i = 1; i < polygon.Rings.Count; i++)
			shellArea -= Math.Abs(SphericalExcess(polygon.Rings[i])) * EarthRadiusMeters * EarthRadiusMeters;
		return shellArea;
	}

	private static double SphericalExcess(IReadOnlyList<(double Lon, double Lat)> ring)
	{
		// Shoelter-like formula for spherical polygon area (simplified for small polygons)
		double sum = 0;
		int n = ring.Count;
		for (int i = 0; i < n; i++)
		{
			var j = (i + 1) % n;
			var lon1 = DegreesToRadians(ring[i].Lon);
			var lat1 = DegreesToRadians(ring[i].Lat);
			var lon2 = DegreesToRadians(ring[j].Lon);
			var lat2 = DegreesToRadians(ring[j].Lat);
			sum += (lon2 - lon1) * (2 + Math.Sin(lat1) + Math.Sin(lat2));
		}
		return sum / 2;
	}

	/// <summary>Total length of linestring segments in meters.</summary>
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_length
	//   "Returns the total length in meters of the lines in the input GEOGRAPHY."
	public static double Length(GeoLineString line)
	{
		double total = 0;
		for (int i = 1; i < line.Points.Count; i++)
		{
			total += DistanceMeters(line.Points[i - 1].Lon, line.Points[i - 1].Lat,
				line.Points[i].Lon, line.Points[i].Lat);
		}
		return total;
	}

	/// <summary>Perimeter of polygon shell + holes in meters.</summary>
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_perimeter
	//   "Returns the length in meters of the boundary of the polygons."
	public static double Perimeter(GeoPolygon polygon)
	{
		double total = 0;
		foreach (var ring in polygon.Rings)
		{
			for (int i = 1; i < ring.Count; i++)
				total += DistanceMeters(ring[i - 1].Lon, ring[i - 1].Lat, ring[i].Lon, ring[i].Lat);
			if (ring.Count > 1)
				total += DistanceMeters(ring[^1].Lon, ring[^1].Lat, ring[0].Lon, ring[0].Lat);
		}
		return total;
	}

	private static GeoPoint GetRepresentativePoint(GeoValue geo)
	{
		return geo switch
		{
			GeoPoint p => p,
			GeoLineString l => new GeoPoint(l.Points[0].Lon, l.Points[0].Lat),
			GeoPolygon poly => new GeoPoint(poly.Rings[0][0].Lon, poly.Rings[0][0].Lat),
			_ => new GeoPoint(0, 0)
		};
	}

	private static double DegreesToRadians(double degrees) => degrees * Math.PI / 180.0;

	/// <summary>Parse WKT string into a GeoValue.</summary>
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_geogfromtext
	//   "Converts a STRING WKT geometry value into a GEOGRAPHY value."
	public static GeoValue ParseWkt(string wkt)
	{
		if (string.IsNullOrWhiteSpace(wkt)) return GeoEmpty.Instance;

		var upper = wkt.Trim();
		if (upper.Contains("EMPTY", StringComparison.OrdinalIgnoreCase))
			return GeoEmpty.Instance;

		if (upper.StartsWith("POINT", StringComparison.OrdinalIgnoreCase))
		{
			var coords = ExtractCoords(upper, "POINT");
			var parts = coords.Trim().Split([' ', '\t'], StringSplitOptions.RemoveEmptyEntries);
			return new GeoPoint(
				double.Parse(parts[0], CultureInfo.InvariantCulture),
				double.Parse(parts[1], CultureInfo.InvariantCulture));
		}

		if (upper.StartsWith("LINESTRING", StringComparison.OrdinalIgnoreCase))
		{
			var coords = ExtractCoords(upper, "LINESTRING");
			return new GeoLineString(ParsePointList(coords));
		}

		if (upper.StartsWith("POLYGON", StringComparison.OrdinalIgnoreCase))
		{
			var content = upper.Substring(upper.IndexOf('('));
			var rings = ParseRings(content);
			return new GeoPolygon(rings);
		}

		// Fallback
		return GeoEmpty.Instance;
	}

	/// <summary>Parse GeoJSON string into a GeoValue.</summary>
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_geogfromgeojson
	//   "Returns a GEOGRAPHY value that corresponds to the input GeoJSON representation."
	public static GeoValue ParseGeoJson(string json)
	{
		if (string.IsNullOrWhiteSpace(json)) return GeoEmpty.Instance;
		try
		{
			using var doc = System.Text.Json.JsonDocument.Parse(json);
			var root = doc.RootElement;
			var type = root.GetProperty("type").GetString();

			return type switch
			{
				"Point" => ParseGeoJsonPoint(root),
				"LineString" => ParseGeoJsonLineString(root),
				"Polygon" => ParseGeoJsonPolygon(root),
				"GeometryCollection" => GeoEmpty.Instance,
				_ => GeoEmpty.Instance
			};
		}
		catch
		{
			return GeoEmpty.Instance;
		}
	}

	private static GeoPoint ParseGeoJsonPoint(System.Text.Json.JsonElement root)
	{
		var coords = root.GetProperty("coordinates");
		return new GeoPoint(coords[0].GetDouble(), coords[1].GetDouble());
	}

	private static GeoLineString ParseGeoJsonLineString(System.Text.Json.JsonElement root)
	{
		var coords = root.GetProperty("coordinates");
		var points = new List<(double, double)>();
		foreach (var pt in coords.EnumerateArray())
			points.Add((pt[0].GetDouble(), pt[1].GetDouble()));
		return new GeoLineString(points);
	}

	private static GeoPolygon ParseGeoJsonPolygon(System.Text.Json.JsonElement root)
	{
		var coords = root.GetProperty("coordinates");
		var rings = new List<IReadOnlyList<(double, double)>>();
		foreach (var ring in coords.EnumerateArray())
		{
			var pts = new List<(double, double)>();
			foreach (var pt in ring.EnumerateArray())
				pts.Add((pt[0].GetDouble(), pt[1].GetDouble()));
			rings.Add(pts);
		}
		return new GeoPolygon(rings);
	}

	private static string ExtractCoords(string wkt, string prefix)
	{
		var idx = wkt.IndexOf('(');
		if (idx < 0) return "";
		return wkt[(idx + 1)..wkt.LastIndexOf(')')];
	}

	private static List<(double Lon, double Lat)> ParsePointList(string coords)
	{
		var points = new List<(double, double)>();
		foreach (var pair in coords.Split(','))
		{
			var parts = pair.Trim().Split([' ', '\t'], StringSplitOptions.RemoveEmptyEntries);
			if (parts.Length >= 2)
			{
				points.Add((
					double.Parse(parts[0], CultureInfo.InvariantCulture),
					double.Parse(parts[1], CultureInfo.InvariantCulture)));
			}
		}
		return points;
	}

	private static List<IReadOnlyList<(double Lon, double Lat)>> ParseRings(string content)
	{
		var rings = new List<IReadOnlyList<(double, double)>>();
		// Split by ),(  pattern
		var inner = content.Trim();
		if (inner.StartsWith("(")) inner = inner[1..];
		if (inner.EndsWith(")")) inner = inner[..^1];

		// Split rings by "),("
		var ringParts = Regex.Split(inner, @"\)\s*,\s*\(");
		foreach (var ringPart in ringParts)
		{
			var cleaned = ringPart.Trim('(', ')');
			rings.Add(ParsePointList(cleaned));
		}
		return rings;
	}
}
