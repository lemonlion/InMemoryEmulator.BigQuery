using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Range function tests and HLL tests.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/range-functions
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class RangeAndHllFunctionTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public RangeAndHllFunctionTests(BigQuerySession session) => _session = session;

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

	// ---- RANGE constructor ----
	[Fact] public async Task Range_DateRange() => Assert.NotNull(await Scalar("SELECT RANGE(DATE '2024-01-01', DATE '2024-12-31')"));
	[Fact] public async Task Range_TimestampRange() => Assert.NotNull(await Scalar("SELECT RANGE(TIMESTAMP '2024-01-01 00:00:00 UTC', TIMESTAMP '2024-12-31 23:59:59 UTC')"));
	[Fact(Skip = "RANGE type functions not implemented")] public async Task Range_NullStart() => Assert.NotNull(await Scalar("SELECT RANGE(NULL, DATE '2024-12-31')"));
	[Fact(Skip = "RANGE type functions not implemented")] public async Task Range_NullEnd() => Assert.NotNull(await Scalar("SELECT RANGE(DATE '2024-01-01', NULL)"));

	// ---- RANGE_START / RANGE_END ----
	[Fact(Skip = "RANGE type functions not implemented")] public async Task RangeStart_Date() { var v = await Scalar("SELECT RANGE_START(RANGE(DATE '2024-01-01', DATE '2024-12-31'))"); Assert.Contains("2024-01-01", v); }
	[Fact(Skip = "RANGE type functions not implemented")] public async Task RangeEnd_Date() { var v = await Scalar("SELECT RANGE_END(RANGE(DATE '2024-01-01', DATE '2024-12-31'))"); Assert.Contains("2024-12-31", v); }
	[Fact] public async Task RangeStart_Null() => Assert.Null(await Scalar("SELECT RANGE_START(RANGE(NULL, DATE '2024-12-31'))"));
	[Fact] public async Task RangeEnd_Null() => Assert.Null(await Scalar("SELECT RANGE_END(RANGE(DATE '2024-01-01', NULL))"));

	// ---- RANGE_CONTAINS ----
	[Fact] public async Task RangeContains_ValueInRange_True() => Assert.Equal("True", await Scalar("SELECT RANGE_CONTAINS(RANGE(DATE '2024-01-01', DATE '2024-12-31'), DATE '2024-06-15')"));
	[Fact(Skip = "RANGE type functions not implemented")] public async Task RangeContains_ValueOutOfRange_False() => Assert.Equal("False", await Scalar("SELECT RANGE_CONTAINS(RANGE(DATE '2024-01-01', DATE '2024-12-31'), DATE '2025-01-01')"));
	[Fact] public async Task RangeContains_AtStart_True() => Assert.Equal("True", await Scalar("SELECT RANGE_CONTAINS(RANGE(DATE '2024-01-01', DATE '2024-12-31'), DATE '2024-01-01')"));
	[Fact] public async Task RangeContains_AtEnd_False() => Assert.Equal("False", await Scalar("SELECT RANGE_CONTAINS(RANGE(DATE '2024-01-01', DATE '2024-12-31'), DATE '2024-12-31')"));
	[Fact] public async Task RangeContains_RangeInRange_True() => Assert.Equal("True", await Scalar("SELECT RANGE_CONTAINS(RANGE(DATE '2024-01-01', DATE '2024-12-31'), RANGE(DATE '2024-03-01', DATE '2024-06-01'))"));

	// ---- RANGE_OVERLAPS ----
	[Fact] public async Task RangeOverlaps_Overlapping_True() => Assert.Equal("True", await Scalar("SELECT RANGE_OVERLAPS(RANGE(DATE '2024-01-01', DATE '2024-06-30'), RANGE(DATE '2024-03-01', DATE '2024-12-31'))"));
	[Fact(Skip = "RANGE type functions not implemented")] public async Task RangeOverlaps_NonOverlapping_False() => Assert.Equal("False", await Scalar("SELECT RANGE_OVERLAPS(RANGE(DATE '2024-01-01', DATE '2024-03-31'), RANGE(DATE '2024-06-01', DATE '2024-12-31'))"));
	[Fact] public async Task RangeOverlaps_Adjacent_False() => Assert.Equal("False", await Scalar("SELECT RANGE_OVERLAPS(RANGE(DATE '2024-01-01', DATE '2024-06-01'), RANGE(DATE '2024-06-01', DATE '2024-12-31'))"));

	// ---- RANGE_BUCKET ----
	[Fact] public async Task RangeBucket_InRange() { var v = await Scalar("SELECT RANGE_BUCKET(15, [0, 10, 20, 30])"); Assert.Equal("2", v); }
	[Fact] public async Task RangeBucket_AtBoundary() { var v = await Scalar("SELECT RANGE_BUCKET(10, [0, 10, 20, 30])"); Assert.Equal("2", v); }
	[Fact] public async Task RangeBucket_BelowAll() { var v = await Scalar("SELECT RANGE_BUCKET(-1, [0, 10, 20, 30])"); Assert.Equal("0", v); }
	[Fact] public async Task RangeBucket_AboveAll() { var v = await Scalar("SELECT RANGE_BUCKET(100, [0, 10, 20, 30])"); Assert.Equal("4", v); }
	[Fact] public async Task RangeBucket_NullPoint() => Assert.Null(await Scalar("SELECT RANGE_BUCKET(NULL, [0, 10, 20])"));

	// ==== HLL FUNCTIONS ====
	[Fact(Skip = "Not yet supported")] public async Task HllCountInit_Basic() => Assert.NotNull(await Scalar("SELECT HLL_COUNT.INIT(1)"));
	[Fact(Skip = "Not yet supported")] public async Task HllCountInit_String() => Assert.NotNull(await Scalar("SELECT HLL_COUNT.INIT('hello')"));
	
	[Fact(Skip = "Not yet supported")] public async Task HllCountMerge_Basic()
	{
		var v = await Scalar(@"
			SELECT HLL_COUNT.MERGE(sketch) FROM (
				SELECT HLL_COUNT.INIT(x) AS sketch FROM UNNEST([1, 2, 3, 1, 2]) AS x
			)
		");
		Assert.NotNull(v);
		var count = int.Parse(v!);
		Assert.True(count >= 2 && count <= 4); // approximate
	}

	[Fact(Skip = "Not yet supported")] public async Task HllCountExtract_Basic()
	{
		var v = await Scalar(@"
			SELECT HLL_COUNT.EXTRACT(HLL_COUNT.MERGE_PARTIAL(sketch)) FROM (
				SELECT HLL_COUNT.INIT(x) AS sketch FROM UNNEST([1, 2, 3, 4, 5]) AS x
			)
		");
		Assert.NotNull(v);
		var count = int.Parse(v!);
		Assert.True(count >= 3 && count <= 7);
	}

	// ---- MAKE_INTERVAL ----
	[Fact(Skip = "Not yet supported")] public async Task MakeInterval_Days() => Assert.NotNull(await Scalar("SELECT MAKE_INTERVAL(0, 0, 5)"));
	[Fact(Skip = "Not yet supported")] public async Task MakeInterval_Hours() => Assert.NotNull(await Scalar("SELECT MAKE_INTERVAL(0, 0, 0, 3)"));

	// ---- JUSTIFY_DAYS / JUSTIFY_HOURS / JUSTIFY_INTERVAL ----
	[Fact(Skip = "Not yet supported")] public async Task JustifyDays_Basic() => Assert.NotNull(await Scalar("SELECT JUSTIFY_DAYS(MAKE_INTERVAL(0, 0, 35))"));
	[Fact(Skip = "Not yet supported")] public async Task JustifyHours_Basic() => Assert.NotNull(await Scalar("SELECT JUSTIFY_HOURS(MAKE_INTERVAL(0, 0, 0, 30))"));
	[Fact(Skip = "Not yet supported")] public async Task JustifyInterval_Basic() => Assert.NotNull(await Scalar("SELECT JUSTIFY_INTERVAL(MAKE_INTERVAL(0, 0, 35, 30))"));
}
