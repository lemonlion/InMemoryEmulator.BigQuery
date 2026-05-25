using Google;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Integration tests for bugs fixed in research round 15:
/// - DATE_DIFF/DATETIME_DIFF/TIMESTAMP_DIFF with WEEK(weekday) and WEEK/ISOWEEK
/// - POW(0, negative) and POW(negative, non-integer) error conditions
/// - JSON_EXTRACT with consecutive array indices
/// </summary>
[Collection(IntegrationCollection.Name)]
public class Round15BugFixTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public Round15BugFixTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_r15_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ================================================================
	// DATE_DIFF with WEEK(weekday)
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_diff
	// ================================================================

	[Fact]
	public async Task DateDiff_WeekMonday_CrossesBoundary()
	{
		// Jan 1 2024 (Mon) to Jan 8 2024 (Mon) = 1 Monday boundary crossed
		var v = await S("SELECT DATE_DIFF(DATE '2024-01-08', DATE '2024-01-01', WEEK(MONDAY))");
		Assert.Equal("1", v);
	}

	[Fact]
	public async Task DateDiff_WeekMonday_SameWeek()
	{
		// Jan 2 (Tue) to Jan 5 (Fri) in same Monday-week = 0
		var v = await S("SELECT DATE_DIFF(DATE '2024-01-05', DATE '2024-01-02', WEEK(MONDAY))");
		Assert.Equal("0", v);
	}

	[Fact]
	public async Task DateDiff_WeekSunday_CrossesBoundary()
	{
		// Jan 6 (Sat) to Jan 7 (Sun) crosses Sunday boundary = 1
		var v = await S("SELECT DATE_DIFF(DATE '2024-01-07', DATE '2024-01-06', WEEK(SUNDAY))");
		Assert.Equal("1", v);
	}

	[Fact]
	public async Task DateDiff_WeekFriday_TwoWeeks()
	{
		// Jan 1 (Mon) to Jan 15 (Mon): Friday boundaries at Jan 5, Jan 12 = 2
		var v = await S("SELECT DATE_DIFF(DATE '2024-01-15', DATE '2024-01-01', WEEK(FRIDAY))");
		Assert.Equal("2", v);
	}

	[Fact]
	public async Task DateDiff_Isoweek()
	{
		// ISOWEEK = Monday boundaries
		var v = await S("SELECT DATE_DIFF(DATE '2024-01-08', DATE '2024-01-01', ISOWEEK)");
		Assert.Equal("1", v);
	}

	// ================================================================
	// DATETIME_DIFF with WEEK(weekday)
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_diff
	// ================================================================

	[Fact]
	public async Task DatetimeDiff_WeekMonday()
	{
		var v = await S("SELECT DATETIME_DIFF(DATETIME '2024-01-08 10:00:00', DATETIME '2024-01-01 10:00:00', WEEK(MONDAY))");
		Assert.Equal("1", v);
	}

	[Fact]
	public async Task DatetimeDiff_Isoweek()
	{
		var v = await S("SELECT DATETIME_DIFF(DATETIME '2024-01-08 10:00:00', DATETIME '2024-01-01 10:00:00', ISOWEEK)");
		Assert.Equal("1", v);
	}

	// ================================================================
	// TIMESTAMP_DIFF with WEEK / WEEK(weekday)
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_diff
	// ================================================================

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task TimestampDiff_Week()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_diff
		//   "TIMESTAMP_DIFF does not support the WEEK date part when the argument is TIMESTAMP type"
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAsync<GoogleApiException>(async () =>
			await client.ExecuteQueryAsync("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-14 00:00:00 UTC', TIMESTAMP '2024-01-01 00:00:00 UTC', WEEK)", parameters: null));
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task TimestampDiff_WeekMonday()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_diff
		//   "TIMESTAMP_DIFF does not support the WEEK(MONDAY) date part when the argument is TIMESTAMP type"
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAsync<GoogleApiException>(async () =>
			await client.ExecuteQueryAsync("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-14 00:00:00 UTC', TIMESTAMP '2024-01-01 00:00:00 UTC', WEEK(MONDAY))", parameters: null));
	}

	// ================================================================
	// POW error conditions
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#pow
	// ================================================================

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Pow_ZeroNegativeExponent_Throws()
	{
		// "Generates an error if X is 0 and Y is a finite value less than 0."
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAnyAsync<Exception>(
			() => client.ExecuteQueryAsync("SELECT POW(0, -1)", parameters: null));
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Pow_NegativeBaseNonIntegerExponent_Throws()
	{
		// "Generates an error if X is a finite value less than 0 and Y is a noninteger."
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAnyAsync<Exception>(
			() => client.ExecuteQueryAsync("SELECT POW(-1, 0.5)", parameters: null));
	}

	[Fact]
	public async Task Pow_NormalCases_StillWork()
	{
		Assert.Equal("8", await S("SELECT CAST(POW(2, 3) AS INT64)"));
		Assert.Equal("1", await S("SELECT CAST(POW(5, 0) AS INT64)"));
		Assert.Equal("0.25", await S("SELECT POW(2, -2)"));
	}

	// ================================================================
	// JSON_EXTRACT with consecutive array indices
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_extract
	// ================================================================

	[Fact]
	public async Task JsonExtract_ConsecutiveArrayIndices()
	{
		var v = await S("SELECT JSON_EXTRACT('[[[1]]]', '$[0][0][0]')");
		Assert.Equal("1", v);
	}

	[Fact]
	public async Task JsonExtract_NestedObjectArray()
	{
		var v = await S("SELECT JSON_EXTRACT('{\"a\":[[10,20],[30,40]]}', '$.a[1][0]')");
		Assert.Equal("30", v);
	}

	[Fact]
	public async Task JsonExtractScalar_ConsecutiveArrayIndices()
	{
		var v = await S("SELECT JSON_EXTRACT_SCALAR('[[[42]]]', '$[0][0][0]')");
		Assert.Equal("42", v);
	}

	[Fact]
	public async Task JsonExtract_DoubleNestedArray()
	{
		var v = await S("SELECT JSON_EXTRACT('[[1,2],[3,4]]', '$[1][1]')");
		Assert.Equal("4", v);
	}

	[Fact]
	public async Task JsonValue_ConsecutiveArrayIndices()
	{
		var v = await S("SELECT JSON_VALUE('[[[99]]]', '$[0][0][0]')");
		Assert.Equal("99", v);
	}

	[Fact]
	public async Task JsonExtract_SingleArrayIndex_StillWorks()
	{
		var v = await S("SELECT JSON_EXTRACT('{\"a\":[1,2,3]}', '$.a[1]')");
		Assert.Equal("2", v);
	}

	// ================================================================
	// EXTRACT(WEEK(weekday) FROM date) - parser ordering fix
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#extract
	// ================================================================

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Extract_WeekMonday()
	{
		// Jan 8 2024 is a Monday. First Monday = Jan 1. Jan 8 = week 2.
		var v = await S("SELECT EXTRACT(WEEK(MONDAY) FROM DATE '2024-01-08')");
		Assert.Equal("2", v);
	}

	[Fact]
	public async Task Extract_WeekSunday()
	{
		// Jan 7 2024 is a Sunday. First Sunday = Jan 7. Jan 7 = week 1.
		var v = await S("SELECT EXTRACT(WEEK(SUNDAY) FROM DATE '2024-01-07')");
		Assert.Equal("1", v);
	}

	// GO emulator produces different results than real BigQuery.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Extract_WeekSaturday_BeforeFirstSaturday()
	{
		// Jan 1 2024 is Monday. First Saturday = Jan 6. Jan 5 (Fri) = week 0.
		var v = await S("SELECT EXTRACT(WEEK(SATURDAY) FROM DATE '2024-01-05')");
		Assert.Equal("0", v);
	}
}
