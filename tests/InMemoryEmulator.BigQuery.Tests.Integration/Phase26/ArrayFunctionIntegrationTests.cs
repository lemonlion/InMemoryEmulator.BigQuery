using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration.Phase26;

/// <summary>
/// Integration tests for Phase 26: Array functions.
/// GENERATE_DATE_ARRAY, GENERATE_TIMESTAMP_ARRAY, IN UNNEST (array membership),
/// LOGICAL_AND/LOGICAL_OR with UNNEST (all/any containment), aggregate subqueries over arrays.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class ArrayFunctionIntegrationTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public ArrayFunctionIntegrationTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
	}

	public async ValueTask DisposeAsync()
	{
		await _fixture.DisposeAsync();
	}

	#region GENERATE_DATE_ARRAY

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_date_array
	//   "Returns an array of dates."
	[Fact]
	public async Task GenerateDateArray_DefaultStep()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY('2016-10-05', '2016-10-08')) AS len", parameters: null);
		Assert.Equal(4L, Convert.ToInt64(results.ToList()[0]["len"]));
	}

	[Fact]
	public async Task GenerateDateArray_WithStep()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY('2016-10-05', '2016-10-09', INTERVAL 2 DAY)) AS len",
			parameters: null);
		Assert.Equal(3L, Convert.ToInt64(results.ToList()[0]["len"]));
	}

	[Fact]
	public async Task GenerateDateArray_MonthStep()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY('2016-01-01', '2016-05-01', INTERVAL 2 MONTH)) AS len",
			parameters: null);
		Assert.Equal(3L, Convert.ToInt64(results.ToList()[0]["len"]));
	}

	#endregion

	#region GENERATE_TIMESTAMP_ARRAY

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_timestamp_array
	[Fact]
	public async Task GenerateTimestampArray_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT ARRAY_LENGTH(GENERATE_TIMESTAMP_ARRAY('2016-10-05 00:00:00', '2016-10-07 00:00:00', INTERVAL 1 DAY)) AS len",
			parameters: null);
		Assert.Equal(3L, Convert.ToInt64(results.ToList()[0]["len"]));
	}

	#endregion

	#region ARRAY_INCLUDES

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task ArrayIncludes_Found()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT 2 IN UNNEST([1, 2, 3]) AS result", parameters: null);
		Assert.True(Convert.ToBoolean(results.ToList()[0]["result"]));
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task ArrayIncludes_NotFound()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT 5 IN UNNEST([1, 2, 3]) AS result", parameters: null);
		Assert.False(Convert.ToBoolean(results.ToList()[0]["result"]));
	}

	#endregion

	#region ARRAY_INCLUDES_ALL / ARRAY_INCLUDES_ANY

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task ArrayIncludesAll_True()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT (SELECT LOGICAL_AND(x IN UNNEST([1, 2, 3, 4])) FROM UNNEST([2, 3]) AS x) AS result", parameters: null);
		Assert.True(Convert.ToBoolean(results.ToList()[0]["result"]));
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task ArrayIncludesAll_False()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT (SELECT LOGICAL_AND(x IN UNNEST([1, 2, 3])) FROM UNNEST([2, 5]) AS x) AS result", parameters: null);
		Assert.False(Convert.ToBoolean(results.ToList()[0]["result"]));
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task ArrayIncludesAny_True()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT (SELECT LOGICAL_OR(x IN UNNEST([1, 2, 3])) FROM UNNEST([5, 2]) AS x) AS result", parameters: null);
		Assert.True(Convert.ToBoolean(results.ToList()[0]["result"]));
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task ArrayIncludesAny_False()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT (SELECT LOGICAL_OR(x IN UNNEST([1, 2, 3])) FROM UNNEST([5, 6]) AS x) AS result", parameters: null);
		Assert.False(Convert.ToBoolean(results.ToList()[0]["result"]));
	}

	#endregion

	#region ARRAY_MAX / ARRAY_MIN

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task ArrayMax_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT (SELECT MAX(x) FROM UNNEST([3, 1, 4, 1, 5]) AS x) AS result", parameters: null);
		Assert.Equal(5L, Convert.ToInt64(results.ToList()[0]["result"]));
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task ArrayMin_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT (SELECT MIN(x) FROM UNNEST([3, 1, 4, 1, 5]) AS x) AS result", parameters: null);
		Assert.Equal(1L, Convert.ToInt64(results.ToList()[0]["result"]));
	}

	#endregion

	#region ARRAY_SUM / ARRAY_AVG

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task ArraySum_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT (SELECT SUM(x) FROM UNNEST([1, 2, 3, 4]) AS x) AS result", parameters: null);
		Assert.Equal(10L, Convert.ToInt64(results.ToList()[0]["result"]));
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task ArrayAvg_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT (SELECT AVG(x) FROM UNNEST([1, 2, 3, 4]) AS x) AS result", parameters: null);
		Assert.Equal(2.5, Convert.ToDouble(results.ToList()[0]["result"]), 5);
	}

	#endregion
}
