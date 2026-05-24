using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Tests for IGNORE NULLS with NTH_VALUE, LAG, and LEAD navigation functions.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
public class IgnoreNullsNavigationTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public IgnoreNullsNavigationTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		return result.ToList();
	}

	// ---- NTH_VALUE IGNORE NULLS ----

	/// <summary>
	/// NTH_VALUE(val, 2 IGNORE NULLS) should return the 2nd non-null value in the frame.
	/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#nth_value
	///   "If ignore_nulls is true, excludes NULL values from the calculation."
	/// </summary>
	[Fact]
	public async Task NthValue_IgnoreNulls_ReturnsNthNonNullValue()
	{
		var rows = await Query(@"
			SELECT id, val,
				NTH_VALUE(val, 2 IGNORE NULLS) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nth
			FROM UNNEST([
				STRUCT(1 AS id, CAST(NULL AS INT64) AS val),
				STRUCT(2, NULL),
				STRUCT(3, 10),
				STRUCT(4, 20),
				STRUCT(5, 30)
			]) ORDER BY id");

		// 2nd non-null value is 20 (after 10)
		Assert.Equal("20", rows[0]["nth"]?.ToString());
		Assert.Equal("20", rows[2]["nth"]?.ToString());
		Assert.Equal("20", rows[4]["nth"]?.ToString());
	}

	/// <summary>
	/// NTH_VALUE(val, 1 IGNORE NULLS) should return the 1st non-null value.
	/// </summary>
	[Fact]
	public async Task NthValue_IgnoreNulls_FirstNonNull()
	{
		var rows = await Query(@"
			SELECT id, val,
				NTH_VALUE(val, 1 IGNORE NULLS) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nth
			FROM UNNEST([
				STRUCT(1 AS id, CAST(NULL AS INT64) AS val),
				STRUCT(2, NULL),
				STRUCT(3, 10),
				STRUCT(4, 20)
			]) ORDER BY id");

		// 1st non-null value is 10
		Assert.Equal("10", rows[0]["nth"]?.ToString());
		Assert.Equal("10", rows[3]["nth"]?.ToString());
	}

	/// <summary>
	/// NTH_VALUE(val, 3 IGNORE NULLS) when there are fewer than 3 non-null values should return NULL.
	/// </summary>
	[Fact]
	public async Task NthValue_IgnoreNulls_InsufficientNonNulls_ReturnsNull()
	{
		var rows = await Query(@"
			SELECT id,
				NTH_VALUE(val, 3 IGNORE NULLS) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nth
			FROM UNNEST([
				STRUCT(1 AS id, CAST(NULL AS INT64) AS val),
				STRUCT(2, 10),
				STRUCT(3, 20)
			]) ORDER BY id");

		// Only 2 non-null values, requesting 3rd → NULL
		Assert.Null(rows[0]["nth"]);
	}

	// ---- LAG IGNORE NULLS ----

	/// <summary>
	/// LAG(val IGNORE NULLS) with no offset - BigQuery rejects IGNORE NULLS for LAG.
	/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#lag
	///   "IGNORE NULLS and RESPECT NULLS are not allowed for analytic function LAG"
	/// </summary>
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Lag_IgnoreNulls_NoOffset_ReturnsPreviousNonNull()
	{
		var ex = await Assert.ThrowsAsync<Google.GoogleApiException>(() => Query(@"
			SELECT id, val,
				LAG(val IGNORE NULLS) OVER (ORDER BY id) AS lagged
			FROM UNNEST([
				STRUCT(1 AS id, 10 AS val),
				STRUCT(2, CAST(NULL AS INT64)),
				STRUCT(3, CAST(NULL AS INT64)),
				STRUCT(4, 40)
			]) ORDER BY id"));

		Assert.Contains("IGNORE NULLS and RESPECT NULLS are not allowed for analytic function LAG", ex.Message);
	}

	/// <summary>
	/// LAG(val, 1 IGNORE NULLS) - BigQuery rejects IGNORE NULLS for LAG.
	/// </summary>
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Lag_IgnoreNulls_ExplicitOffset_ReturnsPreviousNonNull()
	{
		var ex = await Assert.ThrowsAsync<Google.GoogleApiException>(() => Query(@"
			SELECT id, val,
				LAG(val, 1 IGNORE NULLS) OVER (ORDER BY id) AS lagged
			FROM UNNEST([
				STRUCT(1 AS id, 10 AS val),
				STRUCT(2, CAST(NULL AS INT64)),
				STRUCT(3, 30)
			]) ORDER BY id"));

		Assert.Contains("IGNORE NULLS and RESPECT NULLS are not allowed for analytic function LAG", ex.Message);
	}

	/// <summary>
	/// LAG(val, 2 IGNORE NULLS) - BigQuery rejects IGNORE NULLS for LAG.
	/// </summary>
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Lag_IgnoreNulls_Offset2_Skips2NonNulls()
	{
		var ex = await Assert.ThrowsAsync<Google.GoogleApiException>(() => Query(@"
			SELECT id, val,
				LAG(val, 2 IGNORE NULLS) OVER (ORDER BY id) AS lagged
			FROM UNNEST([
				STRUCT(1 AS id, 10 AS val),
				STRUCT(2, 20),
				STRUCT(3, CAST(NULL AS INT64)),
				STRUCT(4, 40),
				STRUCT(5, 50)
			]) ORDER BY id"));

		Assert.Contains("IGNORE NULLS and RESPECT NULLS are not allowed for analytic function LAG", ex.Message);
	}

	/// <summary>
	/// LAG with IGNORE NULLS and a default value - BigQuery rejects IGNORE NULLS for LAG.
	/// </summary>
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Lag_IgnoreNulls_WithDefault()
	{
		var ex = await Assert.ThrowsAsync<Google.GoogleApiException>(() => Query(@"
			SELECT id, val,
				LAG(val, 1, -1 IGNORE NULLS) OVER (ORDER BY id) AS lagged
			FROM UNNEST([
				STRUCT(1 AS id, CAST(NULL AS INT64) AS val),
				STRUCT(2, 20)
			]) ORDER BY id"));

		Assert.Contains("IGNORE NULLS and RESPECT NULLS are not allowed for analytic function LAG", ex.Message);
	}

	// ---- LEAD IGNORE NULLS ----

	/// <summary>
	/// LEAD(val IGNORE NULLS) - BigQuery rejects IGNORE NULLS for LEAD.
	/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#lead
	///   "IGNORE NULLS and RESPECT NULLS are not allowed for analytic function LEAD"
	/// </summary>
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Lead_IgnoreNulls_NoOffset_ReturnsNextNonNull()
	{
		var ex = await Assert.ThrowsAsync<Google.GoogleApiException>(() => Query(@"
			SELECT id, val,
				LEAD(val IGNORE NULLS) OVER (ORDER BY id) AS led
			FROM UNNEST([
				STRUCT(1 AS id, 10 AS val),
				STRUCT(2, CAST(NULL AS INT64)),
				STRUCT(3, CAST(NULL AS INT64)),
				STRUCT(4, 40)
			]) ORDER BY id"));

		Assert.Contains("IGNORE NULLS and RESPECT NULLS are not allowed for analytic function LEAD", ex.Message);
	}

	/// <summary>
	/// LEAD(val, 1 IGNORE NULLS) - BigQuery rejects IGNORE NULLS for LEAD.
	/// </summary>
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Lead_IgnoreNulls_ExplicitOffset()
	{
		var ex = await Assert.ThrowsAsync<Google.GoogleApiException>(() => Query(@"
			SELECT id, val,
				LEAD(val, 1 IGNORE NULLS) OVER (ORDER BY id) AS led
			FROM UNNEST([
				STRUCT(1 AS id, 10 AS val),
				STRUCT(2, CAST(NULL AS INT64)),
				STRUCT(3, 30)
			]) ORDER BY id"));

		Assert.Contains("IGNORE NULLS and RESPECT NULLS are not allowed for analytic function LEAD", ex.Message);
	}

	/// <summary>
	/// LEAD with IGNORE NULLS and a default value - BigQuery rejects IGNORE NULLS for LEAD.
	/// </summary>
	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Lead_IgnoreNulls_WithDefault()
	{
		var ex = await Assert.ThrowsAsync<Google.GoogleApiException>(() => Query(@"
			SELECT id, val,
				LEAD(val, 1, -1 IGNORE NULLS) OVER (ORDER BY id) AS led
			FROM UNNEST([
				STRUCT(1 AS id, 10 AS val),
				STRUCT(2, CAST(NULL AS INT64))
			]) ORDER BY id"));

		Assert.Contains("IGNORE NULLS and RESPECT NULLS are not allowed for analytic function LEAD", ex.Message);
	}
}
