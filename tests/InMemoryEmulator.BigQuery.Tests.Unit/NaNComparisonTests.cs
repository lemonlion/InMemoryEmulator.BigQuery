using InMemoryEmulator.BigQuery.SqlEngine;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Unit;

/// <summary>
/// Unit tests for NaN comparison semantics (Round 17).
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#floating_point_type
///   "All comparisons with NaN return FALSE, except for != which returns TRUE."
///   "NaN values are grouped together when grouping and sorting."
/// </summary>
public class NaNComparisonTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		return new QueryExecutor(store);
	}

	[Fact]
	public void NaN_Equals_NaN_ReturnsFalse()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT IEEE_DIVIDE(0, 0) = IEEE_DIVIDE(0, 0)");
		Assert.Equal("false", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void NaN_NotEquals_NaN_ReturnsTrue()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT IEEE_DIVIDE(0, 0) != IEEE_DIVIDE(0, 0)");
		Assert.Equal("true", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void NaN_LessThan_Number_ReturnsFalse()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT IEEE_DIVIDE(0, 0) < 1");
		Assert.Equal("false", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void NaN_GreaterThan_Number_ReturnsFalse()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT IEEE_DIVIDE(0, 0) > 1");
		Assert.Equal("false", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void NaN_LessThanOrEqual_ReturnsFalse()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT IEEE_DIVIDE(0, 0) <= 1");
		Assert.Equal("false", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void NaN_GreaterThanOrEqual_ReturnsFalse()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT IEEE_DIVIDE(0, 0) >= 1");
		Assert.Equal("false", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void NaN_Between_ReturnsFalse()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT IEEE_DIVIDE(0, 0) BETWEEN -1 AND 1");
		Assert.Equal("false", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void NaN_In_List_ReturnsFalse()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT IEEE_DIVIDE(0, 0) IN (1, 2, 3)");
		Assert.Equal("false", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void NaN_In_NaN_ReturnsFalse()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT IEEE_DIVIDE(0, 0) IN (IEEE_DIVIDE(0, 0))");
		Assert.Equal("false", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void SimpleCase_NaN_NoMatch()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT CASE IEEE_DIVIDE(0, 0) WHEN IEEE_DIVIDE(0, 0) THEN 'match' ELSE 'no match' END");
		Assert.Equal("no match", rows[0].F[0].V?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_string
	//   "A returned NaN or 0 will not be signed."
	[Fact]
	public void Cast_NaN_AsString_ReturnsNaN()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT CAST(IEEE_DIVIDE(0, 0) AS STRING)");
		Assert.Equal("nan", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Cast_LargeFloat_AsString_UsesLowercaseE()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT CAST(1e20 AS STRING)");
		Assert.Equal("1e+20", rows[0].F[0].V?.ToString());
	}
}
