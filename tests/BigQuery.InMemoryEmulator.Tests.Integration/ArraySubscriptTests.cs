using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for array subscript operators (OFFSET, ORDINAL, SAFE_OFFSET, SAFE_ORDINAL).
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_subscript_operator
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ArraySubscriptTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public ArraySubscriptTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var rows = (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- OFFSET (0-based) ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_subscript_operator
	//   "array_expression[OFFSET(zero_based_offset)] — Accesses an ARRAY element by position and returns the element."
	[Fact] public async Task Offset_First() => Assert.Equal("10", await Scalar("SELECT [10, 20, 30][OFFSET(0)]"));
	[Fact] public async Task Offset_Middle() => Assert.Equal("20", await Scalar("SELECT [10, 20, 30][OFFSET(1)]"));
	[Fact] public async Task Offset_Last() => Assert.Equal("30", await Scalar("SELECT [10, 20, 30][OFFSET(2)]"));
	[Fact] public async Task Offset_SingleElement() => Assert.Equal("42", await Scalar("SELECT [42][OFFSET(0)]"));
	[Fact] public async Task Offset_StringArray() => Assert.Equal("b", await Scalar("SELECT ['a', 'b', 'c'][OFFSET(1)]"));

	// ---- ORDINAL (1-based) ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_subscript_operator
	//   "array_expression[ORDINAL(one_based_offset)] — Like OFFSET but the index starts at 1."
	[Fact] public async Task Ordinal_First() => Assert.Equal("10", await Scalar("SELECT [10, 20, 30][ORDINAL(1)]"));
	[Fact] public async Task Ordinal_Middle() => Assert.Equal("20", await Scalar("SELECT [10, 20, 30][ORDINAL(2)]"));
	[Fact] public async Task Ordinal_Last() => Assert.Equal("30", await Scalar("SELECT [10, 20, 30][ORDINAL(3)]"));

	// ---- SAFE_OFFSET (0-based, NULL on out-of-range) ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_subscript_operator
	//   "array_expression[SAFE_OFFSET(zero_based_offset)] — Like OFFSET but returns NULL if the index is out of range."
	[Fact] public async Task SafeOffset_Valid() => Assert.Equal("20", await Scalar("SELECT [10, 20, 30][SAFE_OFFSET(1)]"));
	[Fact] public async Task SafeOffset_OutOfRange() => Assert.Null(await Scalar("SELECT [10, 20, 30][SAFE_OFFSET(5)]"));
	[Fact] public async Task SafeOffset_Negative() => Assert.Null(await Scalar("SELECT [10, 20, 30][SAFE_OFFSET(-1)]"));
	[Fact] public async Task SafeOffset_EmptyArray() => Assert.Null(await Scalar("SELECT CAST([] AS ARRAY<INT64>)[SAFE_OFFSET(0)]"));

	// ---- SAFE_ORDINAL (1-based, NULL on out-of-range) ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_subscript_operator
	//   "array_expression[SAFE_ORDINAL(one_based_offset)] — Like ORDINAL but returns NULL if the index is out of range."
	[Fact] public async Task SafeOrdinal_Valid() => Assert.Equal("20", await Scalar("SELECT [10, 20, 30][SAFE_ORDINAL(2)]"));
	[Fact] public async Task SafeOrdinal_OutOfRange() => Assert.Null(await Scalar("SELECT [10, 20, 30][SAFE_ORDINAL(5)]"));
	[Fact] public async Task SafeOrdinal_Zero() => Assert.Null(await Scalar("SELECT [10, 20, 30][SAFE_ORDINAL(0)]"));

	// ---- Combined with aggregate functions ----
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/approximate_aggregate_functions#approx_quantiles
	[Fact]
	public async Task Offset_WithApproxQuantiles()
	{
		var v = await Scalar("SELECT (APPROX_QUANTILES(x, 4))[OFFSET(2)] FROM UNNEST([1, 2, 3, 4, 5]) AS x");
		Assert.NotNull(v);
	}

	[Fact]
	public async Task Ordinal_WithApproxQuantiles()
	{
		var v = await Scalar("SELECT (APPROX_QUANTILES(x, 4))[ORDINAL(3)] FROM UNNEST([1, 2, 3, 4, 5]) AS x");
		Assert.NotNull(v);
	}

	// ---- With expressions as index ----
	[Fact] public async Task Offset_WithExpression() => Assert.Equal("30", await Scalar("SELECT [10, 20, 30][OFFSET(1 + 1)]"));
	[Fact] public async Task Offset_WithCast() => Assert.Equal("20", await Scalar("SELECT [10, 20, 30][OFFSET(CAST(1 AS INT64))]"));

	// ---- Nested array subscript ----
	[Fact] public async Task Offset_OfGenerateArray() => Assert.Equal("5", await Scalar("SELECT (GENERATE_ARRAY(1, 10))[OFFSET(4)]"));

	// ---- With GENERATE_ARRAY ----
	[Fact] public async Task Offset_WithGenerateArray() => Assert.Equal("3", await Scalar("SELECT (GENERATE_ARRAY(1, 5))[OFFSET(2)]"));
	[Fact] public async Task SafeOffset_WithGenerateArray() => Assert.Null(await Scalar("SELECT (GENERATE_ARRAY(1, 3))[SAFE_OFFSET(10)]"));

	// ---- With ARRAY_CONCAT ----
	[Fact] public async Task Offset_WithArrayConcat() => Assert.Equal("4", await Scalar("SELECT (ARRAY_CONCAT([1, 2], [3, 4]))[OFFSET(3)]"));

	// ---- NULL array ----
	[Fact] public async Task Offset_NullArray() => Assert.Null(await Scalar("SELECT CAST(NULL AS ARRAY<INT64>)[SAFE_OFFSET(0)]"));
}
