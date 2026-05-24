using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Round 22 bug fix tests: FORMAT '%T' type-aware SQL literals for BOOL, DATE, TIMESTAMP, DATETIME, TIME.
/// Also verifies ABS overflow behavior for MIN_INT64.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class Round22BugFixTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public Round22BugFixTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
	}

	public async ValueTask DisposeAsync()
	{
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
	// Bug 1: FORMAT('%T', BOOL) should produce uppercase TRUE/FALSE
	// ================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#format_string
	//   %T behavior table: "Produces a string that's a valid GoogleSQL constant"
	//   For BOOL, valid SQL literals are TRUE/FALSE (uppercase keywords)

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Format_T_Bool_True_ReturnsUppercase()
	{
		var result = await S("SELECT FORMAT('%T', TRUE)");
		Assert.Equal("TRUE", result);
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Format_T_Bool_False_ReturnsUppercase()
	{
		var result = await S("SELECT FORMAT('%T', FALSE)");
		Assert.Equal("FALSE", result);
	}

	[Fact]
	public async Task Format_t_Bool_True_ReturnsLowercase()
	{
		// %t produces readable form (lowercase)
		var result = await S("SELECT FORMAT('%t', TRUE)");
		Assert.Equal("true", result);
	}

	[Fact]
	public async Task Format_t_Bool_False_ReturnsLowercase()
	{
		var result = await S("SELECT FORMAT('%t', FALSE)");
		Assert.Equal("false", result);
	}

	// ================================================================
	// Bug 2: FORMAT('%T', DATE) should produce DATE "YYYY-MM-DD"
	// ================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#format_string
	//   %T behavior table: | DATE | 2011-02-03 | DATE "2011-02-03" |

	[Fact]
	public async Task Format_T_Date_ProducesTypeLiteral()
	{
		var result = await S("SELECT FORMAT('%T', DATE '2024-01-01')");
		Assert.Equal("DATE \"2024-01-01\"", result);
	}

	[Fact]
	public async Task Format_t_Date_ProducesReadableForm()
	{
		// %t for DATE is just the date string
		var result = await S("SELECT FORMAT('%t', DATE '2024-01-01')");
		Assert.Equal("2024-01-01", result);
	}

	// ================================================================
	// Bug 2b: FORMAT('%T', TIMESTAMP) should produce TIMESTAMP "..."
	// ================================================================

	// Ref: %T behavior table: | TIMESTAMP | ... | TIMESTAMP "2011-02-03 04:05:06+00" |

	[Fact]
	public async Task Format_T_Timestamp_ProducesTypeLiteral()
	{
		var result = await S("SELECT FORMAT('%T', TIMESTAMP '2024-01-01 12:00:00 UTC')");
		Assert.NotNull(result);
		Assert.StartsWith("TIMESTAMP \"", result);
		Assert.EndsWith("\"", result);
	}

	// ================================================================
	// Bug 2c: FORMAT('%T', DATETIME) should produce DATETIME "..."
	// ================================================================

	[Fact]
	public async Task Format_T_Datetime_ProducesTypeLiteral()
	{
		var result = await S("SELECT FORMAT('%T', DATETIME '2024-01-01 12:00:00')");
		Assert.NotNull(result);
		Assert.StartsWith("DATETIME \"", result);
		Assert.EndsWith("\"", result);
	}

	// ================================================================
	// Bug 2d: FORMAT('%T', TIME) should produce TIME "..."
	// ================================================================

	[Fact]
	public async Task Format_T_Time_ProducesTypeLiteral()
	{
		var result = await S("SELECT FORMAT('%T', TIME '14:30:00')");
		Assert.NotNull(result);
		Assert.StartsWith("TIME \"", result);
		Assert.EndsWith("\"", result);
	}

	// ================================================================
	// Bug 3: ABS(MIN_INT64) should error
	// ================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#abs
	//   "If X is the minimum possible INT64 value, an error is generated."

	[Fact]
	public async Task Abs_MinInt64_ViaExpression_Errors()
	{
		// Use CAST to ensure the value is INT64 (bypasses parser overflow to double)
		await Assert.ThrowsAnyAsync<Exception>(async () =>
			await S("SELECT ABS(CAST(-9223372036854775808 AS INT64))"));
	}

	// ================================================================
	// Verification: other FORMAT '%T' behaviors still correct
	// ================================================================

	[Fact]
	public async Task Format_T_Int64_NoPrefix()
	{
		var result = await S("SELECT FORMAT('%T', 42)");
		Assert.Equal("42", result);
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Format_T_String_SingleQuoted()
	{
		var result = await S("SELECT FORMAT('%T', 'hello')");
		Assert.Equal("'hello'", result);
	}

	[Fact]
	public async Task Format_T_Null_ProducesNULL()
	{
		var result = await S("SELECT FORMAT('%T', CAST(NULL AS INT64))");
		Assert.Equal("NULL", result);
	}

	[Fact]
	public async Task Format_T_Float64_WithDecimal()
	{
		var result = await S("SELECT FORMAT('%T', 3.14)");
		Assert.Equal("3.14", result);
	}
}
