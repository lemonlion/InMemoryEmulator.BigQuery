using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Integration tests for bugs fixed in research round 16:
/// - FORMAT %e/%E: 3-digit exponent normalized to 2-digit (BigQuery standard)
/// - FORMAT %g/%G: Default precision 6, lowercase e for %g
/// - FORMAT %T: String values produce quoted SQL literal
/// </summary>
[Collection(IntegrationCollection.Name)]
public class Round16BugFixTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public Round16BugFixTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_r16_{Guid.NewGuid():N}"[..30];
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
	// Bug 1: FORMAT %e produces 3-digit exponent (e+000) instead of 2-digit (e+00)
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements
	//   "e: Scientific notation (mantissa/exponent), lowercase" → example: 3.926500e+02
	// ================================================================

	[Fact]
	public async Task Format_E_DefaultPrecision_TwoDigitExponent()
	{
		var result = await S("SELECT FORMAT('%e', 100.0)");
		Assert.Equal("1.000000e+02", result);
	}

	[Fact]
	public async Task Format_E_CustomPrecision_TwoDigitExponent()
	{
		var result = await S("SELECT FORMAT('%.2e', 3.14)");
		Assert.Equal("3.14e+00", result);
	}

	[Fact]
	public async Task Format_E_Basic()
	{
		var result = await S("SELECT FORMAT('%e', 3.14)");
		Assert.Equal("3.140000e+00", result);
	}

	// ================================================================
	// Bug 2: FORMAT %E produces 3-digit exponent (E+000) instead of 2-digit (E+00)
	// Same root cause as Bug 1, uppercase variant.
	// ================================================================

	[Fact]
	public async Task Format_E_Uppercase_TwoDigitExponent()
	{
		var result = await S("SELECT FORMAT('%E', 3.14)");
		Assert.Equal("3.140000E+00", result);
	}

	// ================================================================
	// Bug 3: FORMAT %T for STRING does not produce single-quoted literal
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements
	//   "%T" table: STRING → "quoted string literal" (e.g., 'sample')
	// ================================================================

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Format_T_String_SingleQuotes()
	{
		var result = await S("SELECT FORMAT('%T', 'hello')");
		Assert.Equal("'hello'", result);
	}

	[Fact]
	public async Task Format_T_Integer_NoQuotes()
	{
		// %T for INT64 produces just the number, no quotes
		var result = await S("SELECT FORMAT('%T', 42)");
		Assert.Equal("42", result);
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Format_T_Boolean_NoQuotes()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#format_string
		//   %T produces "a valid SQL literal" for BOOL → TRUE/FALSE (uppercase)
		var result = await S("SELECT FORMAT('%T', true)");
		Assert.Equal("TRUE", result);
	}

	[Fact]
	public async Task Format_T_Null_ProducesNULL()
	{
		// %T for NULL produces "NULL"
		var result = await S("SELECT FORMAT('%T', CAST(NULL AS INT64))");
		Assert.Equal("NULL", result);
	}

	// ================================================================
	// Bug 4: FORMAT %g doesn't apply default precision 6
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements
	//   "%g" default precision is 6. Uses scientific notation when exponent >= p.
	// ================================================================

	[Fact]
	public async Task Format_G_LargeNumber_ScientificNotation()
	{
		// 39265000.0 with precision 6: exponent=7 >= p=6, uses scientific
		var result = await S("SELECT FORMAT('%g', 39265000.0)");
		Assert.Equal("3.9265e+07", result);
	}

	[Fact]
	public async Task Format_G_SmallNumber_DecimalNotation()
	{
		// 392.65 with precision 6: exponent=2 < p=6, uses decimal
		var result = await S("SELECT FORMAT('%g', 392.65)");
		Assert.Equal("392.65", result);
	}

	[Fact]
	public async Task Format_G_VerySmall_ScientificLowercase()
	{
		// 0.00001 with precision 6: exponent=-5 < -4, uses scientific with lowercase e
		var result = await S("SELECT FORMAT('%g', 0.00001)");
		Assert.Equal("1e-05", result);
	}

	// ================================================================
	// Verify %t (lowercase) is unchanged - unquoted for strings
	// ================================================================

	[Fact]
	public async Task Format_t_String_Unquoted()
	{
		var result = await S("SELECT FORMAT('%t', 'hello')");
		Assert.Equal("hello", result);
	}

	[Fact]
	public async Task Format_t_Date_Readable()
	{
		var result = await S("SELECT FORMAT('%t', DATE '2024-01-01')");
		Assert.Equal("2024-01-01", result);
	}
}
