using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 41: SUBSTR negative length error, REPEAT negative error,
/// LEFT/RIGHT negative length error, JSON_EXTRACT_SCALAR non-scalar returns NULL,
/// MOD float division by zero error, DIV with float inputs.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests41 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public ParityVerificationTests41(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
	}

	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string> ScalarAsync(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var row = result.First();
		return row[0]?.ToString() ?? "NULL";
	}

	private async Task AssertThrowsAsync(string sql)
	{
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAnyAsync<Exception>(async () =>
		{
			var result = await client.ExecuteQueryAsync(sql, parameters: null);
			// Force materialization
			_ = result.ToList();
		});
	}

	// ============================================================
	// SUBSTR with negative length → error
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#substr
	//   "If length is negative, the function produces an error."
	[Fact]
	public async Task Substr_NegativeLength_ThrowsError()
	{
		await AssertThrowsAsync("SELECT SUBSTR('hello', 1, -1)");
	}

	// GO emulator crash: query causes emulator process to crash.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task Substr_ZeroLength_ReturnsEmpty()
	{
		var result = await ScalarAsync("SELECT SUBSTR('hello', 1, 0)");
		Assert.Equal("", result);
	}

	[Fact]
	public async Task Substr_PositiveLength_Works()
	{
		var result = await ScalarAsync("SELECT SUBSTR('hello', 2, 3)");
		Assert.Equal("ell", result);
	}

	// ============================================================
	// REPEAT with negative count → error
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#repeat
	//   "This function returns an error if the repetitions value is negative."
	[Fact]
	public async Task Repeat_NegativeCount_ThrowsError()
	{
		await AssertThrowsAsync("SELECT REPEAT('abc', -1)");
	}

	[Fact]
	public async Task Repeat_ZeroCount_ReturnsEmpty()
	{
		var result = await ScalarAsync("SELECT REPEAT('abc', 0)");
		Assert.Equal("", result);
	}

	[Fact]
	public async Task Repeat_NullCount_ReturnsNull()
	{
		var result = await ScalarAsync("SELECT REPEAT('abc', NULL)");
		Assert.Equal("NULL", result);
	}

	// ============================================================
	// LEFT/RIGHT with negative length → error
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#left
	//   "If length is negative, an error will be returned."
	[Fact]
	public async Task Left_NegativeLength_ThrowsError()
	{
		await AssertThrowsAsync("SELECT LEFT('hello', -1)");
	}

	[Fact]
	public async Task Left_ZeroLength_ReturnsEmpty()
	{
		var result = await ScalarAsync("SELECT LEFT('hello', 0)");
		Assert.Equal("", result);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#right
	//   "If length is negative, an error will be returned."
	[Fact]
	public async Task Right_NegativeLength_ThrowsError()
	{
		await AssertThrowsAsync("SELECT RIGHT('hello', -1)");
	}

	[Fact]
	public async Task Right_ZeroLength_ReturnsEmpty()
	{
		var result = await ScalarAsync("SELECT RIGHT('hello', 0)");
		Assert.Equal("", result);
	}

	// ============================================================
	// JSON_EXTRACT_SCALAR / JSON_VALUE — non-scalar returns NULL
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_extract_scalar
	//   "If the selected JSON value is not a scalar... returns NULL."
	[Fact]
	public async Task JsonExtractScalar_Object_ReturnsNull()
	{
		var result = await ScalarAsync(@"SELECT JSON_EXTRACT_SCALAR('{""a"":{""b"":1}}', '$.a')");
		Assert.Equal("NULL", result);
	}

	[Fact]
	public async Task JsonExtractScalar_Array_ReturnsNull()
	{
		var result = await ScalarAsync(@"SELECT JSON_EXTRACT_SCALAR('{""a"":[1,2,3]}', '$.a')");
		Assert.Equal("NULL", result);
	}

	[Fact]
	public async Task JsonExtractScalar_ScalarString_ReturnsValue()
	{
		var result = await ScalarAsync(@"SELECT JSON_EXTRACT_SCALAR('{""a"":""hello""}', '$.a')");
		Assert.Equal("hello", result);
	}

	[Fact]
	public async Task JsonExtractScalar_ScalarNumber_ReturnsValue()
	{
		var result = await ScalarAsync(@"SELECT JSON_EXTRACT_SCALAR('{""a"":42}', '$.a')");
		Assert.Equal("42", result);
	}

	[Fact]
	public async Task JsonValue_Object_ReturnsNull()
	{
		var result = await ScalarAsync(@"SELECT JSON_VALUE('{""a"":{""b"":1}}', '$.a')");
		Assert.Equal("NULL", result);
	}

	// ============================================================
	// MOD with float division by zero → error
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#mod
	//   "Generates a division by zero error if Y is 0."
	[Fact]
	public async Task Mod_FloatDivisionByZero_ThrowsError()
	{
		await AssertThrowsAsync("SELECT MOD(5.0, 0.0)");
	}

	[Fact]
	public async Task Mod_IntDivisionByZero_ThrowsError()
	{
		await AssertThrowsAsync("SELECT MOD(5, 0)");
	}

	// BigQuery docs: MOD only accepts INT64, UINT64, NUMERIC, BIGNUMERIC — not FLOAT64
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#mod
	//   Return type table shows only INT64, NUMERIC, BIGNUMERIC.
	[Fact]
	public async Task Mod_FloatInputs_ThrowsError()
	{
		await AssertThrowsAsync("SELECT MOD(5.5, 2.0)");
	}

	// ============================================================
	// DIV with float inputs — should divide then truncate
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#div
	//   Return type table shows only INT64, NUMERIC, BIGNUMERIC — FLOAT64 is not supported.
	// BigQuery docs: DIV only accepts INT64, UINT64, NUMERIC, BIGNUMERIC — not FLOAT64
	[Fact]
	public async Task Div_FloatInputs_ThrowsError()
	{
		await AssertThrowsAsync("SELECT DIV(1.5, 0.7)");
	}

	[Fact]
	public async Task Div_FloatInputs_Negative_ThrowsError()
	{
		await AssertThrowsAsync("SELECT DIV(-7.5, 2.0)");
	}

	[Fact]
	public async Task Div_FloatDivisionByZero_ThrowsError()
	{
		await AssertThrowsAsync("SELECT DIV(1.5, 0.0)");
	}

	[Fact]
	public async Task Div_IntInputs_Works()
	{
		var result = await ScalarAsync("SELECT DIV(7, 2)");
		Assert.Equal("3", result);
	}
}
