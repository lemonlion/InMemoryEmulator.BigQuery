using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for NULL propagation in LPAD, RPAD, TIMESTAMP_ADD, TIMESTAMP_SUB.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#lpad
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_add
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class NullPropagationFunctionTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public NullPropagationFunctionTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_npf_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }

	// ===== LPAD NULL propagation =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#lpad
	//   "Returns NULL if any input is NULL."

	[Fact]
	public async Task Lpad_NullInput_ReturnsNull()
	{
		Assert.Null(await S("SELECT LPAD(CAST(NULL AS STRING), 5, '0')"));
	}

	[Fact]
	public async Task Lpad_NullLength_ReturnsNull()
	{
		Assert.Null(await S("SELECT LPAD('hello', CAST(NULL AS INT64), '0')"));
	}

	[Fact]
	public async Task Lpad_NullPad_ReturnsNull()
	{
		Assert.Null(await S("SELECT LPAD('hello', 10, CAST(NULL AS STRING))"));
	}

	[Fact]
	public async Task Lpad_Normal_Works()
	{
		Assert.Equal("00hello", await S("SELECT LPAD('hello', 7, '0')"));
	}

	// ===== RPAD NULL propagation =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#rpad
	//   "Returns NULL if any input is NULL."

	[Fact]
	public async Task Rpad_NullInput_ReturnsNull()
	{
		Assert.Null(await S("SELECT RPAD(CAST(NULL AS STRING), 5, '0')"));
	}

	[Fact]
	public async Task Rpad_NullLength_ReturnsNull()
	{
		Assert.Null(await S("SELECT RPAD('hello', CAST(NULL AS INT64), '0')"));
	}

	[Fact]
	public async Task Rpad_NullPad_ReturnsNull()
	{
		Assert.Null(await S("SELECT RPAD('hello', 10, CAST(NULL AS STRING))"));
	}

	[Fact]
	public async Task Rpad_Normal_Works()
	{
		Assert.Equal("hello00", await S("SELECT RPAD('hello', 7, '0')"));
	}

	// ===== TIMESTAMP_ADD NULL propagation =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_add
	//   "Returns NULL if any argument is NULL."

	[Fact]
	public async Task TimestampAdd_NullTimestamp_ReturnsNull()
	{
		Assert.Null(await S("SELECT TIMESTAMP_ADD(CAST(NULL AS TIMESTAMP), INTERVAL 1 HOUR)"));
	}

	[Fact]
	public async Task TimestampAdd_NullInterval_ReturnsNull()
	{
		Assert.Null(await S("SELECT TIMESTAMP_ADD(TIMESTAMP '2024-01-01 00:00:00', INTERVAL CAST(NULL AS INT64) HOUR)"));
	}

	[Fact]
	public async Task TimestampAdd_Normal_Works()
	{
		var v = await S("SELECT CAST(TIMESTAMP_ADD(TIMESTAMP '2024-01-01 00:00:00', INTERVAL 1 HOUR) AS STRING)");
		Assert.Contains("2024-01-01", v!);
	}

	// ===== TIMESTAMP_SUB NULL propagation =====
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_sub

	[Fact]
	public async Task TimestampSub_NullTimestamp_ReturnsNull()
	{
		Assert.Null(await S("SELECT TIMESTAMP_SUB(CAST(NULL AS TIMESTAMP), INTERVAL 1 HOUR)"));
	}

	[Fact]
	public async Task TimestampSub_Normal_Works()
	{
		var v = await S("SELECT CAST(TIMESTAMP_SUB(TIMESTAMP '2024-01-01 12:00:00', INTERVAL 1 HOUR) AS STRING)");
		Assert.Contains("2024-01-01", v!);
	}
}
