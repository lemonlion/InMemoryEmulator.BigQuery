using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 33: DATETIME precision operations.
/// DATETIME_TRUNC MILLISECOND/MICROSECOND, DATETIME_ADD/SUB with MILLISECOND/MICROSECOND.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests33 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests33(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv33_{Guid.NewGuid():N}"[..28];
		await _fixture.CreateDatasetAsync(_ds);
	}

	public async ValueTask DisposeAsync()
	{
		try
		{
			var c = await _fixture.GetClientAsync();
			await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true });
		}
		catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null);
		var rows = result.ToList();
		return rows.Count == 0 ? null : rows[0][0]?.ToString();
	}

	// ───────────────────────────────────────────────────────────────────────────
	// DATETIME_TRUNC MILLISECOND
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_trunc
	//   "MILLISECOND: Truncates to the millisecond."
	// ───────────────────────────────────────────────────────────────────────────

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_string
	//   Trailing zeros in fractional seconds are trimmed.
	[Fact] public async Task DatetimeTrunc_Millisecond()
	{
		var result = await S("SELECT CAST(DATETIME_TRUNC(DATETIME '2024-06-15 12:34:56.789012', MILLISECOND) AS STRING)");
		Assert.Equal("2024-06-15 12:34:56.789", result);
	}

	[Fact] public async Task DatetimeTrunc_Microsecond()
	{
		// MICROSECOND truncation preserves full microsecond precision (no sub-microsecond in BigQuery)
		var result = await S("SELECT CAST(DATETIME_TRUNC(DATETIME '2024-06-15 12:34:56.789012', MICROSECOND) AS STRING)");
		Assert.Equal("2024-06-15 12:34:56.789012", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// DATETIME_ADD with MILLISECOND
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_add
	// ───────────────────────────────────────────────────────────────────────────

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_string
	//   Trailing zeros in fractional seconds are trimmed.
	[Fact] public async Task DatetimeAdd_Millisecond()
	{
		var result = await S("SELECT CAST(DATETIME_ADD(DATETIME '2024-01-01 00:00:00', INTERVAL 500 MILLISECOND) AS STRING)");
		Assert.Equal("2024-01-01 00:00:00.500", result);
	}

	[Fact] public async Task DatetimeAdd_Microsecond()
	{
		var result = await S("SELECT CAST(DATETIME_ADD(DATETIME '2024-01-01 00:00:00', INTERVAL 123456 MICROSECOND) AS STRING)");
		Assert.Equal("2024-01-01 00:00:00.123456", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// DATETIME_SUB with MILLISECOND
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task DatetimeSub_Millisecond()
	{
		var result = await S("SELECT CAST(DATETIME_SUB(DATETIME '2024-01-01 00:00:01', INTERVAL 500 MILLISECOND) AS STRING)");
		Assert.Equal("2024-01-01 00:00:00.500", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// DATETIME_DIFF with MILLISECOND/MICROSECOND
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_diff
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task DatetimeDiff_Millisecond()
	{
		var result = await S(@"
			SELECT DATETIME_DIFF(
				DATETIME '2024-01-01 00:00:01',
				DATETIME '2024-01-01 00:00:00',
				MILLISECOND)");
		Assert.Equal("1000", result);
	}

	[Fact] public async Task DatetimeDiff_Microsecond()
	{
		var result = await S(@"
			SELECT DATETIME_DIFF(
				DATETIME '2024-01-01 00:00:01',
				DATETIME '2024-01-01 00:00:00',
				MICROSECOND)");
		Assert.Equal("1000000", result);
	}
}
