namespace BigQuery.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Identifies which backend a parity-validated test runs against.
/// Controlled by the <c>BIGQUERY_TEST_TARGET</c> environment variable.
/// </summary>
public enum TestTarget
{
	/// <summary>
	/// Default — FakeBigQueryHandler backed by InMemoryDataStore.
	/// Full SDK HTTP pipeline without a real emulator or cloud instance.
	/// </summary>
	InMemory,

	/// <summary>
	/// goccy/bigquery-emulator running in Docker.
	/// Uses ZetaSQL for SQL parsing and SQLite for storage.
	/// High fidelity but some known divergences from real BigQuery.
	/// </summary>
	BigQueryEmulator,

	/// <summary>
	/// Real Google Cloud BigQuery instance.
	/// Requires GCP credentials and a project ID.
	/// Highest fidelity — the source of truth for behavioral parity.
	/// </summary>
	BigQueryCloud
}
