using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;

namespace BigQuery.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Abstraction over dataset/table creation for parity-validated tests.
/// In-memory tests use FakeBigQueryHandler; emulator and cloud tests use real BigQueryClient.
/// </summary>
public interface ITestDatasetFixture : IAsyncDisposable
{
	/// <summary>The backend this fixture targets.</summary>
	TestTarget Target { get; }

	/// <summary>True when running against a remote target (emulator or cloud).</summary>
	bool IsRemote { get; }

	/// <summary>
	/// Gets the BigQueryClient for this fixture.
	/// </summary>
	Task<BigQueryClient> GetClientAsync();

	/// <summary>
	/// Creates a dataset.
	/// </summary>
	/// <param name="datasetId">The dataset ID to create.</param>
	/// <param name="options">Optional creation options.</param>
	Task<BigQueryDataset> CreateDatasetAsync(
		string datasetId,
		CreateDatasetOptions? options = null);

	/// <summary>
	/// Creates a table with the given schema in the specified dataset.
	/// </summary>
	/// <param name="datasetId">The dataset containing the table.</param>
	/// <param name="tableId">The table ID to create.</param>
	/// <param name="schema">The table schema.</param>
	/// <param name="options">Optional creation options.</param>
	Task<BigQueryTable> CreateTableAsync(
		string datasetId,
		string tableId,
		TableSchema schema,
		CreateTableOptions? options = null);
}
