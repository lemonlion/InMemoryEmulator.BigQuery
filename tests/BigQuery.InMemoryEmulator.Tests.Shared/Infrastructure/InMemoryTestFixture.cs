using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;

namespace BigQuery.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Fixture that creates datasets and tables backed by the in-memory emulator.
/// Uses the real BigQueryClient SDK pipeline (HTTP serialization/deserialization)
/// for an apples-to-apples comparison with the emulator and cloud fixtures.
/// </summary>
public sealed class InMemoryTestFixture : ITestDatasetFixture
{
	public TestTarget Target => TestTarget.InMemory;
	public bool IsRemote => false;

	private InMemoryBigQueryResult? _result;
	private InMemoryBigQueryResult Result => _result ??= InMemoryBigQuery.Create();

	public Task<BigQueryClient> GetClientAsync()
	{
		return Task.FromResult(Result.Client);
	}

	public async Task<BigQueryDataset> CreateDatasetAsync(
		string datasetId,
		CreateDatasetOptions? options = null)
	{
		var client = await GetClientAsync();
		return await client.CreateDatasetAsync(datasetId, options: options);
	}

	public async Task<BigQueryTable> CreateTableAsync(
		string datasetId,
		string tableId,
		TableSchema schema,
		CreateTableOptions? options = null)
	{
		var client = await GetClientAsync();
		return await client.CreateTableAsync(datasetId, tableId, schema, options: options);
	}

	public ValueTask DisposeAsync()
	{
		_result?.Dispose();
		_result = null;
		return ValueTask.CompletedTask;
	}
}
