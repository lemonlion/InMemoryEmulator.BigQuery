using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;

namespace BigQuery.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Fixture that creates datasets and tables against a real Google Cloud BigQuery instance.
/// Requires BIGQUERY_PROJECT_ID environment variable and Application Default Credentials.
/// </summary>
public sealed class CloudTestFixture : ITestDatasetFixture
{
	private readonly BigQuerySession _session;
	private readonly List<string> _createdDatasets = [];

	public CloudTestFixture(BigQuerySession session) => _session = session;

	public TestTarget Target => TestTarget.BigQueryCloud;
	public bool IsRemote => true;

	public Task<BigQueryClient> GetClientAsync()
	{
		return Task.FromResult(_session.RemoteClient
			?? throw new InvalidOperationException("Cloud client not initialized"));
	}

	public async Task<BigQueryDataset> CreateDatasetAsync(
		string datasetId,
		CreateDatasetOptions? options = null)
	{
		var client = await GetClientAsync();
		var dataset = await client.CreateDatasetAsync(datasetId, options: options);
		_createdDatasets.Add(datasetId);
		return dataset;
	}

	public async Task<BigQueryTable> CreateTableAsync(
		string datasetId,
		string tableId,
		TableSchema schema,
		CreateTableOptions? options = null)
	{
		var client = await GetClientAsync();
		return await client.CreateTableAsync(datasetId, tableId, schema, options);
	}

	public async ValueTask DisposeAsync()
	{
		if (_session.RemoteClient is { } client)
		{
			foreach (var datasetId in _createdDatasets)
			{
				try
				{
					await client.DeleteDatasetAsync(datasetId, new DeleteDatasetOptions { DeleteContents = true });
				}
				catch
				{
					// Best-effort cleanup
				}
			}
		}
		_createdDatasets.Clear();
	}
}
