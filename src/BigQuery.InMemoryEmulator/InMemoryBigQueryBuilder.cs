using Google.Apis.Bigquery.v2;
using Google.Apis.Bigquery.v2.Data;
using Google.Apis.Services;
using Google.Cloud.BigQuery.V2;

namespace BigQuery.InMemoryEmulator;

/// <summary>
/// Fluent builder for creating complex multi-dataset in-memory BigQuery instances.
/// </summary>
public class InMemoryBigQueryBuilder
{
	private string _projectId = "test-project";
	private readonly List<(string DatasetId, Action<InMemoryDatasetBuilder>? Configure)> _datasets = [];
	private Func<HttpRequestMessage, HttpResponseMessage?>? _faultInjector;

	/// <summary>Sets the project ID.</summary>
	public InMemoryBigQueryBuilder WithProjectId(string projectId)
	{
		_projectId = projectId;
		return this;
	}

	/// <summary>Adds a dataset to be created.</summary>
	public InMemoryBigQueryBuilder AddDataset(string datasetId, Action<InMemoryDatasetBuilder>? configure = null)
	{
		_datasets.Add((datasetId, configure));
		return this;
	}

	/// <summary>Sets the fault injector.</summary>
	public InMemoryBigQueryBuilder WithFaultInjector(Func<HttpRequestMessage, HttpResponseMessage?>? injector)
	{
		_faultInjector = injector;
		return this;
	}

	/// <summary>Builds and returns the in-memory BigQuery instance.</summary>
	public InMemoryBigQueryResult Build()
	{
		var store = new InMemoryDataStore(_projectId);

		foreach (var (datasetId, configure) in _datasets)
		{
			var dataset = new InMemoryDataset(datasetId);
			store.Datasets[datasetId] = dataset;

			if (configure is not null)
			{
				var builder = new InMemoryDatasetBuilder(dataset);
				configure(builder);
			}
		}

		var handler = new FakeBigQueryHandler(store);
		if (_faultInjector is not null)
			handler.FaultInjector = _faultInjector;

		var factory = new FakeBigQueryHttpClientFactory(handler);

		var initializer = new BaseClientService.Initializer
		{
			HttpClientFactory = factory,
			ApplicationName = "BigQuery.InMemoryEmulator",
		};
		var service = new BigqueryService(initializer);
		var client = new BigQueryClientImpl(_projectId, service);

		return new InMemoryBigQueryResult(client, store, handler, service);
	}
}
