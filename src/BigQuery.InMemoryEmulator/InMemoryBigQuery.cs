using Google.Apis.Bigquery.v2;
using Google.Apis.Json;
using Google.Apis.Services;
using Google.Cloud.BigQuery.V2;
using Newtonsoft.Json;

namespace BigQuery.InMemoryEmulator;

/// <summary>
/// Entry point for creating in-memory BigQuery instances.
/// Returns a real <see cref="BigQueryClient"/> backed by an in-memory data store
/// via HTTP interception.
/// </summary>
/// <remarks>
/// Ref: https://cloud.google.com/bigquery/docs/reference/rest
///   "The BigQuery API provides access to BigQuery resources and operations."
/// The SDK sends HTTP requests which our FakeBigQueryHandler intercepts.
/// </remarks>
public static class InMemoryBigQuery
{
	/// <summary>
	/// Creates an in-memory BigQuery instance with an optional pre-configured dataset.
	/// </summary>
	/// <param name="projectId">GCP project ID for the emulated instance.</param>
	/// <param name="datasetId">Optional dataset to create immediately.</param>
	/// <param name="configureDataset">Optional callback to configure the dataset.</param>
	/// <returns>An <see cref="InMemoryBigQueryResult"/> with a real BigQueryClient.</returns>
	public static InMemoryBigQueryResult Create(
		string projectId = "test-project",
		string? datasetId = null,
		Action<InMemoryDatasetBuilder>? configureDataset = null)
	{
		var store = new InMemoryDataStore(projectId);

		if (datasetId is not null)
		{
			var dataset = new InMemoryDataset(datasetId);
			store.Datasets[datasetId] = dataset;

			if (configureDataset is not null)
			{
				var builder = new InMemoryDatasetBuilder(dataset);
				configureDataset(builder);
			}
		}

		var handler = new FakeBigQueryHandler(store);
		var factory = new FakeBigQueryHttpClientFactory(handler);

		var initializer = new BaseClientService.Initializer
		{
			HttpClientFactory = factory,
			ApplicationName = "BigQuery.InMemoryEmulator",
			// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
			//   DATETIME/DATE/TIME values are returned as strings.
			//   Newtonsoft.Json default DateParseHandling.DateTime auto-converts
			//   date-like strings to DateTime objects, causing InvalidCastException
			//   in BigQueryRow.ConvertSingleValue which does (string)rawValue.
			Serializer = CreateSerializer(),
		};
		var service = new BigqueryService(initializer);
		var client = new BigQueryClientImpl(projectId, service);

		return new InMemoryBigQueryResult(client, store, handler, service);
	}

	/// <summary>
	/// Creates a builder for more complex multi-dataset setups.
	/// </summary>
	public static InMemoryBigQueryBuilder Builder() => new();

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
	//   DATETIME/DATE/TIME values are returned as formatted strings in the REST API.
	//   Newtonsoft.Json default DateParseHandling.DateTime would auto-convert these
	//   date-formatted strings to DateTime objects during deserialization, causing
	//   InvalidCastException in BigQueryRow which casts cell values via (string)rawValue.
	internal static NewtonsoftJsonSerializer CreateSerializer()
	{
		var settings = NewtonsoftJsonSerializer.CreateDefaultSettings();
		settings.DateParseHandling = DateParseHandling.None;
		return new NewtonsoftJsonSerializer(settings);
	}
}
