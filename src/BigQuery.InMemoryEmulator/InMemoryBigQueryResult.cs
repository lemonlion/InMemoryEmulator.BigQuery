using Google.Apis.Bigquery.v2;
using Google.Cloud.BigQuery.V2;
using Google.Cloud.BigQuery.Storage.V1;
using BigQuery.InMemoryEmulator.StorageApi;

namespace BigQuery.InMemoryEmulator;

/// <summary>
/// Result of creating an in-memory BigQuery instance.
/// Provides three tiers of access: production-like client, test setup, and diagnostics.
/// </summary>
public sealed class InMemoryBigQueryResult : IDisposable
{
	/// <summary>Tier 1 — Production-like: a real BigQueryClient backed by the in-memory store.</summary>
	public BigQueryClient Client { get; }

	/// <summary>Tier 2 — Test setup: direct access to the backing data store.</summary>
	public InMemoryDataStore Store { get; }

	/// <summary>Tier 3 — Fault injection &amp; diagnostics: the HTTP handler.</summary>
	public FakeBigQueryHandler Handler { get; }

	private readonly BigqueryService _service;

	internal InMemoryBigQueryResult(
		BigQueryClient client,
		InMemoryDataStore store,
		FakeBigQueryHandler handler,
		BigqueryService service)
	{
		Client = client;
		Store = store;
		Handler = handler;
		_service = service;
	}

	/// <summary>Sets or clears the fault injector on the handler.</summary>
	public void SetFaultInjector(Func<HttpRequestMessage, HttpResponseMessage?>? injector)
	{
		Handler.FaultInjector = injector;
	}

	/// <summary>
	/// Creates a <see cref="BigQueryReadClient"/> backed by the same in-memory data store.
	/// Use this for testing code that uses the BigQuery Storage Read API (gRPC).
	/// </summary>
	/// <remarks>
	/// Ref: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1
	///   "BigQuery Read API — The Read API can be used to read data from BigQuery."
	/// </remarks>
	public BigQueryReadClient CreateReadClient()
	{
		return InMemoryBigQueryReadClientFactory.Create(Store);
	}

	public void Dispose()
	{
		Client.Dispose();
		_service.Dispose();
	}
}
