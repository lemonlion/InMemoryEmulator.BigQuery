using Google.Apis.Bigquery.v2;
using Google.Cloud.BigQuery.V2;

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

	public void Dispose()
	{
		Client.Dispose();
		_service.Dispose();
	}
}
