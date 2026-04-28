using Google.Cloud.BigQuery.Storage.V1;

namespace BigQuery.InMemoryEmulator.StorageApi;

/// <summary>
/// Factory for creating in-memory <see cref="BigQueryReadClient"/> instances
/// backed by an <see cref="InMemoryDataStore"/>.
/// </summary>
/// <remarks>
/// Ref: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1
///   "BigQuery Read API — The Read API can be used to read data from BigQuery."
///
/// Uses a custom <see cref="Grpc.Core.CallInvoker"/> to intercept gRPC calls,
/// so no real gRPC server or network is required.
/// </remarks>
public static class InMemoryBigQueryReadClientFactory
{
	/// <summary>
	/// Creates a <see cref="BigQueryReadClient"/> that reads from the given in-memory data store.
	/// </summary>
	/// <param name="store">The in-memory data store containing BigQuery tables.</param>
	/// <returns>A fully functional <see cref="BigQueryReadClient"/> for testing.</returns>
	/// <example>
	/// <code>
	/// var result = InMemoryBigQuery.Create("my-project", "my-dataset", ds =&gt;
	///     ds.AddTable("my-table", schema));
	///
	/// var readClient = InMemoryBigQueryReadClientFactory.Create(result.Store);
	///
	/// var session = readClient.CreateReadSession(
	///     parent: "projects/my-project",
	///     readSession: new ReadSession { Table = "projects/my-project/datasets/my-dataset/tables/my-table" },
	///     maxStreamCount: 1);
	/// </code>
	/// </example>
	public static BigQueryReadClient Create(InMemoryDataStore store)
	{
		var callInvoker = new InMemoryBigQueryReadCallInvoker(store);

		// Ref: https://cloud.google.com/dotnet/docs/reference/Google.Cloud.BigQuery.Storage.V1/latest/Google.Cloud.BigQuery.Storage.V1.BigQueryReadClientBuilder
		//   "BigQueryReadClientBuilder builds a BigQueryReadClient from configuration."
		// Setting CallInvoker directly bypasses channel/credential creation.
		var builder = new BigQueryReadClientBuilder
		{
			CallInvoker = callInvoker,
		};

		return builder.Build();
	}
}
