using Google.Cloud.BigQuery.V2;
using Microsoft.Extensions.DependencyInjection;

namespace BigQuery.InMemoryEmulator;

/// <summary>
/// DI integration for replacing BigQueryClient with the in-memory emulator.
/// </summary>
public static class ServiceCollectionExtensions
{
	/// <summary>
	/// Replaces any registered <see cref="BigQueryClient"/> with an in-memory emulator.
	/// Preserves the original service lifetime.
	/// </summary>
	public static IServiceCollection UseInMemoryBigQuery(
		this IServiceCollection services,
		Action<InMemoryBigQueryOptions>? configure = null)
	{
		var options = new InMemoryBigQueryOptions();
		configure?.Invoke(options);

		// Remove existing BigQueryClient registrations
		var existingDescriptor = services.FirstOrDefault(d => d.ServiceType == typeof(BigQueryClient));
		var lifetime = existingDescriptor?.Lifetime ?? ServiceLifetime.Singleton;
		services.RemoveAll<BigQueryClient>();

		// Register factory that creates the in-memory client
		services.Add(new ServiceDescriptor(typeof(BigQueryClient), _ =>
		{
			var builder = InMemoryBigQuery.Builder().WithProjectId(options.ProjectId);
			foreach (var (datasetId, cfg) in options.Datasets)
				builder.AddDataset(datasetId, cfg);

			var result = builder.Build();
			options.OnClientCreated?.Invoke(result);
			return result.Client;
		}, lifetime));

		return services;
	}

	private static void RemoveAll<T>(this IServiceCollection services)
	{
		var descriptors = services.Where(d => d.ServiceType == typeof(T)).ToList();
		foreach (var descriptor in descriptors)
			services.Remove(descriptor);
	}
}
