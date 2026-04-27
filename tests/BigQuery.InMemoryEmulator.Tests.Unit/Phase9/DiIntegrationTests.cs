using Google.Cloud.BigQuery.V2;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase9;

/// <summary>
/// Phase 9: DI integration tests for UseInMemoryBigQuery extension method.
/// </summary>
public class DiIntegrationTests
{
	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets
	//   "The dataset resource provides access to your datasets."

	[Fact]
	public void UseInMemoryBigQuery_ReplacesSingletonRegistration()
	{
		var services = new ServiceCollection();
		services.AddSingleton<BigQueryClient>(_ =>
			throw new InvalidOperationException("Should not be called"));

		services.UseInMemoryBigQuery();

		var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<BigQueryClient>();
		Assert.NotNull(client);
		Assert.IsType<BigQueryClientImpl>(client);
	}

	[Fact]
	public void UseInMemoryBigQuery_PreservesSingletonLifetime()
	{
		var services = new ServiceCollection();
		services.AddSingleton<BigQueryClient>(_ =>
			throw new InvalidOperationException("Should not be called"));

		services.UseInMemoryBigQuery();

		var descriptor = services.Single(d => d.ServiceType == typeof(BigQueryClient));
		Assert.Equal(ServiceLifetime.Singleton, descriptor.Lifetime);
	}

	[Fact]
	public void UseInMemoryBigQuery_PreservesTransientLifetime()
	{
		var services = new ServiceCollection();
		services.AddTransient<BigQueryClient>(_ =>
			throw new InvalidOperationException("Should not be called"));

		services.UseInMemoryBigQuery();

		var descriptor = services.Single(d => d.ServiceType == typeof(BigQueryClient));
		Assert.Equal(ServiceLifetime.Transient, descriptor.Lifetime);
	}

	[Fact]
	public void UseInMemoryBigQuery_PreservesScopedLifetime()
	{
		var services = new ServiceCollection();
		services.AddScoped<BigQueryClient>(_ =>
			throw new InvalidOperationException("Should not be called"));

		services.UseInMemoryBigQuery();

		var descriptor = services.Single(d => d.ServiceType == typeof(BigQueryClient));
		Assert.Equal(ServiceLifetime.Scoped, descriptor.Lifetime);
	}

	[Fact]
	public void UseInMemoryBigQuery_NoExistingRegistration_DefaultsSingleton()
	{
		var services = new ServiceCollection();
		services.UseInMemoryBigQuery();

		var descriptor = services.Single(d => d.ServiceType == typeof(BigQueryClient));
		Assert.Equal(ServiceLifetime.Singleton, descriptor.Lifetime);
	}

	[Fact]
	public void UseInMemoryBigQuery_WithProjectId_UsesCustomProject()
	{
		var services = new ServiceCollection();
		services.UseInMemoryBigQuery(options => options.ProjectId = "my-project");

		var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<BigQueryClient>();
		Assert.Equal("my-project", client.ProjectId);
	}

	[Fact]
	public async Task UseInMemoryBigQuery_WithDatasets_CreatesAll()
	{
		var services = new ServiceCollection();
		services.UseInMemoryBigQuery(options =>
		{
			options.AddDataset("dataset_one");
			options.AddDataset("dataset_two");
		});

		var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<BigQueryClient>();

		var ds1 = await client.GetDatasetAsync("dataset_one");
		var ds2 = await client.GetDatasetAsync("dataset_two");
		Assert.NotNull(ds1);
		Assert.NotNull(ds2);
	}

	[Fact]
	public void UseInMemoryBigQuery_OnClientCreated_Fires()
	{
		InMemoryBigQueryResult? capturedResult = null;
		var services = new ServiceCollection();
		services.UseInMemoryBigQuery(options =>
		{
			options.OnClientCreated = result => capturedResult = result;
		});

		var provider = services.BuildServiceProvider();
		_ = provider.GetRequiredService<BigQueryClient>();

		Assert.NotNull(capturedResult);
		Assert.NotNull(capturedResult!.Store);
		Assert.NotNull(capturedResult.Handler);
	}

	[Fact]
	public void UseInMemoryBigQuery_SingletonReturnsSameInstance()
	{
		var services = new ServiceCollection();
		services.AddSingleton<BigQueryClient>(_ =>
			throw new InvalidOperationException("Should not be called"));
		services.UseInMemoryBigQuery();

		var provider = services.BuildServiceProvider();
		var client1 = provider.GetRequiredService<BigQueryClient>();
		var client2 = provider.GetRequiredService<BigQueryClient>();
		Assert.Same(client1, client2);
	}

	[Fact]
	public void UseInMemoryBigQuery_TransientReturnsDifferentInstances()
	{
		var services = new ServiceCollection();
		services.AddTransient<BigQueryClient>(_ =>
			throw new InvalidOperationException("Should not be called"));
		services.UseInMemoryBigQuery();

		var provider = services.BuildServiceProvider();
		var client1 = provider.GetRequiredService<BigQueryClient>();
		var client2 = provider.GetRequiredService<BigQueryClient>();
		Assert.NotSame(client1, client2);
	}

	[Fact]
	public void UseInMemoryBigQuery_RemovesMultipleExistingRegistrations()
	{
		var services = new ServiceCollection();
		services.AddSingleton<BigQueryClient>(_ =>
			throw new InvalidOperationException("First"));
		services.AddSingleton<BigQueryClient>(_ =>
			throw new InvalidOperationException("Second"));

		services.UseInMemoryBigQuery();

		var descriptors = services.Where(d => d.ServiceType == typeof(BigQueryClient)).ToList();
		Assert.Single(descriptors);
	}
}
