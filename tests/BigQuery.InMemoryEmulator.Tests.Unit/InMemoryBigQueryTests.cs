using BigQuery.InMemoryEmulator;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit;

/// <summary>
/// Tests the full InMemoryBigQuery.Create() → BigQueryClient pipeline.
/// </summary>
public class InMemoryBigQueryTests
{
	[Fact]
	public async Task Create_ClientCanCreateDataset()
	{
		// Arrange
		using var result = InMemoryBigQuery.Create();

		// Act
		var dataset = await result.Client.CreateDatasetAsync("test_ds");

		// Assert
		Assert.NotNull(dataset);
		Assert.Equal("test_ds", dataset.Reference.DatasetId);
	}

	[Fact]
	public async Task Create_ClientCanGetDataset()
	{
		// Arrange
		using var result = InMemoryBigQuery.Create();
		await result.Client.CreateDatasetAsync("test_ds");

		// Act
		var dataset = await result.Client.GetDatasetAsync("test_ds");

		// Assert
		Assert.NotNull(dataset);
		Assert.Equal("test_ds", dataset.Reference.DatasetId);
	}

	[Fact]
	public async Task Create_ClientCanListDatasets()
	{
		// Arrange
		using var result = InMemoryBigQuery.Create();
		await result.Client.CreateDatasetAsync("ds1");
		await result.Client.CreateDatasetAsync("ds2");

		// Act
		var datasets = result.Client.ListDatasets().ToList();

		// Assert
		Assert.Equal(2, datasets.Count);
	}

	[Fact]
	public async Task Create_ClientCanDeleteDataset()
	{
		// Arrange
		using var result = InMemoryBigQuery.Create();
		await result.Client.CreateDatasetAsync("test_ds");

		// Act
		await result.Client.DeleteDatasetAsync("test_ds");

		// Assert
		var ex = await Assert.ThrowsAsync<Google.GoogleApiException>(
			() => result.Client.GetDatasetAsync("test_ds"));
		Assert.Contains("Not found", ex.Message);
	}

	[Fact]
	public async Task Create_WithPreConfiguredDataset_DatasetExists()
	{
		// Arrange & Act
		using var result = InMemoryBigQuery.Create(datasetId: "preconfigured");

		// Assert
		var dataset = await result.Client.GetDatasetAsync("preconfigured");
		Assert.NotNull(dataset);
		Assert.Equal("preconfigured", dataset.Reference.DatasetId);
	}
}
