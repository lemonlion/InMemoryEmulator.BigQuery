using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for dataset CRUD operations through the real BigQueryClient SDK pipeline.
/// These tests run against all three targets (in-memory, emulator, cloud).
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class DatasetCrudTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public DatasetCrudTests(BigQuerySession session)
	{
		_session = session;
	}

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
	}

	public async ValueTask DisposeAsync()
	{
		await _fixture.DisposeAsync();
	}

	[Fact]
	public async Task Client_CreateDataset_ReturnsDataset()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/insert
		//   "Creates a new empty dataset."

		// Arrange
		var client = await _fixture.GetClientAsync();
		var datasetId = $"test_create_{Guid.NewGuid():N}"[..30];

		try
		{
			// Act
			var dataset = await client.CreateDatasetAsync(datasetId);

			// Assert
			Assert.NotNull(dataset);
			Assert.Equal(datasetId, dataset.Reference.DatasetId);
		}
		finally
		{
			try { await client.DeleteDatasetAsync(datasetId); } catch { }
		}
	}

	[Fact]
	public async Task Client_GetDataset_ReturnsCreated()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/get
		//   "Returns the dataset specified by datasetID."

		// Arrange
		var client = await _fixture.GetClientAsync();
		var datasetId = $"test_get_{Guid.NewGuid():N}"[..30];

		try
		{
			await client.CreateDatasetAsync(datasetId);

			// Act
			var dataset = await client.GetDatasetAsync(datasetId);

			// Assert
			Assert.NotNull(dataset);
			Assert.Equal(datasetId, dataset.Reference.DatasetId);
		}
		finally
		{
			try { await client.DeleteDatasetAsync(datasetId); } catch { }
		}
	}

	[Fact]
	public async Task Client_ListDatasets_ReturnsAll()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list
		//   "Lists all datasets in the specified project."

		// Arrange
		var client = await _fixture.GetClientAsync();
		var ds1 = $"test_list1_{Guid.NewGuid():N}"[..30];
		var ds2 = $"test_list2_{Guid.NewGuid():N}"[..30];

		try
		{
			await client.CreateDatasetAsync(ds1);
			await client.CreateDatasetAsync(ds2);

			// Act
			var datasets = client.ListDatasets().ToList();

			// Assert
			Assert.Contains(datasets, d => d.Reference.DatasetId == ds1);
			Assert.Contains(datasets, d => d.Reference.DatasetId == ds2);
		}
		finally
		{
			try { await client.DeleteDatasetAsync(ds1); } catch { }
			try { await client.DeleteDatasetAsync(ds2); } catch { }
		}
	}

	[Fact]
	public async Task Client_DeleteDataset_Succeeds()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/delete
		//   "Deletes the dataset specified by the datasetId value."

		// Arrange
		var client = await _fixture.GetClientAsync();
		var datasetId = $"test_delete_{Guid.NewGuid():N}"[..30];
		await client.CreateDatasetAsync(datasetId);

		// Act
		await client.DeleteDatasetAsync(datasetId);

		// Assert — getting deleted dataset should throw
		var ex = await Assert.ThrowsAsync<Google.GoogleApiException>(
			() => client.GetDatasetAsync(datasetId));
		Assert.Equal(System.Net.HttpStatusCode.NotFound, (System.Net.HttpStatusCode)ex.HttpStatusCode);
	}

	[Fact]
	public async Task Client_GetDeletedDataset_ThrowsNotFound()
	{
		// Arrange
		var client = await _fixture.GetClientAsync();
		var datasetId = $"test_notfound_{Guid.NewGuid():N}"[..30];

		// Act & Assert
		var ex = await Assert.ThrowsAsync<Google.GoogleApiException>(
			() => client.GetDatasetAsync(datasetId));
		Assert.Equal(System.Net.HttpStatusCode.NotFound, (System.Net.HttpStatusCode)ex.HttpStatusCode);
	}
}
