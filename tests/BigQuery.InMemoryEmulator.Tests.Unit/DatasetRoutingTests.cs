using System.Net;
using System.Text;
using BigQuery.InMemoryEmulator;
using Google.Apis.Bigquery.v2.Data;
using Newtonsoft.Json;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit;

/// <summary>
/// Tests FakeBigQueryHandler dataset CRUD routing at the HTTP level.
/// </summary>
public class DatasetRoutingTests
{
	private readonly InMemoryDataStore _store;
	private readonly FakeBigQueryHandler _handler;
	private readonly HttpClient _httpClient;

	public DatasetRoutingTests()
	{
		_store = new InMemoryDataStore("test-project");
		_handler = new FakeBigQueryHandler(_store);
		_httpClient = new HttpClient(_handler)
		{
			BaseAddress = new Uri("https://bigquery.googleapis.com")
		};
	}

	[Fact]
	public async Task CreateDataset_Returns200_WithDataset()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/insert
		//   "Creates a new empty dataset."

		// Arrange
		var body = new Dataset
		{
			DatasetReference = new DatasetReference
			{
				ProjectId = "test-project",
				DatasetId = "new_dataset"
			},
			Description = "A test dataset",
			FriendlyName = "New Dataset",
			Location = "US",
		};
		var json = JsonConvert.SerializeObject(body);
		var content = new StringContent(json, Encoding.UTF8, "application/json");

		// Act
		var response = await _httpClient.PostAsync(
			"/bigquery/v2/projects/test-project/datasets", content);

		// Assert
		Assert.Equal(HttpStatusCode.OK, response.StatusCode);

		var responseJson = await response.Content.ReadAsStringAsync();
		var result = JsonConvert.DeserializeObject<Dataset>(responseJson)!;
		Assert.Equal("new_dataset", result.DatasetReference?.DatasetId);
		Assert.Equal("test-project", result.DatasetReference?.ProjectId);
		Assert.Equal("A test dataset", result.Description);
		Assert.Equal("US", result.Location);

		// Verify stored in data store
		Assert.True(_store.Datasets.ContainsKey("new_dataset"));
	}

	[Fact]
	public async Task CreateDataset_AlreadyExists_Returns409()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/insert
		//   Returns 409 "ALREADY_EXISTS" if dataset already exists.

		// Arrange
		_store.Datasets["existing"] = new InMemoryDataset("existing");
		var body = new Dataset
		{
			DatasetReference = new DatasetReference
			{
				ProjectId = "test-project",
				DatasetId = "existing"
			}
		};
		var json = JsonConvert.SerializeObject(body);
		var content = new StringContent(json, Encoding.UTF8, "application/json");

		// Act
		var response = await _httpClient.PostAsync(
			"/bigquery/v2/projects/test-project/datasets", content);

		// Assert
		Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
	}

	[Fact]
	public async Task GetDataset_ReturnsDataset()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/get
		//   "Returns the dataset specified by datasetID."

		// Arrange
		_store.Datasets["my_ds"] = new InMemoryDataset("my_ds")
		{
			Description = "My dataset",
			Location = "EU",
		};

		// Act
		var response = await _httpClient.GetAsync(
			"/bigquery/v2/projects/test-project/datasets/my_ds");

		// Assert
		Assert.Equal(HttpStatusCode.OK, response.StatusCode);

		var responseJson = await response.Content.ReadAsStringAsync();
		var result = JsonConvert.DeserializeObject<Dataset>(responseJson)!;
		Assert.Equal("my_ds", result.DatasetReference?.DatasetId);
		Assert.Equal("My dataset", result.Description);
		Assert.Equal("EU", result.Location);
	}

	[Fact]
	public async Task GetDataset_NotFound_Returns404()
	{
		// Act
		var response = await _httpClient.GetAsync(
			"/bigquery/v2/projects/test-project/datasets/nonexistent");

		// Assert
		Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
	}

	[Fact]
	public async Task ListDatasets_ReturnsAll()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list
		//   "Lists all datasets in the specified project."

		// Arrange
		_store.Datasets["ds1"] = new InMemoryDataset("ds1");
		_store.Datasets["ds2"] = new InMemoryDataset("ds2");

		// Act
		var response = await _httpClient.GetAsync(
			"/bigquery/v2/projects/test-project/datasets");

		// Assert
		Assert.Equal(HttpStatusCode.OK, response.StatusCode);

		var responseJson = await response.Content.ReadAsStringAsync();
		var result = JsonConvert.DeserializeObject<DatasetList>(responseJson)!;
		Assert.Equal(2, result.Datasets?.Count);
	}

	[Fact]
	public async Task ListDatasets_EmptyProject_ReturnsEmptyList()
	{
		// Act
		var response = await _httpClient.GetAsync(
			"/bigquery/v2/projects/test-project/datasets");

		// Assert
		Assert.Equal(HttpStatusCode.OK, response.StatusCode);

		var responseJson = await response.Content.ReadAsStringAsync();
		var result = JsonConvert.DeserializeObject<DatasetList>(responseJson)!;
		Assert.Equal("bigquery#datasetList", result.Kind);
	}

	[Fact]
	public async Task DeleteDataset_ReturnsNoContent()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/delete
		//   "Deletes the dataset specified by the datasetId value."
		//   "If successful, the response body is an empty JSON object."

		// Arrange
		_store.Datasets["to_delete"] = new InMemoryDataset("to_delete");

		// Act
		var response = await _httpClient.DeleteAsync(
			"/bigquery/v2/projects/test-project/datasets/to_delete");

		// Assert
		Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
		Assert.False(_store.Datasets.ContainsKey("to_delete"));
	}

	[Fact]
	public async Task DeleteDataset_NotFound_Returns404()
	{
		// Act
		var response = await _httpClient.DeleteAsync(
			"/bigquery/v2/projects/test-project/datasets/nonexistent");

		// Assert
		Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
	}

	[Fact]
	public async Task PatchDataset_UpdatesFields()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/patch
		//   "The patch method only replaces fields that are provided in the submitted dataset resource."

		// Arrange
		_store.Datasets["my_ds"] = new InMemoryDataset("my_ds")
		{
			Description = "Original",
			FriendlyName = "Original Name",
		};

		var patchBody = new Dataset
		{
			Description = "Updated description",
		};
		var json = JsonConvert.SerializeObject(patchBody);
		var content = new StringContent(json, Encoding.UTF8, "application/json");
		var request = new HttpRequestMessage(new HttpMethod("PATCH"),
			"/bigquery/v2/projects/test-project/datasets/my_ds")
		{
			Content = content
		};

		// Act
		var response = await _httpClient.SendAsync(request);

		// Assert
		Assert.Equal(HttpStatusCode.OK, response.StatusCode);

		var responseJson = await response.Content.ReadAsStringAsync();
		var result = JsonConvert.DeserializeObject<Dataset>(responseJson)!;
		Assert.Equal("Updated description", result.Description);
		// FriendlyName should be preserved since it was not in the patch
		Assert.Equal("Original Name", result.FriendlyName);
	}

	[Fact]
	public async Task PatchDataset_NotFound_Returns404()
	{
		// Arrange
		var patchBody = new Dataset { Description = "Updated" };
		var json = JsonConvert.SerializeObject(patchBody);
		var content = new StringContent(json, Encoding.UTF8, "application/json");
		var request = new HttpRequestMessage(new HttpMethod("PATCH"),
			"/bigquery/v2/projects/test-project/datasets/nonexistent")
		{
			Content = content
		};

		// Act
		var response = await _httpClient.SendAsync(request);

		// Assert
		Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
	}
}
