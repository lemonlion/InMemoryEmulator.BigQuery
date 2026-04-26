using System.Net;
using System.Text;
using BigQuery.InMemoryEmulator;
using Google.Apis.Bigquery.v2.Data;
using Newtonsoft.Json;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit;

/// <summary>
/// Tests FakeBigQueryHandler table CRUD routing at the HTTP level.
/// </summary>
public class TableRoutingTests
{
	private readonly InMemoryDataStore _store;
	private readonly FakeBigQueryHandler _handler;
	private readonly HttpClient _httpClient;

	public TableRoutingTests()
	{
		_store = new InMemoryDataStore("test-project");
		_store.Datasets["test_ds"] = new InMemoryDataset("test_ds");
		_handler = new FakeBigQueryHandler(_store);
		_httpClient = new HttpClient(_handler)
		{
			BaseAddress = new Uri("https://bigquery.googleapis.com")
		};
	}

	private static TableSchema SimpleSchema() => new()
	{
		Fields = new List<TableFieldSchema>
		{
			new() { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
			new() { Name = "name", Type = "STRING", Mode = "NULLABLE" },
		}
	};

	[Fact]
	public async Task CreateTable_Returns200_WithTable()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/insert
		//   "Creates a new, empty table in the dataset."

		// Arrange
		var body = new Table
		{
			TableReference = new TableReference
			{
				ProjectId = "test-project",
				DatasetId = "test_ds",
				TableId = "new_table"
			},
			Schema = SimpleSchema(),
		};
		var json = JsonConvert.SerializeObject(body);
		var content = new StringContent(json, Encoding.UTF8, "application/json");

		// Act
		var response = await _httpClient.PostAsync(
			"/bigquery/v2/projects/test-project/datasets/test_ds/tables", content);

		// Assert
		Assert.Equal(HttpStatusCode.OK, response.StatusCode);

		var responseJson = await response.Content.ReadAsStringAsync();
		var result = JsonConvert.DeserializeObject<Table>(responseJson)!;
		Assert.Equal("new_table", result.TableReference?.TableId);
		Assert.Equal("test_ds", result.TableReference?.DatasetId);
		Assert.Equal(2, result.Schema?.Fields?.Count);
	}

	[Fact]
	public async Task CreateTable_DatasetNotFound_Returns404()
	{
		// Arrange
		var body = new Table
		{
			TableReference = new TableReference
			{
				ProjectId = "test-project",
				DatasetId = "nonexistent",
				TableId = "t1"
			},
			Schema = SimpleSchema(),
		};
		var json = JsonConvert.SerializeObject(body);
		var content = new StringContent(json, Encoding.UTF8, "application/json");

		// Act
		var response = await _httpClient.PostAsync(
			"/bigquery/v2/projects/test-project/datasets/nonexistent/tables", content);

		// Assert
		Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
	}

	[Fact]
	public async Task CreateTable_AlreadyExists_Returns409()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/insert
		//   Returns 409 if table already exists.

		// Arrange
		var ds = _store.Datasets["test_ds"];
		ds.Tables["existing"] = new InMemoryTable("test_ds", "existing", SimpleSchema());

		var body = new Table
		{
			TableReference = new TableReference
			{
				ProjectId = "test-project",
				DatasetId = "test_ds",
				TableId = "existing"
			},
			Schema = SimpleSchema(),
		};
		var json = JsonConvert.SerializeObject(body);
		var content = new StringContent(json, Encoding.UTF8, "application/json");

		// Act
		var response = await _httpClient.PostAsync(
			"/bigquery/v2/projects/test-project/datasets/test_ds/tables", content);

		// Assert
		Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
	}

	[Fact]
	public async Task GetTable_ReturnsTable()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/get
		//   "Gets the specified table resource by table ID."

		// Arrange
		var ds = _store.Datasets["test_ds"];
		ds.Tables["my_table"] = new InMemoryTable("test_ds", "my_table", SimpleSchema())
		{
			Description = "My table",
		};

		// Act
		var response = await _httpClient.GetAsync(
			"/bigquery/v2/projects/test-project/datasets/test_ds/tables/my_table");

		// Assert
		Assert.Equal(HttpStatusCode.OK, response.StatusCode);

		var responseJson = await response.Content.ReadAsStringAsync();
		var result = JsonConvert.DeserializeObject<Table>(responseJson)!;
		Assert.Equal("my_table", result.TableReference?.TableId);
		Assert.Equal("My table", result.Description);
		Assert.Equal(2, result.Schema?.Fields?.Count);
	}

	[Fact]
	public async Task GetTable_NotFound_Returns404()
	{
		// Act
		var response = await _httpClient.GetAsync(
			"/bigquery/v2/projects/test-project/datasets/test_ds/tables/nonexistent");

		// Assert
		Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
	}

	[Fact]
	public async Task ListTables_ReturnsAll()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/list
		//   "Lists all tables in the specified dataset."

		// Arrange
		var ds = _store.Datasets["test_ds"];
		ds.Tables["t1"] = new InMemoryTable("test_ds", "t1", SimpleSchema());
		ds.Tables["t2"] = new InMemoryTable("test_ds", "t2", SimpleSchema());

		// Act
		var response = await _httpClient.GetAsync(
			"/bigquery/v2/projects/test-project/datasets/test_ds/tables");

		// Assert
		Assert.Equal(HttpStatusCode.OK, response.StatusCode);

		var responseJson = await response.Content.ReadAsStringAsync();
		var result = JsonConvert.DeserializeObject<TableList>(responseJson)!;
		Assert.Equal(2, result.Tables?.Count);
	}

	[Fact]
	public async Task DeleteTable_ReturnsNoContent()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/delete
		//   "Deletes the table specified by tableId from the dataset."

		// Arrange
		var ds = _store.Datasets["test_ds"];
		ds.Tables["to_delete"] = new InMemoryTable("test_ds", "to_delete", SimpleSchema());

		// Act
		var response = await _httpClient.DeleteAsync(
			"/bigquery/v2/projects/test-project/datasets/test_ds/tables/to_delete");

		// Assert
		Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
		Assert.False(ds.Tables.ContainsKey("to_delete"));
	}

	[Fact]
	public async Task PatchTable_AddsColumn()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/patch
		//   "Updates information in an existing table. The patch method replaces
		//    fields that are provided in the submitted table resource."

		// Arrange
		var ds = _store.Datasets["test_ds"];
		ds.Tables["my_table"] = new InMemoryTable("test_ds", "my_table", SimpleSchema());

		var patchSchema = new TableSchema
		{
			Fields = new List<TableFieldSchema>
			{
				new() { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new() { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new() { Name = "email", Type = "STRING", Mode = "NULLABLE" },
			}
		};
		var patchBody = new Table { Schema = patchSchema };
		var json = JsonConvert.SerializeObject(patchBody);
		var content = new StringContent(json, Encoding.UTF8, "application/json");
		var request = new HttpRequestMessage(new HttpMethod("PATCH"),
			"/bigquery/v2/projects/test-project/datasets/test_ds/tables/my_table")
		{
			Content = content
		};

		// Act
		var response = await _httpClient.SendAsync(request);

		// Assert
		Assert.Equal(HttpStatusCode.OK, response.StatusCode);

		var responseJson = await response.Content.ReadAsStringAsync();
		var result = JsonConvert.DeserializeObject<Table>(responseJson)!;
		Assert.Equal(3, result.Schema?.Fields?.Count);
	}
}
