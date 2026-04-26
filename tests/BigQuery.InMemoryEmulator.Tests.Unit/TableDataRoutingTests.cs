using System.Net;
using System.Text;
using BigQuery.InMemoryEmulator;
using Google.Apis.Bigquery.v2.Data;
using Newtonsoft.Json;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit;

/// <summary>
/// Tests FakeBigQueryHandler table data (insertAll + data) routing.
/// </summary>
public class TableDataRoutingTests
{
	private readonly InMemoryDataStore _store;
	private readonly FakeBigQueryHandler _handler;
	private readonly HttpClient _httpClient;

	public TableDataRoutingTests()
	{
		_store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		var schema = new TableSchema
		{
			Fields = new List<TableFieldSchema>
			{
				new() { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new() { Name = "name", Type = "STRING", Mode = "NULLABLE" },
			}
		};
		ds.Tables["test_table"] = new InMemoryTable("test_ds", "test_table", schema);
		_store.Datasets["test_ds"] = ds;

		_handler = new FakeBigQueryHandler(_store);
		_httpClient = new HttpClient(_handler)
		{
			BaseAddress = new Uri("https://bigquery.googleapis.com")
		};
	}

	[Fact]
	public async Task InsertAll_StoresRows()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll
		//   "Streams data into BigQuery one record at a time without needing to run a load job."

		// Arrange
		var insertRequest = new TableDataInsertAllRequest
		{
			Rows = new List<TableDataInsertAllRequest.RowsData>
			{
				new()
				{
					InsertId = "row1",
					Json = new Dictionary<string, object> { ["id"] = 1, ["name"] = "Alice" }
				},
				new()
				{
					InsertId = "row2",
					Json = new Dictionary<string, object> { ["id"] = 2, ["name"] = "Bob" }
				}
			}
		};
		var json = JsonConvert.SerializeObject(insertRequest);
		var content = new StringContent(json, Encoding.UTF8, "application/json");

		// Act
		var response = await _httpClient.PostAsync(
			"/bigquery/v2/projects/test-project/datasets/test_ds/tables/test_table/insertAll",
			content);

		// Assert
		Assert.Equal(HttpStatusCode.OK, response.StatusCode);

		var table = _store.Datasets["test_ds"].Tables["test_table"];
		Assert.Equal(2, table.RowCount);
	}

	[Fact]
	public async Task InsertAll_WithDedup_PreventsDuplicates()
	{
		// Arrange
		var insertRequest = new TableDataInsertAllRequest
		{
			Rows = new List<TableDataInsertAllRequest.RowsData>
			{
				new()
				{
					InsertId = "dedup1",
					Json = new Dictionary<string, object> { ["id"] = 1, ["name"] = "Alice" }
				}
			}
		};
		var json = JsonConvert.SerializeObject(insertRequest);

		// Insert twice with same insertId
		var content1 = new StringContent(json, Encoding.UTF8, "application/json");
		await _httpClient.PostAsync(
			"/bigquery/v2/projects/test-project/datasets/test_ds/tables/test_table/insertAll",
			content1);

		var content2 = new StringContent(json, Encoding.UTF8, "application/json");
		await _httpClient.PostAsync(
			"/bigquery/v2/projects/test-project/datasets/test_ds/tables/test_table/insertAll",
			content2);

		// Assert — dedup should prevent second insert
		var table = _store.Datasets["test_ds"].Tables["test_table"];
		Assert.Equal(1, table.RowCount);
	}

	[Fact]
	public async Task ListTableData_ReturnsRows()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
		//   "Lists the content of a table in rows."

		// Arrange — insert some rows directly
		var table = _store.Datasets["test_ds"].Tables["test_table"];
		lock (table.RowLock)
		{
			table.Rows.Add(new InMemoryRow(
				new Dictionary<string, object?> { ["id"] = 1L, ["name"] = "Alice" }));
			table.Rows.Add(new InMemoryRow(
				new Dictionary<string, object?> { ["id"] = 2L, ["name"] = "Bob" }));
		}

		// Act
		var response = await _httpClient.GetAsync(
			"/bigquery/v2/projects/test-project/datasets/test_ds/tables/test_table/data");

		// Assert
		Assert.Equal(HttpStatusCode.OK, response.StatusCode);

		var responseJson = await response.Content.ReadAsStringAsync();
		var result = JsonConvert.DeserializeObject<TableDataList>(responseJson)!;
		Assert.Equal(2, result.TotalRows);
		Assert.Equal(2, result.Rows?.Count);
	}

	[Fact]
	public async Task ListTableData_Paginated()
	{
		// Arrange — insert 5 rows
		var table = _store.Datasets["test_ds"].Tables["test_table"];
		lock (table.RowLock)
		{
			for (var i = 1; i <= 5; i++)
				table.Rows.Add(new InMemoryRow(
					new Dictionary<string, object?> { ["id"] = (long)i, ["name"] = $"User{i}" }));
		}

		// Act — get first page with maxResults=2
		var response = await _httpClient.GetAsync(
			"/bigquery/v2/projects/test-project/datasets/test_ds/tables/test_table/data?maxResults=2");

		// Assert
		Assert.Equal(HttpStatusCode.OK, response.StatusCode);

		var responseJson = await response.Content.ReadAsStringAsync();
		var result = JsonConvert.DeserializeObject<TableDataList>(responseJson)!;
		Assert.Equal(2, result.Rows?.Count);
		Assert.NotNull(result.PageToken); // Should have a next page token
		Assert.Equal(5, result.TotalRows);
	}

	[Fact]
	public async Task InsertAll_TableNotFound_Returns404()
	{
		// Arrange
		var insertRequest = new TableDataInsertAllRequest
		{
			Rows = new List<TableDataInsertAllRequest.RowsData>
			{
				new()
				{
					Json = new Dictionary<string, object> { ["id"] = 1 }
				}
			}
		};
		var json = JsonConvert.SerializeObject(insertRequest);
		var content = new StringContent(json, Encoding.UTF8, "application/json");

		// Act
		var response = await _httpClient.PostAsync(
			"/bigquery/v2/projects/test-project/datasets/test_ds/tables/nonexistent/insertAll",
			content);

		// Assert
		Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
	}
}
