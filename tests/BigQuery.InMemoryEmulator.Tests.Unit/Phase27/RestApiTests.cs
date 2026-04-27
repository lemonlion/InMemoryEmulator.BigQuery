using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase27;

/// <summary>
/// Phase 27: REST API — jobs.insert (copy), routines CRUD.
/// </summary>
public class RestApiTests
{
	private static (FakeBigQueryHandler Handler, InMemoryDataStore Store) CreateHandler()
	{
		var store = new InMemoryDataStore("test-project");
		var handler = new FakeBigQueryHandler(store);
		return (handler, store);
	}

	private static void SeedTable(InMemoryDataStore store)
	{
		var ds = new InMemoryDataset("ds1");
		store.Datasets["ds1"] = ds;

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
			]
		};
		var table = new InMemoryTable("ds1", "src_table", schema);
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 1L, ["name"] = "Alice" }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 2L, ["name"] = "Bob" }));
		ds.Tables["src_table"] = table;
	}

	#region jobs.insert (copy)

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#JobConfigurationTableCopy
	//   "Copies a table."
	[Fact]
	public async Task CopyJob_CopiesToNewTable()
	{
		var (handler, store) = CreateHandler();
		SeedTable(store);

		var client = new HttpClient(handler) { BaseAddress = new Uri("https://bigquery.googleapis.com") };
		var body = new
		{
			configuration = new
			{
				copy = new
				{
					sourceTable = new { projectId = "test-project", datasetId = "ds1", tableId = "src_table" },
					destinationTable = new { projectId = "test-project", datasetId = "ds1", tableId = "dst_table" },
				}
			}
		};

		var json = System.Text.Json.JsonSerializer.Serialize(body);
		var response = await client.PostAsync(
			"/bigquery/v2/projects/test-project/jobs",
			new StringContent(json, System.Text.Encoding.UTF8, "application/json"));

		Assert.True(response.IsSuccessStatusCode, $"Expected success but got {response.StatusCode}");
		Assert.True(store.Datasets["ds1"].Tables.ContainsKey("dst_table"));
		Assert.Equal(2, store.Datasets["ds1"].Tables["dst_table"].Rows.Count);
	}

	#endregion

	#region routines.list

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/routines/list
	[Fact]
	public async Task Routines_List_ReturnsEmpty()
	{
		var (handler, store) = CreateHandler();
		store.Datasets["ds1"] = new InMemoryDataset("ds1");

		var client = new HttpClient(handler) { BaseAddress = new Uri("https://bigquery.googleapis.com") };
		var response = await client.GetAsync("/bigquery/v2/projects/test-project/datasets/ds1/routines");

		Assert.True(response.IsSuccessStatusCode);
		var content = await response.Content.ReadAsStringAsync();
		Assert.Contains("routines", content);
	}

	#endregion

	#region routines.insert

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/routines/insert
	[Fact]
	public async Task Routines_Insert_CreatesRoutine()
	{
		var (handler, store) = CreateHandler();
		store.Datasets["ds1"] = new InMemoryDataset("ds1");

		var client = new HttpClient(handler) { BaseAddress = new Uri("https://bigquery.googleapis.com") };
		var body = new
		{
			routineReference = new { projectId = "test-project", datasetId = "ds1", routineId = "my_func" },
			routineType = "SCALAR_FUNCTION",
			language = "SQL",
			definitionBody = "x * 2",
			arguments = new[] { new { name = "x", dataType = new { typeKind = "INT64" } } },
			returnType = new { typeKind = "INT64" },
		};

		var json = System.Text.Json.JsonSerializer.Serialize(body);
		var response = await client.PostAsync(
			"/bigquery/v2/projects/test-project/datasets/ds1/routines",
			new StringContent(json, System.Text.Encoding.UTF8, "application/json"));

		Assert.True(response.IsSuccessStatusCode, $"Expected success but got {response.StatusCode}");
		Assert.True(store.Datasets["ds1"].Routines.ContainsKey("my_func"));
	}

	#endregion

	#region routines.get

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/routines/get
	[Fact]
	public async Task Routines_Get_ReturnsRoutine()
	{
		var (handler, store) = CreateHandler();
		var ds = new InMemoryDataset("ds1");
		store.Datasets["ds1"] = ds;
		ds.Routines["my_func"] = new InMemoryRoutine("ds1", "my_func", "SCALAR_FUNCTION", "SQL", "x * 2",
			[("x", "INT64")], "INT64");

		var client = new HttpClient(handler) { BaseAddress = new Uri("https://bigquery.googleapis.com") };
		var response = await client.GetAsync(
			"/bigquery/v2/projects/test-project/datasets/ds1/routines/my_func");

		Assert.True(response.IsSuccessStatusCode);
		var content = await response.Content.ReadAsStringAsync();
		Assert.Contains("my_func", content);
	}

	#endregion

	#region routines.delete

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/routines/delete
	[Fact]
	public async Task Routines_Delete_RemovesRoutine()
	{
		var (handler, store) = CreateHandler();
		var ds = new InMemoryDataset("ds1");
		store.Datasets["ds1"] = ds;
		ds.Routines["my_func"] = new InMemoryRoutine("ds1", "my_func", "SCALAR_FUNCTION", "SQL", "x * 2",
			[("x", "INT64")], "INT64");

		var client = new HttpClient(handler) { BaseAddress = new Uri("https://bigquery.googleapis.com") };
		var response = await client.DeleteAsync(
			"/bigquery/v2/projects/test-project/datasets/ds1/routines/my_func");

		Assert.True(response.IsSuccessStatusCode);
		Assert.False(ds.Routines.ContainsKey("my_func"));
	}

	#endregion
}
