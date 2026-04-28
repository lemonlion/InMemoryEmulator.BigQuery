using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase31;

/// <summary>
/// Unit tests for load jobs (jobs.insert with Configuration.Load).
/// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#JobConfigurationLoad
///   "Configures a load job."
/// </summary>
public class LoadJobTests
{
	[Fact]
	public async Task UploadCsv_InsertsRowsIntoTable()
	{
		var result = InMemoryBigQuery.Create("test-project", "test_ds");
		var client = result.Client;

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
			]
		};

		var csv = "1,alpha\n2,beta\n";
		using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(csv));

		var job = await client.UploadCsvAsync(
			client.GetTableReference("test_ds", "load_test"),
			schema,
			stream);

		Assert.NotNull(job);
		Assert.Equal("DONE", job.Status.State);

		// Verify rows were inserted
		var allRows = client.ListRows("test_ds", "load_test", schema).ToList();
		Assert.Equal(2, allRows.Count);
	}

	[Fact]
	public async Task UploadCsv_WithHeaderRow_SkipsHeader()
	{
		var result = InMemoryBigQuery.Create("test-project", "test_ds");
		var client = result.Client;

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
			]
		};

		var csv = "id,name\n1,alpha\n2,beta\n";
		using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(csv));

		var options = new UploadCsvOptions { SkipLeadingRows = 1 };
		var job = await client.UploadCsvAsync(
			client.GetTableReference("test_ds", "csv_header_test"),
			schema,
			stream,
			options);

		Assert.NotNull(job);

		var allRows = client.ListRows("test_ds", "csv_header_test", schema).ToList();
		Assert.Equal(2, allRows.Count);
	}

	[Fact]
	public async Task UploadJson_InsertsRowsIntoTable()
	{
		var result = InMemoryBigQuery.Create("test-project", "test_ds");
		var client = result.Client;

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
			]
		};

		// Newline-delimited JSON
		var json = "{\"id\":1,\"name\":\"alpha\"}\n{\"id\":2,\"name\":\"beta\"}\n";
		using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

		var job = await client.UploadJsonAsync(
			client.GetTableReference("test_ds", "json_test"),
			schema,
			stream);

		Assert.NotNull(job);
		Assert.Equal("DONE", job.Status.State);

		var allRows = client.ListRows("test_ds", "json_test", schema).ToList();
		Assert.Equal(2, allRows.Count);
	}

	[Fact]
	public async Task UploadCsv_CreatesTableIfNotExists()
	{
		var result = InMemoryBigQuery.Create("test-project", "test_ds");
		var client = result.Client;

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "value", Type = "STRING", Mode = "NULLABLE" },
			]
		};

		var csv = "hello\nworld\n";
		using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(csv));

		await client.UploadCsvAsync(
			client.GetTableReference("test_ds", "new_table"),
			schema,
			stream);

		// Table should now exist
		var table = await client.GetTableAsync("test_ds", "new_table");
		Assert.NotNull(table);
	}

	[Fact]
	public async Task UploadCsv_WithEmptyData_CreatesEmptyTable()
	{
		var result = InMemoryBigQuery.Create("test-project", "test_ds");
		var client = result.Client;

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
			]
		};

		using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(""));

		await client.UploadCsvAsync(
			client.GetTableReference("test_ds", "empty_table"),
			schema,
			stream);

		var allRows = client.ListRows("test_ds", "empty_table", schema).ToList();
		Assert.Empty(allRows);
	}

	[Fact]
	public async Task UploadJson_FieldValuesAreCorrect()
	{
		var result = InMemoryBigQuery.Create("test-project", "test_ds");
		var client = result.Client;

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "score", Type = "FLOAT", Mode = "NULLABLE" },
			]
		};

		var json = "{\"id\":42,\"name\":\"test\",\"score\":3.14}\n";
		using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

		await client.UploadJsonAsync(
			client.GetTableReference("test_ds", "json_values_test"),
			schema,
			stream);

		// Query to verify values are correct
		var queryResult = await client.ExecuteQueryAsync(
			"SELECT id, name, score FROM test_ds.json_values_test",
			parameters: null);

		var allRows = queryResult.ToList();

		Assert.Single(allRows);
		Assert.Equal(42L, allRows[0]["id"]);
		Assert.Equal("test", allRows[0]["name"]);
		Assert.Equal(3.14, (double)allRows[0]["score"], 2);
	}

	[Fact]
	public async Task UploadCsv_FieldValuesAreQueryable()
	{
		var result = InMemoryBigQuery.Create("test-project", "test_ds");
		var client = result.Client;

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "city", Type = "STRING", Mode = "NULLABLE" },
			]
		};

		var csv = "1,London\n2,Paris\n3,Berlin\n";
		using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(csv));

		await client.UploadCsvAsync(
			client.GetTableReference("test_ds", "csv_query_test"),
			schema,
			stream);

		var queryResult = await client.ExecuteQueryAsync(
			"SELECT city FROM test_ds.csv_query_test WHERE id > 1 ORDER BY city",
			parameters: null);

		var cities = queryResult.Select(row => (string)row["city"]).ToList();

		Assert.Equal(["Berlin", "Paris"], cities);
	}
}
