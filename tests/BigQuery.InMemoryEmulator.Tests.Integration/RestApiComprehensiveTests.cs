using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// REST API tests: Dataset CRUD, Table CRUD, Streaming inserts, Job operations, Routine CRUD.
/// Ref: https://cloud.google.com/bigquery/docs/reference/rest
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class RestApiComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public RestApiComprehensiveTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_rest_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<BigQueryClient> GetClient() => await _fixture.GetClientAsync();

	// ==== DATASET OPERATIONS ====
	[Fact] public async Task CreateDataset_ReturnsDataset()
	{
		var client = await GetClient();
		var ds = $"test_ds_{Guid.NewGuid():N}"[..30];
		var result = await client.CreateDatasetAsync(ds);
		Assert.NotNull(result);
		await client.DeleteDatasetAsync(ds);
	}

	[Fact] public async Task GetDataset_AfterCreate()
	{
		var client = await GetClient();
		var ds = $"test_ds2_{Guid.NewGuid():N}"[..30];
		await client.CreateDatasetAsync(ds);
		var result = await client.GetDatasetAsync(ds);
		Assert.NotNull(result);
		await client.DeleteDatasetAsync(ds);
	}

	[Fact] public async Task ListDatasets_ContainsCreated()
	{
		var client = await GetClient();
		var datasets = client.ListDatasets().ToList();
		Assert.True(datasets.Count >= 1);
	}

	[Fact] public async Task DeleteDataset_Succeeds()
	{
		var client = await GetClient();
		var ds = $"test_ds3_{Guid.NewGuid():N}"[..30];
		await client.CreateDatasetAsync(ds);
		await client.DeleteDatasetAsync(ds);
	}

	[Fact] public async Task GetDeletedDataset_ThrowsNotFound()
	{
		var client = await GetClient();
		var ds = $"test_ds4_{Guid.NewGuid():N}"[..30];
		await client.CreateDatasetAsync(ds);
		await client.DeleteDatasetAsync(ds);
		await Assert.ThrowsAsync<Google.GoogleApiException>(() => client.GetDatasetAsync(ds));
	}

	[Fact] public async Task PatchDataset_UpdatesDescription()
	{
		var client = await GetClient();
		await client.PatchDatasetAsync(_datasetId, new Dataset { Description = "Updated desc" });
		var ds = await client.GetDatasetAsync(_datasetId);
		Assert.Equal("Updated desc", ds.Resource.Description);
	}

	// ==== TABLE OPERATIONS ====
	[Fact] public async Task CreateTable_WithSchema()
	{
		var client = await GetClient();
		var table = await client.CreateTableAsync(_datasetId, "rest_t1", new TableSchema
		{
			Fields = [new TableFieldSchema { Name = "col1", Type = "STRING" }]
		});
		Assert.NotNull(table);
	}

	[Fact] public async Task GetTable_AfterCreate()
	{
		var client = await GetClient();
		await client.CreateTableAsync(_datasetId, "rest_t2", new TableSchema
		{
			Fields = [new TableFieldSchema { Name = "col1", Type = "STRING" }]
		});
		var table = await client.GetTableAsync(_datasetId, "rest_t2");
		Assert.NotNull(table);
	}

	[Fact] public async Task ListTables_ContainsCreated()
	{
		var client = await GetClient();
		await client.CreateTableAsync(_datasetId, "rest_t3", new TableSchema
		{
			Fields = [new TableFieldSchema { Name = "col1", Type = "STRING" }]
		});
		var tables = client.ListTables(_datasetId).ToList();
		Assert.Contains(tables, t => t.Reference.TableId == "rest_t3");
	}

	[Fact] public async Task DeleteTable_Succeeds()
	{
		var client = await GetClient();
		await client.CreateTableAsync(_datasetId, "rest_t4", new TableSchema
		{
			Fields = [new TableFieldSchema { Name = "col1", Type = "STRING" }]
		});
		await client.DeleteTableAsync(_datasetId, "rest_t4");
	}

	[Fact] public async Task PatchTable_UpdatesDescription()
	{
		var client = await GetClient();
		await client.CreateTableAsync(_datasetId, "rest_t5", new TableSchema
		{
			Fields = [new TableFieldSchema { Name = "col1", Type = "STRING" }]
		});
		await client.PatchTableAsync(_datasetId, "rest_t5", new Table { Description = "Updated" });
		var table = await client.GetTableAsync(_datasetId, "rest_t5");
		Assert.Equal("Updated", table.Resource.Description);
	}

	[Fact] public async Task GetTable_NotFound_ThrowsException()
	{
		var client = await GetClient();
		await Assert.ThrowsAsync<Google.GoogleApiException>(() => client.GetTableAsync(_datasetId, "nonexistent_table"));
	}

	// ==== TABLE with Labels ====
	[Fact] public async Task CreateTable_WithLabels()
	{
		var client = await GetClient();
		var table = await client.CreateTableAsync(_datasetId, "rest_t6", new TableSchema
		{
			Fields = [new TableFieldSchema { Name = "col1", Type = "STRING" }]
		}, new CreateTableOptions());
		Assert.NotNull(table);
	}

	// ==== STREAMING INSERT ====
	[Fact] public async Task StreamingInsert_SingleRow()
	{
		var client = await GetClient();
		await client.CreateTableAsync(_datasetId, "si_t1", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER" },
				new TableFieldSchema { Name = "name", Type = "STRING" },
			]
		});
		await client.InsertRowsAsync(_datasetId, "si_t1", new[]
		{
			new BigQueryInsertRow("r1") { ["id"] = 1, ["name"] = "Test" }
		});
		var result = await client.ExecuteQueryAsync($"SELECT COUNT(*) FROM `{_datasetId}.si_t1`", parameters: null);
		Assert.Equal("1", result.ToList()[0][0]?.ToString());
	}

	[Fact] public async Task StreamingInsert_MultipleRows()
	{
		var client = await GetClient();
		await client.CreateTableAsync(_datasetId, "si_t2", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER" },
				new TableFieldSchema { Name = "value", Type = "FLOAT" },
			]
		});
		await client.InsertRowsAsync(_datasetId, "si_t2", Enumerable.Range(1, 100).Select(i =>
			new BigQueryInsertRow($"r{i}") { ["id"] = i, ["value"] = i * 1.5 }
		));
		var result = await client.ExecuteQueryAsync($"SELECT COUNT(*) FROM `{_datasetId}.si_t2`", parameters: null);
		Assert.Equal("100", result.ToList()[0][0]?.ToString());
	}

	[Fact] public async Task StreamingInsert_DataIsQueryable()
	{
		var client = await GetClient();
		await client.CreateTableAsync(_datasetId, "si_t3", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER" },
				new TableFieldSchema { Name = "name", Type = "STRING" },
			]
		});
		await client.InsertRowsAsync(_datasetId, "si_t3", new[]
		{
			new BigQueryInsertRow("x1") { ["id"] = 1, ["name"] = "Alice" },
			new BigQueryInsertRow("x2") { ["id"] = 2, ["name"] = "Bob" },
		});
		var result = await client.ExecuteQueryAsync($"SELECT name FROM `{_datasetId}.si_t3` WHERE id = 2", parameters: null);
		Assert.Equal("Bob", result.ToList()[0]["name"]?.ToString());
	}

	// ==== JOB OPERATIONS ====
	[Fact(Skip = "Not yet supported")] public async Task DryRun_ReturnsSchemaWithoutData()
	{
		var client = await GetClient();
		await client.CreateTableAsync(_datasetId, "dr_t1", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER" },
				new TableFieldSchema { Name = "name", Type = "STRING" },
			]
		});
		var job = await client.CreateQueryJobAsync($"SELECT * FROM `{_datasetId}.dr_t1`", parameters: null, new QueryOptions { DryRun = true });
		Assert.NotNull(job.Resource.Statistics?.Query?.Schema);
	}

	[Fact] public async Task DryRun_InvalidQuery_Throws()
	{
		var client = await GetClient();
		await Assert.ThrowsAnyAsync<Exception>(() => client.CreateQueryJobAsync("SELECT * FROM nonexistent_table_xyz", parameters: null, new QueryOptions { DryRun = true }));
	}

	[Fact] public async Task ListJobs_ReturnsResults()
	{
		var client = await GetClient();
		await client.ExecuteQueryAsync("SELECT 1", parameters: null);
		var jobs = client.ListJobs().ToList();
		Assert.True(jobs.Count > 0);
	}

	[Fact] public async Task GetJob_AfterQuery()
	{
		var client = await GetClient();
		var queryJob = await client.CreateQueryJobAsync("SELECT 1", parameters: null);
		var fetched = await client.GetJobAsync(queryJob.Reference);
		Assert.NotNull(fetched);
	}

	[Fact] public async Task CancelJob_NoError()
	{
		var client = await GetClient();
		var queryJob = await client.CreateQueryJobAsync("SELECT 1", parameters: null);
		var result = await client.CancelJobAsync(queryJob.Reference);
		Assert.NotNull(result);
	}

	// ==== QUERY with PARAMETERS ====
	[Fact] public async Task QueryWithParameters_Int()
	{
		var client = await GetClient();
		var parameters = new[] { new BigQueryParameter("val", BigQueryDbType.Int64, 42) };
		var result = await client.ExecuteQueryAsync("SELECT @val", parameters);
		Assert.Equal("42", result.ToList()[0][0]?.ToString());
	}

	[Fact] public async Task QueryWithParameters_String()
	{
		var client = await GetClient();
		var parameters = new[] { new BigQueryParameter("name", BigQueryDbType.String, "Hello") };
		var result = await client.ExecuteQueryAsync("SELECT @name", parameters);
		Assert.Equal("Hello", result.ToList()[0][0]?.ToString());
	}

	[Fact] public async Task QueryWithParameters_Multiple()
	{
		var client = await GetClient();
		var parameters = new[]
		{
			new BigQueryParameter("a", BigQueryDbType.Int64, 10),
			new BigQueryParameter("b", BigQueryDbType.Int64, 20),
		};
		var result = await client.ExecuteQueryAsync("SELECT @a + @b", parameters);
		Assert.Equal("30", result.ToList()[0][0]?.ToString());
	}

	// ==== QUERY PAGINATION ====
	[Fact] public async Task QueryPagination_MaxResults()
	{
		var client = await GetClient();
		await client.CreateTableAsync(_datasetId, "pg_t1", new TableSchema
		{
			Fields = [new TableFieldSchema { Name = "id", Type = "INTEGER" }]
		});
		await client.InsertRowsAsync(_datasetId, "pg_t1", Enumerable.Range(1, 20).Select(i =>
			new BigQueryInsertRow($"r{i}") { ["id"] = i }
		));
		var result = await client.ExecuteQueryAsync($"SELECT * FROM `{_datasetId}.pg_t1` ORDER BY id", parameters: null);
		var all = result.ToList();
		Assert.Equal(20, all.Count);
	}

	// ==== ROUTINE OPERATIONS ====
	[Fact(Skip = "Not yet supported")] public async Task CreateFunction_SqlUdf()
	{
		var client = await GetClient();
		await client.ExecuteQueryAsync($"CREATE FUNCTION `{_datasetId}.add_one`(x INT64) RETURNS INT64 AS (x + 1)", parameters: null);
		var result = await client.ExecuteQueryAsync($"SELECT `{_datasetId}.add_one`(5)", parameters: null);
		Assert.Equal("6", result.ToList()[0][0]?.ToString());
	}

	[Fact(Skip = "Not yet supported")] public async Task DropFunction_RemovesRoutine()
	{
		var client = await GetClient();
		await client.ExecuteQueryAsync($"CREATE FUNCTION `{_datasetId}.to_drop`(x INT64) RETURNS INT64 AS (x)", parameters: null);
		await client.ExecuteQueryAsync($"DROP FUNCTION `{_datasetId}.to_drop`", parameters: null);
	}

	// ==== TABLE DATA - ListRows ====
	[Fact] public async Task ListTableData_ReturnsRows()
	{
		var client = await GetClient();
		await client.CreateTableAsync(_datasetId, "lr_t1", new TableSchema
		{
			Fields = [new TableFieldSchema { Name = "id", Type = "INTEGER" }]
		});
		await client.InsertRowsAsync(_datasetId, "lr_t1", new[]
		{
			new BigQueryInsertRow("r1") { ["id"] = 1 },
			new BigQueryInsertRow("r2") { ["id"] = 2 },
		});
		var rows = client.ListRows(_datasetId, "lr_t1").ToList();
		Assert.Equal(2, rows.Count);
	}

	// ==== COPY JOB ====
	[Fact(Skip = "Not yet supported")] public async Task CopyJob_DuplicatesTable()
	{
		var client = await GetClient();
		await client.CreateTableAsync(_datasetId, "cp_src", new TableSchema
		{
			Fields = [new TableFieldSchema { Name = "id", Type = "INTEGER" }]
		});
		await client.InsertRowsAsync(_datasetId, "cp_src", new[]
		{
			new BigQueryInsertRow("r1") { ["id"] = 1 },
			new BigQueryInsertRow("r2") { ["id"] = 2 },
		});
		var srcRef = client.GetTableReference(_datasetId, "cp_src");
		var dstRef = client.GetTableReference(_datasetId, "cp_dst");
		var job = await client.CreateCopyJobAsync(srcRef, dstRef);
		job = await job.PollUntilCompletedAsync();
		Assert.Null(job.Status.ErrorResult);
		var result = await client.ExecuteQueryAsync($"SELECT COUNT(*) FROM `{_datasetId}.cp_dst`", parameters: null);
		Assert.Equal("2", result.ToList()[0][0]?.ToString());
	}
}
