using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for Phase 20: Job variants, dry run, labels.
/// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class JobVariantTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public JobVariantTests(BigQuerySession session)
	{
		_session = session;
	}

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_job_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.CreateTableAsync(_datasetId, "items", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "value", Type = "STRING", Mode = "NULLABLE" },
			]
		});
		await client.InsertRowsAsync(_datasetId, "items",
		[
			new BigQueryInsertRow("r1") { ["id"] = 1, ["value"] = "a" },
			new BigQueryInsertRow("r2") { ["id"] = 2, ["value"] = "b" },
		]);
	}

	public async ValueTask DisposeAsync()
	{
		try
		{
			var client = await _fixture.GetClientAsync();
			await client.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true });
		}
		catch { }
		await _fixture.DisposeAsync();
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#JobConfiguration
	//   "dryRun: If set, don't actually run this job."

	[Fact]
	public async Task DryRun_ReturnsSchemaWithoutExecuting()
	{
		var client = await _fixture.GetClientAsync();

		var job = await client.CreateQueryJobAsync(
			$"SELECT id, value FROM `{_datasetId}.items`",
			parameters: null,
			options: new QueryOptions { DryRun = true });

		// Dry run jobs complete immediately with statistics
		Assert.Equal("DONE", job.Status.State);
		// Should have schema info
		Assert.NotNull(job.Statistics?.Query?.TotalBytesProcessed);
	}

	[Fact]
	public async Task DryRun_InvalidQuery_ThrowsError()
	{
		var client = await _fixture.GetClientAsync();

		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
		//   "If the query is invalid, an error returns."
		await Assert.ThrowsAnyAsync<Exception>(async () =>
			await client.CreateQueryJobAsync(
				"SELECT FROM nonexistent_table_xyz",
				parameters: null,
				options: new QueryOptions { DryRun = true }));
	}

	[Fact]
	public async Task CreateQueryJob_WithLabels()
	{
		var client = await _fixture.GetClientAsync();

		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
		//   "labels: The labels associated with this query."
		var options = new QueryOptions
		{
			Labels = new Dictionary<string, string> { ["env"] = "test", ["team"] = "analytics" }
		};

		var job = await client.CreateQueryJobAsync(
			$"SELECT id FROM `{_datasetId}.items`",
			parameters: null,
			options: options);

		// Labels should be propagated to the job resource
		Assert.Equal("test", job.Resource.Configuration?.Labels?["env"]);
		Assert.Equal("analytics", job.Resource.Configuration?.Labels?["team"]);
	}

	[Fact]
	public async Task JobCancel_CompletedJob()
	{
		var client = await _fixture.GetClientAsync();

		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/cancel
		//   "Requests that a job be cancelled."
		var job = await client.CreateQueryJobAsync(
			$"SELECT * FROM `{_datasetId}.items`",
			parameters: null);

		// In-memory jobs complete instantly, cancel should still succeed
		await job.CancelAsync();

		// Job should still be DONE (it already completed)
		var fetched = await client.GetJobAsync(job.Reference);
		Assert.Equal("DONE", fetched.Status.State);
	}

	[Fact]
	public async Task InsertJob_WithScriptQuery()
	{
		var client = await _fixture.GetClientAsync();

		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language
		//   "Multi-statement queries are scripts."
		var job = await client.CreateQueryJobAsync(
			$"DECLARE x INT64 DEFAULT 1; SELECT x;",
			parameters: null,
			options: new QueryOptions { DefaultDataset = new DatasetReference { DatasetId = _datasetId } });

		var results = await client.GetQueryResultsAsync(job.Reference);
		Assert.Single(results);
	}
}
