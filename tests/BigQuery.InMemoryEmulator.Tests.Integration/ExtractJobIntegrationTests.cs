using BigQuery.InMemoryEmulator.Tests.Infrastructure;
using Google;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for extract jobs (CreateExtractJob) via the BigQueryClient SDK.
/// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#JobConfigurationExtract
///   "Configures an extract job."
/// The emulator returns a completed job without writing data (no real GCS).
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ExtractJobIntegrationTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public ExtractJobIntegrationTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_ext_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "source_table", schema);
	}

	public ValueTask DisposeAsync() => _fixture.DisposeAsync();

	[Fact]
	public async Task CreateExtractJob_ReturnsCompletedJob()
	{
		var client = await _fixture.GetClientAsync();

		var job = await client.CreateExtractJobAsync(
			client.GetTableReference(_datasetId, "source_table"),
			"gs://fake-bucket/output.csv");

		Assert.NotNull(job);
		Assert.Equal("DONE", job.Status.State);
	}

	[Fact]
	public async Task CreateExtractJob_MultipleDestinations_ReturnsCompletedJob()
	{
		var client = await _fixture.GetClientAsync();

		var job = await client.CreateExtractJobAsync(
			client.GetTableReference(_datasetId, "source_table"),
			new[] { "gs://fake-bucket/part1.csv", "gs://fake-bucket/part2.csv" });

		Assert.NotNull(job);
		Assert.Equal("DONE", job.Status.State);
	}

	[Fact]
	public async Task CreateExtractJob_NonexistentTable_ThrowsError()
	{
		var client = await _fixture.GetClientAsync();

		await Assert.ThrowsAsync<GoogleApiException>(async () =>
		{
			await client.CreateExtractJobAsync(
				client.GetTableReference(_datasetId, "nonexistent"),
				"gs://fake-bucket/output.csv");
		});
	}
}
