using Google;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase31;

/// <summary>
/// Unit tests for extract jobs (jobs.insert with Configuration.Extract).
/// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#JobConfigurationExtract
///   "Configures an extract job."
/// The emulator returns a completed no-op job since there is no real GCS to write to.
/// </summary>
public class ExtractJobTests
{
	[Fact]
	public async Task CreateExtractJob_ReturnsCompletedJob()
	{
		var result = InMemoryBigQuery.Create("test-project", "test_ds");
		var client = result.Client;

		// First create a table with some data
		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync("test_ds", "extract_source", schema);

		var job = await client.CreateExtractJobAsync(
			client.GetTableReference("test_ds", "extract_source"),
			"gs://fake-bucket/output.csv");

		Assert.NotNull(job);
		Assert.Equal("DONE", job.Status.State);
	}

	[Fact]
	public async Task CreateExtractJob_WithMultipleDestinations_ReturnsCompletedJob()
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
		await client.CreateTableAsync("test_ds", "multi_extract_source", schema);

		var job = await client.CreateExtractJobAsync(
			client.GetTableReference("test_ds", "multi_extract_source"),
			new[] { "gs://fake-bucket/part1.csv", "gs://fake-bucket/part2.csv" });

		Assert.NotNull(job);
		Assert.Equal("DONE", job.Status.State);
	}

	[Fact]
	public async Task CreateExtractJob_SourceTableNotFound_ReturnsError()
	{
		var result = InMemoryBigQuery.Create("test-project", "test_ds");
		var client = result.Client;

		// Don't create the table — it should not exist
		await Assert.ThrowsAsync<GoogleApiException>(async () =>
		{
			await client.CreateExtractJobAsync(
				client.GetTableReference("test_ds", "nonexistent_table"),
				"gs://fake-bucket/output.csv");
		});
	}
}
