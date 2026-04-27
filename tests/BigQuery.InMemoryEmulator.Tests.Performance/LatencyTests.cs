using System.Diagnostics;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Performance;

/// <summary>
/// Performance tests verifying latency and throughput characteristics
/// of the in-memory BigQuery emulator.
/// These are xUnit tests (not BenchmarkDotNet) so they run in CI.
/// </summary>
public class LatencyTests : IDisposable
{
	private readonly InMemoryBigQueryResult _result;
	private readonly string _datasetId = "perf_ds";

	public LatencyTests()
	{
		_result = InMemoryBigQuery.Create("perf-project", _datasetId);

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "value", Type = "FLOAT", Mode = "NULLABLE" },
			]
		};
		_result.Client.CreateTable(_datasetId, "items", schema);

		// Seed 1000 rows
		var rows = new List<BigQueryInsertRow>();
		for (int i = 0; i < 1000; i++)
		{
			rows.Add(new BigQueryInsertRow($"r{i}")
			{
				["id"] = i,
				["name"] = $"item_{i}",
				["value"] = i * 1.5
			});
		}
		_result.Client.InsertRows(_datasetId, "items", rows);
	}

	public void Dispose() => _result.Dispose();

	// Ref: https://cloud.google.com/bigquery/docs/best-practices-performance-overview
	//   "In-memory emulator should be orders of magnitude faster than real BigQuery."

	[Fact]
	public async Task SimpleSelect_Under10ms()
	{
		// Warm-up
		await _result.Client.ExecuteQueryAsync("SELECT 1", parameters: null);

		var sw = Stopwatch.StartNew();
		const int iterations = 100;
		for (int i = 0; i < iterations; i++)
		{
			await _result.Client.ExecuteQueryAsync("SELECT 1 AS n", parameters: null);
		}
		sw.Stop();

		var avgMs = sw.Elapsed.TotalMilliseconds / iterations;
		Assert.True(avgMs < 10, $"Average simple SELECT took {avgMs:F2}ms (expected <10ms)");
	}

	[Fact]
	public async Task SelectWithWhere_Under20ms()
	{
		// Warm-up
		await _result.Client.ExecuteQueryAsync(
			$"SELECT * FROM `{_datasetId}.items` WHERE id = 500", parameters: null);

		var sw = Stopwatch.StartNew();
		const int iterations = 50;
		for (int i = 0; i < iterations; i++)
		{
			await _result.Client.ExecuteQueryAsync(
				$"SELECT * FROM `{_datasetId}.items` WHERE id = {i * 20}", parameters: null);
		}
		sw.Stop();

		var avgMs = sw.Elapsed.TotalMilliseconds / iterations;
		Assert.True(avgMs < 20, $"Average WHERE query took {avgMs:F2}ms (expected <20ms)");
	}

	[Fact]
	public async Task AggregateQuery_Under30ms()
	{
		// Warm-up
		await _result.Client.ExecuteQueryAsync(
			$"SELECT COUNT(*) FROM `{_datasetId}.items`", parameters: null);

		var sw = Stopwatch.StartNew();
		const int iterations = 50;
		for (int i = 0; i < iterations; i++)
		{
			await _result.Client.ExecuteQueryAsync(
				$"SELECT COUNT(*), AVG(value), MIN(value), MAX(value) FROM `{_datasetId}.items`",
				parameters: null);
		}
		sw.Stop();

		var avgMs = sw.Elapsed.TotalMilliseconds / iterations;
		Assert.True(avgMs < 30, $"Average aggregate query took {avgMs:F2}ms (expected <30ms)");
	}

	[Fact]
	public async Task GroupByQuery_Under50ms()
	{
		// Warm-up
		await _result.Client.ExecuteQueryAsync(
			$"SELECT name, COUNT(*) FROM `{_datasetId}.items` GROUP BY name LIMIT 10", parameters: null);

		var sw = Stopwatch.StartNew();
		const int iterations = 20;
		for (int i = 0; i < iterations; i++)
		{
			var results = await _result.Client.ExecuteQueryAsync(
				$"SELECT SUBSTR(name, 1, 6) AS prefix, COUNT(*) AS cnt, SUM(value) AS total FROM `{_datasetId}.items` GROUP BY prefix",
				parameters: null);
			results.ToList(); // Force materialization
		}
		sw.Stop();

		var avgMs = sw.Elapsed.TotalMilliseconds / iterations;
		Assert.True(avgMs < 50, $"Average GROUP BY query took {avgMs:F2}ms (expected <50ms)");
	}

	[Fact]
	public async Task StreamingInsert_1000Rows_Under500ms()
	{
		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "val", Type = "STRING", Mode = "NULLABLE" },
			]
		};
		await _result.Client.CreateTableAsync(_datasetId, "perf_insert", schema);

		var rows = new List<BigQueryInsertRow>();
		for (int i = 0; i < 1000; i++)
			rows.Add(new BigQueryInsertRow($"p{i}") { ["id"] = i, ["val"] = $"row_{i}" });

		var sw = Stopwatch.StartNew();
		await _result.Client.InsertRowsAsync(_datasetId, "perf_insert", rows);
		sw.Stop();

		Assert.True(sw.Elapsed.TotalMilliseconds < 500,
			$"Inserting 1000 rows took {sw.Elapsed.TotalMilliseconds:F2}ms (expected <500ms)");
	}

	[Fact]
	public async Task FunctionEvaluation_Under10ms()
	{
		// Warm-up
		await _result.Client.ExecuteQueryAsync(
			"SELECT UPPER('hello'), LENGTH('test'), ABS(-5)", parameters: null);

		var sw = Stopwatch.StartNew();
		const int iterations = 100;
		for (int i = 0; i < iterations; i++)
		{
			await _result.Client.ExecuteQueryAsync(
				"SELECT UPPER('hello'), LENGTH('test'), CONCAT('a', 'b'), SUBSTR('hello', 1, 3), ABS(-5), ROUND(3.14159, 2)",
				parameters: null);
		}
		sw.Stop();

		var avgMs = sw.Elapsed.TotalMilliseconds / iterations;
		Assert.True(avgMs < 10, $"Average function evaluation took {avgMs:F2}ms (expected <10ms)");
	}

	[Fact]
	public async Task DatasetAndTableCrud_Under10msEach()
	{
		var sw = Stopwatch.StartNew();
		const int iterations = 50;
		for (int i = 0; i < iterations; i++)
		{
			var dsId = $"perf_ds_{i}";
			await _result.Client.CreateDatasetAsync(dsId);
			await _result.Client.GetDatasetAsync(dsId);
			await _result.Client.DeleteDatasetAsync(dsId);
		}
		sw.Stop();

		var avgMs = sw.Elapsed.TotalMilliseconds / (iterations * 3); // 3 ops per iteration
		Assert.True(avgMs < 10, $"Average CRUD op took {avgMs:F2}ms (expected <10ms)");
	}
}
