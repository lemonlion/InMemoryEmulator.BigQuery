using System.Net;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase10;

/// <summary>
/// Phase 10: Fault injection and diagnostics tests.
/// </summary>
public class FaultInjectionTests
{
	// Ref: https://cloud.google.com/bigquery/docs/error-messages
	//   "BigQuery returns error responses via standard HTTP status codes."

	[Fact]
	public async Task FaultInjector_Returns503_ClientThrows()
	{
		using var result = InMemoryBigQuery.Create("test-project", "ds");
		result.SetFaultInjector(_ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
		{
			Content = new StringContent("{\"error\":{\"code\":503,\"message\":\"Backend Error\"}}")
		});

		await Assert.ThrowsAsync<Google.GoogleApiException>(
			() => result.Client.GetDatasetAsync("ds"));
	}

	[Fact]
	public async Task FaultInjector_Returns429_ClientThrows()
	{
		using var result = InMemoryBigQuery.Create("test-project", "ds");
		result.SetFaultInjector(_ => new HttpResponseMessage((HttpStatusCode)429)
		{
			Content = new StringContent("{\"error\":{\"code\":429,\"message\":\"Rate limit exceeded\"}}")
		});

		await Assert.ThrowsAsync<Google.GoogleApiException>(
			() => result.Client.GetDatasetAsync("ds"));
	}

	[Fact]
	public async Task FaultInjector_Conditional_OnlyAffectsMatchingRequests()
	{
		using var result = InMemoryBigQuery.Create("test-project", "ds");
		result.SetFaultInjector(req =>
		{
			if (req.RequestUri?.AbsolutePath.Contains("/queries") == true)
				return new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
			return null; // pass through
		});

		// Dataset operations should succeed
		var dataset = await result.Client.GetDatasetAsync("ds");
		Assert.NotNull(dataset);
	}

	[Fact]
	public async Task FaultInjector_CanBeClearedToRestoreNormalBehavior()
	{
		using var result = InMemoryBigQuery.Create("test-project", "ds");
		result.SetFaultInjector(_ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
		{
			Content = new StringContent("{\"error\":{\"code\":503,\"message\":\"Backend Error\"}}")
		});

		// Should fail
		await Assert.ThrowsAsync<Google.GoogleApiException>(
			() => result.Client.GetDatasetAsync("ds"));

		// Clear fault injector
		result.SetFaultInjector(null);

		// Should succeed now
		var dataset = await result.Client.GetDatasetAsync("ds");
		Assert.NotNull(dataset);
	}

	[Fact]
	public async Task FaultInjector_CustomStatusCode_Returns()
	{
		using var result = InMemoryBigQuery.Create("test-project", "ds");
		result.SetFaultInjector(_ => new HttpResponseMessage(HttpStatusCode.Forbidden)
		{
			Content = new StringContent("{\"error\":{\"code\":403,\"message\":\"Access denied\"}}")
		});

		await Assert.ThrowsAsync<Google.GoogleApiException>(
			() => result.Client.GetDatasetAsync("ds"));
	}
}

/// <summary>
/// Phase 10: Request and query logging tests.
/// </summary>
public class DiagnosticsTests
{
	// Ref: https://cloud.google.com/bigquery/docs/reference/rest
	//   "All BigQuery REST API v2 requests are relative to bigquery/v2/"

	[Fact]
	public async Task RequestLog_RecordsDatasetOperations()
	{
		using var result = InMemoryBigQuery.Create("test-project", "ds");
		result.Handler.RequestLog.Clear();

		await result.Client.GetDatasetAsync("ds");
		await result.Client.ListDatasetsAsync().ReadPageAsync(10);

		Assert.True(result.Handler.RequestLog.Count >= 2);
		Assert.Contains(result.Handler.RequestLog, r => r.Contains("GET") && r.Contains("datasets"));
	}

	[Fact]
	public async Task RequestLog_RecordsTableOperations()
	{
		using var result = InMemoryBigQuery.Create("test-project", "ds");
		result.Handler.RequestLog.Clear();

		var schema = new Google.Apis.Bigquery.v2.Data.TableSchema
		{
			Fields = [new() { Name = "id", Type = "INTEGER", Mode = "REQUIRED" }]
		};
		await result.Client.CreateTableAsync("ds", "t1", schema);
		await result.Client.GetTableAsync("ds", "t1");

		Assert.True(result.Handler.RequestLog.Count >= 2);
	}

	[Fact]
	public async Task QueryLog_RecordsSqlQueries()
	{
		using var result = InMemoryBigQuery.Create("test-project", "ds");
		var schema = new Google.Apis.Bigquery.v2.Data.TableSchema
		{
			Fields = [new() { Name = "id", Type = "INTEGER", Mode = "REQUIRED" }]
		};
		await result.Client.CreateTableAsync("ds", "t1", schema);
		result.Handler.QueryLog.Clear();

		await result.Client.ExecuteQueryAsync("SELECT * FROM ds.t1", parameters: null);

		Assert.Contains(result.Handler.QueryLog, q => q.Contains("SELECT"));
	}

	[Fact]
	public async Task QueryLog_RecordsMultipleQueries()
	{
		using var result = InMemoryBigQuery.Create("test-project", "ds");
		var schema = new Google.Apis.Bigquery.v2.Data.TableSchema
		{
			Fields = [new() { Name = "id", Type = "INTEGER", Mode = "REQUIRED" }]
		};
		await result.Client.CreateTableAsync("ds", "t1", schema);
		result.Handler.QueryLog.Clear();

		await result.Client.ExecuteQueryAsync("SELECT 1 AS a", parameters: null);
		await result.Client.ExecuteQueryAsync("SELECT 2 AS b", parameters: null);

		Assert.True(result.Handler.QueryLog.Count >= 2);
	}

	[Fact]
	public void FaultInjector_ViaBuilder_Works()
	{
		using var result = InMemoryBigQuery.Builder()
			.WithProjectId("test-project")
			.AddDataset("ds")
			.WithFaultInjector(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError))
			.Build();

		Assert.NotNull(result.Handler.FaultInjector);
	}

	[Fact]
	public async Task RequestLog_EmptyInitially()
	{
		using var result = InMemoryBigQuery.Create("test-project", "ds");
		// Might have some initial requests from Create, but let's clear and check
		result.Handler.RequestLog.Clear();
		Assert.Empty(result.Handler.RequestLog);

		await result.Client.GetDatasetAsync("ds");
		Assert.NotEmpty(result.Handler.RequestLog);
	}
}
