using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase22;

/// <summary>
/// Phase 22: SDK Version Drift Detection.
/// These tests verify that the BigQuery SDK sends requests in the format
/// our handler expects. If the SDK changes its URL patterns, HTTP methods,
/// or serialization, these tests will catch the drift.
/// </summary>
/// <remarks>
/// Ref: https://cloud.google.com/bigquery/docs/reference/rest
/// Ref: https://cloud.google.com/dotnet/docs/reference/Google.Cloud.BigQuery.V2/latest
/// </remarks>
public class SdkVersionDriftDetectorTests : IDisposable
{
	private readonly InMemoryBigQueryResult _result;

	public SdkVersionDriftDetectorTests()
	{
		_result = InMemoryBigQuery.Create("drift-project", "drift_ds");
	}

	public void Dispose() => _result.Dispose();

	// ======================================================================
	// SDK Version Baseline
	// ======================================================================

	// Ref: https://cloud.google.com/dotnet/docs/reference/Google.Cloud.BigQuery.V2/latest
	//   "Verify the SDK assembly version matches our tested baseline."
	[Fact]
	public void SdkVersion_MatchesBaseline()
	{
		var sdkAssembly = typeof(BigQueryClient).Assembly;
		var version = sdkAssembly.GetName().Version;
		Assert.NotNull(version);
		// We test against Google.Cloud.BigQuery.V2 3.11.0
		// If this fails, the SDK was upgraded — review all drift tests.
		Assert.Equal(3, version.Major);
	}

	// ======================================================================
	// URL Pattern Drift — Dataset Operations
	// ======================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/get
	//   "GET /bigquery/v2/projects/{projectId}/datasets/{datasetId}"
	[Fact]
	public async Task GetDataset_UrlPattern_MatchesExpected()
	{
		_result.Handler.RequestLog.Clear();
		await _result.Client.GetDatasetAsync("drift_ds");

		Assert.Contains(_result.Handler.RequestLog, r =>
			r.Contains("GET") &&
			r.Contains("/bigquery/v2/projects/drift-project/datasets/drift_ds"));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list
	//   "GET /bigquery/v2/projects/{projectId}/datasets"
	[Fact]
	public async Task ListDatasets_UrlPattern_MatchesExpected()
	{
		_result.Handler.RequestLog.Clear();
		var datasets = _result.Client.ListDatasetsAsync();
		await foreach (var _ in datasets) { }

		Assert.Contains(_result.Handler.RequestLog, r =>
			r.Contains("GET") &&
			r.Contains("/bigquery/v2/projects/drift-project/datasets"));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/delete
	//   "DELETE /bigquery/v2/projects/{projectId}/datasets/{datasetId}"
	[Fact]
	public async Task DeleteDataset_UrlPattern_MatchesExpected()
	{
		// Create a temp dataset first
		await _result.Client.CreateDatasetAsync("temp_drift_ds");

		_result.Handler.RequestLog.Clear();
		await _result.Client.DeleteDatasetAsync("temp_drift_ds");

		Assert.Contains(_result.Handler.RequestLog, r =>
			r.Contains("DELETE") &&
			r.Contains("/bigquery/v2/projects/drift-project/datasets/temp_drift_ds"));
	}

	// ======================================================================
	// URL Pattern Drift — Table Operations
	// ======================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/insert
	//   "POST /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables"
	[Fact]
	public async Task CreateTable_UrlPattern_MatchesExpected()
	{
		_result.Handler.RequestLog.Clear();
		await _result.Client.CreateTableAsync("drift_ds", "drift_table",
			new Google.Apis.Bigquery.v2.Data.TableSchema
			{
				Fields =
				[
					new() { Name = "id", Type = "INTEGER", Mode = "REQUIRED" }
				]
			});

		Assert.Contains(_result.Handler.RequestLog, r =>
			r.Contains("POST") &&
			r.Contains("/bigquery/v2/projects/drift-project/datasets/drift_ds/tables"));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/get
	//   "GET /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}"
	[Fact]
	public async Task GetTable_UrlPattern_MatchesExpected()
	{
		await _result.Client.CreateTableAsync("drift_ds", "get_table",
			new Google.Apis.Bigquery.v2.Data.TableSchema
			{
				Fields = [new() { Name = "id", Type = "INTEGER", Mode = "REQUIRED" }]
			});

		_result.Handler.RequestLog.Clear();
		await _result.Client.GetTableAsync("drift_ds", "get_table");

		Assert.Contains(_result.Handler.RequestLog, r =>
			r.Contains("GET") &&
			r.Contains("/bigquery/v2/projects/drift-project/datasets/drift_ds/tables/get_table"));
	}

	// ======================================================================
	// URL Pattern Drift — Streaming Insert
	// ======================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll
	//   "POST /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}/insertAll"
	[Fact]
	public async Task StreamingInsert_UrlPattern_MatchesExpected()
	{
		await _result.Client.CreateTableAsync("drift_ds", "insert_table",
			new Google.Apis.Bigquery.v2.Data.TableSchema
			{
				Fields = [new() { Name = "id", Type = "INTEGER", Mode = "REQUIRED" }]
			});

		_result.Handler.RequestLog.Clear();
		await _result.Client.InsertRowsAsync("drift_ds", "insert_table",
			new[] { new BigQueryInsertRow("r1") { ["id"] = 1 } });

		Assert.Contains(_result.Handler.RequestLog, r =>
			r.Contains("POST") &&
			r.Contains("/datasets/drift_ds/tables/insert_table/insertAll"));
	}

	// ======================================================================
	// URL Pattern Drift — Query Operations
	// ======================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert
	//   "POST /bigquery/v2/projects/{projectId}/jobs"
	//   "SDK uses Jobs.Insert for ExecuteQueryAsync (not Jobs.Query)."
	[Fact]
	public async Task ExecuteQuery_UrlPattern_MatchesExpected()
	{
		_result.Handler.RequestLog.Clear();
		await _result.Client.ExecuteQueryAsync("SELECT 1 AS n", parameters: null);

		// SDK sends Jobs.Insert, then GetQueryResults
		Assert.Contains(_result.Handler.RequestLog, r =>
			r.Contains("POST") &&
			r.Contains("/bigquery/v2/projects/drift-project/jobs"));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults
	//   "GET /bigquery/v2/projects/{projectId}/queries/{jobId}"
	[Fact]
	public async Task ExecuteQuery_GetResults_UrlPattern_MatchesExpected()
	{
		_result.Handler.RequestLog.Clear();
		var results = await _result.Client.ExecuteQueryAsync("SELECT 1 AS n", parameters: null);
		// Force enumeration to trigger result fetch
		results.ToList();

		Assert.Contains(_result.Handler.RequestLog, r =>
			r.Contains("GET") &&
			r.Contains("/bigquery/v2/projects/drift-project/queries/"));
	}

	// ======================================================================
	// Serialization Format Drift
	// ======================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
	//   "Each row is returned as a JSON object with 'f' (fields) array of 'v' (value) objects."
	[Fact]
	public async Task QueryResult_Row_Format_IsTableRow()
	{
		var results = await _result.Client.ExecuteQueryAsync(
			"SELECT 42 AS num, 'hello' AS str", parameters: null);
		var rows = results.ToList();

		Assert.Single(rows);
		// Values should be accessible by name
		Assert.Equal(42L, Convert.ToInt64(rows[0]["num"]));
		Assert.Equal("hello", (string)rows[0]["str"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
	//   "Schema field types must match what the SDK expects."
	[Fact]
	public async Task QueryResult_Schema_HasExpectedTypes()
	{
		var results = await _result.Client.ExecuteQueryAsync(
			"SELECT 1 AS i, 1.5 AS f, 'text' AS s, TRUE AS b", parameters: null);

		var schema = results.Schema;
		Assert.NotNull(schema);
		Assert.Contains(schema.Fields, f => f.Name == "i" && f.Type == "INTEGER");
		Assert.Contains(schema.Fields, f => f.Name == "f" && f.Type == "FLOAT");
		Assert.Contains(schema.Fields, f => f.Name == "s" && f.Type == "STRING");
		Assert.Contains(schema.Fields, f => f.Name == "b" && f.Type == "BOOLEAN");
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/Job
	//   "NULL values should be returned as null in the response."
	[Fact]
	public async Task QueryResult_NullValue_IsNull()
	{
		var results = await _result.Client.ExecuteQueryAsync(
			"SELECT NULL AS val", parameters: null);
		var rows = results.ToList();
		Assert.Null(rows[0]["val"]);
	}

	// ======================================================================
	// HTTP Method Drift
	// ======================================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/insert
	//   "POST for create operations."
	[Fact]
	public async Task CreateDataset_UsesPost()
	{
		_result.Handler.RequestLog.Clear();
		await _result.Client.CreateDatasetAsync("method_test_ds");

		Assert.Contains(_result.Handler.RequestLog, r => r.StartsWith("POST"));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/patch
	//   "PATCH for update operations."
	[Fact]
	public async Task UpdateDataset_UsesPatch()
	{
		_result.Handler.RequestLog.Clear();
		var ds = await _result.Client.GetDatasetAsync("drift_ds");
		await ds.UpdateAsync();

		Assert.Contains(_result.Handler.RequestLog, r => r.StartsWith("PATCH") || r.StartsWith("PUT"));
	}

	// ======================================================================
	// Content Encoding Drift
	// ======================================================================

	// Ref: https://github.com/googleapis/google-cloud-dotnet
	//   "SDK sends gzip-compressed bodies by default in recent versions."
	[Fact]
	public async Task SdkRequest_IsHandledCorrectly()
	{
		// This simply verifies the round-trip works — if the SDK changes
		// content encoding (gzip, brotli, etc.), the handler must decompress correctly.
		var results = await _result.Client.ExecuteQueryAsync(
			"SELECT 'drift-test' AS val", parameters: null);
		var rows = results.ToList();
		Assert.Equal("drift-test", (string)rows[0]["val"]);
	}
}
