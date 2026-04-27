using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for UDFs and routines (Phase 17).
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class RoutineTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public RoutineTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_rtn_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "value", Type = "INTEGER", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "data", schema);
		await client.InsertRowsAsync(_datasetId, "data", new[]
		{
			new BigQueryInsertRow("r1") { ["id"] = 1, ["value"] = 10 },
			new BigQueryInsertRow("r2") { ["id"] = 2, ["value"] = 20 },
		});
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

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions
	//   "A user-defined function (UDF) lets you create a function using a SQL expression."
	[Fact]
	public async Task CreateTempFunction_And_Invoke()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$@"CREATE TEMP FUNCTION double_it(x INT64) AS (x * 2);
			SELECT id, double_it(value) AS doubled FROM `{_datasetId}.data` ORDER BY id;",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(2, rows.Count);
		Assert.Equal(20L, Convert.ToInt64(rows[0]["doubled"]));
		Assert.Equal(40L, Convert.ToInt64(rows[1]["doubled"]));
	}

	[Fact]
	public async Task CreateTempFunction_MultipleParams()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$@"CREATE TEMP FUNCTION add_values(a INT64, b INT64) AS (a + b);
			SELECT add_values(10, 20) AS result;",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(30L, Convert.ToInt64(rows[0]["result"]));
	}

	[Fact]
	public async Task CreateTempFunction_StringFunction()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			@"CREATE TEMP FUNCTION greet(name STRING) AS (CONCAT('Hello, ', name, '!'));
			SELECT greet('World') AS greeting;",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal("Hello, World!", (string)rows[0]["greeting"]);
	}

	[Fact]
	public async Task CreateOrReplace_Overwrites()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			@"CREATE TEMP FUNCTION my_fn(x INT64) AS (x + 1);
			CREATE OR REPLACE TEMP FUNCTION my_fn(x INT64) AS (x + 100);
			SELECT my_fn(5) AS result;",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(105L, Convert.ToInt64(rows[0]["result"]));
	}
}

/// <summary>
/// Integration tests for JavaScript UDFs via the Jint engine.
/// These are InMemoryOnly because real BigQuery uses its own JS engine.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions#javascript-udf-structure
/// </summary>
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class JsUdfRoutineTests : IDisposable
{
	private readonly InMemoryBigQueryResult _result;

	public JsUdfRoutineTests()
	{
		_result = InMemoryBigQuery.Create("test-project", "test_ds");
		_result.Store.JsUdfEngine = new BigQuery.InMemoryEmulator.JsUdfs.JintJsUdfEngine();
	}

	public void Dispose() => _result.Dispose();

	[Fact]
	public async Task JsUdf_SimpleArithmetic()
	{
		var results = await _result.Client.ExecuteQueryAsync(
			@"CREATE TEMP FUNCTION plusOne(x FLOAT64) RETURNS FLOAT64 LANGUAGE js AS ""return x+1;"";
			SELECT plusOne(4) AS result;",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(5.0, Convert.ToDouble(rows[0]["result"]));
	}

	[Fact]
	public async Task JsUdf_MultipleParameters()
	{
		var results = await _result.Client.ExecuteQueryAsync(
			@"CREATE TEMP FUNCTION multiply(x FLOAT64, y FLOAT64) RETURNS FLOAT64 LANGUAGE js AS ""return x*y;"";
			SELECT multiply(6, 7) AS result;",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(42.0, Convert.ToDouble(rows[0]["result"]));
	}

	[Fact]
	public async Task JsUdf_StringManipulation()
	{
		var results = await _result.Client.ExecuteQueryAsync(
			@"CREATE TEMP FUNCTION shout(s STRING) RETURNS STRING LANGUAGE js AS ""return s.toUpperCase();"";
			SELECT shout('hello world') AS result;",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal("HELLO WORLD", (string)rows[0]["result"]);
	}

	[Fact]
	public async Task JsUdf_NullHandling()
	{
		var results = await _result.Client.ExecuteQueryAsync(
			@"CREATE TEMP FUNCTION retNull(x FLOAT64) RETURNS FLOAT64 LANGUAGE js AS ""return null;"";
			SELECT retNull(42) AS result;",
			parameters: null);
		var rows = results.ToList();
		Assert.True(rows[0]["result"] is null or DBNull or "");
	}

	[Fact]
	public async Task JsUdf_OrReplace()
	{
		var results = await _result.Client.ExecuteQueryAsync(
			@"CREATE TEMP FUNCTION myJs(x FLOAT64) RETURNS FLOAT64 LANGUAGE js AS ""return x+1;"";
			CREATE OR REPLACE TEMP FUNCTION myJs(x FLOAT64) RETURNS FLOAT64 LANGUAGE js AS ""return x+100;"";
			SELECT myJs(5) AS result;",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(105.0, Convert.ToDouble(rows[0]["result"]));
	}
}
