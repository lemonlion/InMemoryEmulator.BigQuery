using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for expanded built-in functions (Phase 6 supplement).
/// Covers date/time, math edge cases, hash, JSON, conditional, and array functions.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class ExpandedFunctionTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public ExpandedFunctionTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_efn_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "value", Type = "FLOAT", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "ts", Type = "TIMESTAMP", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "data", schema);
		await client.InsertRowsAsync(_datasetId, "data", new[]
		{
			new BigQueryInsertRow("r1") { ["id"] = 1, ["name"] = "Alice", ["value"] = 10.5, ["ts"] = "2024-01-15 10:30:00 UTC" },
			new BigQueryInsertRow("r2") { ["id"] = 2, ["name"] = "Bob", ["value"] = -3.7, ["ts"] = "2024-06-20 14:00:00 UTC" },
			new BigQueryInsertRow("r3") { ["id"] = 3, ["name"] = null, ["value"] = null, ["ts"] = null },
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

	// --- Date/Time Functions ---

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#current_timestamp
	//   "Returns the current date and time as a TIMESTAMP."
	[Fact]
	public async Task CurrentTimestamp_ReturnsNonNull()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT CURRENT_TIMESTAMP() AS ts",
			parameters: null);
		var rows = results.ToList();
		Assert.NotNull(rows[0]["ts"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#current_date
	//   "Returns the current date."
	[Fact]
	public async Task CurrentDate_ReturnsNonNull()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT CURRENT_DATE() AS dt",
			parameters: null);
		var rows = results.ToList();
		Assert.NotNull(rows[0]["dt"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#extract
	//   "Returns the value of a specified date part."
	[Fact]
	public async Task Extract_Year()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT EXTRACT(YEAR FROM ts) AS yr FROM `{_datasetId}.data` WHERE id = 1",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(2024L, Convert.ToInt64(rows[0]["yr"]));
	}

	[Fact]
	public async Task Extract_Month()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT EXTRACT(MONTH FROM ts) AS mo FROM `{_datasetId}.data` WHERE id = 2",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(6L, Convert.ToInt64(rows[0]["mo"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_add
	//   "Adds a specified time interval to a DATE."
	[Fact]
	public async Task DateDiff_DaysBetween()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT DATE_DIFF(DATE '2024-03-01', DATE '2024-01-01', DAY) AS days",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(60L, Convert.ToInt64(rows[0]["days"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_add
	//   "Adds a time interval to a TIMESTAMP."
	[Fact]
	public async Task TimestampAdd_AddsDays()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT TIMESTAMP_ADD(TIMESTAMP '2024-01-01 00:00:00 UTC', INTERVAL 1 DAY) AS result",
			parameters: null);
		var rows = results.ToList();
		Assert.NotNull(rows[0]["result"]);
	}

	// --- Math Functions ---

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#safe_divide
	//   "Equivalent to a / b, but returns NULL if b is zero."
	[Fact]
	public async Task SafeDivide_ByZero_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT SAFE_DIVIDE(10, 0) AS result",
			parameters: null);
		var rows = results.ToList();
		Assert.Null(rows[0]["result"]);
	}

	[Fact]
	public async Task IeeeDivide_ByZero_ReturnsInfinity()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT IEEE_DIVIDE(10.0, 0.0) AS result",
			parameters: null);
		var rows = results.ToList();
		var val = Convert.ToDouble(rows[0]["result"]);
		Assert.True(double.IsInfinity(val));
	}

	[Fact]
	public async Task Abs_NegativeValue()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT ABS(value) AS abs_val FROM `{_datasetId}.data` WHERE id = 2",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(3.7, Convert.ToDouble(rows[0]["abs_val"]), 1);
	}

	[Fact]
	public async Task Round_DecimalPlaces()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT ROUND(3.14159, 2) AS rounded",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(3.14, Convert.ToDouble(rows[0]["rounded"]), 2);
	}

	[Fact]
	public async Task Greatest_ReturnsMax()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT GREATEST(1, 5, 3) AS g",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(5L, Convert.ToInt64(rows[0]["g"]));
	}

	[Fact]
	public async Task Least_ReturnsMin()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT LEAST(1, 5, 3) AS l",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(1L, Convert.ToInt64(rows[0]["l"]));
	}

	// --- Conditional Functions ---

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions#ifnull
	//   "If expr is NULL, returns default_expr; otherwise returns expr."
	[Fact]
	public async Task Ifnull_ReturnsDefault()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT IFNULL(name, 'unknown') AS n FROM `{_datasetId}.data` WHERE id = 3",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal("unknown", (string)rows[0]["n"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions#nullif
	//   "Returns NULL if expr1 = expr2, otherwise returns expr1."
	[Fact]
	public async Task Nullif_EqualValues_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT NULLIF(5, 5) AS result",
			parameters: null);
		var rows = results.ToList();
		Assert.Null(rows[0]["result"]);
	}

	[Fact]
	public async Task Nullif_DifferentValues_ReturnsFirst()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT NULLIF(5, 3) AS result",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(5L, Convert.ToInt64(rows[0]["result"]));
	}

	// --- Hash Functions ---

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#md5
	//   "Computes the hash of the input using the MD5 algorithm."
	[Fact]
	public async Task Md5_ReturnsBytes()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT TO_HEX(MD5('test')) AS h",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal("098f6bcd4621d373cade4e832627b4f6", (string)rows[0]["h"]);
	}

	[Fact]
	public async Task Sha256_ReturnsBytes()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT TO_HEX(SHA256('test')) AS h",
			parameters: null);
		var rows = results.ToList();
		var hex = (string)rows[0]["h"];
		Assert.StartsWith("9f86d081", hex);
	}

	[Fact]
	public async Task FarmFingerprint_ReturnsInt64()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT FARM_FINGERPRINT('test') AS fp",
			parameters: null);
		var rows = results.ToList();
		Assert.NotNull(rows[0]["fp"]);
	}

	// --- String Functions ---

	[Fact]
	public async Task Length_ReturnsCorrectCount()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT LENGTH('hello') AS len",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(5L, Convert.ToInt64(rows[0]["len"]));
	}

	[Fact]
	public async Task Trim_RemovesWhitespace()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT TRIM('  hello  ') AS trimmed",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal("hello", (string)rows[0]["trimmed"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_contains
	//   "Returns TRUE if value matches the regular expression."
	[Fact]
	public async Task RegexpContains_Match()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT name FROM `{_datasetId}.data` WHERE REGEXP_CONTAINS(name, '^A')",
			parameters: null);
		var rows = results.ToList();
		Assert.Single(rows);
		Assert.Equal("Alice", (string)rows[0]["name"]);
	}

	// --- Array Functions ---

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_array
	//   "Returns an array of values."
	[Fact]
	public async Task GenerateArray_CreatesSequence()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT ARRAY_LENGTH(GENERATE_ARRAY(1, 5)) AS len",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(5L, Convert.ToInt64(rows[0]["len"]));
	}

	// --- CAST ---

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast
	//   "Converts a value to the specified type."
	[Fact]
	public async Task Cast_IntToString()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT CAST(42 AS STRING) AS s",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal("42", (string)rows[0]["s"]);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#safe_cast
	//   "SAFE_CAST returns NULL instead of raising an error."
	[Fact]
	public async Task SafeCast_InvalidInput_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT SAFE_CAST('abc' AS INT64) AS result",
			parameters: null);
		var rows = results.ToList();
		Assert.Null(rows[0]["result"]);
	}

	// --- JSON Functions ---

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_extract_scalar
	//   "Extracts a scalar value from a JSON string."
	[Fact]
	public async Task JsonExtractScalar_ReturnsValue()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			@"SELECT JSON_EXTRACT_SCALAR('{""name"":""Alice"",""age"":30}', '$.name') AS n",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal("Alice", (string)rows[0]["n"]);
	}

	[Fact]
	public async Task JsonValue_ReturnsValue()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			@"SELECT JSON_VALUE('{""x"":42}', '$.x') AS v",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal("42", (string)rows[0]["v"]);
	}

	// --- Aggregate Functions ---

	[Fact]
	public async Task Countif_CountsMatching()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT COUNTIF(value > 0) AS cnt FROM `{_datasetId}.data`",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(1L, Convert.ToInt64(rows[0]["cnt"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#string_agg
	//   "Returns a value (either STRING or BYTES) obtained by concatenating non-null values."
	[Fact]
	public async Task StringAgg_CollectsValues()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT STRING_AGG(name, ',') AS names FROM `{_datasetId}.data`",
			parameters: null);
		var rows = results.ToList();
		var names = (string)rows[0]["names"];
		Assert.Contains("Alice", names);
		Assert.Contains("Bob", names);
	}
}
