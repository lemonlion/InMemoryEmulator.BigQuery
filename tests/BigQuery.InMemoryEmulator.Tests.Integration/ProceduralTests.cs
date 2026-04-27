using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for procedural language features (Phases 15-16):
/// DECLARE, SET, IF/ELSE, LOOP, WHILE, temp tables, multi-statement.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class ProceduralTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public ProceduralTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_proc_{Guid.NewGuid():N}"[..30];
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
			new BigQueryInsertRow("r3") { ["id"] = 3, ["value"] = 30 },
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

	// --- DECLARE / SET ---

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#declare
	//   "DECLARE declares a variable with the given type."
	[Fact]
	public async Task DeclareAndSet_SelectWithVariable()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$@"DECLARE threshold INT64 DEFAULT 15;
			SELECT * FROM `{_datasetId}.data` WHERE value > threshold;",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(2, rows.Count); // 20 and 30
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#set
	//   "SET updates the value of a variable."
	[Fact]
	public async Task Set_UpdatesVariableValue()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$@"DECLARE x INT64 DEFAULT 5;
			SET x = 25;
			SELECT * FROM `{_datasetId}.data` WHERE value > x;",
			parameters: null);
		var rows = results.ToList();
		Assert.Single(rows); // Only 30
	}

	// --- IF/ELSE ---

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#if
	//   "IF evaluates a condition. If the condition is true, then the IF block is executed."
	[Fact]
	public async Task IfElse_TrueBranch()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$@"DECLARE flag BOOL DEFAULT TRUE;
			IF flag THEN
				SELECT 'yes' AS answer;
			ELSE
				SELECT 'no' AS answer;
			END IF;",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal("yes", (string)rows[0]["answer"]);
	}

	[Fact]
	public async Task IfElse_FalseBranch()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$@"DECLARE flag BOOL DEFAULT FALSE;
			IF flag THEN
				SELECT 'yes' AS answer;
			ELSE
				SELECT 'no' AS answer;
			END IF;",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal("no", (string)rows[0]["answer"]);
	}

	// --- ASSERT ---

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#assert
	//   "Evaluates a condition. If false, raises an error."
	[Fact]
	public async Task Assert_True_NoError()
	{
		var client = await _fixture.GetClientAsync();
		// Should succeed without error
		var results = await client.ExecuteQueryAsync(
			$@"ASSERT (SELECT COUNT(*) FROM `{_datasetId}.data`) > 0;
			SELECT 'ok' AS status;",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal("ok", (string)rows[0]["status"]);
	}

	// --- Multi-statement ---

	[Fact]
	public async Task MultiStatement_LastResultReturned()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$@"DECLARE total INT64;
			SET total = (SELECT SUM(value) FROM `{_datasetId}.data`);
			SELECT total AS result;",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(60L, Convert.ToInt64(rows[0]["result"]));
	}

	// --- Temp Tables ---

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#create_temp_table
	//   "CREATE TEMP TABLE creates a temporary table."
	[Fact]
	public async Task CreateTempTable_InsertAndQuery()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$@"CREATE TEMP TABLE tmp (val INT64);
			INSERT INTO tmp (val) VALUES (1), (2), (3);
			SELECT SUM(val) AS total FROM tmp;",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(6L, Convert.ToInt64(rows[0]["total"]));
	}

	// --- WHILE loop ---

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#while
	//   "WHILE evaluates a condition and executes the loop body while the condition is true."
	[Fact]
	public async Task While_Loop()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			@"DECLARE i INT64 DEFAULT 0;
			DECLARE result INT64 DEFAULT 0;
			WHILE i < 5 DO
				SET i = i + 1;
				SET result = result + i;
			END WHILE;
			SELECT result;",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(15L, Convert.ToInt64(rows[0].RawRow.F[0].V)); // 1+2+3+4+5
	}
}
