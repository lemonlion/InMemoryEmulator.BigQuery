using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for advanced procedural language: EXCEPTION handling, REPEAT,
/// FOR...IN, EXECUTE IMMEDIATE with INTO/USING, nested BEGIN, variables, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ProceduralAdvancedTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public ProceduralAdvancedTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_proc_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	private async Task<List<BigQueryRow>> Q(string sql)
	{
		var client = await _fixture.GetClientAsync();
		return (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
	}

	// ---- DECLARE with types ----
	[Fact] public async Task Declare_Int64() => Assert.Equal("42", await S("DECLARE x INT64 DEFAULT 42; SELECT x;"));
	[Fact] public async Task Declare_Float64() => Assert.Equal("3.14", await S("DECLARE x FLOAT64 DEFAULT 3.14; SELECT x;"));
	[Fact] public async Task Declare_String() => Assert.Equal("hello", await S("DECLARE x STRING DEFAULT 'hello'; SELECT x;"));
	[Fact] public async Task Declare_Bool() => Assert.Equal("True", await S("DECLARE x BOOL DEFAULT TRUE; SELECT x;"));
	[Fact] public async Task Declare_InferredType() => Assert.Equal("42", await S("DECLARE x DEFAULT 42; SELECT x;"));
	[Fact] public async Task Declare_NullDefault() => Assert.Null(await S("DECLARE x INT64; SELECT x;"));
	[Fact] public async Task Declare_Multiple() => Assert.Equal("30", await S("DECLARE a INT64 DEFAULT 10; DECLARE b INT64 DEFAULT 20; SELECT a + b;"));

	// ---- SET with expressions ----
	[Fact] public async Task Set_Expression()
	{
		var v = await S("DECLARE x INT64; SET x = 10 * 5 + 2; SELECT x;");
		Assert.Equal("52", v);
	}
	[Fact] public async Task Set_Concat()
	{
		var v = await S("DECLARE s STRING DEFAULT 'hello'; SET s = CONCAT(s, ' world'); SELECT s;");
		Assert.Equal("hello world", v);
	}
	[Fact] public async Task Set_FromSelect()
	{
		var v = await S("DECLARE x INT64; SET x = (SELECT 42); SELECT x;");
		Assert.Equal("42", v);
	}

	// ---- IF ----
	[Fact] public async Task If_True()
	{
		var v = await S("DECLARE x INT64 DEFAULT 1; IF x = 1 THEN SET x = 10; END IF; SELECT x;");
		Assert.Equal("10", v);
	}
	[Fact] public async Task If_False()
	{
		var v = await S("DECLARE x INT64 DEFAULT 1; IF x = 2 THEN SET x = 10; END IF; SELECT x;");
		Assert.Equal("1", v);
	}
	[Fact] public async Task If_Else()
	{
		var v = await S("DECLARE x INT64 DEFAULT 5; DECLARE r STRING; IF x > 10 THEN SET r = 'big'; ELSE SET r = 'small'; END IF; SELECT r;");
		Assert.Equal("small", v);
	}
	[Fact] public async Task If_ElseIf()
	{
		var v = await S("DECLARE x INT64 DEFAULT 5; DECLARE r STRING; IF x > 10 THEN SET r = 'big'; ELSEIF x > 3 THEN SET r = 'medium'; ELSE SET r = 'small'; END IF; SELECT r;");
		Assert.Equal("medium", v);
	}

	// ---- WHILE ----
	[Fact] public async Task While_Counter()
	{
		var v = await S("DECLARE x INT64 DEFAULT 0; WHILE x < 5 DO SET x = x + 1; END WHILE; SELECT x;");
		Assert.Equal("5", v);
	}
	[Fact] public async Task While_NoExecution()
	{
		var v = await S("DECLARE x INT64 DEFAULT 10; WHILE x < 5 DO SET x = x + 1; END WHILE; SELECT x;");
		Assert.Equal("10", v);
	}

	// ---- LOOP ----
	[Fact] public async Task Loop_WithBreak()
	{
		var v = await S("DECLARE x INT64 DEFAULT 0; LOOP SET x = x + 1; IF x >= 3 THEN BREAK; END IF; END LOOP; SELECT x;");
		Assert.Equal("3", v);
	}
	[Fact] public async Task Loop_WithLeave()
	{
		var v = await S("DECLARE x INT64 DEFAULT 0; LOOP SET x = x + 1; IF x >= 5 THEN LEAVE; END IF; END LOOP; SELECT x;");
		Assert.Equal("5", v);
	}

	// ---- REPEAT ----
	[Fact] public async Task Repeat_Basic()
	{
		var v = await S("DECLARE x INT64 DEFAULT 0; REPEAT SET x = x + 1; UNTIL x >= 3 END REPEAT; SELECT x;");
		Assert.Equal("3", v);
	}
	[Fact] public async Task Repeat_ExecutesAtLeastOnce()
	{
		var v = await S("DECLARE x INT64 DEFAULT 100; REPEAT SET x = x + 1; UNTIL TRUE END REPEAT; SELECT x;");
		Assert.Equal("101", v);
	}

	// ---- FOR...IN ----
	[Fact] public async Task ForIn_Basic()
	{
		var v = await S("DECLARE total INT64 DEFAULT 0; FOR rec IN (SELECT x FROM UNNEST([1,2,3]) AS x) DO SET total = total + rec.x; END FOR; SELECT total;");
		Assert.Equal("6", v);
	}
	[Fact] public async Task ForIn_Empty()
	{
		var v = await S("DECLARE total INT64 DEFAULT 0; FOR rec IN (SELECT x FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x) DO SET total = total + rec.x; END FOR; SELECT total;");
		Assert.Equal("0", v);
	}

	// ---- BEGIN...END ----
	[Fact] public async Task BeginEnd_Scoping()
	{
		var v = await S("DECLARE x INT64 DEFAULT 1; BEGIN DECLARE y INT64 DEFAULT 2; SET x = x + y; END; SELECT x;");
		Assert.Equal("3", v);
	}
	[Fact] public async Task BeginEnd_Nested()
	{
		var v = await S("DECLARE x INT64 DEFAULT 1; BEGIN BEGIN SET x = x + 1; END; SET x = x + 1; END; SELECT x;");
		Assert.Equal("3", v);
	}

	// ---- EXCEPTION handling ----
	[Fact] public async Task Exception_CatchesError()
	{
		var v = await S(@"
			DECLARE r STRING DEFAULT 'ok';
			BEGIN
				SELECT 1 / 0;
			EXCEPTION WHEN ERROR THEN
				SET r = 'caught';
			END;
			SELECT r;
		");
		Assert.Equal("caught", v);
	}
	[Fact] public async Task Exception_ErrorMessage()
	{
		var v = await S(@"
			DECLARE msg STRING;
			BEGIN
				SELECT ERROR('custom error');
			EXCEPTION WHEN ERROR THEN
				SET msg = @@error.message;
			END;
			SELECT msg;
		");
		Assert.NotNull(v);
		Assert.Contains("custom error", v);
	}
	[Fact] public async Task Exception_NoError_SkipsHandler()
	{
		var v = await S(@"
			DECLARE r STRING DEFAULT 'no error';
			BEGIN
				SET r = 'executed';
			EXCEPTION WHEN ERROR THEN
				SET r = 'caught';
			END;
			SELECT r;
		");
		Assert.Equal("executed", v);
	}

	// ---- ASSERT ----
	[Fact] public async Task Assert_True_Passes()
	{
		var v = await S("DECLARE x INT64 DEFAULT 5; ASSERT x > 0; SELECT x;");
		Assert.Equal("5", v);
	}
	[Fact] public async Task Assert_False_Fails()
	{
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAnyAsync<Exception>(() =>
			client.ExecuteQueryAsync("DECLARE x INT64 DEFAULT -1; ASSERT x > 0 AS 'x must be positive';", parameters: null));
	}

	// ---- CONTINUE ----
	[Fact] public async Task Continue_SkipsRemainder()
	{
		var v = await S(@"
			DECLARE total INT64 DEFAULT 0;
			DECLARE i INT64 DEFAULT 0;
			WHILE i < 5 DO
				SET i = i + 1;
				IF MOD(i, 2) = 0 THEN CONTINUE; END IF;
				SET total = total + i;
			END WHILE;
			SELECT total;
		");
		Assert.Equal("9", v); // 1 + 3 + 5
	}

	// ---- EXECUTE IMMEDIATE ----
	[Fact] public async Task ExecuteImmediate_Simple()
	{
		var v = await S("EXECUTE IMMEDIATE 'SELECT 42';");
		Assert.Equal("42", v);
	}
	[Fact] public async Task ExecuteImmediate_WithConcat()
	{
		// Use a numeric variable to avoid string literal substitution issues
		var v = await S("DECLARE x INT64 DEFAULT 42; EXECUTE IMMEDIATE CONCAT('SELECT ', CAST(x AS STRING));");
		Assert.Equal("42", v);
	}

	// ---- CASE statement (procedural) ----
	[Fact] public async Task CaseStatement_MatchFirst()
	{
		var v = await S(@"
			DECLARE x INT64 DEFAULT 1;
			DECLARE r STRING;
			CASE
				WHEN x = 1 THEN SET r = 'one';
				WHEN x = 2 THEN SET r = 'two';
				ELSE SET r = 'other';
			END CASE;
			SELECT r;
		");
		Assert.Equal("one", v);
	}
	[Fact] public async Task CaseStatement_MatchElse()
	{
		var v = await S(@"
			DECLARE x INT64 DEFAULT 99;
			DECLARE r STRING;
			CASE
				WHEN x = 1 THEN SET r = 'one';
				WHEN x = 2 THEN SET r = 'two';
				ELSE SET r = 'other';
			END CASE;
			SELECT r;
		");
		Assert.Equal("other", v);
	}

	// ---- CREATE TEMP TABLE in procedural ----
	[Fact] public async Task CreateTempTable_InsertAndSelect()
	{
		var v = await S(@"
			CREATE TEMP TABLE tmp (id INT64, name STRING);
			INSERT INTO tmp VALUES (1, 'hello');
			SELECT name FROM tmp;
		");
		Assert.Equal("hello", v);
	}

	// ---- RAISE ----
	[Fact] public async Task Raise_WithMessage()
	{
		var client = await _fixture.GetClientAsync();
		var ex = await Assert.ThrowsAnyAsync<Exception>(() =>
			client.ExecuteQueryAsync("RAISE USING MESSAGE = 'test error';", parameters: null));
		Assert.Contains("test error", ex.Message);
	}

	// ---- Nested IF ----
	[Fact] public async Task If_Nested()
	{
		var v = await S(@"
			DECLARE x INT64 DEFAULT 15;
			DECLARE r STRING;
			IF x > 10 THEN
				IF x > 20 THEN
					SET r = 'very big';
				ELSE
					SET r = 'big';
				END IF;
			ELSE
				SET r = 'small';
			END IF;
			SELECT r;
		");
		Assert.Equal("big", v);
	}

	// ---- Variable in WHERE clause ----
	[Fact] public async Task Variable_InWhere()
	{
		var client = await _fixture.GetClientAsync();
		await client.CreateTableAsync(_datasetId, "pdata", new TableSchema
		{
			Fields = [new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" }, new TableFieldSchema { Name = "val", Type = "INTEGER" }]
		});
		await client.InsertRowsAsync(_datasetId, "pdata", new[]
		{
			new BigQueryInsertRow("r1") { ["id"] = 1, ["val"] = 10 },
			new BigQueryInsertRow("r2") { ["id"] = 2, ["val"] = 20 },
			new BigQueryInsertRow("r3") { ["id"] = 3, ["val"] = 30 },
		});
		var v = await S($"DECLARE threshold INT64 DEFAULT 15; SELECT COUNT(*) FROM `{_datasetId}.pdata` WHERE val > threshold;");
		Assert.Equal("2", v);
	}

	// ---- Multiple result sets (last wins) ----
	[Fact] public async Task MultipleSelects_LastWins()
	{
		var v = await S("SELECT 1; SELECT 2; SELECT 3;");
		Assert.Equal("3", v);
	}
}
