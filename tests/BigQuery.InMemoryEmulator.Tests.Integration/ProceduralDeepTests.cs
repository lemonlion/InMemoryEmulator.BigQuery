using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Deep integration tests for procedural scripting: variables, control flow, error handling, dynamic SQL.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ProceduralDeepTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public ProceduralDeepTests(BigQuerySession session) => _session = session;

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

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		return result.ToList();
	}

	private async Task<string?> Scalar(string sql)
	{
		var rows = await Query(sql);
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ============================================================
	// DECLARE and SET
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#declare
	[Fact]
	public async Task Declare_Int64()
	{
		var result = await Scalar("DECLARE x INT64; SET x = 42; SELECT x;");
		Assert.Equal("42", result);
	}

	[Fact]
	public async Task Declare_String()
	{
		var result = await Scalar("DECLARE s STRING; SET s = 'hello'; SELECT s;");
		Assert.Equal("hello", result);
	}

	[Fact]
	public async Task Declare_Float64()
	{
		var result = await Scalar("DECLARE f FLOAT64; SET f = 3.14; SELECT f;");
		Assert.StartsWith("3.14", result);
	}

	[Fact]
	public async Task Declare_Bool()
	{
		var result = await Scalar("DECLARE b BOOL; SET b = TRUE; SELECT b;");
		Assert.Equal("True", result);
	}

	[Fact]
	public async Task Declare_Date()
	{
		var result = await Scalar("DECLARE d DATE; SET d = '2024-06-15'; SELECT d;");
		Assert.Equal("2024-06-15", result);
	}

	[Fact]
	public async Task Declare_WithDefault()
	{
		var result = await Scalar("DECLARE x INT64 DEFAULT 99; SELECT x;");
		Assert.Equal("99", result);
	}

	[Fact]
	public async Task Declare_WithExpressionDefault()
	{
		var result = await Scalar("DECLARE x INT64 DEFAULT 10 + 20; SELECT x;");
		Assert.Equal("30", result);
	}

	[Fact]
	public async Task Declare_MultipleVariables()
	{
		var result = await Scalar("DECLARE a INT64 DEFAULT 1; DECLARE b INT64 DEFAULT 2; SELECT a + b;");
		Assert.Equal("3", result);
	}

	[Fact]
	public async Task Set_ToExpression()
	{
		var result = await Scalar("DECLARE x INT64 DEFAULT 5; SET x = x * 2 + 1; SELECT x;");
		Assert.Equal("11", result);
	}

	[Fact]
	public async Task Set_ToFunctionResult()
	{
		var result = await Scalar("DECLARE s STRING; SET s = CONCAT('hello', ' ', 'world'); SELECT s;");
		Assert.Equal("hello world", result);
	}

	[Fact]
	public async Task Set_Reassignment()
	{
		var result = await Scalar("DECLARE x INT64 DEFAULT 1; SET x = 2; SET x = 3; SELECT x;");
		Assert.Equal("3", result);
	}

	[Fact]
	public async Task Variable_InSelect()
	{
		var result = await Scalar("DECLARE multiplier INT64 DEFAULT 5; SELECT 10 * multiplier;");
		Assert.Equal("50", result);
	}

	// ============================================================
	// IF / ELSEIF / ELSE
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#if
	[Fact]
	public async Task If_TrueCondition()
	{
		var result = await Scalar("DECLARE x INT64 DEFAULT 10; DECLARE r STRING DEFAULT 'no'; IF x > 5 THEN SET r = 'yes'; END IF; SELECT r;");
		Assert.Equal("yes", result);
	}

	[Fact]
	public async Task If_FalseCondition()
	{
		var result = await Scalar("DECLARE x INT64 DEFAULT 3; DECLARE r STRING DEFAULT 'no'; IF x > 5 THEN SET r = 'yes'; END IF; SELECT r;");
		Assert.Equal("no", result);
	}

	[Fact]
	public async Task If_Else()
	{
		var result = await Scalar("DECLARE x INT64 DEFAULT 3; DECLARE r STRING; IF x > 5 THEN SET r = 'big'; ELSE SET r = 'small'; END IF; SELECT r;");
		Assert.Equal("small", result);
	}

	[Fact]
	public async Task If_Elseif_Else()
	{
		var result = await Scalar(@"
			DECLARE x INT64 DEFAULT 5;
			DECLARE r STRING;
			IF x > 10 THEN SET r = 'high';
			ELSEIF x > 3 THEN SET r = 'mid';
			ELSE SET r = 'low';
			END IF;
			SELECT r;");
		Assert.Equal("mid", result);
	}

	[Fact]
	public async Task If_Nested()
	{
		var result = await Scalar(@"
			DECLARE x INT64 DEFAULT 10;
			DECLARE r STRING DEFAULT 'none';
			IF x > 5 THEN
				IF x > 8 THEN SET r = 'very high';
				ELSE SET r = 'high';
				END IF;
			END IF;
			SELECT r;");
		Assert.Equal("very high", result);
	}

	[Fact]
	public async Task If_ComplexCondition()
	{
		var result = await Scalar(@"
			DECLARE a INT64 DEFAULT 5;
			DECLARE b INT64 DEFAULT 10;
			DECLARE r STRING DEFAULT 'no';
			IF a > 3 AND b < 20 THEN SET r = 'yes'; END IF;
			SELECT r;");
		Assert.Equal("yes", result);
	}

	// ============================================================
	// WHILE loop
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#while
	[Fact]
	public async Task While_CountTo10()
	{
		var result = await Scalar(@"
			DECLARE i INT64 DEFAULT 0;
			WHILE i < 10 DO SET i = i + 1; END WHILE;
			SELECT i;");
		Assert.Equal("10", result);
	}

	[Fact]
	public async Task While_NeverExecutes()
	{
		var result = await Scalar(@"
			DECLARE i INT64 DEFAULT 100;
			WHILE i < 10 DO SET i = i + 1; END WHILE;
			SELECT i;");
		Assert.Equal("100", result);
	}

	[Fact]
	public async Task While_WithBreak()
	{
		var result = await Scalar(@"
			DECLARE i INT64 DEFAULT 0;
			WHILE TRUE DO
				SET i = i + 1;
				IF i = 5 THEN BREAK; END IF;
			END WHILE;
			SELECT i;");
		Assert.Equal("5", result);
	}

	[Fact]
	public async Task While_WithContinue()
	{
		var result = await Scalar(@"
			DECLARE i INT64 DEFAULT 0;
			DECLARE s INT64 DEFAULT 0;
			WHILE i < 10 DO
				SET i = i + 1;
				IF MOD(i, 2) = 0 THEN CONTINUE; END IF;
				SET s = s + i;
			END WHILE;
			SELECT s;");
		// Sum of odd 1-9: 1+3+5+7+9 = 25
		Assert.Equal("25", result);
	}

	[Fact]
	public async Task While_BuildingSum()
	{
		var result = await Scalar(@"
			DECLARE i INT64 DEFAULT 1;
			DECLARE total INT64 DEFAULT 0;
			WHILE i <= 100 DO
				SET total = total + i;
				SET i = i + 1;
			END WHILE;
			SELECT total;");
		Assert.Equal("5050", result);
	}

	// ============================================================
	// LOOP
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#loop
	[Fact]
	public async Task Loop_WithBreak()
	{
		var result = await Scalar(@"
			DECLARE i INT64 DEFAULT 0;
			LOOP
				SET i = i + 1;
				IF i >= 7 THEN BREAK; END IF;
			END LOOP;
			SELECT i;");
		Assert.Equal("7", result);
	}

	[Fact]
	public async Task Loop_WithContinueAndBreak()
	{
		var result = await Scalar(@"
			DECLARE i INT64 DEFAULT 0;
			DECLARE s INT64 DEFAULT 0;
			LOOP
				SET i = i + 1;
				IF i > 10 THEN BREAK; END IF;
				IF i < 5 THEN CONTINUE; END IF;
				SET s = s + i;
			END LOOP;
			SELECT s;");
		// Sum of 5..10: 5+6+7+8+9+10 = 45
		Assert.Equal("45", result);
	}

	// ============================================================
	// Nested loops
	// ============================================================

	[Fact]
	public async Task NestedWhile()
	{
		var result = await Scalar(@"
			DECLARE i INT64 DEFAULT 0;
			DECLARE j INT64;
			DECLARE total INT64 DEFAULT 0;
			WHILE i < 3 DO
				SET j = 0;
				WHILE j < 4 DO
					SET total = total + 1;
					SET j = j + 1;
				END WHILE;
				SET i = i + 1;
			END WHILE;
			SELECT total;");
		Assert.Equal("12", result); // 3 * 4
	}

	// ============================================================
	// BEGIN ... EXCEPTION
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#beginexceptionend
	[Fact]
	public async Task BeginException_CatchesDivisionByZero()
	{
		var result = await Scalar(@"
			DECLARE r STRING DEFAULT 'no error';
			BEGIN
				SELECT 1/0;
			EXCEPTION WHEN ERROR THEN
				SET r = 'caught';
			END;
			SELECT r;");
		Assert.Equal("caught", result);
	}

	[Fact]
	public async Task BeginException_NoError_SkipsHandler()
	{
		var result = await Scalar(@"
			DECLARE r STRING DEFAULT 'ok';
			BEGIN
				SET r = 'still ok';
			EXCEPTION WHEN ERROR THEN
				SET r = 'error';
			END;
			SELECT r;");
		Assert.Equal("still ok", result);
	}

	// ============================================================
	// EXECUTE IMMEDIATE
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#execute_immediate
	[Fact]
	public async Task ExecuteImmediate_SimpleQuery()
	{
		var result = await Scalar("EXECUTE IMMEDIATE 'SELECT 42'; ");
		Assert.Equal("42", result);
	}

	[Fact]
	public async Task ExecuteImmediate_WithVariable()
	{
		var result = await Scalar(@"
			DECLARE sql STRING DEFAULT 'SELECT 1 + 2';
			EXECUTE IMMEDIATE sql;");
		Assert.Equal("3", result);
	}

	[Fact]
	public async Task ExecuteImmediate_WithConcat()
	{
		var result = await Scalar(@"
			DECLARE col STRING DEFAULT '42';
			EXECUTE IMMEDIATE CONCAT('SELECT ', col);");
		Assert.Equal("42", result);
	}

	[Fact]
	public async Task ExecuteImmediate_Into()
	{
		var result = await Scalar(@"
			DECLARE x INT64;
			EXECUTE IMMEDIATE 'SELECT 99' INTO x;
			SELECT x;");
		Assert.Equal("99", result);
	}

	// ============================================================
	// CREATE TEMP TABLE
	// ============================================================

	[Fact]
	public async Task CreateTempTable_InsertAndSelect()
	{
		var result = await Scalar(@"
			CREATE TEMP TABLE tmp (id INT64, val STRING);
			INSERT INTO tmp VALUES (1, 'a'), (2, 'b');
			SELECT COUNT(*) FROM tmp;");
		Assert.Equal("2", result);
	}

	[Fact]
	public async Task CreateTempTable_UsedInSubsequentStatement()
	{
		var result = await Scalar(@"
			CREATE TEMP TABLE tmp (x INT64);
			INSERT INTO tmp VALUES (10), (20), (30);
			SELECT SUM(x) FROM tmp;");
		Assert.Equal("60", result);
	}

	// ============================================================
	// CREATE TEMP FUNCTION (SQL UDF)
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions
	[Fact]
	public async Task CreateTempFunction_Basic()
	{
		var result = await Scalar(@"
			CREATE TEMP FUNCTION double_it(x INT64) AS (x * 2);
			SELECT double_it(21);");
		Assert.Equal("42", result);
	}

	[Fact]
	public async Task CreateTempFunction_MultiParam()
	{
		var result = await Scalar(@"
			CREATE TEMP FUNCTION add_mul(a INT64, b INT64, c INT64) AS (a + b * c);
			SELECT add_mul(1, 2, 3);");
		Assert.Equal("7", result); // 1 + 2*3
	}

	[Fact]
	public async Task CreateTempFunction_StringUdf()
	{
		var result = await Scalar(@"
			CREATE TEMP FUNCTION greet(name STRING) AS (CONCAT('Hello, ', name, '!'));
			SELECT greet('World');");
		Assert.Equal("Hello, World!", result);
	}

	[Fact]
	public async Task CreateTempFunction_NestedCalls()
	{
		var result = await Scalar(@"
			CREATE TEMP FUNCTION inc(x INT64) AS (x + 1);
			SELECT inc(inc(inc(0)));");
		Assert.Equal("3", result);
	}

	[Fact]
	public async Task CreateOrReplace_TempFunction()
	{
		var result = await Scalar(@"
			CREATE TEMP FUNCTION f(x INT64) AS (x * 10);
			CREATE OR REPLACE TEMP FUNCTION f(x INT64) AS (x * 100);
			SELECT f(5);");
		Assert.Equal("500", result);
	}

	// ============================================================
	// DROP FUNCTION
	// ============================================================

	[Fact]
	public async Task DropFunction_IfExists()
	{
		var result = await Scalar(@"
			CREATE TEMP FUNCTION myfn(x INT64) AS (x);
			DROP FUNCTION IF EXISTS myfn;
			SELECT 'ok';");
		Assert.Equal("ok", result);
	}

	// ============================================================
	// ASSERT
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#assert
	[Fact]
	public async Task Assert_TrueCondition()
	{
		var result = await Scalar(@"
			ASSERT 1 + 1 = 2;
			SELECT 'passed';");
		Assert.Equal("passed", result);
	}

	[Fact]
	public async Task Assert_WithMessage_Passes()
	{
		var result = await Scalar(@"
			ASSERT 5 > 3 AS 'Five should be greater than three';
			SELECT 'ok';");
		Assert.Equal("ok", result);
	}

	// ============================================================
	// Transactions (stubs)
	// ============================================================

	[Fact]
	public async Task BeginTransaction_CommitTransaction()
	{
		var result = await Scalar(@"
			BEGIN TRANSACTION;
			DECLARE x INT64 DEFAULT 42;
			COMMIT TRANSACTION;
			SELECT x;");
		Assert.Equal("42", result);
	}

	// ============================================================
	// Variable used in DML
	// ============================================================

	[Fact]
	public async Task Variable_InDml()
	{
		var client = await _fixture.GetClientAsync();
		var schema = new Google.Apis.Bigquery.v2.Data.TableSchema
		{
			Fields = [new() { Name = "id", Type = "INTEGER", Mode = "REQUIRED" }, new() { Name = "val", Type = "STRING", Mode = "NULLABLE" }]
		};
		await client.CreateTableAsync(_datasetId, "proc_tbl", schema);

		var result = await Scalar($@"
			DECLARE v STRING DEFAULT 'scripted';
			INSERT INTO `{_datasetId}.proc_tbl` (id, val) VALUES (1, v);
			SELECT val FROM `{_datasetId}.proc_tbl` WHERE id = 1;");
		Assert.Equal("scripted", result);
	}

	// ============================================================
	// CASE statement (procedural, not expression)
	// ============================================================

	[Fact]
	public async Task CaseStatement_SearchedForm()
	{
		var result = await Scalar(@"
			DECLARE x INT64 DEFAULT 7;
			DECLARE r STRING;
			CASE
				WHEN x > 10 THEN SET r = 'high';
				WHEN x > 5 THEN SET r = 'mid';
				ELSE SET r = 'low';
			END CASE;
			SELECT r;");
		Assert.Equal("mid", result);
	}

	// ============================================================
	// Multiple statements selecting from tables
	// ============================================================

	[Fact]
	public async Task MultiStatement_CreateInsertQuery()
	{
		var result = await Scalar($@"
			CREATE TEMP TABLE nums (n INT64);
			DECLARE i INT64 DEFAULT 1;
			WHILE i <= 5 DO
				INSERT INTO nums VALUES (i);
				SET i = i + 1;
			END WHILE;
			SELECT SUM(n) FROM nums;");
		Assert.Equal("15", result);
	}

	[Fact]
	public async Task MultiStatement_ConditionalInsert()
	{
		var result = await Scalar($@"
			CREATE TEMP TABLE results (val INT64);
			DECLARE i INT64 DEFAULT 1;
			WHILE i <= 10 DO
				IF MOD(i, 3) = 0 THEN
					INSERT INTO results VALUES (i);
				END IF;
				SET i = i + 1;
			END WHILE;
			SELECT COUNT(*) FROM results;");
		Assert.Equal("3", result); // 3, 6, 9
	}

	// ============================================================
	// FOR...IN loop
	// ============================================================

	[Fact]
	public async Task ForIn_SimpleQuery()
	{
		var result = await Scalar(@"
			DECLARE total INT64 DEFAULT 0;
			FOR record IN (SELECT x FROM UNNEST([1, 2, 3, 4, 5]) AS x) DO
				SET total = total + record.x;
			END FOR;
			SELECT total;");
		Assert.Equal("15", result);
	}

	[Fact]
	public async Task ForIn_EmptyResultSet()
	{
		var result = await Scalar(@"
			DECLARE total INT64 DEFAULT 0;
			FOR record IN (SELECT x FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x) DO
				SET total = total + record.x;
			END FOR;
			SELECT total;");
		Assert.Equal("0", result);
	}

	[Fact]
	public async Task ForIn_WithBreak()
	{
		var result = await Scalar(@"
			DECLARE total INT64 DEFAULT 0;
			FOR record IN (SELECT x FROM UNNEST([1, 2, 3, 4, 5]) AS x) DO
				IF record.x > 3 THEN BREAK; END IF;
				SET total = total + record.x;
			END FOR;
			SELECT total;");
		Assert.Equal("6", result); // 1+2+3
	}

	// ============================================================
	// RAISE
	// ============================================================

	[Fact]
	public async Task Raise_CaughtByException()
	{
		var result = await Scalar(@"
			DECLARE r STRING DEFAULT 'not caught';
			BEGIN
				RAISE USING MESSAGE = 'test error';
			EXCEPTION WHEN ERROR THEN
				SET r = 'caught';
			END;
			SELECT r;");
		Assert.Equal("caught", result);
	}

	// ============================================================
	// Variable scoping
	// ============================================================

	[Fact]
	public async Task Variable_OuterScopeVisibleInner()
	{
		var result = await Scalar(@"
			DECLARE x INT64 DEFAULT 10;
			BEGIN
				SET x = x + 5;
			END;
			SELECT x;");
		Assert.Equal("15", result);
	}
}
