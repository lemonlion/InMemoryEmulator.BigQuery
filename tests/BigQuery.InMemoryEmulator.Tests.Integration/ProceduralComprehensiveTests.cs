using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Comprehensive procedural language tests.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ProceduralComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public ProceduralComprehensiveTests(BigQuerySession session) => _session = session;

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

	// ---- DECLARE + SET + SELECT ----
	[Fact] public async Task Declare_And_Set()
	{
		var v = await Scalar("DECLARE x INT64; SET x = 42; SELECT x;");
		Assert.Equal("42", v);
	}

	[Fact] public async Task Declare_WithDefault()
	{
		var v = await Scalar("DECLARE x INT64 DEFAULT 10; SELECT x;");
		Assert.Equal("10", v);
	}

	[Fact] public async Task Declare_String()
	{
		var v = await Scalar("DECLARE s STRING DEFAULT 'hello'; SELECT s;");
		Assert.Equal("hello", v);
	}

	[Fact] public async Task Declare_Multiple()
	{
		var v = await Scalar("DECLARE a INT64 DEFAULT 1; DECLARE b INT64 DEFAULT 2; SELECT a + b;");
		Assert.Equal("3", v);
	}

	[Fact] public async Task Set_Expression()
	{
		var v = await Scalar("DECLARE x INT64; SET x = 3 * 7 + 1; SELECT x;");
		Assert.Equal("22", v);
	}

	[Fact] public async Task Set_FromSelect()
	{
		var v = await Scalar("DECLARE x INT64; SET x = (SELECT 99); SELECT x;");
		Assert.Equal("99", v);
	}

	// ---- IF / ELSEIF / ELSE ----
	[Fact] public async Task If_TrueBranch()
	{
		var v = await Scalar("DECLARE r STRING DEFAULT 'none'; IF TRUE THEN SET r = 'yes'; END IF; SELECT r;");
		Assert.Equal("yes", v);
	}

	[Fact] public async Task If_FalseBranch_Else()
	{
		var v = await Scalar("DECLARE r STRING DEFAULT 'none'; IF FALSE THEN SET r = 'if'; ELSE SET r = 'else'; END IF; SELECT r;");
		Assert.Equal("else", v);
	}

	[Fact(Skip = "Procedural: ELSEIF clause not supported")] public async Task If_ElseIf()
	{
		var v = await Scalar(@"
			DECLARE x INT64 DEFAULT 2;
			DECLARE r STRING;
			IF x = 1 THEN SET r = 'one';
			ELSEIF x = 2 THEN SET r = 'two';
			ELSE SET r = 'other';
			END IF;
			SELECT r;
		");
		Assert.Equal("two", v);
	}

	[Fact(Skip = "Procedural: nested IF not supported")] public async Task If_NestedIf()
	{
		var v = await Scalar(@"
			DECLARE x INT64 DEFAULT 5;
			DECLARE r STRING;
			IF x > 0 THEN
				IF x > 10 THEN SET r = 'big';
				ELSE SET r = 'small';
				END IF;
			ELSE SET r = 'negative';
			END IF;
			SELECT r;
		");
		Assert.Equal("small", v);
	}

	// ---- WHILE ----
	[Fact] public async Task While_CountsTo5()
	{
		var v = await Scalar(@"
			DECLARE i INT64 DEFAULT 0;
			WHILE i < 5 DO
				SET i = i + 1;
			END WHILE;
			SELECT i;
		");
		Assert.Equal("5", v);
	}

	[Fact] public async Task While_SumTo10()
	{
		var v = await Scalar(@"
			DECLARE i INT64 DEFAULT 1;
			DECLARE total INT64 DEFAULT 0;
			WHILE i <= 10 DO
				SET total = total + i;
				SET i = i + 1;
			END WHILE;
			SELECT total;
		");
		Assert.Equal("55", v);
	}

	// ---- LOOP / BREAK ----
	[Fact] public async Task Loop_WithBreak()
	{
		var v = await Scalar(@"
			DECLARE i INT64 DEFAULT 0;
			LOOP
				SET i = i + 1;
				IF i >= 3 THEN BREAK; END IF;
			END LOOP;
			SELECT i;
		");
		Assert.Equal("3", v);
	}

	[Fact] public async Task Loop_WithLeave()
	{
		var v = await Scalar(@"
			DECLARE i INT64 DEFAULT 0;
			LOOP
				SET i = i + 1;
				IF i >= 5 THEN LEAVE; END IF;
			END LOOP;
			SELECT i;
		");
		Assert.Equal("5", v);
	}

	// ---- CONTINUE / ITERATE ----
	[Fact] public async Task While_WithContinue()
	{
		var v = await Scalar(@"
			DECLARE i INT64 DEFAULT 0;
			DECLARE s INT64 DEFAULT 0;
			WHILE i < 10 DO
				SET i = i + 1;
				IF MOD(i, 2) = 0 THEN CONTINUE; END IF;
				SET s = s + i;
			END WHILE;
			SELECT s;
		");
		Assert.Equal("25", v); // 1+3+5+7+9
	}

	// ---- FOR ----
	[Fact(Skip = "Not yet supported")] public async Task For_OverQuery()
	{
		var v = await Scalar($@"
			CREATE TABLE `{_datasetId}.ft1` (val INT64);
			INSERT INTO `{_datasetId}.ft1` VALUES (10), (20), (30);
			DECLARE total INT64 DEFAULT 0;
			FOR rec IN (SELECT val FROM `{_datasetId}.ft1`) DO
				SET total = total + rec.val;
			END FOR;
			SELECT total;
		");
		Assert.Equal("60", v);
	}

	// ---- REPEAT ----
	[Fact(Skip = "Not yet supported")] public async Task Repeat_Until()
	{
		var v = await Scalar(@"
			DECLARE i INT64 DEFAULT 0;
			REPEAT
				SET i = i + 1;
			UNTIL i >= 4
			END REPEAT;
			SELECT i;
		");
		Assert.Equal("4", v);
	}

	// ---- CASE (procedural) ----
	[Fact(Skip = "Not yet supported")] public async Task ProceduralCase_Simple()
	{
		var v = await Scalar(@"
			DECLARE x INT64 DEFAULT 2;
			DECLARE r STRING;
			CASE x
				WHEN 1 THEN SET r = 'one';
				WHEN 2 THEN SET r = 'two';
				ELSE SET r = 'other';
			END CASE;
			SELECT r;
		");
		Assert.Equal("two", v);
	}

	// ---- ASSERT ----
	[Fact] public async Task Assert_TrueCondition_NoError()
	{
		await Scalar("ASSERT TRUE AS 'This should pass'; SELECT 1;");
	}

	[Fact] public async Task Assert_FalseCondition_ThrowsError()
	{
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAnyAsync<Exception>(async () => await client.ExecuteQueryAsync("ASSERT FALSE AS 'assertion failed';", parameters: null));
	}

	// ---- RAISE ----
	[Fact] public async Task Raise_ThrowsError()
	{
		var client = await _fixture.GetClientAsync();
		await Assert.ThrowsAnyAsync<Exception>(async () => await client.ExecuteQueryAsync("RAISE USING MESSAGE = 'custom error';", parameters: null));
	}

	// ---- BEGIN ... EXCEPTION WHEN ERROR THEN ... END ----
	[Fact] public async Task BeginException_CatchesError()
	{
		var v = await Scalar(@"
			DECLARE r STRING DEFAULT 'no error';
			BEGIN
				SELECT 1/0;
			EXCEPTION WHEN ERROR THEN
				SET r = 'caught';
			END;
			SELECT r;
		");
		Assert.Equal("caught", v);
	}

	// ---- EXECUTE IMMEDIATE ----
	[Fact(Skip = "Not yet supported")] public async Task ExecuteImmediate_SimpleSelect()
	{
		var v = await Scalar("DECLARE result INT64; EXECUTE IMMEDIATE 'SELECT 42' INTO result; SELECT result;");
		Assert.Equal("42", v);
	}

	[Fact(Skip = "Not yet supported")] public async Task ExecuteImmediate_DynamicSql()
	{
		var v = await Scalar(@"
			DECLARE tbl STRING DEFAULT 'SELECT 1 + 2';
			DECLARE r INT64;
			EXECUTE IMMEDIATE tbl INTO r;
			SELECT r;
		");
		Assert.Equal("3", v);
	}

	// ---- CREATE TEMP FUNCTION ----
	[Fact] public async Task CreateTempFunction_Sql()
	{
		var v = await Scalar(@"
			CREATE TEMP FUNCTION double_it(x INT64) RETURNS INT64 AS (x * 2);
			SELECT double_it(21);
		");
		Assert.Equal("42", v);
	}

	[Fact] public async Task CreateTempFunction_StringReturn()
	{
		var v = await Scalar(@"
			CREATE TEMP FUNCTION greet(name STRING) RETURNS STRING AS (CONCAT('Hello, ', name, '!'));
			SELECT greet('World');
		");
		Assert.Equal("Hello, World!", v);
	}

	// ---- Variables in SQL ----
	[Fact(Skip = "Procedural: variable references in WHERE not supported")] public async Task Variable_InWhereClause()
	{
		var v = await Scalar($@"
			CREATE TABLE `{_datasetId}.vt1` (id INT64, val STRING);
			INSERT INTO `{_datasetId}.vt1` VALUES (1, 'a'), (2, 'b'), (3, 'c');
			DECLARE target_id INT64 DEFAULT 2;
			SELECT val FROM `{_datasetId}.vt1` WHERE id = target_id;
		");
		Assert.Equal("b", v);
	}

	[Fact(Skip = "Procedural: variable references in INSERT not supported")] public async Task Variable_InInsert()
	{
		var v = await Scalar($@"
			CREATE TABLE `{_datasetId}.vt2` (id INT64, val STRING);
			DECLARE my_id INT64 DEFAULT 99;
			DECLARE my_val STRING DEFAULT 'inserted';
			INSERT INTO `{_datasetId}.vt2` VALUES (my_id, my_val);
			SELECT val FROM `{_datasetId}.vt2` WHERE id = 99;
		");
		Assert.Equal("inserted", v);
	}

	// ---- @@row_count ----
	[Fact(Skip = "Procedural: @@row_count system variable not supported")] public async Task RowCount_AfterInsert()
	{
		var v = await Scalar($@"
			CREATE TABLE `{_datasetId}.rc1` (id INT64);
			INSERT INTO `{_datasetId}.rc1` VALUES (1), (2), (3);
			SELECT @@row_count;
		");
		Assert.Equal("3", v);
	}

	// ---- Transaction stubs (no-op) ----
	[Fact(Skip = "Not yet supported")] public async Task BeginTransaction_NoError()
	{
		await Scalar("BEGIN TRANSACTION; SELECT 1; COMMIT TRANSACTION;");
	}

	// ---- RETURN ----
	[Fact(Skip = "Not yet supported")] public async Task Return_ExitsEarly()
	{
		var v = await Scalar(@"
			DECLARE x INT64 DEFAULT 1;
			BEGIN
				SET x = 10;
				RETURN;
				SET x = 99;
			END;
			SELECT x;
		");
		Assert.Equal("10", v);
	}

	// ---- Multiple statements producing results ----
	[Fact] public async Task MultiStatement_LastSelectIsResult()
	{
		var v = await Scalar(@"
			DECLARE a INT64 DEFAULT 5;
			DECLARE b INT64 DEFAULT 10;
			SELECT a + b;
		");
		Assert.Equal("15", v);
	}

	// ---- DROP FUNCTION ----
	[Fact(Skip = "Not yet supported")] public async Task DropFunction_IfExists()
	{
		await Scalar("CREATE TEMP FUNCTION my_fn(x INT64) RETURNS INT64 AS (x); DROP FUNCTION IF EXISTS my_fn; SELECT 1;");
	}
}
