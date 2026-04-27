using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase28;

/// <summary>
/// Phase 28: Procedural language — EXECUTE IMMEDIATE, FOR...IN, REPEAT,
/// procedural CASE, Labels, CALL, transactions, exception handling.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language
/// </summary>
public class ProceduralLanguageTests
{
	private static (SqlEngine.ProceduralExecutor Exec, InMemoryDataStore Store) CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		store.Datasets["ds"] = new InMemoryDataset("ds");
		return (new SqlEngine.ProceduralExecutor(store, "ds"), store);
	}

	#region EXECUTE IMMEDIATE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#execute_immediate
	//   "Executes a dynamic SQL statement on the fly."
	[Fact]
	public void ExecuteImmediate_SimpleSql()
	{
		var (exec, _) = CreateExecutor();
		var result = exec.Execute("EXECUTE IMMEDIATE 'SELECT 1 + 2 AS val';");
		Assert.Single(result.Rows);
		Assert.Equal("3", result.Rows[0].F[0].V);
	}

	[Fact]
	public void ExecuteImmediate_WithVariable()
	{
		var (exec, _) = CreateExecutor();
		var result = exec.Execute(@"
			DECLARE sql_str STRING DEFAULT 'SELECT 42 AS answer';
			EXECUTE IMMEDIATE sql_str;
		");
		Assert.Single(result.Rows);
		Assert.Equal("42", result.Rows[0].F[0].V);
	}

	[Fact]
	public void ExecuteImmediate_IntoVariable()
	{
		var (exec, _) = CreateExecutor();
		var result = exec.Execute(@"
			DECLARE x INT64;
			EXECUTE IMMEDIATE 'SELECT 99' INTO x;
			SELECT x AS val;
		");
		Assert.Single(result.Rows);
		Assert.Equal("99", result.Rows[0].F[0].V);
	}

	#endregion

	#region FOR...IN

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#for-in
	//   "Loops over each row produced by a SQL statement."
	[Fact]
	public void ForIn_IteratesOverRows()
	{
		var (exec, _) = CreateExecutor();
		var result = exec.Execute(@"
			DECLARE total INT64 DEFAULT 0;
			FOR record IN (SELECT val FROM UNNEST([1, 2, 3]) AS val)
			DO
				SET total = total + record.val;
			END FOR;
			SELECT total AS val;
		");
		Assert.Single(result.Rows);
		Assert.Equal("6", result.Rows[0].F[0].V);
	}

	#endregion

	#region REPEAT

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#repeat
	//   "Executes the body until the condition is true."
	[Fact]
	public void Repeat_UntilCondition()
	{
		var (exec, _) = CreateExecutor();
		var result = exec.Execute(@"
			DECLARE i INT64 DEFAULT 0;
			REPEAT
				SET i = i + 1;
			UNTIL i >= 5
			END REPEAT;
			SELECT i AS val;
		");
		Assert.Single(result.Rows);
		Assert.Equal("5", result.Rows[0].F[0].V);
	}

	#endregion

	#region Procedural CASE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#case_statement
	//   "Executes the first matching WHEN clause's statement list."
	[Fact]
	public void CaseStatement_MatchesValue()
	{
		var (exec, _) = CreateExecutor();
		var result = exec.Execute(@"
			DECLARE x INT64 DEFAULT 2;
			DECLARE result STRING;
			CASE x
				WHEN 1 THEN SET result = 'one';
				WHEN 2 THEN SET result = 'two';
				WHEN 3 THEN SET result = 'three';
			END CASE;
			SELECT result AS val;
		");
		Assert.Single(result.Rows);
		Assert.Equal("two", result.Rows[0].F[0].V);
	}

	[Fact]
	public void CaseStatement_ElseBranch()
	{
		var (exec, _) = CreateExecutor();
		var result = exec.Execute(@"
			DECLARE x INT64 DEFAULT 99;
			DECLARE result STRING;
			CASE x
				WHEN 1 THEN SET result = 'one';
				ELSE SET result = 'other';
			END CASE;
			SELECT result AS val;
		");
		Assert.Single(result.Rows);
		Assert.Equal("other", result.Rows[0].F[0].V);
	}

	#endregion

	#region Labels

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#labels
	//   "Labels can be used to break out of or continue in a loop."
	[Fact]
	public void Labels_BreakOuterLoop()
	{
		var (exec, _) = CreateExecutor();
		var result = exec.Execute(@"
			DECLARE x INT64 DEFAULT 0;
			outer_loop: LOOP
				SET x = x + 1;
				IF x >= 3 THEN
					BREAK;
				END IF;
			END LOOP;
			SELECT x AS val;
		");
		Assert.Single(result.Rows);
		Assert.Equal("3", result.Rows[0].F[0].V);
	}

	#endregion

	#region CALL

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#call
	//   "Calls a stored procedure."
	[Fact]
	public void Call_NoOp_DoesNotThrow()
	{
		var (exec, _) = CreateExecutor();
		// CALL to a non-existent procedure should be a no-op in our emulator
		// (procedures are stubs)
		exec.Execute("CALL ds.some_procedure();");
		// Just verifying it doesn't throw
	}

	#endregion

	#region Transactions

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#begin_transaction
	//   "BEGIN TRANSACTION starts a transaction."
	[Fact]
	public void BeginCommitTransaction_NoOp()
	{
		var (exec, _) = CreateExecutor();
		var result = exec.Execute(@"
			BEGIN TRANSACTION;
			SELECT 1 AS val;
			COMMIT TRANSACTION;
		");
		Assert.Single(result.Rows);
		Assert.Equal("1", result.Rows[0].F[0].V);
	}

	[Fact]
	public void RollbackTransaction_NoOp()
	{
		var (exec, _) = CreateExecutor();
		var result = exec.Execute(@"
			BEGIN TRANSACTION;
			SELECT 42 AS val;
			ROLLBACK TRANSACTION;
		");
		// After rollback, last result should still be available (it's a no-op)
		Assert.Single(result.Rows);
		Assert.Equal("42", result.Rows[0].F[0].V);
	}

	#endregion

	#region Exception Handling

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#begin_exception
	//   "BEGIN...EXCEPTION WHEN ERROR THEN...END handles exceptions."
	[Fact]
	public void BeginException_CatchesError()
	{
		var (exec, _) = CreateExecutor();
		var result = exec.Execute(@"
			DECLARE result STRING DEFAULT 'no error';
			BEGIN
				RAISE USING MESSAGE = 'test error';
			EXCEPTION WHEN ERROR THEN
				SET result = 'caught';
			END;
			SELECT result AS val;
		");
		Assert.Single(result.Rows);
		Assert.Equal("caught", result.Rows[0].F[0].V);
	}

	#endregion
}
