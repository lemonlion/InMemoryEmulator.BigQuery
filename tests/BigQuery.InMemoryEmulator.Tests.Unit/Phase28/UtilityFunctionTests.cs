using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase28;

/// <summary>
/// Phase 28: ERROR() function and SESSION_USER() function.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/debugging_functions#error
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/security_functions#session_user
/// </summary>
public class UtilityFunctionTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		store.Datasets["ds"] = new InMemoryDataset("ds");
		return new QueryExecutor(store, "ds");
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/debugging_functions#error
	//   "Produces an error with the given error_message."
	[Fact]
	public void Error_ThrowsWithMessage()
	{
		var exec = CreateExecutor();
		var ex = Assert.ThrowsAny<System.Exception>(() =>
			exec.Execute("SELECT ERROR('something went wrong') AS val"));
		Assert.Contains("something went wrong", ex.Message);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/security_functions#session_user
	//   "Returns the email address of the user running the query."
	[Fact]
	public void SessionUser_ReturnsNonNull()
	{
		var exec = CreateExecutor();
		var result = exec.Execute("SELECT SESSION_USER() AS val");
		Assert.Single(result.Rows);
		Assert.NotNull(result.Rows[0].F[0].V);
		Assert.Equal("emulator@bigquery.local", result.Rows[0].F[0].V);
	}
}
