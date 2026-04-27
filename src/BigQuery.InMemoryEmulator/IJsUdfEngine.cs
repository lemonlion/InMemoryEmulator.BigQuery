namespace BigQuery.InMemoryEmulator;

/// <summary>
/// Interface for executing JavaScript UDF bodies.
/// Implement this to enable LANGUAGE js support in CREATE FUNCTION statements.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions#javascript-udf-structure
/// </summary>
public interface IJsUdfEngine
{
	/// <summary>
	/// Executes a JavaScript UDF body with the given parameter names and argument values.
	/// </summary>
	/// <param name="body">The JavaScript function body (e.g. "return x + 1;").</param>
	/// <param name="parameterNames">The names of the function parameters.</param>
	/// <param name="arguments">The argument values (already evaluated from SQL expressions).</param>
	/// <returns>The result of the JavaScript execution, or null.</returns>
	object? Execute(string body, IReadOnlyList<string> parameterNames, IReadOnlyList<object?> arguments);
}
