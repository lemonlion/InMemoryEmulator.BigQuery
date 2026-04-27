namespace BigQuery.InMemoryEmulator.JsUdfs;

/// <summary>
/// Extension methods for registering JavaScript UDF support.
/// </summary>
public static class JsUdfExtensions
{
	/// <summary>
	/// Enables JavaScript UDF execution on this data store using the Jint engine.
	/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions#javascript-udf-structure
	/// </summary>
	public static InMemoryDataStore UseJsUdfs(this InMemoryDataStore store)
	{
		store.JsUdfEngine = new JintJsUdfEngine();
		return store;
	}
}
