namespace BigQuery.InMemoryEmulator;

/// <summary>
/// Configuration options for <see cref="FakeBigQueryHandler"/>.
/// </summary>
public class FakeBigQueryHandlerOptions
{
	/// <summary>
	/// Optional fault injector for simulating errors.
	/// If the delegate returns a non-null <see cref="HttpResponseMessage"/>,
	/// that response is returned immediately.
	/// </summary>
	public Func<HttpRequestMessage, HttpResponseMessage?>? FaultInjector { get; set; }
}
