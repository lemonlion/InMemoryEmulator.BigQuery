using Google.Apis.Http;

namespace BigQuery.InMemoryEmulator;

/// <summary>
/// Implements <see cref="IHttpClientFactory"/> to inject the <see cref="FakeBigQueryHandler"/>
/// into the Google API client pipeline. No auth initializers are added.
/// </summary>
/// <remarks>
/// Ref: https://cloud.google.com/bigquery/docs/reference/rest
///   The SDK uses BaseClientService.Initializer.HttpClientFactory to create the HTTP client.
///   By providing our own factory, we intercept all SDK HTTP calls.
/// </remarks>
public class FakeBigQueryHttpClientFactory : IHttpClientFactory
{
	private readonly HttpMessageHandler _handler;

	public FakeBigQueryHttpClientFactory(HttpMessageHandler handler)
	{
		_handler = handler ?? throw new ArgumentNullException(nameof(handler));
	}

	public ConfigurableHttpClient CreateHttpClient(CreateHttpClientArgs args)
	{
		var configurableHandler = new ConfigurableMessageHandler(_handler)
		{
			IsLoggingEnabled = false,
		};
		var client = new ConfigurableHttpClient(configurableHandler);
		return client;
	}
}
