using BigQuery.InMemoryEmulator;
using Google.Apis.Http;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit;

public class FakeBigQueryHttpClientFactoryTests
{
	[Fact]
	public void CreateHttpClient_ReturnsConfigurableClient()
	{
		// Arrange
		var store = new InMemoryDataStore("test-project");
		var handler = new FakeBigQueryHandler(store);
		var factory = new FakeBigQueryHttpClientFactory(handler);

		// Act
		var client = factory.CreateHttpClient(new CreateHttpClientArgs());

		// Assert
		Assert.NotNull(client);
		Assert.IsType<ConfigurableHttpClient>(client);
	}
}
