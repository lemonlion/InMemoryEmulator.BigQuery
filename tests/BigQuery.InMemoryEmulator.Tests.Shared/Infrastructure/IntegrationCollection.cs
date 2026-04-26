namespace BigQuery.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Constants for the integration test collection. The actual
/// <c>[CollectionDefinition]</c> must live in each test project's assembly
/// (xUnit requires fixture sources in the same assembly).
/// </summary>
public static class IntegrationCollection
{
	public const string Name = "Integration";
}
