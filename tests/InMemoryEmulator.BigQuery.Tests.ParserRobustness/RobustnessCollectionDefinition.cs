using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.ParserRobustness;

[CollectionDefinition(IntegrationCollection.Name)]
public class RobustnessCollectionDefinition : ICollectionFixture<BigQuerySession>;
