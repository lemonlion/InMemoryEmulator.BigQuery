namespace BigQuery.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Creates the per-test-class <see cref="ITestDatasetFixture"/> backed by
/// the shared <see cref="BigQuerySession"/>.
/// </summary>
public static class TestFixtureFactory
{
	/// <summary>
	/// Creates a per-test-class fixture. The target (in-memory, emulator, or cloud)
	/// is determined by the session, which reads <c>BIGQUERY_TEST_TARGET</c>
	/// once at construction.
	/// </summary>
	public static ITestDatasetFixture Create(BigQuerySession session) =>
		session.Target switch
		{
			TestTarget.BigQueryCloud => new CloudTestFixture(session),
			TestTarget.BigQueryEmulator => new EmulatorTestFixture(session),
			_ => new InMemoryTestFixture(),
		};
}
