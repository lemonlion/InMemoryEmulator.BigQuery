namespace BigQuery.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Trait name constants for categorising parity-validated tests.
/// Apply via <c>[Trait(TestTraits.Target, TestTraits.All)]</c>.
/// </summary>
public static class TestTraits
{
	/// <summary>Trait name for test target scope.</summary>
	public const string Target = "Target";

	/// <summary>Runs against all three targets: in-memory, emulator, and cloud.</summary>
	public const string All = "All";

	/// <summary>Only meaningful against in-memory (direct InMemoryDataStore, fault injection, parser tests, etc.).</summary>
	public const string InMemoryOnly = "InMemoryOnly";

	/// <summary>Only meaningful against the goccy/bigquery-emulator Docker target.</summary>
	public const string EmulatorOnly = "EmulatorOnly";

	/// <summary>Only meaningful against real Google Cloud BigQuery.</summary>
	public const string CloudOnly = "CloudOnly";

	/// <summary>Documents a known divergence between in-memory and real BigQuery.</summary>
	public const string KnownDivergence = "KnownDivergence";

	/// <summary>Documents a known divergence between the goccy emulator and real BigQuery.</summary>
	public const string EmulatorDivergence = "EmulatorDivergence";
}
