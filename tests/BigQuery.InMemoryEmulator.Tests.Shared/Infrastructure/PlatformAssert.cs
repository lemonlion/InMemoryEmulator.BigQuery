using System.Net;
using Xunit.Sdk;

namespace BigQuery.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Helper for asserting values that may differ between test targets.
/// </summary>
public static class PlatformAssert
{
	/// <summary>
	/// Asserts the same expected value across all targets.
	/// </summary>
	public static void AssertForTarget<T>(
		ITestDatasetFixture fixture,
		T actual,
		T expected,
		string? reason = null)
	{
		AssertForTarget(fixture, actual, expected, expected, expected, reason);
	}

	/// <summary>
	/// Asserts different expected values per target.
	/// </summary>
	public static void AssertForTarget<T>(
		ITestDatasetFixture fixture,
		T actual,
		T expectedInMemory,
		T expectedEmulator,
		T expectedCloud,
		string? reason = null)
	{
		var expected = fixture.Target switch
		{
			TestTarget.BigQueryCloud => expectedCloud,
			TestTarget.BigQueryEmulator => expectedEmulator,
			_ => expectedInMemory,
		};

		if (!Equals(actual, expected))
		{
			var message = $"Expected {expected} for target {fixture.Target}, but got {actual}";
			if (reason is not null)
				message += $" — {reason}";
			throw new TestPlatformAssertException(message);
		}
	}

	/// <summary>
	/// Asserts error response characteristics.
	/// </summary>
	public static void AssertError(
		ITestDatasetFixture fixture,
		Google.GoogleApiException exception,
		HttpStatusCode expectedStatus,
		string expectedReason,
		string? expectedMessageContains = null)
	{
		if ((HttpStatusCode)exception.HttpStatusCode != expectedStatus)
			throw new TestPlatformAssertException(
				$"Expected HTTP {(int)expectedStatus} for target {fixture.Target}, but got {exception.HttpStatusCode}");

		if (expectedMessageContains is not null &&
			!(exception.Message?.Contains(expectedMessageContains, StringComparison.OrdinalIgnoreCase) ?? false))
		{
			throw new TestPlatformAssertException(
				$"Expected error message to contain '{expectedMessageContains}' for target {fixture.Target}, but got: {exception.Message}");
		}
	}
}

/// <summary>
/// Exception type for platform assert failures.
/// </summary>
public class TestPlatformAssertException : Exception
{
	public TestPlatformAssertException(string message) : base(message) { }
}
