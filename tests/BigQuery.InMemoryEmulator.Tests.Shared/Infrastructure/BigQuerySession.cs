using System.Diagnostics;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// xUnit collection fixture — singleton shared by all integration tests.
/// Reads <c>BIGQUERY_TEST_TARGET</c> to determine which backend to use.
/// Manages Docker container lifecycle for the goccy/bigquery-emulator target.
/// </summary>
public class BigQuerySession : IAsyncLifetime
{
	public TestTarget Target { get; private set; }
	public bool IsCloud => Target == TestTarget.BigQueryCloud;
	public bool IsEmulator => Target == TestTarget.BigQueryEmulator;
	public bool IsRemote => IsCloud || IsEmulator;
	public BigQueryClient? RemoteClient { get; private set; }

	private string? _containerId;
	private const string EmulatorImage = "ghcr.io/goccy/bigquery-emulator:0.6.6";
	private const int EmulatorRestPort = 9050;
	private const string EmulatorProjectId = "test-project";

	public ValueTask InitializeAsync()
	{
		var target = Environment.GetEnvironmentVariable("BIGQUERY_TEST_TARGET");
		Target = target switch
		{
			"cloud" => TestTarget.BigQueryCloud,
			"emulator" => TestTarget.BigQueryEmulator,
			_ => TestTarget.InMemory,
		};

		return Target switch
		{
			TestTarget.BigQueryCloud => new ValueTask(InitializeCloudAsync()),
			TestTarget.BigQueryEmulator => new ValueTask(InitializeEmulatorAsync()),
			_ => ValueTask.CompletedTask,
		};
	}

	private async Task InitializeCloudAsync()
	{
		var projectId = Environment.GetEnvironmentVariable("BIGQUERY_PROJECT_ID")
			?? throw new InvalidOperationException(
				"BIGQUERY_PROJECT_ID environment variable is required for cloud target");

		RemoteClient = await BigQueryClient.CreateAsync(projectId);
	}

	private async Task InitializeEmulatorAsync()
	{
		await StartEmulatorContainerAsync();

		var builder = new BigQueryClientBuilder
		{
			ProjectId = EmulatorProjectId,
			BaseUri = $"http://localhost:{EmulatorRestPort}/bigquery/v2/",
			Credential = GoogleCredential.FromAccessToken("fake-token"),
		};
		RemoteClient = await builder.BuildAsync();
	}

	private async Task StartEmulatorContainerAsync()
	{
		// Start the goccy/bigquery-emulator Docker container
		var startInfo = new ProcessStartInfo
		{
			FileName = "docker",
			Arguments = $"run -d --rm -p {EmulatorRestPort}:{EmulatorRestPort} {EmulatorImage} --project={EmulatorProjectId}",
			RedirectStandardOutput = true,
			RedirectStandardError = true,
			UseShellExecute = false,
		};

		using var process = Process.Start(startInfo)
			?? throw new InvalidOperationException("Failed to start Docker container");

		_containerId = (await process.StandardOutput.ReadToEndAsync()).Trim();
		await process.WaitForExitAsync();

		if (process.ExitCode != 0)
		{
			var error = await process.StandardError.ReadToEndAsync();
			throw new InvalidOperationException(
				$"Failed to start emulator container (exit code {process.ExitCode}): {error}");
		}

		// Health check with retry
		using var httpClient = new HttpClient();
		var healthUrl = $"http://localhost:{EmulatorRestPort}/bigquery/v2/projects/{EmulatorProjectId}/datasets";
		var maxRetries = 20;
		for (var i = 0; i < maxRetries; i++)
		{
			try
			{
				var response = await httpClient.GetAsync(healthUrl);
				if (response.IsSuccessStatusCode)
					return;
			}
			catch
			{
				// Container not ready yet
			}

			await Task.Delay(TimeSpan.FromMilliseconds(500 * (i + 1)));
		}

		throw new InvalidOperationException(
			$"Emulator container failed to become healthy after {maxRetries} retries");
	}

	public async ValueTask DisposeAsync()
	{
		RemoteClient?.Dispose();

		if (_containerId is not null)
		{
			try
			{
				var stopInfo = new ProcessStartInfo
				{
					FileName = "docker",
					Arguments = $"stop {_containerId}",
					RedirectStandardOutput = true,
					RedirectStandardError = true,
					UseShellExecute = false,
				};
				using var process = Process.Start(stopInfo);
				if (process is not null)
					await process.WaitForExitAsync();
			}
			catch
			{
				// Best-effort cleanup
			}
		}
	}
}
