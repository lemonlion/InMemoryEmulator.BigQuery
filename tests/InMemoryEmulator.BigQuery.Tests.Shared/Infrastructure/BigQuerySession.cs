using System.Diagnostics;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Infrastructure;

/// <summary>
/// xUnit collection fixture — singleton shared by all integration tests.
/// Reads <c>BIGQUERY_TEST_TARGET</c> to determine which backend to use.
/// Manages Docker container lifecycle for the goccy/bigquery-emulator target.
/// Includes automatic crash detection and container restart.
/// </summary>
public class BigQuerySession : IAsyncLifetime
{
	public TestTarget Target { get; private set; }
	public bool IsCloud => Target == TestTarget.BigQueryCloud;
	public bool IsEmulator => Target == TestTarget.BigQueryEmulator;
	public bool IsRemote => IsCloud || IsEmulator;
	public BigQueryClient? RemoteClient { get; private set; }

	private string? _containerId;
	private bool _externalEmulator;
	private const string EmulatorImage = "ghcr.io/goccy/bigquery-emulator:0.7.2";
	private const int EmulatorRestPort = 9050;
	private const string EmulatorProjectId = "test-project";
	private readonly SemaphoreSlim _restartLock = new(1, 1);

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
		_externalEmulator = !string.IsNullOrEmpty(
			Environment.GetEnvironmentVariable("BIGQUERY_EMULATOR_EXTERNAL"));

		if (!_externalEmulator)
			await StartEmulatorContainerAsync();

		await RebuildClientAsync();
	}

	private async Task RebuildClientAsync()
	{
		RemoteClient?.Dispose();
		var builder = new BigQueryClientBuilder
		{
			ProjectId = EmulatorProjectId,
			BaseUri = $"http://localhost:{EmulatorRestPort}/bigquery/v2/",
			Credential = GoogleCredential.FromAccessToken("fake-token"),
		};
		RemoteClient = await builder.BuildAsync();
	}

	/// <summary>
	/// Checks if the emulator is still alive. If it has crashed, waits for it to recover
	/// (via Docker restart policy or manual restart) and rebuilds the client.
	/// </summary>
	public async Task EnsureEmulatorHealthyAsync()
	{
		if (Target != TestTarget.BigQueryEmulator)
			return;

		await _restartLock.WaitAsync();
		try
		{
			if (await IsEmulatorHealthyAsync())
				return;

			if (!_externalEmulator)
			{
				// Self-managed: stop the dead container and start a new one
				await StopContainerAsync();
				await StartEmulatorContainerAsync();
			}
			else
			{
				// Externally managed with restart policy: wait for Docker to restart it
				await WaitForEmulatorRecoveryAsync();
			}

			await RebuildClientAsync();
		}
		finally
		{
			_restartLock.Release();
		}
	}

	private async Task WaitForEmulatorRecoveryAsync()
	{
		using var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
		var healthUrl = $"http://localhost:{EmulatorRestPort}/bigquery/v2/projects/{EmulatorProjectId}/datasets";
		var maxRetries = 60; // Up to 30 seconds
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
				// Not ready yet
			}

			await Task.Delay(TimeSpan.FromMilliseconds(500));
		}

		throw new InvalidOperationException(
			"Emulator failed to recover after crash — Docker restart may have exceeded retry limit");
	}

	private async Task<bool> IsEmulatorHealthyAsync()
	{
		try
		{
			using var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
			var healthUrl = $"http://localhost:{EmulatorRestPort}/bigquery/v2/projects/{EmulatorProjectId}/datasets";
			var response = await httpClient.GetAsync(healthUrl);
			return response.IsSuccessStatusCode;
		}
		catch
		{
			return false;
		}
	}

	private async Task StartEmulatorContainerAsync()
	{
		// Start the goccy/bigquery-emulator Docker container with memory limit to prevent host OOM
		var startInfo = new ProcessStartInfo
		{
			FileName = "docker",
			Arguments = $"run -d -p {EmulatorRestPort}:{EmulatorRestPort} --memory=2g --name bq-emulator-{Guid.NewGuid():N} {EmulatorImage} --project={EmulatorProjectId}",
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
		var maxRetries = 30;
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

			await Task.Delay(TimeSpan.FromMilliseconds(500));
		}

		throw new InvalidOperationException(
			$"Emulator container failed to become healthy after {maxRetries} retries");
	}

	private async Task StopContainerAsync()
	{
		if (_containerId is null)
			return;

		try
		{
			var stopInfo = new ProcessStartInfo
			{
				FileName = "docker",
				Arguments = $"rm -f {_containerId}",
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

		_containerId = null;
	}

	public async ValueTask DisposeAsync()
	{
		RemoteClient?.Dispose();

		if (!_externalEmulator)
			await StopContainerAsync();
	}
}
