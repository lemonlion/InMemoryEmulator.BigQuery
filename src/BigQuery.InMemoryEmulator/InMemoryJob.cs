using Google.Apis.Bigquery.v2.Data;

namespace BigQuery.InMemoryEmulator;

// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs
//   "A job resource represents an asynchronous task."

/// <summary>
/// Tracks a BigQuery job. In-memory jobs complete instantly.
/// </summary>
internal class InMemoryJob
{
	public string JobId { get; }
	public string ProjectId { get; }
	public string State { get; set; } = "DONE";
	public DateTimeOffset CreationTime { get; }
	public DateTimeOffset StartTime { get; }
	public DateTimeOffset EndTime { get; }
	public string? Query { get; set; }
	public string? DefaultDatasetId { get; set; }
	public IList<QueryParameter>? Parameters { get; set; }
	public string? StatementType { get; set; }
	public long TotalBytesProcessed { get; set; }
	public long NumDmlAffectedRows { get; set; }
	public IDictionary<string, string>? Labels { get; set; }
	public bool IsDryRun { get; set; }

	public TableSchema? ResultSchema { get; set; }
	public List<TableRow>? ResultRows { get; set; }
	public long TotalRows { get; set; }

	public InMemoryJob(string projectId, string? jobId = null)
	{
		ProjectId = projectId;
		JobId = jobId ?? Guid.NewGuid().ToString();
		var now = DateTimeOffset.UtcNow;
		CreationTime = now;
		StartTime = now;
		EndTime = now;
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#JobResource
	public Job ToJobResource() => new()
	{
		Kind = "bigquery#job",
		ETag = Guid.NewGuid().ToString(),
		Id = $"{ProjectId}:{JobId}",
		JobReference = new JobReference { ProjectId = ProjectId, JobId = JobId },
		Configuration = new JobConfiguration
		{
			Query = new JobConfigurationQuery { Query = Query, UseLegacySql = false },
			Labels = Labels as Dictionary<string, string> ?? Labels?.ToDictionary(kv => kv.Key, kv => kv.Value),
			DryRun = IsDryRun ? true : null
		},
		Status = new JobStatus { State = State },
		Statistics = new JobStatistics
		{
			CreationTime = CreationTime.ToUnixTimeMilliseconds(),
			StartTime = StartTime.ToUnixTimeMilliseconds(),
			EndTime = EndTime.ToUnixTimeMilliseconds(),
			Query = new JobStatistics2
			{
				TotalBytesProcessed = TotalBytesProcessed,
				TotalBytesBilled = 0,
				CacheHit = false,
				StatementType = StatementType,
				NumDmlAffectedRows = NumDmlAffectedRows > 0 ? NumDmlAffectedRows : null,
			}
		}
	};
}
