$f = 'src\BigQuery.InMemoryEmulator\FakeBigQueryHandler.cs'
$c = [IO.File]::ReadAllText($f)

$anchor = "`t// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/list" + "`r`n" + "`tprivate HttpResponseMessage HandleListJobs"

$cancelMethod = @"
	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/cancel
	//   "Requests that a job be cancelled."
	private HttpResponseMessage HandleCancelJob(string jobId)
	{
		if (!Jobs.TryGetValue(jobId, out var job))
			return BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
				`$"Not found: Job {_store.ProjectId}:{jobId}");

		// In-memory jobs complete instantly so cancel is a no-op
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/cancel
		//   Response: { "kind": "bigquery#jobCancelResponse", "job": { ... } }
		var response = new Google.Apis.Bigquery.v2.Data.JobCancelResponse
		{
			Kind = "bigquery#jobCancelResponse",
			Job = job.ToJobResource(),
		};
		return BuildJsonResponse(response);
	}

"@

$c = $c.Replace($anchor, $cancelMethod + "`r`n" + $anchor)
[IO.File]::WriteAllText($f, $c)
Write-Output "Added HandleCancelJob"
