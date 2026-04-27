using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase20;

/// <summary>
/// Unit tests for Phase 20: Job variants, dry run, labels.
/// </summary>
public class JobVariantTests
{
	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#JobConfiguration
	//   "dryRun: If set, don't actually run this job."

	[Fact]
	public void ToJobResource_IncludesLabels()
	{
		var job = new InMemoryJob("test-project", "j1")
		{
			Query = "SELECT 1",
			Labels = new Dictionary<string, string> { ["env"] = "test", ["team"] = "data" }
		};

		var resource = job.ToJobResource();

		Assert.NotNull(resource.Configuration?.Labels);
		Assert.Equal("test", resource.Configuration!.Labels["env"]);
		Assert.Equal("data", resource.Configuration!.Labels["team"]);
	}

	[Fact]
	public void ToJobResource_IncludesStatementType()
	{
		var job = new InMemoryJob("test-project") { Query = "SELECT 1", StatementType = "SELECT" };
		var resource = job.ToJobResource();

		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobStatistics2
		//   "statementType: The type of query statement, if valid."
		Assert.Equal("SELECT", resource.Statistics?.Query?.StatementType);
	}

	[Fact]
	public void ToJobResource_IncludesNumDmlAffectedRows()
	{
		var job = new InMemoryJob("test-project")
		{
			Query = "DELETE FROM t WHERE true",
			StatementType = "DELETE",
			NumDmlAffectedRows = 42
		};
		var resource = job.ToJobResource();

		Assert.Equal(42, resource.Statistics?.Query?.NumDmlAffectedRows);
	}

	[Fact]
	public void ToJobResource_DryRun_IncludesDryRunFlag()
	{
		var job = new InMemoryJob("test-project") { Query = "SELECT 1", IsDryRun = true };
		var resource = job.ToJobResource();

		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#JobConfiguration
		//   "dryRun: If set, don't actually run this job."
		Assert.True(resource.Configuration?.DryRun);
	}

	[Fact]
	public void ToJobResource_DryRun_HasNoResults()
	{
		var job = new InMemoryJob("test-project") { Query = "SELECT 1", IsDryRun = true };
		var resource = job.ToJobResource();

		Assert.Null(job.ResultRows);
		Assert.Equal(0, job.TotalRows);
	}
}
