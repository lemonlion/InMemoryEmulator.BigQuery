using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit;

public class InMemoryJobTests
{
	[Fact]
	public void Create_ImmediatelyDone()
	{
		var job = new InMemoryJob("test-project");

		Assert.Equal("DONE", job.State);
		Assert.NotNull(job.JobId);
		Assert.Equal(job.CreationTime, job.EndTime);
	}

	[Fact]
	public void ToJobResource_HasRequiredFields()
	{
		var job = new InMemoryJob("test-project", "my-job") { Query = "SELECT 1" };
		var resource = job.ToJobResource();

		Assert.Equal("bigquery#job", resource.Kind);
		Assert.Equal("test-project:my-job", resource.Id);
		Assert.Equal("DONE", resource.Status?.State);
		Assert.Equal("SELECT 1", resource.Configuration?.Query?.Query);
		Assert.NotNull(resource.Statistics);
	}
}
