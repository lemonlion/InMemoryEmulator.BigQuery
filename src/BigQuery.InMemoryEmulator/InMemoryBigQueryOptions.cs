namespace BigQuery.InMemoryEmulator;

/// <summary>
/// Options for configuring the in-memory BigQuery instance.
/// Used by <see cref="ServiceCollectionExtensions.UseInMemoryBigQuery"/>.
/// </summary>
public class InMemoryBigQueryOptions
{
	/// <summary>The project ID for the emulated instance. Defaults to "test-project".</summary>
	public string ProjectId { get; set; } = "test-project";

	/// <summary>Datasets to pre-create.</summary>
	public List<(string DatasetId, Action<InMemoryDatasetBuilder>? Configure)> Datasets { get; } = [];

	/// <summary>Adds a dataset to be created on initialization.</summary>
	public InMemoryBigQueryOptions AddDataset(string datasetId, Action<InMemoryDatasetBuilder>? configure = null)
	{
		Datasets.Add((datasetId, configure));
		return this;
	}

	/// <summary>Callback invoked after the client is created.</summary>
	public Action<InMemoryBigQueryResult>? OnClientCreated { get; set; }
}
