using Google.Apis.Bigquery.v2.Data;

namespace BigQuery.InMemoryEmulator;

/// <summary>
/// Builder for configuring an in-memory dataset during <see cref="InMemoryBigQuery.Create"/>.
/// </summary>
public class InMemoryDatasetBuilder
{
	private readonly InMemoryDataset _dataset;

	internal InMemoryDatasetBuilder(InMemoryDataset dataset)
	{
		_dataset = dataset;
	}

	/// <summary>
	/// Adds a table with the given schema to the dataset.
	/// </summary>
	public InMemoryDatasetBuilder AddTable(string tableId, TableSchema schema)
	{
		var table = new InMemoryTable(_dataset.DatasetId, tableId, schema);
		_dataset.Tables[tableId] = table;
		return this;
	}

	/// <summary>Sets the dataset description.</summary>
	public InMemoryDatasetBuilder WithDescription(string description)
	{
		_dataset.Description = description;
		return this;
	}

	/// <summary>Sets the dataset location.</summary>
	public InMemoryDatasetBuilder WithLocation(string location)
	{
		_dataset.Location = location;
		return this;
	}

	/// <summary>Sets labels on the dataset.</summary>
	public InMemoryDatasetBuilder WithLabels(IDictionary<string, string> labels)
	{
		_dataset.Labels = labels;
		return this;
	}

	/// <summary>Sets the default table expiration in milliseconds.</summary>
	public InMemoryDatasetBuilder WithDefaultTableExpiration(long milliseconds)
	{
		_dataset.DefaultTableExpirationMs = milliseconds;
		return this;
	}
}
