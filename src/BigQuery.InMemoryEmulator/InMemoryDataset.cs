using System.Collections.Concurrent;

namespace BigQuery.InMemoryEmulator;

/// <summary>
/// Represents an in-memory BigQuery dataset containing tables, views, and routines.
/// </summary>
public class InMemoryDataset
{
	public string DatasetId { get; }
	public string? Description { get; set; }
	public string? FriendlyName { get; set; }
	public string Location { get; set; } = "US";
	public IDictionary<string, string>? Labels { get; set; }
	public long? DefaultTableExpirationMs { get; set; }
	public DateTimeOffset CreationTime { get; }
	public DateTimeOffset LastModifiedTime { get; internal set; }
	public string Etag { get; internal set; }

	internal ConcurrentDictionary<string, InMemoryTable> Tables { get; } = new();
	internal ConcurrentDictionary<string, InMemoryRoutine> Routines { get; } = new();

	public InMemoryDataset(string datasetId)
	{
		DatasetId = datasetId ?? throw new ArgumentNullException(nameof(datasetId));
		CreationTime = DateTimeOffset.UtcNow;
		LastModifiedTime = CreationTime;
		Etag = Guid.NewGuid().ToString();
	}
}
