using System.Collections.Concurrent;
using Google.Apis.Bigquery.v2.Data;

namespace BigQuery.InMemoryEmulator;

/// <summary>
/// Represents an in-memory BigQuery table with schema-enforced typed row storage.
/// </summary>
public class InMemoryTable
{
	public string DatasetId { get; }
	public string TableId { get; }
	public TableSchema Schema { get; internal set; }
	public DateTimeOffset CreationTime { get; }
	public DateTimeOffset LastModifiedTime { get; internal set; }
	public string Etag { get; internal set; }
	public string? Description { get; set; }
	public string? FriendlyName { get; set; }
	public IDictionary<string, string>? Labels { get; set; }

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TimePartitioning
	public TimePartitioning? TimePartitioning { get; set; }
	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#RangePartitioning
	public RangePartitioning? RangePartitioning { get; set; }
	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Clustering
	public Clustering? Clustering { get; set; }
	/// <summary>When true, queries must include a partition filter.</summary>
	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Table.FIELDS.require_partition_filter
	public bool RequirePartitionFilter { get; set; }

	/// <summary>
	/// If this table represents a VIEW, stores the view's SQL query AST.
	/// When non-null, querying this table re-executes the view query instead of reading stored rows.
	/// </summary>
	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#ViewDefinition
	internal SqlEngine.SelectStatement? ViewQuery { get; set; }

	internal List<InMemoryRow> Rows { get; } = [];
	internal readonly object RowLock = new();

	public long RowCount
	{
		get { lock (RowLock) return Rows.Count; }
	}

	public InMemoryTable(string datasetId, string tableId, TableSchema schema)
	{
		DatasetId = datasetId ?? throw new ArgumentNullException(nameof(datasetId));
		TableId = tableId ?? throw new ArgumentNullException(nameof(tableId));
		Schema = schema ?? throw new ArgumentNullException(nameof(schema));
		CreationTime = DateTimeOffset.UtcNow;
		LastModifiedTime = CreationTime;
		Etag = Guid.NewGuid().ToString();
	}

	/// <summary>
	/// Updates the table schema. Used by PATCH and PUT operations.
	/// </summary>
	/// <remarks>
	/// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/patch
	///   "The schema can be updated by appending new fields."
	/// </remarks>
	internal void UpdateSchema(TableSchema newSchema)
	{
		Schema = newSchema;
	}
}
