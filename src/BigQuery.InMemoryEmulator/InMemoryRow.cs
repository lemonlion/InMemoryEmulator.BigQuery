namespace BigQuery.InMemoryEmulator;

/// <summary>
/// Internal row representation for BigQuery tables.
/// Each key is a column name, each value is the typed CLR value.
/// </summary>
internal class InMemoryRow
{
	/// <summary>
	/// Column name → typed value mapping.
	/// Values follow the BigQuery type system (see BigQueryTypeSystem).
	/// </summary>
	public Dictionary<string, object?> Fields { get; }

	/// <summary>Optional dedup key for streaming inserts.</summary>
	public string? InsertId { get; }

	/// <summary>When this row was inserted (for streaming buffer tracking).</summary>
	public DateTimeOffset InsertedAt { get; }

	public InMemoryRow(Dictionary<string, object?> fields, string? insertId = null)
	{
		Fields = fields ?? throw new ArgumentNullException(nameof(fields));
		InsertId = insertId;
		InsertedAt = DateTimeOffset.UtcNow;
	}
}
