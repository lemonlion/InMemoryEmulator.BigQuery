using System.Collections.Concurrent;

namespace BigQuery.InMemoryEmulator;

/// <summary>
/// In-memory storage for BigQuery resources. Manages the project â†’ dataset â†’ table hierarchy.
/// Thread-safe via ConcurrentDictionary.
/// </summary>
public class InMemoryDataStore
{
	/// <summary>The GCP project ID this store emulates.</summary>
	public string ProjectId { get; }

	/// <summary>
	/// Optional JavaScript UDF engine. Set this to enable LANGUAGE js support.
	/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions#javascript-udf-structure
	/// </summary>
	public IJsUdfEngine? JsUdfEngine { get; set; }

	internal ConcurrentDictionary<string, InMemoryDataset> Datasets { get; } = new();

		public InMemoryDataStore(string projectId)
	{
		ProjectId = projectId ?? throw new ArgumentNullException(nameof(projectId));
	}

	/// <summary>
	/// Exports the entire store state as a JSON string.
	/// </summary>
	/// <remarks>
	/// Ref: State persistence pattern for test fixtures and seeding.
	/// </remarks>
	public string ExportState()
	{
		var state = new Dictionary<string, object>();
		foreach (var (dsId, ds) in Datasets)
		{
			var tables = new Dictionary<string, object>();
			foreach (var (tblId, tbl) in ds.Tables)
			{
				lock (tbl.RowLock)
				{
					tables[tblId] = new
					{
						schema = tbl.Schema.Fields.Select(f => new { f.Name, f.Type, f.Mode }).ToList(),
						rows = tbl.Rows.Select(r => r.Fields).ToList()
					};
				}
			}
			state[dsId] = tables;
		}
		return Newtonsoft.Json.JsonConvert.SerializeObject(state, Newtonsoft.Json.Formatting.Indented);
	}

	/// <summary>
	/// Exports the store state to a file.
	/// </summary>
	public void ExportStateToFile(string path)
	{
		File.WriteAllText(path, ExportState());
	}

	/// <summary>
	/// Imports state from a JSON string, replacing all existing data.
	/// </summary>
	public void ImportState(string json)
	{
		var state = Newtonsoft.Json.JsonConvert.DeserializeObject<
			Dictionary<string, Dictionary<string, DatasetTableState>>>(json);
		if (state is null) return;

		Datasets.Clear();
		foreach (var (dsId, tables) in state)
		{
			var ds = new InMemoryDataset(dsId);
			foreach (var (tblId, tblState) in tables)
			{
				var schema = new Google.Apis.Bigquery.v2.Data.TableSchema
				{
					Fields = tblState.Schema.Select(f =>
						new Google.Apis.Bigquery.v2.Data.TableFieldSchema
						{
							Name = f.Name, Type = f.Type, Mode = f.Mode
						}).ToList()
				};
				var tbl = new InMemoryTable(dsId, tblId, schema);
				if (tblState.Rows is not null)
				{
					foreach (var row in tblState.Rows)
					{
						tbl.Rows.Add(new InMemoryRow(
							new Dictionary<string, object?>(row, StringComparer.OrdinalIgnoreCase)));
					}
				}
				ds.Tables[tblId] = tbl;
			}
			Datasets[dsId] = ds;
		}
	}

	/// <summary>
	/// Imports state from a file.
	/// </summary>
	public void ImportStateFromFile(string path)
	{
		ImportState(File.ReadAllText(path));
	}

	internal class DatasetTableState
	{
		public List<FieldState> Schema { get; set; } = [];
		public List<Dictionary<string, object?>>? Rows { get; set; }
	}

	internal class FieldState
	{
		public string Name { get; set; } = "";
		public string Type { get; set; } = "STRING";
		public string? Mode { get; set; }
	}
}
