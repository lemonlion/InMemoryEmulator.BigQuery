namespace BigQuery.InMemoryEmulator;

/// <summary>
/// Represents an in-memory BigQuery routine (function or procedure).
/// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/routines
/// </summary>
public class InMemoryRoutine
{
	public string DatasetId { get; }
	public string RoutineId { get; }
	public string RoutineType { get; } // "SCALAR_FUNCTION" or "PROCEDURE"
	public string Language { get; } // "SQL" or "JAVASCRIPT"
	public string Body { get; }
	public IReadOnlyList<(string Name, string Type)> Parameters { get; }
	public string? ReturnType { get; }
	public DateTimeOffset CreationTime { get; }

	public InMemoryRoutine(string datasetId, string routineId, string routineType,
		string language, string body, IReadOnlyList<(string Name, string Type)> parameters,
		string? returnType = null)
	{
		DatasetId = datasetId;
		RoutineId = routineId;
		RoutineType = routineType;
		Language = language;
		Body = body;
		Parameters = parameters;
		ReturnType = returnType;
		CreationTime = DateTimeOffset.UtcNow;
	}
}
