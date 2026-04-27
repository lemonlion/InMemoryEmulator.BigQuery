using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using BigQuery.InMemoryEmulator.SqlEngine;
using Google.Apis.Bigquery.v2.Data;
using Newtonsoft.Json;

namespace BigQuery.InMemoryEmulator;

/// <summary>
/// HttpMessageHandler that intercepts BigQuery SDK HTTP calls and routes them
/// to the in-memory data store. This is the core interception point.
/// </summary>
/// <remarks>
/// Ref: https://cloud.google.com/bigquery/docs/reference/rest
///   All BigQuery REST API v2 requests are relative to https://bigquery.googleapis.com/bigquery/v2/
/// </remarks>
public class FakeBigQueryHandler : HttpMessageHandler
{
	private readonly InMemoryDataStore _store;

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest
	//   Regex patterns for routing (compiled, static). Checked most-specific-first.
	private static readonly Regex TableDataRoute = new(
		@"/bigquery/v2/projects/(?<project>[^/]+)/datasets/(?<dataset>[^/]+)/tables/(?<table>[^/]+)/(?<action>insertAll|data)",
		RegexOptions.Compiled);

	private static readonly Regex TableRoute = new(
		@"/bigquery/v2/projects/(?<project>[^/]+)/datasets/(?<dataset>[^/]+)/tables(?:/(?<table>[^/?]+))?",
		RegexOptions.Compiled);

	private static readonly Regex QueryRoute = new(
		@"/bigquery/v2/projects/(?<project>[^/]+)/queries(?:/(?<job>[^/?]+))?",
		RegexOptions.Compiled);

	private static readonly Regex JobRoute = new(
		@"/bigquery/v2/projects/(?<project>[^/]+)/jobs(?:/(?<job>[^/?]+)(?:/(?<action>[^/?]+))?)?",
		RegexOptions.Compiled);

	private static readonly Regex DatasetRoute = new(
		@"/bigquery/v2/projects/(?<project>[^/]+)/datasets(?:/(?<dataset>[^/?]+))?",
		RegexOptions.Compiled);

	private static readonly Regex RoutineRoute = new(
		@"/bigquery/v2/projects/(?<project>[^/]+)/datasets/(?<dataset>[^/]+)/routines(?:/(?<routine>[^/?]+))?",
		RegexOptions.Compiled);

	/// <summary>
	/// Optional fault injector. If it returns a non-null response, that response is
	/// returned immediately without processing the request.
	/// </summary>
	public Func<HttpRequestMessage, HttpResponseMessage?>? FaultInjector { get; set; }

	/// <summary>Records all requests for test assertions.</summary>
	public ConcurrentBag<string> RequestLog { get; } = [];

	/// <summary>Records all SQL queries for test assertions.</summary>
	public ConcurrentBag<string> QueryLog { get; } = [];

	internal ConcurrentDictionary<string, InMemoryJob> Jobs { get; } = new();

	public FakeBigQueryHandler(InMemoryDataStore store)
	{
		_store = store ?? throw new ArgumentNullException(nameof(store));
	}

	protected override async Task<HttpResponseMessage> SendAsync(
		HttpRequestMessage request, CancellationToken cancellationToken)
	{
		// 1. Fault injection check
		if (FaultInjector?.Invoke(request) is { } faultResponse)
			return faultResponse;

		// 2. Log request
		RequestLog.Add($"{request.Method} {request.RequestUri}");

		// 3. Route â€” matched most-specific-first to avoid ambiguity
		var path = request.RequestUri?.AbsolutePath ?? string.Empty;
		var method = request.Method;

		// TableData routes (most specific â€” check before table routes)
		if (TableDataRoute.Match(path) is { Success: true } tdMatch)
			return await RouteTableData(method, tdMatch, request, cancellationToken);

		// Table routes
		if (TableRoute.Match(path) is { Success: true } tMatch)
			return await RouteTable(method, tMatch, request, cancellationToken);

		// Query results routes
		if (QueryRoute.Match(path) is { Success: true } qMatch)
			return await RouteQuery(method, qMatch, request, cancellationToken);

		// Job routes
		if (JobRoute.Match(path) is { Success: true } jMatch)
			return await RouteJob(method, jMatch, request, cancellationToken);

		// Dataset routes
		if (DatasetRoute.Match(path) is { Success: true } dMatch)
			return await RouteDataset(method, dMatch, request, cancellationToken);

		// Routine routes
		if (RoutineRoute.Match(path) is { Success: true } rMatch)
			return await RouteRoutine(method, rMatch, request, cancellationToken);

		return BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
			$"Unknown route: {method} {path}");
	}

	#region Dataset routing

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets
	//   Dataset CRUD operations.
	private async Task<HttpResponseMessage> RouteDataset(
		HttpMethod method, Match match, HttpRequestMessage request, CancellationToken ct)
	{
		var datasetId = match.Groups["dataset"].Value;
		var hasDatasetId = !string.IsNullOrEmpty(datasetId);

		if (method == HttpMethod.Post && !hasDatasetId)
			return await HandleCreateDataset(request);

		if (method == HttpMethod.Get && hasDatasetId)
			return HandleGetDataset(datasetId);

		if (method == HttpMethod.Get && !hasDatasetId)
			return HandleListDatasets(request);

		if (method == HttpMethod.Delete && hasDatasetId)
			return HandleDeleteDataset(datasetId, request);

		if (method == HttpMethod.Patch && hasDatasetId)
			return await HandlePatchDataset(datasetId, request);

		if (method == HttpMethod.Put && hasDatasetId)
			return await HandleUpdateDataset(datasetId, request);

		return BuildErrorResponse(HttpStatusCode.BadRequest, "invalid",
			$"Unsupported dataset operation: {method} with datasetId={datasetId}");
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/insert
	//   "Creates a new empty dataset."
	//   Request body: Dataset. Response body: newly created Dataset.
	private async Task<HttpResponseMessage> HandleCreateDataset(HttpRequestMessage request)
	{
		var body = await ReadBodyAsync<Dataset>(request);
		if (body?.DatasetReference?.DatasetId is not { } datasetId || string.IsNullOrEmpty(datasetId))
			return BuildErrorResponse(HttpStatusCode.BadRequest, "invalid",
				"Dataset reference with datasetId is required.");

		if (!_store.Datasets.TryAdd(datasetId, CreateDatasetFromRequest(body)))
		{
			// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/insert
			//   Returns 409 if dataset already exists.
			return BuildErrorResponse(HttpStatusCode.Conflict, "duplicate",
				$"Already Exists: Dataset {_store.ProjectId}:{datasetId}");
		}

		var stored = _store.Datasets[datasetId];
		return BuildJsonResponse(ToDatasetResource(stored), HttpStatusCode.OK);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/get
	//   "Returns the dataset specified by datasetID."
	//   Response body: Dataset.
	private HttpResponseMessage HandleGetDataset(string datasetId)
	{
		if (!_store.Datasets.TryGetValue(datasetId, out var dataset))
			return BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
				$"Not found: Dataset {_store.ProjectId}:{datasetId}");

		return BuildJsonResponse(ToDatasetResource(dataset));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list
	//   "Lists all datasets in the specified project."
	//   Response body: DatasetList with kind, etag, nextPageToken, datasets[].
	private HttpResponseMessage HandleListDatasets(HttpRequestMessage request)
	{
		var queryParams = System.Web.HttpUtility.ParseQueryString(
			request.RequestUri?.Query ?? string.Empty);

		var allDatasets = _store.Datasets.Values.ToList();

		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list
		//   maxResults: The maximum number of results to return in a single response page.
		var maxResults = int.TryParse(queryParams["maxResults"], out var mr) ? mr : allDatasets.Count;

		var result = new DatasetList
		{
			Kind = "bigquery#datasetList",
			ETag = Guid.NewGuid().ToString(),
			Datasets = allDatasets
				.Take(maxResults)
				.Select(ds => new DatasetList.DatasetsData
				{
					Kind = "bigquery#dataset",
					Id = $"{_store.ProjectId}:{ds.DatasetId}",
					DatasetReference = new DatasetReference
					{
						ProjectId = _store.ProjectId,
						DatasetId = ds.DatasetId,
					},
					FriendlyName = ds.FriendlyName,
					Labels = ds.Labels as IDictionary<string, string>,
					Location = ds.Location,
				})
				.ToList(),
		};

		return BuildJsonResponse(result);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/delete
	//   "Deletes the dataset specified by the datasetId value."
	//   "Before you can delete a dataset, you must delete all its tables,
	//    either manually or by specifying deleteContents."
	//   "If successful, the response body is an empty JSON object."
	private HttpResponseMessage HandleDeleteDataset(string datasetId, HttpRequestMessage request)
	{
		if (!_store.Datasets.TryGetValue(datasetId, out var dataset))
			return BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
				$"Not found: Dataset {_store.ProjectId}:{datasetId}");

		var queryParams = System.Web.HttpUtility.ParseQueryString(
			request.RequestUri?.Query ?? string.Empty);
		var deleteContents = string.Equals(queryParams["deleteContents"], "true",
			StringComparison.OrdinalIgnoreCase);

		if (!deleteContents && !dataset.Tables.IsEmpty)
			return BuildErrorResponse(HttpStatusCode.BadRequest, "invalid",
				$"Dataset {_store.ProjectId}:{datasetId} is still in use. " +
				"Delete all tables before deleting the dataset, or use deleteContents=true.");

		_store.Datasets.TryRemove(datasetId, out _);
		return new HttpResponseMessage(HttpStatusCode.NoContent);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/patch
	//   "The patch method only replaces fields that are provided in the submitted dataset resource."
	//   Request body: Dataset. Response body: Dataset.
	private async Task<HttpResponseMessage> HandlePatchDataset(
		string datasetId, HttpRequestMessage request)
	{
		if (!_store.Datasets.TryGetValue(datasetId, out var dataset))
			return BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
				$"Not found: Dataset {_store.ProjectId}:{datasetId}");

		var body = await ReadBodyAsync<Dataset>(request);
		if (body is null)
			return BuildErrorResponse(HttpStatusCode.BadRequest, "invalid",
				"Request body is required for PATCH.");

		// Patch semantics: only update fields that are provided (non-null)
		if (body.Description is not null)
			dataset.Description = body.Description;
		if (body.FriendlyName is not null)
			dataset.FriendlyName = body.FriendlyName;
		if (body.Location is not null)
			dataset.Location = body.Location;
		if (body.Labels is not null)
			dataset.Labels = body.Labels;
		if (body.DefaultTableExpirationMs is not null)
			dataset.DefaultTableExpirationMs = body.DefaultTableExpirationMs;

		dataset.LastModifiedTime = DateTimeOffset.UtcNow;
		dataset.Etag = Guid.NewGuid().ToString();

		return BuildJsonResponse(ToDatasetResource(dataset));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/update
	//   "The update method replaces the entire dataset resource."
	//   Request body: Dataset. Response body: Dataset.
	private async Task<HttpResponseMessage> HandleUpdateDataset(
		string datasetId, HttpRequestMessage request)
	{
		if (!_store.Datasets.TryGetValue(datasetId, out var dataset))
			return BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
				$"Not found: Dataset {_store.ProjectId}:{datasetId}");

		var body = await ReadBodyAsync<Dataset>(request);
		if (body is null)
			return BuildErrorResponse(HttpStatusCode.BadRequest, "invalid",
				"Request body is required for PUT.");

		// Full replace semantics
		dataset.Description = body.Description;
		dataset.FriendlyName = body.FriendlyName;
		dataset.Location = body.Location ?? dataset.Location;
		dataset.Labels = body.Labels;
		dataset.DefaultTableExpirationMs = body.DefaultTableExpirationMs;
		dataset.LastModifiedTime = DateTimeOffset.UtcNow;
		dataset.Etag = Guid.NewGuid().ToString();

		return BuildJsonResponse(ToDatasetResource(dataset));
	}

	#endregion

	#region Stub routes (to be implemented in later phases)

	private async Task<HttpResponseMessage> RouteTableData(
		HttpMethod method, Match match, HttpRequestMessage request, CancellationToken ct)
	{
		var datasetId = match.Groups["dataset"].Value;
		var tableId = match.Groups["table"].Value;
		var action = match.Groups["action"].Value;

		if (method == HttpMethod.Post && action == "insertAll")
			return await HandleStreamingInsert(datasetId, tableId, request);

		if (method == HttpMethod.Get && action == "data")
			return HandleListTableData(datasetId, tableId, request);

		return BuildErrorResponse(HttpStatusCode.BadRequest, "invalid",
			$"Unsupported table data operation: {method} {action}");
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll
	//   "Streams data into BigQuery one record at a time without needing to run a load job."
	//   Request body: TableDataInsertAllRequest. Response body: TableDataInsertAllResponse.
	private async Task<HttpResponseMessage> HandleStreamingInsert(
		string datasetId, string tableId, HttpRequestMessage request)
	{
		if (!_store.Datasets.TryGetValue(datasetId, out var dataset))
			return BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
				$"Not found: Dataset {_store.ProjectId}:{datasetId}");

		if (!dataset.Tables.TryGetValue(tableId, out var table))
			return BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
				$"Not found: Table {_store.ProjectId}:{datasetId}.{tableId}");

		var body = await ReadBodyAsync<TableDataInsertAllRequest>(request);
		if (body?.Rows is null || body.Rows.Count == 0)
		{
			return BuildJsonResponse(new TableDataInsertAllResponse
			{
				Kind = "bigquery#tableDataInsertAllResponse"
			});
		}

		var insertErrors = new List<TableDataInsertAllResponse.InsertErrorsData>();

		lock (table.RowLock)
		{
			for (var i = 0; i < body.Rows.Count; i++)
			{
				var rowData = body.Rows[i];
				var insertId = rowData.InsertId;

				// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll
				//   "If you provide insertId, BigQuery uses it for best-effort de-duplication."
				if (insertId is not null &&
					table.Rows.Any(r => r.InsertId == insertId))
				{
					continue; // Deduplicate
				}

				var fields = new Dictionary<string, object?>();
				if (rowData.Json is IDictionary<string, object> jsonDict)
				{
					foreach (var kvp in jsonDict)
						fields[kvp.Key] = kvp.Value;
				}

				table.Rows.Add(new InMemoryRow(fields, insertId));
			}
		}

		var response = new TableDataInsertAllResponse
		{
			Kind = "bigquery#tableDataInsertAllResponse",
			InsertErrors = insertErrors.Count > 0 ? insertErrors : null,
		};

		return BuildJsonResponse(response);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
	//   "Lists the content of a table in rows."
	//   Response body: TableDataList.
	private HttpResponseMessage HandleListTableData(
		string datasetId, string tableId, HttpRequestMessage request)
	{
		if (!_store.Datasets.TryGetValue(datasetId, out var dataset))
			return BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
				$"Not found: Dataset {_store.ProjectId}:{datasetId}");

		if (!dataset.Tables.TryGetValue(tableId, out var table))
			return BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
				$"Not found: Table {_store.ProjectId}:{datasetId}.{tableId}");

		var queryParams = System.Web.HttpUtility.ParseQueryString(
			request.RequestUri?.Query ?? string.Empty);

		var startIndex = int.TryParse(queryParams["startIndex"], out var si) ? si : 0;
		var pageToken = queryParams["pageToken"];
		if (pageToken is not null && int.TryParse(pageToken, out var pt))
			startIndex = pt;

		List<InMemoryRow> allRows;
		lock (table.RowLock)
		{
			allRows = table.Rows.ToList();
		}

		var totalRows = allRows.Count;
		var maxResults = int.TryParse(queryParams["maxResults"], out var mr) ? mr : totalRows;
		var pageRows = allRows.Skip(startIndex).Take(maxResults).ToList();

		string? nextPageToken = null;
		if (startIndex + maxResults < totalRows)
			nextPageToken = (startIndex + maxResults).ToString();

		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
		//   Response rows use f[].v format where all values are returned as strings.
		var rows = pageRows.Select(row =>
		{
			var tableRow = new TableRow
			{
				F = table.Schema.Fields?.Select(field =>
				{
					var value = row.Fields.TryGetValue(field.Name, out var v) ? v : null;
					return new TableCell { V = ConvertValueToString(value) };
				}).ToList() ?? []
			};
			return tableRow;
		}).ToList();

		var result = new TableDataList
		{
			Kind = "bigquery#tableDataList",
			ETag = table.Etag,
			TotalRows = totalRows,
			PageToken = nextPageToken,
			Rows = rows,
		};

		return BuildJsonResponse(result);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
	//   "Note: BigQuery returns all values as strings in the f[].v format."
	private static object? ConvertValueToString(object? value)
	{
		if (value is null) return null;
		if (value is IDictionary<string, object?> record)
		{
			// Nested RECORD: return as f[].v structure
			return new TableRow
			{
				F = record.Select(kvp => new TableCell { V = ConvertValueToString(kvp.Value) }).ToList()
			};
		}
		if (value is IList<object?> array)
		{
			return array.Select(item => new TableCell { V = ConvertValueToString(item) }).ToList();
		}
		return value.ToString();
	}

	private async Task<HttpResponseMessage> RouteTable(
		HttpMethod method, Match match, HttpRequestMessage request, CancellationToken ct)
	{
		var datasetId = match.Groups["dataset"].Value;
		var tableId = match.Groups["table"].Value;
		var hasTableId = !string.IsNullOrEmpty(tableId);

		if (method == HttpMethod.Post && !hasTableId)
			return await HandleCreateTable(datasetId, request);

		if (method == HttpMethod.Get && hasTableId)
			return HandleGetTable(datasetId, tableId);

		if (method == HttpMethod.Get && !hasTableId)
			return HandleListTables(datasetId, request);

		if (method == HttpMethod.Delete && hasTableId)
			return HandleDeleteTable(datasetId, tableId);

		if (method == HttpMethod.Patch && hasTableId)
			return await HandlePatchTable(datasetId, tableId, request);

		if (method == HttpMethod.Put && hasTableId)
			return await HandleUpdateTable(datasetId, tableId, request);

		return BuildErrorResponse(HttpStatusCode.BadRequest, "invalid",
			$"Unsupported table operation: {method}");
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/insert
	//   "Creates a new, empty table in the dataset."
	//   Request body: Table. Response body: newly created Table.
	private async Task<HttpResponseMessage> HandleCreateTable(
		string datasetId, HttpRequestMessage request)
	{
		if (!_store.Datasets.TryGetValue(datasetId, out var dataset))
			return BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
				$"Not found: Dataset {_store.ProjectId}:{datasetId}");

		var body = await ReadBodyAsync<Table>(request);
		if (body?.TableReference?.TableId is not { } tableId || string.IsNullOrEmpty(tableId))
			return BuildErrorResponse(HttpStatusCode.BadRequest, "invalid",
				"Table reference with tableId is required.");

		var schema = body.Schema ?? new TableSchema();
		var table = new InMemoryTable(datasetId, tableId, schema)
		{
			Description = body.Description,
			FriendlyName = body.FriendlyName,
			Labels = body.Labels,
			// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Table
			TimePartitioning = body.TimePartitioning,
			RangePartitioning = body.RangePartitioning,
			Clustering = body.Clustering,
			RequirePartitionFilter = body.RequirePartitionFilter ?? false,
		};

		if (!dataset.Tables.TryAdd(tableId, table))
			return BuildErrorResponse(HttpStatusCode.Conflict, "duplicate",
				$"Already Exists: Table {_store.ProjectId}:{datasetId}.{tableId}");

		return BuildJsonResponse(ToTableResource(dataset, table));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/get
	//   "Gets the specified table resource by table ID."
	//   Response body: Table.
	private HttpResponseMessage HandleGetTable(string datasetId, string tableId)
	{
		if (!_store.Datasets.TryGetValue(datasetId, out var dataset))
			return BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
				$"Not found: Dataset {_store.ProjectId}:{datasetId}");

		if (!dataset.Tables.TryGetValue(tableId, out var table))
			return BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
				$"Not found: Table {_store.ProjectId}:{datasetId}.{tableId}");

		return BuildJsonResponse(ToTableResource(dataset, table));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/list
	//   "Lists all tables in the specified dataset."
	//   Response body: TableList.
	private HttpResponseMessage HandleListTables(string datasetId, HttpRequestMessage request)
	{
		if (!_store.Datasets.TryGetValue(datasetId, out var dataset))
			return BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
				$"Not found: Dataset {_store.ProjectId}:{datasetId}");

		var tables = dataset.Tables.Values.ToList();

		var result = new TableList
		{
			Kind = "bigquery#tableList",
			ETag = Guid.NewGuid().ToString(),
			TotalItems = tables.Count,
			Tables = tables.Select(t => new TableList.TablesData
			{
				Kind = "bigquery#table",
				Id = $"{_store.ProjectId}:{datasetId}.{t.TableId}",
				TableReference = new TableReference
				{
					ProjectId = _store.ProjectId,
					DatasetId = datasetId,
					TableId = t.TableId,
				},
				FriendlyName = t.FriendlyName,
				Labels = t.Labels as IDictionary<string, string>,
				Type = "TABLE",
			}).ToList(),
		};

		return BuildJsonResponse(result);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/delete
	//   "Deletes the table specified by tableId from the dataset."
	//   Response body: empty.
	private HttpResponseMessage HandleDeleteTable(string datasetId, string tableId)
	{
		if (!_store.Datasets.TryGetValue(datasetId, out var dataset))
			return BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
				$"Not found: Dataset {_store.ProjectId}:{datasetId}");

		if (!dataset.Tables.TryRemove(tableId, out _))
			return BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
				$"Not found: Table {_store.ProjectId}:{datasetId}.{tableId}");

		return new HttpResponseMessage(HttpStatusCode.NoContent);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/patch
	//   "The patch method only replaces fields that are provided in the submitted table resource."
	//   Request body: Table. Response body: Table.
	private async Task<HttpResponseMessage> HandlePatchTable(
		string datasetId, string tableId, HttpRequestMessage request)
	{
		if (!_store.Datasets.TryGetValue(datasetId, out var dataset))
			return BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
				$"Not found: Dataset {_store.ProjectId}:{datasetId}");

		if (!dataset.Tables.TryGetValue(tableId, out var table))
			return BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
				$"Not found: Table {_store.ProjectId}:{datasetId}.{tableId}");

		var body = await ReadBodyAsync<Table>(request);
		if (body is null)
			return BuildErrorResponse(HttpStatusCode.BadRequest, "invalid",
				"Request body is required for PATCH.");

		// Patch semantics: only update provided fields
		if (body.Description is not null)
			table.Description = body.Description;
		if (body.FriendlyName is not null)
			table.FriendlyName = body.FriendlyName;
		if (body.Labels is not null)
			table.Labels = body.Labels;
		if (body.Schema is not null)
			table.UpdateSchema(body.Schema);

		table.LastModifiedTime = DateTimeOffset.UtcNow;
		table.Etag = Guid.NewGuid().ToString();

		return BuildJsonResponse(ToTableResource(dataset, table));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/update
	//   "The update method replaces the entire table resource."
	//   Request body: Table. Response body: Table.
	private async Task<HttpResponseMessage> HandleUpdateTable(
		string datasetId, string tableId, HttpRequestMessage request)
	{
		if (!_store.Datasets.TryGetValue(datasetId, out var dataset))
			return BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
				$"Not found: Dataset {_store.ProjectId}:{datasetId}");

		if (!dataset.Tables.TryGetValue(tableId, out var table))
			return BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
				$"Not found: Table {_store.ProjectId}:{datasetId}.{tableId}");

		var body = await ReadBodyAsync<Table>(request);
		if (body is null)
			return BuildErrorResponse(HttpStatusCode.BadRequest, "invalid",
				"Request body is required for PUT.");

		table.Description = body.Description;
		table.FriendlyName = body.FriendlyName;
		table.Labels = body.Labels;
		if (body.Schema is not null)
			table.UpdateSchema(body.Schema);

		table.LastModifiedTime = DateTimeOffset.UtcNow;
		table.Etag = Guid.NewGuid().ToString();

		return BuildJsonResponse(ToTableResource(dataset, table));
	}

	private Table ToTableResource(InMemoryDataset dataset, InMemoryTable table)
	{
		return new Table
		{
			Kind = "bigquery#table",
			Id = $"{_store.ProjectId}:{dataset.DatasetId}.{table.TableId}",
			TableReference = new TableReference
			{
				ProjectId = _store.ProjectId,
				DatasetId = dataset.DatasetId,
				TableId = table.TableId,
			},
			Schema = table.Schema,
			Description = table.Description,
			FriendlyName = table.FriendlyName,
			Labels = table.Labels as IDictionary<string, string>,
			NumRows = (ulong?)table.RowCount,
			NumBytes = 0,
			CreationTime = table.CreationTime.ToUnixTimeMilliseconds(),
			LastModifiedTime = (ulong?)table.LastModifiedTime.ToUnixTimeMilliseconds(),
			ETag = table.Etag,
			Type = "TABLE",
			TimePartitioning = table.TimePartitioning,
			RangePartitioning = table.RangePartitioning,
			Clustering = table.Clustering,
			RequirePartitionFilter = table.RequirePartitionFilter ? true : null,
		};
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
	//   "Runs a BigQuery SQL query synchronously and returns query results if the query completes within a specified timeout."
	private async Task<HttpResponseMessage> RouteQuery(
		HttpMethod method, Match match, HttpRequestMessage request, CancellationToken ct)
	{
		var jobId = match.Groups["job"].Value;

		if (method == HttpMethod.Post && string.IsNullOrEmpty(jobId))
			return await HandleSyncQuery(request);

		if (method == HttpMethod.Get && !string.IsNullOrEmpty(jobId))
			return HandleGetQueryResults(jobId, request);

		return BuildErrorResponse(HttpStatusCode.BadRequest, "invalid",
			$"Unsupported query operation: {method}");
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs
	//   Job CRUD and execution.
	private async Task<HttpResponseMessage> RouteJob(
		HttpMethod method, Match match, HttpRequestMessage request, CancellationToken ct)
	{
		var jobId = match.Groups["job"].Value;
		var hasJobId = !string.IsNullOrEmpty(jobId);

		var action = match.Groups["action"].Value;

		if (method == HttpMethod.Post && hasJobId && action == "cancel")
			return HandleCancelJob(jobId);

		if (method == HttpMethod.Post && !hasJobId)
			return await HandleInsertJob(request);

		if (method == HttpMethod.Get && hasJobId)
			return HandleGetJob(jobId);

		if (method == HttpMethod.Get && !hasJobId)
			return HandleListJobs(request);

		return BuildErrorResponse(HttpStatusCode.BadRequest, "invalid",
			$"Unsupported job operation: {method}");
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
	private async Task<HttpResponseMessage> HandleSyncQuery(HttpRequestMessage request)
	{
		var body = await ReadBodyAsync<QueryRequest>(request);
		if (body?.Query is null)
			return BuildErrorResponse(HttpStatusCode.BadRequest, "invalid", "Missing query.");

		QueryLog.Add(body.Query);

		var defaultDatasetId = body.DefaultDataset?.DatasetId;
		var job = new InMemoryJob(_store.ProjectId)
		{
			Query = body.Query,
			DefaultDatasetId = defaultDatasetId,
			Parameters = body.QueryParameters,
			StatementType = "SELECT",
			Labels = body.Labels,
		};

		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
		//   "dryRun: If set to true, BigQuery doesn't run the job."
		if (body.DryRun == true)
		{
			job.IsDryRun = true;
			Jobs[job.JobId] = job;
			var dryResponse = new QueryResponse
			{
				Kind = "bigquery#queryResponse",
				JobReference = new JobReference { ProjectId = _store.ProjectId, JobId = job.JobId },
				JobComplete = true,
				TotalBytesProcessed = 0,
			};
			return BuildJsonResponse(dryResponse);
		}

		try
		{
			// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language
			//   Multi-statement queries (scripts) contain semicolons
			TableSchema schema;
			List<TableRow> rows;
			if (body.Query.Contains(';'))
			{
				var procExecutor = new SqlEngine.ProceduralExecutor(_store, defaultDatasetId);
				(schema, rows) = procExecutor.Execute(body.Query);
			}
			else
			{
				var executor = new QueryExecutor(_store, defaultDatasetId);
				executor.SetParameters(body.QueryParameters);
				(schema, rows) = executor.Execute(body.Query);
			}

			job.ResultSchema = schema;
			job.ResultRows = rows;
			job.TotalRows = rows.Count;
			Jobs[job.JobId] = job;

			// Apply maxResults pagination
			var maxResults = (int?)body.MaxResults;
			var pageRows = maxResults.HasValue ? rows.Take(maxResults.Value).ToList() : rows;
			string? pageToken = null;
			if (maxResults.HasValue && maxResults.Value < rows.Count)
				pageToken = maxResults.Value.ToString();

			// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#QueryResponse
			var response = new QueryResponse
			{
				Kind = "bigquery#queryResponse",
				Schema = schema,
				JobReference = new JobReference { ProjectId = _store.ProjectId, JobId = job.JobId },
				TotalRows = (ulong)rows.Count,
				Rows = pageRows,
				JobComplete = true,
				TotalBytesProcessed = 0,
				PageToken = pageToken,
			};

			return BuildJsonResponse(response);
		}
		catch (Exception ex)
		{
			return BuildErrorResponse(HttpStatusCode.BadRequest, "invalidQuery", ex.Message);
		}
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert
	private async Task<HttpResponseMessage> HandleInsertJob(HttpRequestMessage request)
	{
		var body = await ReadBodyAsync<Job>(request);
		var queryConfig = body?.Configuration?.Query;
		if (queryConfig?.Query is null)
			return BuildErrorResponse(HttpStatusCode.BadRequest, "invalid", "Missing query configuration.");

		QueryLog.Add(queryConfig.Query);

		var jobId = body?.JobReference?.JobId ?? Guid.NewGuid().ToString();
		var defaultDatasetId = queryConfig.DefaultDataset?.DatasetId;
		var job = new InMemoryJob(_store.ProjectId, jobId)
		{
			Query = queryConfig.Query,
			DefaultDatasetId = defaultDatasetId,
			Parameters = queryConfig.QueryParameters,
			StatementType = "SELECT",
			Labels = body?.Configuration?.Labels,
		};

		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#JobConfiguration
		//   "dryRun: If set, don't actually run this job."
		if (body?.Configuration?.DryRun == true)
		{
			job.IsDryRun = true;
			// Validate the query even for dry runs — real BigQuery rejects invalid SQL
			try
			{
				var executor = new QueryExecutor(_store, defaultDatasetId);
				executor.SetParameters(queryConfig.QueryParameters);
				var (schema, _) = executor.Execute(queryConfig.Query);
				job.ResultSchema = schema;
			}
			catch (Exception ex)
			{
				return BuildErrorResponse(HttpStatusCode.BadRequest, "invalidQuery", ex.Message);
			}
			Jobs[job.JobId] = job;
			return BuildJsonResponse(job.ToJobResource());
		}

		try
		{
			// Support multi-statement scripts
			TableSchema schema;
			List<TableRow> rows;
			if (queryConfig.Query.Contains(';'))
			{
				var procExecutor = new SqlEngine.ProceduralExecutor(_store, defaultDatasetId);
				(schema, rows) = procExecutor.Execute(queryConfig.Query);
			}
			else
			{
				var executor = new QueryExecutor(_store, defaultDatasetId);
				executor.SetParameters(queryConfig.QueryParameters);
				(schema, rows) = executor.Execute(queryConfig.Query);
			}

			job.ResultSchema = schema;
			job.ResultRows = rows;
			job.TotalRows = rows.Count;
		}
		catch (Exception ex)
		{
			job.State = "DONE";
			// Store error but still return job
			Jobs[job.JobId] = job;
			var errorJob = job.ToJobResource();
			errorJob.Status!.ErrorResult = new ErrorProto { Message = ex.Message, Reason = "invalidQuery" };
			return BuildJsonResponse(errorJob);
		}

		Jobs[job.JobId] = job;
		return BuildJsonResponse(job.ToJobResource());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/get
	private HttpResponseMessage HandleGetJob(string jobId)
	{
		if (!Jobs.TryGetValue(jobId, out var job))
			return BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
				$"Not found: Job {_store.ProjectId}:{jobId}");

		return BuildJsonResponse(job.ToJobResource());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/cancel
	//   "Requests that a job be cancelled."
	private HttpResponseMessage HandleCancelJob(string jobId)
	{
		if (!Jobs.TryGetValue(jobId, out var job))
			return BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
				$"Not found: Job {_store.ProjectId}:{jobId}");

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

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/list
	private HttpResponseMessage HandleListJobs(HttpRequestMessage request)
	{
		var jobList = new JobList
		{
			Kind = "bigquery#jobList",
			Jobs = Jobs.Values.Select(j => new JobList.JobsData
			{
				Id = $"{j.ProjectId}:{j.JobId}",
				JobReference = new JobReference { ProjectId = j.ProjectId, JobId = j.JobId },
				Status = new JobStatus { State = j.State },
			}).ToList(),
		};
		return BuildJsonResponse(jobList);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults
	private HttpResponseMessage HandleGetQueryResults(string jobId, HttpRequestMessage request)
	{
		if (!Jobs.TryGetValue(jobId, out var job))
			return BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
				$"Not found: Job {_store.ProjectId}:{jobId}");

		var queryParams = System.Web.HttpUtility.ParseQueryString(
			request.RequestUri?.Query ?? string.Empty);

		var startIndex = 0;
		var pageToken = queryParams["pageToken"];
		if (pageToken is not null && int.TryParse(pageToken, out var pt))
			startIndex = pt;

		var rows = job.ResultRows ?? [];
		var maxResults = int.TryParse(queryParams["maxResults"], out var mr) ? mr : rows.Count;
		var pageRows = rows.Skip(startIndex).Take(maxResults).ToList();

		string? nextPageToken = null;
		if (startIndex + maxResults < rows.Count)
			nextPageToken = (startIndex + maxResults).ToString();

		var response = new GetQueryResultsResponse
		{
			Kind = "bigquery#getQueryResultsResponse",
			Schema = job.ResultSchema,
			JobReference = new JobReference { ProjectId = _store.ProjectId, JobId = job.JobId },
			TotalRows = (ulong?)job.TotalRows,
			Rows = pageRows,
			JobComplete = true,
			TotalBytesProcessed = 0,
			PageToken = nextPageToken,
		};

		return BuildJsonResponse(response);
	}

	private Task<HttpResponseMessage> RouteRoutine(
		HttpMethod method, Match match, HttpRequestMessage request, CancellationToken ct)
	{
		return Task.FromResult(BuildErrorResponse(HttpStatusCode.NotFound, "notFound",
			"Routine operations not yet implemented."));
	}

	#endregion

	#region Helpers

	private InMemoryDataset CreateDatasetFromRequest(Dataset body)
	{
		var datasetId = body.DatasetReference!.DatasetId!;
		return new InMemoryDataset(datasetId)
		{
			Description = body.Description,
			FriendlyName = body.FriendlyName,
			Location = body.Location ?? "US",
			Labels = body.Labels,
			DefaultTableExpirationMs = body.DefaultTableExpirationMs,
		};
	}

	private Dataset ToDatasetResource(InMemoryDataset ds)
	{
		return new Dataset
		{
			Kind = "bigquery#dataset",
			Id = $"{_store.ProjectId}:{ds.DatasetId}",
			DatasetReference = new DatasetReference
			{
				ProjectId = _store.ProjectId,
				DatasetId = ds.DatasetId,
			},
			Description = ds.Description,
			FriendlyName = ds.FriendlyName,
			Location = ds.Location,
			Labels = ds.Labels as IDictionary<string, string>,
			DefaultTableExpirationMs = ds.DefaultTableExpirationMs,
			CreationTime = ds.CreationTime.ToUnixTimeMilliseconds(),
			LastModifiedTime = ds.LastModifiedTime.ToUnixTimeMilliseconds(),
			ETag = ds.Etag,
		};
	}

	private static async Task<T?> ReadBodyAsync<T>(HttpRequestMessage request) where T : class
	{
		if (request.Content is null) return null;
		var stream = await request.Content.ReadAsStreamAsync();

		// The Google APIs SDK compresses request bodies with GZip by default.
		// Detect GZip (magic bytes 0x1F 0x8B) and decompress transparently.
		Stream contentStream = stream;
		if (request.Content.Headers.ContentEncoding.Contains("gzip") || await IsGzipStreamAsync(stream))
		{
			contentStream = new System.IO.Compression.GZipStream(stream,
				System.IO.Compression.CompressionMode.Decompress);
		}

		using var reader = new StreamReader(contentStream);
		var json = await reader.ReadToEndAsync();
		if (string.IsNullOrWhiteSpace(json)) return null;
		return JsonConvert.DeserializeObject<T>(json);
	}

	private static async Task<bool> IsGzipStreamAsync(Stream stream)
	{
		if (!stream.CanSeek) return false;
		var pos = stream.Position;
		var buffer = new byte[2];
		var read = await stream.ReadAsync(buffer);
		stream.Position = pos;
		return read >= 2 && buffer[0] == 0x1F && buffer[1] == 0x8B;
	}

	internal HttpResponseMessage BuildJsonResponse<T>(T body, HttpStatusCode status = HttpStatusCode.OK)
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest
		//   Use SDK-compatible serialization to ensure response format matches what the SDK expects.
		var json = JsonConvert.SerializeObject(body);
		return new HttpResponseMessage(status)
		{
			Content = new StringContent(json, Encoding.UTF8, "application/json")
		};
	}

	internal static HttpResponseMessage BuildErrorResponse(
		HttpStatusCode status, string reason, string message)
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest
		//   All errors follow the Google API error format.
		var errorBody = new
		{
			error = new
			{
				code = (int)status,
				message,
				errors = new[]
				{
					new
					{
						message,
						domain = "global",
						reason
					}
				},
				status = status switch
				{
					HttpStatusCode.NotFound => "NOT_FOUND",
					HttpStatusCode.Conflict => "ALREADY_EXISTS",
					HttpStatusCode.BadRequest => "INVALID_ARGUMENT",
					HttpStatusCode.Forbidden => "PERMISSION_DENIED",
					(HttpStatusCode)429 => "RESOURCE_EXHAUSTED",
					HttpStatusCode.InternalServerError => "INTERNAL",
					HttpStatusCode.ServiceUnavailable => "UNAVAILABLE",
					_ => "UNKNOWN"
				}
			}
		};

		var json = JsonConvert.SerializeObject(errorBody);
		return new HttpResponseMessage(status)
		{
			Content = new StringContent(json, Encoding.UTF8, "application/json")
		};
	}

	#endregion
}
