using System.Net;

namespace BigQuery.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Catalog of expected error responses, validated against real BigQuery in the weekly cloud parity run.
/// </summary>
public static class ErrorCatalog
{
	public record ErrorSpec(HttpStatusCode Status, string Reason, string MessagePattern);

	public static ErrorSpec DatasetNotFound(string datasetId) => new(
		HttpStatusCode.NotFound,
		Reason: "notFound",
		MessagePattern: $"*{datasetId}*");

	public static ErrorSpec DatasetAlreadyExists(string datasetId) => new(
		HttpStatusCode.Conflict,
		Reason: "duplicate",
		MessagePattern: $"*{datasetId}*already exists*");

	public static ErrorSpec TableNotFound(string tableId) => new(
		HttpStatusCode.NotFound,
		Reason: "notFound",
		MessagePattern: $"*{tableId}*");

	public static ErrorSpec TableAlreadyExists(string tableId) => new(
		HttpStatusCode.Conflict,
		Reason: "duplicate",
		MessagePattern: $"*{tableId}*already exists*");

	public static ErrorSpec InvalidQuery(string? messagePart = null) => new(
		HttpStatusCode.BadRequest,
		Reason: "invalidQuery",
		MessagePattern: messagePart ?? "*");

	public static ErrorSpec InvalidRequest(string? messagePart = null) => new(
		HttpStatusCode.BadRequest,
		Reason: "invalid",
		MessagePattern: messagePart ?? "*");
}
