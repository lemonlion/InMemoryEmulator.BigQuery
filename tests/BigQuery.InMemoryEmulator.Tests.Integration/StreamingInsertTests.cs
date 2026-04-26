using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for streaming inserts and table data listing.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class StreamingInsertTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public StreamingInsertTests(BigQuerySession session)
	{
		_session = session;
	}

	private static TableSchema SimpleSchema() => new()
	{
		Fields = new List<TableFieldSchema>
		{
			new() { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
			new() { Name = "name", Type = "STRING", Mode = "NULLABLE" },
		}
	};

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_ins_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public async ValueTask DisposeAsync()
	{
		try
		{
			var client = await _fixture.GetClientAsync();
			await client.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true });
		}
		catch { }
		await _fixture.DisposeAsync();
	}

	[Fact]
	public async Task Client_InsertRows_ThenListData_ReturnsRows()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll
		//   "Streams data into BigQuery one record at a time."

		// Arrange
		var client = await _fixture.GetClientAsync();
		await client.CreateTableAsync(_datasetId, "insert_test", SimpleSchema());

		var rows = new[]
		{
			new BigQueryInsertRow("row1") { ["id"] = 1, ["name"] = "Alice" },
			new BigQueryInsertRow("row2") { ["id"] = 2, ["name"] = "Bob" },
		};

		// Act
		await client.InsertRowsAsync(_datasetId, "insert_test", rows);

		// Assert — list the data back
		var result = client.ListRows(_datasetId, "insert_test", SimpleSchema());
		var allRows = result.ToList();
		Assert.Equal(2, allRows.Count);
	}

	[Fact]
	public async Task Client_InsertRows_WithInsertId_Deduplicates()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll
		//   "If you provide insertId the BigQuery best effort de-duplicates."

		// Arrange
		var client = await _fixture.GetClientAsync();
		await client.CreateTableAsync(_datasetId, "dedup_test", SimpleSchema());

		var rows = new[]
		{
			new BigQueryInsertRow("same_id") { ["id"] = 1, ["name"] = "Alice" },
		};

		// Act — insert same row twice
		await client.InsertRowsAsync(_datasetId, "dedup_test", rows);
		await client.InsertRowsAsync(_datasetId, "dedup_test", rows);

		// Assert
		var result = client.ListRows(_datasetId, "dedup_test", SimpleSchema());
		var allRows = result.ToList();
		Assert.Single(allRows);
	}

	[Fact]
	public async Task Client_ListData_Paginated_ReturnsAllPages()
	{
		// Arrange
		var client = await _fixture.GetClientAsync();
		await client.CreateTableAsync(_datasetId, "page_test", SimpleSchema());

		var rows = Enumerable.Range(1, 10)
			.Select(i => new BigQueryInsertRow($"row{i}") { ["id"] = i, ["name"] = $"User{i}" })
			.ToArray();
		await client.InsertRowsAsync(_datasetId, "page_test", rows);

		// Act — list with automatic pagination (SDK handles pageToken)
		var result = client.ListRows(_datasetId, "page_test", SimpleSchema());
		var allRows = result.ToList();

		// Assert
		Assert.Equal(10, allRows.Count);
	}

	[Fact]
	public async Task Client_InsertRows_NestedStruct_RoundTrips()
	{
		// Arrange
		var client = await _fixture.GetClientAsync();

		var schema = new TableSchema
		{
			Fields = new List<TableFieldSchema>
			{
				new() { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new()
				{
					Name = "address",
					Type = "RECORD",
					Mode = "NULLABLE",
					Fields = new List<TableFieldSchema>
					{
						new() { Name = "city", Type = "STRING" },
						new() { Name = "zip", Type = "STRING" },
					}
				},
			}
		};

		await client.CreateTableAsync(_datasetId, "nested_test", schema);

		var rows = new[]
		{
			new BigQueryInsertRow("row1")
			{
				["id"] = 1,
				["address"] = new BigQueryInsertRow
				{
					["city"] = "London",
					["zip"] = "SW1A 1AA",
				}
			},
		};

		// Act
		await client.InsertRowsAsync(_datasetId, "nested_test", rows);

		// Assert
		var result = client.ListRows(_datasetId, "nested_test", schema);
		var allRows = result.ToList();
		Assert.Single(allRows);
	}
}
