using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for table CRUD operations through the real BigQueryClient SDK pipeline.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class TableCrudTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public TableCrudTests(BigQuerySession session)
	{
		_session = session;
	}

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_tbl_{Guid.NewGuid():N}"[..30];
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

	private static TableSchema SimpleSchema() => new()
	{
		Fields = new List<TableFieldSchema>
		{
			new() { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
			new() { Name = "name", Type = "STRING", Mode = "NULLABLE" },
		}
	};

	[Fact]
	public async Task Client_CreateTable_WithSchema_Succeeds()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/insert
		//   "Creates a new, empty table in the dataset."

		// Arrange
		var client = await _fixture.GetClientAsync();

		// Act
		var table = await client.CreateTableAsync(_datasetId, "test_table", SimpleSchema());

		// Assert
		Assert.NotNull(table);
		Assert.Equal("test_table", table.Reference.TableId);
		Assert.Equal(2, table.Schema.Fields.Count);
	}

	[Fact]
	public async Task Client_GetTable_ReturnsSchema()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/get
		//   "Gets the specified table resource by table ID."

		// Arrange
		var client = await _fixture.GetClientAsync();
		await client.CreateTableAsync(_datasetId, "my_table", SimpleSchema());

		// Act
		var table = await client.GetTableAsync(_datasetId, "my_table");

		// Assert
		Assert.NotNull(table);
		Assert.Equal("my_table", table.Reference.TableId);
		Assert.Equal(2, table.Schema.Fields.Count);
		Assert.Equal("id", table.Schema.Fields[0].Name);
		Assert.Equal("name", table.Schema.Fields[1].Name);
	}

	[Fact]
	public async Task Client_ListTables_ReturnsAll()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/list

		// Arrange
		var client = await _fixture.GetClientAsync();
		await client.CreateTableAsync(_datasetId, "t1", SimpleSchema());
		await client.CreateTableAsync(_datasetId, "t2", SimpleSchema());

		// Act
		var tables = client.ListTables(_datasetId).ToList();

		// Assert
		Assert.Equal(2, tables.Count);
	}

	[Fact]
	public async Task Client_DeleteTable_Succeeds()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/delete
		//   "Deletes the table specified by tableId from the dataset."

		// Arrange
		var client = await _fixture.GetClientAsync();
		await client.CreateTableAsync(_datasetId, "to_delete", SimpleSchema());

		// Act
		await client.DeleteTableAsync(_datasetId, "to_delete");

		// Assert
		var ex = await Assert.ThrowsAsync<Google.GoogleApiException>(
			() => client.GetTableAsync(_datasetId, "to_delete"));
		Assert.Equal(System.Net.HttpStatusCode.NotFound, (System.Net.HttpStatusCode)ex.HttpStatusCode);
	}

	[Fact]
	public async Task Client_PatchTable_AddsColumn()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/patch

		// Arrange
		var client = await _fixture.GetClientAsync();
		await client.CreateTableAsync(_datasetId, "to_patch", SimpleSchema());

		var newSchema = new TableSchema
		{
			Fields = new List<TableFieldSchema>
			{
				new() { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new() { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new() { Name = "email", Type = "STRING", Mode = "NULLABLE" },
			}
		};

		// Act
		var table = await client.UpdateTableAsync(_datasetId, "to_patch", new Table { Schema = newSchema });

		// Assert
		Assert.Equal(3, table.Schema.Fields.Count);
	}
}
