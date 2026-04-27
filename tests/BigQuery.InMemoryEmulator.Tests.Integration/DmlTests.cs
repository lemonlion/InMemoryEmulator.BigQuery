using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for DML operations (Phase 8): INSERT, UPDATE, DELETE, MERGE.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class DmlTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public DmlTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_dml_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "value", Type = "INTEGER", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "data", schema);

		await client.InsertRowsAsync(_datasetId, "data", new[]
		{
			new BigQueryInsertRow("r1") { ["id"] = 1, ["name"] = "Alice", ["value"] = 10 },
			new BigQueryInsertRow("r2") { ["id"] = 2, ["name"] = "Bob", ["value"] = 20 },
			new BigQueryInsertRow("r3") { ["id"] = 3, ["name"] = "Charlie", ["value"] = 30 },
		});

		// Empty target table for INSERT INTO ... SELECT
		var targetSchema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "value", Type = "INTEGER", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "target", targetSchema);

		// Merge source table
		await client.CreateTableAsync(_datasetId, "source", schema);
		await client.InsertRowsAsync(_datasetId, "source", new[]
		{
			new BigQueryInsertRow("s1") { ["id"] = 2, ["name"] = "Bob Updated", ["value"] = 25 },
			new BigQueryInsertRow("s2") { ["id"] = 4, ["name"] = "Dave", ["value"] = 40 },
		});
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

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#insert_statement
	//   "Use the INSERT statement to add rows to a table."
	[Fact]
	public async Task InsertValues_AddsRows()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"INSERT INTO `{_datasetId}.target` (id, name, value) VALUES (10, 'Test', 100)",
			parameters: null);

		var results = await client.ExecuteQueryAsync(
			$"SELECT * FROM `{_datasetId}.target` WHERE id = 10",
			parameters: null);
		var rows = results.ToList();
		Assert.Single(rows);
		Assert.Equal("Test", (string)rows[0]["name"]);
	}

	[Fact]
	public async Task InsertSelect_CopiesRows()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"INSERT INTO `{_datasetId}.target` SELECT * FROM `{_datasetId}.data`",
			parameters: null);

		var results = await client.ExecuteQueryAsync(
			$"SELECT COUNT(*) AS cnt FROM `{_datasetId}.target`",
			parameters: null);
		var rows = results.ToList();
		var count = Convert.ToInt64(rows[0]["cnt"]);
		Assert.True(count >= 3);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#update_statement
	//   "Use the UPDATE statement to update existing rows in a table."
	[Fact]
	public async Task Update_ModifiesMatchingRows()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"UPDATE `{_datasetId}.data` SET value = 99 WHERE name = 'Alice'",
			parameters: null);

		var results = await client.ExecuteQueryAsync(
			$"SELECT value FROM `{_datasetId}.data` WHERE name = 'Alice'",
			parameters: null);
		var rows = results.ToList();
		Assert.Single(rows);
		Assert.Equal(99L, Convert.ToInt64(rows[0]["value"]));
	}

	[Fact]
	public async Task Update_NoMatch_ChangesNothing()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"UPDATE `{_datasetId}.data` SET value = 999 WHERE name = 'ZZZ'",
			parameters: null);

		var results = await client.ExecuteQueryAsync(
			$"SELECT COUNT(*) AS cnt FROM `{_datasetId}.data`",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(3L, Convert.ToInt64(rows[0]["cnt"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#delete_statement
	//   "Use the DELETE statement to delete rows from a table."
	[Fact]
	public async Task Delete_RemovesMatchingRows()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"DELETE FROM `{_datasetId}.data` WHERE name = 'Charlie'",
			parameters: null);

		var results = await client.ExecuteQueryAsync(
			$"SELECT COUNT(*) AS cnt FROM `{_datasetId}.data`",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(2L, Convert.ToInt64(rows[0]["cnt"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement
	//   "Use the MERGE statement to combine INSERT, UPDATE, and DELETE operations."
	[Fact]
	public async Task Merge_UpdatesAndInserts()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$@"MERGE `{_datasetId}.data` AS t
			USING `{_datasetId}.source` AS s
			ON t.id = s.id
			WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value
			WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, s.name, s.value)",
			parameters: null);

		var results = await client.ExecuteQueryAsync(
			$"SELECT * FROM `{_datasetId}.data` ORDER BY id",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(4, rows.Count); // 3 original + 1 inserted (Dave)
	}
}
