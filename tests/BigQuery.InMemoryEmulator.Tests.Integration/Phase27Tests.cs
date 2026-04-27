using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Phase 27 integration tests: DDL extensions, DML (TRUNCATE), navigation functions,
/// aggregate functions, and REST API (copy job, routines).
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class Phase27Tests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public Phase27Tests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_p27_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();

		// Seed source table for DDL tests
		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "score", Type = "FLOAT", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "src", schema);
		await client.InsertRowsAsync(_datasetId, "src", new[]
		{
			new BigQueryInsertRow("r1") { ["id"] = 1, ["name"] = "Alice", ["score"] = 95.5 },
			new BigQueryInsertRow("r2") { ["id"] = 2, ["name"] = "Bob", ["score"] = 82.0 },
			new BigQueryInsertRow("r3") { ["id"] = 3, ["name"] = "Carol", ["score"] = 70.0 },
			new BigQueryInsertRow("r4") { ["id"] = 4, ["name"] = "Dave", ["score"] = 88.5 },
			new BigQueryInsertRow("r5") { ["id"] = 5, ["name"] = "Eve", ["score"] = 91.0 },
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

	#region TRUNCATE TABLE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#truncate_table_statement
	//   "Deletes all rows from the named table."
	[Fact]
	public async Task TruncateTable_RemovesAllRows()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"TRUNCATE TABLE `{_datasetId}.src`", parameters: null);

		var results = await client.ExecuteQueryAsync(
			$"SELECT COUNT(*) AS cnt FROM `{_datasetId}.src`", parameters: null);
		var rows = results.ToList();
		Assert.Equal(0L, Convert.ToInt64(rows[0]["cnt"]));
	}

	[Fact]
	public async Task TruncateTable_PreservesSchema()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"TRUNCATE TABLE `{_datasetId}.src`", parameters: null);

		var table = await client.GetTableAsync(_datasetId, "src");
		Assert.Equal(3, table.Schema.Fields.Count);
	}

	#endregion

	#region CREATE TABLE LIKE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_like
	//   "Creates a new table with the same schema as the source table."
	[Fact]
	public async Task CreateTableLike_CopiesSchemaOnly()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"CREATE TABLE `{_datasetId}.like_tbl` LIKE `{_datasetId}.src`", parameters: null);

		var table = await client.GetTableAsync(_datasetId, "like_tbl");
		Assert.Equal(3, table.Schema.Fields.Count);

		var results = await client.ExecuteQueryAsync(
			$"SELECT COUNT(*) AS cnt FROM `{_datasetId}.like_tbl`", parameters: null);
		Assert.Equal(0L, Convert.ToInt64(results.ToList()[0]["cnt"]));
	}

	#endregion

	#region CREATE TABLE COPY / CLONE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_copy
	//   "Creates a new table by copying the schema and data from the source table."
	[Fact]
	public async Task CreateTableCopy_CopiesSchemaAndData()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"CREATE TABLE `{_datasetId}.copy_tbl` COPY `{_datasetId}.src`", parameters: null);

		var table = await client.GetTableAsync(_datasetId, "copy_tbl");
		Assert.Equal(3, table.Schema.Fields.Count);

		var results = await client.ExecuteQueryAsync(
			$"SELECT COUNT(*) AS cnt FROM `{_datasetId}.copy_tbl`", parameters: null);
		Assert.Equal(5L, Convert.ToInt64(results.ToList()[0]["cnt"]));
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_clone
	[Fact]
	public async Task CreateTableClone_CopiesSchemaAndData()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"CREATE TABLE `{_datasetId}.clone_tbl` CLONE `{_datasetId}.src`", parameters: null);

		var results = await client.ExecuteQueryAsync(
			$"SELECT COUNT(*) AS cnt FROM `{_datasetId}.clone_tbl`", parameters: null);
		Assert.Equal(5L, Convert.ToInt64(results.ToList()[0]["cnt"]));
	}

	#endregion

	#region ALTER COLUMN

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_column_set_data_type
	[Fact]
	public async Task AlterColumn_SetDataType()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"ALTER TABLE `{_datasetId}.src` ALTER COLUMN score SET DATA TYPE STRING",
			parameters: null);

		var table = await client.GetTableAsync(_datasetId, "src");
		var field = table.Schema.Fields.First(f => f.Name == "score");
		Assert.Equal("STRING", field.Type);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_column_drop_not_null
	[Fact]
	public async Task AlterColumn_DropNotNull()
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(
			$"ALTER TABLE `{_datasetId}.src` ALTER COLUMN id DROP NOT NULL",
			parameters: null);

		var table = await client.GetTableAsync(_datasetId, "src");
		var field = table.Schema.Fields.First(f => f.Name == "id");
		Assert.Equal("NULLABLE", field.Mode);
	}

	#endregion

	#region ALTER TABLE SET OPTIONS

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_table_set_options
	[Fact]
	public async Task AlterTable_SetOptions_NoError()
	{
		var client = await _fixture.GetClientAsync();
		// Should not throw
		await client.ExecuteQueryAsync(
			$"ALTER TABLE `{_datasetId}.src` SET OPTIONS (description = 'test')",
			parameters: null);
	}

	#endregion

	#region Navigation Functions

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#nth_value
	[Fact]
	public async Task NthValue_ReturnsNthRow()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT score, NTH_VALUE(score, 2) OVER (ORDER BY score) AS nv FROM `{_datasetId}.src` ORDER BY score",
			parameters: null);
		var rows = results.ToList();
		// 2nd value in the ordered window (70, 82, 88.5, 91, 95.5) = 82
		Assert.Equal(82.0, Convert.ToDouble(rows[1]["nv"]), 5);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#percentile_cont
	[Fact]
	public async Task PercentileCont_Median()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT PERCENTILE_CONT(score, 0.5) OVER () AS median FROM `{_datasetId}.src` LIMIT 1",
			parameters: null);
		var rows = results.ToList();
		// Sorted scores: 70, 82, 88.5, 91, 95.5 → median at index 2 = 88.5
		Assert.Equal(88.5, Convert.ToDouble(rows[0]["median"]), 5);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#percentile_disc
	[Fact]
	public async Task PercentileDisc_Median()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			$"SELECT PERCENTILE_DISC(score, 0.5) OVER () AS median FROM `{_datasetId}.src` LIMIT 1",
			parameters: null);
		var rows = results.ToList();
		// Sorted scores: 70, 82, 88.5, 91, 95.5
		// CUME_DIST: 0.2, 0.4, 0.6, 0.8, 1.0
		// First with CUME_DIST >= 0.5 is 88.5 (0.6)
		Assert.Equal(88.5, Convert.ToDouble(rows[0]["median"]), 5);
	}

	#endregion

	#region CREATE/DROP SCHEMA

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_schema_statement
	[Fact]
	public async Task CreateSchema_CreatesDataset()
	{
		var client = await _fixture.GetClientAsync();
		var newDs = $"test_schema_{Guid.NewGuid():N}"[..30];
		try
		{
			await client.ExecuteQueryAsync($"CREATE SCHEMA {newDs}", parameters: null);
			var dataset = await client.GetDatasetAsync(newDs);
			Assert.NotNull(dataset);
		}
		finally
		{
			try { await client.DeleteDatasetAsync(newDs); } catch { }
		}
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#drop_schema_statement
	[Fact]
	public async Task DropSchema_RemovesDataset()
	{
		var client = await _fixture.GetClientAsync();
		var newDs = $"test_schema_{Guid.NewGuid():N}"[..30];
		await client.ExecuteQueryAsync($"CREATE SCHEMA {newDs}", parameters: null);

		await client.ExecuteQueryAsync($"DROP SCHEMA {newDs}", parameters: null);
		await Assert.ThrowsAsync<Google.GoogleApiException>(
			() => client.GetDatasetAsync(newDs));
	}

	#endregion
}
