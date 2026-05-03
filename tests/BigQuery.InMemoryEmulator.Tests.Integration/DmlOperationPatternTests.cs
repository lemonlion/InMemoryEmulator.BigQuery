using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for DML operations: INSERT, UPDATE, DELETE, MERGE.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DmlOperationPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public DmlOperationPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_dml_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($"CREATE TABLE `{_datasetId}.t` (id INT64, name STRING, value INT64)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	private async Task Exec(string sql)
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
	}

	// INSERT
	[Fact] public async Task Insert_SingleRow()
	{
		await Exec("INSERT INTO `{ds}.t` (id, name, value) VALUES (1, 'Alice', 100)");
		Assert.Equal("1", await Scalar("SELECT COUNT(*) FROM `{ds}.t`"));
	}

	[Fact] public async Task Insert_MultipleRows()
	{
		await Exec("INSERT INTO `{ds}.t` (id, name, value) VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30)");
		Assert.Equal("3", await Scalar("SELECT COUNT(*) FROM `{ds}.t`"));
	}

	[Fact] public async Task Insert_WithNull()
	{
		await Exec("INSERT INTO `{ds}.t` (id, name, value) VALUES (1, 'Test', NULL)");
		Assert.Null(await Scalar("SELECT value FROM `{ds}.t` WHERE id = 1"));
	}

	[Fact] public async Task Insert_Select()
	{
		await Exec("INSERT INTO `{ds}.t` (id, name, value) VALUES (1, 'A', 10), (2, 'B', 20)");
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($"CREATE TABLE `{_datasetId}.t2` (id INT64, name STRING, value INT64)", parameters: null);
		await Exec("INSERT INTO `{ds}.t2` (id, name, value) SELECT id, name, value FROM `{ds}.t` WHERE value > 15");
		Assert.Equal("1", await Scalar("SELECT COUNT(*) FROM `{ds}.t2`"));
	}

	// UPDATE
	[Fact] public async Task Update_SingleColumn()
	{
		await Exec("INSERT INTO `{ds}.t` (id, name, value) VALUES (1, 'Alice', 100)");
		await Exec("UPDATE `{ds}.t` SET value = 200 WHERE id = 1");
		Assert.Equal("200", await Scalar("SELECT value FROM `{ds}.t` WHERE id = 1"));
	}

	[Fact] public async Task Update_MultipleColumns()
	{
		await Exec("INSERT INTO `{ds}.t` (id, name, value) VALUES (1, 'Alice', 100)");
		await Exec("UPDATE `{ds}.t` SET name = 'Bob', value = 200 WHERE id = 1");
		Assert.Equal("Bob", await Scalar("SELECT name FROM `{ds}.t` WHERE id = 1"));
		Assert.Equal("200", await Scalar("SELECT value FROM `{ds}.t` WHERE id = 1"));
	}

	[Fact] public async Task Update_MultipleRows()
	{
		await Exec("INSERT INTO `{ds}.t` (id, name, value) VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30)");
		await Exec("UPDATE `{ds}.t` SET value = value * 2 WHERE value > 15");
		Assert.Equal("10", await Scalar("SELECT value FROM `{ds}.t` WHERE id = 1"));
		Assert.Equal("40", await Scalar("SELECT value FROM `{ds}.t` WHERE id = 2"));
		Assert.Equal("60", await Scalar("SELECT value FROM `{ds}.t` WHERE id = 3"));
	}

	[Fact] public async Task Update_SetNull()
	{
		await Exec("INSERT INTO `{ds}.t` (id, name, value) VALUES (1, 'Alice', 100)");
		await Exec("UPDATE `{ds}.t` SET value = NULL WHERE id = 1");
		Assert.Null(await Scalar("SELECT value FROM `{ds}.t` WHERE id = 1"));
	}

	[Fact] public async Task Update_WhereTrue()
	{
		await Exec("INSERT INTO `{ds}.t` (id, name, value) VALUES (1, 'A', 10), (2, 'B', 20)");
		await Exec("UPDATE `{ds}.t` SET value = 0 WHERE TRUE");
		Assert.Equal("0", await Scalar("SELECT SUM(value) FROM `{ds}.t`"));
	}

	// DELETE
	[Fact] public async Task Delete_ByCondition()
	{
		await Exec("INSERT INTO `{ds}.t` (id, name, value) VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30)");
		await Exec("DELETE FROM `{ds}.t` WHERE value > 15");
		Assert.Equal("1", await Scalar("SELECT COUNT(*) FROM `{ds}.t`"));
	}

	[Fact] public async Task Delete_All()
	{
		await Exec("INSERT INTO `{ds}.t` (id, name, value) VALUES (1, 'A', 10), (2, 'B', 20)");
		await Exec("DELETE FROM `{ds}.t` WHERE TRUE");
		Assert.Equal("0", await Scalar("SELECT COUNT(*) FROM `{ds}.t`"));
	}

	[Fact] public async Task Delete_NoMatch()
	{
		await Exec("INSERT INTO `{ds}.t` (id, name, value) VALUES (1, 'A', 10)");
		await Exec("DELETE FROM `{ds}.t` WHERE value > 100");
		Assert.Equal("1", await Scalar("SELECT COUNT(*) FROM `{ds}.t`"));
	}

	// MERGE
	[Fact] public async Task Merge_InsertWhenNotMatched()
	{
		await Exec("INSERT INTO `{ds}.t` (id, name, value) VALUES (1, 'A', 10)");
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($"CREATE TABLE `{_datasetId}.src` (id INT64, name STRING, value INT64)", parameters: null);
		await Exec("INSERT INTO `{ds}.src` (id, name, value) VALUES (1, 'A_new', 100), (2, 'B', 200)");
		await Exec(@"MERGE `{ds}.t` AS target
			USING `{ds}.src` AS source
			ON target.id = source.id
			WHEN MATCHED THEN UPDATE SET name = source.name, value = source.value
			WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (source.id, source.name, source.value)");
		Assert.Equal("2", await Scalar("SELECT COUNT(*) FROM `{ds}.t`"));
		Assert.Equal("A_new", await Scalar("SELECT name FROM `{ds}.t` WHERE id = 1"));
		Assert.Equal("200", await Scalar("SELECT value FROM `{ds}.t` WHERE id = 2"));
	}

	[Fact] public async Task Merge_DeleteWhenMatched()
	{
		await Exec("INSERT INTO `{ds}.t` (id, name, value) VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30)");
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($"CREATE TABLE `{_datasetId}.del_src` (id INT64, name STRING, value INT64)", parameters: null);
		await Exec("INSERT INTO `{ds}.del_src` (id, name, value) VALUES (2, 'B', 0)");
		await Exec(@"MERGE `{ds}.t` AS target
			USING `{ds}.del_src` AS source
			ON target.id = source.id
			WHEN MATCHED THEN DELETE");
		Assert.Equal("2", await Scalar("SELECT COUNT(*) FROM `{ds}.t`"));
	}

	// TRUNCATE
	[Fact] public async Task Truncate_Table()
	{
		await Exec("INSERT INTO `{ds}.t` (id, name, value) VALUES (1, 'A', 10), (2, 'B', 20)");
		await Exec("TRUNCATE TABLE `{ds}.t`");
		Assert.Equal("0", await Scalar("SELECT COUNT(*) FROM `{ds}.t`"));
	}

	// INSERT with expressions
	[Fact] public async Task Insert_WithExpression()
	{
		await Exec("INSERT INTO `{ds}.t` (id, name, value) VALUES (1, CONCAT('Hello', ' World'), 2 + 3)");
		Assert.Equal("Hello World", await Scalar("SELECT name FROM `{ds}.t` WHERE id = 1"));
		Assert.Equal("5", await Scalar("SELECT value FROM `{ds}.t` WHERE id = 1"));
	}
}
