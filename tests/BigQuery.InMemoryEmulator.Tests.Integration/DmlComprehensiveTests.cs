using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Comprehensive DML edge case tests: INSERT, UPDATE, DELETE, MERGE.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DmlComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public DmlComprehensiveTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_dml_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<BigQueryClient> GetClient() => await _fixture.GetClientAsync();

	private async Task Exec(string sql)
	{
		var client = await GetClient();
		await client.ExecuteQueryAsync(sql, parameters: null);
	}

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await GetClient();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		return result.ToList();
	}

	private async Task<string?> Scalar(string sql)
	{
		var rows = await Query(sql);
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	private async Task CreateSimpleTable(string tableName)
	{
		var client = await GetClient();
		await client.CreateTableAsync(_datasetId, tableName, new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING" },
				new TableFieldSchema { Name = "value", Type = "FLOAT" },
			]
		});
	}

	// ---- INSERT VALUES ----
	[Fact] public async Task Insert_SingleRow()
	{
		await CreateSimpleTable("t1");
		await Exec($"INSERT INTO `{_datasetId}.t1` (id, name, value) VALUES (1, 'Alice', 10.0)");
		var rows = await Query($"SELECT * FROM `{_datasetId}.t1`");
		Assert.Single(rows);
		Assert.Equal("1", rows[0]["id"]?.ToString());
	}

	[Fact] public async Task Insert_MultipleRows()
	{
		await CreateSimpleTable("t2");
		await Exec($"INSERT INTO `{_datasetId}.t2` (id, name, value) VALUES (1, 'A', 1.0), (2, 'B', 2.0), (3, 'C', 3.0)");
		var rows = await Query($"SELECT COUNT(*) FROM `{_datasetId}.t2`");
		Assert.Equal("3", rows[0][0]?.ToString());
	}

	[Fact] public async Task Insert_WithNull()
	{
		await CreateSimpleTable("t3");
		await Exec($"INSERT INTO `{_datasetId}.t3` (id, name, value) VALUES (1, NULL, NULL)");
		var rows = await Query($"SELECT name, value FROM `{_datasetId}.t3` WHERE id = 1");
		Assert.Null(rows[0]["name"]);
	}

	[Fact] public async Task Insert_Select()
	{
		await CreateSimpleTable("t4a");
		await CreateSimpleTable("t4b");
		await Exec($"INSERT INTO `{_datasetId}.t4a` (id, name, value) VALUES (1, 'X', 100.0)");
		await Exec($"INSERT INTO `{_datasetId}.t4b` SELECT * FROM `{_datasetId}.t4a`");
		var rows = await Query($"SELECT COUNT(*) FROM `{_datasetId}.t4b`");
		Assert.Equal("1", rows[0][0]?.ToString());
	}

	[Fact(Skip = "Needs investigation")] public async Task Insert_Select_WithTransformation()
	{
		await CreateSimpleTable("t5a");
		await CreateSimpleTable("t5b");
		await Exec($"INSERT INTO `{_datasetId}.t5a` (id, name, value) VALUES (1, 'hello', 5.0)");
		await Exec($"INSERT INTO `{_datasetId}.t5b` (id, name, value) SELECT id + 10, UPPER(name), value * 2 FROM `{_datasetId}.t5a`");
		var rows = await Query($"SELECT * FROM `{_datasetId}.t5b`");
		Assert.Equal("11", rows[0]["id"]?.ToString());
		Assert.Equal("HELLO", rows[0]["name"]?.ToString());
		Assert.Equal("10.0", rows[0]["value"]?.ToString());
	}

	[Fact] public async Task Insert_DuplicateRows()
	{
		await CreateSimpleTable("t6");
		await Exec($"INSERT INTO `{_datasetId}.t6` (id, name, value) VALUES (1, 'A', 1.0)");
		await Exec($"INSERT INTO `{_datasetId}.t6` (id, name, value) VALUES (1, 'B', 2.0)");
		var rows = await Query($"SELECT COUNT(*) FROM `{_datasetId}.t6`");
		Assert.Equal("2", rows[0][0]?.ToString());
	}

	// ---- UPDATE ----
	[Fact] public async Task Update_SingleColumn()
	{
		await CreateSimpleTable("t7");
		await Exec($"INSERT INTO `{_datasetId}.t7` (id, name, value) VALUES (1, 'old', 1.0)");
		await Exec($"UPDATE `{_datasetId}.t7` SET name = 'new' WHERE id = 1");
		Assert.Equal("new", await Scalar($"SELECT name FROM `{_datasetId}.t7` WHERE id = 1"));
	}

	[Fact(Skip = "Needs investigation")] public async Task Update_MultipleColumns()
	{
		await CreateSimpleTable("t8");
		await Exec($"INSERT INTO `{_datasetId}.t8` (id, name, value) VALUES (1, 'old', 1.0)");
		await Exec($"UPDATE `{_datasetId}.t8` SET name = 'new', value = 99.0 WHERE id = 1");
		var rows = await Query($"SELECT name, value FROM `{_datasetId}.t8` WHERE id = 1");
		Assert.Equal("new", rows[0]["name"]?.ToString());
		Assert.Equal("99.0", rows[0]["value"]?.ToString());
	}

	[Fact] public async Task Update_NoMatchingRows()
	{
		await CreateSimpleTable("t9");
		await Exec($"INSERT INTO `{_datasetId}.t9` (id, name, value) VALUES (1, 'A', 1.0)");
		await Exec($"UPDATE `{_datasetId}.t9` SET name = 'B' WHERE id = 999");
		Assert.Equal("A", await Scalar($"SELECT name FROM `{_datasetId}.t9` WHERE id = 1"));
	}

	[Fact] public async Task Update_AllRows()
	{
		await CreateSimpleTable("t10");
		await Exec($"INSERT INTO `{_datasetId}.t10` (id, name, value) VALUES (1, 'A', 1.0), (2, 'B', 2.0)");
		await Exec($"UPDATE `{_datasetId}.t10` SET value = 0 WHERE TRUE");
		var rows = await Query($"SELECT value FROM `{_datasetId}.t10`");
		Assert.All(rows, r => Assert.True(r["value"]?.ToString() == "0" || r["value"]?.ToString() == "0.0"));
	}

	[Fact(Skip = "Needs investigation")] public async Task Update_WithExpression()
	{
		await CreateSimpleTable("t11");
		await Exec($"INSERT INTO `{_datasetId}.t11` (id, name, value) VALUES (1, 'hello', 10.0)");
		await Exec($"UPDATE `{_datasetId}.t11` SET value = value * 2, name = UPPER(name) WHERE id = 1");
		var rows = await Query($"SELECT name, value FROM `{_datasetId}.t11` WHERE id = 1");
		Assert.Equal("HELLO", rows[0]["name"]?.ToString());
		Assert.Equal("20.0", rows[0]["value"]?.ToString());
	}

	// ---- DELETE ----
	[Fact] public async Task Delete_MatchingRow()
	{
		await CreateSimpleTable("t12");
		await Exec($"INSERT INTO `{_datasetId}.t12` (id, name, value) VALUES (1, 'A', 1.0), (2, 'B', 2.0)");
		await Exec($"DELETE FROM `{_datasetId}.t12` WHERE id = 1");
		var rows = await Query($"SELECT COUNT(*) FROM `{_datasetId}.t12`");
		Assert.Equal("1", rows[0][0]?.ToString());
	}

	[Fact] public async Task Delete_AllRows()
	{
		await CreateSimpleTable("t13");
		await Exec($"INSERT INTO `{_datasetId}.t13` (id, name, value) VALUES (1, 'A', 1.0), (2, 'B', 2.0)");
		await Exec($"DELETE FROM `{_datasetId}.t13` WHERE TRUE");
		Assert.Equal("0", await Scalar($"SELECT COUNT(*) FROM `{_datasetId}.t13`"));
	}

	[Fact] public async Task Delete_NoMatch()
	{
		await CreateSimpleTable("t14");
		await Exec($"INSERT INTO `{_datasetId}.t14` (id, name, value) VALUES (1, 'A', 1.0)");
		await Exec($"DELETE FROM `{_datasetId}.t14` WHERE id = 999");
		Assert.Equal("1", await Scalar($"SELECT COUNT(*) FROM `{_datasetId}.t14`"));
	}

	[Fact] public async Task Delete_WithComplexCondition()
	{
		await CreateSimpleTable("t15");
		await Exec($"INSERT INTO `{_datasetId}.t15` (id, name, value) VALUES (1, 'A', 10.0), (2, 'B', 20.0), (3, 'C', 30.0)");
		await Exec($"DELETE FROM `{_datasetId}.t15` WHERE value > 15 AND name != 'C'");
		Assert.Equal("2", await Scalar($"SELECT COUNT(*) FROM `{_datasetId}.t15`"));
	}

	// ---- MERGE ----
	[Fact] public async Task Merge_InsertAndUpdate()
	{
		await CreateSimpleTable("target1");
		await CreateSimpleTable("source1");
		await Exec($"INSERT INTO `{_datasetId}.target1` (id, name, value) VALUES (1, 'Old', 1.0)");
		await Exec($"INSERT INTO `{_datasetId}.source1` (id, name, value) VALUES (1, 'Updated', 99.0), (2, 'New', 50.0)");
		await Exec($@"
			MERGE `{_datasetId}.target1` T
			USING `{_datasetId}.source1` S ON T.id = S.id
			WHEN MATCHED THEN UPDATE SET name = S.name, value = S.value
			WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (S.id, S.name, S.value)
		");
		var rows = await Query($"SELECT * FROM `{_datasetId}.target1` ORDER BY id");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Updated", rows[0]["name"]?.ToString());
		Assert.Equal("New", rows[1]["name"]?.ToString());
	}

	[Fact] public async Task Merge_Delete()
	{
		await CreateSimpleTable("target2");
		await CreateSimpleTable("source2");
		await Exec($"INSERT INTO `{_datasetId}.target2` (id, name, value) VALUES (1, 'A', 1.0), (2, 'B', 2.0)");
		await Exec($"INSERT INTO `{_datasetId}.source2` (id, name, value) VALUES (1, 'Del', 0.0)");
		await Exec($@"
			MERGE `{_datasetId}.target2` T
			USING `{_datasetId}.source2` S ON T.id = S.id
			WHEN MATCHED THEN DELETE
		");
		Assert.Equal("1", await Scalar($"SELECT COUNT(*) FROM `{_datasetId}.target2`"));
	}

	[Fact(Skip = "Not yet supported")] public async Task Merge_ConditionalMatch()
	{
		await CreateSimpleTable("target3");
		await CreateSimpleTable("source3");
		await Exec($"INSERT INTO `{_datasetId}.target3` (id, name, value) VALUES (1, 'A', 10.0), (2, 'B', 20.0)");
		await Exec($"INSERT INTO `{_datasetId}.source3` (id, name, value) VALUES (1, 'X', 100.0), (2, 'Y', 200.0)");
		await Exec($@"
			MERGE `{_datasetId}.target3` T
			USING `{_datasetId}.source3` S ON T.id = S.id
			WHEN MATCHED AND S.value > 150 THEN UPDATE SET value = S.value
			WHEN MATCHED THEN UPDATE SET name = S.name
		");
		var rows = await Query($"SELECT * FROM `{_datasetId}.target3` ORDER BY id");
		Assert.Equal("X", rows[0]["name"]?.ToString());
		Assert.Equal("200.0", rows[1]["value"]?.ToString());
	}

	// ---- INSERT with expression computations ----
	[Fact] public async Task Insert_WithComputedExpressions()
	{
		await CreateSimpleTable("t16");
		await Exec($"INSERT INTO `{_datasetId}.t16` (id, name, value) VALUES (1, CONCAT('Hello', ' ', 'World'), 2.0 * 3.0)");
		var rows = await Query($"SELECT name, value FROM `{_datasetId}.t16`");
		Assert.Equal("Hello World", rows[0]["name"]?.ToString());
		Assert.True(rows[0]["value"]?.ToString() == "6" || rows[0]["value"]?.ToString() == "6.0");
	}

	// ---- UPDATE with subquery ----
	[Fact(Skip = "Not yet supported")] public async Task Update_WithSubquery()
	{
		await CreateSimpleTable("t17a");
		await CreateSimpleTable("t17b");
		await Exec($"INSERT INTO `{_datasetId}.t17a` (id, name, value) VALUES (1, 'A', 0.0)");
		await Exec($"INSERT INTO `{_datasetId}.t17b` (id, name, value) VALUES (1, 'B', 99.0)");
		await Exec($"UPDATE `{_datasetId}.t17a` SET value = (SELECT MAX(value) FROM `{_datasetId}.t17b`) WHERE id = 1");
		Assert.Equal("99.0", await Scalar($"SELECT value FROM `{_datasetId}.t17a` WHERE id = 1"));
	}
}
