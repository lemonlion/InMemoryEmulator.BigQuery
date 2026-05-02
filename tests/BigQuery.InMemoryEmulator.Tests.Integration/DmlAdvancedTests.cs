using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for DML: MERGE, complex UPDATE, complex DELETE, INSERT SELECT, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DmlAdvancedTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public DmlAdvancedTests(BigQuerySession session) => _session = session;

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

	private async Task<List<BigQueryRow>> Q(string sql)
	{
		var client = await _fixture.GetClientAsync();
		return (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
	}

	private async Task<string?> S(string sql)
	{
		var rows = await Q(sql);
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	private async Task Exec(string sql)
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(sql, parameters: null);
	}

	private async Task SetupTable(string name, params (int id, string name, int value)[] data)
	{
		var client = await _fixture.GetClientAsync();
		await client.CreateTableAsync(_datasetId, name, new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING" },
				new TableFieldSchema { Name = "value", Type = "INTEGER" },
			]
		});
		if (data.Length > 0)
		{
			await client.InsertRowsAsync(_datasetId, name, data.Select((d, i) =>
				new BigQueryInsertRow($"r{i}") { ["id"] = d.id, ["name"] = d.name, ["value"] = d.value }).ToArray());
		}
	}

	// ---- INSERT ... VALUES ----
	[Fact] public async Task Insert_SingleRow()
	{
		await SetupTable("ins1");
		await Exec($"INSERT INTO `{_datasetId}.ins1` (id, name, value) VALUES (1, 'Alice', 100)");
		Assert.Equal("1", await S($"SELECT COUNT(*) FROM `{_datasetId}.ins1`"));
	}

	[Fact] public async Task Insert_MultipleRows()
	{
		await SetupTable("ins2");
		await Exec($"INSERT INTO `{_datasetId}.ins2` (id, name, value) VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30)");
		Assert.Equal("3", await S($"SELECT COUNT(*) FROM `{_datasetId}.ins2`"));
	}

	// ---- INSERT ... SELECT ----
	[Fact] public async Task InsertSelect_FromTable()
	{
		await SetupTable("src1", (1, "A", 10), (2, "B", 20));
		await SetupTable("dst1");
		await Exec($"INSERT INTO `{_datasetId}.dst1` SELECT * FROM `{_datasetId}.src1`");
		Assert.Equal("2", await S($"SELECT COUNT(*) FROM `{_datasetId}.dst1`"));
	}

	[Fact] public async Task InsertSelect_WithWhere()
	{
		await SetupTable("src2", (1, "A", 10), (2, "B", 20), (3, "C", 30));
		await SetupTable("dst2");
		await Exec($"INSERT INTO `{_datasetId}.dst2` SELECT * FROM `{_datasetId}.src2` WHERE value > 15");
		Assert.Equal("2", await S($"SELECT COUNT(*) FROM `{_datasetId}.dst2`"));
	}

	// ---- UPDATE ----
	[Fact] public async Task Update_AllRows()
	{
		await SetupTable("upd1", (1, "A", 10), (2, "B", 20));
		await Exec($"UPDATE `{_datasetId}.upd1` SET value = 99 WHERE TRUE");
		var rows = await Q($"SELECT value FROM `{_datasetId}.upd1` ORDER BY id");
		Assert.All(rows, r => Assert.Equal("99", r["value"]?.ToString()));
	}

	[Fact] public async Task Update_WithCondition()
	{
		await SetupTable("upd2", (1, "A", 10), (2, "B", 20), (3, "C", 30));
		await Exec($"UPDATE `{_datasetId}.upd2` SET value = value * 2 WHERE id > 1");
		Assert.Equal("10", await S($"SELECT value FROM `{_datasetId}.upd2` WHERE id = 1"));
		Assert.Equal("40", await S($"SELECT value FROM `{_datasetId}.upd2` WHERE id = 2"));
	}

	[Fact] public async Task Update_MultipleColumns()
	{
		await SetupTable("upd3", (1, "A", 10));
		await Exec($"UPDATE `{_datasetId}.upd3` SET name = 'Z', value = 999 WHERE id = 1");
		var row = (await Q($"SELECT name, value FROM `{_datasetId}.upd3` WHERE id = 1")).First();
		Assert.Equal("Z", row["name"]?.ToString());
		Assert.Equal("999", row["value"]?.ToString());
	}

	// ---- DELETE ----
	[Fact] public async Task Delete_WithCondition()
	{
		await SetupTable("del1", (1, "A", 10), (2, "B", 20), (3, "C", 30));
		await Exec($"DELETE FROM `{_datasetId}.del1` WHERE id = 2");
		Assert.Equal("2", await S($"SELECT COUNT(*) FROM `{_datasetId}.del1`"));
	}

	[Fact] public async Task Delete_AllRows()
	{
		await SetupTable("del2", (1, "A", 10), (2, "B", 20));
		await Exec($"DELETE FROM `{_datasetId}.del2` WHERE TRUE");
		Assert.Equal("0", await S($"SELECT COUNT(*) FROM `{_datasetId}.del2`"));
	}

	[Fact] public async Task Delete_NoMatch()
	{
		await SetupTable("del3", (1, "A", 10));
		await Exec($"DELETE FROM `{_datasetId}.del3` WHERE id = 999");
		Assert.Equal("1", await S($"SELECT COUNT(*) FROM `{_datasetId}.del3`"));
	}

	// ---- MERGE ----
	[Fact] public async Task Merge_InsertAndUpdate()
	{
		await SetupTable("target1", (1, "A", 10), (2, "B", 20));
		await SetupTable("source1", (2, "B2", 25), (3, "C", 30));
		await Exec($@"
			MERGE `{_datasetId}.target1` T
			USING `{_datasetId}.source1` S ON T.id = S.id
			WHEN MATCHED THEN UPDATE SET name = S.name, value = S.value
			WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (S.id, S.name, S.value)");
		Assert.Equal("3", await S($"SELECT COUNT(*) FROM `{_datasetId}.target1`"));
		Assert.Equal("B2", await S($"SELECT name FROM `{_datasetId}.target1` WHERE id = 2"));
		Assert.Equal("25", await S($"SELECT value FROM `{_datasetId}.target1` WHERE id = 2"));
	}

	[Fact] public async Task Merge_DeleteMatched()
	{
		await SetupTable("target2", (1, "A", 10), (2, "B", 20), (3, "C", 30));
		await SetupTable("source2", (2, "B", 20));
		await Exec($@"
			MERGE `{_datasetId}.target2` T
			USING `{_datasetId}.source2` S ON T.id = S.id
			WHEN MATCHED THEN DELETE");
		Assert.Equal("2", await S($"SELECT COUNT(*) FROM `{_datasetId}.target2`"));
	}

	[Fact] public async Task Merge_WithConditions()
	{
		await SetupTable("target3", (1, "A", 10), (2, "B", 20));
		await SetupTable("source3", (1, "A", 100), (2, "B", 200), (3, "C", 300));
		await Exec($@"
			MERGE `{_datasetId}.target3` T
			USING `{_datasetId}.source3` S ON T.id = S.id
			WHEN MATCHED AND S.value > 150 THEN UPDATE SET value = S.value
			WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (S.id, S.name, S.value)");
		Assert.Equal("10", await S($"SELECT value FROM `{_datasetId}.target3` WHERE id = 1")); // not updated (100 < 150)
		Assert.Equal("200", await S($"SELECT value FROM `{_datasetId}.target3` WHERE id = 2")); // updated
		Assert.Equal("3", await S($"SELECT COUNT(*) FROM `{_datasetId}.target3`")); // 3 inserted
	}

	[Fact] public async Task Merge_UpdateAndInsertNotMatched()
	{
		await SetupTable("target4", (1, "A", 10), (2, "B", 20), (3, "C", 30));
		await SetupTable("source4", (1, "A", 100));
		await Exec($@"
			MERGE `{_datasetId}.target4` T
			USING `{_datasetId}.source4` S ON T.id = S.id
			WHEN MATCHED THEN UPDATE SET value = S.value
			WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (S.id, S.name, S.value)");
		Assert.Equal("100", await S($"SELECT value FROM `{_datasetId}.target4` WHERE id = 1"));
	}

	// ---- UPDATE with expression ----
	[Fact] public async Task Update_WithExpression()
	{
		await SetupTable("upd4", (1, "A", 10), (2, "B", 20));
		await Exec($"UPDATE `{_datasetId}.upd4` SET name = CONCAT(name, '_updated') WHERE TRUE");
		Assert.Equal("A_updated", await S($"SELECT name FROM `{_datasetId}.upd4` WHERE id = 1"));
	}

	// ---- DELETE with subquery ----
	[Fact] public async Task Delete_WithSubquery()
	{
		await SetupTable("del4", (1, "A", 10), (2, "B", 20), (3, "C", 30));
		await Exec($"DELETE FROM `{_datasetId}.del4` WHERE id IN (SELECT id FROM `{_datasetId}.del4` WHERE value > 15)");
		Assert.Equal("1", await S($"SELECT COUNT(*) FROM `{_datasetId}.del4`"));
		Assert.Equal("A", await S($"SELECT name FROM `{_datasetId}.del4`"));
	}
}
