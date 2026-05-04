using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for DML: INSERT, UPDATE, DELETE, MERGE with complex conditions.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DmlExtendedTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public DmlExtendedTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_dml_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }
	private async Task Exec(string sql) { var c = await _fixture.GetClientAsync(); await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); }

	// ---- INSERT patterns ----
	[Fact] public async Task Insert_SingleRow()
	{
		await Exec("CREATE TABLE `{ds}.t1` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.t1` VALUES (1, 'Alice')");
		Assert.Equal("1", await S("SELECT COUNT(*) FROM `{ds}.t1`"));
	}
	[Fact] public async Task Insert_MultipleRows()
	{
		await Exec("CREATE TABLE `{ds}.t2` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.t2` VALUES (1,'A'),(2,'B'),(3,'C')");
		Assert.Equal("3", await S("SELECT COUNT(*) FROM `{ds}.t2`"));
	}
	[Fact] public async Task Insert_WithColumnList()
	{
		await Exec("CREATE TABLE `{ds}.t3` (id INT64, name STRING, val INT64)");
		await Exec("INSERT INTO `{ds}.t3` (id, name) VALUES (1, 'Alice')");
		var rows = await Q("SELECT * FROM `{ds}.t3`");
		Assert.Single(rows);
		Assert.Null(rows[0]["val"]);
	}
	[Fact] public async Task Insert_FromSelect()
	{
		await Exec("CREATE TABLE `{ds}.t4a` (id INT64, name STRING)");
		await Exec("CREATE TABLE `{ds}.t4b` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.t4a` VALUES (1,'A'),(2,'B'),(3,'C')");
		await Exec("INSERT INTO `{ds}.t4b` SELECT * FROM `{ds}.t4a` WHERE id <= 2");
		Assert.Equal("2", await S("SELECT COUNT(*) FROM `{ds}.t4b`"));
	}
	[Fact] public async Task Insert_WithExpressions()
	{
		await Exec("CREATE TABLE `{ds}.t5` (id INT64, doubled INT64, label STRING)");
		await Exec("INSERT INTO `{ds}.t5` VALUES (1, 1*2, CONCAT('item_', CAST(1 AS STRING)))");
		var rows = await Q("SELECT * FROM `{ds}.t5`");
		Assert.Equal("2", rows[0]["doubled"]?.ToString());
		Assert.Equal("item_1", rows[0]["label"]?.ToString());
	}
	[Fact] public async Task Insert_NullValues()
	{
		await Exec("CREATE TABLE `{ds}.t6` (id INT64, val INT64)");
		await Exec("INSERT INTO `{ds}.t6` VALUES (1, NULL)");
		Assert.Null((await Q("SELECT val FROM `{ds}.t6`"))[0]["val"]);
	}

	// ---- UPDATE patterns ----
	[Fact] public async Task Update_AllRows()
	{
		await Exec("CREATE TABLE `{ds}.u1` (id INT64, val INT64)");
		await Exec("INSERT INTO `{ds}.u1` VALUES (1,10),(2,20),(3,30)");
		await Exec("UPDATE `{ds}.u1` SET val = val * 2 WHERE true");
		Assert.Equal("120", await S("SELECT SUM(val) FROM `{ds}.u1`")); // 20+40+60
	}
	[Fact] public async Task Update_WithCondition()
	{
		await Exec("CREATE TABLE `{ds}.u2` (id INT64, status STRING)");
		await Exec("INSERT INTO `{ds}.u2` VALUES (1,'active'),(2,'active'),(3,'inactive')");
		await Exec("UPDATE `{ds}.u2` SET status = 'archived' WHERE status = 'inactive'");
		Assert.Equal("1", await S("SELECT COUNT(*) FROM `{ds}.u2` WHERE status = 'archived'"));
	}
	[Fact] public async Task Update_MultipleColumns()
	{
		await Exec("CREATE TABLE `{ds}.u3` (id INT64, name STRING, val INT64)");
		await Exec("INSERT INTO `{ds}.u3` VALUES (1,'old',10)");
		await Exec("UPDATE `{ds}.u3` SET name = 'new', val = 99 WHERE id = 1");
		var rows = await Q("SELECT * FROM `{ds}.u3`");
		Assert.Equal("new", rows[0]["name"]?.ToString());
		Assert.Equal("99", rows[0]["val"]?.ToString());
	}
	[Fact] public async Task Update_SetToNull()
	{
		await Exec("CREATE TABLE `{ds}.u4` (id INT64, val INT64)");
		await Exec("INSERT INTO `{ds}.u4` VALUES (1,10)");
		await Exec("UPDATE `{ds}.u4` SET val = NULL WHERE id = 1");
		Assert.Null((await Q("SELECT val FROM `{ds}.u4`"))[0]["val"]);
	}
	[Fact] public async Task Update_WithExpression()
	{
		await Exec("CREATE TABLE `{ds}.u5` (id INT64, price FLOAT64, tax FLOAT64)");
		await Exec("INSERT INTO `{ds}.u5` VALUES (1,100,0)");
		await Exec("UPDATE `{ds}.u5` SET tax = price * 0.1 WHERE true");
		Assert.Equal("10", await S("SELECT tax FROM `{ds}.u5`"));
	}
	[Fact] public async Task Update_NoMatchingRows()
	{
		await Exec("CREATE TABLE `{ds}.u6` (id INT64, val INT64)");
		await Exec("INSERT INTO `{ds}.u6` VALUES (1,10)");
		await Exec("UPDATE `{ds}.u6` SET val = 99 WHERE id = 999");
		Assert.Equal("10", await S("SELECT val FROM `{ds}.u6`"));
	}

	// ---- DELETE patterns ----
	[Fact] public async Task Delete_WithCondition()
	{
		await Exec("CREATE TABLE `{ds}.d1` (id INT64, val INT64)");
		await Exec("INSERT INTO `{ds}.d1` VALUES (1,10),(2,20),(3,30)");
		await Exec("DELETE FROM `{ds}.d1` WHERE val < 25");
		Assert.Equal("1", await S("SELECT COUNT(*) FROM `{ds}.d1`"));
	}
	[Fact] public async Task Delete_AllRows()
	{
		await Exec("CREATE TABLE `{ds}.d2` (id INT64)");
		await Exec("INSERT INTO `{ds}.d2` VALUES (1),(2),(3)");
		await Exec("DELETE FROM `{ds}.d2` WHERE true");
		Assert.Equal("0", await S("SELECT COUNT(*) FROM `{ds}.d2`"));
	}
	[Fact] public async Task Delete_NoMatch()
	{
		await Exec("CREATE TABLE `{ds}.d3` (id INT64)");
		await Exec("INSERT INTO `{ds}.d3` VALUES (1),(2),(3)");
		await Exec("DELETE FROM `{ds}.d3` WHERE id > 100");
		Assert.Equal("3", await S("SELECT COUNT(*) FROM `{ds}.d3`"));
	}
	[Fact] public async Task Delete_WithComplexCondition()
	{
		await Exec("CREATE TABLE `{ds}.d4` (id INT64, grp STRING, val INT64)");
		await Exec("INSERT INTO `{ds}.d4` VALUES (1,'A',10),(2,'A',20),(3,'B',30),(4,'B',40)");
		await Exec("DELETE FROM `{ds}.d4` WHERE grp = 'A' AND val < 15");
		Assert.Equal("3", await S("SELECT COUNT(*) FROM `{ds}.d4`"));
	}

	// ---- MERGE patterns ----
	[Fact] public async Task Merge_InsertOnly()
	{
		await Exec("CREATE TABLE `{ds}.m1` (id INT64, name STRING)");
		await Exec("CREATE TABLE `{ds}.m1s` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.m1s` VALUES (1,'Alice'),(2,'Bob')");
		await Exec(@"
			MERGE `{ds}.m1` t
			USING `{ds}.m1s` s ON t.id = s.id
			WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)");
		Assert.Equal("2", await S("SELECT COUNT(*) FROM `{ds}.m1`"));
	}
	[Fact] public async Task Merge_UpdateOnly()
	{
		await Exec("CREATE TABLE `{ds}.m2` (id INT64, name STRING)");
		await Exec("CREATE TABLE `{ds}.m2s` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.m2` VALUES (1,'old')");
		await Exec("INSERT INTO `{ds}.m2s` VALUES (1,'new')");
		await Exec(@"
			MERGE `{ds}.m2` t
			USING `{ds}.m2s` s ON t.id = s.id
			WHEN MATCHED THEN UPDATE SET name = s.name");
		Assert.Equal("new", await S("SELECT name FROM `{ds}.m2`"));
	}
	[Fact] public async Task Merge_UpsertPattern()
	{
		await Exec("CREATE TABLE `{ds}.m3` (id INT64, val INT64)");
		await Exec("CREATE TABLE `{ds}.m3s` (id INT64, val INT64)");
		await Exec("INSERT INTO `{ds}.m3` VALUES (1,10)");
		await Exec("INSERT INTO `{ds}.m3s` VALUES (1,20),(2,30)");
		await Exec(@"
			MERGE `{ds}.m3` t
			USING `{ds}.m3s` s ON t.id = s.id
			WHEN MATCHED THEN UPDATE SET val = s.val
			WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)");
		Assert.Equal("2", await S("SELECT COUNT(*) FROM `{ds}.m3`"));
		Assert.Equal("20", await S("SELECT val FROM `{ds}.m3` WHERE id = 1"));
	}
	[Fact] public async Task Merge_DeletePattern()
	{
		await Exec("CREATE TABLE `{ds}.m4` (id INT64, active BOOL)");
		await Exec("CREATE TABLE `{ds}.m4s` (id INT64)");
		await Exec("INSERT INTO `{ds}.m4` VALUES (1,true),(2,true),(3,true)");
		await Exec("INSERT INTO `{ds}.m4s` VALUES (2)");
		await Exec(@"
			MERGE `{ds}.m4` t
			USING `{ds}.m4s` s ON t.id = s.id
			WHEN MATCHED THEN DELETE");
		Assert.Equal("2", await S("SELECT COUNT(*) FROM `{ds}.m4`"));
	}

	// ---- Multi-step DML ----
	[Fact] public async Task Multi_InsertUpdateDelete()
	{
		await Exec("CREATE TABLE `{ds}.ms1` (id INT64, val INT64)");
		await Exec("INSERT INTO `{ds}.ms1` VALUES (1,10),(2,20),(3,30)");
		await Exec("UPDATE `{ds}.ms1` SET val = val + 5 WHERE id = 1");
		await Exec("DELETE FROM `{ds}.ms1` WHERE id = 3");
		var rows = await Q("SELECT id, val FROM `{ds}.ms1` ORDER BY id");
		Assert.Equal(2, rows.Count);
		Assert.Equal("15", rows[0]["val"]?.ToString());
	}
	[Fact] public async Task Multi_InsertThenSelectAggregate()
	{
		await Exec("CREATE TABLE `{ds}.ms2` (id INT64, amount FLOAT64)");
		await Exec("INSERT INTO `{ds}.ms2` VALUES (1,10.5),(2,20.3),(3,15.2)");
		Assert.Equal("46", await S("SELECT SUM(amount) FROM `{ds}.ms2`"));
		Assert.Equal("3", await S("SELECT COUNT(*) FROM `{ds}.ms2`"));
	}
}
