using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// UPDATE and DELETE DML statement patterns.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class UpdateDeletePatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public UpdateDeletePatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_udp_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }
	private async Task E(string sql) { var c = await _fixture.GetClientAsync(); await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); }

	// ---- UPDATE basic ----
	[Fact] public async Task Update_SingleRow()
	{
		await E("CREATE TABLE `{ds}.t1` (id INT64, name STRING, val INT64)");
		await E("INSERT INTO `{ds}.t1` VALUES (1,'Alice',100),(2,'Bob',200),(3,'Carol',300)");
		await E("UPDATE `{ds}.t1` SET val = 150 WHERE id = 1");
		var v = await S("SELECT val FROM `{ds}.t1` WHERE id = 1");
		Assert.Equal("150", v);
	}
	[Fact] public async Task Update_MultipleRows()
	{
		await E("CREATE TABLE `{ds}.t2` (id INT64, name STRING, dept STRING, salary INT64)");
		await E("INSERT INTO `{ds}.t2` VALUES (1,'Alice','Eng',100),(2,'Bob','Eng',200),(3,'Carol','Sales',300)");
		await E("UPDATE `{ds}.t2` SET salary = salary + 10 WHERE dept = 'Eng'");
		var rows = await Q("SELECT salary FROM `{ds}.t2` WHERE dept = 'Eng' ORDER BY id");
		Assert.Equal("110", rows[0]["salary"]?.ToString());
		Assert.Equal("210", rows[1]["salary"]?.ToString());
	}
	[Fact] public async Task Update_AllRows()
	{
		await E("CREATE TABLE `{ds}.t3` (id INT64, active BOOL)");
		await E("INSERT INTO `{ds}.t3` VALUES (1,true),(2,true),(3,false)");
		await E("UPDATE `{ds}.t3` SET active = false WHERE true");
		var v = await S("SELECT COUNT(*) FROM `{ds}.t3` WHERE active = false");
		Assert.Equal("3", v);
	}
	[Fact] public async Task Update_SetMultipleColumns()
	{
		await E("CREATE TABLE `{ds}.t4` (id INT64, name STRING, val INT64)");
		await E("INSERT INTO `{ds}.t4` VALUES (1,'Alice',100)");
		await E("UPDATE `{ds}.t4` SET name = 'Alice_Updated', val = 999 WHERE id = 1");
		var rows = await Q("SELECT name, val FROM `{ds}.t4` WHERE id = 1");
		Assert.Equal("Alice_Updated", rows[0]["name"]?.ToString());
		Assert.Equal("999", rows[0]["val"]?.ToString());
	}
	[Fact] public async Task Update_WithExpression()
	{
		await E("CREATE TABLE `{ds}.t5` (id INT64, price FLOAT64, qty INT64)");
		await E("INSERT INTO `{ds}.t5` VALUES (1,10.0,5),(2,20.0,3)");
		await E("UPDATE `{ds}.t5` SET price = price * 1.1 WHERE qty > 4");
		var v = await S("SELECT price FROM `{ds}.t5` WHERE id = 1");
		Assert.Equal("11", v);
	}
	[Fact] public async Task Update_WithCase()
	{
		await E("CREATE TABLE `{ds}.t6` (id INT64, grade STRING, score INT64)");
		await E("INSERT INTO `{ds}.t6` VALUES (1,'?',90),(2,'?',75),(3,'?',50)");
		await E("UPDATE `{ds}.t6` SET grade = CASE WHEN score >= 80 THEN 'A' WHEN score >= 60 THEN 'B' ELSE 'C' END WHERE true");
		var rows = await Q("SELECT grade FROM `{ds}.t6` ORDER BY id");
		Assert.Equal("A", rows[0]["grade"]?.ToString());
		Assert.Equal("B", rows[1]["grade"]?.ToString());
		Assert.Equal("C", rows[2]["grade"]?.ToString());
	}
	[Fact] public async Task Update_NoMatch()
	{
		await E("CREATE TABLE `{ds}.t7` (id INT64, val INT64)");
		await E("INSERT INTO `{ds}.t7` VALUES (1,100),(2,200)");
		await E("UPDATE `{ds}.t7` SET val = 999 WHERE id = 99");
		var rows = await Q("SELECT val FROM `{ds}.t7` ORDER BY id");
		Assert.Equal("100", rows[0]["val"]?.ToString()); // unchanged
		Assert.Equal("200", rows[1]["val"]?.ToString());
	}

	// ---- DELETE ----
	[Fact] public async Task Delete_SingleRow()
	{
		await E("CREATE TABLE `{ds}.d1` (id INT64, name STRING)");
		await E("INSERT INTO `{ds}.d1` VALUES (1,'Alice'),(2,'Bob'),(3,'Carol')");
		await E("DELETE FROM `{ds}.d1` WHERE id = 2");
		var v = await S("SELECT COUNT(*) FROM `{ds}.d1`");
		Assert.Equal("2", v);
	}
	[Fact] public async Task Delete_MultipleRows()
	{
		await E("CREATE TABLE `{ds}.d2` (id INT64, dept STRING)");
		await E("INSERT INTO `{ds}.d2` VALUES (1,'Eng'),(2,'Eng'),(3,'Sales'),(4,'HR')");
		await E("DELETE FROM `{ds}.d2` WHERE dept = 'Eng'");
		var v = await S("SELECT COUNT(*) FROM `{ds}.d2`");
		Assert.Equal("2", v);
	}
	[Fact] public async Task Delete_AllRows()
	{
		await E("CREATE TABLE `{ds}.d3` (id INT64)");
		await E("INSERT INTO `{ds}.d3` VALUES (1),(2),(3)");
		await E("DELETE FROM `{ds}.d3` WHERE true");
		var v = await S("SELECT COUNT(*) FROM `{ds}.d3`");
		Assert.Equal("0", v);
	}
	[Fact] public async Task Delete_NoMatch()
	{
		await E("CREATE TABLE `{ds}.d4` (id INT64)");
		await E("INSERT INTO `{ds}.d4` VALUES (1),(2),(3)");
		await E("DELETE FROM `{ds}.d4` WHERE id = 99");
		var v = await S("SELECT COUNT(*) FROM `{ds}.d4`");
		Assert.Equal("3", v);
	}
	[Fact] public async Task Delete_WithCondition()
	{
		await E("CREATE TABLE `{ds}.d5` (id INT64, val INT64)");
		await E("INSERT INTO `{ds}.d5` VALUES (1,10),(2,20),(3,30),(4,40),(5,50)");
		await E("DELETE FROM `{ds}.d5` WHERE val > 30");
		var v = await S("SELECT COUNT(*) FROM `{ds}.d5`");
		Assert.Equal("3", v);
	}
	[Fact] public async Task Delete_WithSubquery()
	{
		await E("CREATE TABLE `{ds}.d6` (id INT64, name STRING)");
		await E("INSERT INTO `{ds}.d6` VALUES (1,'Alice'),(2,'Bob'),(3,'Carol')");
		await E("CREATE TABLE `{ds}.exclude` (id INT64)");
		await E("INSERT INTO `{ds}.exclude` VALUES (1),(3)");
		await E("DELETE FROM `{ds}.d6` WHERE id IN (SELECT id FROM `{ds}.exclude`)");
		var rows = await Q("SELECT name FROM `{ds}.d6`");
		Assert.Single(rows);
		Assert.Equal("Bob", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task Delete_WithIn()
	{
		await E("CREATE TABLE `{ds}.d7` (id INT64, status STRING)");
		await E("INSERT INTO `{ds}.d7` VALUES (1,'active'),(2,'inactive'),(3,'active'),(4,'deleted')");
		await E("DELETE FROM `{ds}.d7` WHERE status IN ('inactive', 'deleted')");
		var v = await S("SELECT COUNT(*) FROM `{ds}.d7`");
		Assert.Equal("2", v);
	}

	// ---- INSERT patterns ----
	[Fact] public async Task Insert_Values()
	{
		await E("CREATE TABLE `{ds}.i1` (id INT64, name STRING)");
		await E("INSERT INTO `{ds}.i1` VALUES (1,'Alice'),(2,'Bob')");
		var v = await S("SELECT COUNT(*) FROM `{ds}.i1`");
		Assert.Equal("2", v);
	}
	[Fact] public async Task Insert_Select()
	{
		await E("CREATE TABLE `{ds}.src` (id INT64, name STRING, val INT64)");
		await E("INSERT INTO `{ds}.src` VALUES (1,'Alice',100),(2,'Bob',200)");
		await E("CREATE TABLE `{ds}.dest` (id INT64, name STRING, val INT64)");
		await E("INSERT INTO `{ds}.dest` SELECT * FROM `{ds}.src` WHERE val > 150");
		var rows = await Q("SELECT * FROM `{ds}.dest`");
		Assert.Single(rows);
		Assert.Equal("Bob", rows[0]["name"]?.ToString());
	}
	[Fact] public async Task Insert_WithColumns()
	{
		await E("CREATE TABLE `{ds}.i2` (id INT64, name STRING, val INT64)");
		await E("INSERT INTO `{ds}.i2` (id, name) VALUES (1,'Alice')");
		var rows = await Q("SELECT * FROM `{ds}.i2`");
		Assert.Single(rows);
		Assert.Null(rows[0]["val"]);
	}
}
