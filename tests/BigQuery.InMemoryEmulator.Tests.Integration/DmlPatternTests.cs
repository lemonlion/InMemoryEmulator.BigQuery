using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// INSERT/UPDATE/DELETE/MERGE DML patterns with complex conditions.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DmlPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public DmlPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_dp_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task Exec(string sql)
	{
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null);
	}
	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- INSERT ----
	[Fact] public async Task Insert_SingleRow()
	{
		await Exec("CREATE TABLE `{ds}.t1` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.t1` VALUES (1, 'Alice')");
		Assert.Equal("1", await S("SELECT COUNT(*) FROM `{ds}.t1`"));
	}
	[Fact] public async Task Insert_MultipleRows()
	{
		await Exec("CREATE TABLE `{ds}.t2` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.t2` VALUES (1,'a'),(2,'b'),(3,'c')");
		Assert.Equal("3", await S("SELECT COUNT(*) FROM `{ds}.t2`"));
	}
	[Fact] public async Task Insert_WithNull()
	{
		await Exec("CREATE TABLE `{ds}.t3` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.t3` VALUES (1, NULL)");
		var v = await S("SELECT name FROM `{ds}.t3` WHERE id = 1");
		Assert.Null(v);
	}
	[Fact] public async Task Insert_SelectFrom()
	{
		await Exec("CREATE TABLE `{ds}.src` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.src` VALUES (1,'a'),(2,'b'),(3,'c')");
		await Exec("CREATE TABLE `{ds}.dst` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.dst` SELECT * FROM `{ds}.src` WHERE id > 1");
		Assert.Equal("2", await S("SELECT COUNT(*) FROM `{ds}.dst`"));
	}
	[Fact] public async Task Insert_WithExpr()
	{
		await Exec("CREATE TABLE `{ds}.t4` (id INT64, doubled INT64)");
		await Exec("CREATE TABLE `{ds}.t4src` (id INT64, val INT64)");
		await Exec("INSERT INTO `{ds}.t4src` VALUES (1,10),(2,20)");
		await Exec("INSERT INTO `{ds}.t4` SELECT id, val * 2 AS doubled FROM `{ds}.t4src`");
		Assert.Equal("40", await S("SELECT doubled FROM `{ds}.t4` WHERE id = 2"));
	}

	// ---- UPDATE ----
	[Fact] public async Task Update_SingleColumn()
	{
		await Exec("CREATE TABLE `{ds}.u1` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.u1` VALUES (1,'old')");
		await Exec("UPDATE `{ds}.u1` SET name = 'new' WHERE id = 1");
		Assert.Equal("new", await S("SELECT name FROM `{ds}.u1` WHERE id = 1"));
	}
	[Fact] public async Task Update_MultipleColumns()
	{
		await Exec("CREATE TABLE `{ds}.u2` (id INT64, name STRING, val INT64)");
		await Exec("INSERT INTO `{ds}.u2` VALUES (1,'a',10)");
		await Exec("UPDATE `{ds}.u2` SET name = 'b', val = 20 WHERE id = 1");
		var rows = await Q("SELECT name, val FROM `{ds}.u2` WHERE id = 1");
		Assert.Equal("b", rows[0]["name"]?.ToString());
		Assert.Equal("20", rows[0]["val"]?.ToString());
	}
	[Fact] public async Task Update_AllRows()
	{
		await Exec("CREATE TABLE `{ds}.u3` (id INT64, status STRING)");
		await Exec("INSERT INTO `{ds}.u3` VALUES (1,'active'),(2,'active'),(3,'active')");
		await Exec("UPDATE `{ds}.u3` SET status = 'inactive' WHERE true");
		var v = await S("SELECT COUNT(*) FROM `{ds}.u3` WHERE status = 'inactive'");
		Assert.Equal("3", v);
	}
	[Fact] public async Task Update_WithExpr()
	{
		await Exec("CREATE TABLE `{ds}.u4` (id INT64, val INT64)");
		await Exec("INSERT INTO `{ds}.u4` VALUES (1,10),(2,20),(3,30)");
		await Exec("UPDATE `{ds}.u4` SET val = val * 2 WHERE id = 2");
		Assert.Equal("40", await S("SELECT val FROM `{ds}.u4` WHERE id = 2"));
	}
	[Fact] public async Task Update_NoMatch()
	{
		await Exec("CREATE TABLE `{ds}.u5` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.u5` VALUES (1,'a')");
		await Exec("UPDATE `{ds}.u5` SET name = 'b' WHERE id = 999");
		Assert.Equal("a", await S("SELECT name FROM `{ds}.u5` WHERE id = 1"));
	}
	[Fact] public async Task Update_SetNull()
	{
		await Exec("CREATE TABLE `{ds}.u6` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.u6` VALUES (1,'a')");
		await Exec("UPDATE `{ds}.u6` SET name = NULL WHERE id = 1");
		Assert.Null(await S("SELECT name FROM `{ds}.u6` WHERE id = 1"));
	}

	// ---- DELETE ----
	[Fact] public async Task Delete_SingleRow()
	{
		await Exec("CREATE TABLE `{ds}.d1` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.d1` VALUES (1,'a'),(2,'b'),(3,'c')");
		await Exec("DELETE FROM `{ds}.d1` WHERE id = 2");
		Assert.Equal("2", await S("SELECT COUNT(*) FROM `{ds}.d1`"));
	}
	[Fact] public async Task Delete_MultipleRows()
	{
		await Exec("CREATE TABLE `{ds}.d2` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.d2` VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')");
		await Exec("DELETE FROM `{ds}.d2` WHERE id > 2");
		Assert.Equal("2", await S("SELECT COUNT(*) FROM `{ds}.d2`"));
	}
	[Fact] public async Task Delete_AllRows()
	{
		await Exec("CREATE TABLE `{ds}.d3` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.d3` VALUES (1,'a'),(2,'b')");
		await Exec("DELETE FROM `{ds}.d3` WHERE true");
		Assert.Equal("0", await S("SELECT COUNT(*) FROM `{ds}.d3`"));
	}
	[Fact] public async Task Delete_NoMatch()
	{
		await Exec("CREATE TABLE `{ds}.d4` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.d4` VALUES (1,'a')");
		await Exec("DELETE FROM `{ds}.d4` WHERE id = 999");
		Assert.Equal("1", await S("SELECT COUNT(*) FROM `{ds}.d4`"));
	}
	[Fact] public async Task Delete_WithExpr()
	{
		await Exec("CREATE TABLE `{ds}.d5` (id INT64, val INT64)");
		await Exec("INSERT INTO `{ds}.d5` VALUES (1,10),(2,20),(3,30),(4,40)");
		await Exec("DELETE FROM `{ds}.d5` WHERE val * 2 > 50");
		Assert.Equal("2", await S("SELECT COUNT(*) FROM `{ds}.d5`")); // 10*2=20, 20*2=40 remain
	}

	// ---- MERGE ----
	[Fact] public async Task Merge_InsertOnly()
	{
		await Exec("CREATE TABLE `{ds}.m1` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.m1` VALUES (1,'a')");
		await Exec("CREATE TABLE `{ds}.m1s` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.m1s` VALUES (2,'b'),(3,'c')");
		await Exec(@"MERGE `{ds}.m1` t USING `{ds}.m1s` s ON t.id = s.id
			WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)");
		Assert.Equal("3", await S("SELECT COUNT(*) FROM `{ds}.m1`"));
	}
	[Fact] public async Task Merge_UpdateOnly()
	{
		await Exec("CREATE TABLE `{ds}.m2` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.m2` VALUES (1,'old'),(2,'old')");
		await Exec("CREATE TABLE `{ds}.m2s` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.m2s` VALUES (1,'new1'),(2,'new2')");
		await Exec(@"MERGE `{ds}.m2` t USING `{ds}.m2s` s ON t.id = s.id
			WHEN MATCHED THEN UPDATE SET name = s.name");
		Assert.Equal("new1", await S("SELECT name FROM `{ds}.m2` WHERE id = 1"));
	}
	[Fact] public async Task Merge_DeleteOnly()
	{
		await Exec("CREATE TABLE `{ds}.m3` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.m3` VALUES (1,'a'),(2,'b'),(3,'c')");
		await Exec("CREATE TABLE `{ds}.m3s` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.m3s` VALUES (1,'a'),(2,'b')");
		await Exec(@"MERGE `{ds}.m3` t USING `{ds}.m3s` s ON t.id = s.id
			WHEN MATCHED THEN DELETE");
		Assert.Equal("1", await S("SELECT COUNT(*) FROM `{ds}.m3`"));
	}
	[Fact] public async Task Merge_Upsert()
	{
		await Exec("CREATE TABLE `{ds}.m4` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.m4` VALUES (1,'a')");
		await Exec("CREATE TABLE `{ds}.m4s` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.m4s` VALUES (1,'updated'),(2,'new')");
		await Exec(@"MERGE `{ds}.m4` t USING `{ds}.m4s` s ON t.id = s.id
			WHEN MATCHED THEN UPDATE SET name = s.name
			WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)");
		Assert.Equal("2", await S("SELECT COUNT(*) FROM `{ds}.m4`"));
		Assert.Equal("updated", await S("SELECT name FROM `{ds}.m4` WHERE id = 1"));
	}

	// ---- INSERT then SELECT verify ----
	[Fact] public async Task Insert_VerifyOrder()
	{
		await Exec("CREATE TABLE `{ds}.io1` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.io1` VALUES (3,'c'),(1,'a'),(2,'b')");
		var rows = await Q("SELECT name FROM `{ds}.io1` ORDER BY id");
		Assert.Equal("a", rows[0]["name"]?.ToString());
		Assert.Equal("b", rows[1]["name"]?.ToString());
		Assert.Equal("c", rows[2]["name"]?.ToString());
	}

	// ---- UPDATE with CASE ----
	[Fact] public async Task Update_WithCase()
	{
		await Exec("CREATE TABLE `{ds}.uc1` (id INT64, val INT64, tier STRING)");
		await Exec("INSERT INTO `{ds}.uc1` VALUES (1,90,NULL),(2,50,NULL),(3,20,NULL)");
		await Exec("UPDATE `{ds}.uc1` SET tier = CASE WHEN val >= 80 THEN 'high' WHEN val >= 40 THEN 'mid' ELSE 'low' END WHERE true");
		Assert.Equal("high", await S("SELECT tier FROM `{ds}.uc1` WHERE id = 1"));
		Assert.Equal("mid", await S("SELECT tier FROM `{ds}.uc1` WHERE id = 2"));
		Assert.Equal("low", await S("SELECT tier FROM `{ds}.uc1` WHERE id = 3"));
	}

	// ---- DELETE with subquery ----
	[Fact] public async Task Delete_WithSubquery()
	{
		await Exec("CREATE TABLE `{ds}.ds1` (id INT64, name STRING)");
		await Exec("INSERT INTO `{ds}.ds1` VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')");
		await Exec("DELETE FROM `{ds}.ds1` WHERE id IN (SELECT id FROM `{ds}.ds1` WHERE id > 2)");
		Assert.Equal("2", await S("SELECT COUNT(*) FROM `{ds}.ds1`"));
	}

	// ---- Multiple DML operations on same table ----
	[Fact] public async Task Dml_Sequence()
	{
		await Exec("CREATE TABLE `{ds}.seq` (id INT64, val INT64)");
		await Exec("INSERT INTO `{ds}.seq` VALUES (1,10),(2,20),(3,30)");
		await Exec("UPDATE `{ds}.seq` SET val = val + 5 WHERE id = 1");
		await Exec("DELETE FROM `{ds}.seq` WHERE id = 3");
		await Exec("INSERT INTO `{ds}.seq` VALUES (4,40)");
		Assert.Equal("3", await S("SELECT COUNT(*) FROM `{ds}.seq`"));
		Assert.Equal("15", await S("SELECT val FROM `{ds}.seq` WHERE id = 1"));
	}
}
