using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// MERGE statement patterns: INSERT, UPDATE, DELETE actions with various conditions.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class MergeStatementCoverageTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public MergeStatementCoverageTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_msc_{Guid.NewGuid():N}"[..30];
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

	// ---- MERGE INSERT only ----
	[Fact] public async Task Merge_InsertOnly()
	{
		await E("CREATE TABLE `{ds}.target` (id INT64, name STRING, val INT64)");
		await E("INSERT INTO `{ds}.target` VALUES (1, 'Alice', 100)");
		await E("CREATE TABLE `{ds}.source` (id INT64, name STRING, val INT64)");
		await E("INSERT INTO `{ds}.source` VALUES (1, 'Alice', 200), (2, 'Bob', 300)");

		await E(@"MERGE `{ds}.target` t USING `{ds}.source` s ON t.id = s.id
			WHEN NOT MATCHED THEN INSERT (id, name, val) VALUES (s.id, s.name, s.val)");

		var rows = await Q("SELECT * FROM `{ds}.target` ORDER BY id");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("100", rows[0]["val"]?.ToString()); // unchanged
		Assert.Equal("Bob", rows[1]["name"]?.ToString());
		Assert.Equal("300", rows[1]["val"]?.ToString());
	}

	// ---- MERGE UPDATE only ----
	[Fact] public async Task Merge_UpdateOnly()
	{
		await E("CREATE TABLE `{ds}.t2` (id INT64, name STRING, val INT64)");
		await E("INSERT INTO `{ds}.t2` VALUES (1, 'Alice', 100), (2, 'Bob', 200)");
		await E("CREATE TABLE `{ds}.s2` (id INT64, name STRING, val INT64)");
		await E("INSERT INTO `{ds}.s2` VALUES (1, 'Alice_Updated', 150), (3, 'Carol', 300)");

		await E(@"MERGE `{ds}.t2` t USING `{ds}.s2` s ON t.id = s.id
			WHEN MATCHED THEN UPDATE SET name = s.name, val = s.val");

		var rows = await Q("SELECT * FROM `{ds}.t2` ORDER BY id");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Alice_Updated", rows[0]["name"]?.ToString());
		Assert.Equal("150", rows[0]["val"]?.ToString());
		Assert.Equal("Bob", rows[1]["name"]?.ToString()); // unchanged
	}

	// ---- MERGE DELETE only ----
	[Fact] public async Task Merge_DeleteOnly()
	{
		await E("CREATE TABLE `{ds}.t3` (id INT64, name STRING, val INT64)");
		await E("INSERT INTO `{ds}.t3` VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Carol', 300)");
		await E("CREATE TABLE `{ds}.s3` (id INT64)");
		await E("INSERT INTO `{ds}.s3` VALUES (1), (3)");

		await E(@"MERGE `{ds}.t3` t USING `{ds}.s3` s ON t.id = s.id
			WHEN MATCHED THEN DELETE");

		var rows = await Q("SELECT * FROM `{ds}.t3` ORDER BY id");
		Assert.Single(rows);
		Assert.Equal("Bob", rows[0]["name"]?.ToString());
	}

	// ---- MERGE INSERT + UPDATE ----
	[Fact] public async Task Merge_InsertAndUpdate()
	{
		await E("CREATE TABLE `{ds}.t4` (id INT64, name STRING, val INT64)");
		await E("INSERT INTO `{ds}.t4` VALUES (1, 'Alice', 100), (2, 'Bob', 200)");
		await E("CREATE TABLE `{ds}.s4` (id INT64, name STRING, val INT64)");
		await E("INSERT INTO `{ds}.s4` VALUES (1, 'Alice_New', 150), (3, 'Carol', 300)");

		await E(@"MERGE `{ds}.t4` t USING `{ds}.s4` s ON t.id = s.id
			WHEN MATCHED THEN UPDATE SET name = s.name, val = s.val
			WHEN NOT MATCHED THEN INSERT (id, name, val) VALUES (s.id, s.name, s.val)");

		var rows = await Q("SELECT * FROM `{ds}.t4` ORDER BY id");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Alice_New", rows[0]["name"]?.ToString());
		Assert.Equal("150", rows[0]["val"]?.ToString());
		Assert.Equal("Bob", rows[1]["name"]?.ToString());
		Assert.Equal("Carol", rows[2]["name"]?.ToString());
	}

	// ---- MERGE UPDATE + DELETE ----
	[Fact] public async Task Merge_UpdateAndDelete()
	{
		await E("CREATE TABLE `{ds}.t5` (id INT64, name STRING, val INT64, active BOOL)");
		await E("INSERT INTO `{ds}.t5` VALUES (1, 'Alice', 100, true), (2, 'Bob', 200, false), (3, 'Carol', 300, true)");
		await E("CREATE TABLE `{ds}.s5` (id INT64, val INT64)");
		await E("INSERT INTO `{ds}.s5` VALUES (1, 150), (2, 250)");

		await E(@"MERGE `{ds}.t5` t USING `{ds}.s5` s ON t.id = s.id
			WHEN MATCHED AND t.active = true THEN UPDATE SET val = s.val
			WHEN MATCHED AND t.active = false THEN DELETE");

		var rows = await Q("SELECT * FROM `{ds}.t5` ORDER BY id");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("150", rows[0]["val"]?.ToString()); // updated
		Assert.Equal("Carol", rows[1]["name"]?.ToString()); // unchanged
	}

	// ---- MERGE all three actions ----
	[Fact] public async Task Merge_AllThreeActions()
	{
		await E("CREATE TABLE `{ds}.t6` (id INT64, name STRING, val INT64)");
		await E("INSERT INTO `{ds}.t6` VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Carol', 300)");
		await E("CREATE TABLE `{ds}.s6` (id INT64, name STRING, val INT64, action STRING)");
		await E("INSERT INTO `{ds}.s6` VALUES (1, 'Alice_New', 150, 'update'), (2, 'Bob', 0, 'delete'), (4, 'Dave', 400, 'insert')");

		await E(@"MERGE `{ds}.t6` t USING `{ds}.s6` s ON t.id = s.id
			WHEN MATCHED AND s.action = 'update' THEN UPDATE SET name = s.name, val = s.val
			WHEN MATCHED AND s.action = 'delete' THEN DELETE
			WHEN NOT MATCHED THEN INSERT (id, name, val) VALUES (s.id, s.name, s.val)");

		var rows = await Q("SELECT * FROM `{ds}.t6` ORDER BY id");
		Assert.Equal(3, rows.Count); // Alice_New, Carol(unchanged), Dave
		Assert.Equal("Alice_New", rows[0]["name"]?.ToString());
		Assert.Equal("Carol", rows[1]["name"]?.ToString());
		Assert.Equal("Dave", rows[2]["name"]?.ToString());
	}

	// ---- MERGE with subquery source ----
	[Fact] public async Task Merge_SubquerySource()
	{
		await E("CREATE TABLE `{ds}.t7` (id INT64, name STRING, val INT64)");
		await E("INSERT INTO `{ds}.t7` VALUES (1, 'Alice', 100), (2, 'Bob', 200)");
		await E("CREATE TABLE `{ds}.raw` (id INT64, name STRING, val INT64)");
		await E("INSERT INTO `{ds}.raw` VALUES (1, 'Alice', 150), (2, 'Bob', 250), (3, 'Carol', 300)");

		await E(@"MERGE `{ds}.t7` t
			USING (SELECT * FROM `{ds}.raw` WHERE val > 200) s
			ON t.id = s.id
			WHEN MATCHED THEN UPDATE SET val = s.val
			WHEN NOT MATCHED THEN INSERT (id, name, val) VALUES (s.id, s.name, s.val)");

		var rows = await Q("SELECT * FROM `{ds}.t7` ORDER BY id");
		Assert.Equal(3, rows.Count);
		Assert.Equal("100", rows[0]["val"]?.ToString()); // Alice not in source (val=150 < 200)
		Assert.Equal("250", rows[1]["val"]?.ToString()); // Bob updated
		Assert.Equal("300", rows[2]["val"]?.ToString()); // Carol inserted
	}

	// ---- MERGE with expression in UPDATE ----
	[Fact] public async Task Merge_UpdateExpression()
	{
		await E("CREATE TABLE `{ds}.t8` (id INT64, name STRING, val INT64)");
		await E("INSERT INTO `{ds}.t8` VALUES (1, 'Alice', 100), (2, 'Bob', 200)");
		await E("CREATE TABLE `{ds}.s8` (id INT64, increment INT64)");
		await E("INSERT INTO `{ds}.s8` VALUES (1, 50), (2, 100)");

		await E(@"MERGE `{ds}.t8` t USING `{ds}.s8` s ON t.id = s.id
			WHEN MATCHED THEN UPDATE SET val = t.val + s.increment");

		var rows = await Q("SELECT * FROM `{ds}.t8` ORDER BY id");
		Assert.Equal("150", rows[0]["val"]?.ToString()); // 100 + 50
		Assert.Equal("300", rows[1]["val"]?.ToString()); // 200 + 100
	}

	// ---- MERGE no match ----
	[Fact] public async Task Merge_NoMatchingRows()
	{
		await E("CREATE TABLE `{ds}.t9` (id INT64, name STRING)");
		await E("INSERT INTO `{ds}.t9` VALUES (1, 'Alice'), (2, 'Bob')");
		await E("CREATE TABLE `{ds}.s9` (id INT64, name STRING)");
		await E("INSERT INTO `{ds}.s9` VALUES (3, 'Carol')");

		await E(@"MERGE `{ds}.t9` t USING `{ds}.s9` s ON t.id = s.id
			WHEN MATCHED THEN UPDATE SET name = s.name
			WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)");

		var rows = await Q("SELECT * FROM `{ds}.t9` ORDER BY id");
		Assert.Equal(3, rows.Count);
		Assert.Equal("Carol", rows[2]["name"]?.ToString());
	}
}
