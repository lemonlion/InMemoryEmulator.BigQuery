using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Deep integration tests for DML operations: INSERT, UPDATE, DELETE, MERGE edge cases.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DmlDeepTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public DmlDeepTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_dml_deep_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();

		// Main data table
		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "value", Type = "FLOAT", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "category", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "active", Type = "BOOLEAN", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "created", Type = "DATE", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "items", schema);
		await client.InsertRowsAsync(_datasetId, "items", new[]
		{
			new BigQueryInsertRow("r1") { ["id"] = 1, ["name"] = "Alpha", ["value"] = 10.5, ["category"] = "A", ["active"] = true, ["created"] = "2024-01-01" },
			new BigQueryInsertRow("r2") { ["id"] = 2, ["name"] = "Beta", ["value"] = 20.0, ["category"] = "B", ["active"] = true, ["created"] = "2024-02-15" },
			new BigQueryInsertRow("r3") { ["id"] = 3, ["name"] = "Gamma", ["value"] = 30.7, ["category"] = "A", ["active"] = false, ["created"] = "2024-03-20" },
			new BigQueryInsertRow("r4") { ["id"] = 4, ["name"] = "Delta", ["value"] = 40.2, ["category"] = "B", ["active"] = true, ["created"] = "2024-04-10" },
			new BigQueryInsertRow("r5") { ["id"] = 5, ["name"] = "Epsilon", ["value"] = 50.9, ["category"] = "C", ["active"] = false, ["created"] = "2024-05-05" },
		});

		// Target table for INSERT INTO ... SELECT
		await client.CreateTableAsync(_datasetId, "target", schema);

		// Merge source
		var mergeSchema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "value", Type = "FLOAT", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "category", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "active", Type = "BOOLEAN", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "created", Type = "DATE", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "merge_source", mergeSchema);
		await client.InsertRowsAsync(_datasetId, "merge_source", new[]
		{
			new BigQueryInsertRow("ms1") { ["id"] = 3, ["name"] = "Gamma Updated", ["value"] = 33.3, ["category"] = "A", ["active"] = true, ["created"] = "2024-03-20" },
			new BigQueryInsertRow("ms2") { ["id"] = 6, ["name"] = "Zeta", ["value"] = 60.0, ["category"] = "D", ["active"] = true, ["created"] = "2024-06-01" },
			new BigQueryInsertRow("ms3") { ["id"] = 7, ["name"] = "Eta", ["value"] = 70.0, ["category"] = "D", ["active"] = true, ["created"] = "2024-07-01" },
		});

		// Wide table for type-specific inserts
		var wideSchema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "str_col", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "int_col", Type = "INTEGER", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "float_col", Type = "FLOAT", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "bool_col", Type = "BOOLEAN", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "date_col", Type = "DATE", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "ts_col", Type = "TIMESTAMP", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "bytes_col", Type = "BYTES", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "numeric_col", Type = "NUMERIC", Mode = "NULLABLE" },
			]
		};
		await client.CreateTableAsync(_datasetId, "wide", wideSchema);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<BigQueryClient> Client() => await _fixture.GetClientAsync();

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await Client();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		return result.ToList();
	}

	private async Task Exec(string sql)
	{
		var client = await Client();
		await client.ExecuteQueryAsync(sql, parameters: null);
	}

	private async Task<long> Count(string table) =>
		Convert.ToInt64((await Query($"SELECT COUNT(*) AS c FROM `{_datasetId}.{table}`"))[0]["c"]);

	// ============================================================
	// INSERT VALUES - Various Types and Patterns
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#insert_statement
	[Fact]
	public async Task Insert_AllColumnTypes()
	{
		await Exec($"INSERT INTO `{_datasetId}.wide` (id, str_col, int_col, float_col, bool_col, date_col, ts_col, numeric_col) VALUES (1, 'hello', 42, 3.14, TRUE, '2024-01-01', '2024-01-01T00:00:00Z', 123.456)");
		var rows = await Query($"SELECT * FROM `{_datasetId}.wide` WHERE id = 1");
		Assert.Single(rows);
		Assert.Equal("hello", (string)rows[0]["str_col"]);
		Assert.Equal(42L, Convert.ToInt64(rows[0]["int_col"]));
	}

	[Fact]
	public async Task Insert_NullValues()
	{
		await Exec($"INSERT INTO `{_datasetId}.wide` (id, str_col, int_col, float_col, bool_col) VALUES (2, NULL, NULL, NULL, NULL)");
		var rows = await Query($"SELECT str_col, int_col, float_col, bool_col FROM `{_datasetId}.wide` WHERE id = 2");
		Assert.Single(rows);
		Assert.Null(rows[0]["str_col"]);
		Assert.Null(rows[0]["int_col"]);
	}

	[Fact]
	public async Task Insert_MultipleRows_SingleStatement()
	{
		await Exec($"INSERT INTO `{_datasetId}.wide` (id, str_col) VALUES (10, 'a'), (11, 'b'), (12, 'c')");
		var count = await Count("wide");
		Assert.True(count >= 3);
	}

	[Fact]
	public async Task Insert_ExplicitColumnSubset()
	{
		await Exec($"INSERT INTO `{_datasetId}.wide` (id, str_col) VALUES (20, 'partial')");
		var rows = await Query($"SELECT * FROM `{_datasetId}.wide` WHERE id = 20");
		Assert.Single(rows);
		Assert.Equal("partial", (string)rows[0]["str_col"]);
		Assert.Null(rows[0]["int_col"]);
	}

	[Fact]
	public async Task Insert_ExpressionValues()
	{
		await Exec($"INSERT INTO `{_datasetId}.wide` (id, str_col, int_col) VALUES (30, CONCAT('hello', ' world'), 1 + 2 + 3)");
		var rows = await Query($"SELECT str_col, int_col FROM `{_datasetId}.wide` WHERE id = 30");
		Assert.Single(rows);
		Assert.Equal("hello world", (string)rows[0]["str_col"]);
		Assert.Equal(6L, Convert.ToInt64(rows[0]["int_col"]));
	}

	[Fact]
	public async Task Insert_FunctionInValues()
	{
		await Exec($"INSERT INTO `{_datasetId}.wide` (id, str_col, date_col) VALUES (31, UPPER('test'), CURRENT_DATE())");
		var rows = await Query($"SELECT str_col FROM `{_datasetId}.wide` WHERE id = 31");
		Assert.Single(rows);
		Assert.Equal("TEST", (string)rows[0]["str_col"]);
	}

	// ============================================================
	// INSERT SELECT - Various Sources
	// ============================================================

	[Fact]
	public async Task InsertSelect_WithWhere()
	{
		await Exec($"INSERT INTO `{_datasetId}.target` SELECT * FROM `{_datasetId}.items` WHERE category = 'A'");
		var rows = await Query($"SELECT COUNT(*) AS c FROM `{_datasetId}.target` WHERE category = 'A'");
		Assert.Equal(2L, Convert.ToInt64(rows[0]["c"]));
	}

	[Fact]
	public async Task InsertSelect_WithAggregation()
	{
		await Exec($@"INSERT INTO `{_datasetId}.target` (id, name, value, category, active, created)
			SELECT 100, category, SUM(value), category, TRUE, '2024-12-31'
			FROM `{_datasetId}.items`
			GROUP BY category");
		var count = await Count("target");
		Assert.True(count >= 3); // 3 categories
	}

	[Fact]
	public async Task InsertSelect_WithOrderByLimit()
	{
		await Exec($@"INSERT INTO `{_datasetId}.target`
			SELECT * FROM `{_datasetId}.items` ORDER BY value DESC LIMIT 2");
		var rows = await Query($"SELECT COUNT(*) AS c FROM `{_datasetId}.target`");
		Assert.True(Convert.ToInt64(rows[0]["c"]) >= 2);
	}

	// ============================================================
	// UPDATE - Various Patterns
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#update_statement
	[Fact]
	public async Task Update_SingleColumn_Literal()
	{
		await Exec($"UPDATE `{_datasetId}.items` SET name = 'Modified' WHERE id = 1");
		var rows = await Query($"SELECT name FROM `{_datasetId}.items` WHERE id = 1");
		Assert.Equal("Modified", (string)rows[0]["name"]);
	}

	[Fact]
	public async Task Update_MultipleColumns()
	{
		await Exec($"UPDATE `{_datasetId}.items` SET name = 'XX', value = 99.9, active = FALSE WHERE id = 2");
		var rows = await Query($"SELECT name, value, active FROM `{_datasetId}.items` WHERE id = 2");
		Assert.Equal("XX", (string)rows[0]["name"]);
	}

	[Fact]
	public async Task Update_WithArithmeticExpression()
	{
		await Exec($"UPDATE `{_datasetId}.items` SET value = value * 2 WHERE id = 1");
		var rows = await Query($"SELECT value FROM `{_datasetId}.items` WHERE id = 1");
		Assert.Equal(21.0, Convert.ToDouble(rows[0]["value"]), 1);
	}

	[Fact]
	public async Task Update_WithStringFunction()
	{
		await Exec($"UPDATE `{_datasetId}.items` SET name = UPPER(name) WHERE category = 'A'");
		var rows = await Query($"SELECT name FROM `{_datasetId}.items` WHERE id = 1");
		Assert.Equal("ALPHA", (string)rows[0]["name"]);
	}

	[Fact]
	public async Task Update_WithCaseExpression()
	{
		await Exec($@"UPDATE `{_datasetId}.items` SET category = 
			CASE WHEN value < 25 THEN 'LOW' WHEN value < 45 THEN 'MID' ELSE 'HIGH' END
			WHERE TRUE");
		var rows = await Query($"SELECT category FROM `{_datasetId}.items` WHERE id = 5");
		Assert.Equal("HIGH", (string)rows[0]["category"]);
	}

	[Fact]
	public async Task Update_SetToNull()
	{
		await Exec($"UPDATE `{_datasetId}.items` SET name = NULL WHERE id = 3");
		var rows = await Query($"SELECT name FROM `{_datasetId}.items` WHERE id = 3");
		Assert.Null(rows[0]["name"]);
	}

	[Fact]
	public async Task Update_WhereWithAnd()
	{
		await Exec($"UPDATE `{_datasetId}.items` SET value = 0 WHERE category = 'A' AND active = TRUE");
		var rows = await Query($"SELECT value FROM `{_datasetId}.items` WHERE id = 1");
		Assert.Equal(0.0, Convert.ToDouble(rows[0]["value"]), 1);
	}

	[Fact]
	public async Task Update_WhereWithOr()
	{
		await Exec($"UPDATE `{_datasetId}.items` SET active = FALSE WHERE id = 1 OR id = 2");
		var rows = await Query($"SELECT COUNT(*) AS c FROM `{_datasetId}.items` WHERE active = FALSE");
		Assert.True(Convert.ToInt64(rows[0]["c"]) >= 2);
	}

	[Fact]
	public async Task Update_WhereIn()
	{
		await Exec($"UPDATE `{_datasetId}.items` SET category = 'X' WHERE id IN (1, 3, 5)");
		var rows = await Query($"SELECT COUNT(*) AS c FROM `{_datasetId}.items` WHERE category = 'X'");
		Assert.Equal(3L, Convert.ToInt64(rows[0]["c"]));
	}

	[Fact]
	public async Task Update_WhereInSubquery()
	{
		await Exec($@"UPDATE `{_datasetId}.items` SET active = FALSE 
			WHERE id IN (SELECT id FROM `{_datasetId}.items` WHERE value > 30)");
		var rows = await Query($"SELECT COUNT(*) AS c FROM `{_datasetId}.items` WHERE active = FALSE AND value > 30");
		Assert.True(Convert.ToInt64(rows[0]["c"]) >= 2);
	}

	[Fact]
	public async Task Update_AllRows()
	{
		await Exec($"UPDATE `{_datasetId}.items` SET category = 'ALL' WHERE TRUE");
		var rows = await Query($"SELECT DISTINCT category FROM `{_datasetId}.items`");
		Assert.Single(rows);
		Assert.Equal("ALL", (string)rows[0]["category"]);
	}

	[Fact]
	public async Task Update_NoMatch_ZeroRows()
	{
		await Exec($"UPDATE `{_datasetId}.items` SET value = 999 WHERE id = 9999");
		var rows = await Query($"SELECT COUNT(*) AS c FROM `{_datasetId}.items` WHERE value = 999");
		Assert.Equal(0L, Convert.ToInt64(rows[0]["c"]));
	}

	// ============================================================
	// DELETE - Various Patterns
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#delete_statement
	[Fact]
	public async Task Delete_SimpleWhere()
	{
		await Exec($"DELETE FROM `{_datasetId}.items` WHERE id = 5");
		var rows = await Query($"SELECT COUNT(*) AS c FROM `{_datasetId}.items` WHERE id = 5");
		Assert.Equal(0L, Convert.ToInt64(rows[0]["c"]));
	}

	[Fact]
	public async Task Delete_ComplexWhere()
	{
		await Exec($"DELETE FROM `{_datasetId}.items` WHERE category = 'B' AND active = TRUE");
		var rows = await Query($"SELECT COUNT(*) AS c FROM `{_datasetId}.items` WHERE category = 'B' AND active = TRUE");
		Assert.Equal(0L, Convert.ToInt64(rows[0]["c"]));
	}

	[Fact]
	public async Task Delete_WhereInSubquery()
	{
		await Exec($@"DELETE FROM `{_datasetId}.items` 
			WHERE id IN (SELECT id FROM `{_datasetId}.items` WHERE value < 25)");
		var rows = await Query($"SELECT COUNT(*) AS c FROM `{_datasetId}.items` WHERE value < 25");
		Assert.Equal(0L, Convert.ToInt64(rows[0]["c"]));
	}

	[Fact]
	public async Task Delete_AllRows()
	{
		await Exec($"DELETE FROM `{_datasetId}.items` WHERE TRUE");
		Assert.Equal(0L, await Count("items"));
	}

	[Fact]
	public async Task Delete_NoMatch()
	{
		var before = await Count("items");
		await Exec($"DELETE FROM `{_datasetId}.items` WHERE id = 99999");
		Assert.Equal(before, await Count("items"));
	}

	[Fact]
	public async Task Delete_WithBetween()
	{
		await Exec($"DELETE FROM `{_datasetId}.items` WHERE value BETWEEN 20 AND 40");
		var rows = await Query($"SELECT COUNT(*) AS c FROM `{_datasetId}.items` WHERE value BETWEEN 20 AND 40");
		Assert.Equal(0L, Convert.ToInt64(rows[0]["c"]));
	}

	[Fact]
	public async Task Delete_WithLike()
	{
		await Exec($"DELETE FROM `{_datasetId}.items` WHERE name LIKE 'A%'");
		var rows = await Query($"SELECT COUNT(*) AS c FROM `{_datasetId}.items` WHERE name LIKE 'A%'");
		Assert.Equal(0L, Convert.ToInt64(rows[0]["c"]));
	}

	// ============================================================
	// MERGE - Various Patterns
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement
	[Fact]
	public async Task Merge_MatchedUpdate_NotMatchedInsert()
	{
		await Exec($@"
			MERGE `{_datasetId}.items` AS T
			USING `{_datasetId}.merge_source` AS S
			ON T.id = S.id
			WHEN MATCHED THEN UPDATE SET name = S.name, value = S.value
			WHEN NOT MATCHED THEN INSERT (id, name, value, category, active, created) VALUES (S.id, S.name, S.value, S.category, S.active, S.created)");
		// id=3 should be updated
		var rows = await Query($"SELECT name FROM `{_datasetId}.items` WHERE id = 3");
		Assert.Equal("Gamma Updated", (string)rows[0]["name"]);
		// id=6 should be inserted
		var rows2 = await Query($"SELECT name FROM `{_datasetId}.items` WHERE id = 6");
		Assert.Single(rows2);
		Assert.Equal("Zeta", (string)rows2[0]["name"]);
	}

	[Fact]
	public async Task Merge_MatchedDelete()
	{
		await Exec($@"
			MERGE `{_datasetId}.items` AS T
			USING `{_datasetId}.merge_source` AS S
			ON T.id = S.id
			WHEN MATCHED THEN DELETE");
		var rows = await Query($"SELECT COUNT(*) AS c FROM `{_datasetId}.items` WHERE id = 3");
		Assert.Equal(0L, Convert.ToInt64(rows[0]["c"]));
	}

	[Fact]
	public async Task Merge_MatchedWithCondition()
	{
		await Exec($@"
			MERGE `{_datasetId}.items` AS T
			USING `{_datasetId}.merge_source` AS S
			ON T.id = S.id
			WHEN MATCHED AND S.value > 50 THEN UPDATE SET value = S.value
			WHEN MATCHED AND S.value <= 50 THEN UPDATE SET name = S.name");
		// id=3 source value is 33.3, so should update name
		var rows = await Query($"SELECT name, value FROM `{_datasetId}.items` WHERE id = 3");
		Assert.Equal("Gamma Updated", (string)rows[0]["name"]);
	}

	[Fact]
	public async Task Merge_NotMatchedOnly()
	{
		var before = await Count("items");
		await Exec($@"
			MERGE `{_datasetId}.items` AS T
			USING `{_datasetId}.merge_source` AS S
			ON T.id = S.id
			WHEN NOT MATCHED THEN INSERT (id, name, value, category, active, created) VALUES (S.id, S.name, S.value, S.category, S.active, S.created)");
		var after = await Count("items");
		Assert.True(after > before);
	}

	[Fact]
	public async Task Merge_SourceFromSubquery()
	{
		await Exec($@"
			MERGE `{_datasetId}.items` AS T
			USING (SELECT 99 AS id, 'New' AS name, 99.9 AS value, 'Z' AS category, TRUE AS active, DATE '2024-12-01' AS created) AS S
			ON T.id = S.id
			WHEN NOT MATCHED THEN INSERT (id, name, value, category, active, created) VALUES (S.id, S.name, S.value, S.category, S.active, S.created)");
		var rows = await Query($"SELECT name FROM `{_datasetId}.items` WHERE id = 99");
		Assert.Single(rows);
		Assert.Equal("New", (string)rows[0]["name"]);
	}

	[Fact]
	public async Task Merge_AllRowsMatch_NoInsert()
	{
		await Exec($@"
			MERGE `{_datasetId}.items` AS T
			USING `{_datasetId}.items` AS S
			ON T.id = S.id
			WHEN MATCHED THEN UPDATE SET value = S.value + 1
			WHEN NOT MATCHED THEN INSERT (id, name, value, category, active, created) VALUES (S.id, S.name, S.value, S.category, S.active, S.created)");
		// All rows should have value incremented
		var rows = await Query($"SELECT value FROM `{_datasetId}.items` WHERE id = 1");
		Assert.Equal(11.5, Convert.ToDouble(rows[0]["value"]), 1);
	}

	// ============================================================
	// UPDATE with FROM clause (JOIN update)
	// ============================================================

	[Fact]
	public async Task Update_WithFromJoin()
	{
		await Exec($@"UPDATE `{_datasetId}.items` AS T
			SET T.name = S.name
			FROM `{_datasetId}.merge_source` AS S
			WHERE T.id = S.id");
		var rows = await Query($"SELECT name FROM `{_datasetId}.items` WHERE id = 3");
		Assert.Equal("Gamma Updated", (string)rows[0]["name"]);
	}

	// ============================================================
	// INSERT with column reordering
	// ============================================================

	[Fact]
	public async Task Insert_ColumnsInDifferentOrder()
	{
		await Exec($"INSERT INTO `{_datasetId}.wide` (str_col, id, int_col) VALUES ('reorder', 40, 99)");
		var rows = await Query($"SELECT str_col, int_col FROM `{_datasetId}.wide` WHERE id = 40");
		Assert.Single(rows);
		Assert.Equal("reorder", (string)rows[0]["str_col"]);
		Assert.Equal(99L, Convert.ToInt64(rows[0]["int_col"]));
	}

	// ============================================================
	// INSERT with duplicate rows (BQ allows duplicates)
	// ============================================================

	[Fact]
	public async Task Insert_DuplicateIds_Allowed()
	{
		await Exec($"INSERT INTO `{_datasetId}.wide` (id, str_col) VALUES (50, 'dup1')");
		await Exec($"INSERT INTO `{_datasetId}.wide` (id, str_col) VALUES (50, 'dup2')");
		var rows = await Query($"SELECT COUNT(*) AS c FROM `{_datasetId}.wide` WHERE id = 50");
		Assert.Equal(2L, Convert.ToInt64(rows[0]["c"]));
	}

	// ============================================================
	// UPDATE with subquery in SET
	// ============================================================

	[Fact]
	public async Task Update_SetToSubqueryResult()
	{
		await Exec($@"UPDATE `{_datasetId}.items` SET value = 
			(SELECT MAX(value) FROM `{_datasetId}.items`) WHERE id = 1");
		var rows = await Query($"SELECT value FROM `{_datasetId}.items` WHERE id = 1");
		Assert.Equal(50.9, Convert.ToDouble(rows[0]["value"]), 1);
	}

	// ============================================================
	// DELETE with EXISTS subquery
	// ============================================================

	[Fact]
	public async Task Delete_WithExists()
	{
		await Exec($@"DELETE FROM `{_datasetId}.items` WHERE EXISTS 
			(SELECT 1 FROM `{_datasetId}.merge_source` WHERE merge_source.id = items.id)");
		var rows = await Query($"SELECT COUNT(*) AS c FROM `{_datasetId}.items` WHERE id = 3");
		Assert.Equal(0L, Convert.ToInt64(rows[0]["c"]));
	}

	// ============================================================
	// INSERT SELECT with UNION ALL source
	// ============================================================

	[Fact]
	public async Task InsertSelect_FromUnionAll()
	{
		await Exec($@"INSERT INTO `{_datasetId}.target`
			SELECT * FROM `{_datasetId}.items` WHERE id = 1
			UNION ALL
			SELECT * FROM `{_datasetId}.items` WHERE id = 2");
		var count = await Count("target");
		Assert.True(count >= 2);
	}

	// ============================================================
	// UPDATE incrementing numeric value
	// ============================================================

	[Fact]
	public async Task Update_IncrementValue()
	{
		var before = (await Query($"SELECT value FROM `{_datasetId}.items` WHERE id = 4"))[0]["value"];
		await Exec($"UPDATE `{_datasetId}.items` SET value = value + 10 WHERE id = 4");
		var after = (await Query($"SELECT value FROM `{_datasetId}.items` WHERE id = 4"))[0]["value"];
		Assert.Equal(Convert.ToDouble(before) + 10, Convert.ToDouble(after), 1);
	}

	// ============================================================
	// INSERT with CAST
	// ============================================================

	[Fact]
	public async Task Insert_WithCast()
	{
		await Exec($"INSERT INTO `{_datasetId}.wide` (id, int_col, float_col) VALUES (60, CAST('123' AS INT64), CAST('3.14' AS FLOAT64))");
		var rows = await Query($"SELECT int_col, float_col FROM `{_datasetId}.wide` WHERE id = 60");
		Assert.Equal(123L, Convert.ToInt64(rows[0]["int_col"]));
	}

	// ============================================================
	// MERGE with multiple WHEN clauses
	// ============================================================

	[Fact]
	public async Task Merge_MultipleMatchedClauses()
	{
		await Exec($@"
			MERGE `{_datasetId}.items` AS T
			USING `{_datasetId}.merge_source` AS S
			ON T.id = S.id
			WHEN MATCHED AND S.value > 60 THEN DELETE
			WHEN MATCHED THEN UPDATE SET name = CONCAT(T.name, '_merged')
			WHEN NOT MATCHED THEN INSERT (id, name, value, category, active, created) VALUES (S.id, S.name, S.value, S.category, S.active, S.created)");
		// id=3 source value=33.3 <= 60, should be updated
		var rows = await Query($"SELECT name FROM `{_datasetId}.items` WHERE id = 3");
		Assert.Contains("_merged", (string)rows[0]["name"]);
	}

	// ============================================================
	// Verify row counts after operations
	// ============================================================

	[Fact]
	public async Task Delete_VerifyRemainingRows()
	{
		var before = await Count("items");
		await Exec($"DELETE FROM `{_datasetId}.items` WHERE id = 1");
		Assert.Equal(before - 1, await Count("items"));
	}

	[Fact]
	public async Task Update_DoesNotChangeRowCount()
	{
		var before = await Count("items");
		await Exec($"UPDATE `{_datasetId}.items` SET value = 0 WHERE TRUE");
		Assert.Equal(before, await Count("items"));
	}
}
