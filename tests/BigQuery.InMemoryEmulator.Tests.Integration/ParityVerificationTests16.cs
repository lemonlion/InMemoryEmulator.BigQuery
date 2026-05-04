using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Parity verification tests batch 16: Complex DML with FROM, MERGE with DELETE,
/// multi-source INSERT, INFORMATION_SCHEMA-like queries, CREATE TABLE AS SELECT,
/// ALTER TABLE operations, numeric precision edge cases, and complex type casting.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests16 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests16(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv16_{Guid.NewGuid():N}"[..28];
		await _fixture.CreateDatasetAsync(_ds);

		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_ds}.products` (id INT64, name STRING, price FLOAT64, category STRING, active BOOL)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_ds}.products` (id, name, price, category, active) VALUES
			(1, 'Widget A', 9.99, 'Hardware', TRUE),
			(2, 'Widget B', 14.99, 'Hardware', TRUE),
			(3, 'Service X', 99.99, 'Software', TRUE),
			(4, 'Service Y', 149.99, 'Software', FALSE),
			(5, 'Gadget C', 24.99, 'Hardware', TRUE)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try
		{
			var c = await _fixture.GetClientAsync();
			await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true });
		}
		catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null);
		var rows = result.ToList();
		return rows.Count == 0 ? null : rows[0][0]?.ToString();
	}

	private async Task<List<BigQueryRow>> Q(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null);
		return result.ToList();
	}

	private async Task Exec(string sql)
	{
		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// UPDATE with FROM (JOIN)
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#update_with_joins
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Update_WithFrom()
	{
		await Exec(@"
			CREATE TABLE `{ds}.price_updates` (product_id INT64, new_price FLOAT64)");
		await Exec(@"
			INSERT INTO `{ds}.price_updates` (product_id, new_price) VALUES (1, 12.99), (2, 17.99)");
		await Exec(@"
			UPDATE `{ds}.products` p
			SET price = u.new_price
			FROM `{ds}.price_updates` u
			WHERE p.id = u.product_id");
		var rows = await Q("SELECT id, CAST(price AS STRING) AS price FROM `{ds}.products` WHERE id IN (1, 2) ORDER BY id");
		Assert.Equal("12.99", rows[0]["price"]?.ToString());
		Assert.Equal("17.99", rows[1]["price"]?.ToString());
	}

	// ───────────────────────────────────────────────────────────────────────────
	// MERGE with DELETE
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Merge_DeleteWhenMatched()
	{
		await Exec(@"
			CREATE TABLE `{ds}.to_delete` (product_id INT64)");
		await Exec(@"
			INSERT INTO `{ds}.to_delete` (product_id) VALUES (4)");
		await Exec(@"
			MERGE `{ds}.products` p
			USING `{ds}.to_delete` d
			ON p.id = d.product_id
			WHEN MATCHED THEN DELETE");
		var result = await S("SELECT COUNT(*) FROM `{ds}.products`");
		Assert.Equal("4", result); // Was 5, deleted Service Y (id=4)
	}

	// ───────────────────────────────────────────────────────────────────────────
	// MERGE with multiple WHEN clauses
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Merge_MultipleWhenClauses()
	{
		await Exec(@"
			CREATE TABLE `{ds}.sync_data` (id INT64, name STRING, price FLOAT64, category STRING, active BOOL)");
		await Exec(@"
			INSERT INTO `{ds}.sync_data` (id, name, price, category, active) VALUES
			(1, 'Widget A', 11.99, 'Hardware', TRUE),
			(6, 'New Item', 39.99, 'Accessories', TRUE)");
		await Exec(@"
			MERGE `{ds}.products` t
			USING `{ds}.sync_data` s
			ON t.id = s.id
			WHEN MATCHED THEN UPDATE SET price = s.price
			WHEN NOT MATCHED THEN INSERT (id, name, price, category, active) VALUES (s.id, s.name, s.price, s.category, s.active)");
		// id=1 should be updated, id=6 should be inserted
		var updated = await S("SELECT CAST(price AS STRING) FROM `{ds}.products` WHERE id = 1");
		Assert.Equal("11.99", updated);
		var inserted = await S("SELECT name FROM `{ds}.products` WHERE id = 6");
		Assert.Equal("New Item", inserted);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// CREATE TABLE AS SELECT (CTAS)
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_as_select
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task CreateTableAsSelect()
	{
		await Exec(@"
			CREATE TABLE `{ds}.hw_products` AS
			SELECT id, name, price FROM `{ds}.products` WHERE category = 'Hardware'");
		var result = await S("SELECT COUNT(*) FROM `{ds}.hw_products`");
		Assert.Equal("3", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// INSERT INTO...SELECT from another table
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task InsertSelect_FromTable()
	{
		await Exec(@"
			CREATE TABLE `{ds}.archived` (id INT64, name STRING, price FLOAT64, category STRING, active BOOL)");
		await Exec(@"
			INSERT INTO `{ds}.archived`
			SELECT * FROM `{ds}.products` WHERE active = FALSE");
		var result = await S("SELECT COUNT(*) FROM `{ds}.archived`");
		Assert.Equal("1", result); // Only Service Y (active=FALSE)
		var name = await S("SELECT name FROM `{ds}.archived`");
		Assert.Equal("Service Y", name);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Numeric precision with arithmetic
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task FloatArithmetic_Precision()
	{
		// 0.1 + 0.2 should not exhibit significant floating point error
		var result = await S("SELECT CAST(0.1 + 0.2 AS STRING)");
		// BigQuery returns "0.30000000000000004" due to floating point
		Assert.StartsWith("0.3", result!);
	}

	[Fact] public async Task Integer_Overflow()
	{
		// BigQuery INT64 max is 9223372036854775807
		var result = await S("SELECT 9223372036854775807");
		Assert.Equal("9223372036854775807", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// BOOL field operations
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task BoolFilter()
	{
		var result = await S("SELECT COUNT(*) FROM `{ds}.products` WHERE active = TRUE");
		Assert.Equal("4", result);
	}

	[Fact] public async Task BoolFilter_Not()
	{
		var result = await S("SELECT COUNT(*) FROM `{ds}.products` WHERE NOT active");
		Assert.Equal("1", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// String comparison in WHERE
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task StringComparison_Greater()
	{
		// Lexicographic comparison: Widget A, Widget B, Service X, Service Y > 'Service'
		var result = await S("SELECT COUNT(*) FROM `{ds}.products` WHERE name > 'Service'");
		Assert.Equal("4", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Complex expressions in SELECT with table data
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ComputedColumn_WithCase()
	{
		var rows = await Q(@"
			SELECT name, 
				CASE 
					WHEN price < 20 THEN 'cheap'
					WHEN price < 100 THEN 'moderate'
					ELSE 'expensive'
				END AS tier
			FROM `{ds}.products`
			ORDER BY id
			LIMIT 3");
		Assert.Equal("cheap", rows[0]["tier"]?.ToString()); // Widget A: 9.99
		Assert.Equal("cheap", rows[1]["tier"]?.ToString()); // Widget B: 14.99
		Assert.Equal("moderate", rows[2]["tier"]?.ToString()); // Service X: 99.99
	}

	// ───────────────────────────────────────────────────────────────────────────
	// CROSS JOIN with UNNEST
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#cross_join
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task CrossJoin_Unnest()
	{
		var result = await S(@"
			SELECT COUNT(*) FROM `{ds}.products`
			CROSS JOIN UNNEST(['tag1', 'tag2', 'tag3']) AS tag
			WHERE category = 'Hardware'");
		Assert.Equal("9", result); // 3 hardware products × 3 tags
	}

	// ───────────────────────────────────────────────────────────────────────────
	// DELETE with subquery
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Delete_WithSubquery()
	{
		await Exec(@"
			DELETE FROM `{ds}.products`
			WHERE id IN (SELECT x FROM UNNEST([4, 5]) AS x)");
		var result = await S("SELECT COUNT(*) FROM `{ds}.products`");
		Assert.Equal("3", result); // 5 - 2 = 3
	}

	// ───────────────────────────────────────────────────────────────────────────
	// ALTER TABLE ADD COLUMN
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_table_add_column
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task AlterTable_AddColumn()
	{
		await Exec("ALTER TABLE `{ds}.products` ADD COLUMN description STRING");
		await Exec("UPDATE `{ds}.products` SET description = 'A basic widget' WHERE id = 1");
		var result = await S("SELECT description FROM `{ds}.products` WHERE id = 1");
		Assert.Equal("A basic widget", result);
	}
}
