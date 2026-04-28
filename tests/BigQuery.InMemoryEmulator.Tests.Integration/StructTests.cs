using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for STRUCT creation, access, and usage in queries.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class StructTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public StructTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		return result.ToList();
	}

	private async Task<string?> Scalar(string sql)
	{
		var rows = await Query(sql);
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- STRUCT in UNNEST ----
	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task Struct_UnnestBasic()
	{
		var rows = await Query("SELECT t.name, t.age FROM UNNEST([STRUCT('Alice' AS name, 30 AS age), STRUCT('Bob', 25)]) AS t ORDER BY t.age");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Bob", rows[0]["name"]?.ToString());
		Assert.Equal("25", rows[0]["age"]?.ToString());
		Assert.Equal("Alice", rows[1]["name"]?.ToString());
		Assert.Equal("30", rows[1]["age"]?.ToString());
	}

	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task Struct_ThreeFields()
	{
		var rows = await Query(@"
			SELECT t.a, t.b, t.c FROM
			UNNEST([STRUCT(1 AS a, 'x' AS b, TRUE AS c), STRUCT(2, 'y', FALSE)]) AS t ORDER BY t.a");
		Assert.Equal(2, rows.Count);
		Assert.Equal("1", rows[0]["a"]?.ToString());
		Assert.Equal("x", rows[0]["b"]?.ToString());
		Assert.Equal("True", rows[0]["c"]?.ToString());
	}

	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task Struct_SingleRow()
	{
		var rows = await Query("SELECT t.id, t.val FROM UNNEST([STRUCT(42 AS id, 'hello' AS val)]) AS t");
		Assert.Single(rows);
		Assert.Equal("42", rows[0]["id"]?.ToString());
		Assert.Equal("hello", rows[0]["val"]?.ToString());
	}

	// ---- STRUCT filter ----
	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task Struct_WhereFilter()
	{
		var rows = await Query(@"
			SELECT t.name FROM UNNEST([
				STRUCT('Alice' AS name, 30 AS age),
				STRUCT('Bob', 25),
				STRUCT('Charlie', 35)
			]) AS t WHERE t.age > 28 ORDER BY t.name");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("Charlie", rows[1]["name"]?.ToString());
	}

	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task Struct_WhereString()
	{
		var rows = await Query(@"
			SELECT t.id FROM UNNEST([
				STRUCT(1 AS id, 'active' AS status),
				STRUCT(2, 'inactive'),
				STRUCT(3, 'active')
			]) AS t WHERE t.status = 'active' ORDER BY t.id");
		Assert.Equal(2, rows.Count);
		Assert.Equal("1", rows[0]["id"]?.ToString());
		Assert.Equal("3", rows[1]["id"]?.ToString());
	}

	// ---- STRUCT aggregate ----
	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task Struct_GroupBy()
	{
		var rows = await Query(@"
			SELECT t.category, SUM(t.amount) AS total FROM UNNEST([
				STRUCT('food' AS category, 10 AS amount),
				STRUCT('food', 20),
				STRUCT('transport', 15),
				STRUCT('transport', 25)
			]) AS t GROUP BY t.category ORDER BY t.category");
		Assert.Equal(2, rows.Count);
		Assert.Equal("food", rows[0]["category"]?.ToString());
		Assert.Equal("30", rows[0]["total"]?.ToString());
		Assert.Equal("transport", rows[1]["category"]?.ToString());
		Assert.Equal("40", rows[1]["total"]?.ToString());
	}

	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task Struct_CountPerGroup()
	{
		var rows = await Query(@"
			SELECT t.grp, COUNT(*) AS cnt FROM UNNEST([
				STRUCT('a' AS grp), STRUCT('a'), STRUCT('b'), STRUCT('b'), STRUCT('b')
			]) AS t GROUP BY t.grp ORDER BY t.grp");
		Assert.Equal(2, rows.Count);
		Assert.Equal("a", rows[0]["grp"]?.ToString());
		Assert.Equal("2", rows[0]["cnt"]?.ToString());
		Assert.Equal("b", rows[1]["grp"]?.ToString());
		Assert.Equal("3", rows[1]["cnt"]?.ToString());
	}

	// ---- STRUCT ordering ----
	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task Struct_OrderByField()
	{
		var rows = await Query(@"
			SELECT t.name, t.score FROM UNNEST([
				STRUCT('Alice' AS name, 85 AS score),
				STRUCT('Bob', 92),
				STRUCT('Charlie', 78)
			]) AS t ORDER BY t.score DESC");
		Assert.Equal("Bob", rows[0]["name"]?.ToString());
		Assert.Equal("Alice", rows[1]["name"]?.ToString());
		Assert.Equal("Charlie", rows[2]["name"]?.ToString());
	}

	// ---- STRUCT with expressions ----
	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task Struct_ComputedField()
	{
		var rows = await Query(@"
			SELECT t.name, t.price * t.qty AS total FROM UNNEST([
				STRUCT('Widget' AS name, 10 AS price, 5 AS qty),
				STRUCT('Gadget', 20, 3)
			]) AS t ORDER BY total DESC");
		Assert.Equal("Widget", rows[1]["name"]?.ToString());
		Assert.Equal("Gadget", rows[0]["name"]?.ToString());
	}

	// ---- STRUCT with LIMIT/OFFSET ----
	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task Struct_LimitOffset()
	{
		var rows = await Query(@"
			SELECT t.id FROM UNNEST([
				STRUCT(1 AS id), STRUCT(2), STRUCT(3), STRUCT(4), STRUCT(5)
			]) AS t ORDER BY t.id LIMIT 2 OFFSET 1");
		Assert.Equal(2, rows.Count);
		Assert.Equal("2", rows[0]["id"]?.ToString());
		Assert.Equal("3", rows[1]["id"]?.ToString());
	}

	// ---- STRUCT with DISTINCT ----
	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task Struct_DistinctField()
	{
		var rows = await Query(@"
			SELECT DISTINCT t.category FROM UNNEST([
				STRUCT('food' AS category), STRUCT('food'), STRUCT('transport')
			]) AS t ORDER BY t.category");
		Assert.Equal(2, rows.Count);
	}

	// ---- STRUCT with NULL fields ----
	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task Struct_NullField()
	{
		var rows = await Query(@"
			SELECT t.id, t.name FROM UNNEST([
				STRUCT(1 AS id, 'Alice' AS name),
				STRUCT(2, CAST(NULL AS STRING))
			]) AS t ORDER BY t.id");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Null(rows[1]["name"]);
	}

	// ---- STRUCT with boolean fields ----
	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task Struct_BoolFilter()
	{
		var rows = await Query(@"
			SELECT t.name FROM UNNEST([
				STRUCT('Alice' AS name, TRUE AS active),
				STRUCT('Bob', FALSE),
				STRUCT('Charlie', TRUE)
			]) AS t WHERE t.active ORDER BY t.name");
		Assert.Equal(2, rows.Count);
		Assert.Equal("Alice", rows[0]["name"]?.ToString());
		Assert.Equal("Charlie", rows[1]["name"]?.ToString());
	}

	// ---- STRUCT with string operations ----
	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task Struct_ConcatFields()
	{
		var rows = await Query(@"
			SELECT CONCAT(t.first, ' ', t.last) AS full_name FROM UNNEST([
				STRUCT('John' AS first, 'Doe' AS last),
				STRUCT('Jane', 'Smith')
			]) AS t ORDER BY full_name");
		Assert.Equal("Jane Smith", rows[0]["full_name"]?.ToString());
		Assert.Equal("John Doe", rows[1]["full_name"]?.ToString());
	}

	// ---- Many STRUCTs ----
	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task Struct_ManyRows()
	{
		var v = await Scalar(@"
			SELECT COUNT(*) FROM UNNEST([
				STRUCT(1 AS id), STRUCT(2), STRUCT(3), STRUCT(4), STRUCT(5),
				STRUCT(6), STRUCT(7), STRUCT(8), STRUCT(9), STRUCT(10)
			]) AS t");
		Assert.Equal("10", v);
	}

	// ---- STRUCT with window function ----
	[Fact(Skip = "UNNEST with STRUCT arrays not supported")]
	public async Task Struct_WindowRowNumber()
	{
		var rows = await Query(@"
			SELECT t.name, t.score,
				ROW_NUMBER() OVER (ORDER BY t.score DESC) AS rank
			FROM UNNEST([
				STRUCT('Alice' AS name, 85 AS score),
				STRUCT('Bob', 92),
				STRUCT('Charlie', 78)
			]) AS t ORDER BY rank");
		Assert.Equal("Bob", rows[0]["name"]?.ToString());
		Assert.Equal("1", rows[0]["rank"]?.ToString());
	}
}
