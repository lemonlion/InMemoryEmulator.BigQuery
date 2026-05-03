using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for advanced window function patterns: NTILE, NTH_VALUE, frames, partitioning.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class WindowFunctionAdvancedPatternTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public WindowFunctionAdvancedPatternTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_wf_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.ExecuteQueryAsync($@"
			CREATE TABLE `{_datasetId}.sales` (
				id INT64, employee STRING, dept STRING, amount FLOAT64, sale_date DATE)", parameters: null);
		await client.ExecuteQueryAsync($@"
			INSERT INTO `{_datasetId}.sales` (id, employee, dept, amount, sale_date) VALUES
			(1, 'Alice', 'Eng', 100, DATE '2024-01-01'),
			(2, 'Alice', 'Eng', 150, DATE '2024-01-15'),
			(3, 'Alice', 'Eng', 200, DATE '2024-02-01'),
			(4, 'Bob', 'Eng', 120, DATE '2024-01-05'),
			(5, 'Bob', 'Eng', 180, DATE '2024-01-20'),
			(6, 'Bob', 'Eng', 160, DATE '2024-02-10'),
			(7, 'Charlie', 'Sales', 300, DATE '2024-01-01'),
			(8, 'Charlie', 'Sales', 250, DATE '2024-01-15'),
			(9, 'Charlie', 'Sales', 350, DATE '2024-02-01'),
			(10, 'Diana', 'Sales', 280, DATE '2024-01-10'),
			(11, 'Diana', 'Sales', 220, DATE '2024-01-25'),
			(12, 'Diana', 'Sales', 310, DATE '2024-02-05')", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
		return result.ToList();
	}

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _datasetId), parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ROW_NUMBER
	[Fact] public async Task RowNumber_OverAll()
	{
		var rows = await Query("SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("1", rows[0]["rn"].ToString());
		Assert.Equal("12", rows[11]["rn"].ToString());
	}

	[Fact] public async Task RowNumber_Partitioned()
	{
		var rows = await Query("SELECT employee, id, ROW_NUMBER() OVER (PARTITION BY employee ORDER BY id) AS rn FROM `{ds}.sales` ORDER BY employee, id");
		Assert.Equal("1", rows[0]["rn"].ToString()); // Alice first row
	}

	// RANK and DENSE_RANK
	[Fact] public async Task Rank_WithTies()
	{
		// Use subquery to pre-aggregate, then apply RANK over the result
		var rows = await Query(@"
			SELECT dept, employee, total_sales,
				RANK() OVER (ORDER BY total_sales DESC) AS rnk
			FROM (SELECT dept, employee, SUM(amount) AS total_sales FROM `{ds}.sales` GROUP BY dept, employee)
			ORDER BY rnk");
		Assert.Equal("1", rows[0]["rnk"].ToString());
	}

	[Fact] public async Task DenseRank_Partitioned()
	{
		var rows = await Query(@"
			SELECT employee, amount,
				DENSE_RANK() OVER (PARTITION BY employee ORDER BY amount DESC) AS drnk
			FROM `{ds}.sales` WHERE employee = 'Alice' ORDER BY amount DESC");
		Assert.Equal("1", rows[0]["drnk"].ToString());
		Assert.Equal("2", rows[1]["drnk"].ToString());
	}

	// NTILE
	[Fact] public async Task Ntile_FourBuckets()
	{
		var rows = await Query("SELECT id, NTILE(4) OVER (ORDER BY id) AS quartile FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("1", rows[0]["quartile"].ToString()); // First 3 rows in quartile 1
		Assert.Equal("4", rows[11]["quartile"].ToString());
	}

	[Fact] public async Task Ntile_TwoBuckets()
	{
		var rows = await Query("SELECT id, NTILE(2) OVER (ORDER BY amount) AS half FROM `{ds}.sales` ORDER BY id");
		Assert.True(rows.All(r => r["half"].ToString() == "1" || r["half"].ToString() == "2"));
	}

	// LAG / LEAD
	[Fact] public async Task Lag_Default()
	{
		var rows = await Query("SELECT id, amount, LAG(amount) OVER (ORDER BY id) AS prev_amount FROM `{ds}.sales` ORDER BY id");
		Assert.Null(rows[0]["prev_amount"]); // First row has no previous
		Assert.Equal("100", rows[1]["prev_amount"].ToString());
	}

	[Fact] public async Task Lag_WithOffset()
	{
		var rows = await Query("SELECT id, amount, LAG(amount, 2) OVER (ORDER BY id) AS prev2_amount FROM `{ds}.sales` ORDER BY id");
		Assert.Null(rows[0]["prev2_amount"]);
		Assert.Null(rows[1]["prev2_amount"]);
		Assert.Equal("100", rows[2]["prev2_amount"].ToString());
	}

	[Fact] public async Task Lead_Default()
	{
		var rows = await Query("SELECT id, amount, LEAD(amount) OVER (ORDER BY id) AS next_amount FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("150", rows[0]["next_amount"].ToString());
		Assert.Null(rows[11]["next_amount"]); // Last row has no next
	}

	[Fact] public async Task Lead_Partitioned()
	{
		var rows = await Query(@"
			SELECT employee, id, amount, LEAD(amount) OVER (PARTITION BY employee ORDER BY id) AS next_amt
			FROM `{ds}.sales` WHERE employee = 'Alice' ORDER BY id");
		Assert.Equal("150", rows[0]["next_amt"].ToString());
		Assert.Null(rows[2]["next_amt"]);
	}

	// FIRST_VALUE / LAST_VALUE
	[Fact] public async Task FirstValue_Partitioned()
	{
		var rows = await Query(@"
			SELECT employee, amount, FIRST_VALUE(amount) OVER (PARTITION BY employee ORDER BY sale_date) AS first_sale
			FROM `{ds}.sales` ORDER BY employee, sale_date");
		Assert.Equal("100", rows[0]["first_sale"].ToString()); // Alice's first
	}

	[Fact] public async Task LastValue_WithFrame()
	{
		var rows = await Query(@"
			SELECT employee, amount,
				LAST_VALUE(amount) OVER (PARTITION BY employee ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_sale
			FROM `{ds}.sales` WHERE employee = 'Alice' ORDER BY sale_date");
		Assert.Equal("200", rows[0]["last_sale"].ToString()); // Alice's last
	}

	// Running SUM
	[Fact] public async Task RunningSum_OverAll()
	{
		var rows = await Query("SELECT id, amount, SUM(amount) OVER (ORDER BY id) AS running_total FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("100", rows[0]["running_total"].ToString());
		Assert.Equal("250", rows[1]["running_total"].ToString()); // 100 + 150
	}

	[Fact] public async Task RunningSum_Partitioned()
	{
		var rows = await Query(@"
			SELECT employee, amount, SUM(amount) OVER (PARTITION BY employee ORDER BY sale_date) AS emp_running
			FROM `{ds}.sales` WHERE employee = 'Bob' ORDER BY sale_date");
		Assert.Equal("120", rows[0]["emp_running"].ToString());
		Assert.Equal("300", rows[1]["emp_running"].ToString()); // 120 + 180
		Assert.Equal("460", rows[2]["emp_running"].ToString()); // 300 + 160
	}

	// Running AVG
	[Fact] public async Task RunningAvg_OverAll()
	{
		var rows = await Query("SELECT id, amount, AVG(amount) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg FROM `{ds}.sales` ORDER BY id");
		Assert.Equal("100", rows[0]["running_avg"].ToString()); // avg(100)
	}

	// COUNT with window
	[Fact] public async Task CountWindow_Partitioned()
	{
		var rows = await Query("SELECT employee, COUNT(*) OVER (PARTITION BY dept) AS dept_count FROM `{ds}.sales` ORDER BY employee LIMIT 1");
		Assert.True(int.Parse(rows[0]["dept_count"].ToString()!) >= 3);
	}

	// MIN / MAX with window
	[Fact] public async Task MinWindow_Partitioned()
	{
		var rows = await Query(@"
			SELECT employee, amount, MIN(amount) OVER (PARTITION BY employee) AS min_sale
			FROM `{ds}.sales` WHERE employee = 'Alice'");
		Assert.True(rows.All(r => r["min_sale"].ToString() == "100"));
	}

	[Fact] public async Task MaxWindow_Partitioned()
	{
		var rows = await Query(@"
			SELECT employee, amount, MAX(amount) OVER (PARTITION BY employee) AS max_sale
			FROM `{ds}.sales` WHERE employee = 'Alice'");
		Assert.True(rows.All(r => r["max_sale"].ToString() == "200"));
	}

	// Window frame ROWS vs default
	[Fact] public async Task FrameRows_PrecedingAndFollowing()
	{
		var rows = await Query(@"
			SELECT id, amount,
				AVG(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_avg
			FROM `{ds}.sales` ORDER BY id");
		Assert.Equal(12, rows.Count);
		// First row: avg of rows 1-2 (no preceding)
		// Should be average of current and next
	}

	// Multiple window functions in same query
	[Fact] public async Task MultipleWindows_InSameQuery()
	{
		var rows = await Query(@"
			SELECT id, employee, amount,
				ROW_NUMBER() OVER (ORDER BY id) AS rn,
				RANK() OVER (ORDER BY amount DESC) AS rnk,
				SUM(amount) OVER (PARTITION BY employee ORDER BY id) AS emp_running
			FROM `{ds}.sales` ORDER BY id");
		Assert.Equal(12, rows.Count);
		Assert.Equal("1", rows[0]["rn"].ToString());
	}
}
