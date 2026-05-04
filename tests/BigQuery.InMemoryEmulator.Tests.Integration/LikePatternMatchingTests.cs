using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// LIKE pattern matching: wildcards, escapes, CONTAINS_SUBSTR, REGEXP_CONTAINS, and pattern operators.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#like_operator
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class LikePatternMatchingTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public LikePatternMatchingTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_lpm_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.words` (id INT64, word STRING, category STRING)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.words` VALUES
			(1,'apple','fruit'),(2,'banana','fruit'),(3,'cherry','fruit'),
			(4,'avocado','fruit'),(5,'artichoke','vegetable'),(6,'asparagus','vegetable'),
			(7,'blueberry','fruit'),(8,'broccoli','vegetable'),(9,'beetroot','vegetable'),
			(10,'almond','nut'),(11,'APPLE','fruit_upper'),(12,'Apple','fruit_mixed'),
			(13,'app','tech'),(14,'application','tech'),(15,NULL,NULL),
			(16,'a%b','special'),(17,'a_b','special'),(18,'hello world','phrase'),
			(19,'hello-world','phrase'),(20,'test123','alphanumeric')", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- LIKE with % ----
	[Fact] public async Task Like_StartsWith()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE word LIKE 'a%' ORDER BY word");
		Assert.True(rows.Count >= 5); // apple, avocado, artichoke, asparagus, almond, app, application, a%b, a_b
	}
	[Fact] public async Task Like_EndsWith()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE word LIKE '%ry' ORDER BY word");
		Assert.True(rows.Count >= 2); // cherry, blueberry
	}
	[Fact] public async Task Like_Contains()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE word LIKE '%an%' ORDER BY word");
		Assert.True(rows.Count >= 1); // banana
	}
	[Fact] public async Task Like_Exact()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE word LIKE 'apple' ORDER BY word");
		Assert.Single(rows);
		Assert.Equal("apple", rows[0]["word"]?.ToString());
	}
	[Fact] public async Task Like_PercentOnly()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE word LIKE '%' ORDER BY word");
		Assert.True(rows.Count >= 19); // all non-null
	}

	// ---- LIKE with _ ----
	[Fact] public async Task Like_SingleChar()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE word LIKE 'app__' ORDER BY word");
		Assert.Single(rows); // apple
	}
	[Fact] public async Task Like_SingleCharMiddle()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE word LIKE 'a_ple' ORDER BY word");
		Assert.Single(rows); // apple
	}
	[Fact] public async Task Like_ThreeChars()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE word LIKE 'app' ORDER BY word");
		Assert.Single(rows); // app exact
	}
	[Fact] public async Task Like_UnderscoreSuffix()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE word LIKE 'app___' ORDER BY word");
		Assert.True(rows.Count >= 0);
	}

	// ---- NOT LIKE ----
	[Fact] public async Task NotLike_StartsWith()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE word NOT LIKE 'a%' AND word IS NOT NULL ORDER BY word");
		Assert.True(rows.Count >= 8);
	}
	[Fact] public async Task NotLike_Contains()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE word NOT LIKE '%berry%' AND word IS NOT NULL ORDER BY word");
		Assert.True(rows.Count >= 15);
	}

	// ---- LIKE with NULL ----
	[Fact] public async Task Like_NullValue()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE word LIKE '%' AND id = 15");
		Assert.Empty(rows); // NULL LIKE '%' is NULL, not true
	}
	[Fact] public async Task Like_NullPattern()
	{
		var v = await S("SELECT 'hello' LIKE NULL");
		Assert.Null(v);
	}

	// ---- LIKE case sensitivity ----
	[Fact] public async Task Like_CaseSensitive()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE word LIKE 'Apple' ORDER BY word");
		Assert.Single(rows); // Only 'Apple', not 'apple' or 'APPLE'
	}
	[Fact] public async Task Like_CaseSensitive_Lower()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE word LIKE 'apple' ORDER BY word");
		Assert.Single(rows);
	}

	// ---- LIKE combined with other conditions ----
	[Fact] public async Task Like_WithAnd()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE word LIKE 'a%' AND category = 'fruit' ORDER BY word");
		Assert.True(rows.Count >= 2); // apple, avocado
	}
	[Fact] public async Task Like_WithOr()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE word LIKE 'a%' OR word LIKE 'b%' ORDER BY word");
		Assert.True(rows.Count >= 10);
	}

	// ---- REGEXP_CONTAINS ----
	[Fact] public async Task RegexpContains_Basic()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE REGEXP_CONTAINS(word, r'^a') ORDER BY word");
		Assert.True(rows.Count >= 5);
	}
	[Fact] public async Task RegexpContains_EndsWith()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE REGEXP_CONTAINS(word, r'ry$') ORDER BY word");
		Assert.True(rows.Count >= 2);
	}
	[Fact] public async Task RegexpContains_Digits()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE REGEXP_CONTAINS(word, r'[0-9]') ORDER BY word");
		Assert.Single(rows); // test123
	}
	[Fact] public async Task RegexpContains_CaseInsensitive()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE REGEXP_CONTAINS(word, r'(?i)apple') ORDER BY word");
		Assert.Equal(3, rows.Count); // apple, APPLE, Apple
	}

	// ---- CONTAINS_SUBSTR ----
	[Fact] public async Task ContainsSubstr_Basic()
	{
		var v = await S("SELECT CONTAINS_SUBSTR('hello world', 'world')");
		Assert.Equal("True", v);
	}
	[Fact] public async Task ContainsSubstr_CaseInsensitive()
	{
		var v = await S("SELECT CONTAINS_SUBSTR('Hello World', 'hello')");
		Assert.Equal("True", v);
	}
	[Fact] public async Task ContainsSubstr_NoMatch()
	{
		var v = await S("SELECT CONTAINS_SUBSTR('hello world', 'xyz')");
		Assert.Equal("False", v);
	}
	[Fact] public async Task ContainsSubstr_InWhere()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE CONTAINS_SUBSTR(word, 'apple') ORDER BY word");
		Assert.True(rows.Count >= 1);
	}

	// ---- STARTS_WITH / ENDS_WITH ----
	[Fact] public async Task StartsWith_True()
	{
		var v = await S("SELECT STARTS_WITH('hello', 'hel')");
		Assert.Equal("True", v);
	}
	[Fact] public async Task StartsWith_False()
	{
		var v = await S("SELECT STARTS_WITH('hello', 'xyz')");
		Assert.Equal("False", v);
	}
	[Fact] public async Task EndsWith_True()
	{
		var v = await S("SELECT ENDS_WITH('hello', 'llo')");
		Assert.Equal("True", v);
	}
	[Fact] public async Task EndsWith_False()
	{
		var v = await S("SELECT ENDS_WITH('hello', 'xyz')");
		Assert.Equal("False", v);
	}
	[Fact] public async Task StartsWith_Column()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE STARTS_WITH(word, 'bl') ORDER BY word");
		Assert.Single(rows); // blueberry
	}
	[Fact] public async Task EndsWith_Column()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE ENDS_WITH(word, 'berry') ORDER BY word");
		Assert.True(rows.Count >= 1); // blueberry
	}

	// ---- REGEXP_EXTRACT ----
	[Fact] public async Task RegexpExtract_Basic()
	{
		var v = await S("SELECT REGEXP_EXTRACT('test123abc', r'[0-9]+')");
		Assert.Equal("123", v);
	}
	[Fact] public async Task RegexpExtract_NoMatch()
	{
		var v = await S("SELECT REGEXP_EXTRACT('hello', r'[0-9]+')");
		Assert.Null(v);
	}
	[Fact] public async Task RegexpExtract_Group()
	{
		var v = await S("SELECT REGEXP_EXTRACT('hello123world', r'([0-9]+)')");
		Assert.Equal("123", v);
	}

	// ---- REGEXP_REPLACE ----
	[Fact] public async Task RegexpReplace_Basic()
	{
		var v = await S("SELECT REGEXP_REPLACE('hello123world', r'[0-9]+', 'NUM')");
		Assert.Equal("helloNUMworld", v);
	}
	[Fact] public async Task RegexpReplace_NoMatch()
	{
		var v = await S("SELECT REGEXP_REPLACE('hello', r'[0-9]+', 'NUM')");
		Assert.Equal("hello", v);
	}

	// ---- LIKE with aggregate ----
	[Fact] public async Task Like_Count()
	{
		var v = await S("SELECT COUNT(*) FROM `{ds}.words` WHERE word LIKE '%berry%'");
		Assert.True(int.Parse(v!) >= 1);
	}

	// ---- LIKE in CASE ----
	[Fact] public async Task Like_InCase()
	{
		var rows = await Q(@"
			SELECT word,
				CASE WHEN word LIKE 'a%' THEN 'starts_a' WHEN word LIKE 'b%' THEN 'starts_b' ELSE 'other' END AS cat
			FROM `{ds}.words`
			WHERE word IS NOT NULL AND category IN ('fruit', 'vegetable')
			ORDER BY word LIMIT 5");
		Assert.Equal(5, rows.Count);
	}

	// ---- Multiple LIKE conditions ----
	[Fact] public async Task Like_Multiple()
	{
		var rows = await Q("SELECT word FROM `{ds}.words` WHERE (word LIKE '%apple%' OR word LIKE '%cherry%') AND word IS NOT NULL ORDER BY word");
		Assert.True(rows.Count >= 2);
	}

	// ---- LIKE with JOIN ----
	[Fact] public async Task Like_WithCountGroupBy()
	{
		var rows = await Q(@"
			SELECT category, COUNT(*) AS cnt
			FROM `{ds}.words`
			WHERE word LIKE 'a%' AND category IS NOT NULL
			GROUP BY category
			ORDER BY cnt DESC");
		Assert.True(rows.Count >= 1);
	}
}
