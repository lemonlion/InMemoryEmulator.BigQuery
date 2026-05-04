using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class SelectExpressionTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public SelectExpressionTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_sel_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
        var client = await _fixture.GetClientAsync();
        await client.ExecuteQueryAsync($@"CREATE TABLE `{_datasetId}.t` (id INT64, a INT64, b INT64, c STRING, d FLOAT64)", parameters: null);
        await client.ExecuteQueryAsync($@"INSERT INTO `{_datasetId}.t` (id, a, b, c, d) VALUES (1,10,20,'hello',1.5),(2,30,40,'world',2.5),(3,50,60,'foo',3.5),(4,NULL,80,'bar',NULL)", parameters: null);
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
        var rows = await Query(sql);
        return rows.Count > 0 ? rows[0][0]?.ToString() : null;
    }

    [Fact] public async Task Literal_Int() => Assert.Equal("42", await Scalar("SELECT 42"));
    [Fact] public async Task Literal_Float() => Assert.Contains("3.14", (await Scalar("SELECT 3.14"))!);
    [Fact] public async Task Literal_String() => Assert.Equal("hello", await Scalar("SELECT 'hello'"));
    [Fact] public async Task Literal_Bool_True() => Assert.Equal("True", await Scalar("SELECT TRUE"));
    [Fact] public async Task Literal_Bool_False() => Assert.Equal("False", await Scalar("SELECT FALSE"));
    [Fact] public async Task Literal_Null() => Assert.Null(await Scalar("SELECT NULL"));
    [Fact] public async Task Arithmetic_Add() => Assert.Equal("30", await Scalar("SELECT a + b FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task Arithmetic_Sub() => Assert.Equal("-10", await Scalar("SELECT a - b FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task Arithmetic_Mul() => Assert.Equal("200", await Scalar("SELECT a * b FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task Arithmetic_Div() => Assert.Equal("0.5", await Scalar("SELECT a / b FROM `{ds}.t` WHERE id = 1"));  // BigQuery / returns FLOAT64
    [Fact] public async Task Arithmetic_Complex() => Assert.Equal("40", await Scalar("SELECT a + b + a FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task Concat_Operator() => Assert.Equal("hello world", await Scalar("SELECT 'hello' || ' ' || 'world'"));
    [Fact] public async Task Concat_Function() => Assert.Equal("helloworld", await Scalar("SELECT CONCAT('hello', 'world')"));
    [Fact] public async Task ColumnAlias()
    {
        var rows = await Query("SELECT a + b as total FROM `{ds}.t` WHERE id = 1");
        Assert.Equal("30", rows[0]["total"]?.ToString());
    }
    [Fact] public async Task SelectStar()
    {
        var rows = await Query("SELECT * FROM `{ds}.t` WHERE id = 1");
        Assert.Single(rows);
    }
    [Fact] public async Task SelectStar_AllRows()
    {
        var rows = await Query("SELECT * FROM `{ds}.t` ORDER BY id");
        Assert.Equal(4, rows.Count);
    }
    [Fact] public async Task SelectDistinct()
    {
        var rows = await Query("SELECT DISTINCT c FROM `{ds}.t` ORDER BY c");
        Assert.Equal(4, rows.Count);
    }
    [Fact] public async Task Select_Length() => Assert.Equal("5", await Scalar("SELECT LENGTH(c) FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task Select_Upper() => Assert.Equal("HELLO", await Scalar("SELECT UPPER(c) FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task Select_Lower() => Assert.Equal("hello", await Scalar("SELECT LOWER(c) FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task Select_Abs() => Assert.Equal("5", await Scalar("SELECT ABS(-5)"));
    [Fact] public async Task Select_Round()
    {
        var v = double.Parse((await Scalar("SELECT ROUND(d, 1) FROM `{ds}.t` WHERE id = 1"))!);
        Assert.Equal(1.5, v);
    }
    [Fact] public async Task Case_Simple() => Assert.Equal("small", await Scalar("SELECT CASE WHEN a > 20 THEN 'big' ELSE 'small' END FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task Case_MultipleWhen() => Assert.Equal("medium", await Scalar("SELECT CASE WHEN a > 40 THEN 'large' WHEN a > 20 THEN 'medium' ELSE 'small' END FROM `{ds}.t` WHERE id = 2"));
    [Fact] public async Task If_InSelect() => Assert.Equal("yes", await Scalar("SELECT IF(a > 5, 'yes', 'no') FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task Nested_Functions() => Assert.Equal("HELLO", await Scalar("SELECT UPPER(CONCAT(c, '')) FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task Math_Mod() => Assert.Equal("1", await Scalar("SELECT MOD(10, 3)"));
    [Fact] public async Task Math_Pow()
    {
        var v = double.Parse((await Scalar("SELECT POW(2, 3)"))!);
        Assert.Equal(8.0, v);
    }
    [Fact] public async Task Math_Sqrt()
    {
        var v = double.Parse((await Scalar("SELECT SQRT(25)"))!);
        Assert.Equal(5.0, v);
    }
    [Fact] public async Task ConstantExpr() => Assert.Equal("100", await Scalar("SELECT 10 * 10"));
    [Fact] public async Task ConstantExpr_String() => Assert.Equal("ab", await Scalar("SELECT 'a' || 'b'"));
    [Fact] public async Task Coalesce_Basic() => Assert.Equal("10", await Scalar("SELECT COALESCE(a, 0) FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task Coalesce_Null() => Assert.Equal("0", await Scalar("SELECT COALESCE(a, 0) FROM `{ds}.t` WHERE id = 4"));
    [Fact] public async Task Nullif_NotEqual() => Assert.Equal("10", await Scalar("SELECT NULLIF(a, 20) FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task Nullif_Equal() => Assert.Null(await Scalar("SELECT NULLIF(a, 10) FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task Greatest() => Assert.Equal("20", await Scalar("SELECT GREATEST(a, b) FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task Least() => Assert.Equal("10", await Scalar("SELECT LEAST(a, b) FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task Cast_IntToString() => Assert.Equal("10", await Scalar("SELECT CAST(a AS STRING) FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task Cast_StringToInt() => Assert.Equal("42", await Scalar("SELECT CAST('42' AS INT64)"));
    [Fact] public async Task Substr() => Assert.Equal("hel", await Scalar("SELECT SUBSTR(c, 1, 3) FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task Replace_Func() => Assert.Equal("hxllo", await Scalar("SELECT REPLACE(c, 'e', 'x') FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task Trim_Basic() => Assert.Equal("hello", await Scalar("SELECT TRIM('  hello  ')"));
    [Fact] public async Task Reverse_Func() => Assert.Equal("olleh", await Scalar("SELECT REVERSE(c) FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task Lpad() => Assert.Equal("00hello", await Scalar("SELECT LPAD(c, 7, '0') FROM `{ds}.t` WHERE id = 1"));
    [Fact] public async Task Rpad() => Assert.Equal("hello00", await Scalar("SELECT RPAD(c, 7, '0') FROM `{ds}.t` WHERE id = 1"));
}