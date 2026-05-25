using Google.Cloud.BigQuery.V2;
using Xunit;

namespace InMemoryEmulator.BigQuery.Tests.Integration;

/// <summary>
/// Parity verification tests batch 40: FormatValue double overflow for large doubles,
/// hash functions (MD5/SHA256/SHA512/SHA1) with BYTES input, ARRAY_AGG DISTINCT preserving NULLs.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests40 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public ParityVerificationTests40(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
	}

	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string> ScalarAsync(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var row = result.First();
		return row[0]?.ToString() ?? "NULL";
	}

	// ============================================================
	// FormatValue double overflow for large floating-point values
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#floating_point_type
	//   "FLOAT64 values represent approximate numeric values with fractional components."
	//   Values > long.MaxValue should still be correctly represented.
	[Fact]
	public async Task LargeFloat64_1e19_FormatsCorrectly()
	{
		// 1e19 > long.MaxValue (9.2e18) — should not overflow to garbage
		var result = await ScalarAsync("SELECT 1e19");
		var parsed = double.Parse(result, System.Globalization.CultureInfo.InvariantCulture);
		Assert.Equal(1e19, parsed);
	}

	[Fact]
	public async Task LargeFloat64_1e20_FormatsCorrectly()
	{
		var result = await ScalarAsync("SELECT 1e20");
		var parsed = double.Parse(result, System.Globalization.CultureInfo.InvariantCulture);
		Assert.Equal(1e20, parsed);
	}

	[Fact]
	public async Task LargeNegativeFloat64_FormatsCorrectly()
	{
		// -1e19 < long.MinValue — should not overflow
		var result = await ScalarAsync("SELECT -1e19");
		var parsed = double.Parse(result, System.Globalization.CultureInfo.InvariantCulture);
		Assert.Equal(-1e19, parsed);
	}

	[Fact]
	public async Task LargeFloat64_Multiplication_FormatsCorrectly()
	{
		// 1e18 * 100 = 1e20 — computed at runtime
		var result = await ScalarAsync("SELECT 1e18 * 100");
		var parsed = double.Parse(result, System.Globalization.CultureInfo.InvariantCulture);
		Assert.Equal(1e20, parsed);
	}

	// ============================================================
	// Hash functions with BYTES input (via FROM_BASE64)
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#md5
	//   "The input can either be STRING or BYTES. The string version treats the input as an array of bytes."
	[Fact]
	public async Task Md5_StringInput()
	{
		// MD5('hello') = XUFAKrxLKna5cZ2REBfFkg==
		var result = await ScalarAsync("SELECT TO_BASE64(MD5('hello'))");
		Assert.Equal("XUFAKrxLKna5cZ2REBfFkg==", result);
	}

	[Fact]
	public async Task Md5_BytesInput_ViaFromBase64()
	{
		// FROM_BASE64('aGVsbG8=') = bytes of 'hello'
		// MD5 of those bytes should be the same as MD5('hello')
		var result = await ScalarAsync("SELECT TO_BASE64(MD5(FROM_BASE64('aGVsbG8=')))");
		Assert.Equal("XUFAKrxLKna5cZ2REBfFkg==", result);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#sha256
	//   "The input can either be STRING or BYTES."
	[Fact]
	public async Task Sha256_StringInput()
	{
		var result = await ScalarAsync("SELECT TO_BASE64(SHA256('hello'))");
		Assert.Equal("LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=", result);
	}

	[Fact]
	public async Task Sha256_BytesInput_ViaFromBase64()
	{
		var result = await ScalarAsync("SELECT TO_BASE64(SHA256(FROM_BASE64('aGVsbG8=')))");
		Assert.Equal("LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=", result);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#sha512
	//   "The input can either be STRING or BYTES."
	[Fact]
	public async Task Sha512_BytesInput_ViaFromBase64()
	{
		var result = await ScalarAsync("SELECT TO_BASE64(SHA512(FROM_BASE64('aGVsbG8=')))");
		Assert.Equal("m3HSJL1i83hdltRq0+o9czGb+8KJDKra4t/3JRlnPKcjI8PZm6XBHXx6zG4UuMXaDEZjR1wuXDre9G9zvN7AQw==", result);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#sha1
	//   "The input can either be STRING or BYTES."
	[Fact]
	public async Task Sha1_BytesInput_ViaFromBase64()
	{
		var result = await ScalarAsync("SELECT TO_BASE64(SHA1(FROM_BASE64('aGVsbG8=')))");
		Assert.Equal("qvTGHdzF6KLavt4PO0gs2a6pQ00=", result);
	}

	[Fact]
	public async Task Md5_BytesInput_DifferentContent()
	{
		// FROM_BASE64('AQID') = bytes [1, 2, 3] (not a valid UTF-8 string)
		// This tests that we're hashing actual bytes, not "System.Byte[]" or UTF-8 decode/re-encode
		var result = await ScalarAsync("SELECT TO_BASE64(MD5(FROM_BASE64('AQID')))");
		// MD5 of [1, 2, 3] = computed value
		var expected = Convert.ToBase64String(
			System.Security.Cryptography.MD5.HashData(new byte[] { 1, 2, 3 }));
		Assert.Equal(expected, result);
	}

	// ============================================================
	// ARRAY_AGG DISTINCT preserves NULLs (one NULL kept)
	// ============================================================

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#array_agg
	//   "DISTINCT: Each distinct value of expression is aggregated only once into the result."
	//   NULLs are treated as equal values for DISTINCT purposes; one NULL is preserved.
	// GO emulator bug: ARRAY_AGG rejects NULL values instead of handling them per BigQuery spec.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task ArrayAgg_Distinct_PreservesOneNull()
	{
		// [1, 1, NULL, 2, NULL] → DISTINCT keeps [1, 2, NULL] → ARRAY_LENGTH = 3
		var result = await ScalarAsync(
			"SELECT ARRAY_LENGTH(ARRAY_AGG(DISTINCT x)) FROM UNNEST([1, 1, CAST(NULL AS INT64), 2, CAST(NULL AS INT64)]) AS x");
		Assert.Equal("3", result);
	}

	[Fact]
	public async Task ArrayAgg_Distinct_IgnoreNulls_ExcludesNulls()
	{
		// With IGNORE NULLS, NULLs are excluded even with DISTINCT
		var result = await ScalarAsync(
			"SELECT ARRAY_LENGTH(ARRAY_AGG(DISTINCT x IGNORE NULLS)) FROM UNNEST([1, 1, CAST(NULL AS INT64), 2, CAST(NULL AS INT64)]) AS x");
		Assert.Equal("2", result);
	}

	// GO emulator bug: ARRAY_AGG rejects NULL values instead of handling them per BigQuery spec.
	[Trait(TestTraits.Target, TestTraits.EmulatorDivergence)]
	[Fact]
	public async Task ArrayAgg_Distinct_StringValues()
	{
		// Test DISTINCT with string values including NULL
		var result = await ScalarAsync(
			"SELECT ARRAY_LENGTH(ARRAY_AGG(DISTINCT x)) FROM UNNEST(['a', 'b', 'a', CAST(NULL AS STRING), 'b', CAST(NULL AS STRING)]) AS x");
		Assert.Equal("3", result);
	}

	// ============================================================
	// COUNT(DISTINCT) correctly excludes NULLs (existing behavior verification)
	// ============================================================

	[Fact]
	public async Task Count_Distinct_ExcludesNulls()
	{
		// COUNT(DISTINCT x) with NULLs — NULLs are never counted
		var result = await ScalarAsync(
			"SELECT COUNT(DISTINCT x) FROM UNNEST([1, 1, CAST(NULL AS INT64), 2, CAST(NULL AS INT64)]) AS x");
		Assert.Equal("2", result);
	}
}
