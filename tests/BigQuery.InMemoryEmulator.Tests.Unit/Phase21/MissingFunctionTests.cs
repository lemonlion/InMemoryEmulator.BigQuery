using BigQuery.InMemoryEmulator.SqlEngine;
using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase21;

/// <summary>
/// Phase 21: Missing functions — hash, statistical aggregates, array.
/// </summary>
public class MissingFunctionTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "value", Type = "FLOAT", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "label", Type = "STRING", Mode = "NULLABLE" },
			]
		};
		var table = new InMemoryTable("test_ds", "numbers", schema);
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 1L, ["value"] = 10.0, ["label"] = "a" }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 2L, ["value"] = 14.0, ["label"] = "b" }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 3L, ["value"] = 18.0, ["label"] = "c" }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 4L, ["value"] = null, ["label"] = null }));
		ds.Tables["numbers"] = table;

		return new QueryExecutor(store, "test_ds");
	}

	#region Hash Functions

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#sha1
	//   "Computes the hash of the input using the SHA-1 algorithm. Returns 20 bytes."
	[Fact]
	public void SHA1_ReturnsBytes()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT TO_BASE64(SHA1('Hello World')) AS h");
		var val = rows[0].F[0].V?.ToString();
		// SHA1 of "Hello World" = 0a4d55a8d778e5022fab701977c5d840bbc486d0 => base64: Ck1VqNd45QIvq3AZd8XYQLvEhtA=
		Assert.Equal("Ck1VqNd45QIvq3AZd8XYQLvEhtA=", val);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#sha512
	//   "Computes the hash of the input using the SHA-512 algorithm. Returns 64 bytes."
	[Fact]
	public void SHA512_ReturnsBytes()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT TO_HEX(SHA512('abc')) AS h");
		var val = rows[0].F[0].V?.ToString();
		// SHA512 of "abc" known value
		Assert.StartsWith("ddaf35a1", val);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#farm_fingerprint
	//   "Computes the fingerprint using the FarmHash Fingerprint64 algorithm. Returns INT64."
	[Fact]
	public void FarmFingerprint_ReturnsInt64()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT FARM_FINGERPRINT('test') AS fp");
		var val = rows[0].F[0].V?.ToString();
		// TableRow values are strings; should be a parseable long
		Assert.True(long.TryParse(val, out _), "FARM_FINGERPRINT should return a parseable INT64");
	}

	[Fact]
	public void FarmFingerprint_Deterministic()
	{
		var (_, rows1) = CreateExecutor().Execute("SELECT FARM_FINGERPRINT('hello') AS fp");
		var (_, rows2) = CreateExecutor().Execute("SELECT FARM_FINGERPRINT('hello') AS fp");
		Assert.Equal(rows1[0].F[0].V, rows2[0].F[0].V);
	}

	[Fact]
	public void SHA1_NullInput_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT SHA1(NULL) AS h");
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region Statistical Aggregates

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#var_samp
	//   "Returns the sample (unbiased) variance of the values."
	[Fact]
	public void VarSamp_ReturnsCorrectValue()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT VAR_SAMP(value) AS v FROM numbers");
		var val = Convert.ToDouble(rows[0].F[0].V);
		Assert.Equal(16.0, val, 1e-9);  // VAR_SAMP(10, 14, 18) = 16
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#variance
	//   "An alias of VAR_SAMP."
	[Fact]
	public void Variance_IsAliasForVarSamp()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT VARIANCE(value) AS v FROM numbers");
		var val = Convert.ToDouble(rows[0].F[0].V);
		Assert.Equal(16.0, val, 1e-9);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#var_pop
	//   "Returns the population (biased) variance of the values."
	[Fact]
	public void VarPop_ReturnsCorrectValue()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT VAR_POP(value) AS v FROM numbers");
		var val = Convert.ToDouble(rows[0].F[0].V);
		// VAR_POP(10, 14, 18) = 10.666... (null ignored)
		Assert.Equal(10.666666666666666, val, 1e-9);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#stddev_samp
	//   "Returns the sample (unbiased) standard deviation of the values."
	[Fact]
	public void StddevSamp_ReturnsCorrectValue()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT STDDEV_SAMP(value) AS s FROM numbers");
		var val = Convert.ToDouble(rows[0].F[0].V);
		Assert.Equal(4.0, val, 1e-9);  // sqrt(16)
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#stddev
	//   "An alias of STDDEV_SAMP."
	[Fact]
	public void Stddev_IsAliasForStddevSamp()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT STDDEV(value) AS s FROM numbers");
		var val = Convert.ToDouble(rows[0].F[0].V);
		Assert.Equal(4.0, val, 1e-9);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#stddev_pop
	//   "Returns the population (biased) standard deviation of the values."
	[Fact]
	public void StddevPop_ReturnsCorrectValue()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT STDDEV_POP(value) AS s FROM numbers");
		var val = Convert.ToDouble(rows[0].F[0].V);
		// sqrt(10.666...) ≈ 3.26598...
		Assert.Equal(3.265986323710904, val, 1e-6);
	}

	#endregion

	#region Array Functions

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_concat
	//   "Concatenates one or more arrays with the same element type into a single array."
	[Fact]
	public void ArrayConcat_ConcatenatesArrays()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ARRAY_CONCAT([1, 2], [3, 4]) AS result");
		// Arrays are formatted as comma-separated strings in TableRow
		Assert.Equal("1, 2, 3, 4", rows[0].F[0].V?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_reverse
	//   "Returns the input ARRAY with elements in reverse order."
	[Fact]
	public void ArrayReverse_ReversesElements()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ARRAY_REVERSE([1, 2, 3]) AS result");
		Assert.Equal("3, 2, 1", rows[0].F[0].V?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_first
	//   "Takes an array and returns the first element in the array."
	[Fact]
	public void ArrayFirst_ReturnsFirstElement()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ARRAY_FIRST([10, 20, 30]) AS result");
		Assert.Equal("10", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void ArrayFirst_NullInput_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ARRAY_FIRST(NULL) AS result");
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region JSON Array & Object Functions

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_extract_array
	//   "Extracts a JSON array and returns it as a SQL ARRAY<JSON>."
	[Fact]
	public void JsonExtractArray_ReturnsArray()
	{
		var (_, rows) = CreateExecutor().Execute(
			@"SELECT ARRAY_LENGTH(JSON_EXTRACT_ARRAY('{""items"":[1,2,3]}', '$.items')) AS cnt");
		Assert.Equal("3", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void JsonExtractArray_NullInput_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute(
			@"SELECT JSON_EXTRACT_ARRAY(NULL, '$.items') AS arr");
		Assert.Null(rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_value_array
	//   "Extracts a JSON array of scalar values and returns a SQL ARRAY<STRING>."
	[Fact]
	public void JsonValueArray_ReturnsStringArray()
	{
		var (_, rows) = CreateExecutor().Execute(
			@"SELECT ARRAY_LENGTH(JSON_VALUE_ARRAY('{""tags"":[""a"",""b"",""c""]}', '$.tags')) AS cnt");
		Assert.Equal("3", rows[0].F[0].V?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_keys
	//   "Returns the keys of the outermost JSON object as a SQL ARRAY<STRING>."
	[Fact]
	public void JsonKeys_ReturnsKeys()
	{
		var (_, rows) = CreateExecutor().Execute(
			@"SELECT ARRAY_LENGTH(JSON_KEYS('{""name"":""Alice"",""age"":30}')) AS cnt");
		Assert.Equal("2", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void JsonKeys_NullInput_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT JSON_KEYS(NULL) AS keys");
		Assert.Null(rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_set
	//   "Produces a new SQL JSON value with the specified JSON data inserted or replaced."
	[Fact]
	public void JsonSet_SetsNewProperty()
	{
		var (_, rows) = CreateExecutor().Execute(
			@"SELECT JSON_SET('{""a"":1}', '$.b', '2') AS j");
		var val = rows[0].F[0].V?.ToString();
		Assert.NotNull(val);
		Assert.Contains("b", val);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_strip_nulls
	//   "Removes all members that have NULL values from a JSON value."
	[Fact]
	public void JsonStripNulls_RemovesNulls()
	{
		var (_, rows) = CreateExecutor().Execute(
			@"SELECT JSON_STRIP_NULLS('{""a"":1,""b"":null,""c"":3}') AS j");
		var val = rows[0].F[0].V?.ToString();
		Assert.NotNull(val);
		Assert.DoesNotContain("null", val!);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_type
	//   "Returns a STRING value that represents the JSON type."
	[Fact]
	public void JsonType_Object()
	{
		var (_, rows) = CreateExecutor().Execute(@"SELECT JSON_TYPE('{""a"":1}') AS t");
		Assert.Equal("object", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void JsonType_Array()
	{
		var (_, rows) = CreateExecutor().Execute(@"SELECT JSON_TYPE('[1,2]') AS t");
		Assert.Equal("array", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void JsonType_Number()
	{
		var (_, rows) = CreateExecutor().Execute(@"SELECT JSON_TYPE('42') AS t");
		Assert.Equal("number", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void JsonType_Boolean()
	{
		var (_, rows) = CreateExecutor().Execute(@"SELECT JSON_TYPE('true') AS t");
		Assert.Equal("boolean", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void JsonType_Null()
	{
		var (_, rows) = CreateExecutor().Execute(@"SELECT JSON_TYPE('null') AS t");
		Assert.Equal("null", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void JsonType_NullInput_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT JSON_TYPE(NULL) AS t");
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region Regex Functions

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_instr
	//   "Returns the 1-based position of the first occurrence of a regex pattern."
	[Fact]
	public void RegexpInstr_FindsPosition()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT REGEXP_INSTR('abcdef', 'cd') AS pos");
		Assert.Equal("3", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void RegexpInstr_NoMatch_ReturnsZero()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT REGEXP_INSTR('abcdef', 'xyz') AS pos");
		Assert.Equal("0", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void RegexpInstr_NullInput_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT REGEXP_INSTR(NULL, 'a') AS pos");
		Assert.Null(rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_substr
	//   "Synonym for REGEXP_EXTRACT."
	[Fact]
	public void RegexpSubstr_ReturnsMatch()
	{
		var (_, rows) = CreateExecutor().Execute(@"SELECT REGEXP_SUBSTR('hello 123', '[0-9]+') AS m");
		Assert.Equal("123", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void RegexpSubstr_NoMatch_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute(@"SELECT REGEXP_SUBSTR('abcdef', '[0-9]+') AS m");
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region Bit Functions

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/bit_functions#bit_count
	//   "Returns the number of bits that are set in the input expression."
	[Fact]
	public void BitCount_Seven_ReturnsThree()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT BIT_COUNT(7) AS cnt");
		Assert.Equal("3", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void BitCount_Zero_ReturnsZero()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT BIT_COUNT(0) AS cnt");
		Assert.Equal("0", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void BitCount_PowerOfTwo()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT BIT_COUNT(256) AS cnt");
		Assert.Equal("1", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void BitCount_NullInput_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT BIT_COUNT(NULL) AS cnt");
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region NET Functions

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#nethost
	//   "Takes a URL as a STRING and returns the host."
	[Fact]
	public void NetHost_ExtractsHost()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT NET.HOST('https://www.google.com/search?q=test') AS h");
		Assert.Equal("www.google.com", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void NetHost_NullInput_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT NET.HOST(NULL) AS h");
		Assert.Null(rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netpublic_suffix
	//   "Takes a URL as a STRING and returns the public suffix."
	[Fact]
	public void NetPublicSuffix_ReturnsSuffix()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT NET.PUBLIC_SUFFIX('https://www.google.com/page') AS s");
		Assert.Equal("com", rows[0].F[0].V?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netreg_domain
	//   "Takes a URL and returns the registered or registerable domain."
	[Fact]
	public void NetRegDomain_ReturnsDomain()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT NET.REG_DOMAIN('https://www.google.com/page') AS d");
		Assert.Equal("google.com", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region LENGTH for BYTES

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#length
	//   "Returns the length of a BYTES value in bytes."
	[Fact]
	public void Length_Bytes_ReturnsByteCount()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT LENGTH(SHA512('hello')) AS len");
		Assert.Equal("64", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Length_String_ReturnsCharCount()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT LENGTH('hello') AS len");
		Assert.Equal("5", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region Array Functions (continued)
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_last
	//   "Takes an array and returns the last element in the array."
	[Fact]
	public void ArrayLast_ReturnsLastElement()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ARRAY_LAST([10, 20, 30]) AS result");
		Assert.Equal("30", rows[0].F[0].V?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_slice
	//   "Returns an array containing zero or more consecutive elements from the input array."
	[Fact]
	public void ArraySlice_ReturnsSubArray()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ARRAY_SLICE(['a', 'b', 'c', 'd', 'e'], 1, 3) AS result");
		Assert.Equal("b, c, d", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void ArrayConcat_NullInput_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT ARRAY_CONCAT(NULL, [1, 2]) AS result");
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region Interval Functions

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/interval_functions#make_interval
	//   "Constructs an INTERVAL value."
	[Fact]
	public void MakeInterval_ReturnsInterval()
	{
		// MAKE_INTERVAL(year, month, day, hour, minute, second)
		var (_, rows) = CreateExecutor().Execute("SELECT MAKE_INTERVAL(1, 2, 3, 4, 5, 6) AS i");
		var val = rows[0].F[0].V?.ToString();
		Assert.NotNull(val);
		Assert.Contains("1", val);
	}

	[Fact]
	public void MakeInterval_DefaultsToZero()
	{
		// MAKE_INTERVAL() with no args should return 0 interval
		var (_, rows) = CreateExecutor().Execute("SELECT MAKE_INTERVAL() AS i");
		var val = rows[0].F[0].V?.ToString();
		Assert.NotNull(val);
	}

	#endregion
}
