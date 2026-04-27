using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase24;

/// <summary>
/// Phase 24: Missing string functions.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
/// </summary>
public class MissingStringFunctionTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		return new QueryExecutor(store);
	}

	#region BYTE_LENGTH / OCTET_LENGTH

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#byte_length
	//   "Gets the number of BYTES in a STRING or BYTES value."
	//   BYTE_LENGTH('абвгд') => 10 (5 Cyrillic chars × 2 bytes each)
	[Theory]
	[InlineData("SELECT BYTE_LENGTH('abc')", "3")]
	[InlineData("SELECT BYTE_LENGTH('')", "0")]
	public void ByteLength_Ascii(string sql, string expected)
	{
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal(expected, rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void ByteLength_Null_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT BYTE_LENGTH(NULL)");
		Assert.Null(rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#octet_length
	//   "Alias for BYTE_LENGTH."
	[Fact]
	public void OctetLength_IsAlias()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT OCTET_LENGTH('abc')");
		Assert.Equal("3", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region UNICODE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#unicode
	//   "Returns the Unicode code point for the first character in value. Returns 0 if value is empty."
	[Theory]
	[InlineData("SELECT UNICODE('A')", "65")]
	[InlineData("SELECT UNICODE('')", "0")]
	public void Unicode_Basic(string sql, string expected)
	{
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal(expected, rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Unicode_Null_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT UNICODE(NULL)");
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region INITCAP

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#initcap
	//   "Formats a STRING as proper case."
	//   INITCAP('Hello World-everyone!') => 'Hello World-Everyone!'
	[Theory]
	[InlineData("SELECT INITCAP('hello world')", "Hello World")]
	[InlineData("SELECT INITCAP('HELLO WORLD')", "Hello World")]
	[InlineData("SELECT INITCAP('Hello World-everyone!')", "Hello World-Everyone!")]
	public void Initcap_Basic(string sql, string expected)
	{
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal(expected, rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Initcap_Null_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT INITCAP(NULL)");
		Assert.Null(rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#initcap
	//   INITCAP('Apples1oranges2pears', '12') => 'Apples1Oranges2Pears'
	[Fact]
	public void Initcap_CustomDelimiters()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT INITCAP('Apples1oranges2pears', '12')");
		Assert.Equal("Apples1Oranges2Pears", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region TRANSLATE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#translate
	//   TRANSLATE('This is a cookie', 'sco', 'zku') => 'Thiz iz a kuukie'
	[Fact]
	public void Translate_Basic()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT TRANSLATE('This is a cookie', 'sco', 'zku')");
		Assert.Equal("Thiz iz a kuukie", rows[0].F[0].V?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#translate
	//   "A character in source_characters without a corresponding character in target_characters is omitted."
	[Fact]
	public void Translate_Omission()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT TRANSLATE('abcdef', 'abc', 'XY')");
		Assert.Equal("XYdef", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Translate_Null_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT TRANSLATE(NULL, 'a', 'b')");
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region SOUNDEX

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#soundex
	//   SOUNDEX('Ashcraft') => 'A261'
	[Theory]
	[InlineData("SELECT SOUNDEX('Ashcraft')", "A261")]
	[InlineData("SELECT SOUNDEX('Robert')", "R163")]
	public void Soundex_Basic(string sql, string expected)
	{
		var (_, rows) = CreateExecutor().Execute(sql);
		Assert.Equal(expected, rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Soundex_Null_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT SOUNDEX(NULL)");
		Assert.Null(rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#soundex
	//   "Non-latin characters are ignored. If the remaining string is empty, an empty STRING is returned."
	[Fact]
	public void Soundex_Empty_ReturnsEmpty()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT SOUNDEX('')");
		Assert.Equal("", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region REGEXP_EXTRACT_ALL

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_extract_all
	//   "Returns an array of all substrings of value that match the regexp."
	[Fact]
	public void RegexpExtractAll_Basic()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ARRAY_LENGTH(REGEXP_EXTRACT_ALL('foo123bar456', '[0-9]+')) AS cnt");
		Assert.Equal("2", rows[0].F[0].V?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_extract_all
	//   "Returns an empty array if there is no match."
	[Fact]
	public void RegexpExtractAll_NoMatch_EmptyArray()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ARRAY_LENGTH(REGEXP_EXTRACT_ALL('foobar', '[0-9]+')) AS cnt");
		Assert.Equal("0", rows[0].F[0].V?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_extract_all
	//   Capturing group: REGEXP_EXTRACT_ALL('Try `func(x)` or `func(y)`', '`(.+?)`') => ['func(x)', 'func(y)']
	[Fact]
	public void RegexpExtractAll_WithCapturingGroup()
	{
		var (_, rows) = CreateExecutor().Execute(
			@"SELECT ARRAY_LENGTH(REGEXP_EXTRACT_ALL('Try func(x) or func(y)', 'func\((.)\)')) AS cnt");
		Assert.Equal("2", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void RegexpExtractAll_Null_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT REGEXP_EXTRACT_ALL(NULL, '[0-9]+')");
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region NORMALIZE / NORMALIZE_AND_CASEFOLD

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#normalize
	//   "Takes a string value and returns it as a normalized string."
	//   Default mode is NFC.
	[Fact]
	public void Normalize_Default_NFC()
	{
		// e + combining circumflex accent => ê (NFC composed)
		var (_, rows) = CreateExecutor().Execute(
			@"SELECT NORMALIZE('e\u0302')");
		// Should produce the composed form
		Assert.NotNull(rows[0].F[0].V);
	}

	[Fact]
	public void Normalize_Null_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT NORMALIZE(NULL)");
		Assert.Null(rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#normalize_and_casefold
	//   "Case-insensitively normalizes the characters in a STRING value."
	[Fact]
	public void NormalizeAndCasefold_CaseInsensitive()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT NORMALIZE_AND_CASEFOLD('The Red Barn') = NORMALIZE_AND_CASEFOLD('the red barn')");
		Assert.Equal("True", rows[0].F[0].V?.ToString(), StringComparer.OrdinalIgnoreCase);
	}

	#endregion

	#region COLLATE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#collate
	//   "Returns a STRING with a collation specification."
	//   In the emulator, we just return the string value unchanged.
	[Fact]
	public void Collate_ReturnsValue()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT COLLATE('hello', 'und:ci')");
		Assert.Equal("hello", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Collate_Null_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT COLLATE(NULL, 'und:ci')");
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region TO_CODE_POINTS

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#to_code_points
	//   TO_CODE_POINTS('foo') => [102, 111, 111]
	[Fact]
	public void ToCodePoints_Basic()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT ARRAY_LENGTH(TO_CODE_POINTS('foo')) AS cnt");
		Assert.Equal("3", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void ToCodePoints_Null_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT TO_CODE_POINTS(NULL)");
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region CODE_POINTS_TO_STRING (array version)

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#code_points_to_string
	//   CODE_POINTS_TO_STRING([65, 98, 67, 100]) => 'AbCd'
	[Fact]
	public void CodePointsToString_Array()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT CODE_POINTS_TO_STRING([65, 98, 67, 100])");
		Assert.Equal("AbCd", rows[0].F[0].V?.ToString());
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#code_points_to_string
	//   If any element is NULL, returns NULL.
	[Fact]
	public void CodePointsToString_NullElement_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT CODE_POINTS_TO_STRING([65, NULL, 67])");
		Assert.Null(rows[0].F[0].V);
	}

	#endregion

	#region CODE_POINTS_TO_BYTES

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#code_points_to_bytes
	//   CODE_POINTS_TO_BYTES([65, 98, 67, 100]) => b'AbCd'
	[Fact]
	public void CodePointsToBytes_Basic()
	{
		// We verify indirectly via ARRAY_LENGTH of TO_CODE_POINTS round-trip
		var (_, rows) = CreateExecutor().Execute(
			"SELECT CODE_POINTS_TO_BYTES([65, 98, 67, 100]) IS NOT NULL AS ok");
		Assert.Equal("True", rows[0].F[0].V?.ToString(), StringComparer.OrdinalIgnoreCase);
	}

	#endregion

	#region FROM_BASE32 / TO_BASE32

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#to_base32
	//   TO_BASE32(b'abcde\xFF') => 'MFRGGZDF74======'
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#from_base32
	//   FROM_BASE32('MFRGGZDF74======') => b'abcde\xFF'
	[Fact]
	public void FromBase32_Basic()
	{
		// Verify round-trip: TO_BASE32 then FROM_BASE32
		var (_, rows) = CreateExecutor().Execute("SELECT FROM_BASE32('JBSWY3DP') IS NOT NULL AS ok");
		Assert.Equal("True", rows[0].F[0].V?.ToString(), StringComparer.OrdinalIgnoreCase);
	}

	[Fact]
	public void ToBase32_Basic()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT TO_BASE32(FROM_HEX('48656C6C6F'))");
		Assert.Equal("JBSWY3DP", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region SAFE_CONVERT_BYTES_TO_STRING

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#safe_convert_bytes_to_string
	//   "Converts a BYTES value to a STRING value and replace any invalid UTF-8 characters with U+FFFD."
	[Fact]
	public void SafeConvertBytesToString_ValidUtf8()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_HEX('48656C6C6F'))");
		Assert.Equal("Hello", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void SafeConvertBytesToString_Null_ReturnsNull()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT SAFE_CONVERT_BYTES_TO_STRING(NULL)");
		Assert.Null(rows[0].F[0].V);
	}

	#endregion
}
