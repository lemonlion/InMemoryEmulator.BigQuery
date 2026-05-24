# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.105] - 2026-05-24

### Fixed
- **Literal NULL operator validation**: BigQuery rejects untyped literal NULL as operands of binary operators (`+`, `-`, `*`, `/`, `=`, `!=`, `<`, `>`, `<=`, `>=`, `||`, `&`, `|`, `^`, `<<`, `>>`), unary `NOT`, and `LIKE`. Emulator now correctly throws errors matching real BigQuery behavior.
- **EvaluatedValueExpr AST node**: Introduced `EvaluatedValueExpr` to distinguish pre-evaluated values from source `LiteralExpr` nodes, preventing false NULL-rejection errors during aggregate/window evaluation.
- **CAST(FLOAT64 AS STRING) formatting**: NaN now returns `"nan"` (lowercase), and whole-number floats return without `.0` suffix (e.g., `25.0` → `"25"`).
- **Fractional seconds formatting**: TIMESTAMP, DATETIME, and TIME `CAST AS STRING` now trims trailing zeros in groups of 3 (outputs 3 or 6 digits) instead of individual character trimming.
- **LAG/LEAD IGNORE NULLS rejection**: BigQuery does not support `IGNORE NULLS` or `RESPECT NULLS` on LAG/LEAD — emulator now throws matching error.
- **LAG/LEAD NULL offset rejection**: Argument 2 to LAG/LEAD must be non-NULL — emulator now throws instead of returning NULL.
- **TIMESTAMP_DIFF WEEK rejection**: BigQuery does not support the `WEEK` date part with TIMESTAMP arguments — emulator now throws matching error.
- **INTERSECT ALL / EXCEPT ALL rejection**: BigQuery only supports DISTINCT variants of these set operations — emulator now throws matching error.
- **ARRAY_TO_STRING type validation**: Now correctly rejects non-STRING arrays with a signature mismatch error.
- **ARRAY_CONCAT NULL rejection**: Now throws "must be an array type but was NULL" instead of silently returning NULL.
- **CONTAINS_SUBSTR NULL search**: Second argument must not be null — now throws instead of returning NULL.
- **STRING_AGG NULL separator**: Argument 2 must be non-NULL — now throws instead of returning NULL.
- **CAST(BOOL AS FLOAT64) rejection**: BigQuery does not support this cast — now throws matching error.
- **JSON_EXTRACT_ARRAY non-array**: Returns empty array for non-array JSON values instead of NULL.
- **REGEXP_EXTRACT_ALL non-participating groups**: Returns empty string for optional non-participating capture groups (matching real BigQuery).
- **Vector distance functions**: Now throw proper errors for unequal-length arrays and zero vectors instead of returning NULL.
- **Query without FROM validation**: Rejects WHERE clauses and aggregate functions in SELECT statements without a FROM clause.
- **ARRAY equality rejection**: BigQuery does not support `=` or `!=` on ARRAY types — now throws matching error.
- **INSERT INTO ... WITH CTE SELECT**: Parser now accepts CTE syntax after INSERT INTO (BigQuery's DML syntax).
- **ROUND IEEE 754 fidelity**: Custom `RoundBigQuery` method uses G17 string representation to determine the true decision digit, matching BigQuery's behavior for midpoint values like `ROUND(4.55, 1) = 4.5` (4.55 is stored as 4.5499... in IEEE 754 binary).
- **REGEXP_EXTRACT_ALL(NULL) returns NULL**: Correctly returns NULL (not empty array) when input is NULL.
- **TO_CODE_POINTS(NULL) returns NULL**: Correctly returns NULL (not empty array) when input is NULL.

### Changed
- 122 test files updated to match real BigQuery behavior (corrected assertions, SQL syntax, and InMemoryOnly markers)
- Tests for unsupported features (ARRAY_FILTER, ARRAY_TRANSFORM, IIF, JSON_CONTAINS, NOW, DOT_PRODUCT, APPROX_*, procedural SQL, QUALIFY, PIVOT/UNPIVOT, RANGE/INTERVAL literals) marked with `[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]`
- `REGEXP_EXTRACT_ALL(NULL)` and `TO_CODE_POINTS(NULL)` integration tests marked InMemoryOnly (BigQuery SDK represents NULL REPEATED fields as non-null array objects, making NULL assertion impossible via SDK)

## [1.0.104] - 2026-05-13

### Added
- `WithHttpMessageHandlerWrapper()` on `InMemoryBigQueryOptions` and `InMemoryBigQueryBuilder` — allows wrapping the `FakeBigQueryHandler` with additional `DelegatingHandler` instances for HTTP-level observability (e.g. TestTrackingDiagrams integration)
- `HttpMessageHandlerWrapper` property on `InMemoryBigQueryOptions` for direct property-style configuration
- Wrapper is automatically wired through the `UseInMemoryBigQuery()` DI extension method

## [1.0.103] - 2026-07-16

### Fixed
- NormalizeSql date-part regex no longer incorrectly converts column names like `quarter`, `day`, `month`, `year` to string literals in INSERT column lists
- Added negative lookahead `(?!\s*(?:VALUES|SELECT|WITH)\b)` to Pattern 1 of the bare-date-part rewrite so that `INSERT INTO t (col1, quarter) VALUES (...)` is preserved as identifiers

### Changed
- INSERT INTO statements across 70 test files now include explicit column lists for Go emulator compatibility

## [1.0.6] - 2026-07-03

### Added
- Range functions: `RANGE`, `RANGE_START`, `RANGE_END`, `RANGE_CONTAINS`, `RANGE_OVERLAPS`, `GENERATE_RANGE_ARRAY`
- Performance test suite with latency benchmarks (7 tests covering SELECT, WHERE, aggregation, GROUP BY, INSERT, functions, CRUD)
- **JsUdfs package**: JavaScript UDF support via Jint engine — `CREATE FUNCTION ... LANGUAGE js AS "..."` now works
  - `JintJsUdfEngine` implementation with full type mapping (number, string, boolean, null, array)
  - `UseJsUdfs()` extension method on `InMemoryDataStore`
  - Supports double-quoted, single-quoted, and triple-quoted (`r"""..."""`) JavaScript bodies
- **ProductionExtensions package**: `AsAsyncEnumerable()`, `MapAsync<T>()`, `ToListAsync()`, `ToListAsync<T>()` extensions for `BigQueryResults`
- `IJsUdfEngine` interface in main package for pluggable JavaScript execution

### Fixed
- Routine name lookups are now case-insensitive (matching real BigQuery behavior)
- `CREATE FUNCTION ... LANGUAGE js` statement parsing (previously only SQL UDFs were supported)

## [1.0.4] - 2026-04-26

### Added
- Hash functions: `SHA1`, `SHA512`, `FARM_FINGERPRINT`
- Statistical aggregate functions: `STDDEV`, `STDDEV_SAMP`, `STDDEV_POP`, `VAR_SAMP`, `VAR_POP`, `VARIANCE`
- Array functions: `ARRAY_CONCAT`, `ARRAY_REVERSE`, `ARRAY_FIRST`, `ARRAY_LAST`, `ARRAY_SLICE`
- Array literal syntax: `[expr, ...]` (tokenizer + parser support for `[` and `]` brackets)
- Dry run query validation: invalid SQL now returns errors even for dry-run jobs

### Fixed
- `QueryOptions.Labels` initialization: fixed `NullReferenceException` when using collection initializer syntax
- Dry-run invalid queries no longer silently succeed — they now return HTTP 400

### Changed
- Upgraded Google.Cloud.BigQuery.V2 SDK from 3.10.0 to 3.11.0

### Added
- Initial project scaffold (Phase 0)
- Solution structure with 3 source projects and 4 test projects
- `FakeBigQueryHandler` — HTTP interception skeleton
- `FakeBigQueryHttpClientFactory` — SDK pipeline integration
- `InMemoryDataStore`, `InMemoryDataset`, `InMemoryTable` — data model
- `InMemoryBigQuery.Create()` — entry point
- `InMemoryBigQueryBuilder` — fluent builder
- `UseInMemoryBigQuery()` — DI integration skeleton
- Test infrastructure: `ITestDatasetFixture`, `BigQuerySession`, `TestFixtureFactory`
- Three test targets: InMemory, BigQueryEmulator (Docker), BigQueryCloud
- CI workflows: test, weekly cloud parity, release
- Scripts: run-tests, start-emulator, stop-emulator
