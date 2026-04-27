# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
