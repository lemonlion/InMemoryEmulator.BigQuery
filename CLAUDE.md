# Contribution Instructions

## Purpose
The sole purpose of this project is to be an in-process emulator of GCP BigQuery for use in integration tests.  It should be mirror the behaviour of GCP BigQuery as much as is feasibly possibly in the context of an in-process emulator.  eg we shouldn't be supporting SQL feature that it doesn't, we want it to fail when GCP fails and pass when GCP passes.  If tests are passing on the InMemoryEmulator but failing when using the Official GO emulator, it's most likely that there is incorrect behaviour in the InMemoryEmulator and the tests are incorrect.  If tests are passing on the InMemoryEmulator but failing when using the Official GCP Bigquery, it's almost certain that there is incorrect behaviour in the InMemoryEmulator and the tests are incorrect - the only exception is convenience methods that help with integration tests (eg a ClearAll method or something like that), not something to do with standard CRUD.

## TDD Workflow

- Always use Test-Driven Development (TDD): write tests first, then follow the red-green-refactor cycle.
- Write a failing test (red), implement the minimum code to make it pass (green), then refactor.
- Write additional failing tests to cover edge cases and error conditions, and repeat the cycle until you have comprehensive test coverage for the feature or bug fix you're working on.
- For every unit test written, if possible, write the equivalent integration test, testing the same functionality from the entry point.

## Bug Fixing

- Always fix all bugs you find along the way, even if they are outside the immediate scope of the current task.
- When fixing a bug, identify missing test coverage in and around the affected area and create that coverage — again following the TDD red-green-refactor cycle.
- Fix any additional bugs discovered during that expanded test coverage work.

## Reflection Policy

- **Do not use reflection as a first resort.** Explore all public API options before considering reflection.
- Reflection on internal/private members of external libraries (e.g., SDK backing fields) is fragile — it can break silently on library updates with no compile-time warning.
- If reflection is genuinely the only viable approach after exhausting alternatives, it may be used — but:
  - **The PR description must explicitly state in bold that reflection is used**, what it targets, and why no public API alternative exists.
  - Add a code comment at the reflection site explaining the dependency and what would break if the internal member is renamed or removed.
  - Prefer a graceful fallback (e.g., leave the value as null) over a hard failure if the reflected member is missing.

## Behavioral Source Requirements

Every piece of behavioral logic in the source code — status codes, validation rules, error conditions, side-effect semantics — **must** be backed by a verified source. This prevents accidental divergence from real Google Cloud BigQuery behavior.

### Rules

1. **Before implementing any behavioral logic**, find and verify the expected behavior from one of the approved sources listed below.
2. **Add a code comment** at the implementation site citing the source (a short URL or description is sufficient). Example:
   ```csharp
   // Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
   //   "Runs a BigQuery SQL query synchronously and returns query results if the query completes within a specified timeout."
   ```
3. **If sources conflict** (e.g., the emulator behaves differently from the documentation), prefer the official documentation over observed emulator behavior. Document the discrepancy in a code comment and mark the relevant integration test with `[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]`.
4. **If no source can be found**, do not guess. Ask for guidance or raise a discussion in the PR.

### Approved Behavioral Sources (in priority order)

| Priority | Source | URL / Location |
|----------|--------|----------------|
| 1 | Google Cloud BigQuery REST API reference | https://cloud.google.com/bigquery/docs/reference/rest |
| 2 | Google Cloud BigQuery .NET SDK API reference | https://cloud.google.com/dotnet/docs/reference/Google.Cloud.BigQuery.V2/latest |
| 3 | Google Cloud BigQuery SQL reference | https://cloud.google.com/bigquery/docs/reference/standard-sql |
| 4 | Google Cloud BigQuery "How-to" guides | https://cloud.google.com/bigquery/docs/how-to |
| 5 | Google Cloud BigQuery conceptual docs | https://cloud.google.com/bigquery/docs/introduction |
| 6 | Google Cloud BigQuery .NET SDK source code | https://github.com/googleapis/google-cloud-dotnet |
| 7 | Observed behavior on a real BigQuery instance | (testing against a live GCP project) |

> **Note:** Source 7 (observed behavior on a real instance) is the weakest evidence. Always cross-reference with sources 1–6 when possible.

## Versioning & Release

- After every session of bug fixes is complete and the full test suite has passed, increment the patch version in `src/Directory.Build.props` (the single `<Version>` property shared by all packages).
- **On `main`:** Commit, create a git tag (`v{version}`), and push both the commit and the tag to origin.
- **On any other branch:** Commit and push the code changes and version bump only. Do not create or push a tag.

## Test Classification Rules

Tests are split into two projects. When creating or moving tests, follow these rules:

### Tests.Integration
- Uses `TestFixtureFactory.Create(session)` / `ITestDatasetFixture` to obtain a dataset, where `session` is an injected `EmulatorSession` (xUnit collection fixture — decorate the test class with `[Collection(IntegrationCollection.Name)]`)
- Goes through the real BigQueryClient SDK pipeline via the in-process test server
- Must **not** use `new InMemoryBigQueryStore()`, `new FaultInjector()`, or any `internal` API
- Can run against in-memory or real GCP BigQuery via `BIGQUERY_TEST_TARGET`
- **This is the primary test project** — every test should be an integration test unless it requires internal API access

### Tests.Unit
- Uses `new InMemoryBigQueryStore()`, `new InMemoryBigQueryClient()`, or any `internal` API directly
- Tests that use the service but also touch internal APIs (e.g., fault injection internals) belong here
- Only runs in-memory — never against a real instance

### Tests.Shared
- Class library (not a test project) — shared infrastructure, fixtures, traits, and models
- Referenced by both Unit and Integration projects

### Key constraint
The Integration project does **not** have `InternalsVisibleTo` access. If a test needs internal APIs, it belongs in Unit.

## Documentation

After any changes are made that might affect the public API or functionality, documentation must be updated to reflect those changes. The documentation should be clear and comprehensive, covering all new features, changes to existing features, and any deprecations or removals. This includes updating the README file (if relevant), but mainly the wiki which can be found in a sister folder to the main repository — `../InMemoryEmulator.BigQuery.wiki`.

## Other stuff
Execute all plans in full, do not skip anything

Fix behavioral bugs in the InMemoryEmulator codebase, ensuring all fixes are backed by verified sources and accompanied by appropriate test coverage.  Don't just change the test to match the buggy behavior — find the root cause in the InMemoryEmulator code and fix it to match real BigQuery behavior. Then add or update tests to verify the fix and prevent regressions.

Assume first and foremost that failures represent missing features/bugs in the InMemoryEmulator.  The tests for InMemoryEmulator could be wrong, so don't assume that because the tests pass on InMemoryEmulator that the tests are correct.  Only assume they are unsupported features in the GO emulator if you have a documented official source proving that.  Fix the bugs/missing features, don't skip the tests to mask lack of parity with real BigQuery.  The goal is complete parity with real BigQuery.

Note: Every time anything is marked as a GO Emulator limitation, you need to reference in the code the exact official document and quote from it proving that it's a limitation in the GO emulator, and you need to have that written as a comment or input string into the attribute.

Remember the source of truth is the official BigQuery docs, not the tests.
