# Test Run: GitHub Public Repositories
Date: 2026-05-09 | Tester: Claude Sonnet 4.6 | Scenario: github-repos

## Outcome: SUCCESS

## Success Criteria
- [x] Pipeline YAML validates successfully (`pvc validate github_repos`)
- [x] `--limit 1` run fetches at least 1 real repository row (fetched 100)
- [x] Schema projection captures: id, name, full_name, description, language, stargazers_count, forks_count, created_at, updated_at, html_url, owner_login
- [x] Full run writes correctly to warehouse (100 rows)
- [x] Incremental deduplication on primary key `id` works correctly on re-run (count stable at 100)
- [~] Warehouse queryable via DuckDB — works, but query path is `warehouse/github_repos/github_repos/data/*.parquet`, not the `github.repos` shorthand in the scenario (no namespace concept in pvc today) [→ F-002]

## What Worked
- Pipeline YAML validates with 0 errors on first attempt: ✓
- HTTP GET with static query params (`per_page=100`, `type=public`): ✓
- Top-level JSON array response with no `records_path` needed: ✓
- Schema projection of 11 columns: ✓
- Dot-path extraction of nested field (`owner.login` → `owner_login`): ✓
- Null handling for `description` and `language` (both can be null): ✓
- ISO 8601 timestamp parsing with timezone (`TIMESTAMP WITH TIME ZONE` in DuckDB): ✓
- Incremental deduplication on integer primary key `id`: ✓
- Re-run stability: count stays exactly 100 after second run: ✓
- Full run produces expected top repos (zookeeper, cassandra, couchdb): ✓

## What Failed

None. All core mechanics worked on first attempt.

## Friction Points

**F-001: Enhancement / UX — Spark startup warning noise**
During `pvc run`, ~20 lines of Spark/JVM/Ivy WARN messages appear before any pvc output. Example:
```
WARNING: Using incubator modules: jdk.incubator.vector
WARN Utils: Your hostname resolves to a loopback address...
WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
```
These warnings obscure the pvc progress output and are confusing for users who just want to ingest data. Especially jarring for a simple single-request HTTP pipeline that doesn't require Spark at all (incremental write uses PyArrow directly).

**F-002: Enhancement / Schema — No namespace field; namespace always equals pipeline name**
The warehouse layout is always `warehouse/<pipeline_name>/<pipeline_name>/data/`. There's no way to configure a namespace. A set of related pipelines (`github_repos`, `github_issues`, `github_commits`) cannot share a `github` namespace, making DuckDB queries verbose and ungrouped. The scenario's target query `FROM github.repos` is not expressible. Hardcoded in writer.py line 46: `namespace = pipeline.name`.

**F-003: Enhancement / Schema — Array-valued fields cannot be projected**
The `topics` field in GitHub API response is an array of strings: `["java", "tapestry", "web-framework"]`. pvc has no mechanism to handle array fields in the schema — dot-path returns the whole array as a Python list, which cannot be cast to any pvc type. Currently must be omitted from the schema.

**F-004: Minor / UX — `records_path` set on a top-level array silently returns 0 rows**
Discovered by code review, not runtime (did not reproduce deliberately to avoid polluting the warehouse). In `fetcher.py` line 51-53:
```python
for key in source.response.records_path.split("."):
    if not isinstance(data, dict):
        return []  # ← silently returns empty list
```
If a user mistakenly sets `records_path: data` on an API that returns a top-level array, they get 0 rows with no warning. The correct behavior should be a clear error: `"records_path 'data' was specified but the response is a JSON array, not an object"`.

## Pipeline Produced
See pipeline.yml in this directory.

## Proposed Fixes
1. F-001: Suppress Spark/Hadoop/Ivy WARN output; only show pvc lines during run. Options: configure Spark log4j to ERROR-only for infrastructure loggers, or redirect JVM stderr to /dev/null.
2. F-002: Add optional `namespace` field to `Pipeline` model; use it in the writer when present. Default to `pipeline.name` for backward compatibility.
3. F-003: Add a `join` or `array_join` transform type that flattens a list field to a delimited string; or add a `first` extract that takes `array[0]`.
4. F-004: In `_parse_response`, after navigating `records_path`, check if result is still a list but the input was not a dict — emit a clear error rather than returning [].

## Notes
- Test project used: `/Users/zephschafer/Documents/GitHub/quipu/` (pvc editable install from `../pvc`)
- The `quipu` project's existing pipelines were not affected
- GitHub unauthenticated rate limit was not hit (60 req/hour; only 2 requests made)
- Spark startup time adds ~8 seconds to every run, even for pipelines that use the PyArrow incremental writer (which doesn't need Spark during data fetch/write — Spark only needed for `_ensure_namespace`)
