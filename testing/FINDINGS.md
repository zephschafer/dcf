# pvc Core Limitations Tracker

Last updated: 2026-05-09 | Total findings: 10 | Open: 5 | Fixed: 5

## Severity Definitions

| Level | Definition |
|-------|-----------|
| **Blocking** | This type of pipeline cannot be built at all with pvc in its current form |
| **Major** | Pipeline can be built but produces wrong, incomplete, or unreliable output |
| **Minor** | Pipeline works correctly but the experience is rough (errors, confusion, extra steps) |
| **Enhancement** | Works, but a feature addition would make it significantly better |

## Category Definitions

| Category | Definition |
|----------|-----------|
| **Schema** | The YAML schema cannot express what's needed (new model fields needed) |
| **Runtime** | The engine fails, produces wrong output, or behaves unexpectedly at execution time |
| **Skill** | The `new-pipeline` Claude skill gives wrong guidance, misses a step, or is unclear |
| **MCP** | An MCP tool fails, returns wrong data, or lacks a needed capability |
| **UX** | Error messages are unhelpful, CLI output is confusing, docs are wrong |
| **Performance** | Correct behavior but unacceptably slow or resource-intensive |

---

## Open Findings

| ID | Severity | Category | Summary | First Seen | Scenario | Status |
|----|----------|----------|---------|------------|---------|--------|
| F-006 | Minor | Skill | `new-pipeline` skill has no guidance on credential creation, token scopes, or storage | 2026-05-09 | github-private-repos | Open |
| F-007 | Minor | UX | `pvc init` is hardcoded to Portland Maps — no general-purpose credential collection for arbitrary API keys | 2026-05-09 | github-private-repos | Open |
| F-008 | Minor | UX | `pvc validate` passes when `{{ env.VAR }}` references an unset variable — validate gives false sense of security | 2026-05-09 | github-private-repos | Open |
| F-009 | Minor | UX | Bad/expired token gives raw `requests.HTTPError` string with no credential-specific guidance or recovery steps | 2026-05-09 | github-private-repos | Open |
| F-010 | Minor | Schema | Bearer auth requires a meaningless `key` field (not used by fetcher) — forces users to supply a dummy value | 2026-05-09 | github-private-repos | Open |

---

## Fixed Findings

| ID | Summary | Fixed In | Notes |
|----|---------|----------|-------|
| F-001 | Spark startup WARN noise obscured pvc output | `spark_session.py` — fd-level stderr redirect + `spark.driver.host=127.0.0.1` | |
| F-002 | No `namespace` field; namespace always equalled pipeline name | `models.py` + `writer/iceberg.py` — optional `namespace` field with fallback to `pipeline.name` | |
| F-003 | Array-valued fields (e.g. `topics`) could not be projected | `models.py` + `transforms.py` — new `array_join` transform | 7 unit tests in `tests/test_transforms.py` |
| F-004 | `records_path` on top-level array silently returned 0 rows | `engine/fetcher.py` — raises `ValueError` with actionable message | 3 unit tests in `tests/test_fetcher.py` |
| F-005 | No warehouse path printed after successful run | `engine/runner.py` — appended `→ <path>` to completion line | |

---

## By Design

| ID | Summary | Rationale |
|----|---------|-----------|
| — | No by-design decisions yet | — |
