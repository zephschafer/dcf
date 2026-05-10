# pvc Core Limitations Tracker

Last updated: 2026-05-10 | Total findings: 25 | Open: 2 | Fixed: 23

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

| ID | Severity | Category | Summary | Scenario |
|----|----------|----------|---------|----------|
| F-024 | Enhancement | Skill | `new-pipeline` skill missing decision guidance on when to use `type: python` vs `type: http` (e.g. GraphQL POST body, cursor pagination) | python-connector |
| F-025 | Enhancement | Skill | `new-pipeline` skill doesn't document auth pattern for Python connectors — `PythonSource` has no `auth` field; correct pattern (pass key as `{{ env.VAR }}` param) is undocumented | python-connector |

---

## Fixed Findings

| ID | Summary | Fixed In | Notes |
|----|---------|----------|-------|
| F-001 | Spark startup WARN noise obscured pvc output | `spark_session.py` — fd-level stderr redirect + `spark.driver.host=127.0.0.1` | |
| F-002 | No `namespace` field; namespace always equalled pipeline name | `models.py` + `writer/iceberg.py` — optional `namespace` field with fallback to `pipeline.name` | |
| F-003 | Array-valued fields (e.g. `topics`) could not be projected | `models.py` + `transforms.py` — new `array_join` transform | 7 unit tests in `tests/test_transforms.py` |
| F-004 | `records_path` on top-level array silently returned 0 rows | `engine/fetcher.py` — raises `ValueError` with actionable message | 3 unit tests in `tests/test_fetcher.py` |
| F-005 | No warehouse path printed after successful run | `engine/runner.py` — appended `→ <path>` to completion line | |
| F-006 | `new-pipeline` skill had no guidance on credential creation, token scopes, or storage | Added credential section to `new-pipeline.md` — covers env vars, project.yml storage, auth type selection | |
| F-007 | `pvc init` hardcoded to Portland Maps — no general credential collection | `cli.py` — removed Portland Maps/regions prompts; init now only sets catalog, prints key storage instructions | |
| F-008 | `pvc validate` passed silently when `{{ env.VAR }}` was unset | `cli.py` — validate now scans YAML for env refs and warns on any that are missing | |
| F-009 | HTTP 401/403 gave raw `requests.HTTPError` with no guidance | `engine/fetcher.py` — 401/403/404/429 now surface with human-readable message + actionable hint | |
| F-010 | Bearer auth required a `key` field that the fetcher never used | `config/models.py` — `Auth.key` is now optional for bearer; required only for query_param/header | |
| F-011 | Terraform `.tf` files missing from pvc repository | `pvc/infra/modules/gcp/main.tf` + `variables.tf` created | |
| F-012 | `append` and `full_refresh` with `catalog: gcp` used unconfigured Spark GCS catalog | `writer/iceberg.py` — all three strategies now route through `_append_gcs`/`_overwrite_gcs`/`_upsert_gcs`; Spark bypassed entirely for GCS | |
| F-013 | `warehouse_reader.py` read only local warehouse — GCS not supported | `warehouse_reader.py` rewritten: GCS blobs downloaded via `google-cloud-storage`, registered as Arrow tables via `conn.register()` | DuckDB 1.5.2 has no GCS extension; approach avoids it entirely |
| F-014 | Billing-not-enabled 403 had no actionable guidance; traceback saved to project.yml | `gcp/bootstrap.py` + `cli.py` — billing error now raises with billing console URL; project.yml stores `str(e)` not traceback | |
| F-015 | No `pvc gcp teardown` command | `cli.py` — added `pvc gcp teardown`; `terraform.py` — added `destroy()`; `bootstrap.py` — added `delete_secret` + `delete_service_account` | |
| F-016 | README GCP section missing Terraform, billing, and API prerequisites | `README.md` — added GCP prerequisites section with required APIs and setup commands | |
| F-017 | `bootstrap.py` hardcoded `quipu-lake` as SA ID and secret name | `gcp/bootstrap.py` — renamed to `pvc-lake` throughout | |
| F-018 | `list_warehouse_tables` only shows GCS tables when `catalog: gcp` | `warehouse_reader.py` — _iter_local_tables() helper; list_tables() now shows both GCS (location='gcs') and local-only (location='local') | `2f5d057` |
| F-019 | `query_warehouse` auto-LIMIT wrapping broke COPY/DDL with cryptic parse error | `warehouse_reader.py` — _is_write_statement() detects write prefixes; DDL bypasses wrapping | `2f5d057` |
| F-020 | No `materialize_model` MCP tool — model persistence required workarounds | `warehouse_reader.py` + `mcp_server.py` — new materialize_model() writes result Parquet locally and uploads to GCS when catalog=gcp | `2f5d057` |
| F-021 | Querying local-only table in GCP mode gave cryptic DuckDB CatalogException | `warehouse_reader.py` — _resolve_table_refs() now falls back to local read_parquet() for tables not in GCS | `2f5d057` |
| F-022 | MCP `run_pipeline` ignored `catalog: gcp` — always wrote to local warehouse | `mcp_server.py` — reads `_project_config().get("catalog", "local")` before calling runner | `c8ea972` |
| F-023 | Connector exceptions showed only `fetch error: {e}` — no traceback, no failure summary | `runner.py` — adds exception class, full traceback (indented), and 3-state completion line (complete / complete with errors / FAILED) | `a1041e0` |

---

## By Design

| ID | Summary | Rationale |
|----|---------|-----------|
| — | No by-design decisions yet | — |
