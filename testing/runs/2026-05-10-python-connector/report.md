# Test Run: Python Connector End-to-End (Linear GraphQL)
Date: 2026-05-10 | Tester: Claude Sonnet 4.6 | Scenario: python-connector

## Outcome: PARTIAL SUCCESS

Live API calls skipped (no LINEAR_API_KEY provided). All static analysis, import
chain testing, and YAML validation completed. Runtime deduplication not tested.

## Success Criteria

- [x] Phase 1: Connector interface documented — function name, signature, return type
      → Documented from fetcher.py + models.py source analysis (see Phase 1 below)
- [x] Phase 1: README / skill gap documented — does docs cover Python connectors?
      → README covers the interface well; skill has two gaps (F-024, F-025)
- [x] Phase 2: Connector imports without error
      → `connectors.linear_issues` imports cleanly via both direct and ddt import chain
- [ ] Phase 2: `ddt run linear_issues --limit 1` returns at least 1 row
      → Skipped — no LINEAR_API_KEY
- [x] Phase 2: `state.name` nested field extracted (via connector-level logic)
      → Flattened in connector: `state_name = node["state"]["name"]`
- [x] Phase 2: `assignee.name` correctly nullable
      → Handled: `assignee_name = node["assignee"]["name"] if node.get("assignee") else None`
- [ ] Phase 3: Full run completes with all pages fetched
      → Skipped — no LINEAR_API_KEY
- [ ] Phase 3: Row count stable across re-runs (deduplication on string `id`)
      → Skipped — no LINEAR_API_KEY
- [ ] Phase 3: Warehouse queryable — SELECT state_name, COUNT(*) GROUP BY state_name
      → Skipped — no LINEAR_API_KEY

## Phase 1 — Connector Interface (discovered from source)

**Exact interface for `type: python` connectors:**

| Property | Value | Source |
|----------|-------|--------|
| Module path | YAML `source.module` — e.g. `connectors.linear_issues` | `fetcher.py:_fetch_python` |
| Function name | YAML `source.function` — any name | `fetcher.py:_fetch_python` |
| Signature | `def fn(dynamic_params: dict) -> list[dict]` | `models.py:PythonSource` docstring |
| `dynamic_params` contents | All params merged: static values + iterate values + CLI `--param` overrides | `runner.py:35` |
| Return type | `list[dict]` — NOT generator, NOT iterator | `fetcher.py:_fetch_python:return fn(dynamic_params)` |
| Auth handling | **No `auth` field on PythonSource** — connector must read credentials from `dynamic_params` (pass via `{{ env.VAR }}` param) or directly from `os.environ` | `models.py:PythonSource` |
| Pagination | Entirely connector-managed — ddt has no pagination protocol | `models.py:PythonSource` docstring |
| No-iterate case | `build_request_sequence([]) → [{}]`; connector called once with static params only | `iterator.py:83` |

**Key observation:** Auth is not carried over from YAML to Python connectors.
The recommended pattern (pass key as static param with `{{ env.VAR }}`) works but is undocumented.

## What Worked

- YAML validation: ✓ — validates with correct warning about `LINEAR_API_KEY` not being set
- Module import via ddt chain: ✓ — `connectors.linear_issues` resolves correctly from project root
- Function resolution: ✓ — `getattr(mod, "fetch_issues")` works as expected
- Env var warning: ✓ — `ddt validate` correctly warns `LINEAR_API_KEY` not set (F-008 working)
- PythonSource model: ✓ — `params` with `{{ env.VAR }}` values are resolved before connector call

## What Failed / Was Blocked

- MCP `run_pipeline` always uses `catalog: local`, ignoring `catalog: gcp` in project.yml.
  For the quipu project (GCP), pipeline data via MCP goes to local warehouse, not GCS.
  [→ Finding F-022: Major / Runtime]

- Connector exceptions produce only `fetch error: {e}` — no traceback, no exit code,
  pipeline "completes" with 0 rows. Cannot distinguish crash from empty result.
  [→ Finding F-023: Minor / UX]

## Friction Points

- Skill doesn't say when to use `type: python` vs `type: http`. A first-time user with
  a GraphQL API might try to express it in YAML (it can't be done — GraphQL requires
  a POST body, which `type: http` doesn't support). Decision criteria are missing.
  [→ Finding F-024: Enhancement / Skill]

- Skill doesn't document that auth isn't carried to Python connectors. The correct
  pattern (pass API key as static param with `{{ env.LINEAR_API_KEY }}`) is discoverable
  but not mentioned anywhere in the skill or README.
  [→ Finding F-025: Enhancement / Skill]

## Pipeline Produced

```yaml
version: 1
name: linear_issues
namespace: linear
description: All issues from the Linear issue tracker

source:
  type: python
  module: connectors.linear_issues
  function: fetch_issues

  params:
    - name: api_key
      type: string
      value: "{{ env.LINEAR_API_KEY }}"

schema:
  columns:
    - name: id
      path: id
      type: string
    - name: title
      path: title
      type: string
    - name: state_name
      path: state_name
      type: string
    - name: priority
      path: priority
      type: integer
    - name: assignee_name
      path: assignee_name
      type: string
    - name: created_at
      path: created_at
      type: timestamp
    - name: updated_at
      path: updated_at
      type: timestamp

build:
  strategy: incremental
  primary_key: id
```

Files written:
- `connectors/linear_issues.py` (in quipu project)
- `pipelines/linear_issues.yml` (in quipu project)

## Proposed Fixes

1. F-022: In MCP `run_pipeline`, read catalog from project.yml the same way CLI does:
   add `from .warehouse_reader import _project_config` (or a dedicated helper) and
   pass `catalog=_project_config().get("catalog", "local")` to `_run()`.

2. F-023: In `runner.py`, replace `print(f"    fetch error: {e}")` with
   `traceback.print_exc()` (or at minimum `print(f"    fetch error: {type(e).__name__}: {e}")`).
   Print a failure summary at the end: "N/M iterations failed."

3. F-024: In `new-pipeline.md`, add a decision table before step 5:
   "Use `type: http` when... | Use `type: python` when..." with concrete examples
   (GraphQL POST body, cursor pagination, HTML scraping, multi-step auth).

4. F-025: In `new-pipeline.md`, add a note under the Python connector section explaining
   that `PythonSource` has no `auth` block — pass credentials as a static param using
   `{{ env.VAR }}` syntax so ddt resolves and passes the key to the connector.
