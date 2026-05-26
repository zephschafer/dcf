You are helping the user debug a failing or misbehaving dcf collector.

**Core principle: probe the live source BEFORE editing any YAML.** Editing in the dark leads to guessing — one live API call gives you ground truth.

Follow these steps in order. Do not skip ahead.

---

## 1. Read the collector

Read `collectors/{name}.yml` (or call `read_collector(name)` via MCP). Identify:
- Source type (`http`, `python`, `sql`)
- URL / module / connection
- Auth pattern (bearer, header, query_param)
- Cadence strategy and iteration axes
- Schema column paths

## 2. Understand the symptom precisely

Ask the user (or use context already given):
- What command was run and what was the **exact** error message or behavior?
- Which stage is failing: fetch, schema projection, or write?
- If data landed: how many rows? What was expected?

Common stage indicators:
- **Fetch error** — HTTP error, ImportError, connection refused
- **Projection error** — KeyError, TypeError, columns all null
- **Write error** — Iceberg/Spark error after data was fetched
- **Silent wrong data** — rows land but values are wrong or missing

## 3. Probe the live source

This step is mandatory. Do not skip it.

**For `type: http` collectors:**

```python
import requests, os

resp = requests.get(
    "<url from YAML>",
    params={"param1": "value1", ...},       # use actual values
    headers={"Authorization": f"Bearer {os.environ['MY_API_KEY']}"},
)
print(resp.status_code)
print(resp.json())
```

What to verify:
- Auth works (401/403 → check env var name in YAML vs actual env var name)
- Response structure matches `records_path` (is the array at `data` or `data.items`?)
- Field names are exactly right (`question_id` vs `questionId`)
- Are there more pages? Unexpected nesting?

**For `type: python` collectors:**

Call the connector function directly with a sample `dynamic_params` dict:

```python
import sys
sys.path.insert(0, ".")
from connectors.my_connector import fetch_data

rows = fetch_data({"start_date": "2024-01-01", "end_date": "2024-01-07", ...})
print(len(rows), rows[:2])
```

**For `type: sql` collectors:**

Run the query directly against the source database to confirm connectivity and schema.

## 4. Query the warehouse to see what landed

```bash
dcf query "SELECT * FROM namespace.collector_name LIMIT 10"
dcf query "SELECT COUNT(*) FROM namespace.collector_name"
```

Or via MCP: `query_warehouse("SELECT * FROM namespace.collector_name LIMIT 10")`

Cross-reference what landed with what the probe returned. If rows are missing, the issue is upstream (fetch or schema). If rows are wrong-typed or null, the issue is schema projection.

## 5. Identify the root cause

Common failure signatures:

| Symptom | Likely cause |
|---|---|
| 401/403 from probe | Env var name mismatch (`{{ env.MY_KEY }}` vs actual var name) |
| `records_path` points to null | Response structure doesn't match — compare to actual response |
| Column all null | `path` field name doesn't match actual JSON field |
| Zero rows, no error | Date range returns empty array on this time window |
| Row count grows on re-run | `primary_key` not deterministic — key field not stable across runs |
| ImportError | Module path wrong or connector not on Python path |
| Type cast error | Field is sometimes null, or integer arriving as string |

## 6. Edit the YAML (or connector)

Only edit once you know the root cause from steps 3–5.

Use `write_collector(name, yaml_content)` via MCP or edit the file directly, then:

```bash
dcf validate my_collector
```

Or `validate_collector(name)` via MCP.

Fix validation errors before proceeding.

## 7. Re-run with `--limit 1` and verify

```bash
dcf run my_collector --limit 1
dcf query "SELECT * FROM namespace.my_collector LIMIT 10"
```

Confirm:
- No errors
- Data looks correct (values, types, row count)
- For `incremental` collectors: re-run the same command — row count must stay the same (confirms upserts work)

## 8. Report

Summarize:
- Root cause
- What was changed (YAML field / connector code)
- Row count after the fixed run
- Confirmation that re-run is stable (for incremental collectors)
