# dcf Quickstart

This guide walks you from zero to a working data collector. The example ingests commits to the dcf repository — no credentials needed, since the repo is public.

---

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Java (required by PySpark — `java -version` to check)

---

## 1. Create a project

dcf is a tool you depend on, not a repo you clone. Create a fresh directory:

```bash
mkdir my-data && cd my-data
```

**`pyproject.toml`:**

```toml
[project]
name = "my-data"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = ["dcf"]

[tool.uv]
package = false

[tool.uv.sources]
dcf = { git = "https://github.com/zephschafer/dcf.git" }
```

**`.gitignore`:**

```
warehouse/
.env
.venv/
__pycache__/
```

```bash
mkdir collectors
uv sync
```

---

## 2. Write a collector

Create `collectors/dcf_commits.yml`:

```yaml
name: dcf_commits
namespace: github
description: Commits to the dcf repository.

source:
  type: http
  url: https://api.github.com/repos/zephschafer/dcf/commits
  params:
    - name: sha
      type: string
      value: main
    - name: per_page
      type: integer
      value: 100
  schema:
    columns:
      - name: sha
        path: sha
        type: string
      - name: author
        path: commit.author.name
        type: string
      - name: email
        path: commit.author.email
        type: string
      - name: message
        path: commit.message
        type: string
      - name: committed_at
        path: commit.author.date
        type: timestamp

cadence:
  strategy: incremental
  primary_key: sha
```

A few things to notice:

- **`namespace: github`** — groups the table under `warehouse/github/`. Without this, the table lands under `warehouse/dcf_commits/`.
- **`commit.author.name`** — dot-notation paths extract values from nested objects in the JSON response.
- **`cadence.strategy: incremental`** — upserts on `sha` each run, so re-running the same collector never creates duplicates.
- **`type: timestamp`** — dcf parses ISO 8601 strings with timezone info into native timestamps.

### What this produces

<table>
<tr>
<td valign="top" width="46%">

**config** (key fields)

```yaml
source:
  type: http
  url: https://api.github.com/repos/zephschafer/dcf/commits
  params:
    - name: sha
      value: main
    - name: per_page
      value: 100
  schema:
    columns:
      - name: sha
        path: sha
        type: string
      - name: author
        path: commit.author.name
        type: string
      # 3 more columns ...

cadence:
  strategy: incremental
  primary_key: sha
```

</td>
<td valign="top" width="54%">

**assembled request** _(1 request per run)_

```
GET https://api.github.com/repos/zephschafer/dcf/commits
    ?sha=main
    &per_page=100
```

**response**

```json
[
  {"sha": "6172fb8", "commit": {"author": {"name": "Zeph", "date": "2026-05-14T..."}}, ...},
  {"sha": "3266e84", "commit": {"author": {"name": "Zeph", "date": "2026-05-12T..."}}, ...}
]
```

**projected → warehouse** (`incremental` on `sha`)

```
sha        author   committed_at   ...  (5 columns)
────────── ──────── ──────────────
6172fb8    Zeph     2026-05-14
3266e84    Zeph     2026-05-12
```

**cadence** — runs once per `dcf run`, upserts on `sha`

```
dcf run dcf_commits
  → warehouse/github/dcf_commits/data/
```

</td>
</tr>
</table>

---

## 3. Validate

```bash
uv run dcf validate dcf_commits
```

```
OK — 'dcf_commits' (2 params, 0 cadence axes, 5 columns)
```

---

## 4. Test with one iteration

```bash
uv run dcf run dcf_commits --limit 1
```

```
[dcf] Running 'dcf_commits' — 1 requests

  [1/1]
    30 rows → writing

[dcf] 'dcf_commits' complete → /your/project/warehouse/github/dcf_commits/data
```

The `--limit 1` flag restricts to the first iteration (useful when your collector iterates over many date ranges or categories). For a single-request collector like this one, it behaves identically to a full run.

---

## 5. Query the warehouse

Data is written as Parquet files and is immediately queryable with DuckDB (no JVM startup):

```python
import duckdb

conn = duckdb.connect()
df = conn.execute("""
    SELECT sha, author, message, committed_at
    FROM read_parquet('warehouse/github/dcf_commits/data/*.parquet')
    ORDER BY committed_at DESC
""").fetchdf()
print(df)
```

Or if you have the MCP server running, use `query_warehouse` and dcf rewrites the table path for you:

```sql
SELECT sha, author, message FROM github.dcf_commits ORDER BY committed_at DESC
```

---

## 6. Run fully and verify deduplication

```bash
uv run dcf run dcf_commits
```

Re-run it a second time. For `incremental` collectors, the row count must stay the same — dcf upserts on `primary_key`, so repeated runs are idempotent:

```python
conn.execute("SELECT COUNT(*) FROM read_parquet('warehouse/github/dcf_commits/data/*.parquet')").fetchone()
# (30,) — same count every time
```

---

## What's next

- **Iterate over date ranges** — add a `date_range` axis to `cadence.iterate` to pull data window by window. Useful for APIs that filter by date (commits, events, logs).
- **Project nested fields** — use dot-notation paths like `commit.author.name` to extract values from nested objects.
- **Project array fields** — use the `array_join` transform to flatten list fields like `topics` into a comma-separated string.
- **Add a Python connector** — for APIs that need pagination, multi-step auth, or response reshaping, write a `connectors/` function and use `type: python`.
- **Collectors that require credentials** — see [docs/authenticated-collector.md](docs/authenticated-collector.md) for how to configure bearer auth and store API keys safely.
- **Ship to the cloud** — run `dcf gcp setup` to provision a GCS-backed Iceberg lake and set `catalog: gcp` in `.dcf/state.yml`.
- **Use Claude to build collectors** — run `dcf mcp setup-desktop` to register the MCP server with Claude Desktop. Claude can then write, validate, and run collectors on your behalf using the `new-collector` skill.

See [README.md](README.md) for the full YAML schema reference and CLI documentation.
