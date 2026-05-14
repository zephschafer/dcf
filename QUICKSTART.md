# ddt Quickstart

This guide walks you from zero to a working data pipeline. The example ingests your private GitHub repositories — it covers credentials, schema projection, and warehouse querying in a single concrete run.

---

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Java (required by PySpark — `java -version` to check)

---

## 1. Create a project

ddt is a tool you depend on, not a repo you clone. Create a fresh directory:

```bash
mkdir my-data && cd my-data
```

**`pyproject.toml`:**

```toml
[project]
name = "my-data"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = ["ddt"]

[tool.uv]
package = false

[tool.uv.sources]
ddt = { git = "https://github.com/zephschafer/ddt.git" }
```

**`project.yml`** (gitignore this file — it holds your credentials):

```yaml
catalog: local
```

**`.gitignore`:**

```
warehouse/
project.yml
.venv/
__pycache__/
```

```bash
mkdir pipelines
uv sync
```

---

## 2. Store your credentials

ddt resolves `{{ env.VAR }}` placeholders in pipeline YAML from two places, in order:

1. OS environment variable (`export GITHUB_TOKEN=...`)
2. `project.yml` key (lowercased, e.g. `github_token: ...`)

For credentials you want to persist across shell sessions, add them to `project.yml`:

```yaml
catalog: local
github_token: ghp_xxxxxxxxxxxx
```

> `project.yml` is gitignored and never committed. It is the right place for API keys.

---

## 3. Write a pipeline

Create `pipelines/github_repos.yml`:

```yaml
version: 1
name: github_repos
namespace: github
description: My private GitHub repositories

source:
  type: http
  url: https://api.github.com/user/repos
  method: GET
  auth:
    type: bearer
    key: token       # required by the schema; not used in the request itself
    value: "{{ env.GITHUB_TOKEN }}"
  params:
    - name: visibility
      type: string
      value: private
    - name: per_page
      type: integer
      value: 100

schema:
  columns:
    - name: id
      path: id
      type: integer
    - name: name
      path: name
      type: string
    - name: full_name
      path: full_name
      type: string
    - name: private
      path: private
      type: boolean
    - name: description
      path: description
      type: string
    - name: language
      path: language
      type: string
    - name: stargazers_count
      path: stargazers_count
      type: integer
    - name: forks_count
      path: forks_count
      type: integer
    - name: created_at
      path: created_at
      type: timestamp
    - name: updated_at
      path: updated_at
      type: timestamp
    - name: default_branch
      path: default_branch
      type: string
    - name: visibility
      path: visibility
      type: string

build:
  strategy: incremental
  primary_key: id
```

A few things to notice:

- **`namespace: github`** — groups the table under `warehouse/github/`. Without this, the table lands under `warehouse/github_repos/`.
- **`auth.key: token`** — bearer auth doesn't use the key field, but the schema requires it. Use any placeholder.
- **`{{ env.GITHUB_TOKEN }}`** — resolved from `project.yml` or your shell environment at run time.
- **`build.strategy: incremental`** — upserts on `id` each run, so re-running the same pipeline never creates duplicates.
- **`type: boolean`** — ddt casts GitHub's JSON `true`/`false` to a native Python bool. Similarly, `timestamp` parses ISO 8601 strings with timezone info.

---

## 4. Validate

```bash
uv run ddt validate github_repos
```

```
OK — 'github_repos' (2 params, 0 iterate axes, 12 columns)
```

> **Note:** validate does not check whether your credentials are set. That check happens at run time.

---

## 5. Test with one iteration

```bash
uv run ddt run github_repos --limit 1
```

```
[ddt] Running 'github_repos' — 1 requests

  [1/1]
    12 rows → writing

[ddt] 'github_repos' complete → /your/project/warehouse/github/github_repos/data
```

The `--limit 1` flag restricts to the first iteration (useful when your pipeline iterates over many date ranges or categories). For a single-request pipeline like this one, it behaves identically to a full run.

If your token is missing or wrong, you will see:

```
# Missing token:
OSError: 'GITHUB_TOKEN' is not set — add it as an environment variable or set 'github_token' in project.yml

# Wrong token:
fetch error: 401 Client Error: Unauthorized for url: https://api.github.com/user/repos?...
```

---

## 6. Query the warehouse

Data is written as Parquet files and is immediately queryable with DuckDB (no JVM startup):

```python
import duckdb

conn = duckdb.connect()
df = conn.execute("""
    SELECT name, language, visibility, private
    FROM read_parquet('warehouse/github/github_repos/data/*.parquet')
    ORDER BY name
""").fetchdf()
print(df)
```

Or if you have the MCP server running, use `query_warehouse` and ddt rewrites the table path for you:

```sql
SELECT name, language FROM github.github_repos ORDER BY name
```

---

## 7. Run fully and verify deduplication

```bash
uv run ddt run github_repos
```

Re-run it a second time. For `incremental` pipelines, the row count must stay the same — ddt upserts on `primary_key`, so repeated runs are idempotent:

```python
conn.execute("SELECT COUNT(*) FROM read_parquet('warehouse/github/github_repos/data/*.parquet')").fetchone()
# (12,) — same count every time
```

---

## What's next

- **Iterate over date ranges** — add a `date_range` iterate axis to pull data window by window. Useful for APIs that filter by date (commits, events, logs).
- **Project nested fields** — use dot-notation paths like `owner.login` to extract values from nested objects.
- **Project array fields** — use the `array_join` transform to flatten list fields like `topics` into a comma-separated string.
- **Add a Python connector** — for APIs that need pagination, multi-step auth, or response reshaping, write a `connectors/` function and use `type: python`.
- **Ship to the cloud** — run `ddt gcp setup` to provision a GCS-backed Iceberg lake and set `catalog: gcp` in `project.yml`.
- **Use Claude to build pipelines** — run `ddt mcp setup-desktop` to register the MCP server with Claude Desktop. Claude can then write, validate, and run pipelines on your behalf using the `new-pipeline` skill.

See [README.md](README.md) for the full YAML schema reference and CLI documentation.
