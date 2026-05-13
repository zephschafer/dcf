# pvc

A framework for building data lakes from scatch. 

It works like this
1. User defines pipelines with basic configs in a YAML (like a dbt model)
2. PVC builds and runs the pipeline
3. Data lake has data

---

## Quickstart 

# pvc Quickstart

This guide walks you from zero to a working data pipeline. The example ingests your private GitHub repositories — it covers credentials, schema projection, and warehouse querying in a single concrete run.

---

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Java (required by PySpark — `java -version` to check)

---

## 1. Create a project

pvc is a tool you depend on, not a repo you clone. Create a fresh directory:

```bash
mkdir my-data && cd my-data
```

**`pyproject.toml`:**

```toml
[project]
name = "my-data"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = ["pvc"]

[tool.uv]
package = false
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

pvc resolves `{{ env.VAR }}` placeholders in pipeline YAML from two places, in order:

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
- **`type: boolean`** — pvc casts GitHub's JSON `true`/`false` to a native Python bool. Similarly, `timestamp` parses ISO 8601 strings with timezone info.

---

## 4. Validate

```bash
uv run pvc validate github_repos
```

```
OK — 'github_repos' (2 params, 0 iterate axes, 12 columns)
```

> **Note:** validate does not check whether your credentials are set. That check happens at run time.

---

## 5. Test with one iteration

```bash
uv run pvc run github_repos --limit 1
```

```
[pvc] Running 'github_repos' — 1 requests

  [1/1]
    12 rows → writing

[pvc] 'github_repos' complete → /your/project/warehouse/github/github_repos/data
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

Or if you have the MCP server running, use `query_warehouse` and pvc rewrites the table path for you:

```sql
SELECT name, language FROM github.github_repos ORDER BY name
```

---

## 7. Run fully and verify deduplication

```bash
uv run pvc run github_repos
```

Re-run it a second time. For `incremental` pipelines, the row count must stay the same — pvc upserts on `primary_key`, so repeated runs are idempotent:

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
- **Ship to the cloud** — run `pvc gcp setup` to provision a GCS-backed Iceberg lake and set `catalog: gcp` in `project.yml`.
- **Use Claude to build pipelines** — run `pvc mcp setup-desktop` to register the MCP server with Claude Desktop. Claude can then write, validate, and run pipelines on your behalf using the `new-pipeline` skill.

---

## Developing pvc

Clone this repo, then create or point to a project for testing:

```bash
git clone https://github.com/Data-Dispatch/pvc
cd pvc
uv sync

# Test against the demo project
git clone https://github.com/Data-Dispatch/quipu-data-generator ../quipu-data-generator
cd ../quipu-data-generator
uv sync   # picks up pvc from ../pvc via editable path dep
uv run pvc validate all
```

Or create a minimal test project:

```bash
mkdir my-test-project && cd my-test-project
cat > pyproject.toml << 'EOF'
[project]
name = "my-test-project"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = ["pvc"]

[tool.uv]
package = false

[tool.uv.sources]
pvc = { path = "../pvc", editable = true }
EOF

cat > project.yml << 'EOF'
catalog: local
EOF

mkdir pipelines
uv sync
uv run pvc validate all   # "OK — 0 pipeline(s)"
```

---

## pvc package structure

```
pvc/
├── cli.py              Entry point (Typer app)
├── project.py          Project root discovery (CWD walk / PVC_PROJECT_DIR)
├── spark_session.py    PySpark + Iceberg session factory
├── mcp_server.py       MCP server (FastMCP)
├── warehouse_reader.py DuckDB-based warehouse query layer
├── config/
│   ├── models.py       Pydantic models for pipeline YAML
│   └── loader.py       YAML loading + env var resolution
├── engine/
│   ├── runner.py       Outer loop (iterate → fetch → project → write)
│   ├── fetcher.py      HTTP and Python source fetchers
│   ├── iterator.py     Cartesian iteration over date ranges and categoricals
│   ├── projector.py    Schema projection (path extraction, transforms)
│   └── transforms.py   Column transforms (crs_reproject, etc.)
├── writer/
│   └── iceberg.py      Iceberg write strategies (incremental / append / full_refresh)
└── gcp/
    ├── bootstrap.py    GCS bucket + service account provisioning
    ├── terraform.py    Terraform wrapper for lake infrastructure
    ├── auth.py         GCP credential helpers
    └── gcloud.py       gcloud CLI wrappers
```
