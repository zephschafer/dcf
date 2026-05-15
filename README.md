# ddt

Declarative Data Tool

It works like this
1. User defines pipelines with basic configs in a YAML (like a dbt model)
2. ddt builds and runs the pipeline
3. Data lake has data

## Quickstart 

This guide walks you from zero to a working data pipeline. The example ingests your private GitHub repositories — it covers credentials, schema projection, and warehouse querying in a single concrete run.

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Java (required by PySpark — `java -version` to check)

### 1. Create a project

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
dependencies = [
    "ddt",
    # add scraper dependencies here (e.g. beautifulsoup4)
]

[tool.uv]
package = false

[tool.uv.sources]
pvc = { git = "https://github.com/zephschafer/ddt.git" }    
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
uv run ddt init        # prompts for API keys, region filter, catalog type → writes project.yml
uv run ddt validate all
```

---

### 2. Store your credentials

```bash
# Run a pipeline
uv run ddt run <name>
uv run ddt run all

# Date range override (for pipelines with date_range iterate)
uv run ddt run portland_permits --start 2024-01-01 --end 2024-03-31

# Limit to first N iterations (useful for testing)
uv run ddt run craigslist_apts --limit 1

# Override a param at runtime
uv run ddt run craigslist_apts --limit 1 --param max_records=5

# Validate YAML without running
uv run ddt validate <name>
uv run ddt validate all

# GCP cloud lake
uv run ddt gcp setup --project-id <id> --region us-central1
uv run ddt gcp status
uv run ddt gcp teardown                  # destroys all GCP resources, resets to catalog: local

# MCP server (for Claude integration)
uv run ddt mcp serve
uv run ddt mcp setup-desktop   # registers ddt in Claude Desktop's config
```

> `project.yml` is gitignored and never committed. It is the right place for API keys.

---

### 3. Write a pipeline

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
    - name: permit_id
      path: PERMIT_ID      # key in raw record; dot-notation for nested JSON
      type: string

    - name: lon
      transform:
        type: crs_reproject
        from_columns: [X_MERCATOR, Y_MERCATOR]
        from_crs: EPSG:3857
        to_crs: EPSG:4326
        component: x       # x = longitude, y = latitude

build:
  strategy: incremental    # incremental | append | full_refresh
  primary_key: permit_id

  staging:                 # optional — write to partition-specific staging tables first
    partition_param: category
    table_pattern: "my_pipeline_{category}_staging"

  merge:                   # optional — union staging tables into one deduplicated table
    table: my_pipeline_loader
    key: permit_id
    dedup:
      type: latest_non_null
      columns: [reviewed_at, issued_at, finaled_at]
```

### Source types

**`type: http`** — structured API responses (JSON or CSV). ddt constructs the request, handles auth, and parses the response.

**`type: python`** — anything that needs custom logic: HTML scraping, multi-step auth, pagination that depends on response content. Write a Python function in `connectors/`; ddt calls it for each iteration.

```yaml
source:
  type: python
  module: connectors.craigslist_apts   # importable from the project root
  function: fetch_region             # called as fn(dynamic_params) → list[dict]
  params:
    - name: region
      type: string
    - name: max_records
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

### 4. Validate

```bash
uv run ddt validate github_repos
```

**With Spark:**

```python
from ddt.spark_session import get_spark
spark = get_spark()

spark.table("local.portland_permits.permits_loader").show()
spark.sql("SELECT neighborhood, COUNT(*) FROM local.craigslist_apts.craigslist_apts GROUP BY 1").show()
```

> **Note:** validate does not check whether your credentials are set. That check happens at run time.

---

### 5. Test with one iteration

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

### 6. Query the warehouse

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

---

## Configuration — `project.yml`

Created by `ddt init` at the project root. Gitignored.

```yaml
portlandmaps_api_key: ""   # blank = use built-in default key
valid_regions: []           # empty = all regions; or [portland, eugene, ...]
catalog: local              # local | gcp
```

---

### 7. Run fully and verify deduplication

```bash
gcloud auth application-default login
uv run ddt gcp setup --project-id my-project --region us-central1
uv run ddt gcp status
```

Re-run it a second time. For `incremental` pipelines, the row count must stay the same — ddt upserts on `primary_key`, so repeated runs are idempotent:

```python
conn.execute("SELECT COUNT(*) FROM read_parquet('warehouse/github/github_repos/data/*.parquet')").fetchone()
# (12,) — same count every time
```

---

## What's next

ddt exposes an MCP server so Claude can build pipelines interactively:

```bash
# Register with Claude Desktop (run once from your project directory)
uv run ddt mcp setup-desktop

# Or start manually
uv run ddt mcp serve
```

**Available tools:** `list_pipelines`, `get_pipeline`, `validate_pipeline`, `run_pipeline`, `list_warehouse_tables`, `query_warehouse`, `write_pipeline`, `write_scraper`.

The `.claude/commands/` directory in your project repo can hold Claude skills (slash commands) that guide the pipeline creation workflow. See the demo project for working examples.

---

## Developing ddt

Clone this repo, then create or point to a project for testing:

```bash
git clone https://github.com/Data-Dispatch/ddt
cd ddt
uv sync

# Test against the demo project
git clone https://github.com/Data-Dispatch/quipu-data-generator ../quipu-data-generator
cd ../quipu-data-generator
uv sync   # picks up ddt from ../ddt via editable path dep
uv run ddt validate all
```

Or create a minimal test project:

```bash
mkdir my-test-project && cd my-test-project
cat > pyproject.toml << 'EOF'
[project]
name = "my-test-project"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = ["ddt"]

[tool.uv]
package = false

[tool.uv.sources]
ddt = { path = "../ddt", editable = true }
EOF

cat > project.yml << 'EOF'
catalog: local
EOF

mkdir pipelines
uv sync
uv run ddt validate all   # "OK — 0 pipeline(s)"
```

---

## ddt package structure

```
ddt/
├── cli.py              Entry point (Typer app)
├── project.py          Project root discovery (CWD walk / DDT_PROJECT_DIR)
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
