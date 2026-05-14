# ddt

A framework for building data lakes from scatch. 
User: Define pipelines by specifying the basic configs (source, destination, API keys) in YAML (like dbt models)
DDT: Builds datalake. Builds and deploys pipeline which writes to datalake.

You install it as a dependency in a separate data project repository. See [quipu-data-generator](https://github.com/zephschafer/quipu) for a working example.

**New to ddt?** Start with the [quickstart guide](QUICKSTART.md) — a step-by-step walkthrough using a real GitHub pipeline.

---

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Java (required by PySpark)

**For GCP cloud lake (`catalog: gcp`) additionally:**
- [Terraform](https://developer.hashicorp.com/terraform/install) v1.x
- A GCP project with **billing enabled**
- `gcloud` CLI authenticated: `gcloud auth application-default login`
- The following GCP APIs enabled on your project:
  ```bash
  gcloud services enable storage.googleapis.com iam.googleapis.com \
    iamcredentials.googleapis.com secretmanager.googleapis.com \
    cloudresourcemanager.googleapis.com --project=YOUR_PROJECT_ID
  ```

---

## Creating a ddt project

A ddt project is a plain directory with this layout:

```
my-data-project/
├── pipelines/           # Pipeline YAML definitions
├── connectors/          # Python connector modules (for type:python sources)
├── warehouse/           # Iceberg data lake — written by ddt (gitignore this)
├── project.yml          # Local config: API keys, catalog type (gitignore this)
├── pyproject.toml       # Depends on ddt; lists any scraper dependencies
└── uv.lock
```

**`pyproject.toml`** for a new project:

```toml
[project]
name = "my-data-project"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "ddt",
    # add scraper dependencies here (e.g. beautifulsoup4)
]

[tool.uv]
package = false

[tool.uv.sources]
ddt = { path = "../ddt", editable = true }  # local dev; swap for a version once published
```

**`.gitignore`** essentials:

```
warehouse/
project.yml
.venv/
__pycache__/
```

**Bootstrap:**

```bash
uv sync
uv run ddt init        # prompts for API keys, region filter, catalog type → writes project.yml
uv run ddt validate all
```

---

## CLI reference

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

---

## Pipeline YAML reference

```yaml
version: 1
name: my_pipeline
description: Optional description

source:
  type: http               # or: python
  url: https://api.example.com/data
  method: GET              # GET | POST

  auth:                    # optional
    type: query_param      # query_param | header | bearer
    key: api_key           # param/header name; omit for bearer (unused)
    value: "{{ env.MY_API_KEY }}"

  params:
    - name: format
      type: string
      value: csv           # static value — always sent with every request

    - name: date_from
      type: date
      format: "%m/%d/%Y"   # no value → covered by iterate below

  iterate:
    # Cartesian product of all iterate axes
    - params: [date_from, date_to]
      type: date_range
      start: "2024-01-01"
      end: today           # or a fixed date; "today" resolves at runtime
      step: 1 day
      window: 1 day        # date_to = date_from + window

    - param: category
      type: categorical
      values: [a, b, c]

  response:
    format: csv            # csv | json
    records_path: data     # dot-path into JSON response to the records array

  rate_limit:              # optional
    requests: 500
    per_minutes: 15

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
      value: 999999                  # static default; override with --param max_records=N
  iterate:
    - param: region
      type: categorical
      values: [portland, eugene, salem, bend, corvallis]
```

The function signature:

```python
def fetch_region(dynamic_params: dict) -> list[dict]:
    region = dynamic_params["region"]
    max_records = dynamic_params["max_records"]
    # ... fetch and return raw records
    return [{"region": region, "price": 1200, ...}, ...]
```

### Build strategies

| `strategy` | Behavior |
|---|---|
| `incremental` | MERGE INTO on `primary_key` — upserts changed rows |
| `append` | Appends every run; no dedup. Use for snapshots and time-series. |
| `full_refresh` | Replaces the entire table on each run |

### Staging + merge pattern

When an API partitions data by a field (e.g. `date_type: review | issued | final`), write each partition to its own staging table and then union+deduplicate them into a single loader table:

```yaml
build:
  strategy: incremental
  primary_key: ivr_number
  staging:
    partition_param: date_type
    table_pattern: "permits_{date_type}_loader_staging"
  merge:
    table: permits_loader
    key: ivr_number
    dedup:
      type: latest_non_null
      columns: [under_review, issued, final]
```

`latest_non_null` dedup: for each `key`, take the most recent non-null value of each listed column across all staging rows. This handles the case where a permit appears in the `review` partition before it has an `issued` date, then later appears in the `issued` partition — the final row will have both dates.

### Env vars in YAML

```yaml
value: "{{ env.MY_API_KEY }}"
```

Resolution order: OS environment variable → `project.yml` key (lowercased).

---

## Warehouse

Data lands in `warehouse/` as Apache Iceberg tables backed by Parquet files. Directory layout:

```
warehouse/
└── <namespace>/
    └── <table>/
        └── data/
            └── *.parquet
```

**With Spark:**

```python
from ddt.spark_session import get_spark
spark = get_spark()

spark.table("local.portland_permits.permits_loader").show()
spark.sql("SELECT neighborhood, COUNT(*) FROM local.craigslist_apts.craigslist_apts GROUP BY 1").show()
```

**With DuckDB (no JVM startup):**

```python
import duckdb
conn = duckdb.connect()
conn.execute("SELECT * FROM read_parquet('warehouse/craigslist_apts/craigslist_apts/data/*.parquet') LIMIT 5").fetchdf()
```

Or use the MCP `query_warehouse` tool, which handles the path rewriting automatically.

---

## Configuration — `project.yml`

Created by `ddt init` at the project root. Gitignored.

```yaml
portlandmaps_api_key: ""   # blank = use built-in default key
valid_regions: []           # empty = all regions; or [portland, eugene, ...]
catalog: local              # local | gcp
```

---

## GCP cloud lake (optional)

```bash
gcloud auth application-default login
uv run ddt gcp setup --project-id my-project --region us-central1
uv run ddt gcp status
```

Updates `project.yml` automatically. Set `catalog: gcp` to route all writes to GCS.

---

## MCP server

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
