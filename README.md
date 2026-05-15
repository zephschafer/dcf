# ddt

D.eclarative D.ata T.ool

It works like this
1. User defines pipelines with basic configs in a YAML (like a dbt model)
2. ddt builds and runs the pipeline
3. Data lake has data

## Quickstart

This guide walks you from zero to a working data pipeline. The example ingests your GitHub repositories.

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
]

[tool.uv]
package = false

[tool.uv.sources]
ddt = { git = "https://github.com/zephschafer/ddt.git" }    
```

**`project.yml`** (gitignore this file — it holds your credentials):

```yaml
gh_pat: ghp_YOUR_TOKEN_HERE
catalog: local
```

Set `gh_pat` to a [GitHub personal access token](https://github.com/settings/tokens) with `repo` scope.

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

### 2. Write a pipeline

Create `pipelines/ddt_commits.yml`:

```yaml
name: ddt_commits
namespace: github
description: Commits to the ddt repository.

source:
  type: http
  url: https://api.github.com/repos/zephschafer/ddt/commits
  method: GET
  params:
    - name: sha
      type: string
      value: main
    - name: per_page
      type: integer
      value: 100
  schema:
    columns:
      - {name: sha,          path: sha,                type: string}
      - {name: author,       path: commit.author.name, type: string}
      - {name: message,      path: commit.message,     type: string}
      - {name: committed_at, path: commit.author.date, type: timestamp}

cadence:
  strategy: incremental
  primary_key: sha

deployment:
  schedule: "0 8 * * *"
```

No credentials required — the ddt repo is public.

---

### 3. Validate

```bash
uv run ddt validate ddt_commits
```

> Validate checks the YAML structure but does not verify credentials.

---

### 4. Run

```bash
uv run ddt run ddt_commits
```

```
[ddt] Running 'ddt_commits' — 1 requests

  [1/1]
    30 rows → writing

[ddt] 'ddt_commits' complete → /your/project/warehouse/github/ddt_commits/data
```

---

### 5. Query the warehouse

Data is written as Parquet files and is immediately queryable with DuckDB:

```python
import duckdb

conn = duckdb.connect()
df = conn.execute("""
    SELECT sha, author, message, committed_at
    FROM read_parquet('warehouse/github/ddt_commits/data/*.parquet')
    ORDER BY committed_at DESC
""").fetchdf()
print(df)
```

---

### 6. Deploy

```bash
uv run ddt deploy ddt_commits
```

This schedules the pipeline to run daily at 8 AM UTC, as configured in `deployment.schedule`.

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
│   ├── runner.py       Outer loop (expand cadence → fetch → project → write)
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
