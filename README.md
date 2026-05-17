# dcf

D.ata C.ollection F.ramework

It works like this
1. User defines pipelines with basic configs in a YAML (like a dbt model)
2. dcf builds and runs the pipeline
3. Data lake has data

## Quickstart

This guide walks you from zero to a working data pipeline. The example ingests your GitHub repositories.

### 1. Create a project

dcf is a tool you depend on, not a repo you clone. Create a fresh directory:

```bash
mkdir dcf-demo && cd dcf-demo
```

**`pyproject.toml`:**

```toml
[project]
name = "dcf-demo"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "dcf",
]

[tool.uv]
package = false

[tool.uv.sources]
dcf = { git = "https://github.com/zephschafer/dcf.git" }    
```

**`project.yml`**

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

### 2. Write a pipeline

Create `pipelines/dcf_commits.yml`:

```yaml
name: dcf_commits
namespace: github
description: Commits to the dcf repository.

source:
  type: http
  url: https://api.github.com/repos/zephschafer/dcf/commits
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

---

### 3. Validate

```bash
uv run dcf validate dcf_commits
```

---

### 4. Run

```bash
uv run dcf run dcf_commits
```

```
[dcf] Running 'dcf_commits' — 1 requests

  [1/1]
    30 rows → writing

[dcf] 'dcf_commits' complete → /your/project/warehouse/github/dcf_commits/data
```

---

### 5. Query the warehouse

```bash
uv run dcf query 'SELECT * FROM github.dcf_commits ORDER BY committed_at DESC'
```

You can also save your SQL to a file and run it with `--file`:

```bash
uv run dcf query --file my_query.sql
```

---

### 6. Deploy

```bash
uv run dcf deploy dcf_commits
```

This schedules the pipeline to run daily at 8 AM UTC, as configured in `deployment.schedule`.

---

## Developing dcf

Clone this repo, then create or point to a project for testing:

```bash
git clone https://github.com/Data-Dispatch/dcf
cd dcf
uv sync

# Test against the demo project
git clone https://github.com/Data-Dispatch/quipu-data-generator ../quipu-data-generator
cd ../quipu-data-generator
uv sync   # picks up dcf from ../dcf via editable path dep
uv run dcf validate all
```

Or create a minimal test project:

```bash
mkdir my-test-project && cd my-test-project
cat > pyproject.toml << 'EOF'
[project]
name = "my-test-project"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = ["dcf"]

[tool.uv]
package = false

[tool.uv.sources]
dcf = { path = "../dcf", editable = true }
EOF

cat > project.yml << 'EOF'
catalog: local
EOF

mkdir pipelines
uv sync
uv run dcf validate all   # "OK — 0 pipeline(s)"
```

---

## dcf package structure

```
dcf/
├── cli.py              Entry point (Typer app)
├── project.py          Project root discovery (CWD walk / DCF_PROJECT_DIR)
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
