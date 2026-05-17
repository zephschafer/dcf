# dcf

D.ata C.ollection F.ramework

It works like this
1. User defines collectors with basic configs in a YAML (like a dbt model)
2. dcf builds and runs the collector
3. Data lake has data

## Quickstart

This guide walks you from zero to a working data collector. The example ingests your GitHub repositories.

### 1. Create a project

dcf is a tool you depend on, not a repo you clone. Create a fresh directory and run `init`:

```bash
mkdir dcf-demo && cd dcf-demo
uvx --from dcf-core dcf init
uv sync
```

This creates `pyproject.toml`, `project.yml`, `.gitignore`, `collectors/`, and an example collector at `collectors/dcf_commits.yml`.

---

### 2. Validate

```bash
uv run dcf validate dcf_commits
```

---

### 3. Run

```bash
uv run dcf run dcf_commits
```

---

### 4. Query the warehouse

```bash
uv run dcf query 'SELECT * FROM github.dcf_commits'
```

You can also save your SQL to a file and run it with `--file`:

```bash
uv run dcf query --file my_query.sql
```

---

### 5. Deploy

```bash
uv run dcf deploy dcf_commits
```

This schedules the collector to run daily at 8 AM UTC, as configured in `deployment.schedule`.

---

## Contributing

```bash
git clone https://github.com/zephschafer/dcf
cd dcf
uv sync
```

To test against a local project, point its `pyproject.toml` at your checkout:

```toml
[tool.uv.sources]
dcf-core = { path = "../dcf", editable = true }
```

Then run `uv sync` in that project and use `uv run dcf` as normal.

**Releasing:** bump `version` in `pyproject.toml` and push to main — GitHub Actions publishes to PyPI automatically.

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
│   ├── models.py       Pydantic models for collector YAML
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
