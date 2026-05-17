# dcf

[![PyPI](https://img.shields.io/pypi/v/dcf-core)](https://pypi.org/project/dcf-core/)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue)](https://pypi.org/project/dcf-core/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/zephschafer/dcf/blob/main/LICENSE)

D.ata C.ollection F.ramework

```bash
uvx --from dcf-core dcf init
```

---

## How it works

1. **Define** a [data collector in YAML](#example) — source, schema, cadence
2. **Run** it with `dcf run`
3. **Query** data from your data lake

---

## Quickstart

```bash
mkdir my-project && cd my-project
uvx --from dcf-core dcf init
uv sync
uv run dcf run dcf_commits
uv run dcf query 'SELECT * FROM github.dcf_commits'
```

`dcf init` creates `pyproject.toml`, `project.yml`, `.gitignore`, `collectors/`, and an example collector.

---

## Example

### dcf collector:

```yaml
name: dcf_commits
namespace: github

source:
  type: http
  url: https://api.github.com/repos/zephschafer/dcf/commits
  method: GET
  params:
    - name: per_page
      type: integer
      value: 100
    - name: since
      type: string
    - name: until
      type: string
  schema:
    columns:
      - {name: sha,          path: sha,                type: string}
      - {name: author,       path: commit.author.name, type: string}
      - {name: message,      path: commit.message,     type: string}
      - {name: committed_at, path: commit.author.date, type: timestamp}

cadence:
  strategy: incremental
  primary_key: sha
  iterate:
    - type: date_range
      params: [since, until]
      start: "2024-01-01"
      end: today
      step: 30 days

deployment:
  schedule: "0 8 * * *"
```

### dcf run
```bash
uv run dcf run dcf_commits
```

### dcf query
```bash
uv run dcf query 'SELECT * FROM github.dcf_commits LIMIT 5'
```

---

## Contributing

```bash
git clone https://github.com/zephschafer/dcf && cd dcf && uv sync
```

Point a local project at your checkout:

```toml
[tool.uv.sources]
dcf-core = { path = "../dcf", editable = true }
```

To verify changes:

```bash
uv run dcf run dcf_commits
uv run dcf query 'SELECT * FROM github.dcf_commits'
```

**Releasing:** bump `version` in `pyproject.toml` and push to main — GitHub Actions publishes to PyPI automatically.

---

## Package structure

```
dcf/
├── cli.py              Entry point (Typer)
├── config/
│   ├── models.py       Pydantic models for collector YAML
│   └── loader.py       YAML loading + env var resolution
├── engine/
│   ├── runner.py       Outer loop (iterate → fetch → project → write)
│   ├── fetcher.py      HTTP and Python source fetchers
│   ├── iterator.py     Date range and categorical iteration
│   ├── projector.py    Schema projection and path extraction
│   └── transforms.py   Column transforms
├── writer/
│   └── iceberg.py      Write strategies (incremental / append / full_refresh)
└── gcp/                GCP auth, provisioning, Terraform wrappers
```
