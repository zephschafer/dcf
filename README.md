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

<div>
    <a href="https://www.loom.com/share/e3fac8ddb842438cb38beaf07f8a43a4">
      <p>Run a DCF Collector with GitHub API - Watch Video</p>
    </a>
    <a href="https://www.loom.com/share/e3fac8ddb842438cb38beaf07f8a43a4">
      <img style="max-width:300px;" src="https://cdn.loom.com/sessions/thumbnails/e3fac8ddb842438cb38beaf07f8a43a4-9e0609f1fe8f65c7-full-play.gif#t=0.1">
    </a>
</div>

---

## Quickstart
#### Get real data. From an API. Into your Lakehouse. Query it with SQL. In 5 lines.

```bash
mkdir dcf-demo && cd dcf-demo
uvx --from dcf-core dcf init
uv sync
uv run dcf run so_questions
uv run dcf query 'SELECT * FROM stackoverflow.so_questions'
```

`dcf init` creates `pyproject.toml`, `project.yml`, `.gitignore`, `collectors/`, and an example collector.

---

## Example

### dcf collector

```yaml
name: so_questions
namespace: stackoverflow

source:
  type: http
  url: https://api.stackexchange.com/2.3/questions
  method: GET
  response:
    records_path: items
  params:
    - {name: site,     type: string,  value: stackoverflow}
    - {name: tagged,   type: string,  value: "python;data-engineering"}
    - {name: order,    type: string,  value: asc}
    - {name: sort,     type: string,  value: creation}
    - {name: pagesize, type: integer, value: 100}
    - {name: fromdate, type: string,  format: "%s"}
    - {name: todate,   type: string,  format: "%s"}
  schema:
    columns:
      - {name: question_id,   path: question_id,   type: integer}
      - {name: title,         path: title,         type: string}
      - {name: score,         path: score,         type: integer}
      - {name: answer_count,  path: answer_count,  type: integer}
      - {name: view_count,    path: view_count,    type: integer}
      - {name: creation_date, path: creation_date, type: integer}
      - {name: link,          path: link,          type: string}

cadence:
  strategy: incremental
  primary_key: question_id
  iterate:
    - type: date_range
      params: [fromdate, todate]
      start: "2024-01-01"
      end: today
      step: 30 days

deployment:
  schedule: "0 8 * * *"
```

### dcf run
```bash
uv run dcf run so_questions
```

### dcf query
```bash
uv run dcf query 'SELECT * FROM stackoverflow.so_questions LIMIT 5'
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
uv run dcf run so_questions
uv run dcf query 'SELECT * FROM stackoverflow.so_questions'
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
