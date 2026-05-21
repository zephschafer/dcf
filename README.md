# dcf -- D.ata C.ollection F.ramework

[![PyPI](https://img.shields.io/pypi/v/dcf-core)](https://pypi.org/project/dcf-core/)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue)](https://pypi.org/project/dcf-core/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/zephschafer/dcf/blob/main/LICENSE)

```bash
uvx --from dcf-core dcf init
```

---

## How it works

1. **Define** a [data collector in YAML](#example) for any http data source
2. **Run** it with `dcf run`
3. **Query** your new data

<p align="center">
  <a href="https://www.youtube.com/watch?v=CeJj3yc6HaI" style="font-size: 1.25em; font-weight: bold; text-decoration: none;">
    ▶️ Video Tutorial: Use DCF to Query Public APIs
  </a>
  <br><br>
  <a href="https://www.youtube.com/watch?v=CeJj3yc6HaI">
    <img src="https://img.youtube.com/vi/CeJj3yc6HaI/maxresdefault.jpg" alt="Use DCF to Query Public APIs" width="50%">
  </a>
</p>

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

`dcf init` creates `pyproject.toml`, `profiles.yml`, `.gitignore`, `collectors/`, and an example collector.

---

## Example

### dcf collector

```yaml
name: so_questions
namespace: stackoverflow

source:
  type: http
  url: https://api.stackexchange.com/2.3/questions
  response:
    records_path: items
  params:
    - name: site
      type: string
      value: stackoverflow
    - name: tagged
      type: string
      value: "python;data-engineering"
  schema:
    columns:
      - name: question_id
        path: question_id
        type: integer
      - name: title
        path: title
        type: string
      - name: creation_date
        path: creation_date
        type: integer

cadence:
  strategy: incremental
  primary_key: question_id
  iterate:
    - type: date_range
      params: [fromdate, todate]
      format: "%s"
      start: "2025-01-01"
      end: today
      step: 30 days
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

## More features

- [Create a collector with Claude Code](docs/create-a-new-collector-w-claude.md) — `/new-collector` skill, installed by `dcf init`
- [Importing collectors from dcf-hub, GitHub, or PyPI](docs/collector-import.md) — `dcf import`
- [Authenticated collectors](docs/authenticated-collector.md) — bearer tokens, API keys, `{{ env.VAR }}`
- [Collector config reference](docs/collector-config.md) — full YAML field reference

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
