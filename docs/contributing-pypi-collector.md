# Publishing a PyPI collector package

Some connectors can't be expressed in YAML alone — they need custom Python for OAuth token refresh, cursor-based pagination, GraphQL, or binary response formats. This guide covers packaging those connectors for distribution via PyPI.

**Use this path when:** your connector requires a Python function to fetch data that `type: http` can't handle.

---

## Package structure

```
dcf-jira/
  dcf_jira/
    __init__.py           # fetch function + get_collector_yaml()
    collectors/
      jira.yml            # YAML template shipped with the package
  pyproject.toml
```

---

## The YAML template

Write a standard dcf collector YAML and save it at `dcf_jira/collectors/jira.yml`. Since the connector uses a Python function, the source type is `python`:

```yaml
name: jira_issues
namespace: jira
description: Issues from a Jira project, fetched via the Python connector.

source:
  type: python
  module: dcf_jira
  function: fetch
  params:
    - name: project_key
      type: string
      value: MYPROJECT
  schema:
    columns:
      - name: id
        path: id
        type: string
      - name: key
        path: key
        type: string
      - name: summary
        path: fields.summary
        type: string
      - name: status
        path: fields.status.name
        type: string

cadence:
  strategy: incremental
  primary_key: id
```

Use `{{ env.VAR }}` for any credentials — they'll be resolved at run time from the user's environment or `.env`.

---

## The Python module

`dcf_jira/__init__.py` needs two things: the `fetch` function dcf will call, and a `get_collector_yaml` function that returns the YAML template as a string.

```python
import importlib.resources


def fetch(params: dict) -> list[dict]:
    """Called by dcf for each iteration step. Returns list of raw records."""
    import requests

    domain = params.get("JIRA_DOMAIN") or __import__("os").environ["JIRA_DOMAIN"]
    token  = params.get("JIRA_TOKEN")  or __import__("os").environ["JIRA_TOKEN"]

    resp = requests.get(
        f"https://{domain}/rest/api/3/search",
        headers={"Authorization": f"Bearer {token}"},
        params={"jql": f"project = {params['project_key']} ORDER BY created ASC",
                "maxResults": 100},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["issues"]


def get_collector_yaml() -> str:
    return (importlib.resources.files("dcf_jira") / "collectors" / "jira.yml").read_text()
```

---

## `pyproject.toml`

```toml
[project]
name = "dcf-jira"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = ["requests"]

[project.entry-points."dcf.collectors"]
jira = "dcf_jira:get_collector_yaml"

[tool.setuptools.package-data]
dcf_jira = ["collectors/*.yml"]
```

The entry point tells dcf how to find and load the bundled YAML after installation.

---

## Naming

| Thing | Convention | Example |
|---|---|---|
| PyPI package | `dcf-<name>` | `dcf-jira` |
| Python module | `dcf_<name>` | `dcf_jira` |
| Collector name in YAML | your choice | `jira_issues` |

---

## Test locally

```bash
# Install your package in editable mode
pip install -e .

# Import it into a dcf project
dcf import pypi:jira

# Set credentials and run
export JIRA_TOKEN=...
export JIRA_DOMAIN=yourorg.atlassian.net
dcf run jira_issues
```

---

## Publish

```bash
python -m build
twine upload dist/*
```

Once published, users install and import in one step:

```bash
dcf import jira
```
