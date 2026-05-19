# Importing collectors

`dcf import` pulls a collector template from dcf-hub, a GitHub repo, or a PyPI package into your project's `collectors/` directory.

---

## Hub collectors

dcf-hub is a curated library of ready-made collector templates. Import one by name:

```bash
dcf import nws           # National Weather Service
dcf import stack_exchange
```

The YAML is copied into `collectors/<name>.yml` and is yours to edit and commit.

---

## Third-party GitHub repos

Anyone can publish a collector as a GitHub repo with a `collector.yml` at the root:

```bash
dcf import alice/dcf-jira
```

This fetches `https://raw.githubusercontent.com/alice/dcf-jira/main/collector.yml` and writes it to `collectors/jira.yml`.

---

## PyPI packages

For connectors that require Python code (OAuth flows, GraphQL, cursor pagination), install via PyPI:

```bash
dcf import pypi:salesforce    # explicit
dcf import salesforce         # also works — hub miss triggers pip install automatically
```

The package is installed and its bundled YAML template is written to `collectors/salesforce.yml`.

---

## Renaming on import

Use `--name` to write the template under a different filename:

```bash
dcf import nws --name weather_portland
# → collectors/weather_portland.yml
```

---

## Required credentials

After importing, dcf prints any `{{ env.VAR }}` placeholders the template needs:

```
[dcf] Imported → collectors/jira.yml

Required env vars:
  export JIRA_TOKEN=...
  export JIRA_DOMAIN=...
```

Set these as environment variables or add them (lowercased) to `project.yml`:

```yaml
catalog: local
jira_token: your-token-here
jira_domain: yourorg.atlassian.net
```

`project.yml` is gitignored and safe for credentials. See [authenticated-collector.md](authenticated-collector.md) for details.

---

## Publishing your own collector

**YAML-only** (simple REST APIs): open a PR to [dcf-hub](https://github.com/zephschafer/dcf-hub) with your `collectors/<name>.yml`.

**Python-backed** (complex auth or pagination): publish a `dcf-<name>` package on PyPI. Ship your YAML template and register it via entry points:

```toml
[project.entry-points."dcf.collectors"]
myconnector = "dcf_myconnector:get_collector_yaml"
```

where `get_collector_yaml` is a callable that returns the YAML string.
