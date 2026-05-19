# Importing collectors

`dcf import` pulls a collector template from dcf-hub, a GitHub repo, or a PyPI package into your project's `collectors/` directory.

## Publishing your own collector

Choose the path that fits your use case:

- [Contributing a hub collector](contributing-hub-collector.md) — YAML only, open a PR to dcf-hub
- [Publishing a GitHub repo collector](contributing-github-collector.md) — share in your own repo, no approval needed
- [Publishing a PyPI package](contributing-pypi-collector.md) — Python-backed connectors with custom auth or pagination

---

## [Hub collectors]((contributing-hub-collector.md)

dcf-hub is a curated library of ready-made collector templates. Import one by name:

```bash
dcf import nws           # National Weather Service
dcf import stack_exchange
```

The YAML is copied into `collectors/<name>.yml` and is yours to edit and commit.

---

## [Third-party GitHub repos](contributing-github-collector.md)

Anyone can publish a collector as a GitHub repo with a `collector.yml` at the root:

```bash
dcf import alice/dcf-jira
```

This fetches `https://raw.githubusercontent.com/alice/dcf-jira/main/collector.yml` and writes it to `collectors/jira.yml`.

---

## [PyPI packages](contributing-pypi-collector.md)

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

Set these as environment variables or add them to `.env`:

```
JIRA_TOKEN=your-token-here
JIRA_DOMAIN=yourorg.atlassian.net
```

`.env` is gitignored and safe for credentials. See [authenticated-collector.md](authenticated-collector.md) for details.

---

## Publishing your own collector

Choose the path that fits your use case:

- [Contributing a hub collector](contributing-hub-collector.md) — YAML only, open a PR to dcf-hub
- [Publishing a GitHub repo collector](contributing-github-collector.md) — share in your own repo, no approval needed
- [Publishing a PyPI package](contributing-pypi-collector.md) — Python-backed connectors with custom auth or pagination
