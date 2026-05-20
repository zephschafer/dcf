# Create a collector with Claude Code

`dcf init` installs a `/new-collector` skill into your project's `.claude/commands/` directory. In Claude Code, typing `/new-collector` launches an interactive guide that walks you through building a collector end-to-end.

## How to use it

1. Run `dcf init` in your project root (if you haven't already).
2. Open Claude Code in that directory.
3. Type `/new-collector` and describe the data source you want to ingest.

Claude will ask clarifying questions and guide you through each step: credentials, probing the API, choosing a source type, writing the YAML (and connector if needed), running a test, and verifying the data.

## What it covers

- Checking and storing credentials (`bearer`, `header`, `query_param`)
- Probing the API to determine whether to use `type: http` or `type: python`
- Writing `collectors/{name}.yml` and, for Python collectors, `connectors/{name}.py`
- Testing with `--limit 1`, querying the result, and confirming dedup on re-run
- Optional GCP deployment

## When to use this vs. the manual docs

Use `/new-collector` when building something from scratch — it's faster and handles edge cases (cursor pagination, GraphQL, HTML scraping) that would otherwise require reading multiple doc pages.

Use the manual docs when you want to understand a specific config option, edit an existing collector, or read before writing.

---

- [Create a collector (manual)](create-a-collector.md)
- [Collector config reference](collector-config.md)
