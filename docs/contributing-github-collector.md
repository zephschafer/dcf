# Publishing a GitHub repo collector

Any GitHub repo can serve as a dcf collector source — no approval or coordination required. This is the right path when your collector is too niche for dcf-hub, when you want your own versioning and release control, or when the collector lives alongside other code in an existing repo.

**Use this path when:** you want to share a collector independently without going through the hub, or the collector is specific to your org/tool.

---

## Repo conventions

`dcf import user/repo` fetches `collector.yml` from the root of the repo on the `main` branch. Follow these conventions so users can import cleanly:

**Name the repo `dcf-<name>`** — this is just a convention, but it makes the collector discoverable and gives dcf a sensible default output filename. `dcf import alice/dcf-jira` writes to `collectors/jira.yml`.

**Place your YAML at the repo root as `collector.yml`.**

**Add a README** documenting:
- What data is collected
- Required env vars (name and where to get them)
- An example `dcf import` command

A minimal repo looks like:

```
dcf-jira/
  collector.yml
  README.md
```

---

## Write the collector YAML

Same format as any dcf collector. See the [collector config reference](collector-config.md). Use `{{ env.VAR }}` for any credentials:

```yaml
name: jira_issues
namespace: jira
description: Open issues from a Jira project.

source:
  type: http
  url: https://{{ env.JIRA_DOMAIN }}/rest/api/3/search
  method: GET
  auth:
    type: bearer
    value: "{{ env.JIRA_TOKEN }}"
  params:
    - {name: jql,        type: string, value: "project = MYPROJECT ORDER BY created ASC"}
    - {name: maxResults, type: integer, value: 100}
    - {name: startAt,    type: integer, value: 0}
  response:
    format: json
    records_path: issues
  schema:
    columns:
      - {name: id,      path: id,                type: string}
      - {name: key,     path: key,               type: string}
      - {name: summary, path: fields.summary,    type: string}
      - {name: status,  path: fields.status.name, type: string}

cadence:
  strategy: incremental
  primary_key: id
```

---

## Test it

```bash
dcf import yourusername/dcf-jira
```

dcf will fetch `https://raw.githubusercontent.com/yourusername/dcf-jira/main/collector.yml` and write it to `collectors/jira.yml`.

---

## Sharing

Users import directly from your repo — no registry, no approval:

```bash
dcf import alice/dcf-jira
```

Share the import command in your README or wherever your users find it.
