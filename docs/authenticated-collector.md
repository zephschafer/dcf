# Pipelines with authentication

Some APIs require credentials — a bearer token, API key, or similar. This document shows how to configure auth in a pipeline and where to store the credentials.

---

## Storing credentials

dcf resolves `{{ env.VAR }}` placeholders in pipeline YAML from two places, in order:

1. OS environment variable (`export GITHUB_TOKEN=...`)
2. `.env` file key (lowercased, e.g. `github_token: ...`)

For credentials you want to persist across shell sessions, add them to `.env`:

```
GITHUB_TOKEN=ghp_xxxxxxxxxxxx
```

`.env` is gitignored and never committed — it is the right place for API keys.

---

## Example: GitHub commits with a PAT

The quickstart example (`so_questions`) uses the Stack Exchange API with no auth. GitHub's API works fine too, but unauthenticated requests are limited to 60/hour. Adding a PAT raises that to 5,000/hour.

This pipeline ingests commits to `zephschafer/dcf` — the same collector shown in the README, but authenticated.

**`collectors/dcf_commits.yml`:**

```yaml
name: dcf_commits
namespace: github
description: Commits to zephschafer/dcf

source:
  type: http
  url: https://api.github.com/repos/zephschafer/dcf/commits
  method: GET
  auth:
    type: bearer
    key: token       # bearer auth ignores this field; any placeholder works
    value: "{{ env.GITHUB_TOKEN }}"
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
      - name: sha
        path: sha
        type: string
      - name: author
        path: commit.author.name
        type: string
      - name: message
        path: commit.message
        type: string
      - name: committed_at
        path: commit.author.date
        type: timestamp

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

A few things to notice:

- **`auth.key: token`** — bearer auth doesn't use the key field, but the schema requires it. Use any placeholder.
- **`{{ env.GITHUB_TOKEN }}`** — resolved from `.env` or your shell environment at run time.
- **`type: timestamp`** — parses ISO 8601 strings with timezone info into native timestamps.

### What this produces

<table>
<tr>
<td valign="top" width="46%">

**config** (key fields)

```yaml
source:
  type: http
  url: https://api.github.com/repos/zephschafer/dcf/commits
  auth:
    type: bearer
    value: "{{ env.GITHUB_TOKEN }}"
  params:
    - name: per_page
      value: 100
    - name: since
      type: string
    - name: until
      type: string
  schema:
    columns:
      - name: sha
        path: sha
        type: string
      - name: author
        path: commit.author.name
        type: string
      - name: message
        path: commit.message
        type: string
      - name: committed_at
        path: commit.author.date
        type: timestamp

cadence:
  strategy: incremental
  primary_key: sha
  iterate:
    - type: date_range
      params: [since, until]
      start: "2024-01-01"
      end: today
      step: 30 days
```

</td>
<td valign="top" width="54%">

**assembled request** _(one per 30-day window)_

```
GET https://api.github.com/repos/zephschafer/dcf/commits
    ?per_page=100
    &since=2024-01-01T00:00:00Z
    &until=2024-01-30T00:00:00Z
Authorization: Bearer ghp_xxxx...
```

**response**

```json
[
  {"sha": "abc123", "commit": {"author": {"name": "Zeph", "date": "2024-01-05T..."},
                                "message": "feat: add iterator"}, ...},
  ...
]
```

**projected → warehouse** (`incremental` on `sha`)

```
sha      author   message              committed_at
──────── ──────── ──────────────────── ────────────
abc123   Zeph     feat: add iterator   2024-01-05
...
```

**cadence** — one request per 30-day window, upserts on `sha`

```
dcf run dcf_commits
  → warehouse/github/dcf_commits/data/
```

</td>
</tr>
</table>

### Error messages

If your token is missing or wrong:

```
# Missing token:
OSError: 'GITHUB_TOKEN' is not set — add it as an environment variable or set 'github_token' in .env

# Wrong token:
fetch error: 401 Client Error: Unauthorized for url: https://api.github.com/repos/zephschafer/dcf/commits?...
```
