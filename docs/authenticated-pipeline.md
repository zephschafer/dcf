# Pipelines with authentication

Some APIs require credentials — a bearer token, API key, or similar. This document shows how to configure auth in a pipeline and where to store the credentials.

---

## Storing credentials

ddt resolves `{{ env.VAR }}` placeholders in pipeline YAML from two places, in order:

1. OS environment variable (`export GITHUB_TOKEN=...`)
2. `project.yml` key (lowercased, e.g. `github_token: ...`)

For credentials you want to persist across shell sessions, add them to `project.yml`:

```yaml
catalog: local
github_token: ghp_xxxxxxxxxxxx
```

`project.yml` is gitignored and never committed — it is the right place for API keys.

---

## Example: GitHub private repositories

This pipeline ingests your private GitHub repositories using a personal access token.

**`pipelines/github_repos.yml`:**

```yaml
name: github_repos
namespace: github
description: My private GitHub repositories

source:
  type: http
  url: https://api.github.com/user/repos
  method: GET
  auth:
    type: bearer
    key: token       # required by the schema; not used in the request itself
    value: "{{ env.GITHUB_TOKEN }}"
  params:
    - name: visibility
      type: string
      value: private
    - name: per_page
      type: integer
      value: 100
  schema:
    columns:
      - name: id
        path: id
        type: integer
      - name: name
        path: name
        type: string
      - name: full_name
        path: full_name
        type: string
      - name: private
        path: private
        type: boolean
      - name: description
        path: description
        type: string
      - name: language
        path: language
        type: string
      - name: stargazers_count
        path: stargazers_count
        type: integer
      - name: forks_count
        path: forks_count
        type: integer
      - name: created_at
        path: created_at
        type: timestamp
      - name: updated_at
        path: updated_at
        type: timestamp
      - name: default_branch
        path: default_branch
        type: string
      - name: visibility
        path: visibility
        type: string

cadence:
  strategy: incremental
  primary_key: id
```

A few things to notice:

- **`auth.key: token`** — bearer auth doesn't use the key field, but the schema requires it. Use any placeholder.
- **`{{ env.GITHUB_TOKEN }}`** — resolved from `project.yml` or your shell environment at run time.
- **`type: boolean`** — ddt casts GitHub's JSON `true`/`false` to a native Python bool.
- **`type: timestamp`** — parses ISO 8601 strings with timezone info into native timestamps.

### What this produces

<table>
<tr>
<td valign="top" width="46%">

**config** (key fields)

```yaml
source:
  type: http
  url: https://api.github.com/user/repos
  auth:
    type: bearer
    value: "{{ env.GITHUB_TOKEN }}"
  params:
    - name: visibility
      value: private
    - name: per_page
      value: 100
  schema:
    columns:
      - name: id
        path: id
        type: integer
      - name: name
        path: name
        type: string
      # 10 more columns ...

cadence:
  strategy: incremental
  primary_key: id
```

</td>
<td valign="top" width="54%">

**assembled request** _(1 request per run)_

```
GET https://api.github.com/user/repos
    ?visibility=private
    &per_page=100
Authorization: Bearer ghp_xxxx...
```

**response**

```json
[
  {"id": 12345, "name": "my-data", "language": "Python", ...},
  {"id": 67890, "name": "my-api",  "language": "Go",     ...}
]
```

**projected → warehouse** (`incremental` on `id`)

```
id       name      language   ...  (12 columns)
──────── ───────── ──────────
12345    my-data   Python
67890    my-api    Go
```

**cadence** — runs once per `ddt run`, upserts on `id`

```
ddt run github_repos
  → warehouse/github/github_repos/data/
```

</td>
</tr>
</table>

### Error messages

If your token is missing or wrong:

```
# Missing token:
OSError: 'GITHUB_TOKEN' is not set — add it as an environment variable or set 'github_token' in project.yml

# Wrong token:
fetch error: 401 Client Error: Unauthorized for url: https://api.github.com/user/repos?...
```
