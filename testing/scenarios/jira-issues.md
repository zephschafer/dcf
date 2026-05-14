# Scenario: Jira Issues (Categorical Iteration + Nested Fields)

## Goal

Build a pipeline that ingests issues from multiple Jira projects, iterating over
projects categorically. Jira is Fivetran's most-requested enterprise connector.
This scenario tests: per-project categorical iteration, highly nested response
fields, and the cartesian product of date × project when combining iterate axes.

**Auth note:** This scenario uses Jira API token auth (Basic auth with email + token),
not OAuth 2.0. OAuth is a separate, harder problem. This lets us focus on the data
shape and iteration complexity first.

## Target API

Jira REST API v3 — Search for issues using JQL

```
GET https://<your-domain>.atlassian.net/rest/api/3/search
  ?jql=project=<KEY> AND updated >= "<YYYY-MM-DD>" AND updated <= "<YYYY-MM-DD>"
  &maxResults=100
  &startAt=<offset>
  &fields=id,key,summary,status,assignee,priority,created,updated,issuetype,labels
```

Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-search/

Response shape:
```json
{
  "startAt": 0,
  "maxResults": 100,
  "total": 250,
  "issues": [
    {
      "id": "10001",
      "key": "PROJ-123",
      "fields": {
        "summary": "...",
        "status": { "name": "In Progress" },
        "assignee": { "displayName": "Alice Smith", "emailAddress": "alice@..." },
        "priority": { "name": "High" },
        "issuetype": { "name": "Bug" },
        "labels": ["backend", "urgent"],
        "created": "2024-01-01T00:00:00.000+0000",
        "updated": "2024-01-15T00:00:00.000+0000"
      }
    }
  ]
}
```

Pagination: offset-based (`startAt` param + `total` in response). NOT Link headers.

Auth: Basic auth — base64(`email:api_token`). API token is NOT an OAuth token.

## Test Phases

### Phase 1 — Single Project, YAML Path

Start with a single project to validate basic Jira field extraction:

1. Write `pipelines/jira_issues.yml` using `type: http`, `records_path: issues`,
   single `categorical` iterate axis for `project_key` with one value
2. Add `date_range` iterate axis on `updated` date params (JQL date params)
3. Schema: issue_id (from id), issue_key (key), summary (fields.summary),
   status (fields.status.name), assignee (fields.assignee.displayName, nullable),
   priority (fields.priority.name, nullable), issue_type (fields.issuetype.name),
   labels (array_join of fields.labels), created (fields.created), updated (fields.updated)
4. Run `ddt validate jira_issues`
5. Run `ddt run jira_issues --limit 1` — one project × one date window
6. Verify: `fields.status.name` extracted correctly (3 levels deep via dot-notation)
7. Verify: `fields.assignee.displayName` is nullable when unassigned

Phase 1 success: single project YAML pipeline works with nested field extraction.

### Phase 2 — Multi-Project Categorical Iteration

Expand to multiple projects using the `categorical` iterate axis:

1. Update pipeline: `categorical` iterate axis over 2-3 project keys
2. Combine with the `date_range` axis — verify cartesian product (N projects × M windows)
3. Run `ddt run jira_issues --limit 1` — should run first project × first date window
4. Run full pipeline
5. Verify row counts: run twice, confirm idempotent on `id` primary key
6. Query warehouse: count issues per project, per status

Phase 2 success: categorical × date_range cartesian product works correctly.

### Phase 3 — Pagination Characterization

Jira uses offset pagination (`startAt`/`total`). Test ddt's behavior:

1. Find a project + date range with >100 issues (`total > maxResults` in response)
2. Run the pipeline — how many rows does ddt return? Is it capped at 100?
3. Document: offset pagination limitation (same root cause as Link header pagination)

Phase 3 success: limitation documented with exact counts.

## Success Criteria

- [ ] Phase 1: Pipeline validates successfully with Jira JQL date params
- [ ] Phase 1: `--limit 1` fetches real Jira issues
- [ ] Phase 1: `fields.status.name` extracted via dot-notation (3 levels)
- [ ] Phase 1: `fields.assignee.displayName` nullable (null when unassigned)
- [ ] Phase 1: `fields.labels` flattened to string via array_join
- [ ] Phase 2: Categorical × date_range cartesian product generates correct request count
- [ ] Phase 2: Incremental deduplication on `id` stable across re-runs
- [ ] Phase 2: Warehouse queryable — `SELECT project_key, status, COUNT(*) FROM jira.jira_issues GROUP BY project_key, status`
- [ ] Phase 3: Pagination limitation confirmed with exact behavior

## Known Complexity

- **JQL date syntax:** Jira's JQL uses `"YYYY-MM-DD"` string format (not ISO 8601 with time).
  Confirm ddt's `date_range` iterate axis can format params as `YYYY-MM-DD`.
- **Deeply nested fields:** `fields.status.name`, `fields.assignee.displayName`,
  `fields.priority.name` are all 3 levels deep. This is the deepest nesting tested so far.
- **Null objects:** `assignee` can be null (not just `displayName` being null — the entire
  `assignee` object is null for unassigned issues). ddt's dot-path extractor must handle
  null intermediate objects gracefully.
- **Offset pagination:** Different mechanism from Link headers but same Blocking limitation.
  `startAt` must be incremented by `maxResults` each request — cannot be expressed in YAML.
- **Timestamp format:** Jira timestamps include timezone offset (`+0000`) not `Z`. Confirm
  ddt's `timestamp` type handles both formats.
- **`issue_id` as string:** Jira's `id` field is a string ("10001"), not an integer.
  Use `type: string` for the primary key.

## Known Expected Findings (Pre-identified)

- **Expected Blocking (Schema):** Offset pagination (`startAt`/`total`) cannot be
  expressed in ddt YAML — same root cause as Link header pagination.
- **To investigate:** Does ddt's `date_range` iterate axis format support `YYYY-MM-DD`
  for JQL params, or only ISO 8601 with time component?
- **To investigate:** Null intermediate objects — if `assignee` is null, does
  `fields.assignee.displayName` return None or raise an error?
- **Enhancement:** `categorical` axis requires hardcoding project keys in YAML.
  A real Fivetran Jira connector would discover all projects dynamically. ddt cannot
  do this — would require a Python connector.

## Credentials Required

Jira API token — NOT an OAuth token. Generate at:
https://id.atlassian.com/manage-profile/security/api-tokens

Store as env vars:
- `JIRA_EMAIL` — the Atlassian account email
- `JIRA_API_TOKEN` — the API token
- `JIRA_DOMAIN` — the Atlassian domain (e.g., `yourcompany.atlassian.net`)

Auth header: `Authorization: Basic <base64("email:token")>`

In ddt YAML:
```yaml
auth:
  type: header
  key: Authorization
  value: "Basic {{ env.JIRA_AUTH_B64 }}"
```
Where `JIRA_AUTH_B64` is pre-computed as `base64("email:token")`.

Alternative: compute the base64 encoding inside a Python connector.

Zeph provides credentials. Set in `project.yml` before the test.

## By Design Decisions from Prior Runs

(None yet — first run of this scenario)

## Notes for Agent

- The test project is `/Users/zephschafer/Documents/GitHub/quipu/`
- Use `namespace: jira` — routes to `warehouse/jira/jira_issues/`
- The `url` in the pipeline YAML must include the full domain:
  `https://{{ env.JIRA_DOMAIN }}/rest/api/3/search`
- For Phase 1, use a project that has at least 10 issues but is unlikely to exceed
  100 per date window (to defer the pagination finding to Phase 3)
- For Phase 3, use a project with high volume or a wide date window that is known
  to have >100 results, to confirm the offset pagination cap
- The `issue_key` field (e.g., "PROJ-123") is more human-readable than `id` — include
  both in the schema and make `id` (string) the primary key
- If base64 pre-computation for the auth header is awkward, suggest a Python connector
  for Phase 2 that handles auth internally — this is a useful finding regardless
