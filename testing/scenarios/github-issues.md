# Scenario: GitHub Issues (Pagination + Incremental Sync)

## Goal

Build a pipeline that ingests issues from a high-volume public GitHub repository,
using date-range incremental sync. This scenario's primary purpose is to surface and
characterize ddt's pagination limitation — nearly every real API is paginated, and
this is the most common Blocking gap for a Fivetran replacement.

**Secondary goal:** Test the `labels` array field (array of objects) and whether
`array_join` is sufficient or a Python connector is needed.

## Target API

GitHub REST API v3 — List repository issues

```
GET https://api.github.com/repos/apache/spark/issues
  ?state=all
  &since=<ISO8601 timestamp>
  &per_page=100
  &page=<N>
```

GitHub API docs: https://docs.github.com/en/rest/issues/issues#list-repository-issues

Response shape (per issue):
```json
{
  "id": 12345,
  "number": 42,
  "title": "...",
  "state": "open",
  "user": { "login": "..." },
  "labels": [{ "id": 1, "name": "bug", "color": "d73a4a" }],
  "body": "...",
  "created_at": "2024-01-01T00:00:00Z",
  "updated_at": "2024-01-02T00:00:00Z",
  "closed_at": null,
  "html_url": "https://github.com/..."
}
```

Pagination: Link header (`rel="next"`) — NOT a query param offset. The API returns
up to 100 items per page; apache/spark has thousands of issues.

## Test Phases

### Phase 1 — YAML Path (Document the Pagination Limitation)

Attempt to build a pipeline using only ddt YAML (no Python connector):

1. Write `pipelines/github_issues.yml` using `type: http`, `date_range` iterate
   axis on `since` param, `per_page: 100`, `namespace: github`
2. Run `ddt validate github_issues` — should pass
3. Run `ddt run github_issues --limit 1` with a narrow date window (one week)
4. Record: how many rows returned? Does ddt follow the Link header to page 2+?
   If only ≤100 rows despite more existing, that is the pagination finding.
5. Run against a wider date window that would require multiple pages — confirm
   the row count is capped at 100 (first page only)

Phase 1 success: pagination limitation confirmed and characterized with exact row counts.

### Phase 2 — Python Connector Path

Write a Python connector at `connectors/github_issues.py` that handles Link-header
pagination properly:

1. Connector function signature should yield dicts (one per issue)
2. Implement: fetch page → yield rows → check Link header → repeat until no `next`
3. Update pipeline YAML to `type: python`, remove iterate axis (connector handles windowing)
4. Run `ddt run github_issues --limit 1` — verify more than 100 rows returned for
   a date window that previously returned exactly 100
5. Verify schema projection: id, number, title, state, user_login (from user.login),
   label_names (array_join of labels[].name), created_at, updated_at, closed_at
6. Run full pipeline, verify incremental deduplication on `id`

Phase 2 success: confirm Python connector path actually works end-to-end for pagination.

## Success Criteria

- [ ] Phase 1: Pipeline YAML validates successfully
- [ ] Phase 1: `--limit 1` run completes (even with truncated data)
- [ ] Phase 1: Pagination limitation confirmed — row count capped at first page
- [ ] Phase 2: Python connector fetches all pages for a date window
- [ ] Phase 2: `user.login` extracted correctly via dot-notation path
- [ ] Phase 2: `labels` field flattened to comma-separated string via array_join or connector
- [ ] Phase 2: Incremental deduplication stable across re-runs
- [ ] Phase 2: Warehouse queryable — `SELECT number, title, state, label_names FROM github.github_issues LIMIT 10`

## Known Complexity

- **Pagination:** GitHub uses Link header (`Link: <url>; rel="next"`), not query-param
  offset. ddt YAML has no pagination field. Phase 1 will confirm this limitation.
- **labels field:** Array of objects `[{id, name, color}]`. Need to extract just `name`
  values and join. `array_join` with `path: labels` joins entire objects as strings —
  may need connector-level extraction or a nested path.
- **Pull requests:** GitHub's issues endpoint also returns PRs (which are issues in the
  GitHub data model). This is fine — just note it in the report.
- **`since` param behavior:** GitHub's `since` param filters by `updated_at`, not
  `created_at`. This is correct for incremental sync (catches edits and closures).
- **Closed issues:** `closed_at` is nullable. Test that null is handled correctly.

## Known Expected Findings (Pre-identified)

- **Expected Blocking (Schema):** ddt YAML has no `pagination` field. The HTTP fetcher
  makes exactly one request per iteration. Link-header pagination cannot be expressed
  in YAML — a Python connector is the only option.
- **Expected Minor (Skill):** `new-pipeline` skill has no guidance on when to use a
  Python connector vs. HTTP YAML. A first-time user would not know to write a connector
  for a paginated API.
- **To investigate:** Does `array_join` with `path: labels` produce `[{id: 1, name: bug}]`
  as a stringified object, or does ddt support `path: labels[].name` (indexed array paths)?
  If not, this is an additional Schema finding.

## Credentials Required

GITHUB_TOKEN — GitHub Personal Access Token with `repo` scope (or public_repo for
public repos). The apache/spark repo is public; unauthenticated access is also fine
for Phase 1, but authenticated access gives higher rate limits (5000 req/hr vs 60).

GITHUB_TOKEN is already configured from Round 2. Use it.

## By Design Decisions from Prior Runs

(None yet — first run of this scenario)

## Notes for Agent

- The test project is `/Users/zephschafer/Documents/GitHub/quipu/`
- Use `namespace: github` — routes to `warehouse/github/github_issues/`
- Use `apache/spark` as the target repo — it has thousands of issues, making the
  pagination limitation clearly visible
- For Phase 1, use a date window of exactly 1 week that you know has >100 issues
  (e.g., any recent week for apache/spark). Confirm that exactly 100 rows are returned.
- For Phase 2, the connector should accept no parameters from the pipeline YAML
  (hardcode the repo and date logic in the connector) OR accept `owner`, `repo`,
  `since`, `until` as params passed from the pipeline YAML iterate axis.
- Record the exact connector function signature that ddt expects for `type: python`
  sources — this is important for the skill to eventually document.
- If `type: python` connector fails to import or run, this is a high-priority finding
  (it means all Blocking YAML limitations are permanently blocking, not just temporarily).
