# Scenario: GitHub Public Repositories (No Auth)

## Goal

Build a pipeline that ingests public repository metadata from a GitHub organization or user.
This is the simplest possible real-world HTTP pipeline: no auth, no iteration, single endpoint,
flat-ish JSON response.

**Target org/user for testing:** `apache` (large org, many repos, stable public data)

## Target API

GitHub REST API v3 — List organization repositories

```
GET https://api.github.com/orgs/{org}/repos?per_page=100&type=public
```

GitHub API docs: https://docs.github.com/en/rest/repos/repos#list-organization-repositories

## Success Criteria

- [ ] Pipeline YAML validates successfully (`ddt validate github-repos`)
- [ ] `--limit 1` run fetches at least 1 real repository row
- [ ] Schema projection captures: id, name, full_name, description, language, stargazers_count, forks_count, created_at, updated_at, html_url
- [ ] Full run (first page, ~100 repos) writes correctly to warehouse
- [ ] Incremental deduplication on primary key `id` works correctly on re-run
- [ ] Warehouse queryable via DuckDB: `SELECT name, stargazers_count FROM github.repos ORDER BY stargazers_count DESC LIMIT 10`

## Known Complexity

- GitHub returns 30 items per page by default; `per_page=100` gets more but still paginated
  (pagination is a known limitation — the goal here is whether one page ingests correctly)
- `description` field can be null (null handling)
- `language` field can be null
- `topics` field is an array of strings (may not be easily projectable)
- `created_at` / `updated_at` are ISO 8601 strings (timestamp casting)
- GitHub unauthenticated rate limit: 60 requests/hour (should not be an issue for this test)

## Credentials Required

None — GitHub public API allows unauthenticated access (rate limited to 60 req/hour).

## Notes for Agent

- Use `full_refresh` build strategy (simplest for baseline test; no primary key needed initially)
- If you want to test incremental, switch to `incremental` with `primary_key: id`
- The response is a top-level JSON array (no nested `records_path` needed — or an empty path)
- Probe the endpoint directly first to see the real response shape before writing YAML
- Test both with and without `records_path` to see how ddt handles top-level arrays

## By Design Decisions from Prior Runs

(None yet)
