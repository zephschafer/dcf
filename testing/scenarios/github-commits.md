# Scenario: GitHub Commits (Date Range Iteration at Scale)

## Goal

Build a pipeline that incrementally ingests commits from a high-volume GitHub
repository using ddt's `date_range` iterate axis. This scenario validates the
incremental sync pattern end-to-end at real scale — the core of Fivetran's value
proposition ("sync everything since last run, don't re-fetch what you have").

**Secondary goal:** Test nested field extraction depth and performance at 1000+ rows.

## Target API

GitHub REST API v3 — List commits

```
GET https://api.github.com/repos/apache/spark/commits
  ?since=<ISO8601>
  &until=<ISO8601>
  &per_page=100
```

GitHub API docs: https://docs.github.com/en/rest/commits/commits#list-commits

Response shape (per commit):
```json
{
  "sha": "abc123...",
  "commit": {
    "author": {
      "name": "...",
      "email": "...",
      "date": "2024-01-01T00:00:00Z"
    },
    "message": "Fix bug in..."
  },
  "author": { "login": "github_username", "id": 12345 },
  "html_url": "https://github.com/..."
}
```

Note: `commit.author` (git author metadata) vs `author` (GitHub user object) are
distinct. The GitHub user `author` can be null if the git email doesn't match a
GitHub account.

Pagination: Link header (same as issues). Use a narrow date window to stay within
one page (≤100 commits) for the YAML path test.

## Test Phases

### Phase 1 — Date Range Iteration (Single Window)

Validate the `date_range` iterate axis with a narrow window that fits on one page:

1. Write `pipelines/github_commits.yml` using `date_range` iterate axis:
   - `params: [since, until]` (two-param form)
   - `start: "2024-01-01"`, `end: "2024-01-31"`, `step: "7 days"`, `window: "7 days"`
   - Schema: sha, commit_message (from commit.message), author_name (commit.author.name),
     author_date (commit.author.date), github_login (author.login, nullable), html_url
2. Run `ddt validate github_commits` — confirm validation passes
3. Run `ddt run github_commits --limit 1` — first iteration (Jan 1–7)
4. Verify: sha is present, commit_message is a string, author_date parses as timestamp,
   github_login is nullable (some commits don't have a matching GitHub user)
5. Run full pipeline (all 4 weekly windows in January 2024)
6. Verify row count is stable across two runs (incremental deduplication on `sha`)

Phase 1 success: date_range iteration works, nested fields extracted, nullable handled.

### Phase 2 — Scale Test (Full Year)

Expand to a full year of commits to test performance and deduplication at scale:

1. Update pipeline: `start: "2023-01-01"`, `end: "2024-01-01"`, `step: "7 days"`, `window: "7 days"`
2. Run full pipeline — ~52 requests, likely 2000–5000 commits
3. Record: total runtime, total rows, Spark startup overhead visible?
4. Re-run immediately — row count must be identical (deduplication at scale)
5. Query warehouse: top 10 committers by commit count, most active weeks

Phase 2 success: deduplication holds at scale, performance is acceptable for a
"replace Fivetran" use case.

## Success Criteria

- [ ] Pipeline YAML validates successfully
- [ ] `--limit 1` fetches commits for one date window
- [ ] `commit.message` extracted via dot-notation path (nested 2 levels)
- [ ] `commit.author.date` parsed as timestamp (nested 3 levels)
- [ ] `author.login` correctly nullable (null when git email ≠ GitHub account)
- [ ] Incremental deduplication on `sha` — row count stable across re-runs
- [ ] Full year run completes without error
- [ ] Warehouse queryable: `SELECT github_login, COUNT(*) as commits FROM github.github_commits GROUP BY github_login ORDER BY commits DESC LIMIT 10`

## Known Complexity

- **Deep nesting:** `commit.author.date` is 3 levels deep. Confirm dot-notation
  works at depth 3 (we've tested depth 2 with `user.login` in prior rounds).
- **Nullable GitHub user:** `author` (the GitHub user object) is null for commits
  where the git author email doesn't match any GitHub account. `author.login` must
  handle null gracefully (not crash with `NoneType has no attribute 'login'`).
- **Duplicate message handling:** `commit.message` can contain newlines and special
  characters. Confirm these don't break Parquet writing.
- **Pagination:** Each 7-day window is unlikely to exceed 100 commits, so pagination
  is not needed for the YAML path test. If a window does exceed 100, document the
  truncation as a note (same Blocking finding from github-issues applies).
- **`sha` as primary key:** SHA is a string, not integer. Verify incremental upsert
  works with string primary keys.

## Known Expected Findings (Pre-identified)

- **Expected Minor (UX):** `new-pipeline` skill has no guidance on choosing `step`
  and `window` sizes for a date_range iterate axis. How does a user know to use
  `"7 days"` vs `"1 month"`? What happens if windows overlap?
- **To investigate:** Performance at 50+ requests — does Spark startup dominate?
  If so, is there a way to batch more rows per Spark write (currently writes once
  per request iteration)?
- **To investigate:** Does null propagation work for `author.login` when `author`
  (the parent object) is null? ddt's dot-path extractor may fail with an AttributeError
  or silently return None — need to confirm which.

## Credentials Required

GITHUB_TOKEN — already configured from Round 2. Required for higher rate limits
(5000 req/hr authenticated vs 60 unauthenticated). With ~52 requests for a full year
at weekly windows, unauthenticated would likely hit rate limits.

## By Design Decisions from Prior Runs

(None yet — first run of this scenario)

## Notes for Agent

- The test project is `/Users/zephschafer/Documents/GitHub/quipu/`
- Use `namespace: github` — routes to `warehouse/github/github_commits/`
- Use `apache/spark` — high commit volume, public repo, guaranteed to have thousands
  of commits per year
- For Phase 1, use January 2024 specifically — this is a stable historical window
  that won't grow (unlike current-month windows). Row count will be reproducible.
- When testing nullable `author.login`, look for commits by bot accounts or external
  contributors — these are most likely to have null GitHub user objects
- For the scale test, record the wall-clock time from `[ddt] Running...` to
  `[ddt] complete →`. This establishes a baseline for future performance comparisons.
- If Spark startup overhead is significant relative to data volume, note it — this
  may inform a future optimization finding (e.g., batch multiple windows into one
  Spark write).
