# Scenario: Rate Limit Resilience (429 Handling + Partial Run Recovery)

## Goal

Test ddt's behavior when it hits API rate limits. Fivetran handles rate limits
transparently (automatic backoff, retry, resume). ddt currently has no retry or
backoff logic — a 429 response is treated the same as any other HTTP error.

This scenario answers three questions:
1. What exactly happens when ddt hits a 429? (crash? log and continue? partial write?)
2. Is the warehouse left in a valid, queryable state after a rate-limited run?
3. Can a user resume a partially-completed run without re-fetching already-written data?

## Target API

GitHub REST API — same endpoint as github-commits, but deliberately configured to
trigger rate limiting:

```
GET https://api.github.com/repos/apache/spark/commits?since=...&until=...
```

**How to trigger rate limiting:**
- Remove GITHUB_TOKEN (use unauthenticated access: 60 requests/hour limit)
- Set `date_range` with enough windows to exceed 60 requests in quick succession
- Alternatively: use a token with a very short per-minute rate (ddt has a `rate_limit`
  config field — test what it actually does)

## Test Phases

### Phase 1 — Characterize 429 Behavior

Set up a pipeline that WILL hit the rate limit:

1. Write `pipelines/github_commits_ratelimit.yml` — same as github-commits but
   with no auth (remove token) and a date range wide enough to require >60 requests
   (e.g., daily windows over 3 months = 90+ requests)
2. Run `ddt run github_commits_ratelimit`
3. Observe: when the 429 hits (after ~60 requests), what does ddt do?
   - Does it crash immediately with an exception?
   - Does it print an error and continue to the next iteration?
   - Does it write partial data before crashing?
   - Does it show the 429 status code clearly?
4. Check the warehouse: is partial data queryable? How many rows were written
   before the 429?

Phase 1 success: 429 behavior fully characterized.

### Phase 2 — Rate Limit Config Field

ddt's YAML schema has a `rate_limit` config field. Test what it actually does:

1. Read `ddt/config/models.py` — what fields does `RateLimit` have?
2. Read `ddt/engine/fetcher.py` — how is `rate_limit` applied?
3. Add a `rate_limit` config to the pipeline (e.g., `requests: 1`, `per_minutes: 1`)
4. Run the pipeline — does the rate limit config prevent 429s by slowing requests?
5. Record: does it work? Is the config sufficient to avoid rate limits on GitHub?

Phase 2 success: rate_limit config behavior documented.

### Phase 3 — Recovery After Partial Run

After Phase 1 (partial run with some data in warehouse), attempt to resume:

1. Re-run the same pipeline with GITHUB_TOKEN set (higher rate limit)
2. Does the incremental strategy correctly pick up from where the pipeline left off?
   Or does it re-fetch all windows from the start?
3. Verify that the final row count is correct (all commits, not duplicates from retry)

Note: ddt's `incremental` strategy upserts on primary key — it doesn't have a
"checkpoint" mechanism that tracks which iterations completed. Re-running means
re-fetching all windows but deduplicating on insert.

Phase 3 success: document whether re-run correctly deduplicates OR re-fetches and
what the user experience is.

## Success Criteria

- [ ] Phase 1: 429 behavior documented — crash vs. continue vs. partial write
- [ ] Phase 1: Exact error message or exception recorded verbatim
- [ ] Phase 1: Warehouse state after partial run confirmed (queryable? corrupted?)
- [ ] Phase 2: `rate_limit` config behavior documented
- [ ] Phase 2: Does rate_limit config prevent 429s if set conservatively?
- [ ] Phase 3: Re-run after partial failure works without data duplication
- [ ] Phase 3: Total row count after re-run matches expected (no missing windows,
  no duplicate rows)

## Known Complexity

- **Triggering rate limits safely:** The unauthenticated GitHub limit (60/hr) is easy
  to hit but slow (must wait for it to reset if you go over). Plan carefully — run
  this scenario when willing to wait up to 1 hour for rate limit reset.
- **Alternately:** ddt may have a `rate_limit` config with `per_minutes` — test
  whether setting it very low (e.g., `requests: 1, per_minutes: 60`) prevents 429s
  by self-throttling.
- **Partial write state:** Depending on when the 429 hits (mid-batch vs. mid-write),
  the warehouse may have partial data for a given iteration window or none at all.
  The `incremental` strategy writes atomically per-request — so either all rows from
  a request are written or none (via PyArrow upsert). Verify this assumption.

## Known Expected Findings (Pre-identified)

- **Expected Major (Runtime):** 429 likely crashes the run with a generic
  `requests.HTTPError`, no retry, no backoff, no checkpoint. The user must manually
  re-run with a working token.
- **Expected Minor (UX):** The 429 error message is likely the same raw HTTP error
  as the 401 (F-009) — no ddt-specific guidance like "you've exceeded the API rate
  limit; add a rate_limit config or wait N minutes."
- **To investigate:** Is the partial warehouse state valid and queryable, or is it
  in a corrupted intermediate state?
- **Enhancement:** ddt could add automatic retry with exponential backoff for 429/503
  responses. This would be a significant reliability improvement and is standard
  behavior for a Fivetran replacement.

## Credentials Required

None required for Phase 1 (deliberately using unauthenticated access to hit limits).
GITHUB_TOKEN required for Phase 3 recovery test.

## By Design Decisions from Prior Runs

(None yet — first run of this scenario)

## Notes for Agent

- The test project is `/Users/zephschafer/Documents/GitHub/quipu/`
- Use `namespace: github` — routes to `warehouse/github/github_commits_ratelimit/`
- **Important:** Unset GITHUB_TOKEN before Phase 1: `unset GITHUB_TOKEN`. Re-set it
  before Phase 3.
- For Phase 1: use daily windows (step: "1 day", window: "1 day") from 2024-01-01 to
  2024-04-01 — that's 90 requests, which will hit the 60/hr unauthenticated limit.
  Run it and wait for the 429.
- If you're concerned about wasting the hour rate limit wait, run Phase 2 first
  (test rate_limit config with GITHUB_TOKEN) to understand the config, then run
  Phase 1 as the final test.
- Check `ddt/config/models.py` for `RateLimit` model before designing the test —
  the config may already support per-second or per-minute throttling that prevents
  429s entirely. If so, Phase 1 still tests the behavior when the rate limit config
  is absent or misconfigured.
- When checking warehouse state after the crash: use DuckDB directly —
  `duckdb.connect().execute("SELECT COUNT(*) FROM read_parquet('warehouse/github/github_commits_ratelimit/data/*.parquet')").fetchone()`
