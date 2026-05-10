# Scenario: GitHub Private Repositories (Credential Lifecycle)

## Goal

Build a pipeline that ingests metadata from the authenticated user's private GitHub
repositories. This scenario tests the full credential lifecycle: guiding a user to
create a token, store it, handle missing/invalid credentials gracefully, and
successfully ingest authenticated data.

This is deliberately a more human-in-the-loop scenario than github-repos. The agent
must act like a real first-time user who does not yet have a token.

**Target user:** The authenticated GitHub user (Zeph, github.com/zephschafer).

## Target API

GitHub REST API v3 — List repositories for the authenticated user

```
GET https://api.github.com/user/repos?visibility=private&per_page=100
```

GitHub API docs: https://docs.github.com/en/rest/repos/repos#list-repositories-for-the-authenticated-user

## Test Phases

This scenario has three sequential phases. Run them in order.

### Phase 1 — Credential Guidance (Before any token exists)

Simulate a new user who has just installed pvc and wants to ingest their private repos
but has NOT yet created a GitHub token. Walk through the following:

1. Run `pvc init` — does it offer to collect a GitHub token? Does it prompt for
   arbitrary credentials, or only the hardcoded Portland Maps key?
2. Read the `new-pipeline` skill — does it include any guidance about credential
   setup, token scopes, or storage?
3. If the skill is silent, attempt to write the pipeline YAML anyway (with
   `{{ env.GITHUB_TOKEN }}` in the auth block) WITHOUT setting the env var.
4. Run `pvc validate github_private_repos` — does it catch the missing credential?
   Or does validation pass and the error only surface at runtime?
5. Run `pvc run github_private_repos --limit 1` WITHOUT the env var set.
   Record the exact error message verbatim. Is it actionable? Does it tell the user
   what to do next?

Phase 1 success: document what guidance the user received (or didn't) and record any
findings about the credential setup UX.

### Phase 2 — Invalid Credentials

Set `GITHUB_TOKEN` to a deliberately bad value: `export GITHUB_TOKEN=ghp_invalid`.

1. Run `pvc run github_private_repos --limit 1`.
2. Record the exact error output verbatim. Does pvc surface the HTTP 401 status code?
   Does it indicate the credential is wrong (vs. missing)?
3. Is there any retry guidance? Any distinction between "token wrong" vs "token expired"?

Unset the bad token before Phase 3: `unset GITHUB_TOKEN`.

### Phase 3 — Happy Path (Valid Credentials Required)

**NOTE FOR AGENT:** This phase requires a real `GITHUB_TOKEN` with `repo` scope.
Check whether `GITHUB_TOKEN` is already set in the environment before proceeding.
If not, pause and ask the user to set it before continuing.

Token setup instructions to relay to the user if needed:

1. Go to github.com → Settings → Developer settings → Personal access tokens → Tokens (classic)
2. Click "Generate new token (classic)"
3. Name it `pvc-test`, set an expiration, check the `repo` scope
4. Copy the token, then either:
   - Run: `export GITHUB_TOKEN=ghp_xxxxx` (session only), OR
   - Add to `project.yml`: `github_token: ghp_xxxxx` (persisted, gitignored)

With a valid token:

1. Run `pvc run github_private_repos --limit 1` — verify it fetches at least one real
   private repo row
2. Check schema: `private` field should be boolean `True`, not the string `"True"`
3. Run the full pipeline
4. Re-run and confirm row count is stable (incremental deduplication)

## Success Criteria

### Phase 1 — Credential Guidance
- [ ] `pvc init` behavior documented: does it offer GitHub credential collection?
- [ ] `new-pipeline` skill behavior documented: does it guide credential setup?
- [ ] `pvc validate` behavior documented: does it catch missing env vars at validate time?
- [ ] Missing-token error message recorded verbatim — rated: actionable / partially actionable / not actionable

### Phase 2 — Invalid Credentials
- [ ] Invalid-token error message recorded verbatim
- [ ] Error clearly distinguishes "credential is wrong" from "credential is missing"
- [ ] HTTP status code (401) is visible in the error output

### Phase 3 — Happy Path
- [ ] Pipeline YAML validates successfully
- [ ] `--limit 1` fetches at least 1 real private repo (confirm `private: true` in warehouse)
- [ ] Schema projection captures: id, name, full_name, private, description, language, stargazers_count, forks_count, created_at, updated_at, default_branch, visibility
- [ ] `private` field is stored as boolean, not string
- [ ] Full run writes to warehouse with stable row count on re-run
- [ ] Warehouse queryable: `SELECT name, private, visibility FROM github_private_repos.github_private_repos LIMIT 10`

## Known Complexity

- `private` field is boolean — tests boolean type casting in the projector
- `visibility` field is a string (`"private"`, `"public"`, `"internal"`) — worth capturing
- Pagination via Link headers is a known schema limitation; one page of results is sufficient for this test
- GitHub authenticated rate limit is 5000 requests/hour — not a concern here
- A PAT with `repo` scope may not show private repos belonging to organizations unless the
  org has granted the PAT access — private personal repos are sufficient

## Credentials Required

`GITHUB_TOKEN` — GitHub Personal Access Token with `repo` scope.

**Phase 3 only.** Phases 1 and 2 explicitly test the no-token and bad-token states.
Do NOT set `GITHUB_TOKEN` before Phase 1. If it is already set in your shell, unset
it temporarily: `unset GITHUB_TOKEN`.

## Known Expected Findings (Pre-identified)

These gaps were identified by code review before running the test. The scenario should
confirm or correct each, and adjust severity based on what is actually observed.

- **Expected F-006 (Minor / Skill):** `new-pipeline` skill has no guidance on credential
  creation, required token scopes, or storage mechanism. A first-time user building an
  authenticated pipeline is left to figure this out independently.

- **Expected F-007 (Minor / UX):** `pvc init` is hardcoded to the Portland Maps API key.
  There is no general-purpose credential collection — no `pvc init --add-key` or equivalent.
  Users needing to store arbitrary API tokens must manually edit `project.yml`.

- **Expected F-008 (Minor / UX):** `pvc validate` passes even when `{{ env.VAR }}`
  references an unset variable, because validation runs with `resolve_env=False`. Users
  may be confused when validate reports success but run immediately fails with
  `EnvironmentError`.

- **Expected F-009 (Minor / UX):** A bad token produces a raw `requests.HTTPError`
  string (`401 Client Error: Unauthorized for url: ...`) with no pvc-specific context
  about which credential is wrong or how to fix it.

The agent should confirm each, adjust the severity if needed, and add any unexpected
findings discovered during the run.

## By Design Decisions from Prior Runs

(None yet)

## Notes for Agent

- Use `namespace: github` in the pipeline YAML to exercise the namespace field added in F-002
- Project the `private` field with `type: boolean` to explicitly test boolean casting
- The test project is `/Users/zephschafer/Documents/GitHub/quipu/`
- Record all error messages verbatim in the run report — do not paraphrase them
- For Phase 1, do NOT set `GITHUB_TOKEN` before starting; unset it if already present
- For Phase 2, use `export GITHUB_TOKEN=ghp_invalid` (a syntactically plausible but invalid token)
- For Phase 3, check `echo $GITHUB_TOKEN` before proceeding; pause and ask user if unset
