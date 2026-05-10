# Test Run: GitHub Private Repositories (Credential Lifecycle)
Date: 2026-05-09 | Tester: Claude Sonnet 4.6 | Scenario: github-private-repos

## Outcome: SUCCESS (all 3 phases complete)

---

## Success Criteria

### Phase 1 — Credential Guidance
- [x] `pvc init` behavior documented: does it offer GitHub credential collection?
- [x] `new-pipeline` skill behavior documented: does it guide credential setup?
- [x] `pvc validate` behavior documented: does it catch missing env vars at validate time?
- [x] Missing-token error message recorded verbatim — rated below

### Phase 2 — Invalid Credentials
- [x] Invalid-token error message recorded verbatim
- [~] Error distinguishes "credential wrong" from "credential missing" — partially (different code paths, but no guidance in either case)
- [x] HTTP status code (401) visible in error output

### Phase 3 — Happy Path
- [x] Pipeline YAML validates successfully
- [x] `--limit 1` fetches 12 real private repos (all have `private: True`)
- [x] Schema projection captures all 12 columns including boolean `private`
- [x] `private` field stored as native `bool` (Python type: bool, DuckDB type: BOOLEAN) — not string
- [x] Full run stable: 12 rows across 3 consecutive runs (incremental deduplication confirmed)
- [x] Warehouse queryable at `warehouse/github/github_private_repos/data/`
- [x] `namespace: github` correctly routes to `warehouse/github/` prefix

---

## Phase 1 Findings

### `pvc init` — Does Not Help

Running `pvc init` prompts only for:
```
PortlandMaps API key (leave blank to use default):
Valid regions, comma-separated (blank for all):
Catalog type (local or gcp) [local]:
```

No GitHub credential collection. No generic key-collection mechanism. A user wanting to store a GitHub token via the CLI has nowhere to go. They must manually edit `project.yml`.
**→ Confirms Expected F-007**

### `new-pipeline` skill — Silent on Credentials

The `new-pipeline` skill (`.claude/commands/new-pipeline.md`) has no mention of:
- How to obtain an API token
- What scopes a GitHub token needs
- How to store a token (env var vs project.yml)
- The `{{ env.VAR }}` syntax for referencing credentials in YAML

The skill jumps straight to probing the API and designing YAML, assuming credentials already exist.
**→ Confirms Expected F-006**

### `pvc validate` — Passes with Missing Token

```
OK — 'github_private_repos' (2 params, 0 iterate axes, 12 columns)
```

Validate reports success even though `GITHUB_TOKEN` is not set. The `resolve_env=False` mode replaces `{{ env.GITHUB_TOKEN }}` with the string `<env>` for structural validation, so no credential check occurs. The user gets a false sense of security.
**→ Confirms Expected F-008**

### Missing Token Error — Verbatim

Running `pvc run github_private_repos --limit 1` without `GITHUB_TOKEN` produces a **full Rich traceback panel** ending with:

```
OSError: 'GITHUB_TOKEN' is not set — add it as an environment variable or set 
'github_token' in project.yml
```

**Rating: Partially actionable.**
- ✓ Names the missing variable (`GITHUB_TOKEN`)
- ✓ Gives two storage options (env var or project.yml)
- ✗ Full internal traceback is shown (8 stack frames with source code) — overwhelming for a user-facing credential error
- ✗ No guidance on HOW to get a GitHub token or what scope is needed
- ✗ `OSError` is confusing — users expect authentication errors, not OS errors (EnvironmentError is a subclass of OSError)
- ✗ Error fires BEFORE Spark starts, which is good, but the traceback format is inconsistent with the `[pvc]` output style

**→ Partially confirms Expected F-009** (new additional dimension: full traceback shown rather than clean error line)

### Unexpected Finding: Bearer Auth Requires Meaningless `key` Field

Writing bearer auth YAML without a `key` field:
```yaml
auth:
  type: bearer
  value: "{{ env.GITHUB_TOKEN }}"
```
...fails validation with:
```
ValidationError: 1 validation error for Pipeline
source.http.auth.key
  Field required [type=missing, ...]
```

The `key` field is required by the Pydantic model for ALL auth types, but for `bearer` auth, `key` is never used (the fetcher hardcodes `Authorization: Bearer <value>`). The user is forced to provide a semantically meaningless placeholder:
```yaml
auth:
  type: bearer
  key: token   # ← meaningless, required only to satisfy the model
  value: "{{ env.GITHUB_TOKEN }}"
```
**→ New Finding F-010 (Minor / Schema)**

---

## Phase 2 Findings

### Invalid Token Error — Verbatim

Running with `GITHUB_TOKEN=ghp_invalid`:

```
[pvc] Running 'github_private_repos' — 1 requests

  [1/1] 
    fetch error: 401 Client Error: Unauthorized for url: https://api.github.com/user/repos?visibility=private&per_page=100

[pvc] 'github_private_repos' complete → /Users/zephschafer/Documents/GitHub/quipu/warehouse/github/github_private_repos/data
```

**Observations:**
- ✓ HTTP 401 status code is visible
- ✓ URL is shown — useful for debugging
- ✓ Error does not crash pvc — fetch error is caught at the request level
- ✗ No hint that the credential is wrong (vs. expired, vs. insufficient scope)
- ✗ The pipeline "completes" with 0 rows — warehouse path is printed even though nothing was written, which is misleading
- ✗ No guidance: "Check your GITHUB_TOKEN has `repo` scope" or "Regenerate your token at github.com/settings/tokens"

**→ Confirms Expected F-009**

### Positive: namespace: github Works

The warehouse path in Phase 2 output confirms the namespace fix (F-002) is working:
```
→ /warehouse/github/github_private_repos/data
```
Not `warehouse/github_private_repos/github_private_repos/data`. ✓

---

## Phase 3 Results

**12 private repos ingested.** All success criteria passed.

- `private` field: native `bool` (BOOLEAN in DuckDB) ✓
- `visibility` field: string `"private"` for all rows ✓
- `default_branch`: captures both `main` and `master` correctly ✓
- `language`: nullable — 5 repos have null language (no false coercion to string "None") ✓
- Incremental deduplication: 12 rows stable across 3 runs ✓
- `namespace: github` routes correctly to `warehouse/github/github_private_repos/` ✓

---

## Pipeline Produced

See pipeline.yml in this directory.

---

## Confirmed Findings Summary (Phases 1 & 2)

| Finding | Expected? | Status |
|---------|-----------|--------|
| F-006: `new-pipeline` skill has no credential guidance | Expected | Confirmed |
| F-007: `pvc init` hardcoded to Portland Maps | Expected | Confirmed |
| F-008: `pvc validate` passes with unset env vars | Expected | Confirmed |
| F-009: Bad token gives raw HTTPError with no credential context | Expected | Confirmed |
| F-010: Bearer auth requires meaningless `key` field | Unexpected | New |

---

## Proposed Fixes (for post-Phase 3 review)

1. **F-006:** Add a "Credentials" section to `new-pipeline` skill. Before step 2 (probe API), guide user through: identify what auth the API needs → create token with correct scope → store in env var or `project.yml` using `{{ env.VAR }}` syntax.

2. **F-007:** Generalize `pvc init` to accept arbitrary key-value credential pairs. Add a `--add-key KEY` prompt or simply prompt for key-value pairs until the user is done, in addition to the Portland Maps fields.

3. **F-008:** In `pvc validate`, optionally attempt env var resolution and warn (not error) when a referenced variable is not set: `⚠ 'GITHUB_TOKEN' is not set — set it before running`.

4. **F-009:** In `runner.py`, catch `requests.HTTPError` specifically and emit a friendlier message: `    auth error: 401 Unauthorized — check that your GITHUB_TOKEN is valid and has the required scope`.

5. **F-010:** Make `Auth.key` optional (`key: str | None = None`) and only require it for `query_param` and `header` auth types. Bearer auth should work with just `type` and `value`.
