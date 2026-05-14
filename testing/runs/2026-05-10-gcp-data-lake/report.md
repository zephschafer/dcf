# Test Run: GCP Data Lake (Remote Warehouse Round-Trip) — Round 2
Date: 2026-05-10 | Tester: Claude Sonnet 4.6 | Scenario: gcp-data-lake

## Outcome: PARTIAL SUCCESS

Phase 1 (`ddt gcp setup`) failed again — this time because the warehouse bucket already exists in GCP from Round 1. Terraform has no record of it in the new state bucket and treats it as a conflict. Phase 2 (GCS write) and Phase 3 (GCS query) both passed after manually injecting `warehouse_bucket` into project.yml. Phase 4 (teardown) ran but silently skipped all resource deletion.

**Clean clone note:** The fresh quipu clone did not contain `github_private_repos.yml` (not committed to GitHub). This was caught immediately by the clean-clone approach. Switched to `github_repos.yml` (written as part of Step 3) — isolates GCP issues from pipeline logic, which is the intended test anyway.

---

## Success Criteria

### Phase 1 — GCP Setup
- [~] `ddt gcp setup` behavior documented ✓ — fails with Terraform 409 when bucket exists
- [ ] `ddt gcp setup` completes without errors — FAIL (F-026: Terraform 409 conflict)
- [ ] GCS bucket created — NOT REACHED (bucket already exists)
- [~] `project.yml` updated with GCP metadata — partial: `setup_status: failed` and `setup_error` written, but ANSI codes in error (F-028)

### Phase 2 — Pipeline Run with GCP Catalog
- [x] `ddt run` with `catalog: gcp` writes Parquet to GCS — PASS (100 rows to `gs://ddt-warehouse-quipu-data-generator/github_repos/github_repos/data/`)
- [x] Incremental upsert deduplication works against GCS — PASS (re-run produced 1 blob, old blob deleted)

### Phase 3 — Query the GCP Warehouse
- [x] `warehouse_reader.query()` reads from GCS when `catalog: gcp` — PASS
- [x] Query returns correct data — PASS (`SELECT id, name, language, stargazers_count` returned real Apache org repos sorted by stars)

### Phase 4 — Teardown
- [~] `ddt gcp teardown` exists and runs — PASS (command exists)
- [ ] GCP resources actually destroyed — FAIL (F-027: command reports success but skipped all deletion — no `tf_state_bucket`/`sa_email` in project.yml)

---

## What Worked

- GCS write path (F-012 fix): `incremental` strategy writes to GCS via PyArrow directly — 100 rows, correct blob naming
- Incremental upsert on GCS (F-012 fix): old blob deleted, new blob written — single file after re-run confirms dedup
- GCS warehouse reader (F-013 fix): `warehouse_reader.query()` downloads blobs via `google-cloud-storage`, registers as Arrow tables, executes DuckDB — returns correct rows
- Teardown command exists (F-015 fix): `ddt gcp teardown --yes` runs without crashing
- Setup error handling (F-014 fix): billing-style errors surface as clean `RuntimeError` message, no full traceback stored in project.yml (though ANSI codes in the message — F-028)
- Clean-clone test framework: immediately caught that `github_private_repos.yml` was not committed to quipu's GitHub repo

---

## What Failed

- `ddt gcp setup` on an existing project (re-run): Terraform 409 conflict on warehouse bucket
  [→ Finding F-026: Major / Runtime]

---

## Friction Points

- `ddt gcp teardown` prints "GCP resources destroyed" when it did nothing
  [→ Finding F-027: Minor / UX]

- `setup_error` in project.yml contained raw ANSI terminal escape codes (`\e[31m`, etc.) making the file unreadable in a text editor
  [→ Finding F-028: Minor / UX]

- `test_config.yml` defaulted to `catalog: local` — had to manually change to `gcp` in the clone before running. The example file's comment says to change it, but it's easy to miss.
  [No finding — addressed by test-pipeline skill instructions]

---

## Pipeline Produced

```yaml
version: 1
name: github_repos
description: GitHub public repositories for the apache organization

source:
  type: http
  url: https://api.github.com/orgs/apache/repos
  method: GET
  params:
    - name: per_page
      type: integer
      value: 100
    - name: type
      type: string
      value: public

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
    - name: description
      path: description
      type: string
    - name: html_url
      path: html_url
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
    - name: owner_login
      path: owner.login
      type: string

build:
  strategy: incremental
  primary_key: id
```

---

## new-pipeline Skill Review

The skill makes no mention of `catalog: gcp`, `ddt gcp setup`, or when to switch to cloud deployment. A user building a production pipeline following the skill end-to-end would never discover the GCP path.
[→ Finding F-029: Enhancement / Skill]

---

## Findings Summary

| Finding | Severity | Status |
|---------|----------|--------|
| F-026: `ddt gcp setup` fails when warehouse bucket already exists (Terraform 409) | Major | Open |
| F-027: `ddt gcp teardown` reports "destroyed" when all steps were skipped | Minor | Open |
| F-028: `setup_error` written to project.yml contains ANSI escape codes | Minor | Open |
| F-029: `new-pipeline` skill has no mention of `catalog: gcp` or `ddt gcp setup` | Enhancement | Open |

---

## Proposed Fixes

1. **F-026 (Major):** Make `ddt gcp setup` idempotent. Two approaches:
   - Add `lifecycle { ignore_changes = all }` to the `google_storage_bucket` resource, or
   - Before Terraform apply, check if the bucket exists and skip creation if so (using `data "google_storage_bucket"` instead of creating)
   - The Terraform state bucket should also be handled — if `ddt-tf-state-<project>` already exists, Terraform should resume from that state rather than starting fresh.

2. **F-027 (Minor):** In `gcp_teardown()`, track what was actually destroyed and print an accurate summary. If no resources were deleted, say "No GCP resources found to destroy (tf_state_bucket and sa_email not set in project.yml)."

3. **F-028 (Minor):** Strip ANSI codes from the error message before writing to project.yml:
   ```python
   import re
   clean_error = re.sub(r'\x1b\[[0-9;]*[mGKH]', '', str(e))
   ```

4. **F-029 (Enhancement):** Add a brief "Deployment" section to `new-pipeline.md` noting that pipelines write to a local warehouse by default, and pointing to `ddt gcp setup` for production cloud deployment.
