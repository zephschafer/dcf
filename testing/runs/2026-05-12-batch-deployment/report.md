# Test Run: Batch Pipeline Deployment
Date: 2026-05-12 | Tester: Claude claude-sonnet-4-6 | Scenario: batch-deployment

## Outcome: SUCCESS

All 15 success criteria passed. No new findings. This was a clean validation round
following all fixes from the Round 1 (failure) and Round 2 (success with in-run fix) runs.

---

## Success Criteria

- [x] Phase 1: `ddt validate github_repos` accepts a `deploy: { schedule: "0 8 * * *" }` block
- [x] Phase 1: `ddt validate` rejects an invalid cron expression with a clear error message
  - Error text: `deploy.schedule 'not a cron' is not a valid cron expression. Expected 5 space-separated fields...`
- [x] Phase 1: `ddt validate` on a pipeline without `deploy:` is unaffected (no regression)
  - `ddt validate craigslist_apts` → OK
- [x] Phase 2: `ddt deploy github_repos` completes without error
  - Existing `ddt-composer` environment detected and reused (no 20-min wait)
  - Build: 2m32s; total deploy: ~3 min
- [x] Phase 2: Cloud Composer DAG named `github_repos` is visible after deploy
  - `gs://us-central1-ddt-composer-a735fe8e-bucket/dags/github_repos.py` confirmed
- [x] Phase 2: Cloud Run job for the pipeline exists after deploy
  - `ddt-job-github-repos` in `us-central1` confirmed (Ready)
- [x] Phase 2: `project.yml` records `deployments.github_repos` with schedule, dag_id, cloud_run_job
  - Full state written including `composer_env`, `image_uri`, `deployed_at`
- [x] Phase 2: `ddt deploy` on a pipeline with no `deploy:` block exits with a clear error
  - `ddt deploy craigslist_apts` → "has no 'deploy:' block in its pipeline YAML"
- [x] Phase 2: `ddt deploy` without `catalog: gcp` exits with a clear error
  - Tested first (test_config.yml has `catalog: local`) → "catalog is not 'gcp'. Batch deployment requires a GCP data lake."
- [x] Phase 3: DAG run completes successfully
  - `gcloud run jobs execute ddt-job-github-repos --wait` → exit 0
- [x] Phase 3: Parquet files appear in `gs://ddt-warehouse-quipu-data-generator/github_repos/github_repos/data/`
  - `2c34273d-f9b7-40f2-aa8d-ca1262dcaec1.parquet` confirmed
- [x] Phase 3: Warehouse query returns rows
  - `SELECT COUNT(*), MAX(name) FROM github_repos.github_repos` → `[{'count_star()': 100, 'max("name")': 'zookeeper'}]`
- [x] Phase 4: Second `ddt deploy` produces exactly one DAG (idempotent)
  - Single `github_repos.py` in Composer bucket; single `ddt-job-github-repos` Cloud Run job
- [x] Phase 4: `ddt undeploy github_repos` removes the DAG and Cloud Run job
  - DAG file removed; Cloud Run job deleted; `deployments` key removed from project.yml
- [x] Phase 4: GCS data files are untouched after `ddt undeploy`
  - Parquet file still at `gs://ddt-warehouse-quipu-data-generator/github_repos/github_repos/data/`

---

## What Worked

- All four phases passed cleanly with no errors or unexpected behavior
- F-033 fix verified: existing Composer environment found and reused without provisioning wait
- F-034 fix verified: Cloud Run container completed successfully (no JAVA_GATEWAY_EXITED)
- Error cases (no `deploy:` block, wrong catalog) produce clear, actionable messages
- Undeploy confirmation prompt names the resources that will be removed and explicitly notes data is preserved

## New Findings

None.

## Friction Points

- `test_config.yml` has `catalog: local` which is correct for most scenarios but requires manual project.yml update for the batch-deployment scenario. The scenario notes document this, but it's an extra step that a real user following `new-pipeline` Step 10 would also hit (they'd need to run `ddt gcp setup` first). This is expected and documented — not a new finding.

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

deploy:
  schedule: "0 8 * * *"
```
