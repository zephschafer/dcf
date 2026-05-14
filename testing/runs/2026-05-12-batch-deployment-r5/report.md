# Test Run: Batch Pipeline Deployment — Round 5
Date: 2026-05-12 | Tester: Claude Sonnet 4.6 | Scenario: batch-deployment

## Outcome: PARTIAL SUCCESS

17/18 success criteria passed. The one failing criterion is stale: the behavior
it tests changed when local Docker deployment was added (see F-047). All GCP batch
deployment functionality works correctly. Three new Minor/UX findings filed.

---

## Success Criteria

- [x] Phase 1: `ddt validate github_repos` accepts a `deploy: { schedule: "0 8 * * *" }` block
  - Output: `OK — 'github_repos' (2 params, 0 iterate axes, 11 columns)`
- [x] Phase 1: `ddt validate` rejects an invalid cron expression with a clear error message
  - Error text: `deploy.schedule 'not a cron' is not a valid cron expression. Expected 5 space-separated fields...`
- [x] Phase 1: `ddt validate` on a pipeline without `deploy:` is unaffected (no regression)
  - `ddt validate craigslist_apts` → OK
- [x] Phase 2: `ddt deploy github_repos` completes without error
  - New `ddt-composer` environment provisioned (27 min); Cloud Build 2m32s; total ~30 min
- [x] Phase 2: Cloud Composer DAG named `github_repos` is visible after deploy
  - `gs://us-central1-ddt-composer-59d3b6ad-bucket/dags/github_repos.py` confirmed
- [x] Phase 2: Cloud Run job for the pipeline exists after deploy
  - `ddt-job-github-repos` in `us-central1` (Ready) confirmed
- [x] Phase 2: `project.yml` records `deployments.github_repos` with schedule, dag_id, cloud_run_job
  - Full state written: schedule, dag_id, cloud_run_job, composer_env, image_uri, deployed_at
- [x] Phase 2: `ddt deploy` on a pipeline with no `deploy:` block exits with a clear error
  - `ddt deploy craigslist_apts` → "has no 'deploy:' block in its pipeline YAML"
- [ ] Phase 2: `ddt deploy` without `catalog: gcp` in `project.yml` exits with a clear error
  - **STALE CRITERION**: `ddt deploy` with `catalog: local` now routes to local Docker
    deployment instead of erroring. Behavior changed in commit `08faf16`. See F-047.
- [x] Phase 2: Terraform state exists at `~/.ddt/terraform/pipelines/github_repos/terraform.tfstate`
  - `main.tf  outputs.tf  terraform.tfstate  terraform.tfvars.json  variables.tf` confirmed
- [x] Phase 2: `terraform show` lists `google_cloud_run_v2_job.pipeline` and `google_storage_bucket_object.dag`
  - Both resources confirmed in `terraform show` output
- [x] Phase 3: DAG run completes successfully (no Airflow task failures)
  - `scheduled__2026-05-11T08:00:00+00:00 | success` — auto-triggered ~4 min after deploy
- [x] Phase 3: Parquet files appear in `gs://ddt-warehouse-quipu-data-generator/github_repos/github_repos/data/`
  - `ce96772c-b01a-4f0b-8d3e-535da5a28178.parquet` confirmed (Phase 3)
- [x] Phase 3: Warehouse query returns rows (data is correct and readable)
  - `SELECT COUNT(*), MAX(name) FROM github_repos.github_repos` → `[{'count_star()': 100, 'max("name")': 'zookeeper'}]`
- [x] Phase 4: Second `ddt deploy` produces exactly one DAG (idempotent)
  - Exactly one `github_repos.py` in Composer bucket; one `ddt-job-github-repos` Cloud Run job
  - Exactly two resources in Terraform state; `deployed_at` updated to `2026-05-12T23:11:55+00:00`
- [x] Phase 4: `ddt undeploy github_repos` removes the DAG and Cloud Run job
  - DAG file removed from Composer bucket; `ddt-job-github-repos` deleted from Cloud Run
- [x] Phase 4: Terraform state directory is removed after `ddt undeploy`
  - `~/.ddt/terraform/pipelines/github_repos/` — confirmed absent after undeploy
- [x] Phase 4: GCS data files are untouched after `ddt undeploy`
  - `907ba79b-c609-40e5-9afc-a9e7254844f8.parquet` still present; warehouse query returns 100 rows

---

## What Worked

- All four phases passed cleanly on the GCP deployment path
- `ddt-composer` environment auto-provisioned (first run in this GCP project since prior env was deleted)
- Second deploy reused existing Composer environment (3 min vs. 27 min for first deploy)
- Terraform `batch_pipeline` module: both `google_cloud_run_v2_job.pipeline` and `google_storage_bucket_object.dag` provisioned correctly
- Undeploy: Terraform destroy cleaned up both resources; state directory removed; warehouse data preserved

## What Failed

None of the GCP batch deployment functionality failed.

## Friction Points

1. **`sa_email` not documented as a required project.yml field** for the GCP deploy scenario.
   `_require_gcp_config()` requires `project_id`, `region`, `warehouse_bucket`, AND `sa_email`,
   but the scenario notes only list the first three. Required manual lookup of the service account.
   [→ Finding F-049: Minor / UX]

2. **`ddt deploy` with `catalog: local` routed to local Docker instead of erroring.**
   The scenario's error-case test (temporarily setting `catalog: local`) triggered a full local
   Docker deployment — including a 1-min image build and container run — which is now the
   correct behavior but contradicts both the scenario's success criterion and the feature spec.
   [→ Finding F-047: Minor / UX]

3. **No feature file for local Docker deployment.**
   The local deploy feature (commit `08faf16`) changed the CLI surface of `ddt deploy` but has
   no `features/local-deployment.md`. The FEATURES.md registry is now incomplete.
   [→ Finding F-048: Minor / UX]

4. **Prior `ddt-composer` environment was gone.**
   The prior run's `ddt undeploy` only removes the DAG (by design), but between runs the
   Composer environment was deleted, forcing a new 27-min provisioning wait. Not a bug —
   documented in Known Complexity — but cost significant test time.

5. **`| tail -20` swallowed background deploy output.**
   Using `| tail -20` in the Phase 4a background command caused the output file to appear
   empty until the command completed. The command did complete successfully (verified by re-running
   directly), but produced no observable output during execution.

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

deploy:
  schedule: "0 8 * * *"
```

---

## New Findings

| ID | Severity | Summary |
|----|----------|---------|
| F-047 | Minor | `features/batch-deployment.md` line 55 and scenario criterion "ddt deploy without catalog: gcp exits with clear error" are stale — behavior changed when local Docker deployment was added |
| F-048 | Minor | Local Docker deployment (local_deploy.py, commit 08faf16) has no feature file in features/ |
| F-049 | Minor | `sa_email` required by `_require_gcp_config()` but not listed in scenario notes as a required project.yml field for GCP deploy test setup |

## Proposed Fixes

1. F-047: Update `features/batch-deployment.md` line 55 to say "with `catalog: local`, deploys locally (Docker); with `catalog: gcp`, deploys to GCP." Update `testing/scenarios/batch-deployment.md` success criterion to match.
2. F-048: Create `features/local-deployment.md` with requirements and acceptance criteria for local Docker deployment. Add to `features/FEATURES.md` registry.
3. F-049: Add `sa_email: <value>` to the scenario notes ("Notes for Agent" section) as a required project.yml field alongside `warehouse_bucket` and `setup_status`.
