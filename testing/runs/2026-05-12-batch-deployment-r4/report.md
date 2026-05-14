# Test Run: Batch Pipeline Deployment — Round 4
Date: 2026-05-12 | Tester: Claude Sonnet 4.6 | Scenario: batch-deployment

## Outcome: SUCCESS

Round 4 is the first full end-to-end success for the batch-deployment scenario.
All 17 success criteria passed. One new finding (F-035) was identified and fixed during the run.

## Success Criteria

- [x] Phase 1: `ddt validate github_repos` accepts a `deploy: { schedule: "0 8 * * *" }` block
- [x] Phase 1: `ddt validate` rejects an invalid cron expression with a clear error message
- [x] Phase 1: `ddt validate` on a pipeline without `deploy:` is unaffected (no regression)
- [x] Phase 2: `ddt deploy github_repos` completes without error
- [x] Phase 2: Cloud Composer DAG named `github_repos` is visible after deploy
- [x] Phase 2: Cloud Run job for the pipeline exists after deploy
- [x] Phase 2: `project.yml` records `deployments.github_repos` with schedule, dag_id, cloud_run_job
- [x] Phase 2: `ddt deploy` on a pipeline with no `deploy:` block exits with a clear error
- [x] Phase 2: `ddt deploy` without `catalog: gcp` in `project.yml` exits with a clear error
- [x] Phase 2: Terraform state exists at `~/.ddt/terraform/pipelines/github_repos/terraform.tfstate`
- [x] Phase 2: `terraform show` lists `google_cloud_run_v2_job.pipeline` and `google_storage_bucket_object.dag`
- [x] Phase 3: DAG run completes successfully (no Airflow task failures) — both scheduled and manual runs succeeded
- [x] Phase 3: Parquet files appear in `gs://ddt-warehouse-quipu-data-generator/github_repos/github_repos/data/`
- [x] Phase 3: Warehouse query returns rows — `SELECT COUNT(*) FROM github_repos.github_repos` → 100 rows
- [x] Phase 4: Second `ddt deploy` produces exactly one DAG (idempotent)
- [x] Phase 4: `ddt undeploy github_repos` removes the DAG and Cloud Run job
- [x] Phase 4: Terraform state directory is removed after `ddt undeploy`
- [x] Phase 4: GCS data files are untouched after `ddt undeploy`

## What Worked

- Cloud Build image build and push to Artifact Registry: ✓
- Terraform `batch_pipeline` module provisioned both `google_cloud_run_v2_job` and `google_storage_bucket_object`: ✓
- Terraform state persisted at `~/.ddt/terraform/pipelines/github_repos/`: ✓
- Composer environment reuse (`ddt-composer` already existed): ✓
- DAG file uploaded to correct GCS bucket path: ✓
- Both scheduled and manual DAG runs succeeded in Airflow 2.11.1: ✓
- 100 rows of `github_repos` data written to GCS warehouse: ✓
- Second `ddt deploy` idempotent — Terraform import handled existing Cloud Run job: ✓
- `ddt undeploy --yes` ran `terraform destroy` and removed state dir: ✓
- Warehouse Parquet file survived `ddt undeploy` unchanged: ✓

## What Failed

- Generated DAG used `CloudRunJobOperator` which does not exist in `apache-airflow-providers-google` installed in Composer 3 / Airflow 2.11.1
  [→ Finding F-035: Major / Runtime — fixed during this run]

## Friction Points

- The fix to F-035 required manually re-uploading the corrected DAG file via `gsutil cp` during the test, since re-running `ddt deploy` would rebuild the container image (~3 min) unnecessarily.
  The Terraform-based approach updates the DAG file content in state, so a `terraform apply` alone (without rebuilding the image) would have been sufficient. Consider a `ddt redeploy-dag` subcommand that only re-applies Terraform without rebuilding the image.

## Pipeline Produced

See `pipeline.yml` in this directory.

## New Findings

| ID | Severity | Summary |
|----|----------|---------|
| F-035 | Major | Generated DAG imported `CloudRunJobOperator` (not in providers-google for Composer 3 / Airflow 2.11); correct name is `CloudRunExecuteJobOperator` |

## Proposed Fixes

1. F-035: Fixed in `ddt/gcp/batch_deploy.py` — `_dag_content()` now uses `CloudRunExecuteJobOperator`. Fix committed in this session.
