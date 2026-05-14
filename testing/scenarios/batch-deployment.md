# Scenario: Batch Pipeline Deployment

## Goal

Test the full batch deployment lifecycle: a pipeline YAML with a `deploy:` block is
validated, deployed to GCP via `ddt deploy`, scheduled in Cloud Composer (Airflow),
executed automatically, and the results land in the GCS warehouse. Then verify that
re-deploying is idempotent and `ddt undeploy` cleans up without touching data.

**This scenario tests new feature code.** The `deploy:` block, `ddt deploy`, and
`ddt undeploy` commands do not exist yet — the first run will surface implementation
findings that drive development of the `batch-deployment` feature.

**The core questions this scenario answers:**
1. Does `ddt validate` accept and check a `deploy:` block in the pipeline YAML?
2. Does `ddt deploy` provision a Cloud Composer DAG and Cloud Run job from the pipeline YAML?
3. Does the deployed DAG execute the pipeline on schedule and write to GCS?
4. Is re-deploying idempotent — no duplicate DAGs?
5. Does `ddt undeploy` cleanly remove the job without touching warehouse data?

## Target Component

This scenario tests ddt's own CLI and GCP provisioning layer — not an external API.
The pipeline used as the test vehicle is `github_repos` (Apache org, public repos, no
auth required), which keeps data fetching simple and isolates any failures to the
deployment infrastructure.

## Test Phases

### Phase 1 — Pipeline YAML with `deploy:` block

1. Add a `deploy:` block to `pipelines/github_repos.yml` in the test clone:
   ```yaml
   deploy:
     schedule: "0 8 * * *"
   ```
2. Run `ddt validate github_repos` — does it accept the `deploy:` block without error?
3. Verify the schedule is checked: set `schedule: "not a cron"` and confirm validate
   rejects it with a clear error message, then restore the valid schedule.
4. Check that `ddt validate` on a pipeline without a `deploy:` block is unaffected
   (no regression).

Phase 1 success: `ddt validate github_repos` passes with the `deploy:` block present
and rejects an invalid cron expression with an actionable error.

### Phase 2 — `ddt deploy` provisions GCP infrastructure via Terraform

1. Verify prerequisites:
   - `gcloud auth list` — authenticated
   - `catalog: gcp` in `project.yml`
   - `ddt gcp setup` completed (warehouse bucket exists)
   - Cloud Composer API enabled: `gcloud services list --enabled | grep composer`
   - Cloud Run API enabled: `gcloud services list --enabled | grep run`
2. Run `ddt deploy github_repos` — record the full output.
3. Check what was provisioned:
   - Cloud Composer environment: `gcloud composer environments list --locations us-central1`
   - Composer DAG: confirm `github_repos` DAG appears in the Composer UI or via:
     `gcloud composer environments run <env> --location us-central1 dags list`
   - Cloud Run job: `gcloud run jobs list --region us-central1`
4. Check `project.yml` — was `deployments.github_repos` written with `schedule`,
   `dag_id`, and `cloud_run_job`?
5. **Verify Terraform state** — confirm resources were provisioned via the
   `batch_pipeline` Terraform module, not raw gcloud calls:
   ```bash
   ls ~/.ddt/terraform/pipelines/github_repos/
   # Expected: main.tf  outputs.tf  terraform.tfstate  terraform.tfvars.json  variables.tf
   terraform -chdir=~/.ddt/terraform/pipelines/github_repos show
   # Expected output contains:
   #   google_cloud_run_v2_job.pipeline
   #   google_storage_bucket_object.dag
   ```
6. Confirm the Terraform state records the correct resource IDs — the Cloud Run job
   name in state must match the job listed by `gcloud run jobs list`.

Phase 2 success: `ddt deploy github_repos` completes, the DAG appears in Composer,
the Cloud Run job exists, `project.yml` records the deployment state, and Terraform
state at `~/.ddt/terraform/pipelines/github_repos/` contains both provisioned resources.

### Phase 3 — DAG execution and data verification

1. If the DAG was deployed paused, unpause it:
   `gcloud composer environments run <env> --location us-central1 dags unpause -- github_repos`
2. Trigger a manual run to avoid waiting for the schedule:
   `gcloud composer environments run <env> --location us-central1 dags trigger -- github_repos`
3. Monitor execution — wait for the DAG run to complete (success or failure).
4. If it fails, diagnose: is the Cloud Run job failing? Check Cloud Run logs:
   `gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=ddt-job-github-repos"`
5. After a successful run, verify data in GCS:
   `gsutil ls gs://<warehouse-bucket>/github_repos/github_repos/data/`
6. Query the warehouse:
   ```bash
   DDT_PROJECT_DIR=$CLONE uv --directory /path/to/ddt run python -c \
     "from ddt.warehouse_reader import query; print(query('SELECT COUNT(*), MAX(name) FROM github_repos.github_repos'))"
   ```

Phase 3 success: DAG run completes, Parquet files exist in GCS, warehouse query
returns rows.

### Phase 4 — Idempotency and teardown

1. Run `ddt deploy github_repos` a second time — same pipeline YAML, no changes.
2. Confirm no duplicate DAG was created:
   `gcloud composer environments run <env> --location us-central1 dags list | grep github_repos`
   (should appear exactly once)
3. Confirm the Terraform state directory still exists and still shows exactly two
   managed resources (no duplicates): `terraform -chdir=~/.ddt/terraform/pipelines/github_repos show`
4. Run `ddt undeploy github_repos`.
5. Confirm the DAG is removed from Composer and the Cloud Run job is gone.
6. **Verify Terraform state directory was removed** by `ddt undeploy`:
   ```bash
   ls ~/.ddt/terraform/pipelines/github_repos/
   # Expected: No such file or directory
   ```
7. Confirm warehouse data is NOT deleted — GCS files remain:
   `gsutil ls gs://<warehouse-bucket>/github_repos/github_repos/data/`
8. Confirm `deployments.github_repos` is removed from `project.yml`.

Phase 4 success: second deploy is idempotent (Terraform applies a diff, not a
duplicate); undeploy runs `terraform destroy` and removes the state dir without
touching warehouse data.

## Success Criteria

- [ ] Phase 1: `ddt validate github_repos` accepts a `deploy: { schedule: "0 8 * * *" }` block
- [ ] Phase 1: `ddt validate` rejects an invalid cron expression with a clear error message
- [ ] Phase 1: `ddt validate` on a pipeline without `deploy:` is unaffected (no regression)
- [ ] Phase 2: `ddt deploy github_repos` completes without error
- [ ] Phase 2: Cloud Composer DAG named `github_repos` is visible after deploy
- [ ] Phase 2: Cloud Run job for the pipeline exists after deploy
- [ ] Phase 2: `project.yml` records `deployments.github_repos` with schedule, dag_id, cloud_run_job
- [ ] Phase 2: `ddt deploy` on a pipeline with no `deploy:` block exits with a clear error
- [ ] Phase 2: `ddt deploy` without `catalog: gcp` in `project.yml` exits with a clear error
- [ ] Phase 2: Terraform state exists at `~/.ddt/terraform/pipelines/github_repos/terraform.tfstate`
- [ ] Phase 2: `terraform show` lists `google_cloud_run_v2_job.pipeline` and `google_storage_bucket_object.dag`
- [ ] Phase 3: DAG run completes successfully (no Airflow task failures)
- [ ] Phase 3: Parquet files appear in `gs://<warehouse-bucket>/github_repos/github_repos/data/`
- [ ] Phase 3: Warehouse query returns rows (data is correct and readable)
- [ ] Phase 4: Second `ddt deploy` produces exactly one DAG (idempotent)
- [ ] Phase 4: `ddt undeploy github_repos` removes the DAG and Cloud Run job
- [ ] Phase 4: Terraform state directory is removed after `ddt undeploy`
- [ ] Phase 4: GCS data files are untouched after `ddt undeploy`

## Known Complexity

- **Cloud Composer provisioning time:** A new Composer environment takes 15–30 minutes
  to provision. If this is the first deploy on a fresh project, plan for a long wait.
  If a Composer environment already exists (from a previous deploy), subsequent deploys
  reuse it and are much faster.
- **Container image:** The Cloud Run job image is built via Cloud Build and pushed to
  Artifact Registry. ddt source is vendored into the build context alongside the user's
  `pipelines/` and `connectors/` directories. A minimal `project.yml` (no secrets) is
  generated; GCP auth in the container comes from the service account attached to the job.
- **IAM propagation:** After `ddt deploy` creates any new service account bindings,
  wait 60 seconds before triggering the DAG — IAM changes can take time to propagate.
- **API enablement:** Cloud Composer, Cloud Run, and (if using container images)
  Artifact Registry APIs must be enabled before `ddt deploy` runs. If they aren't,
  the error should be actionable (tell the user which API to enable and the exact
  `gcloud services enable` command).
- **Cron validation:** Cron expressions are not trivial to validate — "0 8 * * *" is
  valid, "0 25 * * *" is technically parseable but means "never" (hour 25 doesn't
  exist). Test with a clearly invalid string and a valid string.

## Known Expected Findings (Pre-identified)

All original blocking findings have been resolved. Potential findings to watch for in
ongoing runs:

- **Terraform provider download time:** First `ddt deploy` on a new machine downloads
  the Google provider (~30 MB). Subsequent runs reuse the cache at
  `~/.ddt/terraform/.plugin-cache`. Slow first-deploy is expected, not a bug.
- **Terraform import edge cases:** If a Cloud Run job exists in GCP but not in
  Terraform state (e.g. deployed from a different machine, or state file was lost),
  `_import_existing_cloud_run_job()` will import it before apply. If the import fails
  for an unexpected reason, `terraform apply` may still succeed via an in-place update.
- **Composer provisioning time:** If no Composer environment exists, provisioning takes
  20–30 minutes. The deploy command prints progress every 30 seconds.
- **`ddt undeploy` requires local Terraform state:** If the state directory
  `~/.ddt/terraform/pipelines/<name>/` is absent (different machine, state deleted),
  `ddt undeploy` will fail with a clear error pointing to the manual gcloud command.

## Credentials Required

No new credentials beyond what the `gcp-data-lake` scenario already uses:

- `catalog: gcp` — set in `testing/test_config.yml`
- `gcp.project_id` and `gcp.region` — set in `testing/test_config.yml`
- GCP authenticated via `gcloud auth application-default login`
- `gcp.warehouse_bucket` — set after `ddt gcp setup` completes

**Additional GCP APIs that must be enabled before this scenario runs:**
```bash
gcloud services enable composer.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable artifactregistry.googleapis.com  # if using container image approach
```

Note this in `test_config.yml.example` as a GCP prerequisite comment, not a new key.

## By Design Decisions from Prior Runs

(None yet — first run of this scenario)

## Notes for Agent

- This scenario tests **unimplemented feature code**. Expect Phase 1 and Phase 2
  to produce Blocking findings on the first run. Document them and stop — do not
  attempt to work around missing CLI commands or schema by writing custom Python.
- Use `github_repos` as the test pipeline — it is simple (no auth, flat JSON, one
  iterate axis), which isolates failures to the deployment layer.
- The CLONE for this scenario should have `catalog: gcp`, a valid `gcp.warehouse_bucket`,
  and `gcp.setup_status: complete` in `project.yml` (same config as the `gcp-data-lake`
  scenario — copy from `testing/test_config.yml` after adding `warehouse_bucket`).
- If Cloud Composer is already provisioned from a previous run (check with
  `gcloud composer environments list`), note the environment name — `ddt deploy`
  should reuse it rather than creating a new one.
- For Phase 3 DAG execution, prefer `dags trigger` over waiting for the scheduled
  time — the schedule is set to 8am UTC and you don't want to wait for it.
- Record the exact Cloud Run job name and Composer environment name from Phase 2
  output — you'll need these to verify Phase 4 cleanup.
- If Phase 3 reveals a Cloud Run job execution failure, always check Cloud Run logs
  first (`gcloud logging read`) before diagnosing at the ddt layer.
