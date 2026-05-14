# Feature: Batch Pipeline Deployment

**Status:** Draft
**ID:** batch-deployment
**Feature Set:** pipeline-deployment
**Created:** 2026-05-11
**Updated:** 2026-05-11

## Summary

ddt pipelines currently run only when a developer manually executes `ddt run`. This feature adds `ddt deploy`, a CLI command that provisions a production-grade scheduled batch job on GCP with a single command. Deployment configuration — schedule, resource overrides, and other options — lives in the pipeline's YAML file alongside the pipeline definition itself, so pipeline logic and deployment intent are always co-located. The deployed job runs on that schedule using Cloud Composer (managed Apache Airflow) for orchestration and Cloud Run for execution, writing results to the GCS warehouse. Infrastructure is defined in ddt's own Terraform modules — users never write Terraform directly.

## Problem

A pipeline that only runs manually is a development tool, not a data product. Today, making a ddt pipeline run automatically requires a developer to: set up a Composer environment by hand, author a DAG file, configure environment variables and credentials in GCP, wire up the Cloud Run job or Dataproc cluster, and maintain all of this independently of ddt. There is no connection between the pipeline definition (`pipelines/my_pipeline.yml`) and where or how it runs in production. Teams either skip scheduling entirely (pipelines stay manual) or reinvent orchestration infrastructure for each project.

## User Story

As a developer who has built and validated a ddt pipeline, I want to deploy it as a scheduled job with one command, so that my data lake stays current without manual intervention and I don't have to manage orchestration infrastructure separately from my pipelines.

## Requirements

### Must Have

- Deployment configuration is declared in the pipeline YAML under a `deploy:` block (schedule, and any infrastructure overrides)
- `ddt deploy <pipeline-name>` reads the `deploy:` block from the pipeline YAML and provisions the job on GCP
- `ddt deploy` errors clearly if the pipeline YAML has no `deploy:` block
- The deployed job runs `ddt run <pipeline-name>` on the schedule declared in the pipeline YAML
- Infrastructure is provisioned via ddt's own Terraform modules — the user never writes Terraform
- Scheduling is handled by Cloud Composer (managed Apache Airflow); execution runs in a Cloud Run job
- The deployed job writes to the same GCS warehouse bucket configured in `project.yml`
- `ddt undeploy <pipeline-name>` removes the DAG and Cloud Run job without touching warehouse data
- Running `ddt deploy` on an already-deployed pipeline is idempotent — re-reads the pipeline YAML and updates the deployment to match
- Deployment state is recorded in `project.yml` under a `deployments:` key
- Requires `catalog: gcp` and a completed `ddt gcp setup` — error clearly if not

### Nice to Have

- `ddt deploy status <pipeline-name>` shows current deployment state and last run outcome
- `ddt deploy status` (no arg) lists all deployed pipelines and their schedules
- Infrastructure overrides in the `deploy:` block (Composer environment size, Cloud Run CPU/memory, region, or execution target like Dataproc)
- `paused: true` in the `deploy:` block to provision a DAG without activating it
- Failure notifications (email or webhook) configurable in the `deploy:` block

## Acceptance Criteria

- [ ] `pipelines/github_repos.yml` with a `deploy: { schedule: "0 8 * * *" }` block is accepted by `ddt validate github_repos` without error
- [ ] `ddt deploy github_repos` completes without error on a project with `catalog: gcp` and completed GCP setup
- [ ] A Cloud Composer DAG named `github_repos` is visible in the Composer UI after deploy
- [ ] The DAG triggers on the cron schedule declared in the pipeline YAML and runs to completion
- [ ] The pipeline run inside the Cloud Run job writes rows to `gs://<warehouse-bucket>/github_repos/github_repos/data/`
- [ ] `ddt undeploy github_repos` removes the DAG from Composer and the Cloud Run job; warehouse data is untouched
- [ ] Running `ddt deploy github_repos` a second time does not create a second DAG — it updates the existing one to match the current pipeline YAML
- [ ] `ddt deploy github_repos` on a pipeline YAML with no `deploy:` block exits with a clear error message
- [ ] `ddt deploy github_repos` without `catalog: gcp` in `project.yml` exits with a clear error message
- [ ] Deployment state (`schedule`, `dag_id`, `cloud_run_job`) is written to `project.yml` under `deployments.github_repos`

## Out of Scope

- Streaming pipeline deployment (separate feature: `streaming-deployment`)
- Non-GCP deployment targets (AWS MWAA, Azure Data Factory, on-prem Airflow)
- Multi-pipeline DAGs — one ddt pipeline maps to exactly one Composer DAG
- dbt or SQL transformation as part of the batch job (separate concern)
- Pipeline monitoring dashboards or alerting UIs
- Automatic retries on fetch errors (separate feature: `incremental-retry`, if defined)
- Self-hosted Airflow (Cloud Composer only for now)

## Related Scenarios

- [`testing/scenarios/batch-deployment.md`](../testing/scenarios/batch-deployment.md) — full lifecycle: validate deploy: block → ddt deploy → Composer DAG execution → GCS write → idempotency → ddt undeploy

## Design Notes

### Infrastructure default: Composer + Cloud Run

The opinionated default is:
- **Cloud Composer** (managed Apache Airflow) — orchestrates scheduling; one DAG per deployed pipeline
- **Cloud Run job** — executes `ddt run <pipeline>` in a containerized environment; stateless, billed per execution

This keeps costs proportional to pipeline frequency and avoids always-on cluster costs. Future YAML overrides could target Dataproc (Apache Spark) for pipelines requiring cluster-scale processing.

### Terraform module

New module at `ddt/infra/modules/gcp/batch_pipeline/` defining:
- `google_cloud_run_v2_job` — containerized execution unit running `ddt run <name>`
- `google_composer_environment` — shared Airflow environment (created once, reused across pipelines)
- `google_composer_user_workloads_config_map` — DAG config per pipeline
- DAG file generated by ddt and uploaded to the Composer DAGs GCS bucket

The existing Terraform workflow (`ddt/gcp/terraform.py` — `provision()` / `destroy()`) can be extended or reused. The `_MODULE_DIR` pattern already copies `.tf` files to a work directory and runs `terraform apply`.

### Container image (open question)

The Cloud Run job must run `ddt run <pipeline>`, which requires the ddt package, the pipeline YAML, any Python connectors, and project credentials. Options:

1. **User builds image from their project** — ddt generates a `Dockerfile` in the user's project (quipu), the user runs `ddt build` which builds and pushes to Artifact Registry, then `ddt deploy` references that image
2. **ddt packages the user's pipelines/connectors at deploy time** — ddt zips `pipelines/` and `connectors/`, uploads to GCS, Cloud Run job downloads and executes at runtime
3. **Runtime clone** — Cloud Run job clones the quipu repo from GitHub on each run (requires GitHub credentials in the job)

Option 1 is the cleanest long-term but requires a `ddt build` step. Option 2 avoids a container build but adds runtime complexity. **This is the primary open design question for implementation.**

### Pipeline YAML: `deploy:` block

The `deploy:` block is added to a pipeline's YAML alongside `source`, `schema`, and `build`. Example:

```yaml
# pipelines/github_repos.yml
version: 1
name: github_repos
source: ...
schema: ...
build: ...

deploy:
  schedule: "0 8 * * *"   # cron — required
  paused: false            # optional, default false
  # Future overrides:
  # cloud_run:
  #   cpu: 2
  #   memory: 2Gi
  # execution_target: dataproc  # override from default Cloud Run
```

The `deploy:` block requires a new `Deploy` model in `ddt/config/models.py` and a new `Pipeline.deploy` optional field. `ddt validate` should check that `schedule` is a valid cron expression when a `deploy:` block is present.

### CLI surface

New commands in `ddt/cli.py` (following the existing typer sub-app pattern — see `gcp_app`):
```
ddt deploy <name>          # reads deploy: block from pipeline YAML, provisions GCP job
ddt undeploy <name>        # tears down Composer DAG + Cloud Run job
ddt deploy status [<name>] # shows deployment state from project.yml
```

### State in project.yml

```yaml
deployments:
  github_repos:
    schedule: "0 8 * * *"
    dag_id: github_repos
    cloud_run_job: ddt-job-github-repos
    deployed_at: "2026-05-11T08:00:00"
```

### GCP prerequisites

- `catalog: gcp` set in `project.yml`
- `ddt gcp setup` completed (SA, warehouse bucket, Secret Manager key)
- Cloud Composer API enabled (`composer.googleapis.com`)
- Cloud Run API enabled (`run.googleapis.com`)
- Artifact Registry API enabled (`artifactregistry.googleapis.com`) — if using container image approach
