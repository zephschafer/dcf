# Test Run: Streaming Deployment — Round 2
Date: 2026-05-12 | Tester: Claude Sonnet 4.6 | Scenario: streaming-deployment

## Outcome: PARTIAL SUCCESS

Phase 1 (validation) and Phase 2 (provisioning) passed after fixing 4 new findings.
Phase 3 (message ingestion/verification) and Phase 4 (undeploy) were blocked by GCP zone
capacity exhaustion across all attempted zones — a GCP infrastructure issue, not a pvc bug.

---

## Success Criteria

- [x] `pvc validate click_events` passes with streaming type annotation
- [x] Cloud Build produces a container image in Artifact Registry
- [x] Flex Template spec uploaded to GCS
- [x] Terraform provisions `google_dataflow_flex_template_job` without error
- [x] Dataflow job reaches `JOB_STATE_RUNNING` (control plane; job ID assigned)
- [ ] Dataflow workers start and begin consuming Pub/Sub messages
- [ ] Parquet files appear in `gs://pvc-warehouse-quipu-data-generator/click_events/click_events/data/`
- [ ] `pvc query "SELECT * FROM click_events.click_events LIMIT 10"` returns 10 rows
- [ ] Second `pvc deploy click_events` succeeds or handles gracefully
- [ ] `pvc undeploy click_events --yes` drains and removes the job

---

## What Worked

- `pvc validate click_events`: correctly showed `(streaming, subscription: ..., 5 columns)` ✓
- Cloud Build + Artifact Registry: image built successfully with `apache-beam[gcp]` ✓
- Flex Template spec upload to GCS: clean JSON, correct structure ✓
- Terraform provisioning: `google_dataflow_flex_template_job` created in both us-central1 and us-east1 ✓
- Dataflow job control plane reached `JOB_STATE_RUNNING` in multiple attempts ✓

---

## What Failed

### GCP zone capacity exhaustion (F-046)
Every attempted Dataflow worker zone was exhausted:
- us-central1-a, us-central1-b, us-central1-c, us-central1-f
- us-east1-b, us-east1-c

Error: `ZONE_RESOURCE_POOL_EXHAUSTED: Instance '...' creation failed: The zone '...' does not have enough resources available`

This prevented workers from ever starting, so no messages were consumed and no Parquet was written.

---

## New Findings (Round 2)

### F-043 — FIXED: `google_dataflow_flex_template_job` not in GA Terraform provider
`main.tf` used `hashicorp/google` which does not support `google_dataflow_flex_template_job`.
Fix: switched to `hashicorp/google-beta ~> 5.0`.

### F-044 — FIXED: Wrong Dockerfile base image for Flex Template
Original image `python:3.12-slim` has no Dataflow launcher. Fix: changed to
`gcr.io/dataflow-templates-base/python312-template-launcher-base` with
`ENV FLEX_TEMPLATE_PYTHON_PY_FILE` instead of `ENTRYPOINT`.

### F-045 — OPEN: `pvc gcp setup` does not grant `roles/dataflow.worker` to the service account
Dataflow workers need `roles/dataflow.worker` on the SA. `bootstrap.py` only grants
storage/bigquery roles. Users get a cryptic IAM error during job startup.

### F-046 — OPEN (UX/Enhancement): No actionable guidance when zone capacity is exhausted
When `ZONE_RESOURCE_POOL_EXHAUSTED` occurs, pvc surfaces the raw Terraform error with no
suggestion to retry in a different zone or region. Enhancement: detect this error and suggest
`pvc deploy --region us-east4` or similar.

---

## Pipeline Produced

See `pipeline.yml` in this directory.

```yaml
version: 1
name: click_events
source:
  type: pubsub
  subscription: projects/quipu-data-generator/subscriptions/pvc-test-clicks-sub
schema:
  columns:
    - {name: event_id, path: event_id, type: string}
    - {name: user_id, path: user_id, type: integer}
    - {name: action, path: action, type: string}
    - {name: page, path: page, type: string}
    - {name: timestamp, path: timestamp, type: timestamp}
build:
  strategy: append
deploy:
  type: streaming
  window_seconds: 60
```

---

## Proposed Fixes

1. **F-045**: In `gcp/bootstrap.py`, add `roles/dataflow.worker` to the SA grants alongside
   the existing storage/bigquery roles.

2. **F-046**: In `gcp/streaming_deploy.py` `_wait_for_running()`, detect
   `ZONE_RESOURCE_POOL_EXHAUSTED` in job error messages and surface a human-readable
   message with a suggestion to retry in another zone or pass `--region`.
