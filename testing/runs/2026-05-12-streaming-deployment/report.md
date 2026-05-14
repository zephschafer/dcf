# Test Run: Streaming Pipeline Deployment — Round 1
Date: 2026-05-12 | Tester: Claude Sonnet 4.6 | Scenario: streaming-deployment

## Outcome: FAILURE

Blocked in Phase 1. All 6 pre-identified Blocking findings confirmed. Phases 2–4
were not reachable because the pipeline YAML cannot even be loaded by ddt's schema.

## Success Criteria

- [ ] Phase 1: `ddt validate click_events` accepts `source.type: pubsub` and `deploy.type: streaming`
- [x] Phase 1: `ddt validate` rejects malformed YAML with a clear error message
- [x] Phase 1: `ddt validate` on an existing batch pipeline is unaffected (no regression)
- [ ] Phase 2: `ddt deploy click_events` completes without error
- [ ] Phase 2: Dataflow job state is `JOB_STATE_RUNNING` after deploy
- [ ] Phase 2: `project.yml` records `deployments.click_events` with type, subscription, dataflow_job_id
- [x] Phase 2: `ddt deploy` on a pipeline with no `deploy:` block exits with a clear error
- [x] Phase 2: `ddt deploy` without `catalog: gcp` exits with a clear error
- [ ] Phase 2: Terraform state exists at `~/.ddt/terraform/pipelines/click_events/terraform.tfstate`
- [ ] Phase 2: `terraform show` lists `google_dataflow_flex_template_job.pipeline`
- [ ] Phase 3: All 10 published messages appear in the warehouse within 2 minutes
- [ ] Phase 3: Parquet files appear in `gs://<warehouse-bucket>/click_events/click_events/data/`
- [ ] Phase 3: `timestamp` column is correctly typed (not stored as string)
- [ ] Phase 4: Second `ddt deploy` produces exactly one running Dataflow job (idempotent)
- [ ] Phase 4: `ddt undeploy click_events` drains the job (state: `JOB_STATE_DRAINED`)
- [ ] Phase 4: Terraform state directory is removed after `ddt undeploy`
- [ ] Phase 4: GCS data files are untouched after `ddt undeploy`

Success criteria: 4/17 passed

## What Worked

- `ddt validate craigslist_apts` (batch regression): ✓ — no regression in batch validation
- `ddt validate click_events` malformed YAML: ✓ — rejects malformed column YAML with clear error
- `ddt deploy craigslist_apts` (no deploy block): ✓ — clear error: `'craigslist_apts' has no 'deploy:' block`
- `ddt deploy` with `catalog: local`: ✓ — clear error: `catalog is not 'gcp'. Batch deployment requires a GCP data lake.`

## What Failed

- `ddt validate click_events` — two validation errors:
  1. `source — Input tag 'pubsub' found using 'type' does not match any of the expected tags: 'http', 'python'`
  [→ Finding F-037: Blocking / Schema]
  2. `deploy.schedule — Field required` (streaming deploy has no `schedule` cron field)
  [→ Finding F-038: Blocking / Schema]

- `ddt deploy click_events` — fails at model load due to F-037 and F-038 before any deploy logic runs
  [→ Finding F-039: Blocking / Runtime]

## Pre-identified Findings Confirmed but Not Directly Observed (Blocked Upstream)

- No `streaming_pipeline` Terraform module at `ddt/infra/modules/gcp/streaming_pipeline/`
  [→ Finding F-040: Blocking / Runtime — confirmed by codebase inspection]

- No Beam runner code in ddt (no Pub/Sub → GCS pipeline)
  [→ Finding F-041: Blocking / Runtime — confirmed by codebase inspection]

- `ddt undeploy` would call `terraform destroy` (cancel) not drain on a Dataflow job
  [→ Finding F-042: Major / Runtime — confirmed by `batch_deploy.py` inspection]

## Error Messages Observed

```
Validation error in 'click_events': source — Input tag 'pubsub' found using 'type' does not match any of the expected tags: 'http', 'python'
Validation error in 'click_events': deploy.schedule — Field required
```

Both messages are clear and specific. The Pydantic error for the unknown source type
correctly names the valid alternatives. The missing `schedule` field error is accurate
but doesn't yet hint at what a streaming deploy block should look like (acceptable for
pre-implementation state).

## Pipeline Produced

See `pipeline.yml` in this directory — the intended pipeline YAML that ddt cannot yet
validate or deploy.

## Proposed Fixes (Implementation Roadmap)

The 6 findings together form the streaming-deployment implementation checklist:

1. **F-037 + F-038 (models.py):** Add `PubSubSource` model with `subscription: str`;
   extend `Source` union to include it; add `Deploy.type: Literal["batch","streaming"]`
   and `Deploy.window_seconds: int = 60`; make `schedule` optional (required only when
   `type == "batch"`); add validator: streaming pipelines must use `strategy: append`.

2. **F-039 (cli.py / batch_deploy.py):** `ddt deploy` must check `pipeline.deploy.type`
   and route to streaming deploy path when `type == "streaming"`.

3. **F-040 (new Terraform module):** Create `ddt/infra/modules/gcp/streaming_pipeline/`
   with `google_dataflow_flex_template_job` resource; Flex Template spec uploaded to GCS
   as part of deploy.

4. **F-041 (new Beam runner):** Create streaming pipeline code (e.g.
   `ddt/gcp/streaming_deploy.py`) that builds a Beam pipeline:
   `ReadFromPubSub → json.loads → project_message → WindowInto(FixedWindows) → WriteToParquet(GCS)`

5. **F-042 (batch_deploy.py undeploy path):** For streaming pipelines, `ddt undeploy`
   must drain (not destroy) the Dataflow job first: `gcloud dataflow jobs drain <id>`,
   poll until `JOB_STATE_DRAINED`, then run `terraform destroy`.
