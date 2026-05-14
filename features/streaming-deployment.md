# Feature: Streaming Pipeline Deployment

**Status:** Draft
**ID:** streaming-deployment
**Feature Set:** pipeline-deployment
**Created:** 2026-05-12
**Updated:** 2026-05-12

## Summary

ddt batch pipelines run on a schedule — fetch, transform, write. This feature adds a
second deployment mode: streaming. A pipeline with `source.type: pubsub` and
`deploy.type: streaming` runs as a continuous Apache Beam job on Google Cloud Dataflow,
subscribing to a Pub/Sub topic, projecting each message through the pipeline's YAML
schema, and writing windowed Parquet files to the same GCS warehouse as batch pipelines.
From the user's perspective, the interface is identical to `ddt deploy` for batch —
the deployment type is inferred from the pipeline YAML.

## Problem

Batch pipelines can only ingest historical or periodic data — they cannot respond to
events as they happen. A product team that wants to capture clickstreams, webhook
payloads, or real-time sensor readings today must stand up their own Kafka consumer
or Pub/Sub subscriber, manage Dataflow job lifecycle, and wire results into the
warehouse independently of ddt. There is no ddt-native path for event-driven data.

## User Story

As a developer with a GCP Pub/Sub topic receiving real-time events, I want to define
a ddt pipeline that subscribes to it and writes to the warehouse, so that event data
is queryable without me managing Dataflow infrastructure or writing Beam code.

## Requirements

### Must Have

- `source.type: pubsub` is a valid source type in the pipeline YAML, with a
  `subscription` field (full resource path: `projects/<project>/subscriptions/<name>`)
- `deploy.type: streaming` is a valid deploy type, alongside the existing batch
  (`schedule`-based) deploy
- `deploy.window_seconds` controls the Beam fixed-time window (default: 60); messages
  within the window are batched into a single Parquet file write
- `ddt deploy` provisions a Dataflow Flex Template job via the `streaming_pipeline`
  Terraform module when `deploy.type: streaming` is set
- The Dataflow job reads from Pub/Sub, applies the pipeline's schema projection
  to each message, and writes windowed Parquet files to
  `gs://<warehouse-bucket>/<pipeline>/<pipeline>/data/`
- `build.strategy: append` is required for streaming pipelines; `ddt validate`
  rejects `strategy: incremental` on a streaming pipeline with a clear error
- `ddt undeploy` drains the Dataflow job (flush in-flight messages to GCS) before
  destroying Terraform resources — does not use cancel
- Running `ddt deploy` on an already-deployed streaming pipeline is idempotent
- Deployment state recorded in `project.yml` under `deployments:` with `type`,
  `subscription`, `dataflow_job_id`, `deployed_at`
- Warehouse data is untouched by `ddt undeploy`

### Nice to Have

- `deploy.window_type: sliding | session` in addition to the default fixed window
- `deploy.max_messages_per_window` to cap file size
- `ddt deploy status click_events` shows current Dataflow job state and message lag
- IAM auto-provisioning: grant the Dataflow service account `roles/pubsub.subscriber`
  on the subscription during deploy

## Acceptance Criteria

- [ ] `ddt validate click_events` accepts `source.type: pubsub` with a `subscription` field
- [ ] `ddt validate click_events` accepts `deploy.type: streaming` with `window_seconds`
- [ ] `ddt validate` rejects `build.strategy: incremental` on a streaming pipeline with
      a clear error ("streaming pipelines require `build.strategy: append`")
- [ ] `ddt deploy click_events` completes without error on a project with `catalog: gcp`
      and completed GCP setup
- [ ] Dataflow job reaches `JOB_STATE_RUNNING` after deploy
- [ ] Messages published to the Pub/Sub topic appear as Parquet files in GCS within
      `window_seconds + 10` seconds
- [ ] Warehouse query returns the published rows with correct types
- [ ] Second `ddt deploy click_events` (same YAML) does not create a second Dataflow job
- [ ] `ddt undeploy click_events` drains the Dataflow job (`JOB_STATE_DRAINED`) before
      removing infrastructure
- [ ] GCS warehouse data survives `ddt undeploy` intact
- [ ] `deployments.click_events` is removed from `project.yml` after undeploy
- [ ] Terraform state at `~/.ddt/terraform/pipelines/click_events/` is removed after undeploy

## Out of Scope

- Non-GCP streaming runtimes (Flink, Spark Structured Streaming, Kinesis)
- Kafka as a streaming source (only Pub/Sub for now)
- Exactly-once delivery semantics (at-least-once is acceptable for v1)
- Sliding or session windows (fixed-time windows only for v1)
- Streaming → streaming joins or multi-source pipelines
- Schema evolution (adding columns to an existing streaming pipeline)
- Dead-letter queues for malformed messages

## Related Scenarios

- [`testing/scenarios/streaming-deployment.md`](../testing/scenarios/streaming-deployment.md) — full lifecycle: validate, deploy, publish messages, verify data, undeploy

## Design Notes

**New schema fields (in `ddt/config/models.py`):**
- `Source.type` — add `"pubsub"` to the literal type enum (alongside `"http"` and `"python"`)
- `PubSubSource` — new model with `subscription: str` field
- `Deploy.type` — add optional field `Literal["batch", "streaming"]`, default `"batch"`
- `Deploy.window_seconds` — add optional `int`, default `60`
- Validator: if `source.type == "pubsub"`, then `deploy.type` must be `"streaming"`;
  if `deploy.type == "streaming"`, then `build.strategy` must be `"append"`

**New Terraform module (`ddt/infra/modules/gcp/streaming_pipeline/`):**
- Resource: `google_dataflow_flex_template_job` (not `google_cloud_run_v2_job`)
- Variables: `project_id`, `region`, `pipeline_name`, `template_gcs_path`,
  `subscription`, `warehouse_bucket`, `sa_email`, `window_seconds`
- Output: `job_id` (used in `project.yml` deployments state)
- The Flex Template spec JSON is uploaded to GCS as part of the deploy process
  (similar to how the DAG file is uploaded in the batch path)

**New Beam runner (new file — `ddt/gcp/streaming_runner.py` or embedded in `batch_deploy.py`):**
```python
import apache_beam as beam
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.parquetio import WriteToParquet
from apache_beam.transforms.window import FixedWindows

def build_pipeline(pipeline_name, subscription, schema, warehouse_bucket, window_seconds):
    with beam.Pipeline(options=...) as p:
        (
            p
            | ReadFromPubSub(subscription=subscription)
            | beam.Map(json.loads)
            | beam.Map(project_message, columns=schema.columns)
            | beam.WindowInto(FixedWindows(window_seconds))
            | WriteToParquet(
                f"gs://{warehouse_bucket}/{pipeline_name}/{pipeline_name}/data/",
                schema=to_pyarrow_schema(schema.columns),
            )
        )
```

**Flex Template build process (in `batch_deploy.py` or new `streaming_deploy.py`):**
1. Build container image via Cloud Build (same as batch — vendor ddt source + user pipelines)
2. Generate Flex Template JSON spec pointing to the image
3. Upload spec to `gs://<warehouse-bucket>/templates/<pipeline_name>.json`
4. Run `terraform apply` with `template_gcs_path` pointing to the spec

**Drain on undeploy:**
`ddt undeploy` must call `gcloud dataflow jobs drain` and poll until
`JOB_STATE_DRAINED` before running `terraform destroy`. Terraform's GCP provider
deletes Dataflow jobs via cancel (not drain) — do not rely on `terraform destroy`
for clean shutdown.

**Note on idempotency:**
Dataflow Flex Template jobs cannot be "updated" in place — they must be replaced.
Second `ddt deploy` should check if a job with the same name is already running and
either: (a) leave it as-is if the pipeline YAML is unchanged, or (b) drain-and-replace
if the YAML changed. For v1, option (a) is acceptable: if a running job exists with
the same name, report "already deployed" and exit cleanly.

<!-- TODO: consider a testing/scenarios/streaming-error-handling.md covering malformed
     message handling and dead-letter queue scenarios -->
