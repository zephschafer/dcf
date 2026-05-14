# Scenario: Local Streaming Deployment

## Goal

Test the full local streaming deployment lifecycle: a pipeline YAML with `source.type: pubsub`
and `deploy.type: streaming` is deployed locally using Docker — an Apache Kafka broker (KRaft,
no Zookeeper) and a lightweight stream runner container — with no GCP account required. Messages
are published via `ddt publish`, consumed from Kafka, projected through the pipeline schema, and
written as windowed Parquet files to the local warehouse. Then verify idempotency and clean
teardown via `ddt undeploy`.

**This is the first ddt scenario that requires zero external credentials.**

**The core questions this scenario answers:**
1. Does `ddt deploy` (with `catalog: local`) start a Kafka broker and stream runner?
2. Does `ddt publish` successfully deliver messages to the local Kafka topic?
3. Does the stream runner consume messages, project them through the schema, and write Parquet?
4. Is re-deploying idempotent (old containers torn down, new ones started)?
5. Does `ddt undeploy` cleanly remove all Docker resources without touching warehouse data?

## Target Component

This scenario tests `ddt/local_deploy.py` (Docker orchestration), `ddt/local_stream_runner.py`
(Kafka consumer + windowed Parquet writer), and the `ddt publish` CLI command. No external API,
no GCP infrastructure.

## Test Pipeline

```yaml
version: 1
name: click_events
source:
  type: pubsub
  subscription: projects/local-dev/subscriptions/click-events-sub
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
  window_seconds: 15
```

Note: `source.subscription` uses a placeholder path (`projects/local-dev/...`). Local mode
ignores it — the Kafka topic is always derived as `ddt-{pipeline_name}`. The subscription
field exists because the schema requires it for `source.type: pubsub`.

## Test Phases

### Phase 1 — Setup and Validation

1. Clone quipu and inject `test_config.yml` as `project.yml` (standard test setup).
2. Overwrite `catalog: local` in `project.yml`.
3. Write the `click_events.yml` pipeline above to `$CLONE/pipelines/`.
   Use `window_seconds: 15` for fast iteration (not 60).
4. Run `ddt validate click_events` — should show `(streaming, subscription: ..., 5 columns)`.
5. Run `docker info` — confirm Docker Desktop is running.
6. Run `docker pull apache/kafka:latest` — pre-pull to avoid timeout during deploy.

Phase 1 success: validation passes; Docker ready; Kafka image available locally.

### Phase 2 — Local Deploy

1. Run `ddt deploy click_events`.
   - Expect: Docker network `ddt-click_events` created.
   - Expect: Kafka container `ddt-kafka-click_events` started with KRaft (no Zookeeper).
   - Expect: Topic `ddt-click_events` created in Kafka.
   - Expect: Runner image `ddt-local/click_events-stream:latest` built.
   - Expect: Runner container `ddt-runner-click_events` started.
   - Note time for each step (Kafka ~5-10s; first image build ~1-2 min).
2. Run `docker ps` — confirm both `ddt-kafka-click_events` and `ddt-runner-click_events`
   show status `Up`.
3. Run `docker logs ddt-runner-click_events` — should contain "Local stream runner started".
4. Run `ddt deploy-status click_events` — should show `streaming (local Docker + Kafka)`,
   Kafka container name, topic `ddt-click_events`, external bootstrap `localhost:29092`, window 15s.

Phase 2 success: both containers running; runner log confirms successful startup.

### Phase 3 — Message Ingestion and Verification

1. Publish 10 messages:
   ```
   ddt publish click_events \
     '{"event_id":"evt-001","user_id":1,"action":"click","page":"/pricing","timestamp":"2026-05-12T20:00:00Z"}' \
     --count 10
   ```
   Confirm: "Published 10 messages to topic 'ddt-click_events'."

2. Wait 20-25 seconds (window_seconds=15 + buffer for flush and write).

3. Run `ls $CLONE/warehouse/click_events/click_events/data/` — at least one `.parquet` file present.

4. Run `ddt query "SELECT COUNT(*) FROM click_events.click_events"` — should return 10.

5. Run `ddt query "SELECT * FROM click_events.click_events LIMIT 5"` — spot-check all 5 columns.

6. Run `docker logs ddt-runner-click_events` — should contain "Wrote 10 rows to window-*.parquet".

Phase 3 success: 10 rows in warehouse, all 5 columns present and correctly typed.

### Phase 4 — Idempotency and Undeploy

1. Run `ddt deploy click_events` again — should tear down old containers and start fresh.
   Verify existing Parquet files in `warehouse/click_events/` are still present (undeploy
   does not touch warehouse data).
2. Run `ddt publish click_events '{"event_id":"evt-011","user_id":2,"action":"view","page":"/docs","timestamp":"2026-05-12T21:00:00Z"}' --count 3`.
3. Wait 20s — a new Parquet file appears (confirms the fresh deployment is working).
4. Run `ddt undeploy click_events --yes`.
5. Run `docker ps` — no `ddt-kafka-click_events` or `ddt-runner-click_events`.
6. Run `docker network ls | grep ddt-click_events` — network removed.
7. Run `ls $CLONE/warehouse/click_events/click_events/data/` — Parquet files still present.

Phase 4 success: all Docker resources removed; warehouse data intact.

## Success Criteria

- [ ] Phase 1: `ddt validate click_events` passes with `(streaming, ...)` annotation
- [ ] Phase 1: `docker info` succeeds (Docker is running)
- [ ] Phase 2: `ddt deploy click_events` completes without error
- [ ] Phase 2: `docker ps` shows `ddt-kafka-click_events` with status `Up`
- [ ] Phase 2: `docker ps` shows `ddt-runner-click_events` with status `Up`
- [ ] Phase 2: `docker logs ddt-runner-click_events` contains "Local stream runner started"
- [ ] Phase 2: `ddt deploy-status click_events` shows `streaming (local Docker + Kafka)`
- [ ] Phase 3: `ddt publish click_events '...' --count 10` exits 0
- [ ] Phase 3: After 20-25s, at least one Parquet file exists in `warehouse/click_events/click_events/data/`
- [ ] Phase 3: `SELECT COUNT(*) FROM click_events.click_events` returns 10
- [ ] Phase 3: Query results contain all 5 columns (event_id, user_id, action, page, timestamp)
- [ ] Phase 3: `timestamp` column is typed as timestamp, not string
- [ ] Phase 3: `docker logs ddt-runner-click_events` confirms rows were written
- [ ] Phase 4: Second `ddt deploy click_events` completes without error (idempotency)
- [ ] Phase 4: `ddt publish` and window flush work on the fresh deployment
- [ ] Phase 4: `ddt undeploy click_events --yes` completes without error
- [ ] Phase 4: `docker ps` shows no ddt containers; network removed; warehouse data intact

## Known Complexity

- **Kafka startup timing**: `_wait_for_kafka()` retries for 30s using `KafkaAdminClient`.
  On slow machines or with cold Docker, Kafka may take 15-20s to bind. Watch for timeouts.

- **Dual-listener networking**: Kafka exposes two listeners — `INTERNAL` (for the runner
  container on the Docker network at `ddt-kafka-click_events:9092`) and `EXTERNAL` (for
  the host at `localhost:29092`). Misconfiguration shows as `NoBrokersAvailable` errors.

- **Window timer start**: The 15s window timer starts when the runner container starts,
  not when messages are published. Messages published just after a flush can wait up to
  15s for the next window. Always wait 25s after publishing to be safe.

- **First image build**: `ddt-local/click_events-stream:latest` is built fresh on first
  deploy (~1-2 min). Subsequent deploys (idempotency test) should be fast via layer cache.

- **`source.subscription` is required but unused locally**: The schema requires a
  `subscription` field for `source.type: pubsub`. For local mode this path is meaningless
  (Kafka topic is `ddt-{pipeline_name}`). The placeholder path `projects/local-dev/...`
  satisfies the schema but may surface as a UX confusion for new users.

## Known Expected Findings (Pre-identified)

- **UX (likely)**: `source.subscription` is required by the `PubSubSource` model even
  for local deployments where it has no effect. A user who wants a local-only streaming
  pipeline must still write a GCP-style subscription path. Consider making `subscription`
  optional when `catalog: local`, or accepting any string for local mode.

- **UX (possible)**: `window_seconds` cannot be overridden at deploy time without editing
  the pipeline YAML. A `--window-seconds N` flag on `ddt deploy` would improve local
  iteration speed (e.g., 5s windows during development).

- **Runtime (watch for)**: The `_wait_for_kafka()` timeout (30s) may be insufficient on
  first-pull or slow machines. If Kafka takes longer, deploy fails with an unhelpful error.
  Consider increasing the timeout or surfacing the Kafka logs on failure.

## Credentials Required

**None.** This scenario requires no API keys, no GCP credentials, and no cloud accounts.

- Docker Desktop must be running and able to pull from Docker Hub
- `apache/kafka:latest` pulled during Phase 1 (requires internet access)

## By Design Decisions from Prior Runs

(None yet — first run of this scenario)

## Notes for Agent

- Full command prefix: `DDT_PROJECT_DIR=$CLONE uv --directory /Users/zephschafer/Documents/GitHub/ddt run ddt <command>`
- After injecting `test_config.yml` as `project.yml`, explicitly set `catalog: local`.
- Use `window_seconds: 15` in the pipeline YAML (not 60) to keep Phase 3 under 30 seconds.
- Pull `apache/kafka:latest` in Phase 1 to avoid deploy timeout. Command: `docker pull apache/kafka:latest`
- The `source.subscription` placeholder `projects/local-dev/subscriptions/click-events-sub`
  satisfies the schema validator and is not used by the local runner.
- If both containers start but no Parquet appears after 30s, check `docker logs ddt-runner-click_events`
  for connection errors to Kafka — this points to a networking or timing issue.
- If `ddt query` fails to find the table, check that `warehouse_reader.py` is resolving the
  local `warehouse/` directory correctly under `catalog: local`.
- Do NOT use `catalog: gcp` at any point in this scenario.
