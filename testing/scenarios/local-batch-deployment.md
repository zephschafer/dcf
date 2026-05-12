# Scenario: Local Batch Deployment

## Goal

Test the full local batch deployment lifecycle: a pipeline YAML with a `deploy: type: batch`
block is built into a Docker image on the local machine, run once as a container to verify
end-to-end execution, and then cleanly removed via `pvc undeploy`. No GCP account, no Cloud
Build, no Terraform — only Docker.

**The core questions this scenario answers:**
1. Does `pvc deploy` (with `catalog: local`) build a Docker image locally and run it?
2. Does the container correctly execute `pvc run` and write Parquet to the mounted warehouse?
3. Is re-deploying idempotent (layer-cached rebuild + re-run)?
4. Does `pvc undeploy` remove the image cleanly?
5. Are env vars referenced in the pipeline YAML (`{{ env.GH_PAT }}`) available inside the container?

## Target Component

This scenario tests pvc's local Docker deployment path (`pvc/local_deploy.py`). The pipeline
used as the test vehicle is `github_repos`: a simple HTTP source with bearer auth, six
columns, append strategy — already validated in many prior test runs.

## Test Pipeline

```yaml
version: 1
name: github_repos
source:
  type: http
  url: https://api.github.com/user/repos
  auth:
    type: bearer
    token: "{{ env.GH_PAT }}"
  params:
    per_page: 100
schema:
  columns:
    - {name: id, path: id, type: integer}
    - {name: name, path: name, type: string}
    - {name: full_name, path: full_name, type: string}
    - {name: private, path: private, type: boolean}
    - {name: stargazers_count, path: stargazers_count, type: integer}
    - {name: updated_at, path: updated_at, type: timestamp}
build:
  strategy: append
deploy:
  type: batch
  schedule: "0 8 * * *"
```

## Test Phases

### Phase 1 — Setup and Validation

1. Clone quipu and inject `test_config.yml` as `project.yml` (standard test setup).
2. Overwrite `catalog: local` in `project.yml` (test_config may have `catalog: gcp`).
3. Write the `github_repos.yml` pipeline above to `$CLONE/pipelines/`.
4. Run `pvc validate github_repos` — should confirm `(batch, schedule: 0 8 * * *, 6 columns)`.
5. Run `docker info` — confirm Docker Desktop is running.

Phase 1 success: validation passes cleanly; Docker is available.

### Phase 2 — Local Deploy

1. Run `pvc deploy github_repos`.
   - Expect: Docker image `pvc-local/github_repos:latest` is built.
   - Expect: Container runs `pvc run github_repos` and exits 0.
   - Note the build time (first build ~1 min; subsequent builds should be layer-cached).
2. Run `docker images | grep pvc-local/github_repos` — confirm image exists.
3. Run `ls $CLONE/warehouse/github_repos/github_repos/data/` — Parquet file(s) should be present.
4. Run `pvc deploy-status github_repos` — should show `batch (local Docker)` and image tag.

Phase 2 success: image built, container ran to completion, Parquet data in warehouse.

### Phase 3 — Query and Verification

1. Run `pvc query "SELECT COUNT(*) FROM github_repos.github_repos"` — row count > 0.
2. Run `pvc query "SELECT name, stargazers_count FROM github_repos.github_repos LIMIT 5"` — spot-check data.
3. Verify all 6 columns are present and typed correctly (id as integer, updated_at as timestamp).

Phase 3 success: warehouse is queryable; data matches expected GitHub repo schema.

### Phase 4 — Idempotency and Undeploy

1. Run `pvc deploy github_repos` again — should rebuild (fast, layer-cached) and re-run.
2. Check that `warehouse/github_repos/github_repos/data/` has a second Parquet file (append strategy).
3. Run `pvc undeploy github_repos --yes`.
4. Run `docker images | grep pvc-local/github_repos` — should show no output (image removed).
5. Confirm `project.yml` no longer contains `deployments.github_repos`.

Phase 4 success: undeploy removes image; no Docker artifacts remain; warehouse data is untouched.

## Success Criteria

- [ ] Phase 1: `pvc validate github_repos` passes with `(batch, schedule: 0 8 * * *, 6 columns)`
- [ ] Phase 1: `docker info` succeeds (Docker is running)
- [ ] Phase 2: `pvc deploy github_repos` completes without error
- [ ] Phase 2: `docker images | grep pvc-local/github_repos` shows the image
- [ ] Phase 2: Terminal output shows `pvc run github_repos` running to completion inside the container
- [ ] Phase 2: Parquet file(s) exist in `warehouse/github_repos/github_repos/data/`
- [ ] Phase 2: `pvc deploy-status github_repos` shows `batch (local Docker)` and image tag
- [ ] Phase 3: `SELECT COUNT(*)` returns > 0 rows
- [ ] Phase 3: All 6 columns present and correctly typed
- [ ] Phase 4: Second `pvc deploy github_repos` completes without error (idempotency)
- [ ] Phase 4: Second run appends a new Parquet file (strategy: append)
- [ ] Phase 4: `pvc undeploy github_repos --yes` completes without error
- [ ] Phase 4: `docker images | grep pvc-local/github_repos` returns empty
- [ ] Throughout: No GCP credentials used or required at any point

## Known Complexity

- **Env var forwarding**: The pipeline YAML uses `{{ env.GH_PAT }}` but `local_deploy.py`
  runs the container with `docker run --rm`. Host env vars are not automatically forwarded
  into the container. This is the most likely source of a blocking finding — watch for the
  container failing to authenticate with GitHub.

- **Volume mount path**: The container mounts `project_root/warehouse` to `/app/warehouse`.
  If `_project_root()` resolves incorrectly (e.g., to the pvc repo rather than the clone),
  data will land in the wrong place.

- **Layer caching**: The second `pvc deploy` should use Docker's layer cache for the
  `pip install` step (~5s rebuild). If it doesn't, each deploy takes ~1 min — note it as
  a UX finding.

- **schedule field for local batch**: The `deploy: schedule:` field is required by the
  `Deploy` model when `type: batch` but has no effect locally. Users may be confused about
  why they need a cron expression to deploy locally.

## Known Expected Findings (Pre-identified)

- **Blocking (likely)**: `local_deploy.py` does not forward `{{ env.* }}` variables from
  the host into the `docker run` command. The container will fail to fetch GitHub data unless
  `GH_PAT` is baked into the image (which it should not be). Fix: scan the pipeline YAML for
  `{{ env.VAR }}` references and pass each as `-e VAR=$VAR` on the docker run command.

- **Minor/UX (possible)**: `deploy: schedule:` is required for `type: batch` even when
  deploying locally (where scheduling has no effect). Consider making `schedule` optional
  when `catalog: local`.

## Credentials Required

- `GH_PAT` — GitHub personal access token (already in `testing/test_config.yml`)
- Docker Desktop — must be running on the test machine; no GCP credentials needed

## By Design Decisions from Prior Runs

(None yet — first run of this scenario)

## Notes for Agent

- Full command prefix: `PVC_PROJECT_DIR=$CLONE uv --directory /Users/zephschafer/Documents/GitHub/pvc run pvc <command>`
- After injecting `test_config.yml` as `project.yml`, explicitly set `catalog: local` — the test config likely has `catalog: gcp`.
- The pipeline YAML for `github_repos` is well-tested from prior runs. If auth fails inside the container, it is almost certainly the env var forwarding issue (pre-identified above), not a schema or fetch bug.
- Do NOT use `catalog: gcp` at any point in this scenario.
- If Docker is not running, stop and ask the user to start Docker Desktop before continuing.
