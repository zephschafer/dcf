# Scenario: GCP Data Lake (Remote Warehouse Round-Trip)

## Goal

Test the full GCP path: provision a GCS-backed Iceberg data lake with `ddt gcp setup`,
run a pipeline with `catalog: gcp`, and verify the data is queryable. This path has
never been tested. It is ddt's cloud offering — the equivalent of Fivetran writing to
Snowflake or BigQuery.

**The core questions:**
1. Does `ddt gcp setup` complete without errors on a fresh GCP project?
2. Does a pipeline run with `catalog: gcp` successfully write to GCS?
3. Is the GCS-backed warehouse queryable via the MCP `query_warehouse` tool?
4. Does the `new-pipeline` skill mention the GCP path, or does it assume `catalog: local`?

## Prerequisites

- A GCP project with billing enabled
- `gcloud` CLI installed and authenticated (`gcloud auth login`)
- Terraform installed (`terraform -version`)
- Owner or Editor permissions on the GCP project

Confirm these before starting the test. If Terraform is not installed, note this as
a UX gap (ddt requires Terraform but doesn't document this requirement).

## Target API

Use github_repos (same as Round 1/2) — the simplest pipeline, to isolate any
GCP-specific failures from pipeline logic failures. The data content is not the
focus; the write path is.

```
GET https://api.github.com/user/repos?visibility=private&per_page=100
```

Auth: Bearer token (GITHUB_TOKEN — already configured).

## Test Phases

### Phase 1 — GCP Setup

1. Read the `ddt gcp setup` CLI output and documentation:
   - Run `ddt gcp setup --help` — what parameters does it accept?
   - What does it provision? (service account, GCS bucket, Terraform state bucket, IAM)
2. Run `ddt gcp setup` — record the full output
3. Note: does it ask for GCP project ID? Region? Or does it auto-detect from `gcloud`?
4. After setup, check `project.yml` — what fields were added?
5. Verify: the GCS bucket exists in the GCP console

Phase 1 success: `ddt gcp setup` completes, GCS bucket exists, `project.yml` updated.

### Phase 2 — Pipeline Run with GCP Catalog

1. Update `project.yml` to `catalog: gcp`
2. Run `ddt run github_private_repos --limit 1`
3. Verify: does ddt connect to GCS? Does it use Spark with Iceberg GCS catalog?
4. Record: any new Spark configuration errors? Authentication errors?
5. After successful run, verify the warehouse path in GCS:
   - Does the GCS bucket have Iceberg metadata files?
   - Does it have Parquet data files?
6. Run a second time — does incremental upsert work against GCS-backed Iceberg?

Phase 2 success: pipeline writes to GCS, deduplication works, data is in GCS.

### Phase 3 — Query the GCP Warehouse via MCP

1. With `catalog: gcp` set in `project.yml`, use the MCP `query_warehouse` tool:
   ```sql
   SELECT name, language FROM github.github_private_repos LIMIT 10
   ```
2. Does the MCP tool know to read from GCS instead of local? Or does it always
   read from the local `warehouse/` directory?
3. If MCP tool reads local only: this is a Major MCP finding — the query tool
   doesn't follow the catalog setting.
4. Check `ddt/mcp_server.py` and `ddt/warehouse_reader.py` — how does the reader
   determine where to look for Parquet files?

Phase 3 success: MCP tool queries GCS-backed data, or limitation documented.

### Phase 4 — Teardown

1. Run `ddt gcp teardown` (if it exists) or manually destroy Terraform resources
2. Confirm GCS bucket and service account are removed
3. Reset `project.yml` back to `catalog: local`

Phase 4 success: GCP resources cleaned up, no ongoing charges.

## Success Criteria

- [ ] Phase 1: `ddt gcp setup` completes without errors
- [ ] Phase 1: GCS bucket created and visible in GCP console
- [ ] Phase 1: `project.yml` updated with GCP metadata
- [ ] Phase 2: `ddt run` with `catalog: gcp` writes Parquet to GCS
- [ ] Phase 2: Incremental upsert works against GCS-backed Iceberg
- [ ] Phase 3: MCP `query_warehouse` reads from correct location (GCS or local)
- [ ] Phase 3: Query returns correct data
- [ ] Phase 4: GCP resources cleaned up

## Known Complexity

- **Terraform dependency:** `ddt gcp setup` runs Terraform. Terraform must be installed.
  If not, document as a UX gap (undocumented prerequisite).
- **GCP IAM propagation:** Service account permissions can take 30–60 seconds to
  propagate. If the pipeline run immediately after `gcp setup` fails with permissions
  errors, wait 60 seconds and retry before filing a bug.
- **Spark + GCS connector:** Spark requires the GCS connector JAR to write to GCS.
  ddt may need to configure `spark.jars` with the GCS connector. This is likely a
  new Spark configuration requirement — test whether it's already handled.
- **MCP catalog awareness:** `warehouse_reader.py` uses `read_parquet()` with a
  local glob pattern. It may not be aware of `catalog: gcp` at all — querying GCS
  from DuckDB requires `read_parquet('gs://bucket/path/*.parquet')`, which requires
  different DuckDB setup (GCS credentials). This is likely a Major MCP finding.

## Known Expected Findings (Pre-identified)

- **Expected UX gap:** `new-pipeline` skill makes no mention of `catalog: gcp` or
  when to use the GCP path. Users building for production would not know to do `gcp setup`.
- **Expected UX gap:** QUICKSTART.md and README.md mention `ddt gcp setup` but may
  not document the Terraform prerequisite or the full setup flow.
- **Expected Major (MCP):** `query_warehouse` likely reads from local `warehouse/`
  directory regardless of `catalog` setting — it does not query GCS directly.
- **To investigate:** Does Spark with GCS-backed Iceberg require the Google Cloud
  Storage connector JAR? If so, does ddt's `spark_session.py` configure it automatically?

## Credentials Required

- GCP credentials — `gcloud auth login` must be complete, OR a service account key
  must be available. Zeph provides access to the GCP project.
- GITHUB_TOKEN — already configured from Round 2.

Store GCP project ID as `gcp_project_id` in `project.yml` if `gcp setup` doesn't
collect it automatically.

## By Design Decisions from Prior Runs

(None yet — first run of this scenario)

## Notes for Agent

- The test project is `/Users/zephschafer/Documents/GitHub/quipu/`
- **Before starting:** Run `which terraform` to verify Terraform is installed.
  If missing, document as a UX finding and ask Zeph to install before proceeding.
- **Before starting:** Run `gcloud auth list` to verify GCP authentication.
- Use `github_private_repos` as the test pipeline (already exists in quipu/pipelines/).
  This minimizes variables — only the catalog changes between local and GCP runs.
- When checking GCS, use `gsutil ls gs://<bucket>/` to list warehouse files.
  Record the exact GCS path structure — does it match `warehouse/<namespace>/<table>/data/`?
- For Phase 3, check `ddt/warehouse_reader.py` before testing — if it only reads
  from local paths, note this as a finding and skip the MCP query test (the finding
  is already documented).
- **Cost note:** GCS Standard storage is $0.02/GB/month. The test data (a few Parquet
  files, kilobytes) will cost less than $0.01. Terraform state bucket adds minimal cost.
  Teardown in Phase 4 eliminates ongoing charges.
