# Scenario: Warehouse Transformation (The dbt Replacement Test)

## Goal

Test whether ddt + Claude can replace dbt — i.e., after ingesting data via ddt
pipelines, can Claude build analytical models (joins, aggregations, CTEs) on top
of the ddt warehouse, and can those models be persisted as new tables?

**How ddt's warehouse works:** ddt writes Parquet files to `warehouse/<namespace>/<table>/data/`.
The MCP `query_warehouse` tool runs ad-hoc DuckDB against these files using
`read_parquet()` globs. There is no persistent `.db` file. Each query is ephemeral.

**The core questions:**
1. Can Claude write useful multi-table SQL models against ddt Parquet data via MCP?
2. Can query results be written back as a new warehouse table (materialized model)?
3. If not, what is missing to make ddt + Claude a true dbt replacement?

## Target Data

This scenario requires data from prior rounds:
- `warehouse/github/github_repos/` — from Round 1 (github-repos) or Round 2 (github-private-repos)
- `warehouse/github/github_commits/` — from Round 4 (github-commits)

**If github_commits data is not yet available:** Run the github-commits scenario first.
This scenario cannot proceed without at least two tables in the warehouse.

## Test Phases

### Phase 1 — Warehouse Introspection

Test whether Claude (via MCP) can discover what's in the warehouse without being told:

1. Use the MCP `query_warehouse` tool to query `github.github_repos` — does it work?
2. Use the MCP `query_warehouse` tool to query `github.github_commits` — does it work?
3. Attempt to run a `DESCRIBE` or `PRAGMA table_info()` query on a table — does the
   tool support schema introspection?
4. Attempt to list all tables in the warehouse — is there a tool for this, or must
   the user tell Claude what tables exist?

Phase 1 success: document what the MCP tools can and cannot discover autonomously.

### Phase 2 — Multi-Table Analytical Query

Build a useful analytical model joining two ddt warehouse tables:

Goal: "Top 10 repositories by commit count in the last 90 days, with their primary
language and star count"

```sql
WITH commit_counts AS (
    SELECT
        -- extract repo name from commit html_url or from a join
        COUNT(*) AS commit_count,
        DATE_TRUNC('week', author_date) AS week
    FROM github.github_commits
    WHERE author_date >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY week
),
repo_info AS (
    SELECT name, language, stargazers_count
    FROM github.github_repos
)
-- join as able given the available fields
SELECT ...
```

1. Use `query_warehouse` MCP tool to run this query (or a version of it)
2. Identify: what fields are available to join on? Can commits be joined to repos?
   (This depends on what fields the github_commits pipeline captured.)
3. Iterate: if the first query fails, fix and retry — as a dbt user would iterate
   on a model.

Phase 2 success: at least one multi-table join produces useful output.

### Phase 3 — Model Persistence (The dbt-parity Test)

Test whether a transformation result can be saved as a new warehouse table:

1. Attempt to use `query_warehouse` to write results back:
   ```sql
   COPY (SELECT ...) TO 'warehouse/github/repo_activity/data/part-001.parquet'
   ```
2. If COPY fails (MCP tool may restrict writes), document the limitation
3. Attempt an alternative: use the MCP `write_pipeline` tool to create a new pipeline
   that produces the transformed data — is this the intended dbt-replacement pattern?
4. Alternatively: could a Python connector call DuckDB internally and write results?

Phase 3 success: determine whether model persistence is possible via any path,
and document the recommended pattern if it is, or the gap if it isn't.

## Success Criteria

- [ ] Phase 1: `query_warehouse` successfully queries both github_repos and github_commits
- [ ] Phase 1: Schema introspection capability documented (what does MCP expose?)
- [ ] Phase 2: At least one multi-table join runs successfully via MCP
- [ ] Phase 2: Query returns meaningful analytical output (not just raw row dumps)
- [ ] Phase 3: Model persistence path determined — documented whether possible and how
- [ ] Phase 3: If persistence is blocked, specific gap documented with proposed fix
  (e.g., "need a `materialize_model` MCP tool that writes DuckDB query results to warehouse")

## Known Complexity

- **No persistent DuckDB database:** Each `query_warehouse` call is a fresh DuckDB
  connection reading Parquet files via glob. CTEs and temp tables don't persist between
  calls. Multi-step transformations require a single SQL query.
- **Join key availability:** github_commits may not have a `repo_name` field that
  can directly join to github_repos.name. The join key depends on what the github_commits
  pipeline captured. This is the most likely blocker for Phase 2.
- **MCP write restrictions:** The `query_warehouse` tool likely runs read-only DuckDB.
  A `COPY TO` or `CREATE TABLE AS` may be blocked or require a different tool.
- **Table discovery:** There is likely no `list_tables` MCP tool. Claude must know
  (or be told) which tables exist in the warehouse.

## Known Expected Findings (Pre-identified)

- **Expected MCP gap:** `query_warehouse` is likely read-only — no write-back. This
  means transformations are ephemeral: useful for ad-hoc analysis but not for
  building a persistent analytics layer.
- **Expected MCP gap:** No table discovery tool — Claude cannot autonomously know
  what tables exist without being told or reading the filesystem.
- **To investigate:** Is the intended dbt-replacement pattern to build a ddt pipeline
  (type: python) that reads from the warehouse with DuckDB and writes a transformed
  result back? If so, this should be documented and the skill should guide users to it.
- **Enhancement:** A `materialize_model` MCP tool (takes a SQL string, writes result
  to `warehouse/<namespace>/<model_name>/data/`) would close the dbt gap and make
  ddt a true ELT stack rather than just EL.

## Credentials Required

None — uses data already in the local warehouse from prior rounds.

## By Design Decisions from Prior Runs

(None yet — first run of this scenario)

## Notes for Agent

- The test project is `/Users/zephschafer/Documents/GitHub/quipu/`
- **Prerequisites:** `github_repos` and `github_commits` must exist in the warehouse.
  If github_commits hasn't been run yet, use github_repos + github_private_repos
  (two tables) for the join test instead.
- The warehouse is at `warehouse/` relative to the quipu project root.
  DuckDB glob for github_repos: `warehouse/github/github_repos/data/*.parquet`
- For Phase 3, look at `ddt/mcp_server.py` to understand what the `query_warehouse`
  tool does under the hood — can it be coaxed into write mode, or is read-only hardcoded?
- If model persistence via MCP is not possible, the proposed fix should be specific:
  name the new MCP tool, describe its parameters, and explain how it would differ
  from `query_warehouse`. This proposal will inform whether to build the tool.
