# Test Run: Warehouse Transformation (The dbt Replacement Test)
Date: 2026-05-10 | Tester: Claude Sonnet 4.6 | Scenario: warehouse-transformation

## Outcome: PARTIAL SUCCESS

## Success Criteria

- [x] Phase 1: `query_warehouse` successfully queries both github_repos and github_commits
      → Queries `github.github_private_repos` (GCS) ✓. `github_repos.github_repos` exists
        only in local warehouse — blocked by catalog=gcp (see F-018, F-021)
- [x] Phase 1: Schema introspection capability documented (what does MCP expose?)
      → `DESCRIBE` works via `query_warehouse`; `list_warehouse_tables` returns full column
        schemas with types. Both are functional.
- [~] Phase 2: At least one multi-table join runs successfully via MCP
      → Works via read_parquet() bypass (mixing GCS-registered table + local path glob).
        Does NOT work using standard namespace.table syntax for local-only tables. [F-018]
- [x] Phase 2: Query returns meaningful analytical output (not just raw row dumps)
      → Aggregation query (repos by language with public/private breakdown) returned
        useful analytical results.
- [x] Phase 3: Model persistence path determined — documented whether possible and how
      → Possible via COPY TO with LIMIT workaround. Path is cumbersome and not GCS-aware.
        Proposed fix documented (F-020).
- [x] Phase 3: If persistence is blocked, specific gap documented with proposed fix
      → Gap documented: no `materialize_model` tool; COPY TO works locally only, not GCS [F-020]

## Prerequisites Note

`github_commits` was not available in the warehouse. Per scenario guidance, used
`github_repos.github_repos` (local only, 100 rows) and `github.github_private_repos`
(GCS + local, 12 rows) as the two tables for join testing.

Key context: quipu project has `catalog: gcp`. This shaped most of the findings.

## What Worked

- `list_warehouse_tables` MCP tool: ✓ Exists and works — returns GCS tables with full column
  schemas, types, and row counts. Schema introspection is complete and useful.
- `query_warehouse` on GCS tables: ✓ Queries `github.github_private_repos` cleanly with
  grouping, aggregation, and CTE syntax.
- `DESCRIBE` via `query_warehouse`: ✓ Returns column names, types, nullability correctly.
- `read_parquet()` bypass in query_warehouse: ✓ Users can reference local Parquet files
  directly with absolute paths as a workaround for local-only tables.
- Multi-table join (GCS + read_parquet bypass): ✓ Union of public/private repos by language
  returned correct analytical output.
- COPY TO local file: ✓ Works when LIMIT is included in subquery. Writes actual Parquet.

## What Failed

- `list_warehouse_tables` when catalog=gcp returns only GCS tables — 11 of 12 local
  warehouse tables are invisible to MCP. A project with existing local data and a new
  GCP catalog can no longer discover or query prior data via MCP.
  [→ Finding F-018: Major / MCP]

- `query_warehouse` of local-only tables (catalog=gcp) gives raw DuckDB CatalogException:
  `Catalog Error: Table with name "github_repos.github_repos" does not exist because schema
  "github_repos" does not exist.` No mention of the catalog mode or that the table exists
  locally but not in GCS.
  [→ Finding F-021: Minor / UX]

- COPY TO without LIMIT fails with cryptic parse error because `query_warehouse` auto-wraps
  non-LIMIT queries in `SELECT * FROM (...) _q LIMIT 500`. Error:
  `Parser Error: syntax error at or near "SELECT"` — the wrapped SQL is not shown to the user.
  [→ Finding F-019: Minor / UX]

- Model persistence to GCS is not supported: COPY TO writes only to local disk. When
  catalog=gcp, the written Parquet is invisible to `list_warehouse_tables` and won't
  be read back via namespace.table queries.
  [→ Finding F-020: Enhancement / MCP]

## Friction Points

- No github_commits table — scenario required re-scoping to public + private repos. This
  is expected per scenario guidance, not a pvc finding.
- The LIMIT bypass for COPY TO is accidental and fragile. Any user who forgets LIMIT
  gets a misleading parse error with no hint about the root cause.
- Raw `read_parquet()` paths are the only way to query local-only tables in GCP mode,
  but users have no way to discover those paths from MCP — they must know the warehouse
  layout (`warehouse/<namespace>/<table>/data/*.parquet`) or read the filesystem directly.

## Analytical Query Produced (Phase 2)

```sql
WITH public_repos AS (
    SELECT name, language, stargazers_count, 'public' as visibility
    FROM read_parquet('/path/to/warehouse/github_repos/github_repos/data/*.parquet')
),
private_repos AS (
    SELECT name, language, stargazers_count, 'private' as visibility
    FROM github.github_private_repos
)
SELECT language,
    COUNT(*) as total_repos,
    SUM(CASE WHEN visibility='public' THEN 1 ELSE 0 END) as public_count,
    SUM(CASE WHEN visibility='private' THEN 1 ELSE 0 END) as private_count,
    SUM(stargazers_count) as total_stars
FROM (
    SELECT * FROM public_repos UNION ALL SELECT * FROM private_repos
)
GROUP BY language
ORDER BY total_repos DESC
```

Results:
| language | total_repos | public_count | private_count | total_stars |
|----------|-------------|--------------|---------------|-------------|
| Java     | 79          | 79           | 0             | 69934       |
| (null)   | 11          | 7            | 4             | 527         |
| Python   | 5           | 0            | 5             | 0           |
| C        | 4           | 4            | 0             | 4557        |
| C++      | 2           | 2            | 0             | 78          |

## Model Persistence Path (Phase 3)

**Current state:** Model persistence is possible via `COPY TO` workaround, local-only:
```sql
-- Must include LIMIT to prevent auto-wrapping
COPY (SELECT language, COUNT(*) as total_repos 
      FROM github.github_private_repos 
      GROUP BY language LIMIT 500)
TO 'warehouse/github/repo_summary/data/part-001.parquet' (FORMAT PARQUET)
```
This writes a valid Parquet file locally. DOES NOT upload to GCS when catalog=gcp.
The resulting "table" is NOT discoverable via `list_warehouse_tables` or queryable via
namespace.table syntax in GCP mode.

**Recommended dbt-replacement pattern (current):** Python connector pipeline that reads
from warehouse with DuckDB and returns transformed rows. pvc then writes via normal
pipeline mechanism including GCS upload. Cumbersome but correct.

**Proposed fix:** `materialize_model` MCP tool (see F-020).

## Proposed Fixes

1. F-018: `list_warehouse_tables` should show BOTH GCS and local tables when catalog=gcp,
   or accept a `catalog` parameter to specify which to list. At minimum, warn the user
   that local tables are not shown.

2. F-019: Add a DDL/write detection step in `query_warehouse`: if the SQL starts with
   COPY, CREATE, INSERT, or DROP — do NOT wrap in SELECT ... LIMIT. Optionally warn
   that write-back is to local disk only.

3. F-020: Add `materialize_model(sql, namespace, table)` MCP tool that:
   - Runs the SQL, gets results as Arrow table
   - Writes to `warehouse/<namespace>/<table>/data/part-001.parquet` (local)
   - If catalog=gcp, uploads the Parquet to GCS bucket at `<namespace>/<table>/data/`
   - Returns row count and destination path

4. F-021: In `query_warehouse` (GCP mode), after regex substitution fails to match a
   namespace.table pattern, check if the table exists in the LOCAL warehouse and emit
   a helpful error: "Table 'X.Y' exists in the local warehouse but not in the GCS
   catalog. Query it with read_parquet('/path/to/data/*.parquet') or switch to catalog: local."
