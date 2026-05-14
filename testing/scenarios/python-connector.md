# Scenario: Python Connector End-to-End (Linear GraphQL)

## Goal

Test the `type: python` connector path as the PRIMARY focus — not as a workaround
discovered mid-scenario, but as the intended path for a class of APIs that cannot
be expressed in ddt YAML.

Linear's GraphQL API is the vehicle because GraphQL definitively requires a POST
with a request body (a query string), which ddt's HTTP YAML schema cannot express.
This makes a Python connector unavoidable — there is no YAML workaround.

**The core questions this scenario answers:**
1. Does `type: python` actually work end-to-end? (import, call, yield)
2. Does the `new-pipeline` skill know when to recommend a Python connector?
3. Does the skill know how to help a user write one?
4. What is the expected function signature, yield pattern, and error contract?

If `type: python` is broken or undocumented, every Blocking YAML limitation is
permanently blocking — not just temporarily pending a connector. This scenario
determines whether the escape hatch actually works.

## Target API

Linear GraphQL API

```
POST https://api.linear.app/graphql
Content-Type: application/json
Authorization: Bearer <LINEAR_API_KEY>

{
  "query": "{ issues(first: 50, after: \"<cursor>\") { nodes { id title state { name } priority assignee { name } createdAt updatedAt } pageInfo { hasNextPage endCursor } } }"
}
```

Linear API docs: https://developers.linear.app/docs/graphql/working-with-the-graphql-api

Response shape:
```json
{
  "data": {
    "issues": {
      "nodes": [
        {
          "id": "abc-123",
          "title": "Fix authentication bug",
          "state": { "name": "In Progress" },
          "priority": 2,
          "assignee": { "name": "Alice" },
          "createdAt": "2024-01-01T00:00:00.000Z",
          "updatedAt": "2024-01-15T00:00:00.000Z"
        }
      ],
      "pageInfo": {
        "hasNextPage": true,
        "endCursor": "eyJza2lwIjoxMDB9"
      }
    }
  }
}
```

Pagination: cursor-based (`after` GraphQL argument + `pageInfo.endCursor`)
Auth: Bearer token (`LINEAR_API_KEY`)

## Test Phases

### Phase 1 — Read the Connector Documentation

Before writing any code, read ddt's existing documentation on Python connectors:

1. Read `README.md` — does it document the `type: python` source? What is the
   expected connector function signature? What parameters does ddt pass?
2. Read `.claude/commands/new-pipeline.md` — does the skill mention Python connectors?
   Does it explain when to use one or how to write one?
3. Look at `ddt/engine/fetcher.py` — read the code for `type: python` source handling.
   What function name does ddt look for? What arguments does it call it with?
   What return type does it expect (list of dicts? generator? iterator?)?
4. Look at `connectors/` in the quipu test project — are there any existing connector
   examples?

Phase 1 success: document the exact expected connector interface (function name,
signature, return type, parameter passing mechanism). Note any gaps in the docs.

### Phase 2 — Write the Linear Connector

Write `connectors/linear_issues.py` using the interface discovered in Phase 1:

1. Implement cursor-based pagination over Linear issues GraphQL query
2. Extract: id, title, state_name (from state.name), priority (integer, 0-4),
   assignee_name (from assignee.name, nullable), created_at, updated_at
3. Write the corresponding `pipelines/linear_issues.yml` with `type: python`
4. Run `ddt validate linear_issues`
5. Run `ddt run linear_issues --limit 1`
6. If it fails: diagnose carefully — is it a connector import error? A wrong function
   signature? A wrong return type? These are all separate findings.

Phase 2 success: connector imports and runs, returns rows from Linear.

### Phase 3 — Full Run and Quality Check

1. Run full pipeline — all issues across all pages
2. Verify: `state_name` is a string ("In Progress", "Done", etc.)
3. Verify: `priority` is an integer (0=No priority, 1=Urgent, 2=High, 3=Medium, 4=Low)
4. Verify: `assignee_name` is nullable — unassigned issues have null assignee
5. Incremental deduplication on `id` — stable across re-runs
6. Query warehouse: count issues by state, count by assignee

Phase 3 success: full pipeline produces correct, deduplicated data.

## Success Criteria

- [ ] Phase 1: Connector interface documented — function name, signature, return type
- [ ] Phase 1: README / skill gap documented — does docs cover Python connectors?
- [ ] Phase 2: Connector imports without error
- [ ] Phase 2: `ddt run linear_issues --limit 1` returns at least 1 row
- [ ] Phase 2: `state.name` nested field extracted (via connector-level logic)
- [ ] Phase 2: `assignee.name` correctly nullable
- [ ] Phase 3: Full run completes with all pages fetched
- [ ] Phase 3: Row count stable across re-runs (deduplication on string `id`)
- [ ] Phase 3: Warehouse queryable — `SELECT state_name, COUNT(*) FROM linear.linear_issues GROUP BY state_name ORDER BY count DESC`

## Known Complexity

- **GraphQL POST body:** Cannot be expressed in ddt YAML at all. Python connector
  is the only path.
- **Cursor pagination:** `pageInfo.endCursor` from the GraphQL response must be
  passed as the `after` argument in the next query. Stateful — requires a loop in
  the connector.
- **Connector interface unknown:** The exact function name, parameter passing mechanism,
  and return type that ddt expects for `type: python` connectors is not documented
  in user-facing docs (as of last review). Phase 1 must discover this from source code.
- **String ID primary key:** Linear IDs are strings (UUIDs). Confirm incremental
  upsert works with string primary keys (tested in github-commits for sha, but worth
  confirming again).

## Known Expected Findings (Pre-identified)

- **Expected Skill gap (Skill/UX):** The `new-pipeline` skill almost certainly does not
  explain when to use a Python connector vs. HTTP YAML, nor does it explain how to write
  one. A first-time user would have no idea the option exists.
- **Expected UX gap:** The connector function interface (name, signature, return type)
  is not documented anywhere visible to users. Reading source code is required to
  understand how to write a connector.
- **To investigate:** Is the connector interface stable enough to document? Or does it
  need redesign before documenting? (E.g., does ddt pass the pipeline params to the
  connector function? Does it pass auth credentials? Or must the connector manage its
  own auth entirely?)
- **Potentially Blocking:** If `type: python` has a runtime bug (import path wrong,
  wrong function call convention, exception handling missing), this is Critical — it
  means all Blocking YAML limitations are permanently Blocking.

## Credentials Required

`LINEAR_API_KEY` — Linear personal API key.

Generate at: https://linear.app/settings/api (Personal API keys section)

Store as `LINEAR_API_KEY` env var or `linear_api_key` in `project.yml`.

Zeph provides this credential.

## By Design Decisions from Prior Runs

(None yet — first run of this scenario)

## Notes for Agent

- The test project is `/Users/zephschafer/Documents/GitHub/quipu/`
- Use `namespace: linear` — routes to `warehouse/linear/linear_issues/`
- Phase 1 is the most important phase — if the connector interface is undocumented,
  that itself is the primary finding, regardless of whether Linear data ingests
- For Phase 1, read `ddt/engine/fetcher.py` directly to find the `type: python`
  handling code. Look for: how it imports the connector module, what function it calls,
  what arguments it passes, how it collects the return value.
- The connector file must be at `connectors/linear_issues.py` relative to the
  project root (quipu). The function name ddt looks for must match exactly.
- If the connector interface requires the connector to manage pagination AND auth,
  note this in the findings — it means ddt's auth model doesn't carry over to
  Python connectors (a user must re-implement auth in each connector).
- Linear issues may be relatively few (hundreds, not thousands) — use all pages
  for the full run without a date range filter.
