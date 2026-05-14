You are running the ddt testing process for a specific scenario. Your job is to act as a first-time ddt user attempting to build a real pipeline, while also acting as a disciplined QA engineer who records every failure, friction point, and gap precisely.

## What You Are Testing

ddt is a YAML-driven data ingestion framework and Claude plugin (CLI + skills + MCP server). The core promise is that a user can say "build me a pipeline for X" and Claude can successfully orchestrate a working pipeline end-to-end. You are testing whether that promise is true for the given scenario.

## Arguments

The scenario name is passed as an argument (e.g., `/test-pipeline github-repos`). If no argument is given, ask the user which scenario to run.

---

## Step 0: Set up the clean test environment

Before reading the scenario, create an isolated project directory for this run. Every test starts from a fresh clone of quipu so that warehouse data, pipelines, and config from previous runs do not carry over.

**a. Check for test_config.yml**

Look for `testing/test_config.yml` in the ddt repository. If it does not exist, stop and tell the user:

> `testing/test_config.yml` is missing. Copy `testing/test_config.yml.example` to `testing/test_config.yml` and fill in the credentials needed for this scenario. The file is gitignored and will not be committed.

Do not proceed until the user confirms the file exists.

**b. Determine the run directory**

```
testing/runs/YYYY-MM-DD-<scenario-name>/
```

Use today's date. If a `quipu/` subdirectory already exists in that run directory (from a prior attempt), remove it before cloning:

```bash
rm -rf testing/runs/YYYY-MM-DD-<scenario>/quipu
```

**c. Clone quipu**

```bash
git clone https://github.com/zephschafer/quipu.git \
  testing/runs/YYYY-MM-DD-<scenario>/quipu
```

The clone will have no `project.yml` (quipu gitignores it) and no warehouse data — a clean slate.

**d. Inject credentials**

Copy `testing/test_config.yml` into the clone as `project.yml`:

```bash
cp testing/test_config.yml \
  testing/runs/YYYY-MM-DD-<scenario>/quipu/project.yml
```

**e. Record the clone path**

Set `CLONE` to the absolute path of the cloned quipu (e.g. `/Users/zephschafer/Documents/GitHub/ddt/testing/runs/2026-05-10-github-repos/quipu`). All subsequent ddt commands target this path via the `DDT_PROJECT_DIR` environment variable.

Shorthand for all CLI commands from this point forward:

```bash
DDT_PROJECT_DIR=$CLONE uv --directory /Users/zephschafer/Documents/GitHub/ddt run ddt <command>
```

---

## Step 1: Read the Scenario

Read the scenario file at `testing/scenarios/<scenario-name>.md`. This defines:
- The target API and goal
- Success criteria (checklist)
- Known complexity
- Credentials required

Also read:
- `README.md` — the full ddt YAML schema reference
- `.claude/commands/new-pipeline.md` — the existing skill you will simulate using
- `testing/FINDINGS.md` — existing findings (so you don't re-report known issues)
- Any prior runs for this scenario in `testing/runs/` (to build on prior work)

---

## Step 2: Probe the API

Before writing any pipeline, investigate the target API as a real user would:

- Fetch the API documentation (web search or provided URL)
- Make real HTTP requests to representative endpoints to see actual response shapes
- Record: response structure, pagination mechanism, auth mechanism, array/nested fields, date field formats, rate limits
- Note anything that seems hard to express in ddt's current YAML schema

Do NOT skip this step. Understanding the real API response is essential to accurate testing.

---

## Step 3: Attempt Pipeline Creation (Simulating the new-pipeline Skill)

Proceed through the `new-pipeline` skill steps as if you are a first-time user who just installed ddt:

1. Choose source type: `http` or `python`
2. Design the pipeline YAML (iterate axes, auth, params, schema, build strategy)
3. If Python connector needed: design and write it
4. Write the pipeline YAML
5. Validate

**Write files directly to the clone** — do not use MCP write tools (they target the live quipu):

- Connector: write to `$CLONE/connectors/<name>.py`
- Pipeline YAML: write to `$CLONE/pipelines/<name>.yml`

**Validate using the CLI:**

```bash
DDT_PROJECT_DIR=$CLONE uv --directory /Users/zephschafer/Documents/GitHub/ddt run ddt validate <name>
```

**Critical constraint:** Do NOT work around YAML schema limitations by writing custom Python. If the schema cannot express what the API needs, record it as a finding and note the limitation. The test is whether ddt's YAML is expressive enough — not whether Python can compensate.

---

## Step 4: Iterative Testing

Run and iterate until either success or a blocking finding.

**Run with limit:**

```bash
DDT_PROJECT_DIR=$CLONE uv --directory /Users/zephschafer/Documents/GitHub/ddt run ddt run <name> --limit 1
```

If it fails: diagnose the error. Distinguish between:
- **User error** (wrong path, wrong param name) → fix and retry
- **ddt bug or schema gap** → record finding, attempt workaround if possible, continue

**When `--limit 1` succeeds:**

- Verify schema projection: check that all expected columns are present and typed correctly
- Run full pipeline (or a reasonable subset via `--limit`):

```bash
DDT_PROJECT_DIR=$CLONE uv --directory /Users/zephschafer/Documents/GitHub/ddt run ddt run <name>
```

**Query the clone's warehouse** to verify row counts and spot-check data quality. The warehouse lives at `$CLONE/warehouse/`. Query it directly:

```bash
DDT_PROJECT_DIR=$CLONE uv --directory /Users/zephschafer/Documents/GitHub/ddt run python -c \
  "from ddt.warehouse_reader import query; import json; print(query('SELECT * FROM <namespace>.<table> LIMIT 10'))"
```

Or read the Parquet files directly with DuckDB if simpler.

**Mark success criteria checkboxes** if the full run succeeds.

---

## Step 5: Document Findings

### Create the run directory and report

The run directory (`testing/runs/YYYY-MM-DD-<scenario-name>/`) was created in Step 0. Add to it:

**`report.md`** — structured findings report:

```markdown
# Test Run: <Scenario Name>
Date: YYYY-MM-DD | Tester: Claude <model> | Scenario: <scenario-name>

## Outcome: SUCCESS | PARTIAL SUCCESS | FAILURE

## Success Criteria
- [x] Pipeline YAML validates successfully
- [x] --limit 1 run fetches at least 1 real row
- [ ] Full run deduplicates correctly on primary key
... (copy from scenario file, check off what passed)

## What Worked
- <item>: ✓
...

## What Failed
- <description of failure>
  [→ Finding F-XXX: Severity / Category]

## Friction Points (things that were confusing or took extra steps)
- <description>
  [→ Finding F-XXX: Minor / UX]

## Pipeline Produced
See pipeline.yml in this directory. (copy the final YAML here if it worked)

## Proposed Fixes
1. F-XXX: <concise fix proposal>
...
```

**`pipeline.yml`** — copy from `$CLONE/pipelines/<name>.yml` (even if it only partially worked)

**`connector.py`** — copy from `$CLONE/connectors/<name>.py` if one was written

### Update the central tracker

Update `testing/FINDINGS.md`:
- Add new findings to the Open Findings table with the next available F-XXX ID
- Use the severity and category definitions at the top of FINDINGS.md
- Update the header stats (Total, Open counts)
- Do NOT duplicate findings that already exist in the tracker

---

## Step 6: Summary Report

Present a concise summary to the user:

```
## Round Complete: <Scenario Name>

Outcome: <SUCCESS | PARTIAL SUCCESS | FAILURE>

New findings: N
  - F-XXX: <one-line summary> [Blocking/Major/Minor/Enhancement]
  ...

Success criteria: X/Y passed

Proposed next steps:
  1. <fix or decision needed from you>
  ...
```

Wait for the user to review and tell you which findings to fix, mark by-design, or defer.

---

## Rules for Finding Classification

**Never classify a finding as Minor if it prevents a real-world pipeline from working.** When in doubt, classify higher (more severe).

**A finding is Blocking if:** any real API of this type cannot be ingested without a ddt code change.

**A finding is a Skill finding if:** the `new-pipeline` skill gave you wrong or missing guidance that caused you to go down the wrong path.

**A finding is a UX finding if:** an error message was cryptic, a CLI flag was confusing, or you had to read source code to understand what was happening.

**Do not report findings for things that are intentionally out of scope** (e.g., ddt does not claim to support OAuth PKCE flows). Check the README and existing "By Design" entries before filing.

---

## After Findings Are Reviewed

When the user tells you to fix a finding:

1. Implement the fix in the relevant ddt source file
2. Write a pytest unit test in `tests/` that would have caught this issue (if it's a Runtime finding)
3. Re-run the scenario's `--limit 1` test to verify the fix
4. Update `testing/FINDINGS.md`: move the finding to the Fixed table with the git commit hash
5. Update `testing/runs/<run>/report.md` to note the fix

When the user marks a finding "By Design":

1. Move it to the By Design table in `testing/FINDINGS.md` with the rationale the user gave
2. Note it in the scenario file so future rounds don't re-investigate it
