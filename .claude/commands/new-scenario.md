You are helping the user create a new ddt test scenario. A scenario is a structured markdown file in `testing/scenarios/` that drives a `test-pipeline` run. Your job is to produce a precise, actionable scenario that a future test agent can execute without ambiguity.

## Arguments

The scenario name or feature slug is passed as an argument (e.g. `/new-scenario gcp-data-lake` or `/new-scenario incremental-retry`). If no argument is given, ask the user what they want to test.

---

## Step 1: Read context

Read the following before doing anything else:

1. **`testing/scenarios/`** — list existing scenarios so you don't duplicate one
2. **`testing/FINDINGS.md`** — understand what has already been discovered; known issues should appear in the scenario as "Known Expected Findings"
3. **If a matching feature file exists** (`features/<slug>.md`) — read it. The feature's Acceptance Criteria become the scenario's Success Criteria, the Problem becomes the Goal, and the Design Notes inform Known Complexity.

Also read one existing scenario for format reference — `testing/scenarios/python-connector.md` is a good example of a complex scenario with test phases.

---

## Step 2: Clarify scope (ask the user)

Before writing, confirm what you still need to know. Adapt based on what context you already have from the feature file. Key questions to cover:

- **What is the single core question this scenario answers?** (e.g. "Does incremental retry work when a fetch throws a transient error?")
- **What is the target API or system component?** Existing pipeline (name it), a new API, or an internal ddt component (runner, writer, reader)?
- **Should this be one scenario or several?** A scenario is best when it has a clear happy path and 1–3 error/edge cases. Multiple distinct behaviors should be separate scenario files.
- **What credentials are required?** (Which keys from `testing/test_config.yml.example`)
- **Are there any pre-identified failure modes** that the scenario should explicitly watch for?

If the scenario is being created from a feature file, you may already know most of this — ask only what's missing.

---

## Step 3: Design the test phases

Map the feature's acceptance criteria (or the user's stated goals) into ordered test phases. Each phase should have:
- A clear objective
- 3–6 numbered steps
- A phase success condition

Guidelines:
- Phase 1 is usually setup or discovery (read docs, check prerequisites, probe the API)
- Phase 2 is the core happy path (write pipeline, run with `--limit 1`, verify data)
- Phase 3 is full run + deduplication + quality checks
- Phase 4 is teardown or cleanup (only if needed — e.g. GCP resources)

Keep phases small enough that a single phase failure is clearly diagnosable.

---

## Step 4: Write the scenario file

Write `testing/scenarios/<slug>.md` using this structure:

```markdown
# Scenario: <Name>

## Goal

[2–4 sentences: what this scenario tests and why it matters.
State the core question(s) the scenario answers explicitly.]

## Target API

[Endpoint, method, request shape, response shape (with example JSON), pagination mechanism, auth type.
Omit if the scenario tests an internal component rather than an external API.]

## Test Phases

### Phase 1 — <Name>

1. [Step]
2. [Step]
...

Phase 1 success: [one sentence]

### Phase 2 — <Name>

...

## Success Criteria

- [ ] Phase 1: [Criterion — testable, maps to a phase step or acceptance criterion]
- [ ] Phase 2: [Criterion]
...

## Known Complexity

- **[Area]:** [Why this is technically hard or unpredictable]
...

## Known Expected Findings (Pre-identified)

- **[Type]:** [What gap or bug is likely to surface, and why it's expected]
...
(Omit section if nothing is pre-identified)

## Credentials Required

[Key name, what it grants access to, where to obtain it, how to store it (env var or project.yml key)]

## By Design Decisions from Prior Runs

(None yet — first run of this scenario)

## Notes for Agent

- [Concrete instruction for the test-pipeline agent — file paths, command prefixes, things to watch for]
- [Anything that would save the agent time or prevent a wrong turn]
```

**Rules for a good scenario:**
- Success criteria must be checkboxes, numbered by phase, and testable with a specific command or observable output
- Known Complexity identifies failure modes before they happen — this is what distinguishes a well-designed scenario from a vague one
- Notes for Agent are concrete and operational: file paths, exact commands, things to read, things to avoid
- Every credential must appear in `testing/test_config.yml.example` — if a new one is needed, note it explicitly so the example file can be updated

---

## Step 5: Update the feature file (if applicable)

If a feature file exists for this scenario (`features/<slug>.md`), add the scenario to its "Related Scenarios" section:

```markdown
## Related Scenarios
- [`testing/scenarios/<slug>.md`](../testing/scenarios/<slug>.md) — [what aspect it covers]
```

Remove any `TODO:` items in Design Notes that this scenario now addresses.

---

## Step 6: Check test_config.yml.example

If the scenario requires a credential that is not already in `testing/test_config.yml.example`, note this explicitly:

> **Note:** This scenario requires `<KEY_NAME>` which is not yet in `testing/test_config.yml.example`. Add the following line under the appropriate section before running this scenario:
> ```yaml
> <key_name>: ""  # <where to get it>
> ```

Do not modify `test_config.yml.example` yourself — flag it for the user to review.

---

## Step 7: Present to the user

Tell the user:
- The scenario file path (`testing/scenarios/<slug>.md`)
- Whether the feature file was updated with a cross-link
- Whether any new credentials need to be added to `test_config.yml.example`
- What the first command would be to run this scenario: `/test-pipeline <slug>`

Ask if there is anything to revise.
