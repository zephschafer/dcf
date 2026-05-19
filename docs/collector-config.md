# Pipeline Config Reference

A pipeline config supports the construction of a data request pattern. From it, dcf assembles the actual API calls, executes them at the cadence you define, projects the results through a schema, and writes them to the warehouse.

### Example

<table>
<tr>
<td valign="top" width="46%">

**config** (`pipelines/github_commits.yml`)

```yaml
source:
  type: http
  url: https://api.github.com/repos/
       my-org/my-repo/commits
  params:
    - name: per_page
      value: 100
  schema:
    columns:
      - name: sha
        path: sha
        type: string
      - name: message
        path: commit.message
        type: string

cadence:
  strategy: incremental
  primary_key: sha
  iterate:
    - type: date_range
      params: [since, until]
      format: "%Y-%m-%dT%H:%M:%SZ"
      start: "2024-01-01"
      end: today
      step: 7 days
```

</td>
<td valign="top" width="54%">

**assembled request** _(once per 7-day window)_

```
GET https://api.github.com/repos/my-org/my-repo/commits
    ?since=2024-01-01T00:00:00Z
    &until=2024-01-07T00:00:00Z
    &per_page=100
```

**response**

```json
[
  {"sha": "a1b2c3", "commit": {"message": "fix: null check"}},
  {"sha": "d4e5f6", "commit": {"message": "feat: add retry"}}
]
```

**projected → warehouse** (`incremental` on `sha`)

```
sha       message
──────── ─────────────────
a1b2c3   fix: null check
d4e5f6   feat: add retry
```

**cadence** — 124 windows, Jan 2024 → today

```
Jan 2024 [·  ·  ·  ·  ·  ·  ·  ·  ·  ·] May 2026
  req 1   req 2   req 3   ···   req 124
```

</td>
</tr>
</table>

A pipeline config has three primary sections:

- **`source`** — where to fetch data from (HTTP API, Python function, or Pub/Sub), including the `schema` that defines which fields to extract
- **`cadence`** — how many requests to make and how to store the results (iteration axes + write strategy)
- **`deployment`** — when to run (cron schedule for batch, or streaming via Pub/Sub)

---

## Top-level fields

| Field         | Type   | Description                                                                 |
|---------------|--------|-----------------------------------------------------------------------------|
| `name`        | string | Pipeline identifier. Becomes the warehouse table name.                      |
| `namespace`   | string | Groups related pipelines. Maps to a warehouse schema/folder.                |
| `description` | string | Human-readable summary. Optional but recommended.                           |

---

## `source` — where data comes from

The `source` block declares how dcf fetches raw records. Three types are supported.

### `http`

Makes HTTP requests to a REST or CSV API.

```yaml
source:
  type: http
  url: https://api.example.com/records
  method: GET                      # GET or POST; default GET
  auth:
    type: bearer                   # bearer | header | query_param
    value: "{{ env.API_TOKEN }}"
  params:
    - name: per_page
      type: integer
      value: 100
  response:
    format: json                   # json or csv
    records_path: data.items       # dot-path to the records array; omit for top-level array
  rate_limit:
    requests: 500
    per_minutes: 15
  schema:
    columns:
      - name: id
        path: id
        type: integer
      - name: created_at
        path: created_at
        type: timestamp
```

**Auth types:**

| Type          | Fields              | Description                              |
|---------------|---------------------|------------------------------------------|
| `bearer`      | `value`             | Sends `Authorization: Bearer <value>`    |
| `header`      | `key`, `value`      | Sends an arbitrary header                |
| `query_param` | `key`, `value`      | Appends `?key=value` to the URL          |

**Param types:** `string`, `integer`, `float`, `date`, `boolean`. Params without a `value` are injected by a `cadence.iterate` axis.

### `python`

Calls a Python function for sources that need custom pagination or client logic.

```yaml
source:
  type: python
  module: connectors.my_api        # importable module path
  function: fetch_records          # function name
  params:
    - name: org
      type: string
      value: my-org
  schema:
    columns:
      - name: id
        path: id
        type: string
```

The function must have the signature:

```python
def fetch_records(dynamic_params: dict) -> list[dict]:
    ...
```

`dynamic_params` is the resolved param dict for the current iteration step.

### `pubsub`

Subscribes to a Google Cloud Pub/Sub topic for streaming ingestion.

```yaml
source:
  type: pubsub
  subscription: projects/my-project/subscriptions/my-sub
  schema:
    columns:
      - name: id
        path: id
        type: string
```

Pub/Sub sources require `deployment.type: streaming` and `cadence.strategy: append`. See [deployment](#deployment--scheduling).

### Environment variable resolution

Any string value in the config can reference an environment variable using `{{ env.VAR_NAME }}`. dcf resolves these at load time:

1. Checks `os.environ` for `VAR_NAME`
2. Falls back to the matching key in `.env` (lowercased, e.g. `var_name`)
3. Raises an error if neither is found

```yaml
auth:
  type: query_param
  key: api_key
  value: "{{ env.PORTLANDMAPS_API_KEY }}"
```

### `schema` — what columns to extract

The `schema` sub-field of `source` declares the output columns. Columns not listed here are dropped.

```yaml
source:
  type: http
  url: https://api.example.com/records
  ...
  schema:
    columns:
      - name: id
        path: id              # dot-notation path into the raw record
        type: integer
      - name: owner
        path: owner.login     # nested path
        type: string
      - name: created_at
        path: created_at
        type: timestamp
```

**Column fields:**

| Field       | Required | Description                                                   |
|-------------|----------|---------------------------------------------------------------|
| `name`      | yes      | Output column name                                            |
| `path`      | yes*     | Dot-notation path to value in raw record                      |
| `type`      | no       | Cast target (see below)                                       |
| `transform` | yes*     | Transform block (mutually exclusive with `path`)              |

*Each column must have either `path` or `transform`.

**Types:** `string`, `integer`, `float`, `boolean`, `date`, `timestamp`. Values that can't be cast become `null`.

#### Transforms

Use a `transform` block instead of `path` for derived columns.

**`array_join`** — flatten an array field to a delimited string:

```yaml
- name: labels
  transform:
    type: array_join
    path: labels           # dot-path to the array in the raw record
    separator: ","         # default ","
```

**`crs_reproject`** — reproject a coordinate pair between coordinate reference systems:

```yaml
- name: lon
  transform:
    type: crs_reproject
    from_columns: [X_WEB_MERCATOR, Y_WEB_MERCATOR]  # [x, y] column names in raw record
    from_crs: EPSG:3857
    to_crs: EPSG:4326
    component: x           # extract x or y from reprojected result
```

---

## `cadence` — iteration and write strategy

The `cadence` block describes the complete rhythm of the pipeline: how many requests to make (via `iterate`) and how to persist the results (via `strategy`).

```yaml
cadence:
  strategy: incremental   # incremental | append | full_refresh
  primary_key: id         # required for incremental
  iterate:
    - type: date_range
      params: [since, until]
      start: "2024-01-01"
      end: today
      step: 7 days
```

### Write strategy

| Strategy       | Behavior                                                       |
|----------------|----------------------------------------------------------------|
| `incremental`  | Upsert on `primary_key`. Idempotent — reruns are safe.        |
| `append`       | Append all rows. No deduplication.                             |
| `full_refresh` | Delete everything, then write the new result.                  |

`incremental` requires `primary_key`. The other strategies do not.

dcf automatically adds a `dcf_updated_at` column (ISO timestamp, Pacific time) to every write.

### `iterate` — splitting one pipeline into many requests

The `iterate` sub-key lists one or more axes. Multiple axes produce a Cartesian product — every combination is requested. Without `iterate`, dcf makes exactly one request.

#### `date_range`

Slides a time window across a date range, populating one or two params per step.

```yaml
cadence:
  strategy: incremental
  primary_key: id
  iterate:
    - type: date_range
      params: [since, until]          # one param: receives window start; two params: start and end
      format: "%Y-%m-%dT%H:%M:%SZ"   # strftime format for serialization; defaults to ISO date
      start: "2023-01-01"             # ISO date or "today"
      end: today
      step: 7 days                    # how far to advance each iteration
      window: 7 days                  # size of each window; defaults to step
```

Supported duration formats: `N day(s)`, `N week(s)`, `N month(s)` (one month = 30 days).

#### `categorical`

Iterates over a fixed list of values for a single param.

```yaml
cadence:
  strategy: append
  iterate:
    - type: categorical
      param: state
      values: [open, closed, merged]
```

#### Combining axes

Axes are combined as a Cartesian product. The example below produces one request per (week × state) combination:

```yaml
cadence:
  strategy: incremental
  primary_key: id
  iterate:
    - type: date_range
      params: [since, until]
      start: "2024-01-01"
      end: today
      step: 7 days
    - type: categorical
      param: state
      values: [open, closed]
```

### Staging and merge (advanced)

For pipelines that write to multiple staging tables before merging into a final table, `cadence` supports optional `staging` and `merge` blocks. See the examples in `testing/` for usage.

---

## `deployment` — scheduling

```yaml
deployment:
  type: batch          # batch (default) or streaming
  schedule: "0 8 * * *"
  paused: false
```

| Field      | Type    | Description                                                        |
|------------|---------|--------------------------------------------------------------------|
| `type`     | string  | `batch` (cron-driven) or `streaming` (Pub/Sub). Default `batch`.  |
| `schedule` | string  | Cron expression (5-field). Required for `batch`.                   |
| `paused`   | boolean | Set `true` to disable the schedule without deleting it. Default `false`. |

`streaming` requires `source.type: pubsub` and `cadence.strategy: append`.

Omitting the `deployment` block makes the pipeline manual-only (run with `dcf run`).

---

## Execution lifecycle

When you run a pipeline, dcf executes four phases in order.

**1. Expand**

Reads `cadence.iterate` to produce a list of param dicts — one dict per request. With no `iterate` key, this is `[{}]`.

**2. Fetch**

For each param dict, calls the source (HTTP request, Python function, or Pub/Sub pull) and returns raw records as `list[dict]`.

**3. Project**

Applies the `source.schema` to the raw records:
- Extracts values by dot-notation `path` or `transform`
- Casts each column to its declared `type`
- Drops any fields not listed in the schema
- Returns a Pandas DataFrame

**4. Write**

Persists the DataFrame using the `cadence` strategy, then appends `dcf_updated_at`.

Terminal output during a run looks like:

```
[dcf] Running 'github_commits' — 12 requests

  [1/12] since=2024-01-01 until=2024-01-07
    42 rows → writing
  [2/12] since=2024-01-08 until=2024-01-14
    38 rows → writing
  ...

[dcf] 'github_commits' complete → /your/project/warehouse/github/github_commits/data
```

---

## Worked example

A complete pipeline that fetches GitHub commits week-by-week and upserts them into the warehouse.

```yaml
name: github_commits          # → warehouse table name
namespace: github             # → warehouse schema/folder
description: Commits on the main branch, ingested weekly.

source:
  type: http
  url: https://api.github.com/repos/my-org/my-repo/commits
  method: GET
  auth:
    type: bearer
    value: "{{ env.GITHUB_TOKEN }}"   # resolved from env or .env
  params:
    - name: sha
      type: string
      value: main             # static param — same on every request
    - name: per_page
      type: integer
      value: 100
  response:
    format: json              # GitHub returns a top-level array, so no records_path needed
  rate_limit:
    requests: 60
    per_minutes: 60           # GitHub's unauthenticated rate limit is 60/hr; authenticated is 5000/hr
  schema:
    columns:
      - name: sha
        path: sha
        type: string
      - name: author
        path: commit.author.name
        type: string
      - name: email
        path: commit.author.email
        type: string
      - name: message
        path: commit.message
        type: string
      - name: committed_at
        path: commit.author.date
        type: timestamp           # cast from ISO string to timestamp

cadence:
  strategy: incremental
  primary_key: sha            # upsert on commit SHA — reruns won't create duplicates
  iterate:
    - type: date_range
      params: [since, until]          # since gets window start, until gets window end
      format: "%Y-%m-%dT%H:%M:%SZ"   # GitHub expects ISO 8601
      start: "2023-01-01"
      end: today
      step: 7 days                    # one request per week

deployment:
  type: batch
  schedule: "0 6 * * 1"      # every Monday at 6 AM UTC — catches the prior week's commits
```
