You are helping the user create a new dcf collector. dcf is a YAML-driven data ingestion framework that writes to a local Apache Iceberg data lake.

Follow these steps in order. Do not skip ahead — each step informs the next.

---

## 1. Understand the data source

Ask the user (or use context already provided):
- What data do they want to ingest?
- What is the source? (REST API, website to scrape, file, etc.)
- Do they have API docs or a sample URL?

## 2. Check credentials

If the API requires authentication, handle this before writing any collector YAML:

**Does the API need a key or token?**
- Check whether the credential already exists: look for it as an environment variable (e.g. `STRIPE_SECRET_KEY`) or in `.env` as a lowercase key (e.g. `stripe_secret_key`).
- If it doesn't exist, tell the user what they need to create and where to find it. Common patterns:
  - **GitHub PAT:** github.com → Settings → Developer settings → Personal access tokens → Tokens (classic) → generate with needed scopes (e.g. `repo`, `read:org`)
  - **API key:** typically in the service's developer dashboard or settings page
  - **Bearer token:** same as API key; used in `Authorization: Bearer <token>` header

**How to store the credential:**

Option 1 — environment variable (preferred for secrets):
```bash
export MY_API_KEY=sk-xxxx
```

Option 2 — `.env` (convenient for persistent keys; ensure .env is gitignored):
```
MY_API_KEY=sk-xxxx
```

Then reference it in collector YAML as `{{ env.MY_API_KEY }}`.

**Auth type to use in the YAML:**
- `type: bearer` — for `Authorization: Bearer <token>` (GitHub, Stripe, Linear, etc.). The `key` field is optional.
- `type: header` — for custom header auth (e.g. `X-Api-Key`). Requires `key`.
- `type: query_param` — for APIs that take the key as a URL param. Requires `key`.

## 3. Probe the API before writing any code

Before designing the collector, make a real request to understand the response shape. This determines which source type to use and how the scraper needs to work.

```python
import requests
resp = requests.get("https://example.com/api/data", params={...})
print(resp.json())
```

Key questions to answer:
- Is this a **GraphQL API** (POST with a query body)? → `type: python` — `type: http` cannot send a dynamic POST body
- Does the response contain a flat array of records? → `type: http` with `records_path`
- Does it return parallel arrays (e.g. `{"time": [...], "value": [...]}`)? → `type: python` to reshape
- Does pagination require reading the response first (e.g. `next_cursor`, `pageInfo.endCursor`)? → `type: python`
- Does it require HTML parsing or multi-step auth? → `type: python`
- What fields are available and what are their names exactly?

## 4. Reference existing collectors

List what already exists with `ls collectors/` and read the most structurally similar one as a reference before writing anything.

## 5. Choose source type and design the collector

Pick the source type **before** writing any YAML. The wrong choice requires a full rewrite.

### Use `type: http` when all of these are true:
- The request is a **GET** (or a POST with a **static** body — rare)
- Auth is a header, bearer token, or query param (no pre-request needed)
- Pagination is **date-range or categorical** — dcf iterates over known values upfront
- The response is **JSON with a records array** or **CSV**

**Examples:** GitHub REST API, Portland Maps API, OpenWeatherMap, any REST endpoint that returns `{"data": [...]}`.

### Use `type: sql` when the source is a relational database:
- Supports **PostgreSQL** (direct TCP connection) and **GCP Cloud SQL** (socket via Cloud Run or `cloud-sql-proxy`)
- Declare each table you want with its `primary_key`; `columns` is optional (omit to fetch all)
- Each table writes to its own warehouse path under the collector's `namespace`
- No iteration axes — tables are always fetched in full

**Local postgres YAML:**
```yaml
source:
  type: sql
  connection:
    type: postgres
    host: localhost
    port: 5432
    database: my_db
    user: my_user
    password: "{{ env.DB_PASSWORD }}"
  tables:
    - table: orders
      primary_key: order_id
    - table: customers
      primary_key: id
      columns: [id, email, created_at]
```

**Cloud SQL YAML:**
```yaml
source:
  type: sql
  connection:
    type: cloud_sql
    instance: "my-project:us-central1:my-instance"
    database: my_db
    user: my_user
    password: "{{ env.DB_PASSWORD }}"
  tables:
    - table: orders
      primary_key: order_id
```

For `cloud_sql` locally: run `cloud-sql-proxy my-project:us-central1:my-instance` first so the socket is available at `/cloudsql/...`. On Cloud Run, the socket is mounted automatically.

---

### Use `type: python` when any of these is true:
- The API is **GraphQL** — requires a POST body with a dynamic query string; `type: http` cannot express this
- **Cursor pagination** — the next-page token comes from the response (e.g. `pageInfo.endCursor`); you must read the response to know what to request next
- **Response reshaping** — the payload isn't a flat records array (parallel arrays, nested objects that must be flattened, multi-response joins)
- **HTML scraping** — requires BeautifulSoup or similar; `type: http` only handles JSON/CSV
- **Multi-step auth** — OAuth token exchange, session cookies, or any flow requiring a pre-request before the data request

**Examples:** Linear GraphQL API, Craigslist (HTML scraping), Stripe pagination (cursor-based), any API with `{"next_cursor": "..."}` in the response.

### Quick rule of thumb

> If you had to write a `while True` pagination loop or a `requests.post(json={"query": ...})` call, use `type: python`. If a single `requests.get` with URL params is enough, use `type: http`.

---

**`type: http`** — dcf constructs the request and parses the response automatically.

**`type: python`** — write a function in `connectors/` that receives params and returns `list[dict]`. The function is responsible for the full fetch-and-return cycle for one iteration, including all pagination.

### For `type: python`: auth pattern

`PythonSource` has **no `auth` block** in the YAML — there is no `auth:` field for python connectors. Pass the API key as a static param instead, and read it from `dynamic_params` inside the connector:

**Collector YAML:**
```yaml
source:
  type: python
  module: connectors.my_connector
  function: fetch_data
  params:
    - name: api_key
      value: "{{ env.MY_API_KEY }}"
```

**Connector:**
```python
def fetch_data(dynamic_params: dict) -> list[dict]:
    api_key = dynamic_params["api_key"]  # resolved from MY_API_KEY env var
    headers = {"Authorization": f"Bearer {api_key}"}
    ...
```

`{{ env.MY_API_KEY }}` is resolved by dcf before the connector is called — the connector always receives the real value, never the placeholder string.

### For `type: python`: design the scraper function

The function signature is always:
```python
def fetch_data(dynamic_params: dict) -> list[dict]:
    ...
```

`dynamic_params` contains ALL params: both iterate values (e.g. `city=portland`) and static params from the YAML (e.g. `start_date`, `max_records`, `api_key`). The function is responsible for the full fetch-and-return cycle for one iteration.

Important: dcf passes static param values as-is from the YAML. If the YAML has `value: "today"`, the function receives the literal string `"today"` — it does not get resolved to a date. Handle this in the function:
```python
if end_date == "today":
    end_date = date.today().isoformat()
```

### Choose a build strategy

- **`incremental` + `primary_key`** — upsert by key. Each run updates existing rows and inserts new ones. Good for records that change over time (permits, weather observations). Re-running the same date range should produce the same final row count.
- **`append`** — snapshot each run. Good for listings, prices, events where you want a time series.
- **`full_refresh`** — replace the whole table on each run.

### Iteration design

Each iterate axis loops over one param. Multiple axes produce a cartesian product:
- `date_range` — iterates over time windows (only for `type: http`)
- `categorical` — iterates over a list of values (works for both types)

For `type: python` collectors that span a date range, pass `start_date` and `end_date` as **static params** (not iterate axes) and let the scraper fetch the full range in one call per iteration. This is simpler than iterating over dates.

## 6. Write the files

For `type: python` collectors, write the scraper first so you can test the fetch logic in isolation before wiring it into dcf:
1. Write `connectors/{name}.py` using the Write tool
2. Quickly verify the scraper returns sensible data for one iteration by calling it directly
3. Write `collectors/{name}.yml` using the Write tool
4. Run `dcf validate {name}` — fix any errors before proceeding

For `type: http` collectors, just write `collectors/{name}.yml` and validate.

## 7. Test with a small run

Run only the first iteration with `--limit 1` and small params to limit data volume:

```bash
dcf run my_collector --limit 1 --param max_records=5 --end 2024-01-07
```

Watch for:
- Fetch errors (auth, URL, response format)
- Schema projection errors (wrong column paths — check the exact field names from step 3)
- Write errors

## 8. Verify the data

Query the warehouse to confirm the data looks right:

```bash
dcf query "SELECT * FROM namespace.my_collector LIMIT 10"
```

Check: are column types sensible? Are values in the expected range? Is the row count what you expected from the test run?

## 9. Run fully and verify dedup

Run the full collector across all iterations:

```bash
dcf run my_collector --start 2024-01-01 --end 2024-01-07
```

Then **re-run the exact same command**. For `incremental` collectors, the row count must stay the same — this confirms upserts are working and you won't accumulate duplicates on repeated runs:

```bash
dcf query "SELECT COUNT(*) FROM namespace.my_collector"
```

If the count grows on re-run, the primary key is not matching correctly — check that the `id` or key field is constructed deterministically (same inputs always produce the same key).

## 10. (Optional) Deploy to GCP

If the user wants this collector to run on a schedule in the cloud rather than just locally, dcf supports deploying to GCP via Cloud Composer (Airflow) + Cloud Run.

**Prerequisites — run once per project:**
```bash
dcf gcp setup --project-id <gcp-project-id> --region us-central1
```
This provisions a GCS warehouse bucket and a service account. Set `catalog: gcp` in `.dcf/state.yml` (or re-run `dcf init`).

**Enable required GCP APIs:**
```bash
gcloud services enable composer.googleapis.com run.googleapis.com \
  cloudbuild.googleapis.com artifactregistry.googleapis.com
```

**Add a `deployment:` block to the collector YAML:**
```yaml
deployment:
  schedule: "0 8 * * *"   # cron expression — required
  paused: false             # optional, default false
```

**Deploy with one command:**
```bash
dcf deploy <collector-name>    # provisions Cloud Run job + Composer DAG
dcf undeploy <collector-name>  # tears down job/DAG without touching data
dcf deploy-status             # list all deployed collectors
```

Only suggest this step if the user has asked about scheduling, production deployment, or running without manual intervention.

## 11. Done

Report:
- Collector name and warehouse table location (`namespace.table`)
- Number of columns
- Row count after the full test run
- Confirmation that re-run produced the same row count (for `incremental` collectors)
