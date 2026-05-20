# Create a Collector

A collector is a YAML file that defines a data source, a schema, and a cadence. dcf handles the fetching, projecting, and writing.

Make sure you're in a dcf project. If you haven't set one up yet:

```bash
mkdir my-project && cd my-project
uvx --from dcf-core dcf init
```

See the [quickstart](../QUICKSTART.md) for a full walkthrough.

---

## 1. Create the file

Add a YAML file to your `collectors/` directory:

```
collectors/so_posts.yml
```

## 2. Write the YAML

```yaml
name: so_posts
namespace: stackoverflow

source:
  type: http
  url: https://api.stackexchange.com/2.3/posts
  params:
    - name: site
      type: string
      value: stackoverflow
  response:
    records_path: items
  schema:
    columns:
      - name: post_id
        path: post_id
        type: integer
      - name: post_type
        path: post_type
        type: string
      - name: score
        path: score
        type: integer
      - name: creation_date
        path: creation_date
        type: integer

cadence:
  strategy: incremental
  primary_key: post_id
  iterate:
    - type: date_range
      params: [fromdate, todate]
      format: "%s"
      start: "2025-01-01"
      end: today
      step: 30 days
```

### Source

`source` defines where to fetch data. For `type: http`, dcf constructs a GET request with the given params and parses the response at `records_path`. To connect to an API that requires a key or token, see [authenticated collectors](authenticated-collector.md).

### Schema

`schema.columns` declares which fields to extract from each record and what type to cast them to. Use dot notation for nested paths (e.g. `author.name`). Fields not listed are dropped. See the [collector config reference](collector-config.md) for all supported types and transforms.

### Cadence

`strategy: incremental` upserts records by `primary_key`, so re-runs are safe and idempotent. The `date_range` iterate breaks the collection into 30-day windows, injecting `fromdate` and `todate` as request params on each request. See the [collector config reference](collector-config.md) for other strategies and iteration axes.

---

## 3. Run it

Test with a single iteration first:

```bash
uv run dcf run so_posts --limit 1
```

Then run the full collection:

```bash
uv run dcf run so_posts
```

## 4. Query the results

```bash
uv run dcf query "SELECT * FROM stackoverflow.so_posts LIMIT 10"
```

---

## Next steps

- **Add authentication** — [Authenticated collectors](authenticated-collector.md)
- **All config options** — [Collector config reference](collector-config.md)
- **Share your collector** — [Contributing to dcf-hub](contributing-hub-collector.md)

---

## Other APIs to try

- **[Hacker News](https://hn.algolia.com/api/v1/search)** — stories, jobs, and comments via the Algolia search API
- **[Open-Meteo](https://open-meteo.com/en/docs)** — historical and forecast weather data, no auth required
- **[CoinGecko](https://www.coingecko.com/api/documentation)** — crypto prices and market data
- **[NASA APOD](https://api.nasa.gov/)** — astronomy picture of the day (free API key)
- **[TVMaze](https://www.tvmaze.com/api)** — TV schedules and episode data
- **[GitHub public events](https://docs.github.com/en/rest/activity/events)** — public repo activity (rate-limited; no auth needed for basics)
