# Contributing a hub collector

dcf-hub is the curated library of collector templates that users can pull with `dcf import <name>`. This guide covers adding a new collector to the hub.

**Use this path when:** your collector targets a public REST or CSV API, needs no custom Python, and would be useful to the broader dcf community.

---

## Write your collector YAML

Create a standard dcf collector YAML. See the [collector config reference](collector-config.md) for the full field guide. A few hub-specific conventions:

- **Include `description:`** — one sentence explaining what data is collected and from where.
- **Use `{{ env.VAR }}` for credentials** — never hardcode tokens or keys. See [authenticated-collector.md](authenticated-collector.md) for the auth patterns.
- **Name with underscores** — use `lowercase_with_underscores` for the file and collector name (e.g. `national_weather_service.yml`).
- **Set a sensible default iteration range** — `start: "2024-01-01"` and `end: today` with a reasonable `step` is a good baseline. Users can override at run time.

```yaml
name: national_weather_service
namespace: nws
description: Hourly observations from NOAA's National Weather Service API.

source:
  type: http
  url: https://api.weather.gov/stations/{station}/observations
  method: GET
  params:
    - name: station
      type: string
      value: KPDX
    - name: start
      type: string
    - name: end
      type: string
  response:
    format: json
    records_path: features
  schema:
    columns:
      - name: timestamp
        path: properties.timestamp
        type: timestamp
      - name: temperature
        path: properties.temperature.value
        type: float
      - name: station_id
        path: properties.station
        type: string

cadence:
  strategy: incremental
  primary_key: timestamp
  iterate:
    - type: date_range
      params: [start, end]
      start: "2024-01-01"
      end: today
      step: 7 days

deployment:
  schedule: "0 6 * * *"
```

---

## Test it locally

Before opening a PR, verify the collector runs end-to-end in your own dcf project:

```bash
# Copy your YAML into a test project
cp national_weather_service.yml ~/my-dcf-project/collectors/

# Run it
cd ~/my-dcf-project
uv run dcf run national_weather_service

# Query the result
uv run dcf query 'SELECT * FROM nws.national_weather_service LIMIT 5'
```

---

## Open a pull request

1. Fork [github.com/zephschafer/dcf-hub](https://github.com/zephschafer/dcf-hub)
2. Add your file at `collectors/<name>.yml`
3. Open a pull request with a short description of what the collector fetches

Once merged, users can immediately run `dcf import <name>` to pull your template.
