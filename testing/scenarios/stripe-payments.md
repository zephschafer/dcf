# Scenario: Stripe Payments (Cursor Pagination + Financial Data)

## Goal

Build a pipeline that ingests payment records from Stripe. Stripe is Fivetran's
most-used connector; if ddt can't ingest Stripe, it can't claim to replace Fivetran
for most businesses. This scenario tests cursor-based pagination (the dominant pattern
for financial APIs) and validates the Python connector as the workaround path.

**Secondary goal:** Establish whether ddt can handle Stripe's data shapes, which are
deeply nested and contain sub-objects (customer, payment_method_details, charges).

## Target API

Stripe REST API — List PaymentIntents

```
GET https://api.stripe.com/v1/payment_intents
  ?limit=100
  &starting_after=<cursor>
  &created[gte]=<unix timestamp>
  &created[lte]=<unix timestamp>
```

Stripe API docs: https://stripe.com/docs/api/payment_intents/list

Response shape:
```json
{
  "object": "list",
  "data": [
    {
      "id": "pi_...",
      "object": "payment_intent",
      "amount": 2000,
      "currency": "usd",
      "status": "succeeded",
      "customer": "cus_...",
      "description": null,
      "created": 1704067200,
      "metadata": {},
      "payment_method_types": ["card"],
      "charges": {
        "data": [
          {
            "id": "ch_...",
            "amount": 2000,
            "payment_method_details": {
              "card": { "brand": "visa", "last4": "4242" }
            }
          }
        ]
      }
    }
  ],
  "has_more": true,
  "url": "/v1/payment_intents"
}
```

Pagination: cursor-based via `starting_after` (ID of last object in current page)
and `has_more` boolean. NOT Link headers.

Auth: HTTP Basic auth — API key as username, empty string as password.
(Or Bearer: `Authorization: Bearer sk_live_...`)

Note: Stripe `created` field is a Unix timestamp (integer seconds since epoch),
not an ISO 8601 string. ddt's `timestamp` type must handle this.

## Test Phases

### Phase 1 — YAML Path (Document the Pagination Limitation)

Attempt to build a pipeline using only ddt YAML:

1. Write `pipelines/stripe_payments.yml` using `type: http`, `date_range` iterate
   axis (convert ISO dates to Unix timestamps via format string if supported)
2. Use `records_path: data` to extract from the `data` array in the response
3. Run `ddt validate stripe_payments`
4. Run `ddt run stripe_payments --limit 1` for a narrow date window
5. Record: does ddt make only one request? If `has_more: true`, does ddt ignore it?
   Record the exact row count (should be ≤100 even if more exist).
6. Document the cursor pagination limitation — `starting_after` cannot be derived
   from the previous response in ddt YAML.

Phase 1 success: limitation confirmed with exact behavior documented.

### Phase 2 — Python Connector Path

Write a Python connector at `connectors/stripe_payments.py`:

1. Use the `stripe` Python SDK or raw `requests` — whichever is simpler
2. Handle cursor pagination: loop while `has_more` is true, pass `starting_after`
   from last item's `id`
3. Accept `created_gte` and `created_lte` as Unix timestamp params from pipeline YAML
4. Schema: id, amount (cents, integer), currency, status, customer_id, created
   (Unix timestamp → timestamp type), card_brand, card_last4
5. Run `ddt run stripe_payments --limit 1`
6. Verify: `amount` is in cents (not dollars — ddt has no unit conversion)
7. Verify: `created` Unix timestamp parses correctly as a timestamp type
8. Run full pipeline, verify deduplication on `id`

Phase 2 success: Python connector with cursor pagination works end-to-end.

## Success Criteria

- [ ] Phase 1: Pipeline YAML validates successfully
- [ ] Phase 1: Cursor pagination limitation confirmed and documented
- [ ] Phase 2: Python connector successfully paginates through all results
- [ ] Phase 2: `amount` stored as integer (cents, not dollars)
- [ ] Phase 2: `created` Unix timestamp parsed as warehouse timestamp type
- [ ] Phase 2: `charges.data[0].payment_method_details.card.brand` extracted correctly
  (or documented as too deeply nested for easy extraction)
- [ ] Phase 2: Incremental deduplication on `id` stable across re-runs
- [ ] Phase 2: Warehouse queryable — `SELECT id, amount, currency, status, created FROM stripe.stripe_payments ORDER BY created DESC LIMIT 10`

## Known Complexity

- **Cursor pagination:** `starting_after` must be the `id` of the last object in the
  previous response. This is stateful — each page request depends on the previous
  response. Cannot be expressed in ddt YAML.
- **Unix timestamps:** Stripe `created` is seconds since epoch (integer), not ISO 8601.
  ddt's `timestamp` type must handle this format. Unknown if it does — may need
  connector-level conversion.
- **Deeply nested payment details:** `charges.data[0].payment_method_details.card.brand`
  requires array indexing (`[0]`), which ddt's dot-notation may not support. Likely
  needs connector-level extraction.
- **Currency normalization:** `amount` is in the smallest currency unit (cents for USD,
  yen for JPY which has no sub-unit). No normalization in ddt. Document this and note
  that downstream SQL must handle it.
- **Test mode vs. live mode:** Use `sk_test_...` key to avoid real charges. Test mode
  data may be sparse — if no payment intents exist, create a few via Stripe Dashboard
  or CLI before running the test.

## Known Expected Findings (Pre-identified)

- **Expected Blocking (Schema):** Cursor pagination cannot be expressed in ddt YAML.
  `starting_after` must be derived from the previous response, which ddt YAML has no
  mechanism to do.
- **To investigate:** Does ddt's `timestamp` type handle Unix epoch integers? If not,
  this is a Schema/Runtime finding (type casting gap).
- **To investigate:** Does ddt support array indexing in dot-notation paths (e.g.,
  `charges.data.0.payment_method_details.card.brand`)? If not, deeply nested
  array-wrapped objects require Python connector extraction.

## Credentials Required

STRIPE_SECRET_KEY — Stripe secret API key (test mode preferred: `sk_test_...`).

Zeph provides this. Store as `stripe_secret_key` in `project.yml` or as env var.

## By Design Decisions from Prior Runs

(None yet — first run of this scenario)

## Notes for Agent

- The test project is `/Users/zephschafer/Documents/GitHub/quipu/`
- Use `namespace: stripe` — routes to `warehouse/stripe/stripe_payments/`
- Stripe's Basic auth: username = API key, password = empty string.
  In ddt YAML, this maps to `type: header` with `Authorization: Basic <base64(key:)>`,
  or `type: bearer` with the key as value. Try bearer first — Stripe accepts both.
- If no PaymentIntents exist in the test account, create 2-3 test payments via
  Stripe Dashboard (Developers → PaymentIntents → Create) before running the pipeline.
- For the Unix timestamp question: test by projecting `created` with `type: timestamp`
  and checking whether the warehouse column contains a proper datetime or an integer.
- The `stripe` Python package may not be installed in the quipu project. If needed,
  add it to quipu's `pyproject.toml` and run `uv sync` before testing Phase 2.
