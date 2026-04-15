# NetSuite Data Loader — MoorePay HubSpot Migration

---

## ⛔ Data Integrity Rule — No Silent Data Modification

**This loader must never silently alter, default, or invent values from the source CSVs.**

The CSVs are produced by Snowflake and are the authoritative source of truth. Any modification here — even well-intentioned — corrupts the audit trail and creates records in NetSuite that don't match the source system.

**Rules:**

- If a required field is missing or unmapped, the record **must fail with a logged error** — never substitute a default value
- Do not reformat, normalise, or transform field values before sending to NS
- No fallback values in lookups (e.g. `SUBSIDIARY_MAP.get(name, "12")` is wrong — drop the default)
- `.strip()` and converting empty strings to `None` are the only permitted data touches

All known violations have been fixed — every unmapped/blank required field now fails with a logged error and skips the record. See TODO.md for any outstanding items.

---

## Setup

```bash
pip install requests
```

### Credentials

Set environment variables:

```bash
export NS_CONSUMER_KEY="your_consumer_key"
export NS_CONSUMER_SECRET="your_consumer_secret"
export NS_ACCESS_TOKEN="your_access_token"
export NS_TOKEN_SECRET="your_token_secret"
export NS_REALM="4874529-sb3"
```

Or edit `config.py` directly.

### Data Files

Place CSVs in `data/`:

- `customerskleeneexport20260409.csv`
- `billingkleeneexport20260409.csv`
- `subscriptionskleeneexport20260409.csv`
- `oneoffkleeneexport20260409.csv`

---

## CLI Reference

Run in this order for a full migration:

```text
Step 1 — Inspect mappings (no credentials needed)
  python main.py --field-map

Step 2 — Dry run (validate payloads, no API calls)
  python main.py --dry-run
  python main.py --dry-run --entity customer
  python main.py --dry-run --limit 1

Step 3 — Load (live API calls, dependency order must be respected)
  python main.py --entity customer
  python main.py --entity billingAccount
  python main.py --entity subscription
  python main.py --entity oneOff
  python main.py                          # all four in order

Step 4 — Review results
  python main.py --report
  python main.py --report --failures
```

### All flags

| Flag                 | Values                                              | Description                                                                                         |
| -------------------- | --------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `--entity`           | `customer` `billingAccount` `subscription` `oneOff` | Load only this entity type. Omit to run all four in dependency order.                               |
| `--dry-run`          | —                                                   | Build and log payloads without making any API calls.                                                |
| `--limit N`          | integer                                             | Process only the first N records. Use with `--dry-run` or a live run to test a single record.       |
| `--skip-preflight`   | —                                                   | Skip the auth connectivity check at startup.                                                        |
| `--report`           | —                                                   | Print the load state summary (counts per status per entity). No loading. Also prints field mapping. |
| `--failures`         | —                                                   | Add failure details (error message, timestamp) to `--report` output. Must be used with `--report`.  |
| `--field-map`        | —                                                   | Print the CSV column → NetSuite API field mapping table for all loaders. No credentials needed.     |

### Re-runs

The loader is **idempotent**. On re-run:

- Records with `status=success` are skipped automatically
- Failed records are retried
- NetSuite externalId prevents duplicate creation even if state DB is lost

---

## Load Order & Dependencies

```text
1. Customer              (no dependencies)
2. Billing Account       ← references Customer NS internal ID
3. Subscription          ← references Customer + Billing Account NS internal IDs
4. One-Off Invoice       ← references Customer NS internal ID
```

---

## ID Resolution (3-Tier Strategy)

For every record created:

| Tier | Method                                    | When               |
| ---- | ----------------------------------------- | ------------------ |
| 1    | Parse `Location` header from 204 response | Always tried first |
| 2    | `GET /record/v1/{type}/eid:{externalId}`  | If Tier 1 fails    |
| 3    | SuiteQL query by business key             | If Tier 2 fails    |

Records created but with unresolved IDs get status `success_no_id` for manual review.

---

## State Tracking

SQLite database at `state/load_state.db`:

| Column        | Purpose                                        |
| ------------- | ---------------------------------------------- |
| entity_type   | customer, billingAccount, subscription, oneOff |
| external_id   | Your external ID (e.g. MP_HubSpot_xxx)         |
| netsuite_id   | NS internal ID (once resolved)                 |
| status        | pending / success / success_no_id / failed     |
| error_message | API error details on failure                   |

---

## Progress & Outstanding Work

See [TODO.md](TODO.md) for the full prioritised task list (P0 → P1 → P2).

### What has been completed

- All imports and module resolution fixed (`loaders/` package structure)
- Customer loader: **67/68 records loaded into NS**. 1 failed: phone number exceeds NS 32-char limit (`MP_HubSpot_6632970696`).
- Billing account loader: **62/67 records loaded into NS**. Full address resolution implemented (see below). 5 remain: 4 with `name` > 50 chars (NS hard limit), 1 blocked by unloaded customer.
- Billing CSV regenerated: original 100-row export had DDL filter mismatch (LEFT JOIN + wrong date cutoff) producing ghost rows. Regenerated with correct INNER JOIN and Feb 28 cutoff → 67 rows, all matching loaded customers.
- `billAddressList` / `shipAddressList` resolution: NS requires both on every billing account POST. Added `_load_address_maps()` to `BillingAccountLoader.__init__` — queries `customeraddressbook` once at startup via SuiteQL and builds a `customer_ns_id → addressbook_internalid` map. Key finding: the field expects a **plain string** (the `internalid` from `customeraddressbook`), not a nested `{"id": "..."}` object. Confirmed by GET-ing an existing billingAccount in NS.
- SuiteQL pagination: `suiteql_query()` now paginates via `?limit=1000&offset=N` until `hasMore=false` (30,355 address rows across 31 pages).
- 19 customers loaded without default address flags: NS silently accepted the customer records but dropped `defaultBilling`/`defaultShipping`. These were identified via address map misses and repaired directly in NS. All 19 billing accounts subsequently loaded.
- Subscription loader: groups 70 CSV rows into 49 unique subscriptions with nested lines; resolves customer + billing account references
- One-off loader: 26 records, resolves customer by name
- CLI orchestrator: `--entity`, `--dry-run`, `--limit`, `--report`, `--failures`, `--skip-preflight`, `--field-map`
- Idempotent state tracking via SQLite (`state/load_state.db`)
- Structured logging: `logs/YYYY-MM-DD/load_HH-MM-SS.log` (GMT+3), full tracebacks captured to file and terminal
- 3-tier ID resolution (Location header → GET by externalId → SuiteQL fallback)
