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

**Known violations currently in the code (tracked in TODO.md):**
- Country unmapped → silently defaults to `"GB"` (`loaders/customer.py`)
- Subsidiary unmapped → silently defaults to `"12"` (`loaders/subscription.py`, `loaders/one_off.py`)
- Currency unmapped → silently defaults to `"1"` (`loaders/subscription.py`, `loaders/one_off.py`)
- Blank quantity → silently defaults to `1` (`loaders/one_off.py`)

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

## Usage

```bash
# Dry run — build payloads without calling API
python main.py --dry-run

# Load all entities in order (Customer → Billing → Subscription → One-off)
python main.py

# Load only one entity type
python main.py --entity customer
python main.py --entity billingAccount
python main.py --entity subscription
python main.py --entity oneOff

# View state report
python main.py --report
python main.py --report --failures

# Skip auth check (if you know it works)
python main.py --skip-preflight
```

### Re-runs

The loader is **idempotent**. On re-run:
- Records with `status=success` are skipped automatically
- Failed records are retried
- NetSuite externalId prevents duplicate creation even if state DB is lost

---

## Load Order & Dependencies

```
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
- Customer loader: 68 records, standard fields mapped (externalId, companyName, subsidiary, currency, email, phone, terms, addressBook)
- Billing account loader: 100 records, resolves customer NS ID from state tracker
- Subscription loader: groups 70 CSV rows into 49 unique subscriptions with nested lines; resolves customer + billing account references
- One-off loader: 26 records, resolves customer by name
- CLI orchestrator: `--entity`, `--dry-run`, `--limit`, `--report`, `--failures`, `--skip-preflight`
- Idempotent state tracking via SQLite (`state/load_state.db`)
- Structured logging: `logs/YYYY-MM-DD/load_HH-MM-SS.log` (GMT+3), full tracebacks captured to file and terminal
- 3-tier ID resolution (Location header → GET by externalId → SuiteQL fallback)
