# NetSuite Data Loader — MoorePay HubSpot Migration

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

## ⚠ TODOs Before Production Run

### 1. Terms Internal ID
The customer payload uses `{"refName": "Z030 - Payment w/in 30 days net"}` for terms.
If this doesn't resolve, run this SuiteQL to find the ID:
```sql
SELECT id, name FROM term WHERE name LIKE '%Z030%'
```
Then hardcode in `loaders/customer.py`.

### 2. Custom Fields (Customer)
These MoorePay-specific fields need their `custentity_xxx` script IDs:
- Company Reg Number, Segment, Direct Debit, Business/Class
- Dunning Procedure, Dunning Contact, Dunning Level
- Email Preference, Allow Letters to be Emailed
- Electronic Email Recipients, Indexation Date, PO Mandatory

Run: `SELECT scriptid, label FROM customfield WHERE fieldtype = 'ENTITY'`

### 3. Subscription Plan Internal IDs
Subscription plan references use `{"refName": "..."}`.
If this doesn't work, look up IDs:
```sql
SELECT id, name FROM subscriptionplan
```

### 4. Sales Item Internal IDs
Same for subscription line items and one-off invoice items.
```sql
SELECT id, itemid, displayname FROM item WHERE itemid LIKE '%Next Gen%'
```

### 5. One-Off Invoice Record Type
Currently set to `invoice`. Verify this is correct — might need to be
`customSale`, `cashSale`, or a custom record type.

### 6. Subscription API Schema
Request the NetSuite REST schema for the `subscription` record type to verify
field names (especially line items, priceBook, subscriptionPlan).
