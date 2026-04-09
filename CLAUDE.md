# CLAUDE.md — NetSuite Data Loader (MoorePay HubSpot → NetSuite Migration)

## Project Goal

Load customer, billing account, subscription, and one-off invoice data from Snowflake-exported CSVs into NetSuite's Sandbox via REST API. This is a HubSpot-to-NetSuite migration for MoorePay (a Zellis company). The pipeline runs: HubSpot → Snowflake (transformation) → CSV exports → this Python loader → NetSuite REST API.

---

## Architecture

```
netsuite_loader/
├── config.py              # Credentials, paths, retry settings
├── netsuite_client.py     # OAuth 1.0 TBA signing, REST calls, 3-tier ID retrieval
├── state_tracker.py       # SQLite persistence for idempotent loads + ID chaining
├── main.py                # CLI orchestrator (--entity, --dry-run, --report, --failures)
├── loaders/
│   ├── base.py            # Abstract base: CSV reading, load loop, hash, skip-if-done
│   ├── customer.py        # CSV → NetSuite customer payload
│   ├── billing_account.py # CSV → NetSuite billingAccount payload (refs customer)
│   ├── subscription.py    # CSV → NetSuite subscription payload (refs customer + billing acct)
│   └── one_off.py         # CSV → NetSuite invoice payload (refs customer)
├── data/                  # CSV files from Snowflake exports
│   ├── customerskleeneexport20260409.csv      (68 rows)
│   ├── billingkleeneexport20260409.csv        (100 rows)
│   ├── subscriptionskleeneexport20260409.csv  (70 rows → 49 unique subscriptions)
│   └── oneoffkleeneexport20260409.csv         (26 rows)
├── state/                 # SQLite DB (auto-created at runtime)
├── logs/                  # Timestamped log files (auto-created)
├── requirements.txt       # Only: requests>=2.31.0
└── README.md
```

---

## NetSuite API Details

- **Account**: `4874529-sb3` (Sandbox)
- **Base URL**: `https://4874529-sb3.suitetalk.api.netsuite.com/services/rest/record/v1`
- **SuiteQL URL**: `https://4874529-sb3.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql`
- **Auth**: OAuth 1.0 TBA with HMAC-SHA256. Requires: Consumer Key, Consumer Secret, Access Token, Token Secret, Realm.
- **Create response**: HTTP 204 No Content, no body, `Location` header contains internal ID (e.g., `…/customer/800419`).
- **Credentials**: Set via environment variables `NS_CONSUMER_KEY`, `NS_CONSUMER_SECRET`, `NS_ACCESS_TOKEN`, `NS_TOKEN_SECRET`, `NS_REALM` — or edit `config.py` directly.

---

## Load Order (Non-Negotiable Hierarchy)

```
1. Customer              → no dependencies
2. Billing Account       → references Customer (via NS internal ID)
3. Subscription (header) → references Customer + Billing Account
   └─ Subscription Lines → nested in subscription payload as sublist
4. One-Off Invoice       → references Customer
```

Each step uses the **state tracker** to look up the NetSuite internal ID of its parent entity. If the parent hasn't been loaded, the child record is skipped with a logged error.

---

## ID Resolution — 3-Tier Strategy

Applied to **every entity** after a successful POST:

| Tier | Method | Details |
|------|--------|---------|
| 1 | Parse `Location` header | `Location: .../customer/800419` → extract `800419` |
| 2 | GET by externalId | `GET /record/v1/{type}/eid:{externalId}` |
| 3 | SuiteQL query | `SELECT id FROM {type} WHERE {field} = '{value}'` |

Status values: `success` (ID resolved), `success_no_id` (2xx but ID unresolved — needs manual review), `failed` (API error).

---

## State Tracker (SQLite)

Location: `state/load_state.db`

```sql
load_state (
    entity_type   TEXT,     -- 'customer', 'billingAccount', 'subscription', 'oneOff'
    external_id   TEXT,     -- your external ID
    netsuite_id   TEXT,     -- NS internal ID (once resolved)
    status        TEXT,     -- 'pending', 'success', 'success_no_id', 'failed'
    error_message TEXT,     -- API error details
    payload_hash  TEXT,     -- SHA256 of payload for change detection
    tier_used     TEXT,     -- which tier resolved the ID
    attempted_at  TEXT,
    PRIMARY KEY (entity_type, external_id)
)
```

On re-run, records with `status=success` or `success_no_id` are automatically skipped.

---

## Current Status — What Works

- **All imports and module resolution**: tested, working from any working directory.
- **Customer loader**: builds valid payloads for all 68 records. Standard fields mapped: `externalId`, `companyName`, `isPerson`, `subsidiary`, `currency`, `email`, `phone`, `terms`, `addressBook` (with country code mapping). Sample payload validated.
- **Billing account loader**: correctly resolves customer NS ID from state tracker. Blocks if customer not yet loaded. All 100 rows parse correctly.
- **Subscription loader**: groups 70 CSV rows into 49 subscription headers with nested lines. Resolves customer via name→extId→stateTracker chain. Resolves billing account via `{deal_id}_BA` pattern. Correctly blocks when dependencies missing.
- **One-off loader**: 26 rows, resolves customer by name. Builds invoice payloads.
- **Orchestrator**: CLI with `--entity`, `--dry-run`, `--report`, `--failures`, `--skip-preflight`. Dependency warnings. Run logging.
- **Idempotency**: SQLite state + NetSuite externalId upsert semantics.

---

## TODOs — In Priority Order

### P0: Must fix before any API calls

1. **Terms internal ID resolution**
   - `loaders/customer.py` uses `{"refName": "Z030 - Payment w/in 30 days net"}` for `terms`.
   - `refName` may not work for all NS setups. Need to run:
     ```sql
     SELECT id, name FROM term WHERE name LIKE '%Z030%'
     ```
   - Then replace with `{"id": "<actual_id>"}` in the customer payload.

2. **Verify customer payload against sandbox**
   - Do a single test POST of one customer to the sandbox.
   - Check which fields are rejected. NS will return 400/422 with field-level errors.
   - Common issues: `addressBook` structure, `currency`/`subsidiary` reference format, `terms` lookup.

### P1: Required for full pipeline

3. **Custom field mapping (Customer)**
   - ~15 MoorePay-specific fields are flagged as TODOs in `loaders/customer.py`:
     - Company Reg Number, Segment ("Moorepay"), Direct Debit
     - Business/Class ("Managed Services"), Dunning Procedure, Dunning Contact First/Last Name
     - Dunning Level ("Level 1 and Above"), Email Preference ("PDF")
     - Allow Letters to be Emailed, Electronic Email Recipients
     - Indexation Date, PO Mandatory, n/a 1 (Direct Debit setup), n/a 2 (NS Account Number)
   - To find script IDs, run:
     ```sql
     SELECT scriptid, label FROM customfield WHERE fieldtype = 'ENTITY' ORDER BY label
     ```
   - Then add to the customer payload as `payload["custentity_xxx"] = value`.

4. **Subscription plan internal IDs**
   - `loaders/subscription.py` uses `{"refName": "HR Services rolling (LPG)"}` etc.
   - Need:
     ```sql
     SELECT id, name FROM subscriptionplan
     ```
   - Then build a mapping dict and use `{"id": "..."}`.

5. **Sales item internal IDs**
   - Both subscription lines and one-off invoices reference sales items by `refName`.
   - Need:
     ```sql
     SELECT id, itemid, displayname FROM item WHERE isinactive = 'F'
     ```

6. **Subscription REST API schema verification**
   - We don't have the full NS REST schema for the `subscription` record type.
   - Need to verify: field names for `subscriptionPlan`, `priceBook`, `subscriptionLine` sublist, line item structure.
   - User can paste from: `https://system.netsuite.com/help/helpcenter/en_US/APIs/REST_API_Browser/record/v1/2024.2/index.html`
   - Or do: `GET /record/v1/metadata-catalog/subscription` to get the schema programmatically.

7. **One-off invoice record type confirmation**
   - Currently set to `invoice` in `loaders/one_off.py` (`RECORD_TYPE = "invoice"`).
   - May need to be `customSale`, `cashSale`, or a custom record. Verify with the MoorePay NS team.

### P2: Nice to have / hardening

8. **Billing account ↔ subscription linkage gap**
   - Only 14 of 49 subscriptions have a matching billing account in the billing CSV.
   - Root cause: billing DDL date range ends `2026-02-13`, subscription DDL ends `2026-02-28`.
   - The subscription loader handles this gracefully (creates without billing account ref), but the linkage may need to be established later.

9. **Data quality — country field**
   - Customer CSV has "Hampshire" and "Luton" as country values (should be "United Kingdom").
   - Currently mapped to "GB" in `COUNTRY_MAP` with a fallback. Logged as warnings.

10. **Contact subrecords**
    - The customer CSV has contact fields (Contact First Name, Contact Last Name, Job Title) that map to the primary contact.
    - Currently only `title` (Job Title) is set on the customer record. Full contact creation as a separate `contact` record (linked to customer) is not implemented.
    - NS may require contacts as a subcollection: `contactRoles` on the customer record.

11. **Rate limiting / throughput tuning**
    - `config.REQUEST_DELAY_SECONDS = 0.5` is conservative. Can be reduced after testing.
    - NS sandbox rate limits are typically generous.

12. **Retry only failed records**
    - The `--retry-failed` CLI flag is mentioned in the docstring but not implemented.
    - Current behavior: re-run skips `success` records, retries everything else.

---

## Data Context

### CSV Column → NetSuite Field Mappings (Customers)

| CSV Column | NS Field | Status |
|---|---|---|
| External ID 2 | `externalId` | ✅ Mapped |
| Company Name | `companyName` | ✅ Mapped |
| Primary Entity (Req) | `subsidiary.id` | ✅ Mapped (12=Moorepay Ltd, 66=Moorepay Ireland) |
| Currency | `currency.id` | ✅ Mapped (GBP=1, EUR=4) |
| Email | `email` | ✅ Mapped |
| Phone | `phone` | ✅ Mapped |
| Terms | `terms.refName` | ⚠️ Needs ID verification |
| Address fields | `addressBook.items[]` | ✅ Mapped with country code resolution |
| Job Title | `title` | ✅ Mapped |
| Company Reg Number | `custentity_???` | ❌ Custom field ID needed |
| Segment | `custentity_???` | ❌ Custom field ID needed |
| Direct Debit | `custentity_???` | ❌ Custom field ID needed |
| Business/Class | `custentity_???` | ❌ Custom field ID needed |
| Dunning Procedure | `custentity_???` | ❌ Custom field ID needed |
| Dunning Contact First/Last | `custentity_???` | ❌ Custom field ID needed |
| Dunning Level (Req) | `custentity_???` | ❌ Custom field ID needed |
| Email Preference | `custentity_???` | ❌ Custom field ID needed |
| Allow Letters to be Emailed | `custentity_???` | ❌ Custom field ID needed |
| Electronic Email Recipients | `custentity_???` | ❌ Custom field ID needed |
| Indexation Date | `custentity_???` | ❌ Custom field ID needed |
| PO Mandatory | `custentity_???` | ❌ Custom field ID needed |

### CSV Column → NetSuite Field Mappings (Billing Account)

| CSV Column | NS Field | Status |
|---|---|---|
| externalId | `externalId` | ✅ |
| name | `name` | ✅ |
| customer_externalId | `customer.id` | ✅ Resolved via state tracker |
| subsidiary_id | `subsidiary.id` | ✅ Already NS internal ID |
| currency_id | `currency.id` | ✅ Already NS internal ID |
| billingSchedule_id | `billingSchedule.id` | ✅ Already NS internal ID |
| frequency | `frequency.id` | ✅ (e.g., "MONTHLY") |
| startDate | `startDate` | ✅ |
| requestOffCycleInvoice | `requestOffCycleInvoice` | ✅ |
| customerDefault | `customerDefault` | ✅ |
| inactive | `inactive` | ✅ |
| billAddressList_parked | `billAddressList.id` | ⚠️ Mostly null in data |
| shipAddressList_parked | `shipAddressList.id` | ⚠️ Mostly null in data |

### Subscription Grouping Logic

- CSV has 70 rows → 49 unique subscriptions (grouped by `External ID` = deal ID)
- Multi-line subscriptions: 16 groups with 2-6 lines each
- Header fields (same across rows in a group): Subscription Name, Customer, Start Date, End Date, Subscription Plan, Subsidiary, Currency, etc.
- Line fields (differ per row): Sales Item, Lines: Include
- Customer resolution chain: `Customer` (name) → customer CSV `Company Name` → `External ID 2` → state tracker → NS internal ID
- Billing account resolution: `{External ID}_BA` → state tracker → NS internal ID (only works for 14 of 49; rest created without billing account ref)

---

## Snowflake DDL Context (For Reference)

The CSVs are generated by 4 Snowflake DDLs. Key logic embedded in the transforms:

- **Customer DDL**: Pulls from HubSpot deals (closed won, contract check completed, specific onboarding statuses, close date Jan-Feb 2026), joins to companies and contacts, picks primary contact by role priority, derives billing email and implementation email.
- **Billing DDL**: Similar deal filters, fans out by NetSuite account number (companies can have multiple), joins to NS customer for billing address. Traffic cop: only subscription items (excludes one-offs via SALES_ITEM_MAPPING).
- **Subscription DDL**: Same deal filters, derives SUBSCRIPTION_PLAN from line item name via large CASE statement, derives START_DATE based on plan type (payroll plans use payroll commencement date truncated to month start; HR plans use earliest commencement date). Traffic cop: only subscription items. Rejects payroll rows where start date isn't 1st of month.
- **One-Off DDL**: Same base, but traffic cop only allows ONE-OFF items. Same start date logic. Same payroll date rejection.

---

## Testing Approach

1. `python main.py --dry-run --entity customer` — validates payloads without API calls
2. POST a single customer manually (or modify code to load just 1) — verify the payload structure works
3. Fix any field-level errors from the 400/422 response
4. Once customer works: `python main.py --entity customer` to load all 68
5. `python main.py --report` to verify all succeeded
6. Then `python main.py --entity billingAccount`, then subscription, then oneOff
7. After each step: `python main.py --report --failures` to check

---

## Key Design Decisions

- **SQLite for state** (not a file/CSV): supports concurrent reads, atomic writes, and SQL queries for reporting.
- **External IDs as idempotency keys**: NetSuite upserts by externalId, so even if the state DB is lost, re-running won't create duplicates.
- **3-tier ID retrieval**: because the POST response is 204 with no body, we must parse the Location header (Tier 1). Tiers 2 and 3 are fallbacks for edge cases (timeouts, missing headers).
- **Customer name → extId mapping for subscriptions**: because the subscription CSV doesn't carry customer external IDs directly, we resolve via company name. All 49 subscription customers match the 68 customer CSV rows.
- **No external dependencies beyond `requests`**: keeps deployment simple. OAuth 1.0 signing is implemented manually (no `requests-oauthlib`).
