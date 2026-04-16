# CLAUDE.md — NetSuite Data Loader (MoorePay HubSpot → NetSuite Migration)

## Project Goal

Load customer, billing account, subscription, and one-off invoice data from Snowflake-exported CSVs into NetSuite's Sandbox via REST API. This is a HubSpot-to-NetSuite migration for MoorePay (a Zellis company). The pipeline runs: HubSpot → Snowflake (transformation) → CSV exports → this Python loader → NetSuite REST API.

---

## ⛔ STRICT DATA INTEGRITY RULE — DO NOT MODIFY SOURCE DATA

**This loader must never silently alter, default, or invent data values.**

The CSVs are the authoritative source of truth, produced by Snowflake transformations. Any modification here — even well-intentioned — corrupts the audit trail and creates records in NetSuite that don't match the source.

### What this means in practice

- **No silent defaults.** If a required field (subsidiary, currency, country, etc.) is missing or unmapped, the record must **fail with a clear error** — never substitute a guessed value.
- **No data coercion.** Do not reformat, normalise, or transform field values (e.g. phone numbers, dates, country names) before sending. Send exactly what the CSV contains.
- **No fallback values.** `SUBSIDIARY_MAP.get(name, "12")` is wrong — the `, "12"` default must not exist. Same for currency, country, and any other lookup.
- **Whitespace stripping (`.strip()`) is acceptable** — it's not a data change, just cleaning CSV artefacts.
- **`or None` to drop empty strings is acceptable** — sending an empty string to NS is different from omitting the field.

All known violations have been fixed. Every unmapped or blank required field now logs an error and returns `None` to skip the record — no silent defaults remain.

If you are adding or editing any loader code, **do not introduce new defaults or fallbacks**. If a value can't be resolved, log an error and return `None` to skip the record.

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
│   ├── billingkleeneexport20260409.csv        (67 rows — regenerated, DDL filter fixed)
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

## Outstanding Work

See **[TODO.md](TODO.md)** for the full prioritised task list (P0 → P1 → P2). Update it as tasks are completed.

---

## Current Status — What Works

- **All imports and module resolution**: `loaders/` package structure in place, tested.
- **Customer loader**: **68/68 loaded into NS**. `MP_HubSpot_6632970696` (SAFETY-KLEEN) initially failed — phone `'0203 814 8700 - HO  0203 814 8720 - DDI'` exceeded NS 32-char limit. Client (Adam) authorised removing phone entirely; CSV fixed and reloaded. All 68 success.
- **Billing account loader**: **68/68 loaded into NS**. Billing CSV regenerated from 100 → 67 rows (DDL filter fix). `billAddressList`/`shipAddressList` resolved at init via SuiteQL on `customeraddressbook` (30,355 rows, 31 pages). Address IDs are `internalid` from `customeraddressbook`, sent as plain strings — confirmed by inspecting existing billingAccount records via GET. 5 records that had `name` > 50 chars (NS hard limit) and 1 that was blocked by the missing customer are all now loaded. Note: final billing account name format TBD pending Moorepay/Tech discussion (Adam).
- **SuiteQL pagination**: `suiteql_query()` paginates via `?limit=1000&offset=N` until `hasMore=false`.
- **Subscription loader**: groups 70 CSV rows into 49 subscription headers with nested lines. Resolves customer via name→extId→stateTracker chain. Resolves billing account via `{deal_id}_BA` pattern. Correctly blocks when dependencies missing.
- **One-off loader**: 26 rows, resolves customer by name. Builds invoice payloads.
- **Orchestrator**: CLI with `--entity`, `--dry-run`, `--limit`, `--report`, `--failures`, `--skip-preflight`, `--field-map`, `--patch`, `--patch-eer`. Dependency warnings. Structured logging to `logs/YYYY-MM-DD/load_HH-MM-SS.log` (GMT+3), full tracebacks captured to file and terminal.
- **Customer custom fields**: **10 fields now set on all customers.** 9 standard fields (`cseg_busclass`, `cseg_segment`, `custentity_3805_dunning_procedure`, `custentity_3805_dunning_letters_toemail`, `emailpreference`, `custentity_alf_company_reg_num`, `custentityindexationdatecustomer`, `custentity_zellis_po_mandatory`, `custentity_2663_direct_debit`) are built into `build_payload()` and included automatically in every new customer POST — no extra flag needed. `custentity_zellis_elec_email_recipients` requires a separate `--patch-eer` step (two-step POST+PATCH, always run after `--entity customer`). `--patch` is retroactive-only (used once to update the 68 already-loaded customers before fields were added to `build_payload()`).
- **Idempotency**: SQLite state + NetSuite externalId upsert semantics.

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
| Direct Debit | `custentity_2663_direct_debit` | ✅ Auto — in `build_payload()`. bool (Y/True→true) |
| Allow Letters to be Emailed | `custentity_3805_dunning_letters_toemail` | ✅ Auto — in `build_payload()`. bool (Y→true) |
| PO Mandatory | `custentity_zellis_po_mandatory` | ✅ Auto — in `build_payload()`. bool (True/False string) |
| Dunning Procedure | `custentity_3805_dunning_procedure` | ✅ Auto — in `build_payload()`. `{"id": "6"}` (Moorepay \| Dunning Procedure, confirmed by GET on customer/578027) |
| Business/Class | `cseg_busclass` | ✅ Auto — in `build_payload()`. `{"id": "1"}` (Managed Services, confirmed via SuiteQL) |
| Segment | `cseg_segment` | ✅ Auto — in `build_payload()`. `{"id": "2"}` (Moorepay, confirmed via SuiteQL) |
| Company Reg Number | `custentity_alf_company_reg_num` | ✅ Auto — in `build_payload()`. Plain string from CSV |
| Email Preference | `emailpreference` | ✅ Auto — in `build_payload()`. Plain string `"PDF"` |
| Indexation Date | `custentityindexationdatecustomer` | ✅ Auto — in `build_payload()`. Date string (time component stripped from ISO datetime) |
| Dunning Contact First Name | `custentity_???` | ❌ Awaiting client: label lookup needed for custentity6/9/15_2/19/376 |
| Dunning Contact Last Name | `custentity_???` | ❌ Awaiting client: label lookup needed |
| Dunning Level (Req) | `custentity_3805_dunning_level` | ❌ Script ID known — NS value ID for "Level 1 and Above" unresolvable via SuiteQL/REST; NS UI lookup needed |
| Electronic Email Recipients | `custentity_zellis_elec_email_recipients` | ✅ Done — 63/68 linked (2026-04-16). Two-step: POST `customrecord_zellis_elec_email_recipient` (externalId=`{ext_id}_EER`, name=`{ext_id}_EER`, email1=CSV value) → PATCH customer. 5 blank in CSV skipped. `--patch-eer` flag added. |

### CSV Column → NetSuite Field Mappings (Billing Account)

| CSV Column | NS Field | Status |
|---|---|---|
| externalId | `externalId` | ✅ |
| name | `name` | ✅ — ⚠️ NS hard limit: 50 chars. 5 records exceed this. |
| customer_externalId | `customer.id` | ✅ Resolved via state tracker |
| subsidiary_id | `subsidiary.id` | ✅ Already NS internal ID |
| currency_id | `currency.id` | ✅ Already NS internal ID |
| billingSchedule_id | `billingSchedule.id` | ✅ Already NS internal ID |
| frequency | `frequency.id` | ✅ (e.g., "MONTHLY") |
| startDate | `startDate` | ✅ |
| requestOffCycleInvoice | `requestOffCycleInvoice` | ✅ |
| customerDefault | `customerDefault` | ✅ |
| inactive | `inactive` | ✅ |
| _(not in CSV)_ | `billAddressList` | ✅ Resolved at init via SuiteQL on `customeraddressbook`. Uses `internalid` (not `addressbookaddress`). Sent as plain string, not `{"id": "..."}`. Confirmed by GET on existing NS billingAccount. |
| _(not in CSV)_ | `shipAddressList` | ✅ Same resolution as `billAddressList`. |

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
- **Billing DDL**: Similar deal filters, fans out by NetSuite account number (companies can have multiple), joins to NS customer for billing address. Traffic cop: only subscription items (excludes one-offs via SALES_ITEM_MAPPING). **Note:** original export used LEFT JOIN + Feb 13 cutoff, producing 100 rows including 75 ghost rows referencing customers not in the customer extract. Regenerated with INNER JOIN + Feb 28 cutoff → 67 rows, all matching loaded customers.
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
