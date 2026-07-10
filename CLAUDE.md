# CLAUDE.md — NetSuite Data Loader (MoorePay HubSpot → NetSuite Migration)

## Project Goal

Load customer, billing account, subscription, and one-off invoice data from Snowflake-exported CSVs into NetSuite (Sandbox `4874529-sb3` and Production `4874529`) via REST API. This is a HubSpot-to-NetSuite migration for MoorePay (a Zellis company). The pipeline runs: HubSpot → Snowflake (transformation) → CSV exports → this Python loader → NetSuite REST API.

Run steps live in **[README.md](README.md)** (single source of truth). Open
tasks/blockers live in **[TODO.md](TODO.md)**. This file holds durable domain
context — architecture, field mappings, DDL logic, design decisions.

---

## 🔁 Load Revisions (`config.LOAD_REVISION`)

> What/why, ID shapes, and the env setup live in **[README.md](README.md)**.
> Kept here: the code-level detail needed when editing loaders.

Where the suffix is applied:

- `BaseLoader.prepare_records()` auto-suffixes the ext_id in the records tuple
  AND overrides `payload["externalId"]` before POST.
- Subscription/one-off override `prepare_records` and apply the same pattern.
- Dependent loaders (BA→customer, subs→customer/BA/price plan, oneOff→customer,
  EER→customer) wrap parent ext_ids with `config.apply_revision(...)` before
  consulting the state tracker.
- Price-plan reconciliation at startup is scoped to `LIKE 'MP\_PP\_%<rev>'`.
- Tier-3 fallback when `tier3_field == "externalId"` uses the already-suffixed
  value from the records tuple.
- **Prod customers are keyed RAW** (no suffix) — they pre-exist in NS. Only
  BA / subscription / pricePlan / oneOff carry the suffix.

Retrofit script note: `scripts/retrofit_subscription_pricing.py` was built
against `_rvn_01` and is not revision-aware — audit matching logic before reuse.

---

## ⛔ STRICT DATA INTEGRITY RULE — DO NOT MODIFY SOURCE DATA

**Never silently alter, default, or invent source values.** If a required field
is missing or unmapped, **log an error and return `None` to skip the record** —
never substitute a default (`SUBSIDIARY_MAP.get(name, "12")` is wrong — drop the
default). No reformatting/coercion of values before sending to NS. The only
permitted touches are `.strip()` and `"" → None`. When editing loader code, do
not introduce new defaults or fallbacks.

(Full rationale in **[README.md](README.md)**.)

---

## Architecture

```text
netsuite_loader/
├── config.py              # Credentials, paths, retry settings
├── netsuite_client.py     # OAuth 1.0 TBA signing, REST calls, 3-tier ID retrieval
├── state_tracker.py       # SQLite persistence for idempotent loads + ID chaining
├── main.py                # CLI orchestrator (--entity, --dry-run, --report, --failures)
├── loaders/
│   ├── base.py            # Abstract base: CSV reading, load loop, hash, skip-if-done
│   ├── customer.py        # CSV → NetSuite customer payload
│   ├── billing_account.py # CSV → NetSuite billingAccount payload (refs customer)
│   ├── price_plan.py      # CSV → NetSuite pricePlan payload (no NS parent deps)
│   ├── subscription.py    # CSV → NetSuite subscription payload (refs customer + billing acct + price plan per line)
│   └── one_off.py         # CSV → NetSuite invoice payload (refs customer) — groups by Invoice External ID
├── scripts/
│   └── retrofit_subscription_pricing.py  # One-off PATCH script: attach pricePlan.id to existing sub lines
├── data/                  # CSV files from Snowflake exports — ACTIVE FILES ARE DEFINED IN config.py
│   └── ...                 # (customers / billing-accounts / pricing-json / subs / one-offs)
├── validate.py            # Read-only pre-load validator (--validate); runs before every load
├── prod/                  # Read-only inspectors + helpers (prod_check.py, seed_state.py, ...)
├── state/                 # SQLite DB (auto-created at runtime)
├── logs/                  # Timestamped log files (auto-created)
├── requirements.txt       # Only: requests>=2.31.0
└── README.md
```

---

## NetSuite API Details

- **Accounts**: Sandbox `4874529-sb3`, Production `4874529`. `config.py` derives
  `BASE_URL` / `SUITEQL_URL` / `REALM` from `NS_REALM` — no code change to switch.
- **URL shape**: `https://<realm>.suitetalk.api.netsuite.com/services/rest/record/v1`
  (and `.../query/v1/suiteql` for SuiteQL).
- **Auth**: OAuth 1.0 TBA with HMAC-SHA256. Requires Consumer Key/Secret, Access Token/Secret, Realm.
- **Create response**: HTTP 204 No Content, no body, `Location` header contains internal ID (e.g., `…/customer/800419`).
- **Credentials**: env vars `NS_CONSUMER_KEY`, `NS_CONSUMER_SECRET`, `NS_ACCESS_TOKEN`, `NS_TOKEN_SECRET`, `NS_REALM`, `NS_STATE_DB` (see README env setup).

---

## Load Order, ID Resolution & State Tracker

The load hierarchy, the 3-tier ID resolution table, and the state-DB schema all
live in **[README.md](README.md)**. Code-relevant notes:

- Each child resolves its parent's NS internal ID from the state tracker; if the
  parent isn't loaded, the child is skipped with a logged error.
- ID status values: `success`, `success_no_id` (2xx but ID unresolved — manual
  review), `failed`.
- State DB path is `$NS_STATE_DB` (sandbox `state/load_state.db`, prod
  `state/load_state_prod.db`). Being removed in WS2 — see the refactor plan.

---

## Outstanding Work

See **[TODO.md](TODO.md)** for the full prioritised task list (P0 → P1 → P2) and
the open client-input/data blockers. Update it as tasks are completed.

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
| Electronic Email Recipients | `custentity_zellis_elec_email_recipients` | ✅ Via `--patch-eer`. Two-step: POST `customrecord_zellis_elec_email_recipient` (externalId=`{ext_id}_EER`, name=`{ext_id}_EER`, email1=CSV value) → PATCH customer. Rows blank in CSV are skipped. |

### CSV Column → NetSuite Field Mappings (Billing Account)

| CSV Column | NS Field | Status |
|---|---|---|
| externalId | `externalId` | ✅ |
| name | `name` | ✅ — ⚠️ NS hard limit: 50 chars. 5 records exceed this. |
| customer_externalId | `customer.id` | ✅ Bridge to a C-number only (→ customer CSV row → `C-number`). Since 2026-07-09 the BA's customer resolves by **C-number ONLY** — primarily the owning sub's `NETSUITE_ACCOUNT_NUMBER*` via the `<sub>_BA` key, else this bridge. Never used as an NS lookup key itself. |
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

- Subscription rows are grouped by `External ID` (= deal ID) into one subscription per deal. The 1:N fan-out from `SALES_ITEM_MAPPING` normalisation (many line rows per deal) is handled by this grouping-by-deal logic.
- Header fields (same across rows in a group): Subscription Name, Customer, Start Date, End Date, Subsidiary, Currency, etc.
- **`Subscription Plan` and `Price Book` are NOT guaranteed to be on `rows[0]`** — they appear only on the plan-defining row. Loader uses `next()` scan across all group rows to find the first non-empty value.
- Line fields (differ per row): Sales Item, Lines: Include, Price Plan External ID (new)
- Customer resolution (since 2026-07-09): by **C-number (NS `entityid`) ONLY** — subs `NETSUITE_ACCOUNT_NUMBER` → `NETSUITE_ACCOUNT_NUMBER_COMPANY_LEVEL` → customer CSV `C-number` (bridged by `Customer` name, alias-resolved). No externalId or company-name resolution against NS; no state tracker. A customer created by this loader resolves only after the warehouse re-exports with its NS-assigned C-number.
- Billing account resolution: `{External ID}_BA` → state tracker → live NS.
  **Convention (since 2026-07-09): one BA per SUB, keyed on the FULL sub External ID** —
  `<deal>_<subid>_BA` (e.g. sub `498500387059_27401` → BA `498500387059_27401_BA`).
  Previously one BA was shared per deal (`<deal>_BA`, deal = token before the first
  underscore); records loaded before 2026-07-09 remain in NS under that old key shape.
  **BA is MANDATORY (rule since 2026-07-09): a subscription is never POSTed without
  `billingAccount`.** If `<sub>_BA` resolves in neither the state DB nor live NS, the
  loader errors + skips the sub (and prints a blocked-subs summary) — loading without
  a BA makes NS silently attach the customer's DEFAULT billing account, i.e. a wrong
  one. Validate check 6 (BLOCKER) gates the same condition pre-load.
- **Price plan injection**: `Price Plan External ID` column looked up in state DB (`pricePlan` entity type); if found, `"pricePlan": {"id": "<ns_id>"}` is injected into the line payload. Blank PP = line uses plan default rate.

### One-Off Grouping Logic

- One-off rows fan out 1:N like subs. `OneOffLoader` overrides `prepare_records()`
  to group rows by `Invoice External ID`, building one invoice payload per group
  with all rows contributing line items (same `defaultdict` pattern as the
  subscription loader). No price-plan field needed — one-offs carry rate inline.

---

## Snowflake DDL Context (For Reference)

The CSVs are generated by 4 Snowflake DDLs (5 with the new pricing DDL). Key logic embedded in the transforms:

- **Customer DDL**: Pulls from HubSpot deals (closed won, contract check completed, specific onboarding statuses, close date Jan-Feb 2026), joins to companies and contacts, picks primary contact by role priority, derives billing email and implementation email.
- **Billing DDL**: Similar deal filters, fans out by NetSuite account number (companies can have multiple), joins to NS customer for billing address. Traffic cop: only subscription items via `LINE_INDICATOR_RESOLVED` classifier. **Note:** original export used LEFT JOIN + Feb 13 cutoff, producing 100 rows including 75 ghost rows referencing customers not in the customer extract. Regenerated with INNER JOIN + Feb 28 cutoff → 67 rows, all matching loaded customers.
- **Subscription DDL**: Same deal filters, derives `FINAL_SUBSCRIPTION_PLAN` via `SUBSCRIPTION_PLAN_MAPPING` lookup + deal-level inheritance (was a hardcoded CASE; refactored). Derives START_DATE based on plan type (payroll plans use payroll commencement date truncated to month start; HR plans use earliest commencement date). Traffic cop: `LINE_INDICATOR_RESOLVED` classifier filters to `RESOLVED_INDICATOR_TYPE = 'SUBSCRIPTION'`. Rejects payroll rows where start date isn't 1st of month. Outputs `LINE_ITEM_ID` and `Price Plan External ID` columns for the loader's price-plan injection.
- **One-Off DDL**: Same base, but traffic cop filters to `RESOLVED_INDICATOR_TYPE = 'ONE-OFF'`. Same start date logic. Same payroll date rejection.
- **Pricing DDL** (`PRICING_MP_STAGING`): Materialises one Price Plan per (HS line × NS sales item) for SUBSCRIPTION-resolved lines. NET_PRICE resolved per Instruction 4 (which HS price field to use), MINIMUM_AMOUNT per Instruction 5. JSON view (`JSON_PRICING_MP_V2`) emits ready-to-POST payloads.
- **Classifier table** (`LINE_INDICATOR_RESOLVED`): Per-line resolver shared by all four DDLs above. Holds `RESOLVED_INDICATOR_TYPE` + `RESOLUTION_REASON` per line item.

---

## Testing Approach

See the standard load sequence in **[README.md](README.md)** — always
`--validate` first, then dry-run each entity (`--dry-run`), load, and check with
`--report --failures`.

---

## Key Design Decisions

- **SQLite for state** (not a file/CSV): supports concurrent reads, atomic writes, and SQL queries for reporting.
- **External IDs as idempotency keys**: NetSuite upserts by externalId, so even if the state DB is lost, re-running won't create duplicates.
- **3-tier ID retrieval**: because the POST response is 204 with no body, we must parse the Location header (Tier 1). Tiers 2 and 3 are fallbacks for edge cases (timeouts, missing headers).
- **C-number-only customer attachment (2026-07-09)**: subs and BAs resolve/attach their customer by C-number (NS `entityid`) and nothing else — no externalId, no company-name matching against NS. Names and External ID 2 exist only as bridges to a C-number in the CSVs. Rationale: one env-agnostic key, one resolution path shared by loaders and validator (`customer_resolver.py`), no state-tracker seeding. Consequence: after creating a customer, re-export so the subs carry its NS C-number. (Replaces the old name→External ID 2→state-tracker chain.)
- **No external dependencies beyond `requests`**: keeps deployment simple. OAuth 1.0 signing is implemented manually (no `requests-oauthlib`).
- **Per-line classifier as single source of truth** (NEW): all four DDLs filter on `LINE_INDICATOR_RESOLVED.RESOLVED_INDICATOR_TYPE` rather than the static `SALES_ITEM_MAPPING.INDICATOR_TYPE`. Eliminates the cross-file duplicate problem (same HS line emitted to both subs and one-off files) and centralises the slippage + four-exception classification rules.
- **One Price Plan per sub line** (MVP): externalId convention `MP_PP_<LINE_ITEM_ID>_<slug(NS_SALES_ITEM)>`. Future iteration may dedupe shared plans across lines once client confirms reuse rules.
- **One-off grouping**: same 1:N fan-out as subs; mirrors the subscription loader's `groupby(External ID)` pattern.
