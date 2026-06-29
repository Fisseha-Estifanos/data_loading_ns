# CLAUDE.md — NetSuite Data Loader (MoorePay HubSpot → NetSuite Migration)

## Project Goal

Load customer, billing account, subscription, and one-off invoice data from Snowflake-exported CSVs into NetSuite's Sandbox via REST API. This is a HubSpot-to-NetSuite migration for MoorePay (a Zellis company). The pipeline runs: HubSpot → Snowflake (transformation) → CSV exports → this Python loader → NetSuite REST API.

---

## 📌 Project Status — As of 2026-04-27

The DDL layer in Snowflake has evolved significantly since the first round of loading. Several structural changes affect what new CSV exports look like compared to what's currently in NetSuite. Anyone touching the loaders needs to know about these before making changes.

### Recent completions (this session)

- **Price plan loader** (`loaders/price_plan.py`) — **632/632 loaded** into NS sandbox `4874529-sb3`. Reconciliation pass fixes the `LIKE` wildcard bug (escaped to `MP\_PP\_%` with `ESCAPE '\\'` to avoid matching sandbox junk like `MPBPP1`). Loader is idempotent via state DB seed.
- **Subscription loader updated** — `loaders/subscription.py` now injects `pricePlan.id` per line from the state DB. The 1:N fan-out grain from the new CSV (943 rows / ~49 deals) is handled correctly by the existing grouping logic.
- **Retrofit script** (`scripts/retrofit_subscription_pricing.py`) — dry-run verified: 98 `WOULD_PATCH`, 18 `PP_BLANK_IN_SOURCE`, 162 `ORPHANED_IN_NS`, 165 `MISSING_IN_NS`. **Apply run pending** — run `python scripts/retrofit_subscription_pricing.py --apply` to execute the PATCHes.
- **One-off loader** — fan-out grain fix done. `OneOffLoader` now groups by `Invoice External ID` (same pattern as subscription loader).

### What changed upstream (Snowflake) since the original load

- **`SALES_ITEM_MAPPING` was normalised.** Previously: one row per HubSpot line item, with NetSuite sales items joined as a comma-separated string. Now: one row per (HS line item × NS sales item). Effect: the subs DDL (and one-off DDL) fans out 1:N. A single HS line that previously produced one CSV row now produces N rows — one per NS sales item it maps to.
  - **Subs CSV row count: ~350 → ~950.** Same ~79 deals, but more lines per deal.
  - **One-off CSV row count: similar fan-out** — though most of the original 26 one-offs flipped to subscriptions under the new classifier (see below), so one-off output dropped first then partially recovered after the case-fix on `MP_PRODUCT_STATUS`. Currently ~26 rows.

- **A per-line classifier table now decides sub vs one-off.** `PROD.HUBSPOT_NETSUITE_MAPPING.LINE_INDICATOR_RESOLVED` is the single source of truth for whether a HubSpot line item is `SUBSCRIPTION`, `ONE-OFF`, or `NEEDS_CLIENT_INPUT`. All four DDLs (billing, subs, one-off, pricing) join on this table and filter accordingly. The mapping table's `INDICATOR_TYPE` column is no longer the source of truth — it's a fallback default consulted by the classifier.
  - Rule precedence: slippage rule → four-exception (Off Boarding ×2, 2FA, HR Consultancy 0.5 day) price-field rule → unmapped items default to `NEEDS_CLIENT_INPUT` → mapping default fallback.
  - Slippage rule literal: `STARTED_AS_PER_EXPECTATION = 'Delayed - Client'` (with hyphen + spaces — matches DB form, not the human shorthand from the client email).

- **Pricing layer added.** `PROD.HUBSPOT_NETSUITE_MAPPING.PRICING_MP_STAGING` materialises one Price Plan per (HS line item × NS sales item) for SUBSCRIPTION-resolved lines. The corresponding `PROD.NETSUITE_REVERSE_ETL.JSON_PRICING_MP_V2` view emits ready-to-POST payloads keyed on `externalId = MP_PP_<LINE_ITEM_ID>_<slug(NS_SALES_ITEM)>`. Type/option decisions: `pricePlanType.id = 4` (Volume), `pricingOption.id = -101` (Rate), single tier starting at qty 0.

### Pending client input (blocks part of the load)

- **91 unmapped HubSpot items / 1,711 line items** appear on real customer deals but aren't in the v5.2 `MP Active sales items - Identification` catalogue. These currently resolve to `NEEDS_CLIENT_INPUT` in the classifier and are excluded from all entity loads. Awaiting client to confirm: in-scope or not, NS sales item mapping, sub vs one-off.
- **`MP Periodic Charge` semantics** (one-off Instruction 5 value) — no matching field in `LINE_ITEM_DETAILS`. Affected items currently route to `NEEDS_CLIENT_INPUT`.
- **Slippage `Delayed - *` variants** — client rule only mentions "Delayed - Client" but data has 4 other `Delayed - *` values (~2,600 lines). Awaiting confirmation on whether they also trigger one-off.
- **2 `Off Boarding Monthly Access to Read Only Licence` lines** — both `MP_TOTAL_ONE_OFF_COST` and `MP_MONTHLY_FEE` populated/zero. Genuinely ambiguous. Awaiting spot-check.

### What's next on the loader side

1. ~~**Price Plan loader (`loaders/price_plan.py`)**~~ — ✅ **Done.** 632/632 loaded. Idempotent reconciliation pass in place.

2. ~~**Subscription line price-plan retrofit (`scripts/retrofit_subscription_pricing.py`)**~~ — ✅ **Dry-run done.** Apply run pending (`python scripts/retrofit_subscription_pricing.py --apply`). Reports ORPHANED and MISSING without auto-action — see `data-team-handoff-2026-04-27.md` §3 for open questions before applying.

3. ~~**`subscription.py` update**~~ — ✅ **Done.** `pricePlan.id` injection per line from state DB. 1:N fan-out grain verified with new 943-row CSV.

4. ~~**`one_off.py` fan-out grain fix**~~ — ✅ **Done.** `OneOffLoader` now overrides `prepare_records()` to group by `Invoice External ID`, building one invoice payload per group with all rows contributing line items. Same `defaultdict` pattern as subscription loader. No price-plan reference needed (one-offs carry rate inline).

### Data drift warning for retrofit work

- Some sub lines in NS exist for HS lines that no longer appear in the new staging output (slippage flipped them to sub or NEEDS_CLIENT_INPUT). These will surface as ORPHANED in the retrofit script.
- Some new sub lines in the new CSV don't exist in NS yet (new fan-out from mapping normalisation). These will surface as MISSING.
- Don't auto-create or auto-delete — report only, get user sign-off.

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
│   ├── price_plan.py      # CSV → NetSuite pricePlan payload (no NS parent deps)
│   ├── subscription.py    # CSV → NetSuite subscription payload (refs customer + billing acct + price plan per line)
│   └── one_off.py         # CSV → NetSuite invoice payload (refs customer) — groups by Invoice External ID
├── scripts/
│   └── retrofit_subscription_pricing.py  # One-off PATCH script: attach pricePlan.id to existing sub lines
├── data/                  # CSV files from Snowflake exports (active files per config.py)
│   ├── customers-kleene-export-2026-04-09.csv          (68 rows)
│   ├── billing-account-kleene-export-2026-04-22.csv    (67 rows)
│   ├── price-plans-kleene-export-2026-04-27.csv        (632 rows — all loaded)
│   ├── subs-kleene-export-2026-04-27.csv               (943 rows → 49 unique subscriptions)
│   └── one-off-kleene-export-2026-04-27.csv            (25 rows → 7 unique invoices; fan-out fix pending)
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

See **[TODO.md](TODO.md)** for the full prioritised task list (P0 → P1 → P2). Update it as tasks are completed.

---

## Current Status — What Works

- **All imports and module resolution**: `loaders/` package structure in place, tested.
- **Customer loader**: **68/68 loaded into NS**. `MP_HubSpot_6632970696` (SAFETY-KLEEN) initially failed — phone `'0203 814 8700 - HO  0203 814 8720 - DDI'` exceeded NS 32-char limit. Client (Adam) authorised removing phone entirely; CSV fixed and reloaded. All 68 success.
- **Billing account loader**: **68/68 loaded into NS**. Billing CSV regenerated from 100 → 67 rows (DDL filter fix). `billAddressList`/`shipAddressList` resolved at init via SuiteQL on `customeraddressbook` (30,355 rows, 31 pages). Address IDs are `internalid` from `customeraddressbook`, sent as plain strings — confirmed by inspecting existing billingAccount records via GET. 5 records that had `name` > 50 chars (NS hard limit) and 1 that was blocked by the missing customer are all now loaded. Note: final billing account name format TBD pending Moorepay/Tech discussion (Adam).
- **SuiteQL pagination**: `suiteql_query()` paginates via `?limit=1000&offset=N` until `hasMore=false`.
- **Price plan loader**: **632/632 loaded into NS** (2026-04-27). Reconciliation pass at startup pulls all `MP\_PP\_%` plans from NS and seeds state DB. `build_payload()` passes through the pre-shaped `PRICE_PLAN_JSON` column from Snowflake unchanged.
- **Subscription loader**: **49/52 loaded into NS**. Now also injects `pricePlan.id` per line from state DB on new loads. Grouping logic handles the 1:N fan-out grain (943 rows → 49 deals). 3 records still blocked: `442541777135` (TRUSTWISE, no plan in source CSV — data issue), `437881274561` (POWERTICA MV991, NS rejects "First interval of an item cannot be deleted" — NS admin needed), `478126306525` (VALE MILL, BA start date mismatch — NS UI fix needed).
- **Subscription retrofit script**: **Dry-run verified** (98 `WOULD_PATCH`). Apply pending. Report at `scripts/reports/2026-04-27/retrofit_21-40-54.csv`. See `data-team-handoff-2026-04-27.md` §3 for open questions.
- **One-off loader**: 18/26 loaded. Fan-out grain fix now in scope (see §What's next). 8 records blocked on `revenueRecognitionRule = "Rev Rec on Billing"` items — awaiting NS admin or substitute item from client.
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

- Current CSV (`subs-kleene-export-2026-04-27.csv`): 943 rows → 49 unique subscription groups (grouped by `External ID` = deal ID). The 1:N fan-out from `SALES_ITEM_MAPPING` normalisation is handled by the existing grouping-by-deal logic.
- Header fields (same across rows in a group): Subscription Name, Customer, Start Date, End Date, Subsidiary, Currency, etc.
- **`Subscription Plan` and `Price Book` are NOT guaranteed to be on `rows[0]`** — they appear only on the plan-defining row. Loader uses `next()` scan across all group rows to find the first non-empty value.
- Line fields (differ per row): Sales Item, Lines: Include, Price Plan External ID (new)
- Customer resolution chain: `Customer` (name) → customer CSV `Company Name` → `External ID 2` → state tracker → NS internal ID
- Billing account resolution: `{External ID}_BA` → state tracker → NS internal ID
- **Price plan injection**: `Price Plan External ID` column looked up in state DB (`pricePlan` entity type); if found, `"pricePlan": {"id": "<ns_id>"}` is injected into the line payload. Blank PP = line uses plan default rate.

### One-Off Grouping Logic (pending fix)

- Current CSV (`one-off-kleene-export-2026-04-27.csv`): 25 rows → 7 unique invoices (grouped by `Invoice External ID`). Same 1:N fan-out as subs.
- Current loader is one-row-per-invoice — will emit 25 POSTs; NS rejects 18 as duplicate `externalId`s.
- Fix: group rows by `Invoice External ID`, build one payload per group with all line items from the group. Same pattern as subscription loader. No price-plan field needed.

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
- **Customer name → extId mapping for subscriptions**: because the subscription CSV doesn't carry customer external IDs directly, we resolve via company name. All 49 subscription customers match the 68 customer CSV rows.
- **No external dependencies beyond `requests`**: keeps deployment simple. OAuth 1.0 signing is implemented manually (no `requests-oauthlib`).
- **Per-line classifier as single source of truth** (NEW): all four DDLs filter on `LINE_INDICATOR_RESOLVED.RESOLVED_INDICATOR_TYPE` rather than the static `SALES_ITEM_MAPPING.INDICATOR_TYPE`. Eliminates the cross-file duplicate problem (same HS line emitted to both subs and one-off files) and centralises the slippage + four-exception classification rules.
- **One Price Plan per sub line** (MVP): externalId convention `MP_PP_<LINE_ITEM_ID>_<slug(NS_SALES_ITEM)>`. Future iteration may dedupe shared plans across lines once client confirms reuse rules.
- **One-off grouping** (pending fix): same 1:N fan-out as subs; fix mirrors the subscription loader's `groupby(External ID)` pattern.