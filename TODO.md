# TODO — NetSuite Data Loader

> Track progress against the migration pipeline. Update status as tasks are completed.
> Status key: `[ ]` pending · `[~]` in progress · `[x]` done
>
> ⛔ **Data integrity rule:** This loader must never silently alter, default, or invent values.
> If a field is missing or unmapped, the record must **fail with a logged error** — never substitute a default.
> See README.md and CLAUDE.md for the full rule and known violations.

---

## CLI Quick Reference

Run in this order:

```text
python main.py --field-map                        # 1. Inspect all CSV→API field mappings (no credentials)
python main.py --dry-run --entity customer        # 2. Validate payloads before going live
python main.py --dry-run --limit 1                #    Test a single record
python main.py --entity customer                  # 3. Load customers first (no dependencies)
python main.py --entity billingAccount            # 4. Load billing accounts (needs customers)
python main.py --entity subscription              # 5. Load subscriptions (needs customers + billing)
python main.py --entity oneOff                    # 6. Load one-off invoices (needs customers)
python main.py --report                           # 7. Check state summary + field mapping
python main.py --report --failures                #    Include per-record error details
```

| Flag               | Description                                                                          |
| ------------------ | ------------------------------------------------------------------------------------ |
| `--entity`         | Load one entity type: customer, billingAccount, subscription, oneOff                 |
| `--dry-run`        | Build payloads and log them — no API calls made                                      |
| `--limit N`        | Process only first N records                                                         |
| `--skip-preflight` | Skip auth connectivity check at startup                                              |
| `--report`         | Print load state summary. Also prints field mapping. No loading.                     |
| `--failures`       | Add per-record error details to `--report` output                                    |
| `--field-map`      | Print CSV column → NetSuite API field mapping for all loaders. No credentials needed |
| `--patch`          | PATCH existing customer records with custom fields (use with `--entity customer`)    |

---

## P0 — Must fix before any API calls

- [x] **Fix silent data defaults — violates data integrity rule**
  - All changed to hard failures (log error + return None) instead of guessing:
  - `loaders/customer.py`: unmapped country → error + skip ✅
  - `loaders/subscription.py`: unmapped subsidiary → error + skip ✅
  - `loaders/subscription.py`: unmapped currency → error + skip ✅
  - `loaders/one_off.py`: unmapped subsidiary → error + skip ✅
  - `loaders/one_off.py`: unmapped currency → error + skip ✅
  - `loaders/one_off.py`: blank quantity → error + skip ✅

- [x] **Test single customer POST against sandbox**
  - Payload structure validated. Customer `MP_HubSpot_10353346261` created in NS as ID `800518`.
  - "Already exists" recovery path working: Tier 2 GET by externalId resolves ID correctly.
  - Auth bug fixed: OAuth realm must be `4874529_SB3` (uppercase + underscore), not `4874529-sb3`.
  - `respond-async` removed from Prefer header — was causing silent 202 async creates.

- [ ] **Resolve `terms` internal ID** ⚠️ do after full customer load
  - ID is already known: `19` (`Z030 - Payment w/in 30 days net`) — confirmed via SuiteQL
  - Replace `{"refName": "Z030 - Payment w/in 30 days net"}` with `{"id": "19"}` in `loaders/customer.py`
  - Then re-run customers to patch existing records

- [x] **Fix customer `MP_HubSpot_6632970696` phone number (> 32 chars)**
  - Customer failed at load: `USER_ERROR: field phone contained more than the maximum number (32) of characters`
  - **Client instruction (Adam):** remove the phone number completely; he will inform HubSpot/Paul
  - ✅ Phone field blanked in `customers-kleene-export-2026-04-09.csv`, reloaded — now success
  - ✅ `435947798740_BA` also unblocked and loaded successfully

- [ ] **Resolve billing account `name` > 50 char limit — 5 records**
  - NS hard limit: `name` field max 50 characters
  - Pattern is `{CompanyName}_{Frequency}_{Subsidiary}_{Currency}` — suffix `_Monthly_MP_GBP` = 16 chars, leaving 34 for company name
  - **Client response (Adam):** "change logic to have the customer external Id for the meantime — will talk to Moorepay and Tech as the name appears on the invoice"
  - Current loader: still uses CSV `name` field (reverted) — final name format TBD pending Moorepay/Tech discussion
  - Affected records:
    - `236171960549_BA`: `LIND GROUP HOLDING COMPANY LIMITED (NHR)_Monthly_MP_GBP` (55)
    - `459497468152_BA`: `The Automation Partnership(Cambridge)Ltd_Monthly_MP_GBP` (55)
    - `444242733290_BA`: `Dsm Nutritional Products (Uk) Ltd Dalry_Monthly_MP_GBP` (54)
    - `385056850123_BA`: `BLACKMOOR INVESTMENT PARTNERS LIMITED_Monthly_MP_GBP` (52)
    - `393822207222_BA`: `HARNHAM SEARCH AND SELECTION LIMITED_Monthly_MP_GBP` (51)
  - Once name format is decided: update `"name"` line in `loaders/billing_account.py:144`, reset 5 records to `pending`, reload

---

## P1 — Required for full pipeline

- [x] **Load all 68 customers** — 68/68 done ✅

- [x] **Map custom fields on Customer record** — 9 fields patched across all 68 customers ✅
  - All 68 PATCHed (HTTP 204) on 2026-04-15. `python main.py --entity customer --patch`
  - Fields now set on every customer:
    - `cseg_busclass` → `{"id": "1"}` (Managed Services) ✅
    - `cseg_segment` → `{"id": "2"}` (Moorepay) ✅
    - `custentity_3805_dunning_procedure` → `{"id": "6"}` ✅
    - `custentity_3805_dunning_letters_toemail` → `true` ✅
    - `emailpreference` → `"PDF"` ✅
    - `custentity_alf_company_reg_num` → from CSV `Company Reg Number` ✅
    - `custentityindexationdatecustomer` → from CSV `Indexation Date` (date only) ✅
    - `custentity_zellis_po_mandatory` → from CSV `PO Mandatory` ✅
    - `custentity_2663_direct_debit` → from CSV `Direct Debit` ✅
  - 1 field patched using `--patch-eer` flag for 63/68. 5 skipped (blank in CSV).
    - `custentity_zellis_elec_email_recipients` ✅ Done (2026-04-16): 63/68 customers linked. Two-step: POST `customrecord_zellis_elec_email_recipient` (externalId=`{ext_id}_EER`) → PATCH customer. 5 skipped (blank in CSV). Run: `python main.py --entity customer --patch-eer`
  - **Still deferred (awaiting client / Phase 2):**
    - `custentity_3805_dunning_level` — can't resolve "Level 1 and Above" ID via SuiteQL/REST; NS UI lookup needed
    - Dunning Contact First/Last Name — awaiting client mapping for `custentity6/9/15_2/19/376`

- [ ] **Resolve subscription plan internal IDs**
  - `loaders/subscription.py` uses `{"refName": "HR Services rolling (LPG)"}` etc.
  - Run: `SELECT id, name FROM subscriptionplan`
  - Build a mapping dict and switch to `{"id": "..."}`

- [ ] **Resolve sales item internal IDs**
  - Used in both subscription lines and one-off invoices
  - Run: `SELECT id, itemid, displayname FROM item WHERE isinactive = 'F'`
  - Replace refName references in `loaders/subscription.py` and `loaders/one_off.py`

- [ ] **Verify subscription REST API schema**
  - Run: `GET /record/v1/metadata-catalog/subscription`
  - Confirm field names: `subscriptionPlan`, `priceBook`, `subscriptionLine` sublist, line item structure

- [ ] **Confirm one-off invoice record type**
  - Currently `RECORD_TYPE = "invoice"` in `loaders/one_off.py`
  - Verify with MoorePay NS team — may need `customSale`, `cashSale`, or a custom record

- [x] **Load all billing accounts** — 68/68 done ✅
  - Billing CSV regenerated: 100 → 67 rows (DDL fix: INNER JOIN + correct Feb 28 cutoff)
  - `billAddressList`/`shipAddressList` resolved: queries `customeraddressbook` at init, uses `internalid` as plain string
  - SuiteQL pagination added: fetches all 30,355 address rows across 31 pages
  - 19 customers had missing default address flags in NS — repaired directly, all 19 subsequently loaded
  - 5 name > 50 chars resolved and loaded; 1 customer-blocked record unblocked and loaded
  - Note: NS `name` field has a 50-char hard limit — final billing account name format TBD pending Moorepay/Tech discussion (Adam)

- [ ] **Load 49 subscriptions**
  - `python main.py --entity subscription`
  - Verify: `python main.py --report --failures`

- [ ] **Load 26 one-off invoices**
  - `python main.py --entity oneOff`
  - Verify: `python main.py --report --failures`

---

## P2 — Hardening / nice to have

- [ ] **Investigate billing account ↔ subscription linkage gap**
  - Only 14 of 49 subscriptions have a matching billing account
  - Root cause: billing DDL ends 2026-02-13, subscription DDL ends 2026-02-28
  - Subscriptions load without billing account ref for now — linkage may need patching later

- [ ] **Implement `--retry-failed` CLI flag**
  - Docstring references it but it is not implemented
  - Current behaviour: re-run skips `success` records and retries everything else

- [ ] **Implement contact subrecords**
  - CSV has Contact First/Last Name and Job Title
  - Currently only `title` is set on the customer record
  - Full contact creation as a linked `contact` record (or `contactRoles` subcollection) not yet implemented

- [ ] **Rate limit / throughput tuning**
  - `config.REQUEST_DELAY_SECONDS = 0.5` is conservative
  - Reduce after sandbox testing confirms NS rate limits are comfortable
