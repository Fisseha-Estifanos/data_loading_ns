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

| Flag               | Description                                                                              |
| ------------------ | ---------------------------------------------------------------------------------------- |
| `--entity`         | Load one entity type: customer, billingAccount, subscription, oneOff                     |
| `--dry-run`        | Build payloads and log them — no API calls made                                          |
| `--limit N`        | Process only first N records                                                             |
| `--skip-preflight` | Skip auth connectivity check at startup                                                  |
| `--report`         | Print load state summary. Also prints field mapping. No loading.                         |
| `--failures`       | Add per-record error details to `--report` output                                        |
| `--field-map`      | Print CSV column → NetSuite API field mapping for all loaders. No credentials needed     |

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

- [ ] **Fix customer `MP_HubSpot_6632970696` phone number (> 32 chars)**
  - This customer failed at load with `USER_ERROR: field phone contained more than the maximum number (32) of characters`
  - Fix the phone value in the source CSV and reload — then `435947798740_BA` can also load

- [ ] **Resolve billing account `name` > 50 char limit — 5 records**
  - NS hard limit: `name` field max 50 characters
  - Pattern is `{CompanyName}_{Frequency}_{Subsidiary}_{Currency}` — suffix `_Monthly_MP_GBP` = 16 chars, leaving 34 for company name
  - Affected records (awaiting client decision on approach):
    - `236171960549_BA`: `LIND GROUP HOLDING COMPANY LIMITED (NHR)_Monthly_MP_GBP` (55)
    - `459497468152_BA`: `The Automation Partnership(Cambridge)Ltd_Monthly_MP_GBP` (55)
    - `444242733290_BA`: `Dsm Nutritional Products (Uk) Ltd Dalry_Monthly_MP_GBP` (54)
    - `385056850123_BA`: `BLACKMOOR INVESTMENT PARTNERS LIMITED_Monthly_MP_GBP` (52)
    - `393822207222_BA`: `HARNHAM SEARCH AND SELECTION LIMITED_Monthly_MP_GBP` (51)
  - Options: shorten suffix in Snowflake DDL (preferred) or truncate in loader (requires explicit sign-off)

---

## P1 — Required for full pipeline

- [~] **Load all 68 customers**
  - First record (`MP_HubSpot_10353346261` → NS ID `800518`) confirmed working
  - Run: `python main.py --entity customer`
  - Verify: `python main.py --report --failures`

- [~] **Map custom fields on Customer record** ⚠️ do after full customer load
  - Customers load fine without these — do not block the pipeline on them
  - **Confirmed script IDs + types (from live GET on customer/800518):**
    - `custentity_2663_direct_debit` → bool
    - `custentity_3805_dunning_letters_toemail` → bool (Allow Letters to be Emailed)
    - `custentity_zellis_po_mandatory` → bool (PO Mandatory)
    - `custentity_3805_dunning_procedure` → linked record (`{"id": "..."}`) — **NS value ID needed**
    - `cseg_busclass` → linked record (`{"id": "..."}`) — "Managed Services" = ID `1` ✅
  - **Awaiting client response** (sent message requesting these):
    - Labels for `custentity6`, `custentity9`, `custentity15_2`, `custentity19`, `custentity376` → needed to identify Company Reg Number, Segment, Dunning Contact, Dunning Level, Email Preference, Electronic Email Recipients, Indexation Date
    - NS internal ID for the correct MoorePay Dunning Procedure value (`customrecord_3805_dunning_procedure` is not queryable via SuiteQL — NS UI lookup needed)
  - Once client responds: add confirmed mappings to `loaders/customer.py` and re-run to patch existing records

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

- [~] **Load all billing accounts** — 62/67 done
  - Billing CSV regenerated: 100 → 67 rows (DDL fix: INNER JOIN + correct Feb 28 cutoff)
  - `billAddressList`/`shipAddressList` resolved: queries `customeraddressbook` at init, uses `internalid` as plain string
  - SuiteQL pagination added: fetches all 30,355 address rows across 31 pages
  - 19 customers had missing default address flags in NS — repaired directly, all 19 subsequently loaded
  - **5 remaining failures** (awaiting decision — see P0 below):
    - 4 records: billing account `name` > 50 chars (NS hard limit)
    - 1 record: `435947798740_BA` blocked — customer `MP_HubSpot_6632970696` never loaded (phone > 32 chars)

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
