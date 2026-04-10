# TODO — NetSuite Data Loader

> Track progress against the migration pipeline. Update status as tasks are completed.
> Status key: `[ ]` pending · `[~]` in progress · `[x]` done

---

## P0 — Must fix before any API calls

- [x] **Test single customer POST against sandbox**
  - Payload structure validated. Customer `MP_HubSpot_10353346261` created in NS as ID `800518`.
  - "Already exists" recovery path working: Tier 2 GET by externalId resolves ID correctly.
  - Auth bug fixed: OAuth realm must be `4874529_SB3` (uppercase + underscore), not `4874529-sb3`.
  - `respond-async` removed from Prefer header — was causing silent 202 async creates.

- [ ] **Resolve `terms` internal ID** ⚠️ do after full customer load
  - ID is already known: `19` (`Z030 - Payment w/in 30 days net`) — confirmed via SuiteQL
  - Replace `{"refName": "Z030 - Payment w/in 30 days net"}` with `{"id": "19"}` in `loaders/customer.py`
  - Then re-run customers to patch existing records

---

## P1 — Required for full pipeline

- [~] **Load all 68 customers**
  - First record (`MP_HubSpot_10353346261` → NS ID `800518`) confirmed working
  - Run: `python main.py --entity customer`
  - Verify: `python main.py --report --failures`

- [ ] **Map ~15 custom fields on Customer record** ⚠️ do after full customer load
  - `custentity_xxx` fields are intentionally left empty for now — customers load fine without them
  - Live GET response revealed script IDs already on the sandbox record:
    - `custentity_2663_direct_debit`
    - `custentity_3805_dunning_letters_toemail`
    - `custentity_3805_dunning_letters_toprint`
    - `custentity_3805_dunning_manager`
    - `custentity6`, `custentity9`, `custentity15_2`, `custentity19`, `custentity376`
    - `cseg_busclass` (Business/Class segment)
  - Full schema saved to `metadata_customer.json` — use that to match script IDs to CSV column labels
  - Then add mapped values to `loaders/customer.py` and re-run to patch

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

- [ ] **Load all 100 billing accounts**
  - `python main.py --entity billingAccount`
  - Verify: `python main.py --report --failures`

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
