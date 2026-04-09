# TODO — NetSuite Data Loader

> Track progress against the migration pipeline. Update status as tasks are completed.
> Status key: `[ ]` pending · `[~]` in progress · `[x]` done

---

## P0 — Must fix before any API calls

- [~] **Test single customer POST against sandbox**
  - Drop CSV into `data/`, run `python main.py --entity customer --dry-run --limit 1` to inspect payload
  - Then run `python main.py --entity customer --limit 1` and check response
  - Fix any 400/422 field errors (addressBook structure, currency/subsidiary format, terms)

- [ ] **Resolve `terms` internal ID**
  - `loaders/customer.py` uses `{"refName": "Z030 - Payment w/in 30 days net"}` — refName may not resolve
  - Run in SuiteQL: `SELECT id, name FROM term WHERE name LIKE '%Z030%'`
  - Replace with `{"id": "<actual_id>"}` in `loaders/customer.py`

---

## P1 — Required for full pipeline

- [ ] **Map ~15 custom fields on Customer record**
  - Run: `SELECT scriptid, label FROM customfield WHERE fieldtype = 'ENTITY' ORDER BY label`
  - Fields needing `custentity_xxx` IDs:
    - Company Reg Number, Segment ("Moorepay"), Direct Debit
    - Business/Class ("Managed Services"), Dunning Procedure
    - Dunning Contact First Name, Dunning Contact Last Name
    - Dunning Level ("Level 1 and Above"), Email Preference ("PDF")
    - Allow Letters to be Emailed, Electronic Email Recipients
    - Indexation Date, PO Mandatory, n/a 1 (Direct Debit setup), n/a 2 (NS Account Number)
  - Add to payload in `loaders/customer.py` as `payload["custentity_xxx"] = value`

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

- [ ] **Load all 68 customers**
  - Once single-record test passes: `python main.py --entity customer`
  - Verify: `python main.py --report --failures`

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
