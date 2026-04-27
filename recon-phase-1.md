# Recon Phase 1 — Migration Issues & Status

> Tracks every blocker identified during the initial sandbox load. Updated as issues are resolved.
>
> Status: `[x]` resolved · `[~]` partially resolved · `[ ]` still blocked

---

## Pipeline Summary (as of 2026-04-22)

| Entity | Total | Loaded | Blocked |
|---|---|---|---|
| Customer | 68 | 68 ✓ | 0 |
| Billing Account | 68 | 68 ✓ | 0 |
| Subscription | 52 | 50 | 2 |
| One-Off Invoice | 34 | 25 | 9 |

---

## Section A — Subscriptions

### A1 — Missing Subscription Plan

**Root cause:** These deals had no Subscription Plan in the CSV. NS requires a plan to generate subscription lines — without one it rejects the record outright ("You cannot save a subscription that has no lines").

Originally 4 records. 3 now resolved.

| External ID | Customer | Resolution |
|---|---|---|
| 412352092390 | Catapult Limited | [x] Fixed — plan `Moorepay NextGen ROI (M)` added to CSV; loaded 2026-04-20 |
| 412482838771 | JONAS COMPUTING (UK) Ltd | [x] Fixed — plan added to CSV + customer name corrected (`LIMITED` → `Ltd`); loaded 2026-04-20 |
| 396048163025 | Uniqlo Europe Limited | [x] Fixed — plan was present on a non-first row in a 6-row group; loader patched to scan all rows via `next()` instead of reading `rows[0]` only; loaded 2026-04-20 (NS ID 58971) |
| 442541777135 | TRUSTWISE UK LIMITED | [ ] **Still blocked** — single-row group, no Subscription Plan anywhere in source data |

**Action needed:** Client to advise correct Subscription Plan for TRUSTWISE deal `442541777135`.

---

### A2 — Missing Price Book

**Root cause:** One subscription had a valid plan but `Price Book = NOT MAPPED` in every row of its CSV group. NS builds billing intervals from the price book — without one it fails ("First interval of an item cannot be deleted").

| External ID  | Customer                     | Subscription Plan    | Status                              |
|--------------|------------------------------|----------------------|-------------------------------------|
| 437881274561 | POWERTICA COMMODITIES UK LTD | Moorepay NextGen (M) | [x] **Resolved — loaded 2026-04-22** |

**Fix:** New subscription CSV provided with correct price book value. Loaded successfully on 2026-04-22 with 5 lines activated.

---

### A3 — Subscription Start Date Before Billing Account Start Date

**Root cause:** Two subscriptions had a start date that pre-dated their billing account's start date in NS. NS enforces a hard constraint: subscription start date must be ≥ billing account start date.

Originally 2 records. 1 resolved.

**Fix applied (2026-04-17):** New billing CSV (`billing-kleene-export-2026-04-17-A3-fix-applied.csv`) provided with corrected start dates. `--patch-ba-startdate` CLI flag added — reads the billing CSV, GETs each billing account from NS, PATCHes only where dates differ. Three billing accounts were successfully patched (HTTP 204).

| External ID | Customer | Sub Start Date | BA Start Date (NS) | Outcome |
|---|---|---|---|---|
| 385056850123 | BLACKMOOR INVESTMENT PARTNERS LIMITED | 2026-02-01 | was 2026-02-27, patched → 2026-02-01 | [x] Loaded ✅ — 3 lines activated |
| 478126306525 | VALE MILL (ROCHDALE) LIMITED | 2026-04-01 | 2026-04-15 (unchanged) | [ ] **Still blocked** |

**Why VALE MILL is still blocked:** The REST API PATCH returned HTTP 204 (success) but NS silently ignored the date change. NS has a business rule that prevents a billing account's start date from being moved to an *earlier* date via the API. We also confirmed NS enforces the constraint at the customer level — creating the subscription without an explicit billing account reference still fails, because NS auto-links the customer's default billing account.

**Action needed — NS admin:** Manually change billing account `478126306525_BA` (NS internal ID **25659**) start date from `15/04/2026` to `01/04/2026` directly in the NetSuite UI. Once done, reset the record to pending and re-run.

---

## Section B — One-Off Invoices

### B1 — Items Restricted to Subscription Billing Engine

**Root cause:** All 9 failing invoices reference items that NS will not accept on manually-created invoice records. The items exist and are active in NS, but their revenue recognition rule (`Rev Rec on Billing`) ties them exclusively to NS's subscription billing cycle. NS rejects them regardless of how they are referenced — by name, internal ID, or external ID. Error returned: `"Invalid item reference key [item name]"`.

Confirmed via SuiteQL: items are active, correct type (`NonInvtPart`), assigned to correct subsidiaries. The restriction is a configuration setting on the item itself, not a data problem.

**Affected items:**

| NS Item ID | Item Name |
|---|---|
| 18875 | Next Gen ROI (M) - Back Billing - Moorepayhr Managed |
| 18881 | Next Gen ROI (M) - Contract Enforce - Managed payroll |
| 8906 | Next Gen (M) - Contract Enforce - Managed payroll |
| — | Next Gen (S/ROI) - Contract Enforce - Auto Enrolment |
| — | Next Gen ROI (M) - Back Billing - Auto Enrolment |
| — | Next Gen (M/S/ROI) - Technical Delivery Tier 1 |
| — | Technical Delivery Tier 1 |

**Affected invoices (all still blocked):**

| External ID | Customer | Items | Rate |
|---|---|---|---|
| 371407556801 | LUMERA ITM UK Ltd | Back Billing + Contract Enforce variants | £6.38 |
| 372183117016 | ADDEV MATERIALS AEROSPACE LIMITED | Back Billing + Contract Enforce variants | £8.40 |
| 372192197874 | BLACKMOOR INVESTMENT PARTNERS LIMITED | Back Billing + Contract Enforce variants | £6.38 |
| 385061118187 | TRUSTWISE UK LIMITED | Technical Delivery Tier 1 variants | £70.07 |
| 387560761577 | OLYMPUS AUTOMATION LIMITED | Back Billing + Contract Enforce variants | £6.32 |
| 394887045364 | Uniqlo Europe Limited | Back Billing + Contract Enforce variants | £3.50 |
| 394887045365 | Uniqlo Europe Limited | Contract Enforce Auto Enrolment + Back Billing Auto Enrolment | £0.78 |
| 403682927805 | The Automation Partnership (Cambridge) Ltd | Back Billing + Contract Enforce variants | £0.00 |
| 387560761588 | OLYMPUS AUTOMATION LIMITED | Contract Enforce Auto Enrolment variants | — |

**Action needed — NS admin (choose one):**

- **Option A (preferred):** Reconfigure the affected items in NS to allow use on manually-created invoice transactions (change revenue recognition settings or remove transaction restrictions).
- **Option B:** Provide a substitute generic one-off charge item to use in place of the above items. The original item name can be preserved as the line description.
- **Option C:** Confirm these charges are handled entirely through the subscription billing cycle (already billed as part of the subscription) and should be marked out-of-scope for the one-off loader.
