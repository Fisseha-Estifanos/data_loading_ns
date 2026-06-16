# Refactor Plan — Pre-Load Validator + Remove Persistent State DB

> **Purpose of this doc:** a complete, self-contained brief so this work can be started in a
> **fresh session** with no prior context. Read this top-to-bottom and you have everything.
>
> **Status:** PLANNED — not started. Created 2026-06-16.
> **Owner:** Fisseha (Kleene) · NetSuite loader for MoorePay (HubSpot→NetSuite migration).

---

## 0. TL;DR — what we're doing and why

Two changes, in this order of value:

1. **WS1 — Pre-load validator** (do first; biggest win). A read-only step that runs **before any
   POST** and reports **every** data problem at once — name mismatches, key mismatches, subsidiary
   conflicts, bad dates, unmapped items — instead of discovering them one failed POST at a time.

2. **WS2 — Remove the persistent SQLite state DB**, replacing it with **in-memory reconciliation
   from NetSuite at the start of each run**. NS becomes the single source of truth. Kills the
   state↔NS drift class of bugs (the "clear-the-rows / don't-bump-the-revision" dance), the
   sandbox/prod state footgun, and most of the revision gymnastics.

**Why this framing:** In the session that motivated this, ~6 of 7 pain points were *data quality*
(WS1 territory), and exactly 1 was *state drift* (WS2 territory). Removing the state DB alone would
not have prevented the expensive problems — the validator is the higher-leverage change, which is
why it's first.

**End state:** one runbook — `validate → load → done` — NS as the single source of truth, no state
file to babysit.

---

## 1. Context recap (for a cold-start session)

- This is a Python REST loader that pushes Snowflake-exported CSVs into **NetSuite production**
  account `4874529` (also a sandbox `4874529-sb3`). Entities load in a strict hierarchy:
  **customer → billingAccount → pricePlan → subscription → oneOff**.
- Auth = OAuth 1.0 TBA. Creds + `NS_STATE_DB` come from `.env.prod` (sourced before running).
- NetSuite **upserts by `externalId`** and is **queryable by externalId** (`GET .../eid:{externalId}`
  or SuiteQL `WHERE externalid = ...`). This is the linchpin that makes WS2 possible.
- Every externalId we POST ends with `config.LOAD_REVISION` (currently `_rvn_prod_01`), applied by
  `config.apply_revision()`.
- Read `CLAUDE.md` for the full domain context. Key data-quality landmines discovered live (the
  validator must encode all of these — see §5):
  - **Customer name mismatches**: subs/BA CSV `Customer` spellings differ from the loaded NS
    customer `Company Name`. Known trio: `B.& M.MCHUGH LIMITED`→`B&M Mchugh Limited`,
    `Forest Healthcare Ltd`→`Forest Healthcare Limited`, `National Association Of Headteachers`→`Naht`.
  - **Subsidiary conflict**: a subscription's `subsidiary` must match the customer's NS subsidiary.
    B&M Mchugh (cust 19169) is in subsidiary **13** but the CSV sends **12** → NS 400. (Escalated to
    client; still open.)
  - **BA `customer_externalId` keyed wrong**: warehouse sent HubSpot IDs (`MP_HubSpot_…`) instead of
    the loaded customer `External ID 2`. Had to remap.
  - **Blank BA `startDate`** → NS defaults to today → subscription start-date < BA start-date → 400.
    BA `startDate` must be ≤ the earliest sub start date for that deal.
  - **Sales item `NOT MAPPED`** (e.g. HubSpot line `456966763758` "NextGen Employee Assistance
    Programme" → NS item **5580** "Employee Assistance Program"). Loader skips `NOT MAPPED` lines.
  - **Shared billing account per deal**: one BA per *deal*, keyed `<deal_id>_BA` where
    `deal_id = sub External ID before the first underscore` (e.g. `494812626113_a_27397` and
    `494812626113_27397` → both share `494812626113_BA`). Already implemented in `subscription.py`.
  - **Address dependency**: BA load needs each customer to have a **default billing AND default
    shipping** address in NS, else that BA is skipped.

---

## 2. Current architecture (inventory of what changes)

```
config.py            LOAD_REVISION, apply_revision(), STATE_DB (NS_STATE_DB env)
state_tracker.py     SQLite: load_state + run_log tables  ← TO BE REMOVED/REPLACED
netsuite_client.py   suiteql_query(), get_by_external_id(), create_and_resolve_id(), patch_record()
loaders/base.py      load_all(): is_already_loaded() skip + upsert_state(); prepare_records()
loaders/customer.py
loaders/billing_account.py   resolves customer via tracker.get_netsuite_id("customer", rev(ext))
loaders/price_plan.py        _reconcile_existing_plans(): ALREADY the live-NS pattern (the model)
loaders/subscription.py      resolves customer + BA + pricePlan via tracker.get_netsuite_id(...)
loaders/one_off.py           resolves customer via tracker.get_netsuite_id(...)
main.py              orchestration + reporting (tracker.summary/get_failed/get_missing_ids)
scripts/preflight_revision_load.py   read-only preflight (partial; crashes on missing one-off CSV)
prod/seed_state.py                   manually seeds state DB  ← obsolete after WS2
prod/prod_check.py                   read-only NS inspectors (keep; prints STATE_DB path only)
scripts/retrofit_subscription_pricing.py   uses tracker.conn directly  ← one-off; see §7
scripts/deactivate/deactivate_prior_revision.py   parked; references list_successful (unimpl.)
repair_customer_addresses.py         uses tracker.conn directly  ← one-off; see §7
```

### Every state-DB touch point (must all be addressed)
- `loaders/base.py:104` `is_already_loaded()` (skip logic)
- `loaders/base.py:133` `upsert_state()` (record result)
- `loaders/base.py:151` `start_run()/finish_run()` (run_log)
- `loaders/billing_account.py:161` `get_netsuite_id("customer", …)`
- `loaders/billing_account.py:255` `get_netsuite_id("billingAccount", …)` (in `patch_startdates`)
- `loaders/subscription.py:188` customer · `:208` billingAccount · `:543` pricePlan lookups
- `loaders/one_off.py:128` `get_netsuite_id("customer", …)`
- `loaders/customer.py:297,405` `get_netsuite_id`/state for `--patch` and EER linkage
- `loaders/price_plan.py:45,84,93` `_reconcile_existing_plans()` (the pattern to generalize)
- `main.py:604,614,623,771,903` reporting via `tracker.summary/get_*`
- Scripts: `preflight_revision_load.py`, `prod/seed_state.py`, `repair_customer_addresses.py`,
  `scripts/retrofit_subscription_pricing.py`, `scripts/deactivate/deactivate_prior_revision.py`

---

## 3. Target architecture

### 3.1 `NSIndex` — in-memory replacement for the state DB
New module `ns_index.py`. Built once at the start of a run; **queries NS, holds maps in memory,
discarded at exit.** No persistence.

```
class NSIndex:
    def __init__(self, client): ...
    # Bulk-load externalId → internalId maps for an entity, scoped to needed IDs.
    def load(self, entity_type, external_ids=None, like_pattern=None): ...
    def get_id(self, entity_type, external_id) -> str | None      # replaces tracker.get_netsuite_id
    def exists(self, entity_type, external_id) -> bool            # replaces tracker.is_already_loaded
    def add(self, entity_type, external_id, internal_id): ...     # update after a successful POST
```

**How maps are built (per entity) — efficient, scales, no "pull everything":**
- Query NS for **exactly the externalIds we care about**, via SuiteQL `WHERE externalid IN (...)`,
  **chunked** (≤~200 per IN to stay under SQL length limits), paginated via existing
  `suiteql_query()`.
- For price plans, the existing `LIKE 'MP\_PP\_%<rev>' ESCAPE '\\'` pattern also works (already in
  `price_plan.py`).
- The set of externalIds to query is derived from the CSVs at startup (customer extIds referenced
  by BAs/subs, BA extIds, pricePlan extIds, sub extIds). All carry the revision suffix.

**Idempotency** then = `if nsindex.exists(entity, ext): skip`. NS already prevents duplicates on
re-POST, so this is purely to avoid wasted calls and to skip immutable records (subscriptions).

### 3.2 Reporting without SQLite
- Run results accumulate **in memory** during `load_all()` (a list of per-record outcomes).
- At end of run: print the summary (as today) **and** write a per-run results CSV under
  `logs/YYYY-MM-DD/results_HH-MM-SS.csv` (entity, externalId, status, ns_id, error).
- `--report` becomes a **live NS report**: query NS by revision-suffixed externalId patterns and
  count what actually exists, instead of reading SQLite. Failures come from the latest results CSV +
  logs (NS never stored our failed attempts anyway).

### 3.3 What happens to `LOAD_REVISION`
- **Keep it** as the externalId suffix (still useful to namespace a deliberate fresh generation and
  to scope reconciliation `LIKE` queries). 
- **Drop** the machinery that existed only to manage *state-DB* keys (collision pre-checks, the
  "don't bump or you orphan parents" caveat is irrelevant once parents are resolved live from NS by
  their own revisioned externalId — same rule, but now there's no second store to fall out of sync).
- Decision to confirm in session: do we ever bump it again, or freeze at `_rvn_prod_01`? (See §8.)

---

## 4. WS1 — Pre-load validator (DO FIRST)

New module `validate.py` + `--validate` flag (and/or standalone `python validate.py`). **Read-only.**
Runs all checks, prints a single grouped report, exits non-zero if any blocker found. Must be
runnable per-entity and for the whole pipeline.

### 4.1 Behaviour
- Loads the active CSVs (`config.*_CSV`) and queries NS read-only (reuse `NSIndex` from WS2, or
  standalone SuiteQL if WS1 ships first).
- Reports **every** problem found, grouped by check, with the offending row identifiers — never
  stops at the first.
- Distinguishes **BLOCKER** (would fail/skip the load) from **WARNING** (informational, e.g. line at
  book default).
- Exit code 0 = clean, 1 = blockers present.

### 4.2 Check catalog (each maps to a real failure we hit)
| # | Check | Blocker? | Catches |
|---|---|---|---|
| 1 | Every sub `Customer` resolves to a loaded NS customer (with alias map) | Yes | name mismatch trio |
| 2 | Every BA `customer_externalId` matches a customer in NS by `External ID 2` | Yes | HubSpot-ID key bug |
| 3 | Each sub's `subsidiary` == its customer's NS subsidiary (`customersubsidiaryrelationship`) | Yes | B&M Mchugh 12-vs-13 |
| 4 | For each deal: BA `startDate` ≤ earliest sub `Start Date` (and BA startDate not blank) | Yes | Zebra blank-date |
| 5 | Every active sub line `Sales Item` is mapped (not blank / not `NOT MAPPED`) | Warn→Yes | EAP NOT MAPPED |
| 6 | BA↔sub deal alignment: every sub deal has a BA row & vice-versa (deal_id = before first `_`) | Warn | Care Shield gap |
| 7 | Every sub line `Price Plan External ID` exists in NS (revisioned) | Warn | book-default lines |
| 8 | Each BA's customer has default **billing AND shipping** address in NS | Yes | BA skip-on-no-address |
| 9 | BA `subsidiary_id` / `currency_id` present (no blank required fields) | Yes | silent-skip guard |
| 10 | externalId shape sanity (BA = `<deal_id>_BA`, sub carries deal prefix) | Warn | convention drift |

- The **alias map** (check 1/2) lives in one reviewed place (e.g. `config.CUSTOMER_NAME_ALIASES`).
  Seed it with the known trio. This is a *reviewed* mapping, not a silent guess — keep it explicit.
- Address check reuses the `customeraddressbook` query already in `billing_account.py`.

### 4.3 Deliverables
- `validate.py` with the 10 checks, grouped report, exit codes.
- `--validate` wired into `main.py` (per `--entity` and all).
- Replace/retire `scripts/preflight_revision_load.py` (fold its collision/orphan logic in; fix the
  hard crash when `ONEOFF_CSV` file is missing — guard missing CSVs).

---

## 5. WS2 — Remove persistent state DB

### Phase A — Introduce `NSIndex`, leave SQLite in place (parallel, low risk)
1. Write `ns_index.py` (§3.1) + unit-ish checks against prod read-only.
2. Add chunked `WHERE externalid IN (...)` helper to `netsuite_client.py` (or inside NSIndex).
3. Build NSIndex at start of each loader run; **log** map sizes (sanity vs CSV counts).

### Phase B — Switch resolution + skip logic to NSIndex
4. `base.py`: replace `is_already_loaded()` → `nsindex.exists()`; after a successful POST call
   `nsindex.add(entity, ext, ns_id)` so within-run dependents resolve.
5. `billing_account.py`: customer lookup → `nsindex.get_id("customer", rev(ext))`; the
   `patch_startdates` BA lookup → `nsindex.get_id("billingAccount", rev(ext))`.
6. `subscription.py`: customer / billingAccount / pricePlan lookups → `nsindex.get_id(...)`.
7. `one_off.py`: customer lookup → `nsindex.get_id("customer", rev(ext))`.
8. `customer.py`: `--patch` / EER paths → resolve via NSIndex / `get_by_external_id`.
9. `price_plan.py`: replace `_reconcile_existing_plans()` (which seeded SQLite) with NSIndex
   population (same SuiteQL, no SQLite write).

### Phase C — Reporting off NS + results CSV
10. `base.py.load_all()`: accumulate per-record outcomes in memory; write `results_*.csv`; drop
    `start_run/finish_run/upsert_state`.
11. `main.py`: `--report`/`--failures` rebuilt — counts from a live NS query by revisioned
    externalId pattern; failures from the latest results CSV.

### Phase D — Delete the SQLite layer
12. Remove `state_tracker.py`; remove `StateTracker` imports; remove `config.STATE_DB`/`NS_STATE_DB`
    requirement (or keep the env var as a no-op for one release with a deprecation log line).
13. Retire `prod/seed_state.py` (only existed to populate SQLite).
14. Update the two scripts that read `tracker.conn` directly — `repair_customer_addresses.py` and
    `scripts/retrofit_subscription_pricing.py` — to query NS via SuiteQL/NSIndex instead (see §7).
15. `prod/prod_check.py`: drop the "State DB" line from the env banner.

### Phase E — Docs
16. Update `CLAUDE.md` (remove State Tracker section, revise Load Order/ID-resolution narrative,
    revise the `LOAD_REVISION` section), `README.md`, `TODO.md`.

---

## 6. Rollout / sequencing

1. **WS1 first**, shipped and used on the next real load — immediate value, zero risk (read-only).
2. **WS2 Phase A–B behind a flag** (`--use-ns-index`) so we can run old (SQLite) and new (live) side
   by side and diff the resolved IDs on a real batch before committing.
3. Flip default to NSIndex once a full batch matches; then Phase C–E to delete SQLite.
4. Keep a tagged git commit before deleting `state_tracker.py` so rollback is trivial.

---

## 7. One-off scripts that read the DB directly (decide per-script)
- `scripts/retrofit_subscription_pricing.py` — built against `_rvn_01`, not revision-aware; uses
  `tracker.conn`. **Decision:** likely retire, or port its NS lookups to SuiteQL if still needed.
- `repair_customer_addresses.py` — uses `tracker.conn` to list customer ext→ns_id. Port to NSIndex
  or a direct SuiteQL `customer` query.
- `scripts/deactivate/deactivate_prior_revision.py` — parked, references unimplemented
  `list_successful`; rebuild on a live NS query (`WHERE externalid LIKE '%<old_rev>'`) if ever used.

---

## 8. Open decisions to settle at the start of the session
1. **`LOAD_REVISION` future:** freeze at `_rvn_prod_01`, or keep bumping per batch? (Affects whether
   reconciliation uses `IN(...)` of exact IDs vs `LIKE '%<rev>'`.) — *Recommendation: keep the
   suffix, query by exact externalId sets, only bump for a deliberate new generation.*
2. **Validator strictness on check 5/6/7** (warning vs blocker) — *Recommendation: 5 blocker, 6/7
   warning.*
3. **Ship WS1 standalone first** (its own NS queries) or **wait for NSIndex** and build on it. —
   *Recommendation: ship WS1 first with its own light queries; refactor onto NSIndex in WS2 Phase B.*
4. **Keep `NS_STATE_DB` env** as deprecated no-op for one release, or remove outright?

---

## 9. Testing strategy
- **WS1:** run `--validate` against the current `subs-kleene-export-2026-06-15*.csv` + BA CSV; it
  must flag exactly the known issues (B&M subsidiary, any name drift) and pass the clean ones.
- **WS2 Phase B:** dual-run with `--use-ns-index` and assert NSIndex-resolved parent IDs ==
  SQLite-resolved IDs for every record in a real batch (the 13 BAs / 15 subs make a good fixture).
- **Regression:** the Zebra `27397a` reload path (BA + 2 price plans attached) is a good end-to-end
  smoke test; verify with `prod/prod_check.py subscription --externalid …` (now fixed to read the
  priceInterval sublist).
- Always run read-only checks (`prod_check.py whoami`, `--validate`) before any POST.

---

## 10. Definition of done
- [ ] `validate.py` exists; `--validate` runs all 10 checks, exits non-zero on blockers, used before loads.
- [ ] `ns_index.py` exists; all parent resolution + skip logic goes through it.
- [ ] No code imports `StateTracker`; `state_tracker.py` deleted; `prod/seed_state.py` retired.
- [ ] `--report`/`--failures` rebuilt from live NS + results CSV.
- [ ] One-off scripts ported or retired (§7).
- [ ] `CLAUDE.md`, `README.md`, `TODO.md` updated; no stale State-Tracker references.
- [ ] A real batch loads via `validate → load` with no manual state surgery.

---

## 11. First-session kickoff checklist (paste-ready)
1. `source .venv/bin/activate && source .env.prod && python prod/prod_check.py whoami`
2. Re-read this file + `CLAUDE.md` §"State Tracker" / "Load Revisions".
3. Settle the §8 decisions.
4. Build WS1 `validate.py` → run it against the active CSVs → confirm it flags the known issues.
5. Then WS2 Phase A (`ns_index.py`) behind `--use-ns-index`; dual-run + diff on the 13 BA / 15 sub batch.
6. Proceed Phase B→E only after the dual-run matches.
