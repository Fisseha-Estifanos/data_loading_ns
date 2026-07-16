# NetSuite Data Loader — MoorePay HubSpot → NetSuite Migration

**This file is the single source of truth for how to run the pipeline** (sandbox
*and* production). If you're looking for "what steps do I run, in what order" —
it's all here.

- Domain context, field mappings, data landmines → [`CLAUDE.md`](CLAUDE.md)
- Outstanding task list → [`TODO.md`](TODO.md)
- In-flight refactor (remove the state DB) → [`refactors/REFACTOR-PLAN.md`](refactors/REFACTOR-PLAN.md)

---

## The one idea: sandbox and prod are the *same* pipeline

There is **no code difference** between loading to sandbox (`4874529-sb3`) and
production (`4874529`). [`config.py`](config.py) derives the endpoint URL, realm,
and state DB entirely from environment variables. **Switching environments =
sourcing a different env file.** Everything below runs identically in both; the
only env-specific realities are called out in [§ Sandbox vs Prod](#sandbox-vs-prod).

---

## ⛔ Data Integrity Rule — never modify source data

The CSVs are produced by Snowflake and are the authoritative source of truth.
The loader must **never** silently alter, default, or invent values.

- A required field that is missing or unmapped must **fail with a logged error
  and skip the record** — never substitute a default (`SUBSIDIARY_MAP.get(name,
  "12")` is wrong — drop the default).
- No reformatting/normalising of values before sending to NS.
- The only permitted touches: `.strip()` whitespace, and `"" → None` to omit a
  field rather than send an empty string.

---

## One-time setup

```bash
pip install requests          # the only dependency
```

Two env files, **never committed** (covered by `.gitignore` `.env*`):

```bash
# .env  — SANDBOX  (creds from 1Password)
NS_CONSUMER_KEY=...  NS_CONSUMER_SECRET=...  NS_ACCESS_TOKEN=...  NS_TOKEN_SECRET=...
NS_REALM=4874529-sb3
NS_STATE_DB=state/load_state.db

# .env.prod  — PRODUCTION  (creds from 1Password "Netsuite2 Prod - Moorepay")
NS_CONSUMER_KEY=...  NS_CONSUMER_SECRET=...  NS_ACCESS_TOKEN=...  NS_TOKEN_SECRET=...
NS_REALM=4874529                       # NOT -sb3
NS_STATE_DB=state/load_state_prod.db   # separate DB — see § Sandbox vs Prod #2
```

`NS_REALM` and `NS_STATE_DB` are **required** — `config.py` raises if either is unset.

**Prod permissions (once):** the prod integration role needs **Full** access on
the SuiteBilling lists — Price Plans, Price Books, Subscriptions, Subscription
Plans, Billing Accounts — or you get `403 INSUFFICIENT_PERMISSION` on the
price-plan/subscription loads. Get Adam to grant these up front.

---

## The standard load sequence

One pipeline, two environments — the only difference is which env file you source
in **Step 1** (plus the prod-only extras in [§ Sandbox vs Prod](#sandbox-vs-prod)).
Run everything from the repo root (`data_loading/`).

**Pricing prep, in order** — when the Snowflake exports are bulk dumps rather than
batch-scoped:

```text
batch-scoped pricing CSV → filter_to_subs.py → dedup_price_plans.py → main.py --validate → load
```

`filter_to_subs.py` (Step 4) trims every dependency CSV to this batch;
`dedup_price_plans.py` (Step 5) then collapses what remains by pricing shape;
`--validate` (Step 6, incl. the pricing-coverage gate) must pass before you load.

### Step 1 — Choose the environment

The only sandbox-vs-prod switch. Sourcing an env file sets the credentials, realm,
and state DB.

```bash
source .env          # sandbox (4874529-sb3)
# …or…
source .env.prod     # production (4874529)
```

### Step 2 — Confirm the target account

A read-only sanity check before anything touches NetSuite.

```bash
python prod/prod_check.py whoami     # prints SANDBOX or PRODUCTION + account id
```

### Step 3 — Point config at this batch's exports

Edit [`config.py`](config.py): set the `*_CSV` paths to the active Snowflake
exports, and set `LOAD_REVISION` (the externalId suffix appended to every record;
currently `_rvn_prod_01`). No command — just the edit.

### Step 4 — *(Optional)* Trim dependency CSVs to this batch

The customer / pricePlan / billing exports are often bulk dumps with far more rows
than the subs reference. Trimming avoids loading unwanted records and silences
validator findings for rows the subs never touch. Each file is rewritten in place
(timestamped `.bak` saved); the subs CSV is never modified.

```bash
python filter_to_subs.py --dry-run    # preview what would be removed
python filter_to_subs.py              # apply (backs up originals)
```

### Step 5 — *(Optional)* De-duplicate price plans

A NS price plan has no item binding, so plans with identical
currency / type / tiers / min-max are the *same* plan — the item slug in the
externalId is just a label. The full price book is heavily duplicated (e.g. ~20k
rows → ~3k distinct shapes). This collapses the pricing CSV to one row per distinct
shape (item-free externalId `MP_PP_<hash>`) and re-points the subs'
`Price Plan External ID` to match. Both files rewritten in place; idempotent.

```bash
python analyze_priceplan_dedup.py --full   # report duplication (read-only, no creds)
python dedup_price_plans.py --dry-run       # preview the rewrite
python dedup_price_plans.py                 # apply
```

> Interim stopgap. The proper fix moves the same shape-hashing into the pricing +
> subs DDLs (and the loaders).

### Step 6 — Validate

Read-only; reports every data problem at once — name/key mismatches, subsidiary
conflicts, blank/late BA start dates, unmapped sales items, missing addresses,
price-plan coverage. **Run before every load and resolve every ✗ first.** Scope to
one entity with `--entity`.

```bash
python main.py --validate
python main.py --validate --entity subscription   # scope to one entity
```

When subscriptions are in scope, `--validate` also runs the pricing-coverage gate
and fails if any referenced price plan is missing from the pricing CSV. You can run
that gate standalone (no NS creds needed):

```bash
python check_pricing_coverage.py
```

A coverage FAIL usually means the pricing export was generated **unscoped** (the
full price book, capped ~20k rows) instead of scoped to this batch — re-run the
pricing DDL scoped to the batch (`filter_to_subs.py` can't fix it).

#### The validation checks at a glance

**Blockers** exit non-zero and must be resolved before loading; **warnings** are
informational and the load proceeds. *Reads* shows whether the check queries
NetSuite (`NS`), the CSVs, or both. The report groups them under the banners shown
in the *Group* column.

| # | Check | Severity | Reads | Group | What it verifies (and why it matters) |
| --- | --- | --- | --- | --- | --- |
| 1 | Sub customer resolves to an NS customer | Blocker | NS + CSV | Customers | Each subscription's customer resolves in NS by **C-number ONLY** (subs `NETSUITE_ACCOUNT_NUMBER*`, else the customer CSV's `C-number` by name) — the only key the loaders attach by. Also warns when the resolved NS customer's NAME doesn't match (C-number points at the wrong company). |
| 2 | Subs export's own C-number is valid in NS | Blocker | NS | Customers | The C-number the subs file itself carries (`NETSUITE_ACCOUNT_NUMBER*`) actually exists in NS — catches a wrong/stale account number even when check 1 resolved the customer another way. Silent if the subs carry no C-number. |
| 3 | Already in NS (skip) vs must create (load) | Warning | NS | Customers | Per customer: already in NS → skip loading; not in NS → must be created first. Your load-vs-skip roster. |
| 4 | Truly absent vs in NS under a different id | Warning | NS | Customers | For customers check 3 flagged: genuinely missing (create) vs already in NS under a mismatched id (reconcile — don't create a duplicate). |
| 5 | BA customer resolves to an NS customer | Blocker | NS + CSV | Billing accounts | Each BA's customer resolves by **C-number ONLY** — sourced from its owning sub's `NETSUITE_ACCOUNT_NUMBER*` (via the `<sub>_BA` key), else the customer CSV's C-number bridged from `customer_externalId`. |
| 5.1 | BA subsidiary matches its customer | Blocker | NS + CSV | Billing accounts | Each BA's `subsidiary_id` equals the NS subsidiary of the customer it binds to — a mismatch is a hard NS 400 (`Invalid Field Value ... subsidiary`). |
| 6 | Every sub has its own `<deal>_<subid>_BA` | Blocker | NS + CSV | Billing accounts | Each **sub** has its own Billing Account keyed `<sub External ID>_BA` (the `<deal>_<subid>_BA` convention) — already in NS, or one queued in the billing CSV. **Blocks** when **neither**: a subscription is NEVER loaded without its BA — the loader refuses it (NS would otherwise silently attach the customer's *default* BA). A sub links to its own `<sub>_BA`, so the customer's *other* BAs — even a sibling sub's — don't count. |
| 7 | Sub subsidiary == NS customer's subsidiary | Blocker | NS + CSV | Subscriptions | The sub's Subsidiary equals its NS customer's subsidiary. A mismatch is a hard NS 400 at load. |
| 8 | BA startDate <= earliest sub Start Date | Blocker | CSV | Billing accounts | Each BA's startDate is present and on/before its earliest subscription start (a blank/late start breaks the subs it bills). |
| 9 | Active sub line Sales Item is mapped | Blocker | CSV | Subscriptions | Every included sub line has a real Sales Item (not blank / not `NOT MAPPED`) — unmapped lines load silently short. |
| 9.1 | Every included line carries a Price Plan External ID | Blocker | CSV | Price plans | The plan record holds **all** the pricing (currency, min/max, price tiers); a line with no plan bills at the **£0** book-default placeholder. **Blocks**: the loader refuses to load a sub with unpriced lines (mirror of the mandatory-BA gate, check 6). Applies the loader's same-item backfill first, so a blank cell covered by a sibling row doesn't flag. |
| 10 | Sub line Price Plan exists in NS | Warning | NS | Price plans | Each line's price plan already exists in NS, else the line falls back to the £0 book default. |
| 11 | Sub line Price Plan exists in the pricing CSV | Warning | CSV | Price plans | Each line's price plan exists in the pricing CSV (i.e. *can* be created). Missing from both NS and CSV = no source row to push. |
| 12 | BA customer has default billing AND shipping address | Blocker | NS | Billing accounts | Each BA's customer has both a default billing and shipping address in NS — the BA loader needs both or it skips the BA. |
| 13 | BA required fields present | Blocker | CSV | Billing accounts | Each BA has `subsidiary_id` and `currency_id` filled (mandatory NS references). |
| 14 | externalId shape sanity (`<deal>_<subid>_BA`) | Warning | CSV | Billing accounts | Each BA externalId ends in `_BA` **and** its prefix is a sub External ID in the subs CSV — a BA whose prefix matches no sub would load but never be linked. |

### Step 7 — Decide what to load

The validate report tells you, per customer, exactly what to load vs skip.
Customers resolve to NS by **C-number ONLY** (the subs' `NETSUITE_ACCOUNT_NUMBER*`,
else the customer CSV's `C-number` column bridged by name) — never by External ID 2
or company name (convention since 2026-07-09). A customer this loader CREATES gets
an NS-assigned C-number the exports don't know yet: create it, then **re-export so
the subs carry its C-number**, or its subs/BAs cannot attach.

| Report check | Tells you | Action |
| --- | --- | --- |
| **Check 3** — already in NS vs must create | Which customers exist in NS (skip) vs which don't | Load only the customers flagged **NOT in NS** |
| **Check 4** — truly absent vs different id | Whether a flagged customer is genuinely missing or has a mismatched id | Create the truly-absent ones; reconcile the id for the rest (don't duplicate) |
| **Check 6** — every sub has its own `<sub>_BA` | Which subs already have their `<sub External ID>_BA` vs which have none (those subs are **blocked** — subs never load without a BA) | Load the `<sub>_BA` rows in the billing CSV; fill genuine per-sub gaps with `--create-missing-bas` in Step 8 — **before** the subscription load |

### Step 8 — Load in dependency order

Dry-run each entity, then load it. `python main.py` with no `--entity` runs all
five in dependency order automatically; the pairs below just add a checkpoint per
entity.

```bash
# Customers
python main.py --dry-run --entity customer && python main.py --entity customer
python main.py --entity customer --patch-eer        # link Electronic Email Recipients

# Billing accounts (from the billing CSV)
python main.py --dry-run --entity billingAccount && python main.py --entity billingAccount
```

For any **sub** whose `<sub External ID>_BA` is **neither** in NS **nor** in the
billing CSV (flagged by Check 6), the BA loader can synthesize one from the
customer's NS address book + the subscription — one `<sub>_BA` per gap sub,
resolving the customer by C-number, no seeding:

```bash
python main.py --dry-run --entity billingAccount --create-missing-bas   # preview the BAs
python main.py --entity billingAccount --create-missing-bas             # create them
```

> A deal skipped for "no default billing/shipping address" needs its customer
> address added first: `python repair_customer_addresses.py`.

```bash
# Price plans, subscriptions, one-offs
python main.py --dry-run --entity pricePlan    && python main.py --entity pricePlan
python main.py --dry-run --entity subscription && python main.py --entity subscription
python main.py --dry-run --entity oneOff       && python main.py --entity oneOff
```

> `--dry-run` works for **every** entity (it builds and logs the payloads, no API
> calls). For subscriptions it previews the **header** payloads only; the live run
> additionally activates each line and repoints its price interval at the loaded
> price plan (see gotcha #5) — those steps don't appear in the dry-run.

### Step 9 — Verify

```bash
python main.py --report --failures
python prod/prod_check.py sub-intervals --sub-id <ID>   # confirm the price landed on the interval
```

### Step 10 — *(If needed)* Fix a bad subscription load (delete + reload)

NetSuite upserts by externalId, so re-running the loader will **not** repair a
subscription that loaded **without its own billing account** or **bound to the
wrong customer** — the record must be deleted and re-created.
`scripts/cleanup_unbilled_subscriptions.py` finds those subs against live NS,
and (with `--execute`) deletes them + clears their state row so the next
subscription load re-creates them cleanly. Read-only by default; refuses to
`--execute` against prod without `--allow-prod`.

```bash
# Preview subs with no billing account (read-only):
python scripts/cleanup_unbilled_subscriptions.py

# Also catch subs bound to the wrong customer, then delete + un-gate for reload:
python scripts/cleanup_unbilled_subscriptions.py --name-mismatch
python scripts/cleanup_unbilled_subscriptions.py --name-mismatch --execute
```

| Flag | Effect |
| --- | --- |
| *(none)* | Scan the subs CSV; flag subs missing a billing account. |
| `--all-revision` | Scan every NS subscription under the current `LOAD_REVISION`, not just the CSV. |
| `--ext-id EXT` | Target a specific raw sub externalId (repeatable); always a candidate. |
| `--name-mismatch` | Also flag subs whose bound NS customer name mismatches the CSV. |
| `--keep-missing-ba` | Don't treat a missing billing account as a reason (pair with `--name-mismatch`). |
| `--execute` | Actually delete (default is a preview). |
| `--allow-prod` | Permit `--execute` against a production account. |

Then fill the billing-account gaps and reload:

```bash
python main.py --dry-run --entity billingAccount --create-missing-bas
python main.py --entity billingAccount --create-missing-bas
python main.py --entity subscription
```

### Load order & dependencies (non-negotiable)

```text
1. Customer              (no dependencies)
2. Billing Account       ← references Customer NS internal ID
3. Price Plan            (no NS parent deps — refs sales items by name only)
4. Subscription          ← references Customer + Billing Account (MANDATORY) + Price Plan (per line)
5. One-Off Invoice       ← references Customer NS internal ID
```

BA / pricePlan parents are resolved from the state DB; **customers are not** —
see the attach flow below. If a parent can't be resolved, the child is skipped
with a logged error.

### How records attach (C-number convention, since 2026-07-09)

One rule: **a customer is only ever found by its C-number (NS `entityid`) —
nothing else.** No externalId lookups, no company-name matching against NS.
Names / `External ID 2` / `customer_externalId` exist in the CSVs only as
bridges *to* a C-number.

```text
subscription → customer   by C-number: subs NETSUITE_ACCOUNT_NUMBER
                          → NETSUITE_ACCOUNT_NUMBER_COMPANY_LEVEL
                          → customer CSV `C-number` (bridged by Customer name)
billing acct → customer   by C-number: its OWNING SUB's C-number
                          (BA externalId minus `_BA` → sub → columns above)
                          → else customer CSV `C-number` (via customer_externalId)
subscription → billing    by externalId KEY (not a customer lookup):
                          `<sub External ID>_BA` (= `<deal>_<subid>_BA`)
```

**A subscription is NEVER loaded without its billing account.** The loader
resolves `<sub External ID>_BA` from the state DB, then live NS; if neither has
it, the sub is refused (error + skip, with a summary of every blocked sub) —
because loading without a BA doesn't leave the sub unbilled, it makes NS
silently attach the customer's *default* BA (an arbitrary/wrong one). Load the
BA first, then the sub. Validate check 6 blocks on exactly the same condition.

**A subscription is NEVER loaded with unpriced lines** (rule since 2026-07-15).
All pricing — currency, min/max, price tiers — lives in the **price plan** record;
a sub line only references it via `Price Plan External ID`. A line with no plan
bills at the plan's price-book default, and Moorepay's standard books are **£0
placeholders** — so an unpriced line silently creates a £0 line (the April 2026
loads did exactly this and needed the retrofit backfill). The loader refuses any
sub with an included line whose plan id is blank (after same-item backfill), with
a summary of every blocked sub. Validate check 9.1 and `check_pricing_coverage.py`
block/fail on the same condition pre-load.

Because the BA takes its C-number from its own sub, a sub and its BA can never
bind to different customers. `validate.py` mirrors this exactly (checks 1–5.1),
and check 5.1 blocks a BA whose CSV `subsidiary_id` differs from its resolved
customer's NS subsidiary (a hard NS 400 otherwise).

**The loop for NEW customers:** a customer created by this loader gets an
NS-assigned C-number the exports don't know yet, so its subs/BAs cannot attach
until the warehouse knows it. The flow is:

```text
load customer → warehouse picks up its NS C-number → re-export subs
→ --validate (checks 1–4 confirm resolution) → load BA / pricePlan / subscription
```

A record with no resolvable C-number is **skipped with an error** — never
attached by any other key (see the data-integrity rule).

---

## Sandbox vs Prod

The pipeline is identical. These are the only environment-specific realities:

| # | Aspect | Sandbox | Production |
| --- | --- | --- | --- |
| 1 | **Env file** | `source .env` | `source .env.prod` |
| 2 | **State DB** | `state/load_state.db` | `state/load_state_prod.db` — **never cross them.** The DB stores NS *internal* IDs that only mean something in their own account; a sandbox DB used against prod would wire prod records to sandbox internal IDs |
| 3 | **Customers** | Loaded fresh (`--entity customer`) | **Already exist** in NS. Do **not** load them — the BA/subscription loaders resolve them by **C-number** straight from NS (no seeding), so just leave them out of the customer CSV and load BA / pricePlan / subscription on top |
| 4 | **Addresses** | Loaded with the customer | Prod customers may lack a default billing+shipping address; the BA load *skips* without one. Check and patch first (see below) |
| 5 | **Permissions** | Usually open | Role needs SuiteBilling Full grants (see setup) |

### Prod-only extra steps (when the customer already exists)

Pre-existing customers are now resolved **by C-number straight from NS** (no
seeding) — so a prod load needs nothing special for *resolution*; just leave those
customers out of the customer CSV (don't load them). The only prod-specific care
is **addresses**: a billing account needs the customer to have a default
billing + shipping address, so check and patch first, **per deal**:

```bash
# Full readiness check for the deal (customer exists? BA? price plan/item resolve?)
python prod/prod_check.py sub-readiness --deal <sub_externalId> --c-number <C-number>

# Ensure the customer has a default billing + shipping address (BA load needs it)
python prod/prod_check.py address --customer-id <internal_id>
python prod/patch_customer_address.py --customer-id <id> --from-csv --company-name "<NAME>"          # dry-run
python prod/patch_customer_address.py --customer-id <id> --from-csv --company-name "<NAME>" --apply
```

Then run Step 8 **without** `--entity customer` (they already exist in NS).

> **externalId match (gotcha):** the subscription looks for billing account
> `<sub externalId>_BA`. If the sub externalId is `495006175463_27396`, the
> billing CSV `externalId` must be `495006175463_27396_BA` — edit the cell if the
> export emits just `495006175463_BA`.

---

## Five gotchas worth knowing

1. **Separate state DB per environment** — see table #2 above. The #1 way to
   corrupt a prod load.
2. **The customer often already exists in prod.** Don't re-create it — leave it
   out of the customer CSV; the BA/subscription loaders resolve it by C-number.
3. **The billing account needs the customer to have a default address.** The BA
   loader pulls `billAddressList`/`shipAddressList` from the customer's default
   billing+shipping address; no address → BA skipped.
4. **externalIds must match between exports** — sub `X` looks for BA `X_BA`.
5. **The price does NOT live on the subscription line — it lives on the
   subscription's `priceInterval`.** NS auto-creates one interval per line (copied
   from the plan's price book, £0 for Moorepay's standard books). The loader
   (`loaders/subscription.py` → `_activate_subscription_lines`) includes the line
   then **repoints its price interval** at our loaded price plan. PATCHing
   `subscriptionLine.pricePlan` does nothing (NS 204s and ignores it). A freshly
   loaded sub is in `DRAFT`: `recurringAmount` shows the price immediately;
   `totalintervalvalue` / contract value compute only on activation.

---

## CLI reference — `main.py`

| Flag | Values | Description |
| --- | --- | --- |
| `--entity` | `customer` `billingAccount` `pricePlan` `subscription` `oneOff` | Load only this entity. Omit to run all five in dependency order. |
| `--validate` | — | Read-only pre-load validator. Reports every data problem at once; exits non-zero on blockers. Respects `--entity`. **Run before every load.** |
| `--dry-run` | — | Build and log payloads without any API calls. |
| `--limit N` | integer | Process only the first N records (test a single POST). |
| `--report` | — | Print the load-state summary (counts per status per entity). Scoped to the active `LOAD_REVISION`. |
| `--failures` | — | Add failure details to `--report`. Use with `--report`. |
| `--all-revisions` | — | Make `--report` span all revisions instead of just the active one. |
| `--field-map` | — | Print the CSV column → NS API field mapping for all loaders. No creds needed. |
| `--skip-preflight` | — | Skip the auth connectivity check at startup. |
| `--patch` | — | **Retroactive only.** PATCH already-loaded customers with custom fields. Not needed for new loads (fields are in `build_payload()`). Customer only. |
| `--patch-eer` | — | Link `custentity_zellis_elec_email_recipients` (two-step POST+PATCH). Run after `--entity customer`. |
| `--patch-ba-startdate` | — | PATCH billing-account startDates from the CSV where they differ from NS. Use with `--entity billingAccount`. |
| `--create-missing-bas` | — | Create a `<sub External ID>_BA` for each sub that has none (its `<sub>_BA` is neither in NS nor in the billing CSV) from the customer's NS address book + the subscription. Honours `--dry-run`. Use with `--entity billingAccount`. |

## Read-only inspector — `prod/prod_check.py`

Never writes to NS (GET / SuiteQL only). Works against whichever account your env
points at — use it freely before and after any load.

```text
whoami                                          which NS account am I pointed at?
customer    --entityid C43837 | --externalid X | --name "ACME"
billing     --externalid <ext> | --customer-id <id>
item        --name "Employment Law - AL"        does the sales item exist?
externalid  --type pricePlan --id <externalId>  GET any record by externalId
sub-readiness --deal <dealId> --c-number C43837 full pre-load check for a deal
address     --customer-id <id>                  default bill+ship address present?
subscription --id <id> | --externalid <ext>     dump a sub's lines
record      --type <t> --id <id> | --externalid <ext>   raw GET + JSON dump
subline     --sub-id <id> --line 1              raw dump of one line
sub-intervals --sub-id <id>                     dump price intervals (where prices live)
```

---

## Scripts & tooling — the full inventory

Every script in the repo, grouped by whether it is **part of the current load
pipeline** or **stale / one-off** (superseded, served-its-purpose, or parked).
Paths are repo-relative. "Run" commands assume your env is sourced (`source .env`
or `.env.prod`) and you are in the repo root.

> Legend: 🟢 **Active** — used (or usable) in a current load. 🗄️ **Stale/one-off**
> — not part of any current flow; kept for history or future revival, several are
> safe to delete. If you add or retire a script, update this table **and** the
> standard load sequence above in the same change.

### 🟢 Active — entry points

| Script | What it does / when to run | Args |
| --- | --- | --- |
| `main.py` | The orchestrator. Loads all entities in dependency order, or one via `--entity`; also hosts the validator, reporting, and the patch / create-missing operations. | Full flag list in [CLI reference — `main.py`](#cli-reference--mainpy). |
| `validate.py` | Read-only pre-load validator (the checks above). Run **before every load**, standalone or as `python main.py --validate`. | `--entity <customer\|billingAccount\|pricePlan\|subscription\|oneOff>` scopes to one entity's checks (omit = all). |

### 🟢 Active — pre-load data prep & gates (operate on the CSVs)

| Script | What it does / when to run | Args |
| --- | --- | --- |
| `check_pricing_coverage.py` | Credential-free gate: is every price plan the subs reference present in the pricing CSV, and does every included line carry a plan at all? Run the instant a regenerated export lands — catches an **unscoped** pricing export and **unpriced lines** (would bill £0) before you load. A `0/0` pass is reported as *vacuous*, not clean. (Also folded into validator checks 9.1 + 11.) | `--quiet` (exit code only, no per-plan detail) |
| `filter_to_subs.py` | Trims the 3 dependency CSVs (customers, price plans, billing) **in place** to only the rows the current subs reference; writes a timestamped `.bak`. Never touches the subs CSV. (README Step 4.) | `--dry-run` (preview, no write) |
| `dedup_price_plans.py` | **Interim** transform: collapse the pricing CSV to one row per distinct pricing *shape* (item-free `MP_PP_<hash>`) and rewrite each sub line's `Price Plan External ID` to match; in place (`.bak`). (README Step 5.) | `--dry-run` |
| `analyze_priceplan_dedup.py` | Read-only sizing report: how many *distinct* prices the batch actually needs vs how many plan externalIds the subs reference. Diagnostic for the dedup step. | `--full` (also analyse the whole pricing CSV, not just the batch) |

### 🟢 Active — post-load fix & recovery

| Script | What it does / when to run | Args |
| --- | --- | --- |
| `scripts/cleanup_unbilled_subscriptions.py` | Delete subscriptions that loaded **without a billing account** or **bound to the wrong customer**, and clear their state rows so a reload re-creates them. Read-only by default. (README Step 10.) | scope: `--from-csv` (default) \| `--all-revision` \| `--ext-id EXT` (repeatable); reasons: `--name-mismatch`, `--keep-missing-ba`; `--execute`; `--allow-prod` |
| `repair_customer_addresses.py` | PATCH customers **loaded this run** (found via the state DB) that lack a default billing/shipping address — the BA loader needs both. ⚠️ State-DB-driven, so it only covers customers loaded this run; for a **pre-existing** (prod) customer use `prod/patch_customer_address.py` instead. | `--dry-run` |

### 🟢 Active — prod helpers

| Script | What it does / when to run | Args |
| --- | --- | --- |
| `prod/prod_check.py` | Read-only NS inspector (GET / SuiteQL only). Use freely before/after any load. | Subcommands documented in [Read-only inspector](#read-only-inspector--prodprod_checkpy). |
| `prod/patch_customer_address.py` | Add/repair a default billing+shipping address on **one existing** customer, from the customer CSV or explicit values. Run dry first, then `--apply`. | target: `--customer-id` \| `--externalid`; source: `--from-csv --company-name "<NAME>"` or `--addr1/--addr2/--city/--state/--zip/--country/--addressee/--attention`; `--set-defaults`, `--add`, `--line-id`, `--apply` |

### 🟢 Active — optional preflight & ad-hoc diagnostics

| Script | What it does / when to run | Args |
| --- | --- | --- |
| `scripts/preflight_revision_load.py` | Read-only, no NS calls. After bumping `LOAD_REVISION`, reports per-entity counts, **state-DB collisions** (revision not bumped), and parent-lookup orphans. The collision check is the main value; the customer-orphan part is less relevant post-Option-B (customers are no longer seeded into the state DB). | *(none)* |
| `suiteql.py` | Run any SuiteQL `SELECT` and print the rows as a table. The everyday NS lookup tool. | positional: the query string, e.g. `python suiteql.py "SELECT id, name FROM subscriptionplan"` |

### 🟢 Active — core modules (imported by the loaders; not run directly)

`config.py` (paths, creds, `LOAD_REVISION`, `apply_revision`) · `netsuite_client.py`
(OAuth1 signing, REST, 3-tier ID retrieval) · `state_tracker.py` (SQLite idempotency
and ID chaining) · `customer_resolver.py` (C-number → NS id — the ONLY attach key, shared by
the subs and BA loaders) · `pricing_shape.py` (the shape hash the dedup tooling keys
on) · `loaders/base.py` and the five entity loaders under `loaders/`
(`customer`, `billing_account`, `price_plan`, `subscription`, `one_off`) ·
`__init__.py`, `loaders/__init__.py`.

### 🗄️ Stale / one-off — not part of any current flow

| Script | Why it's here / status |
| --- | --- |
| `base.py` (root) | **Duplicate** of `loaders/base.py` — nothing imports it. Safe to delete. |
| `prod/seed_state.py` | **Retired by Option B.** Pre-existing customers are now resolved by C-number straight from NS, so seeding their internal IDs into the state DB is obsolete. |
| `scripts/retrofit_subscription_pricing.py` | One-off backfill for the **first** subscription round (loaded before the price-plan layer existed). Built against `_rvn_01` and **not revision-aware** — audit before any reuse. |
| `scripts/build_priced_subscriptions_report.py` | One-off client reconciliation report built from the retrofit CSV output. Retrofit-era. |
| `scripts/build_retrofit_client_report.py` | One-off client report (summary/patched/orphaned/missing) from the retrofit CSV. Retrofit-era. |
| `prod/probe_line_price.py` | Exploratory probe used to discover how a price attaches to a sub line. Finding (price lives on the `priceInterval`, not the line) is now baked into `loaders/subscription.py`. Kept for reference. |
| `prod/probe_interval_price.py` | Companion probe that found the way to set the amount on the auto-created price interval — now the loader's approach. Kept for reference. |
| `data/check_billing_customers.py` | Premise obsolete under Option B: it asserts every billing-CSV customer exists in the customer CSV, but pre-existing customers legitimately aren't in it now. Validator check 5 covers this correctly against NS. |
| `scripts/filter_subs.py` | Ad-hoc generic subs-CSV subsetter by a key column (`--in/--out/--key/--values/--values-file`). Superseded by working from a batch-scoped export + `filter_to_subs.py`. Still works for manual carving. |
| `get_metadata.py` | One-time custom-field discovery (script IDs + labels per record type). Mapping is complete — revive only to map **new** fields. Positional: record type. |
| `probe_field_ids.py` | One-time SuiteQL resolver for linked-record custom-field internal IDs needed in the customer patch. Done. |
| `debug_auth.py` | One-time OAuth-signing diagnostic (minimal GET, prints request/response). Auth works — revive only if signing breaks. |
| `scripts/deactivate/deactivate_prior_revision.py` | **PARKED — future use, do NOT run as-is.** Would deactivate prior-revision records; needs per-entity review first (subscriptions/posted invoices can't simply be `isInactive`d). `--dry-run/--apply/--current-revision/--entity`. |

---

## How it works (reference)

### ID resolution — 3-tier strategy

Applied after every successful POST (the create response is `204 No Content`):

| Tier | Method                                              | When            |
| ---- | --------------------------------------------------- | --------------- |
| 1    | Parse the `Location` header (`.../customer/800419`) | Always first    |
| 2    | `GET /record/v1/{type}/eid:{externalId}`            | If Tier 1 fails |
| 3    | SuiteQL query by business key                       | If Tier 2 fails |

A record created but with an unresolved ID gets status `success_no_id` (manual review).

### Load revisions (`config.LOAD_REVISION`)

Every externalId POSTed ends with this suffix (currently `_rvn_prod_01`) — it's
always the **last** token. Bump it for a deliberate fresh generation so new
records don't collide with prior ones. **Exception:** prod customers are keyed by
their **raw** `External ID 2` (no suffix) because they pre-exist in NS.

```text
customer:        <External ID 2>            (prod: raw, no suffix)
billingAccount:  <sub External ID>_BA_rvn_prod_01   (= <deal>_<subid>_BA; pre-2026-07-09 records used <deal_id>_BA)
pricePlan:       MP_PP_<line>_<slug>_rvn_prod_01
subscription:    <deal_id>_rvn_prod_01
oneOff invoice:  <Invoice External ID>_rvn_prod_01
```

### State tracking (SQLite)

`$NS_STATE_DB`. Stores `entity_type`, `external_id`, `netsuite_id`, `status`
(`pending`/`success`/`success_no_id`/`failed`), `error_message`, `payload_hash`.
On re-run, `success` / `success_no_id` records are skipped.

> Being removed in WS2 (live NS reconciliation replaces it) — see the refactor plan.

### Idempotency & re-runs

The loader is idempotent: NS upserts by externalId, so re-running won't duplicate
even if the state DB is lost. On re-run, successful records are skipped and failed
ones retried.

**Re-loading a subscription** (immutable post-create): delete its state row *and*
delete the sub in NS first, else it's found as already-existing:

```bash
sqlite3 $NS_STATE_DB "DELETE FROM load_state WHERE entity_type='subscription' AND external_id='<extId>';"
```

### Logging

Structured logs at `logs/YYYY-MM-DD/load_HH-MM-SS.log` (GMT+3), full tracebacks to
file and terminal. A per-revision rolling status is appended to
`logs/<revision>/overall_status.log`.
