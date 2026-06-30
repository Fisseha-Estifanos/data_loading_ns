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

Run from the repo root. This is the canonical hierarchy — the same in both
environments. The **only** change between sandbox and prod is which env file you
source in step 1 (plus the prod extras in [§ Sandbox vs Prod](#sandbox-vs-prod)).

**Pricing prep — canonical order** (when the Snowflake exports are bulk dumps):

```text
proper batch-scoped pricing CSV  →  filter_to_subs.py  →  dedup_price_plans.py  →  main.py --validate  →  load
```

Each arrow maps to a step below (3 → 3b → 3c → 4 → 5). The order matters:
`filter_to_subs.py` trims every dependency CSV down to this batch first; then
`dedup_price_plans.py` collapses what remains by pricing shape; then `--validate`
(incl. the pricing-coverage gate) must pass before you load.

```bash
cd data_loading

# 1. Pick the environment — this is the ONLY sb3-vs-prod switch.
source .env          # sandbox
#   …or…
source .env.prod     # production

# 2. Confirm you're pointed where you think you are (read-only).
python prod/prod_check.py whoami        # prints SANDBOX or PRODUCTION + account id

# 3. Point config.py *_CSV paths at the active Snowflake exports for this batch,
#    and set config.LOAD_REVISION (the externalId suffix; currently _rvn_prod_01).

# 3b. (OPTIONAL) Trim the dependency CSVs to ONLY what this batch's subs need.
#     The customer / pricePlan / billing exports are bulk dumps that carry far
#     more rows than the subs reference; without trimming, the load pushes data
#     you don't want and validate flags problems for rows the subs never touch.
#     This rewrites each file in place (timestamped .bak saved). Subs CSV is
#     never modified. Preview first, then apply.
python filter_to_subs.py --dry-run     # report what would be removed; writes nothing
python filter_to_subs.py               # apply: trim in place, back up originals

# 3c. (OPTIONAL, interim) De-duplicate price plans by pricing shape. A NS price
#     plan has no item binding — plans with identical currency/type/tiers/min/max
#     are the same plan; the item slug in the externalId is just a name. The full
#     price book is ~6.7x duplicated (e.g. 20k rows → ~3k distinct). This collapses
#     the pricing CSV to one row per distinct shape (item-free externalId
#     MP_PP_<hash>) and re-points the subs' `Price Plan External ID` to match.
#     Rewrites both files in place (.bak saved); idempotent. Inspect first:
python analyze_priceplan_dedup.py --full   # report duplication (read-only, no creds)
python dedup_price_plans.py --dry-run      # report what the rewrite would change
python dedup_price_plans.py                # apply: dedupe pricing + remap subs refs
#     NOTE: this is the "from here" stopgap; the proper fix moves the same shape
#     hashing into the pricing + subs DDLs (and the loaders).

# 4. VALIDATE — read-only, reports EVERY data problem at once. Run before every load.
python main.py --validate
#    Exits non-zero if blockers exist (name/key mismatches, subsidiary conflicts,
#    blank/late BA start dates, unmapped sales items, missing addresses, ...).
#    Resolve every ✗ before loading. Scope to one entity with --validate --entity <e>.
#    When subscriptions are in scope, --validate also runs the PRICING COVERAGE
#    GATE (below) and fails if any referenced price plan is missing from the
#    pricing CSV. You can also run that gate on its own — no NS creds needed:
python check_pricing_coverage.py       # PASS only if every sub-referenced plan is in the pricing CSV
#    A FAIL here usually means the pricing export was generated UNSCOPED (the
#    full price book, capped ~20k rows) instead of scoped to this batch's deals.
#    filter_to_subs.py cannot fix it — re-run the pricing DDL scoped to the batch.

# 4b. DECIDE which customers to load. Customers resolve to NS by C-number (the
#     customer CSV's "C-number" column / the subs' NETSUITE_ACCOUNT_NUMBER*),
#     NOT by External ID 2 (a HubSpot id absent from NS). In the validate report,
#     under "CUSTOMERS — subscription resolves to an NS customer":
#       • Check 1.2 lists, per customer, which are already in NS (skip — do NOT
#         load) vs which are NOT in NS and must be created/loaded first.
#       • Check 1.3 splits the "not resolvable by id" ones into "truly absent
#         (create it)" vs "in NS under a different id (reconcile, don't dup)".
#       • Only load (step 5 customer) the customers flagged NOT in NS.

# 4c. DECIDE which billing accounts to load / create. Under the validate report's
#     "BILLING ACCOUNTS — subs customer already has one in NS" section:
#       • Check 2.1 asks NS directly whether each subs customer already has a
#         Billing Account. Load BAs (step 5 billingAccount) only for customers
#         that don't — strip the rest from the billing CSV.
#       • Check 2.1 WARNS for any customer with NO BA in NS AND none in the
#         billing CSV — its sub would be unbilled. Create those in step 5b.

# 5. LOAD in dependency order. Dry-run each first, then the live run.
python main.py --dry-run --entity customer        && python main.py --entity customer
python main.py --entity customer --patch-eer       # link Electronic Email Recipients (2nd step)
python main.py --dry-run --entity billingAccount  && python main.py --entity billingAccount

# 5b. CREATE billing accounts for the gap customers Check 2.1 flagged (no BA in
#     NS and none in the billing CSV). Derives the address from the customer's NS
#     address book and the rest from the subscription; resolves the customer by
#     C-number (no state-tracker seeding needed). Dry-run first.
python create_billing_accounts.py            # DRY RUN — prints the BAs it would create
python create_billing_accounts.py --apply     # POST them
#     A deal skipped for "no default billing/shipping address" needs its customer
#     address added first:  python repair_customer_addresses.py

python main.py --dry-run --entity pricePlan       && python main.py --entity pricePlan
python main.py --entity subscription               # includes lines AND prices the interval
python main.py --entity oneOff

# 6. VERIFY.
python main.py --report --failures
python prod/prod_check.py sub-intervals --sub-id <ID>   # confirm price landed on the interval
```

Running `python main.py` with no `--entity` runs all five in this order
automatically. The dry-run-then-live pairs above just give you a checkpoint at
each step.

### Load order & dependencies (non-negotiable)

```text
1. Customer              (no dependencies)
2. Billing Account       ← references Customer NS internal ID
3. Price Plan            (no NS parent deps — refs sales items by name only)
4. Subscription          ← references Customer + Billing Account + Price Plan (per line)
5. One-Off Invoice       ← references Customer NS internal ID
```

Each child resolves its parent's NS internal ID from the state DB. If the parent
isn't loaded, the child is skipped with a logged error.

---

## Sandbox vs Prod

The pipeline is identical. These are the only environment-specific realities:

| #   | Aspect          | Sandbox                            | Production                                                                                                                                                                                                                 |
| --- | --------------- | ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | **Env file**    | `source .env`                      | `source .env.prod`                                                                                                                                                                                                         |
| 2   | **State DB**    | `state/load_state.db`              | `state/load_state_prod.db` — **never cross them.** The DB stores NS *internal* IDs that only mean something in their own account; a sandbox DB used against prod would wire prod records to sandbox internal IDs           |
| 3   | **Customers**   | Loaded fresh (`--entity customer`) | **Already exist** in NS under their **raw** `External ID 2` (no `_rvn` suffix). Do **not** load them — seed the existing internal ID into the state DB instead (see below), then load BA / pricePlan / subscription on top |
| 4   | **Addresses**   | Loaded with the customer           | Prod customers may lack a default billing+shipping address; the BA load *skips* without one. Check and patch first (see below)                                                                                             |
| 5   | **Permissions** | Usually open                       | Role needs SuiteBilling Full grants (see setup)                                                                                                                                                                            |

### Prod-only extra steps (when the customer already exists)

For a prod load where the customer is already live in NS, insert these between
steps 2 and 5 of the standard sequence, **per deal**:

```bash
# Full readiness check for the deal (customer exists? BA? price plan/item resolve?)
python prod/prod_check.py sub-readiness --deal <sub_externalId> --c-number <C-number>

# Ensure the customer has a default billing + shipping address (BA load needs it)
python prod/prod_check.py address --customer-id <internal_id>
python prod/patch_customer_address.py --customer-id <id> --from-csv --company-name "<NAME>"          # dry-run
python prod/patch_customer_address.py --customer-id <id> --from-csv --company-name "<NAME>" --apply

# Seed the existing customer's internal ID into the state DB so sub/BA link to it
python prod/seed_state.py --company-name "<NAME>" --netsuite-id <internal_id>                        # dry-run
python prod/seed_state.py --company-name "<NAME>" --netsuite-id <internal_id> --apply
```

Then run step 5 **without** `--entity customer` (it's seeded, not loaded).

> **externalId match (gotcha):** the subscription looks for billing account
> `<sub externalId>_BA`. If the sub externalId is `495006175463_27396`, the
> billing CSV `externalId` must be `495006175463_27396_BA` — edit the cell if the
> export emits just `495006175463_BA`.

> The state-DB seeding dance is being removed — see
> [`refactors/REFACTOR-PLAN.md`](refactors/REFACTOR-PLAN.md) WS2 (replace SQLite
> with live NS reconciliation). Until that ships, the steps above are current.

---

## Five gotchas worth knowing

1. **Separate state DB per environment** — see table #2 above. The #1 way to
   corrupt a prod load.
2. **The customer often already exists in prod.** Don't re-create it — seed it.
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

| Flag                   | Values                                                          | Description                                                                                                                                         |
| ---------------------- | --------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--entity`             | `customer` `billingAccount` `pricePlan` `subscription` `oneOff` | Load only this entity. Omit to run all five in dependency order.                                                                                    |
| `--validate`           | —                                                               | Read-only pre-load validator. Reports every data problem at once; exits non-zero on blockers. Respects `--entity`. **Run before every load.**       |
| `--dry-run`            | —                                                               | Build and log payloads without any API calls.                                                                                                       |
| `--limit N`            | integer                                                         | Process only the first N records (test a single POST).                                                                                              |
| `--report`             | —                                                               | Print the load-state summary (counts per status per entity). Scoped to the active `LOAD_REVISION`.                                                  |
| `--failures`           | —                                                               | Add failure details to `--report`. Use with `--report`.                                                                                             |
| `--all-revisions`      | —                                                               | Make `--report` span all revisions instead of just the active one.                                                                                  |
| `--field-map`          | —                                                               | Print the CSV column → NS API field mapping for all loaders. No creds needed.                                                                       |
| `--skip-preflight`     | —                                                               | Skip the auth connectivity check at startup.                                                                                                        |
| `--patch`              | —                                                               | **Retroactive only.** PATCH already-loaded customers with custom fields. Not needed for new loads (fields are in `build_payload()`). Customer only. |
| `--patch-eer`          | —                                                               | Link `custentity_zellis_elec_email_recipients` (two-step POST+PATCH). Run after `--entity customer`.                                                |
| `--patch-ba-startdate` | —                                                               | PATCH billing-account startDates from the CSV where they differ from NS. Use with `--entity billingAccount`.                                        |

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
billingAccount:  <deal_id>_BA_rvn_prod_01
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
