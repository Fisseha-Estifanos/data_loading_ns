# Prod Load Runbook — Single Subscription

How to push **one** subscription into NetSuite **production**, end to end, using
the existing loader pipeline without contaminating it with sandbox state.

This is the **prod-specific** runbook. For the tool reference, the full command
list, and the *why* behind each gotcha (especially pricing), see
[`README.md`](README.md) — it documents the same flow proven in sandbox. This
file focuses on the prod specifics: creds, separate state DB, and the exact
ordered steps.

First target (from Tom, 2026-05-29):

| Field | Value |
|---|---|
| MP Ref | 27396 |
| Deal ID / sub externalId | 495006175463_27396 |
| Customer C-number | C43837 → prod internal id `830950` |

> The sub attaches to the **existing** prod customer C43837. Confirm with
> `prod_check.py` what already exists in prod before loading.

---

## Why prod ≠ "sandbox with different creds"

### 1. Credentials & endpoint — no code change
[`config.py`](../config.py) derives `BASE_URL` / `SUITEQL_URL` / `REALM` from the
`NS_*` env vars. Keep prod creds in a **separate file** (never commit it):

```bash
# .env.prod  — creds from 1Password "Netsuite2 Prod - Moorepay"
NS_CONSUMER_KEY=...
NS_CONSUMER_SECRET=...
NS_ACCESS_TOKEN=...
NS_TOKEN_SECRET=...
NS_REALM=<prod account id, e.g. 4874529>      # NOT ...-sb3
NS_STATE_DB=state/load_state_prod.db          # <-- separate prod DB (see #2)
```

### 2. Separate state DB — **critical**
The state DB stores NetSuite **internal IDs**, which only mean something in their
own account. `state/load_state.db` is full of **sandbox** IDs. Running prod
against it would skip records (marked success) and, worse, wire prod
`customer.id` / `billingAccount.id` / `pricePlan.id` to **sandbox** internal IDs.
Always set `NS_STATE_DB=state/load_state_prod.db` for prod.
(`NS_REALM` and `NS_STATE_DB` are required env vars.)

### 3. The customer already exists in prod
C43837 is a live prod customer. **Do not let the pipeline create a duplicate.**
Find its internal id and **seed** it into the prod state DB; load only price
plan + billing account + subscription on top.

### 4. Permissions
The prod integration role needs **Full** on SuiteBilling lists: **Price Plans,
Price Books, Subscriptions, Subscription Plans, Billing Accounts**. Without it
you'll get `403 INSUFFICIENT_PERMISSION` on the price-plan/sub loads. Get Adam to
grant these once, up front.

---

## Step-by-step

### Step 0 — Confirm the account
```bash
cd data_loading
export $(grep -v '^#' .env.prod | xargs)
python prod/prod_check.py whoami      # must say PRODUCTION, account = prod id
```

### Step 1 — Filtered CSVs from Snowflake
Export the entity CSVs filtered to this deal (Tom validated
`MP_PRODUCT_REF_NUMBER = '27396'`), drop them in `data/`, and point the `*_CSV`
paths in [`config.py`](../config.py) at them. You need: **customers** (with the
address columns for C43837), **pricing-json**, **subscriptions**, and
**billing-accounts**.

> **externalId match (gotcha):** the sub looks for billing account
> `<sub externalId>_BA`. If the sub externalId is `495006175463_27396`, the
> billing CSV `externalId` must be `495006175463_27396_BA` — edit the cell if the
> export emits just `495006175463_BA`.

### Step 2 — Readiness check (read-only)
```bash
python prod/prod_check.py sub-readiness --deal 495006175463_27396 --c-number C43837
```
Confirms the customer exists (+ internal id), whether a billing account exists,
and that the price plan / sales item / subscription plan resolve. **Resolve every
✗ and get Adam/Tom sign-off before loading.**

### Step 3 — Ensure the customer has a default address
The billing-account loader pulls `billAddressList`/`shipAddressList` from the
customer's **default billing + shipping** address. Check, and add from the
customer CSV if missing:
```bash
python prod/prod_check.py address --customer-id 830950
# if missing (dry-run, then --apply):
python prod/patch_customer_address.py --customer-id 830950 --from-csv \
    --company-name "<COMPANY NAME>"
python prod/patch_customer_address.py --customer-id 830950 --from-csv \
    --company-name "<COMPANY NAME>" --apply
```

### Step 4 — Seed the existing customer into the prod state DB
So the sub/BA link to the real C43837 instead of creating a duplicate:
```bash
python prod/seed_state.py --company-name "<COMPANY NAME>" --netsuite-id 830950
python prod/seed_state.py --company-name "<COMPANY NAME>" --netsuite-id 830950 --apply
```

### Step 5 — Load in order (dry-run each first)
```bash
python main.py --dry-run --entity pricePlan      && python main.py --entity pricePlan
python main.py --dry-run --entity billingAccount && python main.py --entity billingAccount
python main.py --entity subscription
#   → note the new sub id from "Tier 1 success: extracted ID <ID>"
python main.py --report --failures
```
Do **not** load `customer` (it already exists — seeded in step 4).

### Step 6 — Verify the price actually landed
```bash
python prod/prod_check.py sub-intervals --sub-id <ID>
```
The included line's interval should show `pricePlan id=<our plan>` and
`recurringAmt=<the £ value>`. This is the real proof — the price lives on the
subscription's **priceInterval**, not the line. The loader sets it automatically
(see [`README.md`](README.md) → pricing, and
[`loaders/subscription.py`](../loaders/subscription.py) `_activate_subscription_lines`).

> `totalintervalvalue` / contract value stay 0 in **DRAFT** and compute on
> **activation** (a NetSuite workflow step). `recurringAmount` is the in-draft
> proof the price is wired.

---

## Affected entities & load order

```
customer        → already exists → SEED (step 4), don't load  + ensure address (step 3)
pricePlan       → load (step 5)
billingAccount  → load (step 5)   [needs customer address + matching _BA externalId]
subscription    → load (step 5)   [includes line + sets price on its interval]
```

---

## Guardrails

- `prod_check.py` is **read-only**. `seed_state.py` writes only to the local
  state DB. `patch_customer_address.py` and the loaders write to NS (dry-run /
  `--apply` gated where applicable).
- Keep `.env.prod` out of git (covered by `.gitignore` `.env.*`).
- Set a `LOAD_REVISION` for prod before loading (audit / externalId shape).
- Never run prod `main.py` without `NS_STATE_DB` pointing at a prod-only DB.
- Re-loading a sub: delete its state row first, and delete the sub in NS (or it's
  found as already-existing):
  `sqlite3 $NS_STATE_DB "DELETE FROM load_state WHERE entity_type='subscription' AND external_id='<extId>';"`
