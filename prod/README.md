# prod/ — Push a single subscription (with correct pricing)

Everything in this folder was built to push **one** subscription into NetSuite
correctly — customer linked, billing account + address attached, and the
**price actually showing**. It also works for sandbox (sb3) testing.

This README is the quick guide: the tools, the gotchas we found, and the
step-by-step flow. Worked example used throughout:

| Thing | Value |
|---|---|
| Deal / sub externalId | `495006175463_27396` |
| Customer (already in NS) | `C44268` → internal id `830950` |
| Sub | id `59971` |
| Price plan (our £102) | `MP_PP_453962471664_EMPLOYMENT_LAW_AL` → `1573355` |

---

## The 5 things we learned (gotchas)

1. **Separate state DB per environment.** The state DB stores NetSuite internal
   IDs, which only mean something in their own account. Set `NS_STATE_DB` with
   the creds. Prod = `state/load_state_prod.db`, sandbox = `state/load_state.db`.
   *(`NS_REALM` and `NS_STATE_DB` are now required env vars — see `config.py`.)*

2. **The customer often already exists in NS.** Don't re-create it. Find its
   internal id and **seed** it into the state DB so the sub/BA link to it
   (`seed_state.py`).

3. **The billing account needs the customer to have a default address.** The BA
   loader pulls `billAddressList`/`shipAddressList` from the customer's default
   billing+shipping address. If the customer has none, add it
   (`patch_customer_address.py`).

4. **externalId must match between exports.** The sub looks for billing account
   `<sub externalId>_BA`. If the sub externalId is `495006175463_27396`, the
   billing CSV `externalId` must be `495006175463_27396_BA` (not just
   `495006175463_BA`).

5. **The price does NOT live on the subscription line — it lives on the
   subscription's `priceInterval`.** NS auto-creates one interval per line
   (copied from the plan's price book, at £0 for Moorepay's standard books).
   The fix (already in `loaders/subscription.py`): include the line, then
   **repoint that line's price interval** at our loaded price plan. PATCHing
   `subscriptionLine.pricePlan` does nothing (NS 204s and ignores it).

---

## The tools (all in `prod/`)

| Script | What it does | Writes? |
|---|---|---|
| `prod_check.py` | Read-only inspector (see commands below) | No (GET / SuiteQL only) |
| `seed_state.py` | Put an existing NS record's internal id into the state DB | Local SQLite only |
| `patch_customer_address.py` | Add / flag a customer's default address | PATCH to NS (`--apply`) |
| `probe_interval_price.py` | Test how a price attaches to an interval (how we found the fix) | PATCH to NS (`--apply`) |
| `probe_line_price.py` | Earlier probe (line-level) — superseded by the interval probe | PATCH to NS (`--apply`) |
| `PROD_RUNBOOK.md` | Longer prod-specific runbook (creds, prod vs sandbox) | — |

### `prod_check.py` commands (read-only)
```
whoami                                   # which NS account am I pointed at?
customer --entityid C44268               # find a customer (by C-number / externalid / name)
address --customer-id 830950             # does the customer have default bill+ship address?
billing --externalid 495006175463_27396_BA_rvn_prod_01
item --name "Employment Law - AL"        # does the sales item exist?
sub-readiness --deal 495006175463_27396 --c-number C44268   # full pre-load check
subscription --externalid 495006175463_27396_rvn_prod_01    # dump sub lines (summary)
subline --sub-id 59971 --line 1          # raw dump of one line
sub-intervals --sub-id 59971             # dump the price intervals (where prices live)
record --type pricePlan --id 1573355     # raw GET any record
```

---

## The flow — push one subscription end to end

Assumes the customer already exists in NS (the common case). Run from the repo
root with your env sourced (`source .env` for sandbox).

```bash
# 0. Confirm which account you're on
python prod/prod_check.py whoami

# 1. Find the existing customer's internal id
python prod/prod_check.py customer --entityid C44268
#    → id=830950

# 2. Make sure the customer has a default billing+shipping address
python prod/prod_check.py address --customer-id 830950
#    If missing, add it from the customer CSV (dry-run first, then --apply):
python prod/patch_customer_address.py --customer-id 830950 --from-csv \
    --company-name "7 LAMPS SECURITY LTD"
python prod/patch_customer_address.py --customer-id 830950 --from-csv \
    --company-name "7 LAMPS SECURITY LTD" --apply

# 3. Seed the existing customer into the state DB (so sub/BA link to it)
python prod/seed_state.py --company-name "7 LAMPS SECURITY LTD" --netsuite-id 830950
python prod/seed_state.py --company-name "7 LAMPS SECURITY LTD" --netsuite-id 830950 --apply

# 4. Check the billing CSV externalId matches:  <sub externalId>_BA
#    e.g. 495006175463_27396_BA   (edit the CSV cell if it's just 495006175463_BA)

# 5. Load the price plan(s) for this deal
python main.py --dry-run --entity pricePlan
python main.py --entity pricePlan
#    → note the new price plan id from "Tier 1 success: extracted ID <PP_ID>"
#    Confirm it landed (by externalId, then raw dump by internal id):
python prod/prod_check.py externalid --type pricePlan --id MP_PP_453962471664_EMPLOYMENT_LAW_AL_rvn_prod_01
python prod/prod_check.py record --type pricePlan --id <PP_ID>

# 6. Load the billing account (auto-attaches the address)
python main.py --dry-run --entity billingAccount
python main.py --entity billingAccount
#    → note the new billing account id from "Tier 1 success: extracted ID <BA_ID>"
#    Confirm it landed AND the address attached (raw dump shows billAddressList/shipAddressList):
python prod/prod_check.py externalid --type billingAccount --id 495006175463_27396_BA_rvn_prod_01
python prod/prod_check.py record --type billingAccount --id <BA_ID>
#Or use a dedicated billing helper (by external id or by customer)
# this specific BA by its externalId:
python prod/prod_check.py billing --externalid 495006175463_27396_BA_rvn_prod_01
# or every BA belonging to the customer:
python prod/prod_check.py billing --customer-id 830950

# 7. Load the subscription (includes the line AND prices it via the interval fix)
python main.py --entity subscription
#    → note the new sub id from "Tier 1 success: extracted ID <ID>"

# 8. Verify the price landed
python prod/prod_check.py sub-intervals --sub-id <ID>
#    line should show: pricePlan id=<our plan>  recurringAmt=102.0
```

### Affected entities & load order
```
customer        → already exists → SEED (step 3), don't load
                  + ensure default address (step 2)
pricePlan       → load (step 5)
billingAccount  → load (step 6)   [needs customer address + matching externalId]
subscription    → load (step 7)   [includes line + sets price on its interval]
```

---

## Notes

- **DRAFT vs activation:** a freshly loaded sub is in `DRAFT`. `recurringAmount`
  on the interval shows the price immediately; `totalintervalvalue` / contract
  value only compute when the subscription is **activated** (a NetSuite step).
- **Re-loading a sub:** the state DB skips records already marked success. To
  re-load, delete its state row first:
  `sqlite3 state/load_state.db "DELETE FROM load_state WHERE entity_type='subscription' AND external_id='<extId>';"`
  (and delete the sub in NS, or it'll be found as already-existing).
- **One-time charge lines** use a different amount field than `recurringAmount`.
  The interval-repoint fix still applies, but re-verify pricing when a deal
  includes one-time lines.
- The pricing fix lives in `loaders/subscription.py` → `_activate_subscription_lines`,
  so it applies to **every** sub loaded from now on, not just this one.
