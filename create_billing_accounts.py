"""
create_billing_accounts.py — synthesize BAs for subs customers that have NONE
=============================================================================
Operator-invoked. Fills the gap check 2.1 flags: a subscription customer with
**no Billing Account in NS AND none in the billing CSV** — its sub would load
unbilled. For each such deal it CREATES a Billing Account in NS, deriving the
billing address from the customer's NS address book and the rest from the
subscription, so you don't have to hand-author a billing-CSV row or seed the
state tracker.

Why this exists separately from the billing-account loader: that loader keys the
customer off the **state tracker** (`apply_revision(customer_externalId)`), which
only works for customers THIS run loaded. Pre-existing customers aren't there.
This script resolves the customer **directly from NS by C-number** (reusing
`validate.Validator`), so no seeding is needed.

What it does NOT do: it never touches customers that already have a BA (in NS or
the CSV) — you load those normally / skip them. Strict data-integrity: a deal
whose currency is unmapped, whose frequency (PRICE_BOOK_FREQUENCY) is blank, or
whose customer lacks a default billing/shipping address is SKIPPED with a logged
reason — never created with invented values.

Field derivation for each created BA (externalId = ``<deal>_BA`` + revision):
  customer       → NS internal id (C-number resolution, via the validator)
  subsidiary     → the customer's NS subsidiary (authoritative)
  currency       → sub ``Currency`` via CURRENCY_MAP
  frequency      → sub ``PRICE_BOOK_FREQUENCY`` (skipped if blank)
  startDate      → earliest sub ``Start Date`` for the deal
  bill/shipAddr  → customer's default billing/shipping address book entry in NS
  name           → ``<Company>_<Frequency>_MP_<Currency>`` (truncated to NS 50)

Usage:
  python create_billing_accounts.py            # DRY RUN — print payloads, write nothing
  python create_billing_accounts.py --apply     # actually POST the billing accounts
"""

import argparse
import logging
from collections import defaultdict

import config
from state_tracker import StateTracker
from validate import Validator, _deal_id
from loaders.subscription import CURRENCY_MAP
from loaders.billing_account import _truncate_ba_name

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)-7s] %(message)s"
)
log = logging.getLogger("create_ba")


def _address_maps(client):
    """customer NS id → default billing / shipping address book internalid.

    Same query and filter the billing-account loader uses, so the addresses we
    attach are identical to a normally-loaded BA.
    """
    bill, ship = {}, {}
    rows = client.suiteql_query(
        "SELECT internalid, entity, defaultbilling, defaultshipping "
        "FROM customeraddressbook "
        "WHERE defaultbilling = 'T' OR defaultshipping = 'T'"
    )
    for r in rows:
        ent = str(r.get("entity", "")).strip()
        aid = str(r.get("internalid", "")).strip()
        if not ent or not aid:
            continue
        if str(r.get("defaultbilling", "")).upper() == "T":
            bill[ent] = aid
        if str(r.get("defaultshipping", "")).upper() == "T":
            ship[ent] = aid
    return bill, ship


def build_payloads(v: Validator, bill_map: dict, ship_map: dict):
    """Return (payloads, skips). One BA payload per gap deal.

    A gap deal = a subscription deal with NO billing-account row in the billing
    CSV whose customer has NO billing account in NS (``ns_cust_has_ba``) — i.e.
    exactly check 2.1's "neither" case, at deal granularity. ``skips`` collects
    (deal, reason) for deals we deliberately don't create, so nothing is silent.
    """
    ba_deals = {_deal_id((r.get("externalId") or "").strip()) for r in (v.bas or [])}
    deal_rows = defaultdict(list)
    for ext, grp in v.sub_groups.items():
        deal_rows[_deal_id(ext)].extend(grp)

    payloads, skips = [], []
    for deal, rows in sorted(deal_rows.items()):
        if deal in ba_deals:
            continue  # a BA is already being loaded for this deal
        name = (rows[0].get("Customer") or "").strip()
        rec = v.customer_ns_record(v.sub_cnum(rows), v.resolve_customer_ext(name))
        if not (rec and rec.get("id")):
            continue  # customer not in NS — check 1 owns this, not us
        cid = str(rec["id"])
        if cid in v.ns_cust_has_ba:
            continue  # customer already has a BA in NS — per the rule, skip

        # Required references — never invented; skip (loudly) if underivable.
        bill_addr, ship_addr = bill_map.get(cid), ship_map.get(cid)
        if not (bill_addr and ship_addr):
            skips.append(
                (
                    deal,
                    f"customer {name!r} (id {cid}) has no default "
                    f"billing/shipping address in NS — run "
                    f"repair_customer_addresses.py first",
                )
            )
            continue
        currency_name = v._group_value(rows, "Currency")
        currency_id = CURRENCY_MAP.get(currency_name)
        if not currency_id:
            skips.append((deal, f"currency {currency_name!r} not in CURRENCY_MAP"))
            continue
        # NS's frequency.id is the UPPER-CASE enum ("MONTHLY") — that's what the
        # billing CSV carries and what every successful BA load sends. The subs'
        # PRICE_BOOK_FREQUENCY is mixed-case ("Monthly"), so normalise the case to
        # match the NS enum (the only transform; value itself is unchanged).
        frequency = v._group_value(rows, "PRICE_BOOK_FREQUENCY").upper()
        if not frequency:
            skips.append((deal, "PRICE_BOOK_FREQUENCY is blank (won't invent one)"))
            continue
        subsidiary_id = rec.get("subsidiary")
        if not subsidiary_id:
            skips.append((deal, f"customer {name!r} has no NS subsidiary on record"))
            continue
        start_dates = sorted(
            (r.get("Start Date") or "").strip()
            for r in rows
            if (r.get("Start Date") or "").strip()
        )
        start_date = start_dates[0] if start_dates else None

        ext_id = config.apply_revision(f"{deal}_BA")
        ba_name, _ = _truncate_ba_name(f"{name}_{frequency}_MP_{currency_name}")
        payload = {
            "externalId": ext_id,
            "name": ba_name,
            "customer": {"id": cid},
            "subsidiary": {"id": str(subsidiary_id)},
            "currency": {"id": str(currency_id)},
            "frequency": {"id": frequency},
            "customerDefault": False,
            "requestOffCycleInvoice": False,
            "inactive": False,
            "billAddressList": bill_addr,
            "shipAddressList": ship_addr,
        }
        if start_date:
            payload["startDate"] = start_date
        payloads.append((ext_id, payload, name))
    return payloads, skips


def main():
    parser = argparse.ArgumentParser(
        description="Create billing accounts for subs customers that have none "
        "(no BA in NS and none in the billing CSV). Dry-run unless --apply."
    )
    parser.add_argument(
        "--apply", action="store_true", help="Actually POST the billing accounts."
    )
    args = parser.parse_args()

    # Reuse the validator for CSV loading, customer resolution, and the NS maps
    # (ns_by_cnum / ns_by_ext / ns_cust_has_ba) — no duplicated resolution logic.
    v = Validator()
    v.run({"subscription"})  # read-only; populates the NS lookup maps
    bill_map, ship_map = _address_maps(v.client)

    payloads, skips = build_payloads(v, bill_map, ship_map)

    print("\n" + "=" * 72)
    print("CREATE BILLING ACCOUNTS" + ("" if args.apply else "  (DRY RUN)"))
    print(f"  revision: {config.LOAD_REVISION}")
    print("=" * 72)
    print(f"  gap deals to create a BA for : {len(payloads)}")
    print(f"  deals skipped (see below)    : {len(skips)}")

    for deal, reason in skips:
        print(f"  ⚠ SKIP deal {deal}: {reason}")

    if not payloads:
        print("\nNothing to create.")
        return

    if not args.apply:
        import json

        for ext_id, payload, name in payloads:
            print(f"\n  would create {ext_id}  ({name})")
            print("   " + json.dumps(payload, indent=2).replace("\n", "\n   "))
        print("\nDRY RUN — re-run with --apply to POST these.")
        return

    tracker = StateTracker()
    ok = fail = 0
    try:
        for ext_id, payload, name in payloads:
            log.info(f"Creating billingAccount {ext_id} ({name})…")
            status, ns_id, error = v.client.create_and_resolve_id(
                record_type="billingAccount",
                payload=payload,
                external_id=ext_id,
                tier3_field="externalId",
                tier3_value=ext_id,
            )
            tracker.upsert_state(
                entity_type="billingAccount",
                external_id=ext_id,
                status=status,
                netsuite_id=ns_id,
                error_message=error,
                payload_hash=None,
                tier_used="create_billing_accounts",
            )
            if status == "success":
                ok += 1
                log.info(f"  ✓ {ext_id} → NS id {ns_id}")
            else:
                fail += 1
                log.error(f"  ✗ {ext_id}: {error}")
    finally:
        tracker.close()
    print(f"\nDone: {ok} created, {fail} failed. Re-run --validate to confirm.")


if __name__ == "__main__":
    main()
