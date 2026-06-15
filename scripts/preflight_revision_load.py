"""
Preflight Revision Load
========================
Read-only sanity check before running a fresh revisioned load. Does NOT call
NetSuite. Reads all five active CSVs, applies `config.LOAD_REVISION`, and
reports:

  1. Per-entity row + revisioned-ext_id counts.
  2. State-DB collisions: any revisioned ext_id that's already a success in
     the state DB (would mean LOAD_REVISION was not bumped).
  3. Parent-lookup orphans: dependent rows whose revisioned parent ext_id is
     NOT yet in the state DB (only meaningful AFTER the parent entity has
     been loaded under the new revision).

Usage:
    python scripts/preflight_revision_load.py
"""

import csv
import sys
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402
from state_tracker import StateTracker  # noqa: E402


def read_csv(path: str) -> list[dict]:
    with open(path, "r", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def section(title: str) -> None:
    print()
    print("=" * 78)
    print(f"  {title}")
    print("=" * 78)


def report_entity(
    name: str,
    rows: list[dict],
    raw_id_fn,
    derived: dict[str, callable] = None,
) -> set[str]:
    """Print revisioned ext_id counts for one entity. Returns the set."""
    derived = derived or {}
    raw_ids: list[str] = []
    for r in rows:
        rid = raw_id_fn(r)
        if rid:
            raw_ids.append(rid)
    unique_raw = set(raw_ids)
    revisioned = {config.apply_revision(r) for r in unique_raw}
    print(f"\n[{name}] rows={len(rows):>5}  unique_raw_ids={len(unique_raw):>4}  "
          f"revisioned_ids={len(revisioned):>4}")
    sample = sorted(revisioned)[:3]
    print(f"    sample: {sample}")
    for label, fn in derived.items():
        derived_ids = {config.apply_revision(fn(rid)) for rid in unique_raw}
        print(f"    derived[{label}] count={len(derived_ids)}  "
              f"sample={sorted(derived_ids)[:3]}")
    return revisioned


def main() -> int:
    print(f"Active LOAD_REVISION: {config.LOAD_REVISION!r}")
    tracker = StateTracker()

    # ── Load CSV row sets ───────────────────────────────────────────────
    section("CSV row counts and revisioned ext_id sets")

    customers = read_csv(config.CUSTOMERS_CSV)
    cust_ids = report_entity(
        "customer",
        customers,
        lambda r: r.get("External ID 2", "").strip(),
    )

    billings = read_csv(config.BILLING_CSV)
    ba_ids = report_entity(
        "billingAccount",
        billings,
        lambda r: r.get("externalId", "").strip(),
    )

    pp_rows = read_csv(config.PRICE_PLANS_CSV)
    pp_ids = report_entity(
        "pricePlan",
        pp_rows,
        lambda r: r.get("PRICE_PLAN_EXTERNAL_ID", "").strip(),
    )

    sub_rows = read_csv(config.SUBSCRIPTIONS_CSV)
    sub_ids = report_entity(
        "subscription",
        sub_rows,
        lambda r: r.get("External ID", "").strip(),
        derived={"BA": lambda raw: f"{raw.split('_', 1)[0]}_BA"},
    )

    one_rows = read_csv(config.ONEOFF_CSV)
    one_ids = report_entity(
        "oneOff",
        one_rows,
        lambda r: r.get("Invoice External ID", "").strip(),
    )

    # ── Collision check ─────────────────────────────────────────────────
    section("State-DB collision check (must all be 0 for a clean revision)")

    def collision_count(entity_type: str, revisioned_ids: set[str]) -> int:
        n = 0
        for ext in revisioned_ids:
            if tracker.is_already_loaded(entity_type, ext):
                n += 1
        return n

    for entity_type, ids in (
        ("customer", cust_ids),
        ("billingAccount", ba_ids),
        ("pricePlan", pp_ids),
        ("subscription", sub_ids),
        ("oneOff", one_ids),
    ):
        n = collision_count(entity_type, ids)
        flag = "  ✗ COLLISION" if n else "  ✓"
        print(f"{flag}  {entity_type:>16s}: {n} of {len(ids)} already in state DB")

    # ── Parent-lookup orphan check ──────────────────────────────────────
    section("Parent-lookup orphans (only meaningful after parents are loaded)")

    # Customer-name → External ID 2 map (raw)
    cust_name_to_raw = {
        r["Company Name"].strip().upper(): r["External ID 2"].strip()
        for r in customers
        if r.get("Company Name") and r.get("External ID 2")
    }

    # billing → customer
    ba_orphans = []
    for r in billings:
        cust_raw = r.get("customer_externalId", "").strip()
        if not cust_raw:
            continue
        cust_rev = config.apply_revision(cust_raw)
        if not tracker.get_netsuite_id("customer", cust_rev):
            ba_orphans.append((r.get("externalId", ""), cust_rev))
    print(f"  billingAccount → customer: {len(ba_orphans)} orphan(s)")
    for ext, miss in ba_orphans[:5]:
        print(f"    {ext}  -> missing customer: {miss}")

    # subscription → customer (via name) + → BA + → price plan
    sub_cust_orphans = defaultdict(int)
    sub_ba_orphans = 0
    sub_pp_orphans = 0
    for r in sub_rows:
        deal = r.get("External ID", "").strip()
        if not deal:
            continue
        cust_name = r.get("Customer", "").strip().upper()
        cust_raw = cust_name_to_raw.get(cust_name)
        if not cust_raw:
            sub_cust_orphans["unmappable_name"] += 1
        else:
            if not tracker.get_netsuite_id("customer", config.apply_revision(cust_raw)):
                sub_cust_orphans["missing_in_state_db"] += 1
        ba_rev = config.apply_revision(f"{deal.split('_', 1)[0]}_BA")
        if not tracker.get_netsuite_id("billingAccount", ba_rev):
            sub_ba_orphans += 1
        pp_raw = (r.get("Price Plan External ID") or "").strip()
        if pp_raw and not tracker.get_netsuite_id(
            "pricePlan", config.apply_revision(pp_raw)
        ):
            sub_pp_orphans += 1
    print(f"  subscription → customer:       {dict(sub_cust_orphans)}")
    print(f"  subscription → billingAccount: {sub_ba_orphans} row(s) missing BA")
    print(f"  subscription → pricePlan:      {sub_pp_orphans} row(s) missing PP")

    # one-off → customer (by name)
    one_cust_orphans = defaultdict(int)
    for r in one_rows:
        cust_name = r.get("Customer (Req)", "").strip().upper()
        cust_raw = cust_name_to_raw.get(cust_name)
        if not cust_raw:
            one_cust_orphans["unmappable_name"] += 1
        elif not tracker.get_netsuite_id(
            "customer", config.apply_revision(cust_raw)
        ):
            one_cust_orphans["missing_in_state_db"] += 1
    print(f"  oneOff → customer: {dict(one_cust_orphans)}")

    print()
    print("Preflight done. No NS calls were made.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
