"""
Price-plan de-duplication analysis (read-only)
==============================================
Answers: "of the price plans THIS batch's subscriptions reference, how many are
actually distinct prices, and how many are duplicates that differ only in the
item baked into their externalId?"

A NS price plan has no item binding (see pricing_shape.py) — so plans with an
identical pricing shape are interchangeable. This script groups the batch's
referenced plans by shape and reports how many real plans you'd need vs how many
externalIds the subs currently reference. Read-only: writes nothing.

Needs the PROPER (batch-scoped) pricing CSV — the per-line PRICE_PLAN_JSON is the
authoritative shape. Plans referenced by the subs but absent from the pricing CSV
(e.g. an unscoped/partial export) are listed separately as "no shape data"; fix
the pricing export (see check_pricing_coverage.py) for the complete picture.

Usage:
  python analyze_priceplan_dedup.py            # batch-scoped analysis
  python analyze_priceplan_dedup.py --full     # also analyse the whole pricing CSV
"""

import os
import csv
import json
import argparse
from collections import defaultdict

# config.py raises without these; this script makes no NetSuite calls.
os.environ.setdefault("NS_REALM", "0000000-sb0")
os.environ.setdefault("NS_STATE_DB", "state/_analysis_unused.db")

import config
from pricing_shape import shape_of, canonical_ext_id


def _read_csv(path):
    try:
        with open(path, encoding="utf-8-sig") as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        return None


def _referenced_plans(subs_rows):
    """raw Price Plan External ID → first (item name, sub External ID) seen."""
    refs = {}
    for r in subs_rows:
        if (r.get("Lines: Include") or "").strip() != "T":
            continue
        pp = (r.get("Price Plan External ID") or "").strip()
        if not pp:
            continue
        refs.setdefault(
            pp,
            (
                (r.get("Sales Item") or "").strip(),
                (r.get("External ID") or "").strip(),
            ),
        )
    return refs


def _shape_index(pricing_rows):
    """raw PRICE_PLAN_EXTERNAL_ID → (shape string, canonical id, parsed plan)."""
    idx = {}
    for r in pricing_rows:
        ext = (r.get("PRICE_PLAN_EXTERNAL_ID") or "").strip()
        raw = r.get("PRICE_PLAN_JSON") or ""
        if not (ext and raw.strip()):
            continue
        try:
            plan = json.loads(raw)
        except json.JSONDecodeError:
            continue
        idx[ext] = (shape_of(plan), canonical_ext_id(plan), plan)
    return idx


def _tiers_summary(plan):
    """One-line human summary of a plan's price (currency, type, min, tiers)."""
    cur = (plan.get("currency") or {}).get("id")
    ppt = (plan.get("pricePlanType") or {}).get("id")
    mn = plan.get("minimumAmount")
    mx = plan.get("maximumAmount")
    tiers = (plan.get("priceTiers") or {}).get("items") or []
    tvals = ", ".join(str(t.get("value")) for t in tiers)
    return f"cur={cur} type={ppt} min={mn} max={mx} tiers=[{tvals}]"


def analyse(label, ext_to_meta, shape_idx):
    """Group `ext_to_meta` (extId → display meta) by pricing shape and print.

    Returns nothing; prints the grouped report. `shape_idx` provides the shape
    for each extId; extIds absent from it are reported as "no shape data".
    """
    by_canonical = defaultdict(list)  # canonical id → [(ext, meta)]
    no_data = []
    for ext, meta in ext_to_meta.items():
        info = shape_idx.get(ext)
        if not info:
            no_data.append((ext, meta))
            continue
        _, canonical, _ = info
        by_canonical[canonical].append((ext, meta))

    have = sum(len(v) for v in by_canonical.values())
    distinct = len(by_canonical)
    print("=" * 72)
    print(f"DEDUP ANALYSIS — {label}")
    print("=" * 72)
    print(f"  externalIds referenced/considered : {len(ext_to_meta)}")
    print(f"  with shape data in pricing CSV     : {have}")
    print(f"  distinct pricing shapes            : {distinct}")
    if have:
        print(
            f"  => {have} externalIds collapse to {distinct} real plans "
            f"(saves {have - distinct}; {have / distinct:.1f}x duplication)"
        )

    # Show only the shapes that actually dedupe more than 1 externalId, plus a
    # count of singletons, to keep the report focused.
    multi = {c: v for c, v in by_canonical.items() if len(v) > 1}
    if multi:
        print(f"\n  shapes shared by >1 referenced externalId ({len(multi)}):")
        for canonical, members in sorted(multi.items(), key=lambda kv: -len(kv[1])):
            _, _, plan = shape_idx[members[0][0]]
            print(f"    {canonical}  ({len(members)} plans)  {_tiers_summary(plan)}")
            for ext, meta in members:
                print(f"        ← {ext}   [{meta[0]}]")
    singletons = distinct - len(multi)
    if singletons:
        print(f"\n  + {singletons} shape(s) referenced by exactly one externalId.")

    if no_data:
        print(
            f"\n  ⚠ {len(no_data)} referenced plan(s) have NO shape data in the "
            f"pricing CSV (can't be analysed/created — fix the pricing export):"
        )
        for ext, meta in sorted(no_data):
            print(f"      {ext}   [{meta[0]}]")
    print("-" * 72 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Analyse price-plan duplication for the current batch "
        "(read-only)."
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Also analyse the entire pricing CSV, not just the batch's refs.",
    )
    args = parser.parse_args()

    subs_rows = _read_csv(config.SUBSCRIPTIONS_CSV)
    pricing_rows = _read_csv(config.PRICE_PLANS_CSV)
    if subs_rows is None:
        raise SystemExit(f"✗ subs CSV not found: {config.SUBSCRIPTIONS_CSV}")
    if pricing_rows is None:
        raise SystemExit(f"✗ pricing CSV not found: {config.PRICE_PLANS_CSV}")

    shape_idx = _shape_index(pricing_rows)
    refs = _referenced_plans(subs_rows)
    analyse("THIS BATCH (subs-referenced plans)", refs, shape_idx)

    if args.full:
        all_ext = {
            (r.get("PRICE_PLAN_EXTERNAL_ID") or "").strip(): (
                (r.get("NETSUITE_SALES_ITEMS") or "").strip(),
                "",
            )
            for r in pricing_rows
            if (r.get("PRICE_PLAN_EXTERNAL_ID") or "").strip()
        }
        analyse("ENTIRE PRICING CSV", all_ext, shape_idx)


if __name__ == "__main__":
    main()
