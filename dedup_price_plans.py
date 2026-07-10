"""
De-duplicate price plans by pricing shape (CSV transform)
=========================================================
OPTIONAL, operator-invoked. The INTERIM "do it from here" step before the proper
fix moves into the pricing + subs DDLs. It removes the unnecessary one-plan-per-
(line × item) duplication and the item slug baked into externalIds, WITHOUT
touching any loader code — the loaders already key off whatever externalIds the
CSVs carry.

What it does, in place (timestamped .bak saved next to each file):
  1. Pricing CSV (config.PRICE_PLANS_CSV): collapse rows to ONE per distinct
     pricing shape (see pricing_shape.py). Each surviving row is re-keyed to the
     item-free canonical externalId `MP_PP_<shape_hash>`, its PRICE_PLAN_JSON
     externalId rewritten to match, and NETSUITE_SALES_ITEMS set to the ';'-
     joined set of items that shared the shape (traceability only — not sent).
  2. Subs CSV (config.SUBSCRIPTIONS_CSV): rewrite each line's `Price Plan
     External ID` from its original to that shape's canonical id, so the subs
     reference the deduped plans. A reference whose plan isn't in the pricing CSV
     is left UNCHANGED (it has no shape to map to — fix the pricing export; see
     check_pricing_coverage.py).

Run order: get the proper batch-scoped pricing CSV → (optional) filter_to_subs →
THIS → validate → load. Idempotent: re-running maps already-canonical ids to
themselves.

Usage:
  python dedup_price_plans.py --dry-run   # report what would change; write nothing
  python dedup_price_plans.py             # apply: rewrite both CSVs, back up originals
"""

import os
import csv
import json
import sys
import argparse
from datetime import datetime
from collections import defaultdict

import config
from pricing_shape import canonical_ext_id


def _read_csv(path):
    """Return (fieldnames, rows), or (None, None) if the file is missing."""
    try:
        with open(path, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            return reader.fieldnames, rows
    except FileNotFoundError:
        return None, None


def _write_csv(path, fieldnames, rows, dry_run):
    """Back up `path` then atomically rewrite it with `rows` (no-op if dry-run)."""
    if dry_run:
        return None
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = f"{path}.bak-{stamp}"
    os.replace(path, backup)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)
    return backup


def build_dedup(pricing_rows):
    """Compute the dedup from the pricing rows.

    Returns ``(alias, canonical_rows, stats)`` where:
      - ``alias``          : original PRICE_PLAN_EXTERNAL_ID → canonical id
      - ``canonical_rows`` : one rewritten pricing row per distinct shape
                             (first-seen order preserved)
      - ``stats``          : dict of counts for the report

    A row whose PRICE_PLAN_JSON won't parse is passed through unchanged (its own
    externalId stays canonical-for-itself) and counted under ``unparseable`` —
    never silently dropped or guessed (data-integrity rule).
    """
    alias = {}
    first_row = {}  # canonical → representative row (the row we keep)
    items_for = defaultdict(set)  # canonical → set of NETSUITE_SALES_ITEMS
    order = []  # canonical ids in first-seen order
    unparseable = 0

    for r in pricing_rows:
        orig = (r.get("PRICE_PLAN_EXTERNAL_ID") or "").strip()
        raw = r.get("PRICE_PLAN_JSON") or ""
        if not orig:
            continue
        try:
            plan = json.loads(raw)
        except json.JSONDecodeError:
            # Keep as its own plan; don't fold or fabricate a shape.
            unparseable += 1
            canonical = orig
            plan = None
        else:
            canonical = canonical_ext_id(plan)

        alias[orig] = canonical
        item = (r.get("NETSUITE_SALES_ITEMS") or "").strip()
        if item:
            items_for[canonical].add(item)
        if canonical not in first_row:
            first_row[canonical] = (dict(r), plan)
            order.append(canonical)

    canonical_rows = []
    for canonical in order:
        row, plan = first_row[canonical]
        out = dict(row)
        out["PRICE_PLAN_EXTERNAL_ID"] = canonical
        # Re-key the embedded payload externalId so the loader's
        # json_ext == ext_id sanity check passes after the rewrite.
        if plan is not None:
            plan["externalId"] = canonical
            out["PRICE_PLAN_JSON"] = json.dumps(plan)
        if "NETSUITE_SALES_ITEMS" in out:
            out["NETSUITE_SALES_ITEMS"] = "; ".join(sorted(items_for[canonical]))
        if "LINE_ITEM_ID" in out:
            out["LINE_ITEM_ID"] = ""  # no longer tied to one HS line
        canonical_rows.append(out)

    stats = {
        "pricing_in": len(
            [r for r in pricing_rows if (r.get("PRICE_PLAN_EXTERNAL_ID") or "").strip()]
        ),
        "pricing_out": len(canonical_rows),
        "unparseable": unparseable,
    }
    return alias, canonical_rows, stats


def rewrite_subs(subs_rows, alias):
    """Rewrite each included line's `Price Plan External ID` to its canonical id.

    Mutates ``subs_rows`` in place. Returns ``(remapped, unmapped)`` counts. A
    value already canonical (idempotent re-run) maps to itself. A value with no
    entry in ``alias`` — its plan isn't in the pricing CSV — is left untouched
    and counted as ``unmapped`` so the caller can warn.
    """
    remapped = unmapped = 0
    for r in subs_rows:
        pp = (r.get("Price Plan External ID") or "").strip()
        if not pp:
            continue
        canonical = alias.get(pp)
        if canonical is None:
            unmapped += 1
            continue
        if canonical != pp:
            r["Price Plan External ID"] = canonical
            remapped += 1
    return remapped, unmapped


def main():
    parser = argparse.ArgumentParser(
        description="De-duplicate price plans by pricing shape and re-point the "
        "subs at the canonical (item-free) externalIds (in place, with backups)."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change without writing any files.",
    )
    args = parser.parse_args()

    p_fields, p_rows = _read_csv(config.PRICE_PLANS_CSV)
    s_fields, s_rows = _read_csv(config.SUBSCRIPTIONS_CSV)
    if p_rows is None:
        sys.exit(f"✗ pricing CSV not found: {config.PRICE_PLANS_CSV}")
    if s_rows is None:
        sys.exit(f"✗ subs CSV not found: {config.SUBSCRIPTIONS_CSV}")

    alias, canonical_rows, stats = build_dedup(p_rows)
    remapped, unmapped = rewrite_subs(s_rows, alias)

    print("=" * 72)
    print(
        "PRICE-PLAN DEDUP" + ("  (DRY RUN — no files written)" if args.dry_run else "")
    )
    print("=" * 72)
    print(f"  pricing CSV : {config.PRICE_PLANS_CSV}")
    print(f"    plans in  : {stats['pricing_in']}")
    print(
        f"    plans out : {stats['pricing_out']}  "
        f"(removed {stats['pricing_in'] - stats['pricing_out']} duplicates)"
    )
    if stats["unparseable"]:
        print(f"    ⚠ {stats['unparseable']} row(s) had unparseable JSON — kept as-is.")
    print(f"  subs CSV    : {config.SUBSCRIPTIONS_CSV}")
    print(f"    line price-plan refs remapped to canonical : {remapped}")
    if unmapped:
        print(
            f"    ⚠ {unmapped} line ref(s) NOT in the pricing CSV — left "
            f"unchanged (fix the pricing export; see check_pricing_coverage.py)."
        )

    if not args.dry_run:
        b1 = _write_csv(config.PRICE_PLANS_CSV, p_fields, canonical_rows, False)
        b2 = _write_csv(config.SUBSCRIPTIONS_CSV, s_fields, s_rows, False)
        print(f"\n  wrote {config.PRICE_PLANS_CSV}  (backup: {os.path.basename(b1)})")
        print(f"  wrote {config.SUBSCRIPTIONS_CSV}  (backup: {os.path.basename(b2)})")
        print("\nDone. Re-run check_pricing_coverage.py / --validate to confirm.")
    else:
        print("\nDry run complete. Re-run without --dry-run to apply.")
    print("-" * 72)


if __name__ == "__main__":
    main()
