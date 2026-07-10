"""
Filter the dependency CSVs down to what the subscriptions need
==============================================================
OPTIONAL, operator-invoked tool. The subscriptions CSV (`config.SUBSCRIPTIONS_CSV`)
is the source of truth for what we are loading *this batch*. The three dependency
CSVs — customers, price plans, and billing accounts — are bulk exports that
routinely carry far more rows than the current subs need (e.g. the pricing export
is capped at 20k rows). Loading or validating against those extra rows pushes data
we don't want and produces spurious blockers/warnings for records the subs never
reference.

Running this script trims each dependency CSV **in place** to only the rows the
current subs reference, after writing a timestamped backup next to the original.
It NEVER touches the subscriptions CSV, and it never edits values inside a kept
row — it only drops rows. Use `--dry-run` to preview without writing.

What "needed" means (derived from the subs CSV):
  - deal ids        = each sub `External ID` up to the first underscore
  - customer names  = each sub `Customer`, alias-resolved (config.CUSTOMER_NAME_ALIASES)
  - price plan ids  = each included line's `Price Plan External ID` (raw)

Filter rules:
  - Billing accounts: keep rows whose `externalId` deal id is in the sub deal ids.
  - Customers:        keep rows whose `External ID 2` is referenced by a kept BA
                      (`customer_externalId`) OR whose `Company Name` matches a
                      sub customer name (alias-resolved, case-insensitive).
  - Price plans:      keep rows whose `PRICE_PLAN_EXTERNAL_ID` is in the sub
                      price-plan ids.

Usage:
  python filter_to_subs.py            # filter all three CSVs in place (+ backups)
  python filter_to_subs.py --dry-run  # report what would change, write nothing
"""

import os
import csv
import sys
import argparse
from datetime import datetime

import config


def _deal_id(external_id: str) -> str:
    """Deal id = the external id token before the first underscore."""
    return external_id.split("_", 1)[0]


def _read_csv(path):
    """Return (fieldnames, rows) for a CSV, or (None, None) if it is missing."""
    try:
        with open(path, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            return reader.fieldnames, rows
    except FileNotFoundError:
        return None, None


def _resolve_name(raw_name: str) -> str:
    """sub `Customer` spelling → canonical Company Name (alias map), UPPERCASED."""
    name = (raw_name or "").strip()
    canonical = config.CUSTOMER_NAME_ALIASES.get(name.upper(), name)
    return canonical.upper()


def needed_from_subs():
    """Read the subs CSV and return the three sets that define what's needed.

    Returns a dict with keys ``deal_ids``, ``customer_names`` (alias-resolved,
    UPPER) and ``price_plans`` (raw external ids). Exits if the subs CSV is
    missing — there is nothing to filter against without it.
    """
    fields, rows = _read_csv(config.SUBSCRIPTIONS_CSV)
    if rows is None:
        sys.exit(f"✗ subscriptions CSV not found: {config.SUBSCRIPTIONS_CSV}")

    deal_ids, customer_names, price_plans = set(), set(), set()
    for r in rows:
        ext = (r.get("External ID") or "").strip()
        if ext:
            deal_ids.add(_deal_id(ext))
        name = (r.get("Customer") or "").strip()
        if name:
            customer_names.add(_resolve_name(name))
        if (r.get("Lines: Include") or "").strip() == "T":
            pp = (r.get("Price Plan External ID") or "").strip()
            if pp:
                price_plans.add(pp)
    return {
        "deal_ids": deal_ids,
        "customer_names": customer_names,
        "price_plans": price_plans,
    }


def _write_filtered(path, fieldnames, kept_rows, dry_run):
    """Back up `path` then rewrite it with only `kept_rows` (no-op if --dry-run).

    The backup is `<path>.bak-YYYYMMDD-HHMMSS`. The new file is written to a temp
    path first and atomically moved into place so a crash can't leave a partial
    CSV. Column order and every value in a kept row are preserved verbatim.
    """
    if dry_run:
        return None
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = f"{path}.bak-{stamp}"
    os.replace(path, backup)  # move original aside (preserves it untouched)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(kept_rows)
    os.replace(tmp, path)
    return backup


def filter_csv(label, path, fieldnames, rows, keep_fn, dry_run):
    """Partition `rows` via `keep_fn`, report, and rewrite the file.

    Returns the count of kept rows. Prints a one-line summary; on a real run also
    prints the backup location. A missing file (rows is None) is reported and
    skipped.
    """
    if rows is None:
        print(f"  ⚠ {label}: file not found, skipping → {path}")
        return None
    kept = [r for r in rows if keep_fn(r)]
    removed = len(rows) - len(kept)
    print(
        f"  {label:14} {len(rows):>6} rows → keep {len(kept):>4}, "
        f"remove {removed:>5}   ({os.path.basename(path)})"
    )
    backup = _write_filtered(path, fieldnames, kept, dry_run)
    if backup:
        print(f"                 backup: {os.path.basename(backup)}")
    return len(kept)


def main():
    parser = argparse.ArgumentParser(
        description="Filter customer / price-plan / billing CSVs to what the "
        "current subscriptions CSV needs (in place, with backups)."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be removed without writing any files.",
    )
    args = parser.parse_args()

    need = needed_from_subs()
    print("Needed by subs in", os.path.basename(config.SUBSCRIPTIONS_CSV) + ":")
    print(
        f"  deals: {len(need['deal_ids'])} · customer names: "
        f"{len(need['customer_names'])} · price plans: {len(need['price_plans'])}"
    )
    if args.dry_run:
        print("  (DRY RUN — no files will be written)")
    print()

    # 1. Billing accounts — keep rows whose deal id is needed. Collect the
    #    customer external ids of the kept BAs so customers can include them.
    ba_fields, ba_rows = _read_csv(config.BILLING_CSV)
    kept_ba_customers = set()
    if ba_rows is not None:
        for r in ba_rows:
            if _deal_id((r.get("externalId") or "").strip()) in need["deal_ids"]:
                cust = (r.get("customer_externalId") or "").strip()
                if cust:
                    kept_ba_customers.add(cust)

    def keep_ba(r):
        return _deal_id((r.get("externalId") or "").strip()) in need["deal_ids"]

    def keep_customer(r):
        ext2 = (r.get("External ID 2") or "").strip()
        name = (r.get("Company Name") or "").strip().upper()
        return ext2 in kept_ba_customers or name in need["customer_names"]

    def keep_priceplan(r):
        return (r.get("PRICE_PLAN_EXTERNAL_ID") or "").strip() in need["price_plans"]

    cust_fields, cust_rows = _read_csv(config.CUSTOMERS_CSV)
    pp_fields, pp_rows = _read_csv(config.PRICE_PLANS_CSV)

    filter_csv("billing", config.BILLING_CSV, ba_fields, ba_rows, keep_ba, args.dry_run)
    filter_csv(
        "customers",
        config.CUSTOMERS_CSV,
        cust_fields,
        cust_rows,
        keep_customer,
        args.dry_run,
    )
    filter_csv(
        "priceplans",
        config.PRICE_PLANS_CSV,
        pp_fields,
        pp_rows,
        keep_priceplan,
        args.dry_run,
    )

    print()
    if args.dry_run:
        print("Dry run complete. Re-run without --dry-run to apply.")
    else:
        print("Done. Originals saved as .bak-<timestamp> next to each file.")


if __name__ == "__main__":
    main()
