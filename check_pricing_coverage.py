"""
Pricing coverage check — does the pricing CSV cover this batch's subs?
======================================================================
OPTIONAL, operator-invoked. A fast, CREDENTIAL-FREE pre-load gate that answers
one question: for every price plan the current subscriptions reference, is there
a source row in the pricing CSV to create it from?

Why this exists separately from validate.py check 7.1: validate.py constructs a
NetSuiteClient (needs creds, makes SuiteQL calls). This touches only two local
CSVs, so you can run it the instant a regenerated pricing export lands — before
loading, before filtering — and get a clean PASS/FAIL with an exit code.

The failure mode it guards against: the pricing export (`PRICING_MP_STAGING` /
`JSON_PRICING_MP_V2`) is sometimes generated UNSCOPED — the full price book,
capped at ~20k rows — instead of scoped to the current deal batch. When that
happens most of this batch's line items fall outside the capped window, so their
plans are simply absent. filter_to_subs.py CANNOT fix this (you can't filter in
rows that aren't there); the pricing DDL must be re-run scoped to the batch.

Coverage is computed on RAW external ids (both the subs column and the pricing
`PRICE_PLAN_EXTERNAL_ID` are raw; the load revision is applied only at POST).

Usage:
  python check_pricing_coverage.py          # PASS/FAIL on the configured CSVs
  python check_pricing_coverage.py --quiet   # only the summary + exit code
"""

import os
import csv
import sys
import argparse

# config.py raises on import unless NS_REALM / NS_STATE_DB are set. This script
# only reads the CSV *path* constants from it — it makes no NetSuite calls — so
# set harmless placeholders if they're absent, keeping the tool credential-free.
os.environ.setdefault("NS_REALM", "0000000-sb0")
os.environ.setdefault("NS_STATE_DB", "state/_coverage_check_unused.db")

import config

# A pricing CSV with this many times more plans than the subs reference is almost
# certainly the unscoped full-book dump, not a per-batch export.
UNSCOPED_RATIO = 10


def _read_csv(path):
    """Return the CSV rows as a list of dicts, or None if the file is missing."""
    try:
        with open(path, encoding="utf-8-sig") as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        return None


def referenced_price_plans(subs_rows):
    """Raw `Price Plan External ID`s across all INCLUDED sub lines (non-blank).

    Mirrors check 7.1 / filter_to_subs: only `Lines: Include == 'T'` lines count,
    and a blank value is skipped (the line intentionally uses the book default).
    """
    refs = set()
    for r in subs_rows:
        if (r.get("Lines: Include") or "").strip() != "T":
            continue
        pp = (r.get("Price Plan External ID") or "").strip()
        if pp:
            refs.add(pp)
    return refs


def pricing_plan_ids(pricing_rows):
    """The set of raw `PRICE_PLAN_EXTERNAL_ID`s present in the pricing CSV."""
    return {
        (r.get("PRICE_PLAN_EXTERNAL_ID") or "").strip()
        for r in pricing_rows
        if (r.get("PRICE_PLAN_EXTERNAL_ID") or "").strip()
    }


def run_coverage_gate(quiet: bool = False) -> int:
    """Print the coverage report and return an exit code (0 PASS / 1 FAIL).

    Reusable by both the CLI ``main()`` and ``main.py --validate``. Reads only
    the configured subs + pricing CSVs (no NetSuite calls). FAILs (returns 1)
    when any price plan referenced by an included sub line has no row in the
    pricing CSV — those lines would load against the £0 price-book default. A
    missing CSV is a hard error (exit, not return) because there is nothing to
    check against.
    """
    subs_rows = _read_csv(config.SUBSCRIPTIONS_CSV)
    pricing_rows = _read_csv(config.PRICE_PLANS_CSV)
    if subs_rows is None:
        sys.exit(f"✗ subscriptions CSV not found: {config.SUBSCRIPTIONS_CSV}")
    if pricing_rows is None:
        sys.exit(f"✗ pricing CSV not found: {config.PRICE_PLANS_CSV}")

    refs = referenced_price_plans(subs_rows)
    have = pricing_plan_ids(pricing_rows)
    missing = sorted(refs - have)
    covered = len(refs) - len(missing)

    print("=" * 72)
    print("PRICING COVERAGE CHECK")
    print(f"  subs CSV    : {config.SUBSCRIPTIONS_CSV}")
    print(f"  pricing CSV : {config.PRICE_PLANS_CSV}")
    print("=" * 72)
    print(f"  price plans referenced by included sub lines : {len(refs)}")
    print(f"  distinct plans present in the pricing CSV    : {len(have)}")
    print(f"  covered                                      : {covered}/{len(refs)}")

    # Loud signal: the pricing CSV dwarfs what the subs need → looks unscoped.
    if refs and len(have) >= UNSCOPED_RATIO * len(refs):
        print(
            f"\n  ⚠ pricing CSV holds {len(have)} plans for only {len(refs)} "
            f"referenced — this looks like an UNSCOPED bulk export (the full "
            f"price book, likely capped at ~20k rows), not a per-batch file. "
            f"filter_to_subs.py cannot fix missing plans; re-run the pricing "
            f"DDL scoped to this batch's deals."
        )

    if missing:
        print(
            f"\n  ✗ {len(missing)} referenced price plan(s) have NO row in the "
            f"pricing CSV (cannot be created — lines would fall back to the £0 "
            f"book default):"
        )
        if not quiet:
            for m in missing:
                print(f"      {m}")
        print("\n" + "-" * 72)
        print(f"RESULT: ✗ FAIL — {len(missing)}/{len(refs)} plans uncovered.")
        print("-" * 72)
        return 1

    print("\n" + "-" * 72)
    print(f"RESULT: ✓ PASS — all {len(refs)} referenced plans present.")
    print("-" * 72)
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Check the pricing CSV covers every price plan the current "
        "subscriptions reference (read-only, no NetSuite calls)."
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Print only the summary line and exit code (no per-plan detail).",
    )
    args = parser.parse_args()
    sys.exit(run_coverage_gate(quiet=args.quiet))


if __name__ == "__main__":
    main()
