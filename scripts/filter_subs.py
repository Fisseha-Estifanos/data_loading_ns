"""
Filter Subscriptions CSV
========================
Read a large subscriptions CSV and write out only the rows whose value in a
chosen "key" column matches one of a supplied list of values. Use this to carve
a small, load-ready subset out of the full subs export when only some
subscriptions (or some product lines) are in scope for a load.

  - Key column is adjustable; default is `MP_PRODUCT_REF_NUMBER` (the MP product
    number Tom validates deals on).
  - Values to keep are supplied on the CLI (--values) or in a file
    (--values-file, one value per line), or by editing VALUES below.

Rows are written **verbatim** — no value is altered, reformatted, or defaulted
(per the project's strict data-integrity rule). Matching is whitespace-tolerant
and ignores a trailing ".0" so numeric-looking keys exported as 27396.0 still
match 27396; the original cell is preserved on write.

Usage:
    # filter the configured full subs CSV to two MP product numbers
    python scripts/filter_subs.py --values 27396 31802

    # different key column + values from a file + explicit paths
    python scripts/filter_subs.py \
        --in  data/subs-kleene-export-2026-05-29.csv \
        --out data/subs-prod-batch1.csv \
        --key "External ID" \
        --values-file scope/batch1_deal_ids.txt
"""

import argparse
import csv
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# This is a pure offline CSV utility — it never calls NetSuite. We only want
# config for the default input path, and config.py hard-requires NS_* env vars
# at import. So make the import optional: if creds aren't sourced, --in is just
# required instead of defaulted.
DEFAULT_IN: "str | None"
try:
    import config  # noqa: E402

    DEFAULT_IN = config.SUBSCRIPTIONS_CSV
except Exception:
    DEFAULT_IN = None

# ---- Editable defaults (override on the CLI) --------------------------------
# The column whose value decides whether a row is kept.
KEY_COLUMN = "MP_PRODUCT_REF_NUMBER"
# The values to keep. Leave empty and pass --values / --values-file instead.
VALUES: list[str] = [
    '27336',
    '27342',
    '27346',
    '27363',
    '27367',
    '27373',
    '27386',
    '27393',
    '27394',
    '27397',
    '27398',
    '27399',
    '27401',
    '27402',
]
# -----------------------------------------------------------------------------


def read_csv(path: str) -> tuple[list[str], list[dict]]:
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return list(reader.fieldnames or []), list(reader)


def norm(value: str) -> str:
    """Normalize a cell for comparison only (never for writing)."""
    v = (value or "").strip()
    if v.endswith(".0"):
        v = v[:-2]
    return v


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Filter a subscriptions CSV down to rows matching a key column.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--in",
        dest="in_path",
        default=DEFAULT_IN,
        required=DEFAULT_IN is None,
        help=(
            f"Input CSV (default: config.SUBSCRIPTIONS_CSV = {DEFAULT_IN})"
            if DEFAULT_IN
            else "Input CSV (required — config unavailable, NS_* env not set)"
        ),
    )
    parser.add_argument(
        "--out",
        dest="out_path",
        help="Output CSV (default: <input>-filtered.csv next to the input)",
    )
    parser.add_argument(
        "--key",
        default=KEY_COLUMN,
        help=f"Column name to filter on (default: {KEY_COLUMN})",
    )
    parser.add_argument(
        "--values",
        nargs="+",
        default=None,
        help="Values to keep, space-separated (e.g. --values 27396 31802)",
    )
    parser.add_argument(
        "--values-file",
        help="File with one value to keep per line (blank lines ignored)",
    )
    args = parser.parse_args()

    # Resolve the value list: CLI > file > VALUES constant.
    values = args.values
    if values is None and args.values_file:
        with open(args.values_file, "r", encoding="utf-8-sig") as f:
            values = [ln.strip() for ln in f if ln.strip()]
    if values is None:
        values = VALUES
    if not values:
        parser.error(
            "No filter values given. Pass --values, --values-file, or edit VALUES."
        )

    out_path = args.out_path
    if not out_path:
        p = Path(args.in_path)
        out_path = str(p.with_name(f"{p.stem}-filtered{p.suffix}"))

    fieldnames, rows = read_csv(args.in_path)
    if args.key not in fieldnames:
        parser.error(
            f"Key column {args.key!r} not found in {args.in_path}.\n"
            f"Available columns: {', '.join(fieldnames)}"
        )

    wanted = {norm(v) for v in values}
    kept = [r for r in rows if norm(r.get(args.key, "")) in wanted]

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(kept)

    # Report: counts + which requested values matched / were missing.
    matched = {norm(r.get(args.key, "")) for r in kept}
    missing = sorted(wanted - matched)

    print("=" * 70)
    print("  FILTER SUBSCRIPTIONS")
    print("=" * 70)
    print(f"  input        : {args.in_path}")
    print(f"  output       : {out_path}")
    print(f"  key column   : {args.key}")
    print(f"  values asked : {len(wanted)}")
    print(f"  rows in      : {len(rows)}")
    print(f"  rows kept    : {len(kept)}")
    print(f"  values found : {len(matched)}")
    if missing:
        print(f"  ⚠ not found  : {', '.join(missing)}")
    print("=" * 70)


if __name__ == "__main__":
    main()
