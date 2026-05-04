"""
Build a client-friendly retrofit reconciliation report.

Reads the latest retrofit CSV from scripts/reports/<date>/ and emits four
small CSVs into scripts/reports/<date>/client/:

  1. summary_by_deal.csv  — one row per HubSpot deal: counts of each outcome
  2. patched.csv          — 99 lines successfully linked to a price plan
  3. orphaned.csv         — NS lines with no source row; client decides keep/remove
  4. missing.csv          — source rows with no NS line; client decides backfill

All files are sorted by HubSpot deal_id so the client can scan a deal's full
state across the three outcome buckets.

Usage:
    python scripts/build_retrofit_client_report.py                # latest report
    python scripts/build_retrofit_client_report.py <path-to-csv>  # specific file
"""

import csv
import re
import sys
from collections import defaultdict
from pathlib import Path

REPORTS_DIR = Path(__file__).resolve().parent / "reports"

PP_LINE_ITEM_RE = re.compile(r"^MP_PP_(\d+)_")


def find_latest_retrofit_csv() -> Path:
    """
    Return the latest retrofit CSV in scripts/reports/<date>/
    which may contain the results from the latest retrofit script.
    """
    candidates = sorted(REPORTS_DIR.glob("*/retrofit_*.csv"))
    if not candidates:
        sys.exit("No retrofit CSVs found under scripts/reports/")
    return candidates[-1]


def extract_line_item_id(pp_ext_id: str) -> str:
    """
    Return the line item ID from a price plan external ID
    which is in the format MP_PP_<line_item_id>_<>
    """
    if not pp_ext_id:
        return ""
    m = PP_LINE_ITEM_RE.match(pp_ext_id)
    return m.group(1) if m else ""


def main(src_path: Path) -> None:
    """
    Build a client-friendly retrofit reconciliation report from the given
    retrofit CSV.

    Args:
        src_path: Path to the retrofit CSV file
    """
    out_dir = src_path.parent / "client"
    out_dir.mkdir(exist_ok=True)

    rows = list(csv.DictReader(src_path.open()))

    by_deal: dict[str, dict[str, int]] = defaultdict(
        lambda: {
            "PATCHED": 0,
            "ORPHANED_IN_NS": 0,
            "MISSING_IN_NS": 0,
            "PP_BLANK_IN_SOURCE": 0,
            "DIVERGENT_AMBIGUOUS": 0,
        }
    )
    patched, orphaned, missing = [], [], []

    for r in rows:
        deal = r["deal_id"]
        outcome = r["outcome"]
        if outcome in by_deal[deal]:
            by_deal[deal][outcome] += 1

        if outcome == "PATCHED":
            patched.append(
                {
                    "hs_deal_id": deal,
                    "hs_line_item_id": extract_line_item_id(r["pp_ext_id"]),
                    "ns_subscription_id": r["subscription_ns_id"],
                    "ns_line_number": r["line_number"],
                    "ns_sales_item": r["item_name"],
                    "ns_price_plan_id": r["pp_ns_id"],
                    "price_plan_ext_id": r["pp_ext_id"],
                }
            )
        elif outcome == "ORPHANED_IN_NS":
            orphaned.append(
                {
                    "hs_deal_id": deal,
                    "ns_subscription_id": r["subscription_ns_id"],
                    "ns_line_number": r["line_number"],
                    "ns_sales_item": r["item_name"],
                    "client_action": "KEEP or REMOVE?",
                }
            )
        elif outcome == "MISSING_IN_NS":
            missing.append(
                {
                    "hs_deal_id": deal,
                    "hs_line_item_id": extract_line_item_id(r["pp_ext_id"]),
                    "ns_sales_item": r["item_name"],
                    "expected_price_plan_ext_id": r["pp_ext_id"],
                    "client_action": "BACKFILL or IGNORE?",
                }
            )

    summary = []
    for deal in sorted(by_deal):
        c = by_deal[deal]
        summary.append(
            {
                "hs_deal_id": deal,
                "patched": c["PATCHED"],
                "orphaned_in_ns": c["ORPHANED_IN_NS"],
                "missing_in_ns": c["MISSING_IN_NS"],
                "pp_blank_in_source": c["PP_BLANK_IN_SOURCE"],
                "divergent_ambiguous": c["DIVERGENT_AMBIGUOUS"],
            }
        )

    def write(path: Path, rows: list[dict]) -> None:
        if not rows:
            return
        with path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)

    write(out_dir / "summary_by_deal.csv", summary)
    write(
        out_dir / "patched.csv",
        sorted(patched, key=lambda r: (r["hs_deal_id"], int(r["ns_line_number"] or 0))),
    )
    write(
        out_dir / "orphaned.csv",
        sorted(
            orphaned, key=lambda r: (r["hs_deal_id"], int(r["ns_line_number"] or 0))
        ),
    )
    write(
        out_dir / "missing.csv",
        sorted(missing, key=lambda r: (r["hs_deal_id"], r["ns_sales_item"])),
    )

    print(f"Source : {src_path}")
    print(f"Output : {out_dir}/")
    print(f"  summary_by_deal.csv  ({len(summary)} deals)")
    print(f"  patched.csv          ({len(patched)} lines)")
    print(f"  orphaned.csv         ({len(orphaned)} lines)")
    print(f"  missing.csv          ({len(missing)} lines)")


if __name__ == "__main__":
    src = Path(sys.argv[1]) if len(sys.argv) > 1 else find_latest_retrofit_csv()
    main(src)
