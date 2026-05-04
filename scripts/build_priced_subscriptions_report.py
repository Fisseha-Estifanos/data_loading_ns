"""
Build a client-facing table of subscription lines with completed pricing.

Joins:
  - latest retrofit CSV (PATCHED rows only)               → NS line + price plan IDs
  - data/price-plans-kleene-export-2026-04-27.csv         → Net Price + Currency
  - data/subs-kleene-export-2026-04-27.csv                → Customer Name

Output:
  scripts/reports/<date>/client/priced_subscriptions.csv

This is intended to be shared with the client for reconciliation. Columns are
ordered for human reading; Net Price is rendered as a plain decimal (no
scientific notation).

Usage:
    python scripts/build_priced_subscriptions_report.py
    python scripts/build_priced_subscriptions_report.py <retrofit-csv-path>
"""

import csv
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = ROOT / "scripts" / "reports"
PRICE_PLANS_CSV = ROOT / "data" / "price-plans-kleene-export-2026-04-27.csv"
SUBS_CSV = ROOT / "data" / "subs-kleene-export-2026-04-27.csv"

CURRENCY_ID_TO_CODE = {"1": "GBP", "4": "EUR"}
PP_LINE_ITEM_RE = re.compile(r"^MP_PP_(\d+)_")


def find_latest_retrofit_csv() -> Path:
    """Finds the latest retrofit CSV file"""
    candidates = sorted(REPORTS_DIR.glob("*/retrofit_*.csv"))
    if not candidates:
        sys.exit("No retrofit CSVs found under scripts/reports/")
    return candidates[-1]


def extract_line_item_id(pp_ext_id: str) -> str:
    """Extracts the line item ID from the price plan external ID"""
    m = PP_LINE_ITEM_RE.match(pp_ext_id or "")
    return m.group(1) if m else ""


def load_price_plan_lookup() -> dict[str, tuple[str, str]]:
    """ext_id → (net_price_str, currency_code)"""
    lookup: dict[str, tuple[str, str]] = {}
    with PRICE_PLANS_CSV.open(encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            ext_id = row["PRICE_PLAN_EXTERNAL_ID"].strip()
            try:
                payload = json.loads(row["PRICE_PLAN_JSON"])
                value = payload["priceTiers"]["items"][0]["value"]
                currency_id = str(payload["currency"]["id"])
            except (KeyError, IndexError, json.JSONDecodeError, TypeError):
                continue
            net_price = f"{float(value):.4f}".rstrip("0").rstrip(".")
            currency = CURRENCY_ID_TO_CODE.get(currency_id, currency_id)
            lookup[ext_id] = (net_price, currency)
    return lookup


def load_customer_lookup() -> dict[str, str]:
    """deal_id (External ID) → customer name. Picks first non-empty per group."""
    lookup: dict[str, str] = {}
    with SUBS_CSV.open(encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            deal = (row.get("External ID") or "").strip()
            cust = (row.get("Customer") or "").strip()
            if deal and cust and deal not in lookup:
                lookup[deal] = cust
    return lookup


def main(src_path: Path) -> None:
    """
    Main function to build the priced subscriptions report

    Args:
        src_path: Path to the retrofit CSV file
    """
    out_dir = src_path.parent / "client"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / "priced_subscriptions.csv"

    pp_lookup = load_price_plan_lookup()
    cust_lookup = load_customer_lookup()

    rows_out: list[dict] = []
    missing_price = 0
    missing_customer = 0

    with src_path.open() as f:
        for r in csv.DictReader(f):
            if r["outcome"] != "PATCHED":
                continue
            pp_ext = r["pp_ext_id"].strip()
            net_price, currency = pp_lookup.get(pp_ext, ("", ""))
            if not net_price:
                missing_price += 1
            customer = cust_lookup.get(r["deal_id"], "")
            if not customer:
                missing_customer += 1
            rows_out.append(
                {
                    "Customer": customer,
                    "NS Subscription ID": r["subscription_ns_id"],
                    "NS Line #": r["line_number"],
                    "Subscription Item": r["item_name"],
                    "NS Price Plan ID": r["pp_ns_id"],
                    "Price Plan External ID": pp_ext,
                    "Net Price": net_price,
                    "Currency": currency,
                    "HubSpot Deal ID": r["deal_id"],
                    "HubSpot Line Item ID": extract_line_item_id(pp_ext),
                }
            )

    rows_out.sort(key=lambda r: (r["HubSpot Deal ID"], int(r["NS Line #"] or 0)))

    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows_out[0].keys()))
        w.writeheader()
        w.writerows(rows_out)

    print(f"Source retrofit : {src_path}")
    print(f"Price plans     : {PRICE_PLANS_CSV.name} ({len(pp_lookup)} plans loaded)")
    print(f"Customers       : {SUBS_CSV.name} ({len(cust_lookup)} deals loaded)")
    print(f"Output          : {out_path}")
    print(f"  rows written  : {len(rows_out)}")
    if missing_price:
        print(f"  ⚠ rows with no net_price match : {missing_price}")
    if missing_customer:
        print(f"  ⚠ rows with no customer match  : {missing_customer}")


if __name__ == "__main__":
    src = Path(sys.argv[1]) if len(sys.argv) > 1 else find_latest_retrofit_csv()
    main(src)
