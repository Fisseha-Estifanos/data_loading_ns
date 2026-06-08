#!/usr/bin/env python3
"""
Patch a customer's address so the billing-account loader can attach it
=======================================================================
The billing-account loader resolves billAddressList / shipAddressList from the
customer's DEFAULT billing + shipping address in NetSuite
(customeraddressbook where defaultbilling='T' / defaultshipping='T'). If the
customer has no default-flagged address, the BA load skips the record.

This script fixes that, two ways — GET-first so it never blindly wipes the
address sublist:

  Mode A (default)  --set-defaults
      The customer ALREADY has an address; we only flag the chosen line as
      default billing + shipping. No address data is invented.

  Mode B            --add  (with --addr1/--city/--zip/--country ...)
      The customer has NO address; add one (with defaults) from values YOU
      supply from the source — never guessed.

Writes to NetSuite (PATCH /customer/{id}). It is dry-run by default; pass
--apply to commit. Always prints the exact payload first.

Examples
--------
    # See what's there first (read-only):
    python prod/prod_check.py record --type customer --id 802818

    # Case 1 — address exists, just flag it default:
    python prod/patch_customer_address.py --customer-id 802818 --set-defaults
    python prod/patch_customer_address.py --customer-id 802818 --set-defaults --apply

    # Case 2 — no address; add one from source values:
    python prod/patch_customer_address.py --customer-id 802818 --add \
        --addr1 "1 High St" --city "Sheffield" --zip "S1 2AB" --country GB --apply
"""
import argparse
import json
import sys
from pathlib import Path

_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import csv  # noqa: E402

import config  # noqa: E402
from netsuite_client import NetSuiteClient  # noqa: E402
from loaders.customer import CustomerLoader  # noqa: E402

OK = "\033[92m✓\033[0m"
NO = "\033[91m✗\033[0m"
WARN = "\033[93m⚠\033[0m"


def get_customer(client, internal_id=None, external_id=None):
    if internal_id:
        url = f"{config.BASE_URL}/customer/{internal_id}?expandSubResources=true"
        resp = client._request("GET", url)
        if resp.status_code == 200:
            return resp.json()
        print(f"{NO} GET customer/{internal_id} → HTTP {resp.status_code}: {resp.text[:300]}")
        return None
    rec = client.get_by_external_id("customer", external_id)
    if not rec:
        print(f"{NO} No customer with externalId={external_id}")
    return rec


def main():
    p = argparse.ArgumentParser(
        description="Patch a customer's address / default flags (PATCH to NS; dry-run by default).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--customer-id", help="Customer internal ID (e.g. 802818)")
    src.add_argument("--externalid", help="Customer externalId")

    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--set-defaults", action="store_true",
                      help="Mode A: flag the existing address as default bill+ship")
    mode.add_argument("--add", action="store_true",
                      help="Mode B: add a new address (provide --addr1/--city/--zip/--country)")
    mode.add_argument("--from-csv", action="store_true",
                      help="Mode C: build the address from config.CUSTOMERS_CSV using the "
                           "same logic as the customer loader (needs --company-name)")

    p.add_argument("--company-name", help="Mode C: company to match in the customer CSV")
    p.add_argument("--line-id", help="Mode A: which address line id to flag (default: first)")
    # Mode B address fields (only used with --add)
    p.add_argument("--addr1")
    p.add_argument("--addr2")
    p.add_argument("--city")
    p.add_argument("--state")
    p.add_argument("--zip")
    p.add_argument("--country", help="ISO code, e.g. GB / IE (NOT a display name)")
    p.add_argument("--addressee")
    p.add_argument("--attention")

    p.add_argument("--apply", action="store_true",
                   help="Actually PATCH. Without this, dry-run preview only.")
    args = p.parse_args()

    realm = config.BASE_URL.split("//")[1].split(".")[0]
    print("─" * 64)
    print(f"  NetSuite account : {realm}")
    print(f"  Mode             : {'APPLY (writing)' if args.apply else 'DRY RUN (preview)'}")
    print("─" * 64)

    client = NetSuiteClient()
    cust = get_customer(client, args.customer_id, args.externalid)
    if not cust:
        sys.exit(1)
    cust_id = cust.get("id")
    print(f"  Customer id={cust_id}  entityId={cust.get('entityId')}  "
          f"company={cust.get('companyName')!r}")

    existing = cust.get("addressBook", {}).get("items", [])
    print(f"  Existing address lines: {len(existing)}")
    for a in existing:
        addr = a.get("addressBookAddress", {})
        print(f"    line id={a.get('id')}  defBill={a.get('defaultBilling')}  "
              f"defShip={a.get('defaultShipping')}  "
              f"addr1={addr.get('addr1')!r} city={addr.get('city')!r} zip={addr.get('zip')!r}")

    # ── Build the addressBook item ───────────────────────────────────────
    if args.set_defaults:
        if not existing:
            print(f"{NO} Customer has NO address to flag. Use --add with source "
                  f"address values (do NOT invent an address).")
            sys.exit(1)
        if args.line_id:
            target = next((a for a in existing if str(a.get("id")) == str(args.line_id)), None)
            if not target:
                print(f"{NO} No address line with id={args.line_id}")
                sys.exit(1)
        else:
            target = existing[0]
        item = {"id": target.get("id"), "defaultBilling": True, "defaultShipping": True}
        print(f"\n  Flagging existing line id={target.get('id')} as default bill+ship.")
    elif args.from_csv:
        if not args.company_name:
            print(f"{NO} --from-csv needs --company-name to match a row in "
                  f"{config.CUSTOMERS_CSV}")
            sys.exit(1)
        target_name = args.company_name.strip().upper()
        row = None
        with open(config.CUSTOMERS_CSV, "r", encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                if r.get("Company Name", "").strip().upper() == target_name:
                    row = r
                    break
        if row is None:
            print(f"{NO} No row for company {args.company_name!r} in {config.CUSTOMERS_CSV}")
            sys.exit(1)
        # Reuse the loader's address builder (country mapping, attention join,
        # defaultBilling/defaultShipping=True) so it's identical to every other
        # customer's address. _build_address doesn't use instance state.
        item = CustomerLoader(client, None)._build_address(row)
        if item is None:
            print(f"{NO} Row for {args.company_name!r} has no usable address "
                  f"(addr1/city/zip all blank).")
            sys.exit(1)
        addr = item.get("addressBookAddress", {})
        print(f"\n  Built address from CSV: addr1={addr.get('addr1')!r} "
              f"city={addr.get('city')!r} zip={addr.get('zip')!r} "
              f"country={addr.get('country')}")
    else:  # --add
        if not args.addr1 and not args.city and not args.zip:
            print(f"{NO} --add needs real address values (--addr1/--city/--zip/--country). "
                  f"Never invent an address.")
            sys.exit(1)
        addr = {
            "addressee": args.addressee,
            "attention": args.attention,
            "addr1": args.addr1,
            "addr2": args.addr2,
            "city": args.city,
            "state": args.state,
            "zip": args.zip,
            "country": {"id": args.country} if args.country else None,
        }
        addr = {k: v for k, v in addr.items() if v is not None}
        item = {
            "defaultBilling": True,
            "defaultShipping": True,
            "label": "Primary",
            "addressBookAddress": addr,
        }

    payload = {"addressBook": {"items": [item]}}
    print("\n  PATCH payload:")
    print(json.dumps(payload, indent=2))
    print(f"\n  → PATCH {config.BASE_URL}/customer/{cust_id}")

    if not args.apply:
        print(f"\n{WARN} DRY RUN — nothing sent. Re-run with --apply to commit.")
        return

    resp = client._request("PATCH", f"{config.BASE_URL}/customer/{cust_id}", payload)
    if resp.status_code in (200, 204):
        print(f"\n{OK} PATCH succeeded (HTTP {resp.status_code}).")
        print("  Verify: python prod/prod_check.py address --customer-id " + str(cust_id))
    else:
        print(f"\n{NO} PATCH failed HTTP {resp.status_code}: {resp.text[:500]}")
        sys.exit(1)


if __name__ == "__main__":
    main()
