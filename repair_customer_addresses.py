"""
repair_customer_addresses.py
=============================
One-time repair: PATCHes customers that exist in NetSuite but have no
addressBook entry (no defaultBilling / defaultShipping address).

The billing account loader requires both billAddressList and shipAddressList,
which reference the customer's address book. This script fills the gap.

How it works:
  1. Queries state tracker for all successfully loaded customers
  2. Queries NS via SuiteQL to find which of those lack address entries
  3. Reads address data from the customer CSV
  4. PATCHes each customer to add a single addressBook entry
     (defaultBilling=true, defaultShipping=true)
  5. Re-queries NS to verify the addresses landed
  6. Logs everything to terminal + log file

Key fix vs original loader:
  - Blank country → defaults to GB (subsidiary 12) or IE (subsidiary 66)
    instead of omitting country, which caused NS to silently drop the address.

Usage:
    python repair_customer_addresses.py --dry-run
    python repair_customer_addresses.py

Place alongside main.py in the data_loading directory.
"""

import csv
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# ── Path setup (same pattern as main.py) ────────────────────────────────
_PROJECT_ROOT = str(Path(__file__).resolve().parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import config
from netsuite_client import NetSuiteClient
from state_tracker import StateTracker

# ── Logging: dual output (file + terminal) ──────────────────────────────
log_dir = Path("logs") / datetime.now().strftime("%Y-%m-%d")
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / f"repair_addresses_{datetime.now().strftime('%H-%M-%S')}.log"

logger = logging.getLogger("repair_addresses")
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler(log_file)
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)-7s] %(message)s"))
logger.addHandler(fh)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)-7s] %(message)s"))
logger.addHandler(ch)

# ── Country mapping (mirrors loaders/customer.py exactly) ───────────────
COUNTRY_MAP = {
    "united kingdom": "GB",
    "ireland": "IE",
    "the netherlands": "NL",
    "united arab emirates": "AE",
    "hampshire": "GB",
    "luton": "GB",
}


def build_address_patch(row: dict) -> dict | None:
    """
    Build a PATCH payload with a single addressBook entry from a customer CSV row.

    Key difference from the original _build_address: blank country is resolved
    using the subsidiary ID (66 → IE, else → GB) rather than being omitted.
    """
    addr1 = row.get("Address 1 : Address 1", "").strip()
    addr2 = row.get("Address 1 : Address 2", "").strip()
    city = row.get("Address 1 : City", "").strip()
    state = row.get("Address 1 : County", "").strip()
    zip_code = row.get("Address 1 : Post Code", "").strip()
    country_raw = row.get("Address 1 : Country (Req) 1", "").strip()
    addressee = row.get("addressee", "").strip()
    attention_first = row.get("Attention First Name", "").strip()
    attention_last = row.get("Attention Last Name", "").strip()
    subsidiary = row.get("Primary Entity (Req)", "").strip()

    if not any([addr1, city, zip_code]):
        return None

    # Resolve country code — the repair fix
    country_code = COUNTRY_MAP.get(country_raw.lower(), "")
    if not country_code and country_raw:
        # Unmapped but non-blank: default based on subsidiary
        fallback = "IE" if subsidiary == "66" else "GB"
        logger.warning(
            f"  Unmapped country '{country_raw}' → defaulting to {fallback} "
            f"(subsidiary={subsidiary})"
        )
        country_code = fallback
    elif not country_code:
        # Blank country: default based on subsidiary
        fallback = "IE" if subsidiary == "66" else "GB"
        logger.warning(
            f"  Blank country field → defaulting to {fallback} "
            f"(subsidiary={subsidiary})"
        )
        country_code = fallback

    # Build inner address, omitting None/empty values
    address_fields = {
        "addressee": addressee or None,
        "attention": f"{attention_first} {attention_last}".strip() or None,
        "addr1": addr1 or None,
        "addr2": addr2 or None,
        "city": city or None,
        "state": state or None,
        "zip": zip_code if zip_code and zip_code != "." else None,
        "country": {"id": country_code},
    }
    address_fields = {k: v for k, v in address_fields.items() if v is not None}

    return {
        "addressBook": {
            "items": [
                {
                    "defaultBilling": True,
                    "defaultShipping": True,
                    "label": "Primary",
                    "addressBookAddress": address_fields,
                }
            ]
        }
    }


def find_customers_missing_addresses(
    client: NetSuiteClient, tracker: StateTracker
) -> dict:
    """
    Cross-reference state tracker with NS customeraddressbook to find
    customers that exist in NS but have no DEFAULT billing/shipping address.

    This matches the billing_account loader's _load_address_maps filter:
    only addresses where defaultbilling='T' OR defaultshipping='T' count.

    Returns: {external_id: ns_internal_id}
    """
    rows = tracker.conn.execute(
        "SELECT external_id, netsuite_id FROM load_state "
        "WHERE entity_type = 'customer' "
        "  AND status = 'success' "
        "  AND netsuite_id IS NOT NULL"
    ).fetchall()
    loaded = {r["external_id"]: r["netsuite_id"] for r in rows}

    logger.info(f"State tracker: {len(loaded)} successfully loaded customers")

    # Query NS in batches for which of these have addresses
    ns_ids = list(loaded.values())
    customers_with_addr = set()
    batch_size = 200

    for i in range(0, len(ns_ids), batch_size):
        batch = ns_ids[i : i + batch_size]
        in_clause = ",".join(f"'{nid}'" for nid in batch)
        # Must match billing_account.py's _load_address_maps filter:
        # only addresses with defaultbilling='T' OR defaultshipping='T'
        query = (
            f"SELECT DISTINCT entity FROM customeraddressbook "
            f"WHERE (defaultbilling = 'T' OR defaultshipping = 'T') "
            f"AND entity IN ({in_clause})"
        )
        try:
            result = client.suiteql_query(query)
            for row in result:
                customers_with_addr.add(str(row.get("entity", "")))
        except Exception as e:
            logger.error(f"SuiteQL batch query failed: {e}")

    logger.info(
        f"NS addressbook: {len(customers_with_addr)} of {len(loaded)} "
        f"customers have default billing/shipping addresses"
    )

    missing = {}
    for ext_id, ns_id in loaded.items():
        if str(ns_id) not in customers_with_addr:
            missing[ext_id] = ns_id

    return missing


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Repair missing customer addresses in NetSuite"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Preview patches without executing"
    )
    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("CUSTOMER ADDRESS REPAIR")
    logger.info(f"Log file: {log_file}")
    if args.dry_run:
        logger.info("MODE: DRY RUN — no changes will be made")
    logger.info("=" * 70)

    client = NetSuiteClient()
    tracker = StateTracker()

    # ── Step 1: Find customers missing addresses ────────────────────────
    logger.info("")
    logger.info("Step 1: Identifying customers missing addresses in NetSuite...")
    missing = find_customers_missing_addresses(client, tracker)

    if not missing:
        logger.info(
            "All loaded customers have default billing/shipping addresses. Nothing to repair."
        )
        tracker.close()
        return

    logger.info(f"Found {len(missing)} customer(s) missing addresses:")
    for ext_id, ns_id in sorted(missing.items()):
        logger.info(f"  {ext_id} → NS ID {ns_id}")

    # ── Step 2: Load address data from CSV ──────────────────────────────
    logger.info("")
    logger.info(f"Step 2: Reading address data from {config.CUSTOMERS_CSV}...")

    csv_rows = {}
    with open(config.CUSTOMERS_CSV) as f:
        for row in csv.DictReader(f):
            ext_id = row.get("External ID 2", "").strip()
            if ext_id in missing:
                csv_rows[ext_id] = row

    logger.info(f"Matched {len(csv_rows)}/{len(missing)} to CSV rows")

    unmatched = set(missing.keys()) - set(csv_rows.keys())
    if unmatched:
        logger.warning(f"{len(unmatched)} customer(s) not in CSV — cannot repair:")
        for ext_id in sorted(unmatched):
            logger.warning(f"  {ext_id} (NS ID {missing[ext_id]})")

    # ── Step 3: Build and execute patches ───────────────────────────────
    logger.info("")
    logger.info("Step 3: Patching addresses...")

    stats = {"patched": 0, "failed": 0, "skipped": 0}

    for ext_id in sorted(csv_rows.keys()):
        row = csv_rows[ext_id]
        ns_id = missing[ext_id]
        company = row.get("Company Name", "").strip()

        logger.info("")
        logger.info(f"  [{ext_id}] {company} (NS ID: {ns_id})")

        patch = build_address_patch(row)
        if not patch:
            logger.warning(f"  → SKIPPED: no usable address data in CSV")
            stats["skipped"] += 1
            continue

        logger.info(f"  → Payload: {json.dumps(patch, indent=4)}")

        if args.dry_run:
            logger.info(f"  → DRY RUN: would PATCH /customer/{ns_id}")
            stats["patched"] += 1
            continue

        # Execute PATCH via netsuite_client._request (no dedicated update method)
        try:
            url = f"{client.base_url}/customer/{ns_id}"
            time.sleep(config.REQUEST_DELAY_SECONDS)
            resp = client._request("PATCH", url, patch)

            if resp.status_code in (200, 204):
                logger.info(f"  → PATCHED OK (HTTP {resp.status_code})")
                stats["patched"] += 1
            else:
                body = resp.text[:500]
                logger.error(f"  → FAILED: HTTP {resp.status_code}: {body}")
                stats["failed"] += 1

        except Exception as e:
            logger.error(f"  → FAILED: {e}")
            stats["failed"] += 1

    # ── Step 4: Verify ──────────────────────────────────────────────────
    if not args.dry_run and stats["patched"] > 0:
        logger.info("")
        logger.info("Step 4: Verifying addresses were created...")
        time.sleep(2)

        still_missing = find_customers_missing_addresses(client, tracker)
        fixed = set(missing.keys()) - set(still_missing.keys())
        broken = set(missing.keys()) & set(still_missing.keys())

        logger.info(f"  Verified: {len(fixed)} now have addresses")
        if broken:
            logger.warning(f"  Still missing: {len(broken)}:")
            for ext_id in sorted(broken):
                logger.warning(f"    {ext_id} (NS ID {missing[ext_id]})")

    # ── Summary ─────────────────────────────────────────────────────────
    logger.info("")
    logger.info("=" * 70)
    logger.info("SUMMARY")
    logger.info("=" * 70)
    logger.info(f"  Patched:  {stats['patched']}")
    logger.info(f"  Failed:   {stats['failed']}")
    logger.info(f"  Skipped:  {stats['skipped']}")
    if args.dry_run:
        logger.info("  (DRY RUN — no changes made)")
    logger.info("=" * 70)

    tracker.close()


if __name__ == "__main__":
    main()
