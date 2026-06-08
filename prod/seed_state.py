#!/usr/bin/env python3
"""
Seed an existing NetSuite record into the (prod) state DB — LOCAL WRITE ONLY
=============================================================================
When a parent entity ALREADY exists in NetSuite (e.g. customer C43837 is live
in prod), the loader has no way to discover its internal ID — a fresh prod
state DB is empty, and we don't want to re-create the customer. This script
writes a single row into the state DB telling the loader "this entity already
exists in NS, here's its internal ID", so dependent records (the subscription,
its billing account, etc.) resolve and link to the real record.

It writes ONLY to the local SQLite state DB (config.STATE_DB / NS_STATE_DB).
It never touches NetSuite — no GET, POST, PATCH, nothing.

The state-DB key the loader looks up is the *revisioned* externalId:
    config.apply_revision(<raw External ID 2>)        e.g. MP_HubSpot_123_rvn_02
This script applies the current config.LOAD_REVISION for you by default.

Which DB? It writes to config.STATE_DB, which honours NS_STATE_DB. Point that
at your prod DB first so you don't seed the sandbox DB by mistake:

    export $(grep -v '^#' .env.prod | xargs)          # sets NS_STATE_DB=...prod.db

Typical flow for the C43837 subscription
----------------------------------------
    # 1. find the existing customer's internal ID in prod
    python prod/prod_check.py customer --entityid C43837
    #    → customer id=800123 ...

    # 2. preview the seed (dry-run is the default)
    python prod/seed_state.py --company-name "ACME PAYROLL LTD" --netsuite-id 800123
    #    (or pass --external-id <raw External ID 2> instead of --company-name)

    # 3. apply it
    python prod/seed_state.py --company-name "ACME PAYROLL LTD" --netsuite-id 800123 --apply

Then load only pricePlan + subscription. No customer/billing load needed.
"""

import argparse
import csv
import sys
from pathlib import Path

_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import config  # noqa: E402
from state_tracker import StateTracker  # noqa: E402

OK = "\033[92m✓\033[0m"
NO = "\033[91m✗\033[0m"
WARN = "\033[93m⚠\033[0m"


def resolve_external_id_from_csv(company_name: str) -> str | None:
    """Mirror the subscription loader: company name → customer CSV External ID 2."""
    target = company_name.strip().upper()
    try:
        with open(config.CUSTOMERS_CSV, "r", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                if row.get("Company Name", "").strip().upper() == target:
                    return row.get("External ID 2", "").strip() or None
    except FileNotFoundError:
        print(f"{NO} Customer CSV not found: {config.CUSTOMERS_CSV}")
        return None
    return None


def main():
    parser = argparse.ArgumentParser(
        description="Seed an existing NS record's internal ID into the state DB "
        "(local SQLite write only — never touches NetSuite).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--netsuite-id",
        required=True,
        help="The internal ID the record ALREADY has in NetSuite (from prod_check.py)",
    )
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument(
        "--external-id",
        help="Raw externalId (e.g. customer 'External ID 2'). Revision applied automatically.",
    )
    src.add_argument(
        "--company-name",
        help="Resolve the raw externalId from the customer CSV by company name.",
    )
    parser.add_argument(
        "--entity",
        default="customer",
        help="Entity type to seed (default: customer). E.g. billingAccount, pricePlan.",
    )
    parser.add_argument(
        "--no-revision",
        action="store_true",
        help="Use the externalId exactly as given, without appending LOAD_REVISION.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually write. Without this flag the script only previews (dry-run).",
    )
    args = parser.parse_args()

    # Resolve the raw externalId
    if args.company_name:
        raw_ext_id = resolve_external_id_from_csv(args.company_name)
        if not raw_ext_id:
            print(
                f"{NO} Could not resolve 'External ID 2' for company "
                f"{args.company_name!r} in {config.CUSTOMERS_CSV}"
            )
            sys.exit(1)
        print(f"  Resolved {args.company_name!r} → External ID 2 = {raw_ext_id}")
    else:
        raw_ext_id = args.external_id.strip()

    key = raw_ext_id if args.no_revision else config.apply_revision(raw_ext_id)

    # Banner — make the target DB unmistakable
    realm_raw = config.BASE_URL.split("//")[1].split(".")[0]
    print("─" * 64)
    print(f"  State DB (target)  : {config.STATE_DB}")
    print(f"  Implied NS account : {realm_raw}")
    print(f"  Entity             : {args.entity}")
    print(f"  External ID (key)  : {key}")
    print(f"  NetSuite internal  : {args.netsuite_id}")
    print(
        f"  Mode               : {'APPLY (writing)' if args.apply else 'DRY RUN (preview)'}"
    )
    print("─" * 64)

    tracker = StateTracker()
    try:
        existing = tracker.get_status(args.entity, key)
        if existing:
            print(f"{WARN} A row already exists for ({args.entity}, {key}):")
            print(
                f"      netsuite_id={existing.get('netsuite_id')}  "
                f"status={existing.get('status')}"
            )
            if existing.get("netsuite_id") not in (None, "", args.netsuite_id):
                print(
                    f"{WARN} It maps to a DIFFERENT internal ID "
                    f"({existing.get('netsuite_id')} → {args.netsuite_id}). "
                    f"Applying will overwrite it."
                )

        if not args.apply:
            print(f"\n{WARN} DRY RUN — nothing written. Re-run with --apply to commit.")
            return

        tracker.upsert_state(
            entity_type=args.entity,
            external_id=key,
            status="success",
            netsuite_id=str(args.netsuite_id),
            error_message="Seeded manually: record pre-existed in NetSuite",
            tier_used="manual_seed",
        )
        # Read back to confirm
        confirmed = tracker.get_netsuite_id(args.entity, key)
        if confirmed == str(args.netsuite_id):
            print(f"\n{OK} Seeded: ({args.entity}, {key}) → {confirmed}")
            print(
                "  The loader will now resolve this parent to the existing NS record."
            )
        else:
            print(f"\n{NO} Write-back check failed: got {confirmed!r}")
            sys.exit(1)
    finally:
        tracker.close()


if __name__ == "__main__":
    main()
