#!/usr/bin/env python3
"""
NetSuite Data Loader — Main Orchestrator
==========================================
Runs entity loaders in dependency order:
  1. Customer
  2. Billing Account  (← depends on Customer)
  3. Subscription      (← depends on Customer + Billing Account)
  4. One-Off Invoice   (← depends on Customer)

Usage:
  python main.py                         # Run all entities in order
  python main.py --entity customer       # Run only customers
  python main.py --entity billingAccount # Run only billing accounts
  python main.py --entity subscription   # Run only subscriptions
  python main.py --entity oneOff         # Run only one-off invoices
  python main.py --report                # Show state summary without loading
  python main.py --report --failures     # Show all failed records with errors
  python main.py --retry-failed customer # Retry only previously failed records

Environment variables for credentials (or edit config.py):
  NS_CONSUMER_KEY, NS_CONSUMER_SECRET, NS_ACCESS_TOKEN, NS_TOKEN_SECRET, NS_REALM
"""
import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Ensure the project root is on sys.path so imports work
# regardless of which directory you run the script from.
_PROJECT_ROOT = str(Path(__file__).resolve().parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import config
from netsuite_client import NetSuiteClient
from state_tracker import StateTracker
from loaders import (
    CustomerLoader,
    BillingAccountLoader,
    SubscriptionLoader,
    OneOffLoader,
)

# ─── Logging Setup ──────────────────────────────────────────────────────


def setup_logging(log_dir: str = "logs"):
    gmt3 = timezone(timedelta(hours=3))
    now = datetime.now(gmt3)
    date_folder = os.path.join(log_dir, now.strftime("%Y-%m-%d"))
    os.makedirs(date_folder, exist_ok=True)
    log_file = os.path.join(date_folder, now.strftime("load_%H-%M-%S.log"))

    # Console: INFO and above
    # File: DEBUG and above (captures tier resolution details)
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)-7s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    # Quiet noisy libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests_oauthlib").setLevel(logging.WARNING)

    logging.info(f"Log file: {log_file}")
    return log_file


# ─── Entity Registry ────────────────────────────────────────────────────

ENTITY_ORDER = ["customer", "billingAccount", "subscription", "oneOff"]

LOADER_MAP = {
    "customer": CustomerLoader,
    "billingAccount": BillingAccountLoader,
    "subscription": SubscriptionLoader,
    "oneOff": OneOffLoader,
}


# ─── Preflight Checks ──────────────────────────────────────────────────


def preflight_check(client: NetSuiteClient) -> bool:
    """Verify connectivity and auth by fetching a single record."""
    logger = logging.getLogger("preflight")
    logger.info("Running preflight auth check...")
    try:
        # Try to fetch record metadata — lightweight call
        url = f"{config.BASE_URL}/customer"
        resp = client._request("GET", url + "?limit=1")
        if resp.status_code < 300:
            logger.info(f"✓ Preflight passed (HTTP {resp.status_code}). Connected to {config.REALM}")
            return True
        else:
            logger.error(
                f"✗ Preflight failed: HTTP {resp.status_code} — {resp.text[:300]}"
            )
            return False
    except Exception as e:
        logger.error(f"✗ Preflight failed: {e}")
        return False


# ─── Field Mapping Report ───────────────────────────────────────────────

# Static definition of every field pushed to NetSuite per loader.
# Columns:
#   csv_col   — exact column header name in the source CSV file (or None if hardcoded/computed)
#   api_field — NetSuite REST field name as sent in the JSON payload
#               (dot notation indicates nesting, e.g. subsidiary.id → {"subsidiary": {"id": ...}})
#   format    — how the value is sent:
#                 "direct"        → raw string value from CSV
#                 "direct|omit"   → raw string, field omitted entirely if blank
#                 "{id}"          → wrapped as {"id": value}
#                 "{id}+map"      → value is looked up in a map first, then wrapped as {"id": ...}
#                 "{refName}"     → wrapped as {"refName": value}
#                 "bool_coerce"   → CSV string "true"/"false" coerced to Python bool
#                 "hardcoded"     → not from CSV; value is fixed in code
#                 "computed"      → derived from multiple CSV columns or logic
#                 "resolved"      → looked up from state tracker (NS internal ID of parent)
#   dtype     — Python/JSON type that reaches the API
#   notes     — caveats, status, TODO items

FIELD_MAPS = {
    "customer": {
        "endpoint": "POST /record/v1/customer",
        "csv_file": "customers CSV",
        "fields": [
            ("External ID 2",                  "externalId",                              "direct",       "str",       ""),
            ("Company Name",                   "companyName",                             "direct",       "str",       ""),
            (None,                             "isPerson",                                "hardcoded",    "bool",      "Always False (company records)"),
            ("Primary Entity (Req)",           "subsidiary.id",                           "{id}",         "str",       "NS internal ID taken as-is from CSV"),
            ("Currency",                       "currency.id",                             "{id}+map",     "str",       "GBP→1, EUR→4 via CURRENCY_MAP"),
            ("Email",                          "email",                                   "direct|omit",  "str",       "Omitted if blank"),
            ("Phone",                          "phone",                                   "direct|omit",  "str",       "Omitted if blank"),
            ("Alt. Phone",                     "altPhone",                                "direct|omit",  "str",       "Omitted if blank"),
            ("Terms",                          "terms.refName",                           "{refName}",    "str",       "⚠ refName lookup — may need ID instead"),
            ("Job Title",                      "title",                                   "direct|omit",  "str",       "Omitted if blank"),
            ("Address 1 : Address 1",          "addressBook[0].addr1",                   "direct|omit",  "str",       "Nested in addressBook.items[0].addressBookAddress"),
            ("Address 1 : Address 2",          "addressBook[0].addr2",                   "direct|omit",  "str",       ""),
            ("Address 1 : City",               "addressBook[0].city",                    "direct|omit",  "str",       ""),
            ("Address 1 : County",             "addressBook[0].state",                   "direct|omit",  "str",       ""),
            ("Address 1 : Post Code",          "addressBook[0].zip",                     "direct|omit",  "str",       ""),
            ("Address 1 : Country (Req) 1",    "addressBook[0].country.id",              "{id}+map",     "str",       "Display name→ISO code via COUNTRY_MAP (e.g. 'united kingdom'→'GB')"),
            ("addressee",                      "addressBook[0].addressee",               "direct|omit",  "str",       ""),
            ("Attention First Name",           "addressBook[0].attention",               "computed",     "str",       "Joined as 'First Last'; omitted if both blank"),
            ("Attention Last Name",            "addressBook[0].attention",               "computed",     "str",       "See above — combined with First Name"),
        ],
        "not_sent": [
            "Company Reg Number", "Segment", "Direct Debit", "Business/Class",
            "Dunning Procedure", "Dunning Contact First Name", "Dunning Contact Last Name",
            "Dunning Level (Req)", "Email Preference", "Allow Letters to be Emailed",
            "Electronic Email Recipients", "Indexation Date", "PO Mandatory",
        ],
    },

    "billingAccount": {
        "endpoint": "POST /record/v1/billingAccount",
        "csv_file": "billing CSV",
        "fields": [
            ("externalId",                     "externalId",                              "direct",       "str",       ""),
            ("name",                           "name",                                    "direct",       "str",       ""),
            ("customer_externalId",            "customer.id",                             "resolved",     "str",       "Looked up from state tracker using customer externalId → NS internal ID"),
            ("subsidiary_id",                  "subsidiary.id",                           "{id}",         "str",       "Already NS internal ID in CSV"),
            ("currency_id",                    "currency.id",                             "{id}",         "str",       "Already NS internal ID in CSV"),
            ("billingSchedule_id",             "billingSchedule.id",                      "{id}|omit",    "str",       "Omitted if blank"),
            ("frequency",                      "frequency.id",                            "{id}",         "str",       "e.g. 'MONTHLY'"),
            ("startDate",                      "startDate",                               "direct",       "str",       "ISO date string"),
            ("customerDefault",                "customerDefault",                         "bool_coerce",  "bool",      "CSV 'true'/'false' string → Python bool"),
            ("requestOffCycleInvoice",         "requestOffCycleInvoice",                  "bool_coerce",  "bool",      ""),
            ("inactive",                       "inactive",                                "bool_coerce",  "bool",      ""),
            ("billAddressList_parked",         "billAddressList.id",                      "{id}|omit",    "str",       "⚠ Mostly null in data"),
            ("shipAddressList_parked",         "shipAddressList.id",                      "{id}|omit",    "str",       "⚠ Mostly null in data"),
        ],
        "not_sent": [],
    },

    "subscription": {
        "endpoint": "POST /record/v1/subscription",
        "csv_file": "subscriptions CSV (70 rows → 49 grouped subscriptions)",
        "fields": [
            ("External ID",                    "externalId",                              "direct",       "str",       "Deal ID; one subscription per unique External ID"),
            ("Subscription Name",              "name",                                    "direct",       "str",       ""),
            ("Customer",                       "customer.id",                             "resolved",     "str",       "Company name→customer extId→state tracker→NS internal ID"),
            ("Subsidiary",                     "subsidiary.id",                           "{id}+map",     "str",       "Display name→NS ID via SUBSIDIARY_MAP (Moorepay Ltd→12, Ireland→66)"),
            ("Currency",                       "currency.id",                             "{id}+map",     "str",       "GBP→1, EUR→4 via CURRENCY_MAP"),
            ("Start Date",                     "startDate",                               "direct",       "str",       "ISO date string"),
            ("End Date",                       "endDate",                                 "direct|omit",  "str",       "Omitted if blank"),
            ("Initial Term",                   "initialTerm",                             "direct|omit",  "str",       "Omitted if blank"),
            (None,                             "billingAccount.id",                       "resolved",     "str",       "Looked up as {ext_id}_BA from state tracker; omitted if not found"),
            ("Subscription Plan",              "subscriptionPlan.refName",                "{refName}|omit","str",      "⚠ refName lookup — needs NS ID resolved"),
            ("Price Book",                     "priceBook.refName",                       "{refName}|omit","str",      "Omitted if blank or 'NOT MAPPED'"),
            ("PO#",                            "poNumber",                                "direct|omit",  "str",       "Omitted if blank"),
            # Line items (one per CSV row in the group)
            ("Sales Item",                     "subscriptionLine[n].item.refName",        "{refName}",    "str",       "⚠ refName lookup — needs NS ID; row skipped if 'NOT MAPPED'"),
            ("Lines: Include",                 "subscriptionLine[n].include",             "computed",     "bool",      "CSV 'T' → True, else False"),
            (None,                             "subscriptionLine[n].subscriptionLineType","hardcoded",    "str",       "Always '1' (standard line type)"),
        ],
        "not_sent": [
            "CPI Type", "Default Renewal Term", "Indexation Date",
        ],
    },

    "oneOff": {
        "endpoint": "POST /record/v1/invoice",
        "csv_file": "one-off CSV",
        "fields": [
            ("Invoice External ID",            "externalId",                              "direct",       "str",       ""),
            ("Customer (Req)",                 "entity.id",                               "resolved",     "str",       "Company name→customer extId→state tracker→NS internal ID"),
            ("Subsidiary",                     "subsidiary.id",                           "{id}+map",     "str",       "Display name→NS ID via SUBSIDIARY_MAP"),
            ("Currency",                       "currency.id",                             "{id}+map",     "str",       "GBP→1, EUR→4 via CURRENCY_MAP"),
            ("Date (Req)",                     "tranDate",                                "direct",       "str",       "ISO date string"),
            ("Item",                           "item.items[0].item.refName",              "{refName}|omit","str",      "⚠ refName lookup; omitted if blank or 'NOT MAPPED'"),
            ("Quantity",                       "item.items[0].quantity",                  "direct",       "float",     "Parsed as float; record skipped if blank"),
            ("Rate per line item",             "item.items[0].rate",                      "direct",       "str",       ""),
            ("Description",                    "item.items[0].description",               "direct|omit",  "str",       ""),
        ],
        "not_sent": [
            "Revenue Start Date Per Line Item",
            "Revenue End Date Per Line Item",
        ],
    },
}


def print_field_mapping_report():
    """Print a structured report of CSV column → NetSuite API field mappings for all loaders."""
    report_logger = logging.getLogger("report")

    lines = []
    lines.append("\n" + "═" * 90)
    lines.append("  PAYLOAD FIELD MAPPING REPORT")
    lines.append("  Which CSV columns are sent to each NetSuite endpoint, how they are passed,")
    lines.append("  and what field names and types are used in the API payload.")
    lines.append("═" * 90)

    for entity, spec in FIELD_MAPS.items():
        lines.append(f"\n── {entity.upper()}  ({spec['endpoint']}) {'─' * max(0, 72 - len(entity) - len(spec['endpoint']))}")
        lines.append(f"   Source: {spec['csv_file']}")
        lines.append("")

        # Header row
        hdr = f"   {'CSV Column':<42}  {'API Field':<42}  {'Format':<16}  {'Type':<7}  Notes"
        lines.append(hdr)
        lines.append("   " + "─" * 135)

        for csv_col, api_field, fmt, dtype, notes in spec["fields"]:
            csv_display = csv_col if csv_col else "[hardcoded/computed]"
            row = (
                f"   {csv_display:<42}  {api_field:<42}  {fmt:<16}  {dtype:<7}  {notes}"
            )
            lines.append(row)

        if spec.get("not_sent"):
            lines.append("")
            lines.append("   NOT sent to API (unmapped/deferred):")
            lines.append(f"     {', '.join(spec['not_sent'])}")

    lines.append("\n" + "─" * 90)
    lines.append("  Format key:")
    lines.append('    direct         Raw string value from CSV, no transformation')
    lines.append('    direct|omit    Raw string; field omitted entirely if blank')
    lines.append('    {id}           Wrapped as {"id": value}')
    lines.append('    {id}+map       Value passed through a lookup map, then wrapped as {"id": ...}')
    lines.append('    {refName}      Wrapped as {"refName": value}')
    lines.append('    {id}|omit      Wrapped as {"id": value} but omitted if blank')
    lines.append('    bool_coerce    CSV string "true"/"false" coerced to Python bool')
    lines.append('    hardcoded      Not from CSV; fixed value in code')
    lines.append('    computed       Derived from multiple columns or conditional logic')
    lines.append('    resolved       NS internal ID looked up from state tracker (parent entity)')
    lines.append("═" * 90 + "\n")

    output = "\n".join(lines)
    print(output)
    report_logger.info(output)


# ─── Report ─────────────────────────────────────────────────────────────


def print_report(tracker: StateTracker, show_failures: bool = False):
    """Print a summary of the current load state — to both terminal and log file."""
    report_logger = logging.getLogger("report")

    lines = []
    lines.append("\n" + "=" * 70)
    lines.append("  LOAD STATE REPORT")
    lines.append("=" * 70)

    for entity in ENTITY_ORDER:
        summary = tracker.summary(entity)
        if not summary:
            lines.append(f"\n  {entity:20s}  — no records tracked")
            continue

        total = sum(summary.values())
        lines.append(f"\n  {entity:20s}  total={total}")
        for status, count in sorted(summary.items()):
            lines.append(f"    {status:20s}: {count}")

        missing = tracker.get_missing_ids(entity)
        if missing:
            lines.append(f"    ⚠ {len(missing)} records created but NS ID unresolved")

    if show_failures:
        lines.append("\n" + "-" * 70)
        lines.append("  FAILED RECORDS")
        lines.append("-" * 70)
        for entity in ENTITY_ORDER:
            failed = tracker.get_failed(entity)
            if not failed:
                continue
            lines.append(f"\n  [{entity}] — {len(failed)} failures:")
            for rec in failed:
                lines.append(f"    extId={rec['external_id']}")
                lines.append(f"      error: {rec['error_message'][:500]}")
                lines.append(f"      at:    {rec['attempted_at']}")

    lines.append("\n" + "=" * 70 + "\n")

    output = "\n".join(lines)
    # Print to terminal
    print(output)
    # Also write to log file
    report_logger.info(output)


# ─── Main ───────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="NetSuite Data Loader")
    parser.add_argument(
        "--entity", choices=ENTITY_ORDER, help="Load only this entity type"
    )
    parser.add_argument(
        "--report", action="store_true", help="Print state report without loading"
    )
    parser.add_argument(
        "--failures", action="store_true", help="Include failure details in report"
    )
    parser.add_argument(
        "--field-map", action="store_true",
        help="Print the CSV column → API field mapping report for all loaders"
    )
    parser.add_argument(
        "--skip-preflight", action="store_true", help="Skip auth preflight check"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build payloads and log them without calling the API",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Process only the first N records (useful for testing a single POST)",
    )
    args = parser.parse_args()

    log_file = setup_logging()
    logger = logging.getLogger("main")

    try:
        _run(args, logger)
    except Exception:
        logger.exception("Unhandled error — see traceback above")
        sys.exit(1)


def _run(args, logger):

    # Field mapping report — no credentials or tracker needed
    if args.field_map:
        print_field_mapping_report()
        return

    # Validate credentials are set
    if not config.CONSUMER_KEY:
        logger.error(
            "Credentials not configured. Source your .env file first:\n"
            "  export $(grep -v '^#' .env | xargs)"
        )
        sys.exit(1)

    # Init components
    client = NetSuiteClient()
    tracker = StateTracker()

    try:
        # Report mode — include field mapping at top
        if args.report:
            print_field_mapping_report()
            print_report(tracker, show_failures=args.failures)
            return

        # Preflight
        if not args.skip_preflight and not args.dry_run:
            if not preflight_check(client):
                logger.error("Preflight failed. Fix auth/connectivity and retry.")
                sys.exit(1)

        # Determine which entities to load
        entities_to_load = [args.entity] if args.entity else ENTITY_ORDER

        # Dependency check: warn if loading a child without its parent
        dependency_map = {
            "billingAccount": ["customer"],
            "subscription": ["customer", "billingAccount"],
            "oneOff": ["customer"],
        }
        if args.entity and args.entity in dependency_map:
            for dep in dependency_map[args.entity]:
                dep_summary = tracker.summary(dep)
                if not dep_summary or dep_summary.get("success", 0) == 0:
                    logger.warning(
                        f"⚠ Loading {args.entity} but dependency '{dep}' has no successful records. "
                        f"Parent references may fail."
                    )

        # Run loaders
        results = {}
        for entity in entities_to_load:
            loader_class = LOADER_MAP[entity]
            loader = loader_class(client, tracker)

            if args.dry_run:
                logger.info(f"DRY RUN: preparing {entity} payloads...")
                records = loader.prepare_records()
                if args.limit is not None:
                    records = records[: args.limit]
                logger.info(
                    f"DRY RUN: {len(records)} {entity} record(s) would be created"
                )
                for ext_id, payload, _ in records:
                    logger.info(
                        f"  Payload for {ext_id}:\n{json.dumps(payload, indent=2)}"
                    )
                results[entity] = {"total": len(records), "dry_run": True}
            else:
                results[entity] = loader.load_all(limit=args.limit)

        # Final summary
        logger.info("\n" + "=" * 50)
        logger.info("LOAD COMPLETE — SUMMARY")
        logger.info("=" * 50)
        for entity, result in results.items():
            logger.info(f"  {entity}: {result}")

        # Print report
        print_report(tracker, show_failures=True)

    finally:
        tracker.close()


if __name__ == "__main__":
    main()
