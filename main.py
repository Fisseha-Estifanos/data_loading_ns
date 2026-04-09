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


# ─── Report ─────────────────────────────────────────────────────────────


def print_report(tracker: StateTracker, show_failures: bool = False):
    """Print a summary of the current load state."""
    print("\n" + "=" * 70)
    print("  LOAD STATE REPORT")
    print("=" * 70)

    for entity in ENTITY_ORDER:
        summary = tracker.summary(entity)
        if not summary:
            print(f"\n  {entity:20s}  — no records tracked")
            continue

        total = sum(summary.values())
        print(f"\n  {entity:20s}  total={total}")
        for status, count in sorted(summary.items()):
            print(f"    {status:20s}: {count}")

        # Missing IDs warning
        missing = tracker.get_missing_ids(entity)
        if missing:
            print(f"    ⚠ {len(missing)} records created but NS ID unresolved")

    if show_failures:
        print("\n" + "-" * 70)
        print("  FAILED RECORDS")
        print("-" * 70)
        for entity in ENTITY_ORDER:
            failed = tracker.get_failed(entity)
            if not failed:
                continue
            print(f"\n  [{entity}] — {len(failed)} failures:")
            for rec in failed:
                print(f"    extId={rec['external_id']}")
                print(f"      error: {rec['error_message'][:200]}")
                print(f"      at:    {rec['attempted_at']}")

    print("\n" + "=" * 70 + "\n")


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
        # Report mode
        if args.report:
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
