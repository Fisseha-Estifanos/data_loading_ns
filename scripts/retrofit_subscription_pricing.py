#!/usr/bin/env python3
"""
Subscription Line Price-Plan Retrofit
======================================
One-off backfill script. The first round of subscriptions was loaded into
NetSuite before the price plan layer existed. This script reads each
already-loaded subscription, resolves the expected price plan per line from
the new subs CSV, looks up the price plan internal ID in the local state DB,
and PATCHes each line with `pricePlan.id`.

Strategy (per agreed Option 1 — see CLAUDE.md project notes):
  - Dedupe source rows by (deal_id, NS_sales_item_name); for ambiguous combos
    where multiple HS line items map to the same NS sales item, take the
    first non-blank Price Plan External ID seen. The 12 truly-divergent
    combos surface in the report under DIVERGENT_AMBIGUOUS so the client
    can spot-check.
  - Don't create or delete any NS records — only PATCH existing lines.
  - MISSING (line in source but absent from NS) and ORPHANED (line in NS
    but absent from source) are reported, never auto-actioned.

Modes:
  --dry-run   (default)   Compute everything, log the plan, write report. No PATCHes.
  --apply                 Same as dry-run but actually PATCHes.
  --limit N               Only process the first N subscriptions (smoke test).
  --report-path PATH      Override default report CSV destination.

Outputs:
  scripts/reports/retrofit_<timestamp>.csv  — per-line outcome
  scripts/reports/retrofit_<timestamp>.log  — full log mirror

Usage:
  python scripts/retrofit_subscription_pricing.py --dry-run
  python scripts/retrofit_subscription_pricing.py --apply --limit 1
  python scripts/retrofit_subscription_pricing.py --apply
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional


def _normalize_item(name: str) -> str:
    """
    Normalise an NS sales item name for the (deal, item) join key.

    NS subscription plans were authored with inconsistent spacing in a few
    item refNames (e.g. ``"Year End  - EEs"`` with a double space) while the
    Snowflake-driven source CSVs always emit single-spaced strings. A
    byte-exact join misses those rows and they surface as ORPHANED.
    Collapsing runs of whitespace to a single space — and stripping ZWSPs —
    fixes ~14 false-positive orphans without risk of matching unrelated items.
    """
    return re.sub(r"\s+", " ", (name or "").replace("​", "")).strip()

# Project root on path so loader/client/tracker imports resolve
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import config  # noqa: E402
from netsuite_client import NetSuiteClient  # noqa: E402
from state_tracker import StateTracker  # noqa: E402

logger = logging.getLogger("retrofit")


# ── Outcome codes (one per line in the report) ─────────────────────────
PATCHED = "PATCHED"                                       # line was PATCHed successfully
WOULD_PATCH = "WOULD_PATCH"                               # dry-run; PATCH was prepared
ALREADY_LINKED = "ALREADY_LINKED"                         # line already has the right pricePlan
PP_BLANK_IN_SOURCE = "PP_BLANK_IN_SOURCE"                 # CSV row has no PP; nothing to attach
PP_NOT_LOADED = "PP_NOT_LOADED"                           # CSV references a PP not in state DB
DIVERGENT_AMBIGUOUS = "DIVERGENT_AMBIGUOUS"               # 2+ HS lines for same (deal, item), differing prices
ORPHANED_IN_NS = "ORPHANED_IN_NS"                         # NS line has no source row
MISSING_IN_NS = "MISSING_IN_NS"                           # source row has no NS line
PATCH_FAILED = "PATCH_FAILED"                             # API rejected the PATCH


# ── Source CSV → (deal, item) → price plan map ─────────────────────────


def build_source_map(subs_csv: str) -> tuple[dict, set]:
    """
    Read the subs CSV and produce two indices:

      pp_by_key      : (deal_id, item_name) → {
                         "pp_ext_id": str | None,    # first non-blank PP for this combo
                         "all_lids":  set[str],      # every HS line_item_id seen
                         "all_pps":   set[str],      # every distinct PP ext_id seen
                       }
      ambiguous_keys : set of (deal_id, item_name) where >1 distinct HS
                       line_item_id mapped to the same combo.

    Item names are normalised the same way the subscription loader does
    (strip + zero-width-space removal) so they line up with NS line refNames.
    """
    pp_by_key: dict[tuple[str, str], dict] = {}
    ambiguous_keys: set[tuple[str, str]] = set()

    with open(subs_csv, encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            deal = (r.get("External ID") or "").strip()
            item = _normalize_item(r.get("Sales Item") or "")
            lid = (r.get("Line Item ID") or "").strip()
            pp = (r.get("Price Plan External ID") or "").strip() or None
            include = (r.get("Lines: Include") or "").strip()
            if include != "T":
                continue
            if not (deal and item):
                continue
            key = (deal, item)
            entry = pp_by_key.setdefault(
                key, {"pp_ext_id": None, "all_lids": set(), "all_pps": set()}
            )
            if lid:
                entry["all_lids"].add(lid)
            if pp:
                entry["all_pps"].add(pp)
                if entry["pp_ext_id"] is None:
                    entry["pp_ext_id"] = pp

    for key, entry in pp_by_key.items():
        if len(entry["all_lids"]) > 1 and len(entry["all_pps"]) > 1:
            ambiguous_keys.add(key)

    return pp_by_key, ambiguous_keys


# ── Per-subscription processing ────────────────────────────────────────


def fetch_subscription_lines(client: NetSuiteClient, sub_ns_id: str) -> Optional[list]:
    """GET a subscription with sub-resources expanded; return the line list or None."""
    url = f"{config.BASE_URL}/subscription/{sub_ns_id}?expandSubResources=true"
    resp = client._request("GET", url)
    if resp.status_code != 200:
        logger.error(
            f"  Cannot GET subscription {sub_ns_id}: HTTP {resp.status_code}"
        )
        return None
    return resp.json().get("subscriptionLine", {}).get("items", [])


def existing_price_plan_id(line: dict) -> Optional[str]:
    """Extract the existing pricePlan.id from a line, if any."""
    pp = line.get("pricePlan") or {}
    pid = pp.get("id")
    return str(pid) if pid else None


def patch_line(
    client: NetSuiteClient, sub_ns_id: str, line_num: int, pp_ns_id: str
) -> tuple[bool, str]:
    """PATCH a single line with pricePlan.id. Returns (ok, error_message)."""
    url = f"{config.BASE_URL}/subscription/{sub_ns_id}/subscriptionLine/{line_num}"
    body = {"pricePlan": {"id": pp_ns_id}}
    resp = client._request("PATCH", url, body)
    if resp.status_code == 204:
        return True, ""
    return False, f"HTTP {resp.status_code}: {resp.text[:300]}"


# ── Main retrofit loop ─────────────────────────────────────────────────


def retrofit(
    client: NetSuiteClient,
    tracker: StateTracker,
    apply: bool,
    limit: Optional[int],
    report_writer: csv.DictWriter,
) -> dict:
    """
    Walk every loaded subscription, derive its expected pricePlan per line,
    and either log or apply the PATCH. Also walk the source map to flag any
    (deal, item) combos that have no NS line at all.
    """
    pp_by_key, ambiguous_keys = build_source_map(config.SUBSCRIPTIONS_CSV)
    logger.info(
        f"Source map: {len(pp_by_key)} (deal, item) keys, "
        f"{len(ambiguous_keys)} ambiguous (divergent) combos."
    )

    # Pull every subscription with a known NS internal ID from the state DB
    rows = tracker.conn.execute(
        "SELECT external_id, netsuite_id FROM load_state "
        "WHERE entity_type = 'subscription' AND netsuite_id IS NOT NULL"
    ).fetchall()
    subscriptions = [(r["external_id"], r["netsuite_id"]) for r in rows]
    if limit:
        subscriptions = subscriptions[:limit]
    logger.info(f"Subscriptions to walk: {len(subscriptions)}")

    counts: dict[str, int] = defaultdict(int)
    seen_keys_in_ns: set[tuple[str, str]] = set()

    for i, (deal_id, sub_ns_id) in enumerate(subscriptions, 1):
        logger.info(f"[{i}/{len(subscriptions)}] Subscription {deal_id} (NS {sub_ns_id})")
        lines = fetch_subscription_lines(client, sub_ns_id)
        if lines is None:
            counts["GET_FAILED"] += 1
            continue

        for line in lines:
            line_num = line.get("lineNumber")
            item_name_raw = (line.get("item") or {}).get("refName", "")
            if not item_name_raw:
                continue
            # Normalise to match how source-CSV item names are keyed in
            # build_source_map (collapses double-spacing in NS plan names).
            item_name = _normalize_item(item_name_raw)
            key = (deal_id, item_name)
            seen_keys_in_ns.add(key)
            # NS auto-creates one line per item refName from the plan, mostly
            # with isIncluded=false (plan filler). Those carry no value and
            # are not candidates for pricing — skip them silently rather than
            # flooding the report with hundreds of fake "ORPHANED" rows.
            if not line.get("isIncluded"):
                continue
            entry = pp_by_key.get(key)
            row = {
                "deal_id": deal_id,
                "subscription_ns_id": sub_ns_id,
                "line_number": line_num,
                "item_name": item_name,
                "pp_ext_id": "",
                "pp_ns_id": "",
                "outcome": "",
                "detail": "",
            }

            if entry is None:
                row["outcome"] = ORPHANED_IN_NS
                row["detail"] = "NS line has no matching row in current subs CSV."
                counts[ORPHANED_IN_NS] += 1
                report_writer.writerow(row)
                continue

            pp_ext_id = entry["pp_ext_id"]
            row["pp_ext_id"] = pp_ext_id or ""

            if not pp_ext_id:
                row["outcome"] = PP_BLANK_IN_SOURCE
                row["detail"] = (
                    "Source rows for this (deal, item) carry no price plan; "
                    "NS will use plan default."
                )
                counts[PP_BLANK_IN_SOURCE] += 1
                report_writer.writerow(row)
                continue

            pp_ns_id = tracker.get_netsuite_id("pricePlan", pp_ext_id)
            row["pp_ns_id"] = pp_ns_id or ""
            if not pp_ns_id:
                row["outcome"] = PP_NOT_LOADED
                row["detail"] = (
                    f"Price plan {pp_ext_id} is not in state DB. "
                    "Run the price plan loader first."
                )
                counts[PP_NOT_LOADED] += 1
                report_writer.writerow(row)
                continue

            existing = existing_price_plan_id(line)
            if existing == pp_ns_id:
                row["outcome"] = ALREADY_LINKED
                row["detail"] = "NS line already references this price plan; skipped."
                counts[ALREADY_LINKED] += 1
                report_writer.writerow(row)
                continue

            if key in ambiguous_keys:
                row["outcome"] = DIVERGENT_AMBIGUOUS
                row["detail"] = (
                    f"Multiple HS lines map to (deal, item) with differing prices "
                    f"({len(entry['all_lids'])} LIDs, {len(entry['all_pps'])} PPs). "
                    f"Picked first; others not represented in NS."
                )
                # Still proceed — log + continue to PATCH (the chosen PP).
                counts[DIVERGENT_AMBIGUOUS] += 1
                # Note: outcome is overwritten below if the PATCH succeeds, but
                # we keep DIVERGENT_AMBIGUOUS as the primary signal.
                report_writer.writerow(row)
                if not apply:
                    continue

            if not apply:
                row["outcome"] = WOULD_PATCH
                row["detail"] = (
                    f"Would PATCH pricePlan.id={pp_ns_id} (extId={pp_ext_id})"
                )
                counts[WOULD_PATCH] += 1
                report_writer.writerow(row)
                continue

            ok, err = patch_line(client, sub_ns_id, line_num, pp_ns_id)
            if ok:
                row["outcome"] = PATCHED
                row["detail"] = f"pricePlan.id={pp_ns_id} attached."
                counts[PATCHED] += 1
            else:
                row["outcome"] = PATCH_FAILED
                row["detail"] = err
                counts[PATCH_FAILED] += 1
            report_writer.writerow(row)

    # MISSING_IN_NS: source has a (deal, item) row but NS has no matching line.
    # Only consider deals we walked; otherwise we'd flood the report with
    # subscriptions we deliberately skipped via --limit.
    walked_deals = {deal for deal, _ in subscriptions}
    for (deal, item), entry in pp_by_key.items():
        if deal not in walked_deals:
            continue
        if (deal, item) in seen_keys_in_ns:
            continue
        report_writer.writerow(
            {
                "deal_id": deal,
                "subscription_ns_id": "",
                "line_number": "",
                "item_name": item,
                "pp_ext_id": entry["pp_ext_id"] or "",
                "pp_ns_id": "",
                "outcome": MISSING_IN_NS,
                "detail": (
                    "Source has this (deal, item) but no NS line was found; "
                    "subscription line was likely never created (1:N fan-out from "
                    "mapping normalisation). User decides backfill strategy."
                ),
            }
        )
        counts[MISSING_IN_NS] += 1

    return dict(counts)


# ── CLI ────────────────────────────────────────────────────────────────


def setup_logging(log_path: str) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-7s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually PATCH (default is dry-run / report only).",
    )
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--report-path",
        default=None,
        help=(
            "Override report CSV path "
            "(default: scripts/reports/<YYYY-MM-DD>/retrofit_HH-MM-SS.csv)."
        ),
    )
    args = parser.parse_args()

    apply = bool(args.apply)
    # Match main.py's logger convention: GMT+3 timestamp, date folder,
    # short HH-MM-SS suffix in the filename.
    gmt3 = timezone(timedelta(hours=3))
    now = datetime.now(gmt3)
    date_folder = os.path.join("scripts", "reports", now.strftime("%Y-%m-%d"))
    os.makedirs(date_folder, exist_ok=True)
    fname_suffix = now.strftime("%H-%M-%S")
    report_path = args.report_path or os.path.join(
        date_folder, f"retrofit_{fname_suffix}.csv"
    )
    log_path = os.path.join(date_folder, f"retrofit_{fname_suffix}.log")

    setup_logging(log_path)
    logger.info(f"Mode: {'APPLY (PATCHes will be sent)' if apply else 'DRY-RUN (no PATCHes)'}")
    logger.info(f"Report CSV : {report_path}")
    logger.info(f"Log file   : {log_path}")

    if not config.CONSUMER_KEY:
        logger.error(
            "Credentials not configured. Source your .env file first:\n"
            "  export $(grep -v '^#' .env | xargs)"
        )
        return 1

    client = NetSuiteClient()
    tracker = StateTracker()

    fieldnames = [
        "deal_id",
        "subscription_ns_id",
        "line_number",
        "item_name",
        "pp_ext_id",
        "pp_ns_id",
        "outcome",
        "detail",
    ]
    try:
        with open(report_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            counts = retrofit(client, tracker, apply, args.limit, writer)
    finally:
        tracker.close()

    logger.info("=" * 60)
    logger.info("  RETROFIT SUMMARY")
    logger.info("=" * 60)
    for k in sorted(counts):
        logger.info(f"  {k:<22} : {counts[k]}")
    logger.info("=" * 60)
    logger.info(f"Report written to {report_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
