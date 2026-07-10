#!/usr/bin/env python3
"""
Cleanup: delete subscriptions that need re-loading (unbilled / mis-bound)
=========================================================================
When a subscription loads **without a billing account** (or bound to the wrong
customer), NetSuite upserts by externalId — re-running the loader will NOT fix
it in place. The record has to be deleted and re-created. This tool finds those
subscriptions and (with ``--execute``) deletes them + clears their state-tracker
row so the next ``main.py --entity subscription`` re-creates them cleanly.

It is **dynamic and reusable** — nothing is hardcoded to a particular batch. It
scans the current batch (the subs CSV in ``config.py``), the whole load revision,
or an explicit list you pass, checks each against live NetSuite, and reports what
qualifies. Read-only by default; deletes only with ``--execute``.

Which account it touches is set by the NS_* env vars (same as the loader). It
refuses to delete in production unless you add ``--allow-prod``.

Selection scope (pick one; default is --from-csv)
-------------------------------------------------
  --from-csv        Every subscription in the subs CSV (config.SUBSCRIPTIONS_CSV),
                    revision-suffixed and looked up in NS. (default)
  --all-revision    Every subscription in NS whose externalId ends with the
                    current LOAD_REVISION — even ones not in the CSV.
  --ext-id EXT      One or more RAW sub externalIds (repeatable). These are always
                    treated as candidates regardless of the reason filters.

Reason filters (why a subscription qualifies for deletion)
----------------------------------------------------------
  (default)         Missing a billing account.
  --name-mismatch   ALSO delete subs whose bound NS customer name doesn't match
                    the customer named in the subs CSV (catches wrong-customer
                    bindings). Off by default.
  --keep-missing-ba Do NOT treat a missing billing account as a reason (use with
                    --name-mismatch to target only mis-bound subs).

Examples
--------
    # Preview this batch's unbilled subs (read-only):
    python scripts/cleanup_unbilled_subscriptions.py

    # Preview unbilled OR mis-bound subs, then delete + un-gate for reload:
    python scripts/cleanup_unbilled_subscriptions.py --name-mismatch
    python scripts/cleanup_unbilled_subscriptions.py --name-mismatch --execute

    # Delete two specific subs by externalId:
    python scripts/cleanup_unbilled_subscriptions.py --ext-id 498500387059_27401 --execute

Then reload:
    python main.py --dry-run --entity billingAccount --create-missing-bas
    python main.py --entity billingAccount --create-missing-bas
    python main.py --entity subscription
"""

import argparse
import csv
import re
import sys
from pathlib import Path

# Put the project root on sys.path so top-level imports resolve regardless of cwd.
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import config  # noqa: E402
from netsuite_client import NetSuiteClient  # noqa: E402
from state_tracker import StateTracker  # noqa: E402

WARN, OK, NO = "⚠️ ", "✓", "✗"


# ─── Environment banner ─────────────────────────────────────────────────────
def show_environment() -> tuple[str, bool]:
    """Print which account the env points at. Returns (realm, looks_sandbox)."""
    realm = config.BASE_URL.split("//")[1].split(".")[0]
    looks_sandbox = "sb" in realm.lower()
    tag = f"{WARN}SANDBOX" if looks_sandbox else "PRODUCTION (no 'sb' in realm)"
    print("─" * 66)
    print(f"  NetSuite account : {realm}")
    print(f"  State DB         : {config.STATE_DB}")
    print(f"  Load revision    : {config.LOAD_REVISION}")
    print(f"  Looks like       : {tag}")
    print("─" * 66)
    return realm, looks_sandbox


# ─── Name matching (mirrors validate.py's _names_match) ─────────────────────
def _norm(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"\b(limited|ltd|llp|plc|inc|co|company|group|services?)\b", " ", s)
    return re.sub(r"[^a-z0-9]", "", s)


def _names_match(a: str, b: str) -> bool:
    x, y = _norm(a), _norm(b)
    if not x or not y:
        return True  # can't compare → don't flag
    return x in y or y in x


# ─── Discovery ──────────────────────────────────────────────────────────────
def _csv_customer_by_sub() -> dict:
    """RAW sub externalId (the CSV 'External ID') → customer name from the subs CSV."""
    out: dict[str, str] = {}
    try:
        with open(config.SUBSCRIPTIONS_CSV, encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                ext = (r.get("External ID") or "").strip()
                if ext:
                    out.setdefault(ext, (r.get("Customer") or "").strip())
    except FileNotFoundError:
        print(f"{WARN}subs CSV not found at {config.SUBSCRIPTIONS_CSV}")
    return out


def _all_revision_ext_ids(client) -> list[str]:
    """RAW sub externalIds for every NS subscription under the current revision."""
    rev = config.LOAD_REVISION
    q = "SELECT externalid FROM subscription " f"WHERE externalid LIKE '%{rev}'"
    raws = []
    for row in client.suiteql_query(q):
        eid = (row.get("externalid") or "").strip()
        if eid.endswith(rev):
            raws.append(eid[: -len(rev)])  # strip suffix back to raw
    return sorted(set(raws))


def _customer_name(record: dict) -> str:
    cust = record.get("customer") or {}
    return cust.get("refName", "") if isinstance(cust, dict) else ""


def _has_billing_account(record: dict) -> bool:
    ba = record.get("billingAccount")
    return bool(ba and (ba.get("id") if isinstance(ba, dict) else ba))


def gather_candidates(
    client, raw_ext_ids, csv_names, want_missing_ba, want_name_mismatch, forced
):
    """For each raw externalId, look up the NS subscription and collect the
    reasons it qualifies for deletion. Returns a list of dicts."""
    rows = []
    for raw in raw_ext_ids:
        rev_ext = config.apply_revision(raw)
        record = client.get_by_external_id("subscription", rev_ext)
        if not record:
            continue  # not in NS → nothing to clean up
        ns_id = record.get("id")
        bound = _customer_name(record)
        expected = csv_names.get(raw, "")

        reasons = []
        if not _has_billing_account(record):
            reasons.append("no billing account")
        if expected and not _names_match(expected, bound):
            reasons.append(f"customer mismatch (CSV {expected!r} → NS {bound!r})")

        enabled = []
        if want_missing_ba and "no billing account" in reasons:
            enabled.append("no billing account")
        if want_name_mismatch:
            enabled += [r for r in reasons if r.startswith("customer mismatch")]
        qualifies = bool(enabled) or (raw in forced)

        rows.append(
            {
                "raw": raw,
                "rev_ext": rev_ext,
                "ns_id": ns_id,
                "bound": bound,
                "expected": expected,
                "reasons": reasons,
                "qualifies": qualifies,
            }
        )
    return rows


# ─── Delete ─────────────────────────────────────────────────────────────────
def delete_subscription(client, ns_id: str) -> tuple[bool, str]:
    resp = client._request("DELETE", f"{config.BASE_URL}/subscription/{ns_id}")
    if resp.status_code in (200, 204):
        return True, ""
    return False, f"HTTP {resp.status_code}: {resp.text[:300]}"


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    scope = ap.add_mutually_exclusive_group()
    scope.add_argument(
        "--from-csv", action="store_true", help="Scan the subs CSV (default)."
    )
    scope.add_argument(
        "--all-revision",
        action="store_true",
        help="Scan every NS subscription under the current revision.",
    )
    ap.add_argument(
        "--ext-id",
        action="append",
        default=[],
        metavar="EXT",
        help="RAW sub externalId to target (repeatable). Always a candidate.",
    )
    ap.add_argument(
        "--name-mismatch",
        action="store_true",
        help="Also delete subs bound to a customer whose name mismatches the CSV.",
    )
    ap.add_argument(
        "--keep-missing-ba",
        action="store_true",
        help="Do NOT treat a missing billing account as a deletion reason.",
    )
    ap.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete (default is a read-only preview).",
    )
    ap.add_argument(
        "--allow-prod",
        action="store_true",
        help="Permit --execute against a production account.",
    )
    args = ap.parse_args()

    realm, looks_sandbox = show_environment()
    if args.execute and not looks_sandbox and not args.allow_prod:
        print(
            f"{NO} Refusing to --execute against what looks like PRODUCTION "
            f"({realm}) without --allow-prod. Aborting."
        )
        sys.exit(2)

    client = NetSuiteClient()
    csv_names = _csv_customer_by_sub()

    # Build the raw-externalId work list from the chosen scope.
    forced = {e.strip() for e in args.ext_id if e.strip()}
    if forced:
        raw_ext_ids = sorted(forced)
    elif args.all_revision:
        raw_ext_ids = _all_revision_ext_ids(client)
    else:  # --from-csv (default)
        raw_ext_ids = sorted(csv_names)

    if not raw_ext_ids:
        print("No subscriptions in scope. Nothing to do.")
        return

    want_missing_ba = not args.keep_missing_ba
    rows = gather_candidates(
        client,
        raw_ext_ids,
        csv_names,
        want_missing_ba=want_missing_ba,
        want_name_mismatch=args.name_mismatch,
        forced=forced,
    )
    candidates = [r for r in rows if r["qualifies"]]

    print(
        f"\nScanned {len(rows)} subscription(s) present in NS; "
        f"{len(candidates)} qualify for deletion.\n"
    )
    if not candidates:
        print(
            "Nothing qualifies. (Enabled reasons: "
            f"{'missing-BA ' if want_missing_ba else ''}"
            f"{'name-mismatch' if args.name_mismatch else ''}".strip() + ")"
        )
        return

    print(f"{'Sub externalId':<26}{'NS id':<9}{'Customer (NS)':<40}Reason(s)")
    print("-" * 110)
    for r in candidates:
        why = "; ".join(r["reasons"]) or "(forced by --ext-id)"
        print(f"{r['raw']:<26}{str(r['ns_id']):<9}{r['bound'][:38]:<40}{why}")

    if not args.execute:
        print(
            f"\n{WARN}DRY RUN — no changes made. Re-run with --execute to delete "
            "the above and clear their state rows for reload."
        )
        return

    print(f"\nDeleting {len(candidates)} subscription(s) from {realm} ...")
    tracker = StateTracker()
    deleted = failed = 0
    for r in candidates:
        ok, err = delete_subscription(client, r["ns_id"])
        if ok:
            tracker.delete_state("subscription", r["rev_ext"])
            deleted += 1
            print(f"  {OK} deleted {r['raw']} (NS {r['ns_id']}) + cleared state")
        else:
            failed += 1
            print(f"  {NO} {r['raw']} (NS {r['ns_id']}): {err}")
    tracker.close()

    print(f"\nDone: {deleted} deleted, {failed} failed.")
    if deleted:
        print("Next: fill the billing-account gaps, then reload:")
        print("  python main.py --dry-run --entity billingAccount --create-missing-bas")
        print("  python main.py --entity billingAccount --create-missing-bas")
        print("  python main.py --entity subscription")


if __name__ == "__main__":
    main()
