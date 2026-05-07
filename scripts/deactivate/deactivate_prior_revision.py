"""
Deactivate Prior-Revision Records (FUTURE USE — DO NOT RUN AS-IS)
==================================================================

Walks the local state DB and finds every successfully-loaded record whose
external_id does NOT end with the *current* `config.LOAD_REVISION`. For each
match, attempts to deactivate it in NS so the prior revision's records stop
appearing in default views/reports while the new revision remains active.

Status: PARKED. The current decision (2026-05-06) is to LEAVE prior-revision
records ALONE in NS. This script is committed for future use, when (if ever)
we want to retire `_rvn_01` or earlier batches. It must be reviewed and
field-checked per entity type before any apply run, because:

  - `customer`, `pricePlan`, `billingAccount`        → toggle `isInactive=true`
  - `subscription`                                   → `isInactive` is NOT the
        field. Subscriptions transition through a status workflow and may
        require a status change order or admin action; verify before patching.
  - `oneOff` (invoice)                               → posted invoices typically
        cannot be deactivated; voiding requires a credit memo. DO NOT default
        to PATCH. Confirm intended action with user first.

How to use (when the time comes):
    1. Read the entity-specific notes above.
    2. Set REVIEWED = True at the bottom of this file.
    3. Run with --dry-run first; inspect every line of output.
    4. Run with --apply only after explicit user sign-off.

Usage:
    python scripts/deactivate/deactivate_prior_revision.py --dry-run
    python scripts/deactivate/deactivate_prior_revision.py --apply
"""

import argparse
import logging
import sys
from pathlib import Path

# Allow the script to find the project's modules when run from anywhere.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402
from netsuite_client import NetSuiteClient  # noqa: E402
from state_tracker import StateTracker  # noqa: E402

logger = logging.getLogger(__name__)


# Hard guard: must be flipped to True after a human has read the entity-
# specific notes in the docstring and confirmed the chosen field/method per
# entity type is correct for the records actually in NS.
REVIEWED = False


# Entity → REST record path (used to build PATCH URL).
# Subscription and oneOff are intentionally excluded until reviewed
# (different deactivation semantics — see the docstring).
DEACTIVATABLE_ENTITIES = {
    "customer": ("customer", {"isInactive": True}),
    "billingAccount": ("billingAccount", {"isInactive": True}),
    "pricePlan": ("pricePlan", {"isInactive": True}),
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Deactivate prior-revision NS records "
            "(parked; do not run without review)."
        )
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be patched without calling NS.",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Actually issue PATCH calls to NS. Requires REVIEWED=True.",
    )
    p.add_argument(
        "--current-revision",
        default=config.LOAD_REVISION,
        help=(
            "Records whose external_id ends with this string are KEPT. "
            "Defaults to config.LOAD_REVISION."
        ),
    )
    p.add_argument(
        "--entity",
        choices=sorted(DEACTIVATABLE_ENTITIES.keys()),
        help=(
            "Limit to one entity type. "
            "Default: all entries in DEACTIVATABLE_ENTITIES."
        ),
    )
    return p.parse_args()


def find_candidates(
    tracker: StateTracker, current_revision: str, entity_filter: str | None
) -> list[tuple[str, str, str]]:
    """
    Return list of (entity_type, external_id, ns_id) for every successfully
    loaded record whose external_id does NOT end with `current_revision`.
    """
    entities = (
        [entity_filter] if entity_filter else list(DEACTIVATABLE_ENTITIES.keys())
    )
    candidates: list[tuple[str, str, str]] = []
    for entity in entities:
        # NOTE: `list_successful` is not yet implemented on StateTracker. When
        # this script is unparked, add a method that returns rows matching:
        #   SELECT external_id, netsuite_id FROM load_state
        #   WHERE entity_type = ? AND status IN ('success', 'success_no_id')
        #     AND netsuite_id IS NOT NULL
        rows = tracker.list_successful(entity)
        for row in rows:
            ext_id = row["external_id"]
            ns_id = row["netsuite_id"]
            if not ns_id:
                continue
            if ext_id.endswith(current_revision):
                continue
            candidates.append((entity, ext_id, ns_id))
    return candidates


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()

    if args.apply and not REVIEWED:
        logger.error(
            "REVIEWED is False — refusing to --apply. Read the script docstring, "
            "verify per-entity deactivation semantics, then set REVIEWED=True."
        )
        return 2

    if not (args.dry_run or args.apply):
        logger.error("Must pass --dry-run or --apply.")
        return 2

    tracker = StateTracker()
    candidates = find_candidates(tracker, args.current_revision, args.entity)

    logger.info(
        "Found %d candidate record(s) to deactivate "
        "(external_id NOT ending in %r).",
        len(candidates),
        args.current_revision,
    )

    if not candidates:
        return 0

    if args.dry_run:
        for entity, ext_id, ns_id in candidates:
            record_path, patch_body = DEACTIVATABLE_ENTITIES[entity]
            logger.info(
                "DRY-RUN PATCH %s/%s (extId=%s) body=%s",
                record_path,
                ns_id,
                ext_id,
                patch_body,
            )
        return 0

    # apply path — REVIEWED is True (guarded above)
    client = NetSuiteClient()
    success = 0
    failed = 0
    for entity, ext_id, ns_id in candidates:
        record_path, patch_body = DEACTIVATABLE_ENTITIES[entity]
        url = f"{config.BASE_URL}/{record_path}/{ns_id}"
        resp = client._request("PATCH", url, patch_body)  # noqa: SLF001
        if resp.status_code in (200, 204):
            logger.info("✓ deactivated %s/%s (extId=%s)", record_path, ns_id, ext_id)
            success += 1
        else:
            logger.error(
                "✗ failed %s/%s (extId=%s): HTTP %s — %s",
                record_path,
                ns_id,
                ext_id,
                resp.status_code,
                resp.text[:300],
            )
            failed += 1

    logger.info("Done. success=%d failed=%d", success, failed)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
