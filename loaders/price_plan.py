"""
Price Plan Loader
==================
Maps the price-plans CSV (exported from PROD.NETSUITE_REVERSE_ETL.JSON_PRICING_MP_V2)
to NetSuite Price Plan records.

Key design:
  - The CSV's PRICE_PLAN_JSON column is already a ready-to-POST payload
    shaped by Snowflake. The loader trusts it — no per-field rebuilding.
  - One row per (HS line item × NS sales item). External ID convention:
        MP_PP_<LINE_ITEM_ID>_<slug(NS_SALES_ITEM)>
  - Reconciliation pass at init: SuiteQL pulls every NS price plan whose
    externalId starts with 'MP_PP_' and seeds the state DB with their
    internal IDs. This makes re-runs idempotent even if the local state DB
    was lost.

Endpoint: POST /services/rest/record/v1/pricePlan
Tier 3 lookup: externalId.

Dependencies: none (price plans reference NS sales items by name only —
no NS internal-ID parents).
"""

import json
import logging
from typing import Optional

import config
from loaders.base import BaseLoader

logger = logging.getLogger(__name__)


class PricePlanLoader(BaseLoader):
    """Price Plan Loader"""

    ENTITY_TYPE = "pricePlan"
    RECORD_TYPE = "pricePlan"
    CSV_PATH = config.PRICE_PLANS_CSV

    EXTID_PREFIX = "MP_PP_"

    def __init__(self, client, tracker):
        super().__init__(client, tracker)
        self._reconcile_existing_plans()

    # ── Reconciliation pass ─────────────────────────────────────────────

    def _reconcile_existing_plans(self) -> None:
        """
        Pull every existing MP_PP_* price plan from NS and seed the state DB
        with externalId → internalId. On re-runs this guarantees the loader
        skips POSTs for plans NS already has even if local state was wiped.
        """
        logger.info(
            "Reconciling existing MP_PP_* price plans from NetSuite "
            "(seeds the state DB so re-runs skip already-loaded plans)..."
        )
        try:
            # Underscores must be escaped: '_' is a single-char wildcard in
            # SQL LIKE, so 'MP_PP_%' would also match 'MPBPP1' and similar
            # externalIds left in this sandbox by earlier work.
            rows = self.client.suiteql_query(
                "SELECT id, externalid FROM pricePlan "
                "WHERE externalid LIKE 'MP\\_PP\\_%' ESCAPE '\\'"
            )
        except Exception as e:
            logger.warning(
                f"Reconciliation query failed: {e}. Continuing without seed; "
                "any MP_PP_* plans already in NS will surface as 'already exists' on POST."
            )
            return

        seeded = 0
        for row in rows:
            ext_id = (row.get("externalid") or "").strip()
            ns_id = str(row.get("id") or "").strip()
            if not (ext_id and ns_id):
                continue
            if self.tracker.get_netsuite_id(self.ENTITY_TYPE, ext_id):
                continue
            self.tracker.upsert_state(
                entity_type=self.ENTITY_TYPE,
                external_id=ext_id,
                status="success",
                netsuite_id=ns_id,
                error_message=None,
                payload_hash=None,
                tier_used="reconcile",
            )
            seeded += 1
        logger.info(
            f"Reconciliation: {len(rows)} existing MP_PP_* plans found in NS, "
            f"{seeded} newly seeded into state DB."
        )

    # ── BaseLoader hooks ────────────────────────────────────────────────

    def get_external_id(self, row: dict) -> str:
        return (row.get("PRICE_PLAN_EXTERNAL_ID") or "").strip()

    def get_tier3_field(self) -> Optional[str]:
        return "externalId"

    def get_tier3_value(self, row: dict) -> Optional[str]:
        return self.get_external_id(row)

    def build_payload(self, row: dict) -> Optional[dict]:
        """
        The CSV's PRICE_PLAN_JSON column is the payload, pre-shaped by Snowflake.
        We parse it and return as-is. Skipping any row whose JSON is unparseable
        rather than guessing field values (data integrity rule).
        """
        ext_id = self.get_external_id(row)
        raw = row.get("PRICE_PLAN_JSON") or ""
        if not raw.strip():
            logger.error(
                f"Price plan {ext_id}: PRICE_PLAN_JSON column is empty. Skipping."
            )
            return None
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as e:
            logger.error(
                f"Price plan {ext_id}: PRICE_PLAN_JSON column is not valid JSON ({e}). "
                f"Skipping. First 200 chars: {raw[:200]!r}"
            )
            return None

        # Sanity-check the externalId in the JSON matches the CSV column.
        # If they disagree, the CSV is internally inconsistent — fail loud.
        json_ext = (payload.get("externalId") or "").strip()
        if json_ext and json_ext != ext_id:
            logger.error(
                f"Price plan {ext_id}: PRICE_PLAN_EXTERNAL_ID column "
                f"disagrees with payload.externalId ({json_ext!r}). Skipping."
            )
            return None

        return payload
