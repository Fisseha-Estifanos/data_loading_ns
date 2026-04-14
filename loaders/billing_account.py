"""
Billing Account Loader
=======================
Maps billingkleeneexport CSV → NetSuite Billing Account records.
Key dependency: Customer must be loaded first (needs customer NS internal ID).
"""

import logging
from typing import Optional

import config
from loaders.base import BaseLoader

logger = logging.getLogger(__name__)


class BillingAccountLoader(BaseLoader):
    """Billing Account Loader"""

    ENTITY_TYPE = "billingAccount"
    RECORD_TYPE = "billingAccount"
    CSV_PATH = config.BILLING_CSV

    def get_external_id(self, row: dict) -> str:
        return row.get("externalId", "").strip()

    def get_tier3_field(self) -> Optional[str]:
        return "name"

    def get_tier3_value(self, row: dict) -> Optional[str]:
        return row.get("name", "").strip()

    def build_payload(self, row: dict) -> Optional[dict]:
        ext_id = self.get_external_id(row)
        name = row.get("name", "").strip()
        customer_ext_id = row.get("customer_externalId", "").strip()

        if not ext_id or not customer_ext_id:
            logger.warning(
                "Skipping billing account: missing externalId or customer_externalId"
            )
            return None

        # ── Resolve Customer Internal ID ────────────────────────────────
        customer_ns_id = self.tracker.get_netsuite_id("customer", customer_ext_id)
        if not customer_ns_id:
            logger.error(
                f"Cannot create billing account {ext_id}: "
                f"customer {customer_ext_id} has no NetSuite ID in state tracker. "
                f"Ensure customers are loaded first."
            )
            return None

        subsidiary_id = row.get("subsidiary_id", "").strip()
        currency_id = row.get("currency_id", "").strip()
        billing_schedule_id = row.get("billingSchedule_id", "").strip()
        frequency = row.get("frequency", "").strip()
        start_date = row.get("startDate", "").strip()

        payload = {
            "externalId": ext_id,
            "name": name,
            "customer": {"id": customer_ns_id},
            "subsidiary": {"id": subsidiary_id},
            "currency": {"id": currency_id},
            "frequency": {"id": frequency},  # e.g. "MONTHLY"
            "startDate": start_date,
            "customerDefault": row.get("customerDefault", "").strip().lower() == "true",
            "requestOffCycleInvoice": row.get("requestOffCycleInvoice", "")
            .strip()
            .lower()
            == "true",
            "inactive": row.get("inactive", "").strip().lower() == "true",
        }

        # Billing schedule
        if billing_schedule_id:
            payload["billingSchedule"] = {"id": billing_schedule_id}

        # Bill/Ship address lists (parked — may be null)
        bill_addr = row.get("billAddressList_parked", "").strip()
        ship_addr = row.get("shipAddressList_parked", "").strip()
        if bill_addr:
            payload["billAddressList"] = {"id": bill_addr}
        if ship_addr:
            payload["shipAddressList"] = {"id": ship_addr}

        return payload
