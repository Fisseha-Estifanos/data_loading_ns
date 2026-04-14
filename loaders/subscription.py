"""
Subscription Loader
====================
Maps subscriptionskleeneexport CSV → NetSuite Subscription records.

Key design:
  - CSV rows are grouped by External ID (deal ID) → one subscription per group
  - Header fields are taken from the first row in each group
  - Each row becomes a subscription line item
  - Customer is resolved by company name → customer CSV External ID 2 → state tracker
  - Billing account is resolved by {deal_id}_BA → state tracker (if available)

Dependencies: Customer and Billing Account must be loaded first.
"""
import csv
import logging
from collections import defaultdict
from typing import Optional

import config
from loaders.base import BaseLoader

logger = logging.getLogger(__name__)

# ── Subsidiary display name → NS internal ID ────────────────────────────
SUBSIDIARY_MAP = {
    "Moorepay Ltd": "12",
    "Moorepay Ireland": "66",
}

# Currency code → NS internal ID
CURRENCY_MAP = {
    "GBP": "1",
    "EUR": "4",
}


class SubscriptionLoader(BaseLoader):
    ENTITY_TYPE = "subscription"
    RECORD_TYPE = "subscription"
    CSV_PATH = config.SUBSCRIPTIONS_CSV

    def __init__(self, client, tracker):
        super().__init__(client, tracker)
        # Pre-load customer name → external ID mapping from customer CSV
        self._customer_name_to_ext_id = self._build_customer_name_map()

    def _build_customer_name_map(self) -> dict:
        """Read customer CSV to build company name → External ID 2 lookup."""
        mapping = {}
        try:
            with open(config.CUSTOMERS_CSV, "r", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    name = row.get("Company Name", "").strip().upper()
                    ext_id = row.get("External ID 2", "").strip()
                    if name and ext_id:
                        mapping[name] = ext_id
        except FileNotFoundError:
            logger.error(f"Customer CSV not found at {config.CUSTOMERS_CSV}")
        logger.info(f"Built customer name→extId map: {len(mapping)} entries")
        return mapping

    def get_external_id(self, row: dict) -> str:
        return row.get("External ID", "").strip()

    def get_tier3_field(self) -> Optional[str]:
        return "externalId"  # SuiteQL can search by externalid on subscription

    def get_tier3_value(self, row: dict) -> Optional[str]:
        return self.get_external_id(row)

    # ── Override prepare_records to handle grouping ──────────────────────

    def prepare_records(self) -> list[tuple[str, dict, dict]]:
        """Group CSV rows by External ID, then build one payload per group."""
        rows = self.read_csv()

        # Group rows by External ID (deal ID)
        groups = defaultdict(list)
        for row in rows:
            ext_id = row.get("External ID", "").strip()
            if ext_id:
                groups[ext_id].append(row)

        records = []
        for ext_id, group_rows in groups.items():
            payload = self._build_grouped_payload(ext_id, group_rows)
            if payload is None:
                logger.warning(f"Skipping subscription {ext_id}: payload build failed")
                continue
            records.append((ext_id, payload, group_rows[0]))

        return records

    def build_payload(self, row: dict) -> Optional[dict]:
        """Not used directly — see _build_grouped_payload."""
        raise NotImplementedError("Use prepare_records for grouped logic")

    def _build_grouped_payload(self, ext_id: str, rows: list[dict]) -> Optional[dict]:
        """Build a subscription payload from a group of CSV rows (1 header + N lines)."""
        header = rows[0]  # Header fields are identical across rows in the group

        customer_name = header.get("Customer", "").strip()
        customer_ext_id = self._customer_name_to_ext_id.get(customer_name.upper())
        if not customer_ext_id:
            logger.error(f"Subscription {ext_id}: cannot resolve customer '{customer_name}' to external ID")
            return None

        customer_ns_id = self.tracker.get_netsuite_id("customer", customer_ext_id)
        if not customer_ns_id:
            logger.error(
                f"Subscription {ext_id}: customer {customer_ext_id} has no NS ID. "
                f"Ensure customers are loaded first."
            )
            return None

        # Resolve billing account (may not exist for all subscriptions)
        billing_account_ext_id = f"{ext_id}_BA"
        billing_account_ns_id = self.tracker.get_netsuite_id("billingAccount", billing_account_ext_id)
        if not billing_account_ns_id:
            logger.warning(
                f"Subscription {ext_id}: no billing account found for {billing_account_ext_id}. "
                f"Will create subscription without billing account reference."
            )

        # ── Header fields ───────────────────────────────────────────────
        subsidiary_name = header.get("Subsidiary", "").strip()
        subsidiary_id = SUBSIDIARY_MAP.get(subsidiary_name)
        if not subsidiary_id:
            logger.error(
                f"Subscription {ext_id}: unmapped subsidiary '{subsidiary_name}' — cannot default. "
                f"Add it to SUBSIDIARY_MAP in loaders/subscription.py."
            )
            return None

        currency_code = header.get("Currency", "").strip()
        currency_id = CURRENCY_MAP.get(currency_code)
        if not currency_id:
            logger.error(
                f"Subscription {ext_id}: unmapped currency '{currency_code}' — cannot default. "
                f"Add it to CURRENCY_MAP in loaders/subscription.py."
            )
            return None

        payload = {
            "externalId": ext_id,
            "name": header.get("Subscription Name", "").strip(),
            "customer": {"id": customer_ns_id},
            "subsidiary": {"id": subsidiary_id},
            "currency": {"id": currency_id},
            "startDate": header.get("Start Date", "").strip(),
            "initialTerm": header.get("Initial Term", "").strip() or None,
        }

        # End date
        end_date = header.get("End Date", "").strip()
        if end_date:
            payload["endDate"] = end_date

        # Billing account (if resolved)
        if billing_account_ns_id:
            payload["billingAccount"] = {"id": billing_account_ns_id}

        # Subscription plan — needs NS internal ID
        # TODO: Resolve subscription plan name → NS internal ID
        #   Run SuiteQL: SELECT id, name FROM subscriptionplan
        #   Map: "HR Services rolling (LPG)" → id
        sub_plan = header.get("Subscription Plan", "").strip()
        if sub_plan:
            payload["subscriptionPlan"] = {"refName": sub_plan}
            # ↑ refName may work; if not, replace with {"id": "..."}

        # Price book
        price_book = header.get("Price Book", "").strip()
        if price_book and price_book != "NOT MAPPED":
            payload["priceBook"] = {"refName": price_book}

        # CPI Type — likely custom field
        # TODO: payload["custrecord_cpi_type"] = header.get("CPI Type", "").strip()

        # Default Renewal Term — likely custom field
        # TODO: payload["custrecord_renewal_term"] = header.get("Default Renewal Term", "").strip()

        # Indexation Date — likely custom field
        # TODO: payload["custrecord_indexation_date"] = header.get("Indexation Date", "").strip()

        # PO#
        po = header.get("PO#", "").strip()
        if po:
            # TODO: verify field name — might be custbody_po or a standard field
            payload["poNumber"] = po

        # ── Line items ──────────────────────────────────────────────────
        lines = []
        for row in rows:
            line = self._build_line(row)
            if line:
                lines.append(line)

        if lines:
            payload["subscriptionLine"] = {"items": lines}

        # Clean None values
        payload = {k: v for k, v in payload.items() if v is not None}

        return payload

    def _build_line(self, row: dict) -> Optional[dict]:
        """Build a single subscription line item from a CSV row."""
        sales_item = row.get("Sales Item", "").strip()
        include = row.get("Lines: Include", "").strip()

        if not sales_item or sales_item == "NOT MAPPED":
            return None

        line = {
            "subscriptionLineType": "1",  # Standard line type
            "include": include == "T",
            # Sales item — needs NS internal ID
            # TODO: Resolve sales item name → NS internal ID
            #   Run SuiteQL: SELECT id, itemid FROM item WHERE itemid = '...'
            "item": {"refName": sales_item},
        }

        return line
