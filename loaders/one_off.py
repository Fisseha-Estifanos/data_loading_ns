"""
One-Off Invoice Loader
=======================
Maps oneoffkleeneexport CSV → NetSuite Invoice (or Custom) records.

These are one-time charges linked to subscriptions.
Dependencies: Customer must be loaded first.
"""

import csv
import logging
from typing import Optional

import config
from loaders.base import BaseLoader

logger = logging.getLogger(__name__)

SUBSIDIARY_MAP = {
    "Moorepay Ltd": "12",
    "Moorepay Ireland": "66",
}

CURRENCY_MAP = {
    "GBP": "1",
    "EUR": "4",
}


class OneOffLoader(BaseLoader):
    """One Off Loader"""

    ENTITY_TYPE = "oneOff"
    RECORD_TYPE = "invoice"  # TODO: Verify — might be 'customSale' or 'cashSale'
    CSV_PATH = config.ONEOFF_CSV

    def __init__(self, client, tracker):
        super().__init__(client, tracker)
        # Build customer name → external ID from customer CSV
        self._customer_name_to_ext_id = {}
        try:
            with open(config.CUSTOMERS_CSV, "r", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    name = row.get("Company Name", "").strip().upper()
                    ext_id = row.get("External ID 2", "").strip()
                    if name and ext_id:
                        self._customer_name_to_ext_id[name] = ext_id
        except FileNotFoundError:
            logger.error(f"Customer CSV not found: {config.CUSTOMERS_CSV}")

    def get_external_id(self, row: dict) -> str:
        return row.get("Invoice External ID", "").strip()

    def get_tier3_field(self) -> Optional[str]:
        return "externalId"

    def get_tier3_value(self, row: dict) -> Optional[str]:
        return self.get_external_id(row)

    def build_payload(self, row: dict) -> Optional[dict]:
        ext_id = self.get_external_id(row)
        customer_name = row.get("Customer (Req)", "").strip()

        # Resolve customer
        customer_ext_id = self._customer_name_to_ext_id.get(customer_name.upper())
        if not customer_ext_id:
            logger.error(f"One-off {ext_id}: cannot resolve customer '{customer_name}'")
            return None

        customer_ns_id = self.tracker.get_netsuite_id("customer", customer_ext_id)
        if not customer_ns_id:
            logger.error(f"One-off {ext_id}: customer {customer_ext_id} has no NS ID")
            return None

        subsidiary_name = row.get("Subsidiary", "").strip()
        subsidiary_id = SUBSIDIARY_MAP.get(subsidiary_name)
        if not subsidiary_id:
            logger.error(
                f"One-off {ext_id}: unmapped subsidiary '{subsidiary_name}' — cannot default. "
                f"Add it to SUBSIDIARY_MAP in loaders/one_off.py."
            )
            return None

        currency_code = row.get("Currency", "").strip()
        currency_id = CURRENCY_MAP.get(currency_code)
        if not currency_id:
            logger.error(
                f"One-off {ext_id}: unmapped currency '{currency_code}' — cannot default. "
                f"Add it to CURRENCY_MAP in loaders/one_off.py."
            )
            return None

        rate = row.get("Rate per line item", "").strip()
        quantity = row.get("Quantity", "").strip()
        if not quantity:
            logger.error(
                f"One-off {ext_id}: blank quantity — cannot default to 1. "
                f"Ensure the source CSV has a Quantity value for this row."
            )
            return None
        description = row.get("Description", "").strip()
        tran_date = row.get("Date (Req)", "").strip()
        item_name = row.get("Item", "").strip()

        payload = {
            "externalId": ext_id,
            "entity": {"id": customer_ns_id},
            "subsidiary": {"id": subsidiary_id},
            "currency": {"id": currency_id},
            "tranDate": tran_date,
            "item": {
                "items": [
                    {
                        "item": (
                            {"refName": item_name}
                            if item_name and item_name != "NOT MAPPED"
                            else None
                        ),
                        "quantity": float(quantity),
                        "rate": rate,
                        "description": description,
                    }
                ]
            },
        }

        # Revenue recognition dates — likely custom columns on the line
        rev_start = row.get("Revenue Start Date Per Line Item", "").strip()
        rev_end = row.get("Revenue End Date Per Line Item", "").strip()
        if rev_start:
            # TODO: Map to custcol_xxx on the line item
            pass
        if rev_end:
            # TODO: Map to custcol_xxx on the line item
            pass

        # Clean None from line items
        payload["item"]["items"] = [
            {k: v for k, v in line.items() if v is not None}
            for line in payload["item"]["items"]
        ]

        payload = {k: v for k, v in payload.items() if v is not None}
        logger.info(f"One Off Payload: {payload}")
        return payload
