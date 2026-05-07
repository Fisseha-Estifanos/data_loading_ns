"""
One-Off Invoice Loader
=======================
Maps one-off CSV → NetSuite Invoice records.

These are one-time charges linked to subscriptions.
Dependencies: Customer must be loaded first.

Grouping logic (1:N fan-out grain):
  The Snowflake DDL produces one row per (HS line item × NS sales item), so a
  single invoice can span multiple CSV rows all sharing the same Invoice External ID.
  This loader groups by Invoice External ID and builds one NS invoice per group,
  with each row in the group contributing one line item.

  Header fields (Customer, Subsidiary, Currency, Date) are taken from rows[0] —
  they are uniform across all rows in a group.
  Line fields (Item, Quantity, Rate, Description) differ per row.
"""

import csv
import logging
from collections import defaultdict
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

    # ── Override prepare_records to handle grouping ──────────────────────

    def prepare_records(self) -> list[tuple[str, dict, dict]]:
        """Group CSV rows by Invoice External ID, then build one payload per group.

        Grouping is keyed on the RAW invoice ext_id (no revision suffix). The
        records tuple uses the revisioned form so state-DB writes and POST
        externalId stay consistent.
        """
        rows = self.read_csv()

        groups: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            raw_ext_id = (row.get("Invoice External ID") or "").strip()
            if raw_ext_id:
                groups[raw_ext_id].append(row)

        records = []
        for raw_ext_id, group_rows in groups.items():
            payload = self._build_grouped_payload(raw_ext_id, group_rows)
            if payload is None:
                logger.warning(
                    f"Skipping one-off {raw_ext_id}: payload build failed"
                )
                continue
            ext_id = config.apply_revision(raw_ext_id)
            payload["externalId"] = ext_id
            records.append((ext_id, payload, group_rows[0]))

        return records

    def build_payload(self, row: dict) -> Optional[dict]:
        """Not used directly — see _build_grouped_payload."""
        raise NotImplementedError("Use prepare_records for grouped logic")

    def _build_grouped_payload(
        self, raw_ext_id: str, rows: list[dict]
    ) -> Optional[dict]:
        """
        Build a single invoice payload from a group of CSV rows that share the
        same Invoice External ID. Header fields come from rows[0]; each row in
        the group contributes one or more line items.

        `raw_ext_id` is the invoice external ID with NO revision suffix.
        Parent customer lookup suffixes the revision before consulting the state
        tracker, since the customer was written with a revisioned externalId.
        """
        header = rows[0]

        customer_name = header.get("Customer (Req)", "").strip()
        customer_ext_id = self._customer_name_to_ext_id.get(customer_name.upper())
        if not customer_ext_id:
            logger.error(
                f"One-off {raw_ext_id}: cannot resolve customer '{customer_name}'"
            )
            return None

        customer_ext_id_rev = config.apply_revision(customer_ext_id)
        customer_ns_id = self.tracker.get_netsuite_id("customer", customer_ext_id_rev)
        if not customer_ns_id:
            logger.error(
                f"One-off {raw_ext_id}: customer {customer_ext_id_rev} has no NS ID"
            )
            return None

        subsidiary_name = header.get("Subsidiary", "").strip()
        subsidiary_id = SUBSIDIARY_MAP.get(subsidiary_name)
        if not subsidiary_id:
            logger.error(
                f"One-off {raw_ext_id}: unmapped subsidiary "
                f"'{subsidiary_name}' — cannot default. "
                f"Add it to SUBSIDIARY_MAP in loaders/one_off.py."
            )
            return None

        currency_code = header.get("Currency", "").strip()
        currency_id = CURRENCY_MAP.get(currency_code)
        if not currency_id:
            logger.error(
                f"One-off {raw_ext_id}: unmapped currency "
                f"'{currency_code}' — cannot default. "
                f"Add it to CURRENCY_MAP in loaders/one_off.py."
            )
            return None

        tran_date = header.get("Date (Req)", "").strip()

        # Build line items from all rows in the group.
        # New grain: one NS sales item per row (comma-split kept for safety in case
        # an older-shaped CSV is ever reprocessed).
        line_items = []
        for row in rows:
            item_names_raw = (row.get("Item") or "").strip().replace("​", "")
            rate = (row.get("Rate per line item") or "").strip()
            quantity = (row.get("Quantity") or "").strip()
            description = (row.get("Description") or "").strip()

            if not quantity:
                logger.error(
                    f"One-off {raw_ext_id}: blank quantity on row with item "
                    f"'{item_names_raw}' — skipping this invoice group."
                )
                return None

            if item_names_raw and item_names_raw != "NOT MAPPED":
                item_names = [p.strip() for p in item_names_raw.split(",") if p.strip()]
            else:
                item_names = []

            if item_names:
                for name in item_names:
                    line: dict = {
                        "item": {"refName": name},
                        "quantity": float(quantity),
                    }
                    if rate:
                        line["rate"] = float(rate)
                    if description:
                        line["description"] = description
                    line_items.append(line)
            else:
                # Description-only line (no item reference)
                line = {"quantity": float(quantity)}
                if rate:
                    line["rate"] = float(rate)
                if description:
                    line["description"] = description
                line_items.append(line)

            # Revenue recognition dates — custom columns on the line; field names TBD
            # rev_start = (row.get("Revenue Start Date Per Line Item") or "").strip()
            # rev_end   = (row.get("Revenue End Date Per Line Item") or "").strip()
            # TODO: map to custcol_xxx once the script IDs are confirmed

        if not line_items:
            logger.error(
                f"One-off {raw_ext_id}: no line items could be built from "
                f"{len(rows)} row(s)"
            )
            return None

        # The base loader's prepare_records() will overwrite payload["externalId"]
        # with the revisioned form before POSTing.
        payload = {
            "externalId": raw_ext_id,
            "entity": {"id": customer_ns_id},
            "subsidiary": {"id": subsidiary_id},
            "currency": {"id": currency_id},
            "tranDate": tran_date,
            "item": {"items": line_items},
        }

        return {k: v for k, v in payload.items() if v is not None}
