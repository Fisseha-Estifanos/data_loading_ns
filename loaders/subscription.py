"""
Subscription Loader
====================
Maps subscriptionskleeneexport CSV → NetSuite Subscription records.

Key design:
  - CSV rows are grouped by External ID (deal ID) → one subscription per group
  - Header fields are taken from the first row in each group
  - Each row produces a list of item names to include (split from comma-separated Sales Item cells)
  - Customer is resolved by company name → customer CSV External ID 2 → state tracker
  - Billing account is resolved by {deal_id}_BA → state tracker (if available)

Two-step subscription creation:
  Step 1 — POST subscription header (subscriptionPlan but NO subscriptionLine items).
            NS auto-creates all plan lines from the plan, all with isIncluded=False.
  Step 2 — GET the created subscription's lines. For each item in the CSV that has
            Lines: Include = T, find its line number and PATCH it to isIncluded=True.

This is required because NS rejects manual subscriptionLine items without explicit prices.
When a plan is set, NS owns line creation; we only control which lines are included.

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

# Subscription term name (from CSV) → NS internal ID
# Confirmed via SuiteQL: SELECT id, name FROM subscriptionterm
# CSV "Custom Term" → NS id -102 ("Custom Term")
# CSV "Evergreen"   → NS id -101 ("Evergreen Term")
TERM_MAP = {
    "Custom Term": "-102",
    "Evergreen": "-101",
}


class SubscriptionLoader(BaseLoader):
    """Subscription Loader"""

    ENTITY_TYPE = "subscription"
    RECORD_TYPE = "subscription"
    CSV_PATH = config.SUBSCRIPTIONS_CSV

    def __init__(self, client, tracker):
        super().__init__(client, tracker)
        # Pre-load customer name → external ID mapping from customer CSV
        self._customer_name_to_ext_id = self._build_customer_name_map()
        # Keyed by ext_id → list of item names from CSV with Lines: Include = T
        # Populated during prepare_records(); consumed during load_all() step 2.
        self._pending_lines: dict[str, list[str]] = {}

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

    def _extract_item_names(self, row: dict) -> list[str]:
        """
        Extract individual item names from a CSV row's Sales Item cell.
        Splits comma-separated values and strips zero-width spaces (U+200B artefacts).
        Only returns names from rows where Lines: Include = T.
        """
        include = row.get("Lines: Include", "").strip()
        if include != "T":
            return []
        sales_item_raw = row.get("Sales Item", "").strip()
        if not sales_item_raw or sales_item_raw == "NOT MAPPED":
            return []
        names = []
        for part in sales_item_raw.split(","):
            name = part.strip().replace("\u200b", "")
            if name:
                names.append(name)
        return names

    def _build_grouped_payload(self, ext_id: str, rows: list[dict]) -> Optional[dict]:
        """
        Build a subscription POST payload from a group of CSV rows.
        Does NOT include subscriptionLine — NS auto-creates lines from the plan.
        Items to activate are stored in self._pending_lines[ext_id] for step 2.
        """
        header = rows[0]  # Header fields are identical across rows in the group

        customer_name = header.get("Customer", "").strip()
        customer_ext_id = self._customer_name_to_ext_id.get(customer_name.upper())
        if not customer_ext_id:
            logger.error(
                f"Subscription {ext_id}: cannot resolve customer '{customer_name}' to external ID"
            )
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
        billing_account_ns_id = self.tracker.get_netsuite_id(
            "billingAccount", billing_account_ext_id
        )
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
            "initialTerm": None,  # resolved below via TERM_MAP
        }

        # End date
        end_date = header.get("End Date", "").strip()
        if end_date:
            payload["endDate"] = end_date

        # Billing account (if resolved)
        if billing_account_ns_id:
            payload["billingAccount"] = {"id": billing_account_ns_id}

        # Initial Term — resolve via TERM_MAP (plain string rejected by NS)
        initial_term_raw = header.get("Initial Term", "").strip()
        if initial_term_raw:
            term_id = TERM_MAP.get(initial_term_raw)
            if term_id:
                payload["initialTerm"] = {"id": term_id}
            else:
                logger.error(
                    f"Subscription {ext_id}: unmapped Initial Term '{initial_term_raw}' — "
                    f"add to TERM_MAP in loaders/subscription.py"
                )
                return None

        # Default Renewal Term — required by NS, resolve via TERM_MAP
        renewal_term_raw = header.get("Default Renewal Term", "").strip()
        if renewal_term_raw:
            term_id = TERM_MAP.get(renewal_term_raw)
            if term_id:
                payload["defaultRenewalTerm"] = {"id": term_id}
            else:
                logger.error(
                    f"Subscription {ext_id}: unmapped Default Renewal Term '{renewal_term_raw}' — "
                    f"add to TERM_MAP in loaders/subscription.py"
                )
                return None

        # Default Renewal Subscription Plan
        renewal_plan = header.get("Default Renewal Subscription Plan", "").strip()
        if renewal_plan:
            payload["defaultRenewalSubscriptionPlan"] = {"refName": renewal_plan}

        # Subscription plan — NS uses this to auto-create subscription lines
        sub_plan = header.get("Subscription Plan", "").strip()
        if sub_plan:
            payload["subscriptionPlan"] = {"refName": sub_plan}

        # Price book
        price_book = header.get("Price Book", "").strip()
        if price_book and price_book != "NOT MAPPED":
            payload["priceBook"] = {"refName": price_book}

        # PO#
        po = header.get("PO#", "").strip()
        if po:
            payload["poNumber"] = po

        # CPI Type — likely custom field, skip for now
        # TODO: payload["custrecord_cpi_type"] = header.get("CPI Type", "").strip()

        # Indexation Date — likely custom field, skip for now
        # TODO: payload["custrecord_indexation_date"] = header.get("Indexation Date", "").strip()

        # ── Collect items to activate (step 2 — not in POST payload) ────
        items_to_include = []
        for row in rows:
            for name in self._extract_item_names(row):
                if name not in items_to_include:
                    items_to_include.append(name)
        self._pending_lines[ext_id] = items_to_include

        # Clean None values
        payload = {k: v for k, v in payload.items() if v is not None}
        logger.info(
            f"Subscription Payload: {payload} | lines_to_activate={items_to_include}"
        )
        return payload

    # ── Override load_all for two-step creation ──────────────────────────

    def load_all(self, limit: int = None) -> dict:
        """
        Two-step subscription creation:
          Step 1 — POST subscription header → NS creates all plan lines (isIncluded=False)
          Step 2 — GET lines, PATCH CSV-specified items to isIncluded=True
        """
        records = self.prepare_records()
        if limit is not None:
            records = records[:limit]
            logger.info(f"--limit {limit}: processing {len(records)} record(s)")
        run_id = self.tracker.start_run(self.ENTITY_TYPE)

        total = len(records)
        success = 0
        failed = 0
        skipped = 0

        logger.info(f"=== Loading {self.ENTITY_TYPE}: {total} records ===")

        for i, (ext_id, payload, row) in enumerate(records, 1):
            if self.tracker.is_already_loaded(self.ENTITY_TYPE, ext_id):
                logger.info(f"[{i}/{total}] SKIP {ext_id} (already loaded)")
                skipped += 1
                continue

            logger.info(f"[{i}/{total}] Creating {self.ENTITY_TYPE}: {ext_id}")

            # Step 1: POST subscription header
            status, ns_id, error = self.client.create_and_resolve_id(
                record_type=self.RECORD_TYPE,
                payload=payload,
                external_id=ext_id,
                tier3_field=self.get_tier3_field(),
                tier3_value=self.get_tier3_value(row) if row else None,
            )

            # Step 2: activate subscription lines (only if step 1 succeeded with a resolved ID)
            if status == "success" and ns_id:
                items = self._pending_lines.get(ext_id, [])
                if items:
                    self._activate_subscription_lines(ext_id, ns_id, items)
                else:
                    logger.info(f"  No lines to activate for {ext_id}")

            self.tracker.upsert_state(
                entity_type=self.ENTITY_TYPE,
                external_id=ext_id,
                status=status,
                netsuite_id=ns_id,
                error_message=error,
                payload_hash=self.hash_payload(payload),
                tier_used=None,
            )

            if status in ("success", "success_no_id"):
                success += 1
                if status == "success_no_id":
                    logger.warning(f"  ⚠ Record created but ID not resolved: {ext_id}")
            else:
                failed += 1
                logger.error(f"  ✗ Failed: {error}")

        self.tracker.finish_run(run_id, total, success, failed, skipped)
        summary = {"total": total, "success": success, "failed": failed, "skipped": skipped}
        logger.info(f"=== {self.ENTITY_TYPE} complete: {summary} ===")
        return summary

    def _activate_subscription_lines(
        self, ext_id: str, ns_id: str, items_to_include: list[str]
    ) -> None:
        """
        GET all auto-created lines for a subscription, then PATCH the ones in
        items_to_include to isIncluded=True.
        Logs a warning for any item not found in the plan's auto-created lines.
        """
        url = f"{config.BASE_URL}/subscription/{ns_id}?expandSubResources=true"
        resp = self.client._request("GET", url)
        if resp.status_code != 200:
            logger.error(
                f"  Cannot GET subscription {ns_id} to activate lines: HTTP {resp.status_code}"
            )
            return

        lines = resp.json().get("subscriptionLine", {}).get("items", [])
        if not lines:
            logger.warning(f"  Subscription {ns_id}: no auto-created lines found from plan")
            return

        # Build item refName → lineNumber map
        item_to_line_num: dict[str, int] = {
            line.get("item", {}).get("refName", ""): line.get("lineNumber")
            for line in lines
        }

        logger.info(
            f"  Activating {len(items_to_include)} line(s) for {ext_id} "
            f"(sub {ns_id}, plan has {len(lines)} total lines)"
        )

        for item_name in items_to_include:
            line_num = item_to_line_num.get(item_name)
            if line_num is None:
                logger.warning(
                    f"  ⚠ '{item_name}' not found in plan lines for sub {ns_id}. "
                    f"Available: {list(item_to_line_num.keys())}"
                )
                continue
            line_url = (
                f"{config.BASE_URL}/subscription/{ns_id}/subscriptionLine/{line_num}"
            )
            patch_resp = self.client._request("PATCH", line_url, {"isIncluded": True})
            if patch_resp.status_code == 204:
                logger.info(f"  ✓ Line {line_num} ({item_name}) → isIncluded=True")
            else:
                logger.error(
                    f"  ✗ Failed to activate line {line_num} ({item_name}): "
                    f"HTTP {patch_resp.status_code}: {patch_resp.text[:300]}"
                )
