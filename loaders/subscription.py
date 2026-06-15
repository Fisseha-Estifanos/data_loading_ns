"""
Subscription Loader
====================
Maps subscriptionskleeneexport CSV → NetSuite Subscription records.

Key design:
  - CSV rows are grouped by External ID (deal ID) → one subscription per group
  - Most header fields are taken from the first row in each group; EXCEPT
    `Subscription Plan` and `Price Book`, which the DDL populates only on the
    plan-defining row (NULL on component/add-on rows). Those two fields are
    resolved by scanning all rows in the group for a non-empty value.
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
        # ext_id → ordered list of (item_name, price_plan_external_id_or_None).
        # price_plan_external_id is populated from the CSV's "Price Plan External ID"
        # column. None means the row had no price plan (NS will fall back to the
        # plan's default rate). The price plan internal ID is resolved at PATCH
        # time via the state tracker.
        self._pending_lines: dict[str, list[tuple[str, Optional[str]]]] = {}

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
        """Group CSV rows by External ID, then build one payload per group.

        Grouping is keyed on the RAW deal ID (no revision suffix). The records
        tuple returned to base.load_all uses the revisioned externalId so that
        state-DB writes and POST externalId are consistent.
        """
        rows = self.read_csv()

        # Group rows by raw External ID (deal ID)
        groups = defaultdict(list)
        for row in rows:
            raw_ext_id = row.get("External ID", "").strip()
            if raw_ext_id:
                groups[raw_ext_id].append(row)

        records = []
        for raw_ext_id, group_rows in groups.items():
            payload = self._build_grouped_payload(raw_ext_id, group_rows)
            if payload is None:
                logger.warning(
                    f"Skipping subscription {raw_ext_id}: payload build failed"
                )
                continue
            ext_id = config.apply_revision(raw_ext_id)
            payload["externalId"] = ext_id
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

        Note on grain: the post-2026-04-27 export is normalised to one NS sales
        item per row (no commas). The split-on-comma path is kept for safety in
        case an older-shaped CSV is reprocessed.
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

    def _build_grouped_payload(
        self, raw_ext_id: str, rows: list[dict]
    ) -> Optional[dict]:
        """
        Build a subscription POST payload from a group of CSV rows.
        Does NOT include subscriptionLine — NS auto-creates lines from the plan.
        Items to activate are stored in self._pending_lines[raw_ext_id] for step 2.

        `raw_ext_id` is the deal ID with NO revision suffix. Parent-entity
        lookups (customer, billing account, price plan) all suffix the revision
        before consulting the state tracker, since those parents were written
        with revisioned externalIds.
        """
        # Most header fields are uniform across rows in the group; see exceptions
        # noted inline for Subscription Plan and Price Book.
        header = rows[0]

        customer_name = header.get("Customer", "").strip()
        customer_ext_id = self._customer_name_to_ext_id.get(customer_name.upper())
        if not customer_ext_id:
            logger.error(
                f"Subscription {raw_ext_id}: cannot resolve customer "
                f"'{customer_name}' to external ID"
            )
            return None

        customer_ext_id_rev = config.apply_revision(customer_ext_id)
        customer_ns_id = self.tracker.get_netsuite_id("customer", customer_ext_id_rev)
        if not customer_ns_id:
            logger.error(
                f"Subscription {raw_ext_id}: customer {customer_ext_id_rev} "
                f"has no NS ID. Ensure customers are loaded first."
            )
            return None

        # Resolve billing account (may not exist for all subscriptions).
        # One billing account is shared across all subscriptions that split off
        # the same deal (e.g. AG HOTELS' 498737819895_27398 and any sibling
        # split share BA 498737819895_BA). The BA ext_id is therefore keyed on
        # the DEAL ID — the token before the first underscore in the sub's
        # External ID — not the full sub External ID:
        #   498737819895_27398   → deal 498737819895 → BA 498737819895_BA
        #   494812626113_a_27397 → deal 494812626113 → BA 494812626113_BA
        # Convention: revision is the last token, so the BA ext_id is
        # `<deal_id>_BA<LOAD_REVISION>` (e.g. 498737819895_BA_rvn_prod_01).
        deal_id = raw_ext_id.split("_", 1)[0]
        billing_account_ext_id = config.apply_revision(f"{deal_id}_BA")
        billing_account_ns_id = self.tracker.get_netsuite_id(
            "billingAccount", billing_account_ext_id
        )
        if not billing_account_ns_id:
            logger.warning(
                f"Subscription {raw_ext_id}: no billing account found for "
                f"{billing_account_ext_id}. Will create subscription without "
                f"billing account reference."
            )

        # ── Header fields ───────────────────────────────────────────────
        subsidiary_name = header.get("Subsidiary", "").strip()
        subsidiary_id = SUBSIDIARY_MAP.get(subsidiary_name)
        if not subsidiary_id:
            logger.error(
                f"Subscription {raw_ext_id}: unmapped subsidiary "
                f"'{subsidiary_name}' — cannot default. "
                f"Add it to SUBSIDIARY_MAP in loaders/subscription.py."
            )
            return None

        currency_code = header.get("Currency", "").strip()
        currency_id = CURRENCY_MAP.get(currency_code)
        if not currency_id:
            logger.error(
                f"Subscription {raw_ext_id}: unmapped currency "
                f"'{currency_code}' — cannot default. "
                f"Add it to CURRENCY_MAP in loaders/subscription.py."
            )
            return None

        # The base loader's prepare_records() will overwrite payload["externalId"]
        # with the revisioned form. We set the raw value here so build/log output
        # is human-readable; it's never the value POSTed.
        payload = {
            "externalId": raw_ext_id,
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
                    f"Subscription {raw_ext_id}: unmapped Initial Term "
                    f"'{initial_term_raw}' — add to TERM_MAP in "
                    f"loaders/subscription.py"
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
                    f"Subscription {raw_ext_id}: unmapped Default Renewal Term "
                    f"'{renewal_term_raw}' — add to TERM_MAP in "
                    f"loaders/subscription.py"
                )
                return None

        # Default Renewal Subscription Plan
        # NOTE (flagged, not fixed this pass): Default Renewal Subscription Plan is
        # derived from BASE.SUBSCRIPTION_PLAN in the DDL, so it has the same
        # non-uniform behaviour as Subscription Plan below — NULL on component
        # rows, populated only on the plan-defining row. If rows[0] is a component
        # row this will silently read blank and the field will be omitted from the
        # payload. Apply the same scan-across-rows fix if required.
        renewal_plan = header.get("Default Renewal Subscription Plan", "").strip()
        if renewal_plan:
            payload["defaultRenewalSubscriptionPlan"] = {"refName": renewal_plan}

        # Subscription plan — NS uses this to auto-create subscription lines.
        # Subscription Plan is NOT uniform across grouped rows: the DDL populates
        # it only on the plan-defining row and emits NULL on component/add-on
        # rows. Row order in the group is driven by CSV order, which is not
        # guaranteed, so `header`/`rows[0]` may land on a component row. Scan the
        # whole group to find the first non-empty value instead.
        sub_plan = next(
            (
                r.get("Subscription Plan", "").strip()
                for r in rows
                if r.get("Subscription Plan", "").strip()
            ),
            "",
        )
        if sub_plan:
            payload["subscriptionPlan"] = {"refName": sub_plan}

        # Price book — same non-uniform pattern as Subscription Plan. The
        # PRICE_BOOK_MAPPING join in the DDL keys on SUBSCRIPTION_PLAN, so Price
        # Book is NULL (→ 'NOT MAPPED' via COALESCE) wherever SUBSCRIPTION_PLAN is
        # NULL. Skip blanks and the 'NOT MAPPED' sentinel; take the first real
        # value found in the group.
        price_book = next(
            (
                r.get("Price Book", "").strip()
                for r in rows
                if r.get("Price Book", "").strip() not in ("", "NOT MAPPED")
            ),
            "",
        )
        if price_book:
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
        # The new CSV grain is one row per (HS line × NS sales item). Multiple
        # rows in a group can map to the same NS item refName (the 111
        # ambiguous combos identified during recon). Per the agreed Option 1
        # strategy: dedupe by item refName, keeping the FIRST non-blank
        # Price Plan External ID seen for that item. Divergent prices across
        # the dropped HS lines are not represented in NS — the retrofit /
        # reconciliation report calls those out separately.
        items_seen: dict[str, Optional[str]] = {}
        items_order: list[str] = []
        for row in rows:
            include = row.get("Lines: Include", "").strip()
            if include != "T":
                continue
            row_pp = (row.get("Price Plan External ID") or "").strip() or None
            for name in self._extract_item_names(row):
                if name not in items_seen:
                    items_seen[name] = row_pp
                    items_order.append(name)
                elif items_seen[name] is None and row_pp is not None:
                    # Backfill if the first row we saw had no PP but a later one does.
                    items_seen[name] = row_pp

        items_to_include = [(name, items_seen[name]) for name in items_order]
        # Key _pending_lines on the revisioned ext_id so load_all (which iterates
        # records keyed on the revisioned form) can find the items at PATCH time.
        self._pending_lines[config.apply_revision(raw_ext_id)] = items_to_include

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
        summary = {
            "total": total,
            "success": success,
            "failed": failed,
            "skipped": skipped,
        }
        logger.info(f"=== {self.ENTITY_TYPE} complete: {summary} ===")
        return summary

    def _activate_subscription_lines(
        self,
        ext_id: str,
        ns_id: str,
        items_to_include: list[tuple[str, Optional[str]]],
    ) -> None:
        """
        For each item in items_to_include, do two things:
          1. PATCH subscriptionLine/{n} → isIncluded=True   (include the line)
          2. Set the line's PRICE by repointing the subscription's auto-created
             priceInterval (matched by lineNumber) at the MP_PP_ price plan we
             loaded for that line.

        WHY (the price does NOT live on the subscription line):
        When NS creates a subscription from a plan + price book it auto-builds
        one `priceInterval` per line (keyed by subscriptionPlanLineNumber),
        each pointing at its OWN auto-generated price plan copied from the book
        — which for Moorepay's standard books is a £0 placeholder. The line's
        rate comes from THAT interval. PATCHing `subscriptionLine.pricePlan`
        does nothing (NS returns 204 and silently drops it — there is no such
        writable field). The supported mechanism, verified against NS, is to
        PATCH the interval's `pricePlan` to our loaded plan, which immediately
        sets the interval's recurringAmount. (totalintervalvalue stays 0 in
        DRAFT and computes on activation.)

        Each entry is (item_name, price_plan_external_id_or_None). Blank price
        plan → line included but its interval is left at the book default.
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
            logger.warning(
                f"  Subscription {ns_id}: no auto-created lines found from plan"
            )
            return

        # Build item refName → lineNumber map
        item_to_line_num: dict[str, int] = {
            line.get("item", {}).get("refName", ""): line.get("lineNumber")
            for line in lines
        }

        # Price intervals (where the price lives), keyed by lineNumber.
        intervals_by_line = self._get_price_intervals_by_line(ns_id)

        logger.info(
            f"  Activating {len(items_to_include)} line(s) for {ext_id} "
            f"(sub {ns_id}, plan has {len(lines)} total lines, "
            f"{len(intervals_by_line)} price intervals)"
        )

        for item_name, pp_ext_id in items_to_include:
            line_num = item_to_line_num.get(item_name)
            if line_num is None:
                logger.warning(
                    f"  ⚠ '{item_name}' not found in plan lines for sub {ns_id}. "
                    f"Available: {list(item_to_line_num.keys())}"
                )
                continue

            # 1. Include the line.
            line_url = (
                f"{config.BASE_URL}/subscription/{ns_id}/subscriptionLine/{line_num}"
            )
            inc_resp = self.client._request("PATCH", line_url, {"isIncluded": True})
            if inc_resp.status_code != 204:
                logger.error(
                    f"  ✗ Failed to include line {line_num} ({item_name}): "
                    f"HTTP {inc_resp.status_code}: {inc_resp.text[:300]}"
                )
                continue
            logger.info(f"  ✓ Line {line_num} ({item_name}) → isIncluded=True")

            # 2. Set the price on the matching interval.
            if not pp_ext_id:
                logger.info(
                    f"    (no price plan in CSV for {item_name}; "
                    f"interval left at book default)"
                )
                continue

            # CSV `Price Plan External ID` is raw; plans were loaded revisioned.
            pp_ext_id_rev = config.apply_revision(pp_ext_id)
            pp_ns_id = self.tracker.get_netsuite_id("pricePlan", pp_ext_id_rev)
            if not pp_ns_id:
                logger.warning(
                    f"  ⚠ Price plan {pp_ext_id_rev} not in state tracker; "
                    f"line {line_num} ({item_name}) left at book default."
                )
                continue

            interval = intervals_by_line.get(line_num)
            if not interval:
                logger.warning(
                    f"  ⚠ No price interval for line {line_num} ({item_name}) on "
                    f"sub {ns_id}; cannot set price. Interval lines available: "
                    f"{sorted(intervals_by_line)}"
                )
                continue

            interval_url = self._interval_self_url(interval, ns_id)
            price_resp = self.client._request(
                "PATCH", interval_url, {"pricePlan": {"id": pp_ns_id}}
            )
            if price_resp.status_code == 204:
                logger.info(
                    f"    ✓ priceInterval (line {line_num}) → pricePlan {pp_ns_id} "
                    f"(extId={pp_ext_id_rev})"
                )
            else:
                logger.error(
                    f"    ✗ Failed to set price on interval (line {line_num}, "
                    f"{item_name}): HTTP {price_resp.status_code}: "
                    f"{price_resp.text[:300]}"
                )

    def _get_price_intervals_by_line(self, ns_id: str) -> dict[int, dict]:
        """
        GET the subscription's priceInterval collection, keyed by lineNumber.
        This is the sublist that actually holds line prices (separate from
        subscriptionLine). Returns {} on failure.
        """
        url = f"{config.BASE_URL}/subscription/{ns_id}/priceInterval?expandSubResources=true"
        resp = self.client._request("GET", url)
        if resp.status_code != 200:
            logger.error(
                f"  Cannot GET price intervals for sub {ns_id}: "
                f"HTTP {resp.status_code}: {resp.text[:200]}"
            )
            return {}
        by_line: dict[int, dict] = {}
        for iv in resp.json().get("items", []):
            line_no = iv.get("lineNumber")
            if line_no is not None:
                by_line[line_no] = iv
        return by_line

    @staticmethod
    def _interval_self_url(interval: dict, ns_id: str) -> str:
        """
        Resolve the PATCH URL for a price interval. Prefer NS's own self link
        (a composite-key URL like
        .../priceInterval/subscriptionPlanLineNumber=1,startDate=2026-05-31);
        fall back to constructing that key from the interval fields.
        """
        for link in interval.get("links", []):
            if link.get("rel") == "self" and link.get("href"):
                return link["href"]
        spln = interval.get("subscriptionPlanLineNumber")
        start = interval.get("startDate")
        return (
            f"{config.BASE_URL}/subscription/{ns_id}/priceInterval/"
            f"subscriptionPlanLineNumber={spln},startDate={start}"
        )
