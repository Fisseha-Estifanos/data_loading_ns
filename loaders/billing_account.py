"""
Billing Account Loader
=======================
Maps billingkleeneexport CSV → NetSuite Billing Account records.
Key dependency: Customer must be loaded first (needs customer NS internal ID).

Address resolution:
  NS requires billAddressList and shipAddressList on every billing account.
  These are not in the CSV — they are looked up live from NS at loader init
  via SuiteQL: SELECT internalid, entity, defaultbilling, defaultshipping
               FROM customeraddressbook WHERE defaultbilling = 'T' OR defaultshipping = 'T'
  One query at startup builds a customer_ns_id → address_id map for both
  billing and shipping. No extra API calls per record.
"""

import logging
from typing import Optional

import config
from loaders.base import BaseLoader

logger = logging.getLogger(__name__)


# NS hard limit on the billingAccount.name field. Names exceeding this fail
# with HTTP 400 "field name contained more than the maximum number ( 50 ) of
# characters allowed." We truncate visibly rather than skip the record.
_NS_BA_NAME_MAX = 50


def _truncate_ba_name(name: str, max_len: int = _NS_BA_NAME_MAX) -> tuple[str, bool]:
    """
    If name exceeds max_len, truncate the company-name portion while preserving
    the standard trailing `_<Frequency>_MP_<Currency>` suffix.

    Returns (new_name, was_truncated). new_name is guaranteed <= max_len.

    Linkage note: BAs are linked to subscriptions/customers by NS internal ID
    (resolved via the state tracker by externalId), NOT by name. Truncating
    here only affects the human-readable label in NS — downstream loaders
    are unaffected.
    """
    if len(name) <= max_len:
        return name, False
    # Locate the standard suffix: `_<Freq>_MP_<Curr>`. Find `_MP_` from the
    # right, then walk back to the underscore before it (start of `_Freq`).
    mp_idx = name.rfind("_MP_")
    if mp_idx == -1:
        return name[:max_len].rstrip(" ,._-&"), True
    freq_start = name.rfind("_", 0, mp_idx)
    if freq_start == -1:
        return name[:max_len].rstrip(" ,._-&"), True
    suffix = name[freq_start:]
    budget = max_len - len(suffix)
    if budget <= 0:
        # Suffix alone exceeds the limit — degrade to plain right-trim.
        return name[:max_len].rstrip(" ,._-&"), True
    company = name[:freq_start]
    truncated_company = company[:budget].rstrip(" ,._-&")
    return f"{truncated_company}{suffix}", True


class BillingAccountLoader(BaseLoader):

    ENTITY_TYPE = "billingAccount"
    RECORD_TYPE = "billingAccount"
    CSV_PATH = config.BILLING_CSV

    def __init__(self, client, tracker):
        super().__init__(client, tracker)
        self._bill_addr_map: dict = (
            {}
        )  # customer NS ID → default billing addressbook ID
        self._ship_addr_map: dict = (
            {}
        )  # customer NS ID → default shipping addressbook ID
        # Collected one entry per name truncation so main.py can re-emit them
        # at the bottom of the run.
        self.name_truncations: list[str] = []
        self._load_address_maps()

    def _load_address_maps(self):
        """
        Query NS once at startup for all customer default billing/shipping
        address IDs. Builds two dicts keyed by customer NS internal ID.
        """
        logger.info(
            "Fetching customer address book entries from NetSuite "
            "(needed for billAddressList / shipAddressList)..."
        )
        try:
            rows = self.client.suiteql_query(
                "SELECT internalid, entity, defaultbilling, defaultshipping "
                "FROM customeraddressbook "
                "WHERE defaultbilling = 'T' OR defaultshipping = 'T'"
                "ORDER BY entity, internalid"
            )
        except Exception as e:
            logger.error(
                f"Address map fetch failed: {e}. billAddressList/shipAddressList will be unresolvable."
            )
            return

        for row in rows:
            entity_id = str(row.get("entity", "")).strip()
            addr_id = str(row.get("internalid", "")).strip()
            if not entity_id or not addr_id:
                continue
            if str(row.get("defaultbilling", "")).upper() == "T":
                self._bill_addr_map[entity_id] = addr_id
            if str(row.get("defaultshipping", "")).upper() == "T":
                self._ship_addr_map[entity_id] = addr_id

        logger.info(
            f"Address maps loaded: {len(self._bill_addr_map)} billing, "
            f"{len(self._ship_addr_map)} shipping addresses fetched from NS."
        )

    def get_external_id(self, row: dict) -> str:
        return row.get("externalId", "").strip()

    def get_tier3_field(self) -> Optional[str]:
        return "name"

    def get_tier3_value(self, row: dict) -> Optional[str]:
        # Match the value POSTed (which may have been truncated) so SuiteQL
        # name-lookups still find the record we created.
        raw = row.get("name", "").strip()
        truncated, _ = _truncate_ba_name(raw)
        return truncated

    def build_payload(self, row: dict) -> Optional[dict]:
        ext_id = self.get_external_id(row)
        raw_name = row.get("name", "").strip()
        customer_ext_id = row.get("customer_externalId", "").strip()

        if not ext_id or not customer_ext_id:
            logger.warning(
                "Skipping billing account: missing externalId or customer_externalId"
            )
            return None

        # NS rejects names > 50 chars. Truncate the company portion while
        # preserving the `_<Frequency>_MP_<Currency>` suffix so the label is
        # still recognisable. Linkages downstream are by NS internal ID, not
        # name, so this is a label-only change.
        name, name_was_truncated = _truncate_ba_name(raw_name)
        if name_was_truncated:
            msg = (
                f"NAME TRUNCATED [{ext_id}]: {len(raw_name)} chars exceeds NS "
                f"{_NS_BA_NAME_MAX}-char limit — original: {raw_name!r}, "
                f"used: {name!r}"
            )
            logger.warning(msg)
            self.name_truncations.append(msg)

        # ── Resolve Customer Internal ID ────────────────────────────────
        # Customer was loaded with a revisioned externalId; the state tracker
        # is keyed on that, so suffix the raw CSV value before lookup.
        customer_ext_id_rev = config.apply_revision(customer_ext_id)
        customer_ns_id = self.tracker.get_netsuite_id("customer", customer_ext_id_rev)
        if not customer_ns_id:
            logger.error(
                f"Cannot create billing account {ext_id}: "
                f"customer {customer_ext_id_rev} has no NetSuite ID in state tracker. "
                f"Ensure customers are loaded first."
            )
            return None

        # ── Resolve Bill/Ship Address IDs ────────────────────────────────
        # NS requires billAddressList and shipAddressList on every billing account.
        # We look up the customer's default billing/shipping address from the
        # map built at init time (one SuiteQL query for all customers).
        bill_addr_id = self._bill_addr_map.get(str(customer_ns_id))
        ship_addr_id = self._ship_addr_map.get(str(customer_ns_id))

        logger.info(
            f"  {ext_id}: address lookup — "
            f"customer_ns_id={customer_ns_id}, "
            f"bill_addr={bill_addr_id}, ship_addr={ship_addr_id}"
        )

        if not bill_addr_id:
            logger.error(
                f"Billing account {ext_id}: no default billing address found in NS "
                f"for customer NS ID {customer_ns_id} ({customer_ext_id}). "
                f"Ensure the customer's addressBook was loaded with defaultBilling=true."
            )
            return None

        if not ship_addr_id:
            logger.error(
                f"Billing account {ext_id}: no default shipping address found in NS "
                f"for customer NS ID {customer_ns_id} ({customer_ext_id}). "
                f"Ensure the customer's addressBook was loaded with defaultShipping=true."
            )
            return None

        subsidiary_id = row.get("subsidiary_id", "").strip()
        currency_id = row.get("currency_id", "").strip()
        billing_schedule_id = row.get("billingSchedule_id", "").strip()
        frequency = row.get("frequency", "").strip()
        start_date = row.get("startDate", "").strip() or None

        payload = {
            "externalId": ext_id,
            "name": name,
            "customer": {"id": customer_ns_id},
            "subsidiary": {"id": subsidiary_id},
            "currency": {"id": currency_id},
            "frequency": {"id": frequency},
            "startDate": start_date,
            "customerDefault": row.get("customerDefault", "").strip().lower() == "true",
            "requestOffCycleInvoice": row.get("requestOffCycleInvoice", "")
            .strip()
            .lower()
            == "true",
            "inactive": row.get("inactive", "").strip().lower() == "true",
            "billAddressList": bill_addr_id,
            "shipAddressList": ship_addr_id,
        }

        if billing_schedule_id:
            payload["billingSchedule"] = {"id": billing_schedule_id}

        # Remove None values (e.g. blank startDate)
        payload = {k: v for k, v in payload.items() if v is not None}
        return payload

    def patch_startdates(self, dry_run: bool = False) -> dict:
        """
        Read the billing CSV and PATCH startDate for any billing account whose
        CSV startDate differs from the value currently in NetSuite.

        Used for A3 fix: correcting billing account start dates so that
        subscription start dates are no longer rejected by NS.
        """
        rows = self.read_csv()
        total = len(rows)
        patched = 0
        skipped = 0
        failed = 0

        logger.info(f"=== Billing account startDate patch: {total} records in CSV ===")

        for row in rows:
            ext_id = self.get_external_id(row)
            csv_start = row.get("startDate", "").strip()
            if not csv_start:
                logger.info(f"  SKIP {ext_id}: no startDate in CSV")
                skipped += 1
                continue

            # State tracker is keyed on the revisioned externalId.
            ns_id = self.tracker.get_netsuite_id(
                self.ENTITY_TYPE, config.apply_revision(ext_id)
            )
            if not ns_id:
                logger.warning(
                    f"  SKIP {ext_id}: no NS ID in state tracker (not yet loaded)"
                )
                skipped += 1
                continue

            # GET current startDate from NS
            get_resp = self.client._request(
                "GET", f"{config.BASE_URL}/billingAccount/{ns_id}"
            )
            if get_resp.status_code != 200:
                logger.error(
                    f"  {ext_id}: GET billingAccount/{ns_id} failed HTTP {get_resp.status_code}"
                )
                failed += 1
                continue

            ns_start = get_resp.json().get("startDate", "")
            # NS returns dates as ISO strings e.g. "2026-02-27"; CSV is same format
            if ns_start == csv_start:
                logger.info(f"  SKIP {ext_id}: startDate already {ns_start} — no change needed")
                skipped += 1
                continue

            logger.info(
                f"  PATCH {ext_id} (NS {ns_id}): startDate {ns_start} → {csv_start}"
                + (" [DRY RUN]" if dry_run else "")
            )

            if dry_run:
                patched += 1
                continue

            patch_resp = self.client._request(
                "PATCH",
                f"{config.BASE_URL}/billingAccount/{ns_id}",
                {"startDate": csv_start},
            )
            if patch_resp.status_code == 204:
                logger.info(f"  ✓ {ext_id}: startDate → {csv_start}")
                patched += 1
            else:
                logger.error(
                    f"  ✗ {ext_id}: PATCH failed HTTP {patch_resp.status_code}: "
                    f"{patch_resp.text[:300]}"
                )
                failed += 1

        summary = {"total": total, "patched": patched, "skipped": skipped, "failed": failed}
        logger.info(f"=== startDate patch complete: {summary} ===")
        return summary
