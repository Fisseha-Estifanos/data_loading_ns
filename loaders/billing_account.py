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

import csv
import logging
from collections import defaultdict
from typing import Optional

import config
from loaders.base import BaseLoader
from customer_resolver import CustomerResolver

logger = logging.getLogger(__name__)


def _deal_id(external_id: str) -> str:
    """Deal id = the external id token before the first underscore."""
    return external_id.split("_", 1)[0]


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
        # Resolve each BA's customer to its NS internal id directly from NS by
        # C-number / externalId (no state tracker) — so a BA row for a
        # pre-existing customer loads without seeding.
        self.resolver = CustomerResolver(client)
        self._prefetch_customers()

    def _prefetch_customers(self) -> None:
        """One batched NS query for every customer the billing CSV references."""
        cnums, ext2s = set(), set()
        for r in self.read_csv():
            ext2 = (r.get("customer_externalId") or "").strip()
            if ext2:
                ext2s.add(ext2)
                c = self.resolver.ext2_to_cnum.get(ext2)
                if c:
                    cnums.add(c)
        self.resolver.prefetch(cnums, ext2s)

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
        # Resolve straight from NS by C-number (bridged from customer_externalId
        # via the customer CSV) or by the loaded/raw externalId — no state
        # tracker, so pre-existing customers resolve without seeding.
        cnum = self.resolver.ext2_to_cnum.get(customer_ext_id)
        customer_ns_id = self.resolver.resolve(cnum, customer_ext_id)
        if not customer_ns_id:
            logger.error(
                f"Cannot create billing account {ext_id}: customer "
                f"{customer_ext_id!r} not found in NS (by C-number or externalId)."
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

    # ── Create missing BAs (folds in the old create_billing_accounts.py) ──────
    def create_missing(self, dry_run: bool = True) -> dict:
        """Create a Billing Account for every subscription customer that has NONE
        — no BA in NS and no BA row in the billing CSV.

        Derives the bill/ship address from the customer's NS address book and the
        rest (currency, frequency, start date) from the subscription; resolves the
        customer by C-number (no state-tracker seeding). Required references are
        never invented — a deal whose currency is unmapped, whose frequency is
        blank, whose customer lacks an address, or whose billing schedule can't be
        derived from the batch's other BAs is SKIPPED with a logged reason.

        Dry-run (default) logs the payloads it would POST; ``dry_run=False`` POSTs
        them and records each in the state tracker as ``<deal>_BA`` so the
        subscription links to it.
        """
        from loaders.subscription import CURRENCY_MAP

        ba_rows = self.read_csv()
        ba_deals = {_deal_id((r.get("externalId") or "").strip()) for r in ba_rows}
        # NS requires a billingSchedule; derive it from the batch's own BAs by
        # frequency (UPPER) so a synthesized BA matches — never invented.
        sched_by_freq: dict[str, set] = defaultdict(set)
        for r in ba_rows:
            fq = (r.get("frequency") or "").strip().upper()
            sid = (r.get("billingSchedule_id") or "").strip()
            if fq and sid:
                sched_by_freq[fq].add(sid)

        # Group subs by deal.
        deal_rows: dict[str, list] = defaultdict(list)
        try:
            with open(config.SUBSCRIPTIONS_CSV, encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    e = (row.get("External ID") or "").strip()
                    if e:
                        deal_rows[_deal_id(e)].append(row)
        except FileNotFoundError:
            logger.error("Subs CSV not found at %s", config.SUBSCRIPTIONS_CSV)
            return {"created": 0, "failed": 0, "skipped": 0}

        # Resolve the subs customers (the resolver was prefetched for the billing
        # CSV's customers; add the subs customers too) and find which already have
        # a BA in NS.
        def _first(rows, *cols):
            for col in cols:
                for r in rows:
                    v = (r.get(col) or "").strip()
                    if v:
                        return v
            return ""

        cnums, ext2s = set(), set()
        for rows in deal_rows.values():
            cnums.add(
                _first(rows, "NETSUITE_ACCOUNT_NUMBER", "NETSUITE_ACCOUNT_NUMBER_COMPANY_LEVEL")
            )
            nm = rows[0].get("Customer", "")
            nc = self.resolver.name_cnum(nm)
            if nc:
                cnums.add(nc)
            ex = self.resolver.name_ext(nm)
            if ex:
                ext2s.add(ex)
        self.resolver.prefetch({c for c in cnums if c}, ext2s)

        # Which of these customers already have a BA in NS?
        cust_ids = set()
        for rows in deal_rows.values():
            cnum = (
                _first(rows, "NETSUITE_ACCOUNT_NUMBER", "NETSUITE_ACCOUNT_NUMBER_COMPANY_LEVEL")
                or self.resolver.name_cnum(rows[0].get("Customer", ""))
            )
            rec = self.resolver.resolve_record(cnum, self.resolver.name_ext(rows[0].get("Customer", "")))
            if rec and rec.get("id"):
                cust_ids.add(str(rec["id"]))
        has_ba = self._customers_with_ba(cust_ids)

        payloads, skips = [], []
        for deal, rows in sorted(deal_rows.items()):
            if deal in ba_deals:
                continue  # a BA row is already being loaded for this deal
            name = (rows[0].get("Customer") or "").strip()
            cnum = (
                _first(rows, "NETSUITE_ACCOUNT_NUMBER", "NETSUITE_ACCOUNT_NUMBER_COMPANY_LEVEL")
                or self.resolver.name_cnum(name)
            )
            rec = self.resolver.resolve_record(cnum, self.resolver.name_ext(name))
            if not (rec and rec.get("id")):
                continue  # not in NS — the customer load owns this
            cid = str(rec["id"])
            if cid in has_ba:
                continue  # already has a BA in NS — skip
            bill_addr, ship_addr = self._bill_addr_map.get(cid), self._ship_addr_map.get(cid)
            if not (bill_addr and ship_addr):
                skips.append((deal, f"customer {name!r} (id {cid}) has no default "
                                    f"billing/shipping address in NS — run "
                                    f"repair_customer_addresses.py first"))
                continue
            currency_name = _first(rows, "Currency")
            currency_id = CURRENCY_MAP.get(currency_name)
            if not currency_id:
                skips.append((deal, f"currency {currency_name!r} not in CURRENCY_MAP"))
                continue
            frequency = _first(rows, "PRICE_BOOK_FREQUENCY").upper()
            if not frequency:
                skips.append((deal, "PRICE_BOOK_FREQUENCY is blank (won't invent one)"))
                continue
            subsidiary_id = rec.get("subsidiary")
            if not subsidiary_id:
                skips.append((deal, f"customer {name!r} has no NS subsidiary on record"))
                continue
            scheds = sched_by_freq.get(frequency)
            if not scheds:
                skips.append((deal, f"no billing schedule known for frequency "
                                    f"{frequency!r} (no {frequency} BA in the billing CSV)"))
                continue
            if len(scheds) > 1:
                skips.append((deal, f"ambiguous billing schedule for {frequency!r}: "
                                    f"{sorted(scheds)}"))
                continue
            start_dates = sorted(
                (r.get("Start Date") or "").strip() for r in rows
                if (r.get("Start Date") or "").strip()
            )
            ext_id = config.apply_revision(f"{deal}_BA")
            ba_name, _ = _truncate_ba_name(f"{name}_{frequency}_MP_{currency_name}")
            payload = {
                "externalId": ext_id,
                "name": ba_name,
                "customer": {"id": cid},
                "subsidiary": {"id": str(subsidiary_id)},
                "currency": {"id": str(currency_id)},
                "frequency": {"id": frequency},
                "billingSchedule": {"id": next(iter(scheds))},
                "customerDefault": False,
                "requestOffCycleInvoice": False,
                "inactive": False,
                "billAddressList": bill_addr,
                "shipAddressList": ship_addr,
            }
            if start_dates:
                payload["startDate"] = start_dates[0]
            payloads.append((ext_id, payload, name))

        for deal, reason in skips:
            logger.warning("create_missing SKIP deal %s: %s", deal, reason)
        logger.info(
            "create_missing: %d BA(s) to create, %d skipped", len(payloads), len(skips)
        )

        created = failed = 0
        import json
        for ext_id, payload, name in payloads:
            if dry_run:
                logger.info("DRY RUN would create %s (%s):\n%s", ext_id, name,
                            json.dumps(payload, indent=2))
                continue
            status, ns_id, error = self.client.create_and_resolve_id(
                record_type="billingAccount", payload=payload,
                external_id=ext_id, tier3_field="externalId", tier3_value=ext_id,
            )
            self.tracker.upsert_state(
                entity_type="billingAccount", external_id=ext_id, status=status,
                netsuite_id=ns_id, error_message=error, payload_hash=None,
                tier_used="create_missing",
            )
            if status == "success":
                created += 1
                logger.info("  ✓ %s → NS id %s", ext_id, ns_id)
            else:
                failed += 1
                logger.error("  ✗ %s: %s", ext_id, error)
        return {"created": created, "failed": failed, "skipped": len(skips),
                "dry_run": dry_run, "total": len(payloads)}

    def _customers_with_ba(self, customer_internal_ids) -> set:
        """Set of customer internal ids that already have ≥1 Billing Account in NS."""
        have = set()
        ids = sorted({str(i) for i in customer_internal_ids if i})
        for i in range(0, len(ids), 200):
            chunk = ids[i : i + 200]
            in_clause = ",".join("'" + c + "'" for c in chunk)
            q = f"SELECT customer FROM billingaccount WHERE customer IN ({in_clause})"
            for row in self.client.suiteql_query(q):
                c = row.get("customer")
                if c:
                    have.add(str(c))
        return have

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
