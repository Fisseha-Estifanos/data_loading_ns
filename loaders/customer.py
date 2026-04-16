"""
Customer Loader
================
Maps customerskleeneexport CSV → NetSuite Customer records.
"""

import json
import logging
from typing import Optional

import config
from loaders.base import BaseLoader

logger = logging.getLogger(__name__)

# ─── Reference Mappings ─────────────────────────────────────────────────
# These map CSV values to NetSuite internal IDs.
# Currency and subsidiary IDs are already numeric in the CSV.
# Country codes need mapping from display name → NS country code.

COUNTRY_MAP = {
    "united kingdom": "GB",
    "ireland": "IE",
    "the netherlands": "NL",
    "united arab emirates": "AE",
    # Data quality issues in source: these are counties/cities, default to GB
    "hampshire": "GB",
    "luton": "GB",
}

# Currency display → NS internal ID (confirmed from billing CSV: GBP=1, EUR=4)
CURRENCY_MAP = {
    "GBP": "1",
    "EUR": "4",
}

# Business/Class segment → NS internal ID
# Confirmed via SuiteQL on customrecord_cseg_busclass: "Managed Services" = 1
BUSCLASS_MAP = {
    "managed services": "1",
}

# Segment → NS internal ID
# Confirmed via SuiteQL on customrecord_cseg_segment: "Moorepay" = 2
SEGMENT_MAP = {
    "moorepay": "2",
}

# Dunning Procedure → NS internal ID
# Confirmed by GETting existing Moorepay customer (id=578027): ID 6
DUNNING_PROCEDURE_MAP = {
    "moorepay | dunning procedure (customer)": "6",
}


def _parse_bool(value: str) -> Optional[bool]:
    """Parse Y/N or True/False CSV strings to Python bool. Returns None if blank/unrecognised."""
    v = value.strip().upper()
    if not v:
        return None
    if v in ("Y", "YES", "TRUE", "1"):
        return True
    if v in ("N", "NO", "FALSE", "0"):
        return False
    return None


def _apply_custom_fields(row: dict, payload: dict, ext_id: str) -> None:
    """
    Mutates payload in-place with all confirmed MoorePay custom fields.
    Called from both build_payload() (new loads) and build_patch_payload() (patching).

    Confirmed NS IDs:
      cseg_busclass          "Managed Services" → {"id": "1"}
      cseg_segment           "Moorepay"         → {"id": "2"}
      custentity_3805_dunning_procedure  → {"id": "6"}
      custentity_3805_dunning_letters_toemail → bool
      emailpreference        → plain string ("PDF")
      custentity_alf_company_reg_num → plain string from CSV
      custentityindexationdatecustomer → date string from CSV
      custentity_zellis_po_mandatory   → bool
      custentity_2663_direct_debit     → bool

    Deferred (awaiting client):
      custentity_3805_dunning_level      — can't resolve ID via SuiteQL/REST
      custentity_zellis_elec_email_recipients — Phase 2 (linked record)
      custentity6/9/15_2/19/376 labels   — awaiting client
    """
    # ── Business/Class (custom segment) ─────────────────────────────────
    busclass_raw = row.get("Business/Class", "").strip()
    if busclass_raw:
        busclass_id = BUSCLASS_MAP.get(busclass_raw.lower())
        if busclass_id:
            payload["cseg_busclass"] = {"id": busclass_id}
        else:
            logger.error(
                f"Unmapped Business/Class '{busclass_raw}' for {ext_id} — "
                f"add to BUSCLASS_MAP in loaders/customer.py"
            )

    # ── Segment (custom segment) ─────────────────────────────────────────
    segment_raw = row.get("Segment", "").strip()
    if segment_raw:
        segment_id = SEGMENT_MAP.get(segment_raw.lower())
        if segment_id:
            payload["cseg_segment"] = {"id": segment_id}
        else:
            logger.error(
                f"Unmapped Segment '{segment_raw}' for {ext_id} — "
                f"add to SEGMENT_MAP in loaders/customer.py"
            )

    # ── Dunning Procedure ────────────────────────────────────────────────
    dunning_raw = row.get("Dunning Procedure", "").strip()
    if dunning_raw:
        dunning_id = DUNNING_PROCEDURE_MAP.get(dunning_raw.lower())
        if dunning_id:
            payload["custentity_3805_dunning_procedure"] = {"id": dunning_id}
        else:
            logger.error(
                f"Unmapped Dunning Procedure '{dunning_raw}' for {ext_id} — "
                f"add to DUNNING_PROCEDURE_MAP in loaders/customer.py"
            )

    # ── Allow Letters to be Emailed ──────────────────────────────────────
    allow_letters = _parse_bool(row.get("Allow Letters to be Emailed", ""))
    if allow_letters is not None:
        payload["custentity_3805_dunning_letters_toemail"] = allow_letters

    # ── Email Preference ─────────────────────────────────────────────────
    email_pref = row.get("Email Preference", "").strip()
    if email_pref:
        payload["emailpreference"] = email_pref

    # ── Company Reg Number ───────────────────────────────────────────────
    company_reg = row.get("Company Reg Number", "").strip()
    if company_reg:
        payload["custentity_alf_company_reg_num"] = company_reg

    # ── Indexation Date (strip time component) ───────────────────────────
    indexation_raw = row.get("Indexation Date", "").strip()
    if indexation_raw:
        # CSV format: "2027-01-16T00:00:00Z" → NS wants "2027-01-16"
        payload["custentityindexationdatecustomer"] = indexation_raw.split("T")[0]

    # ── PO Mandatory ─────────────────────────────────────────────────────
    po_mandatory = _parse_bool(row.get("PO Mandatory", ""))
    if po_mandatory is not None:
        payload["custentity_zellis_po_mandatory"] = po_mandatory

    # ── Direct Debit ─────────────────────────────────────────────────────
    direct_debit = _parse_bool(row.get("Direct Debit", ""))
    if direct_debit is not None:
        payload["custentity_2663_direct_debit"] = direct_debit


class CustomerLoader(BaseLoader):
    """Customer Loader Class"""

    ENTITY_TYPE = "customer"
    RECORD_TYPE = "customer"
    CSV_PATH = config.CUSTOMERS_CSV

    def get_external_id(self, row: dict) -> str:
        return row.get("External ID 2", "").strip()

    def get_tier3_field(self) -> Optional[str]:
        return "companyName"

    def get_tier3_value(self, row: dict) -> Optional[str]:
        return row.get("Company Name", "").strip()

    def build_payload(self, row: dict) -> Optional[dict]:
        ext_id = self.get_external_id(row)
        company_name = row.get("Company Name", "").strip()

        if not company_name:
            logger.warning(f"Skipping {ext_id}: no company name")
            return None

        subsidiary_id = row.get("Primary Entity (Req)", "").strip()
        currency_code = row.get("Currency", "").strip()
        currency_id = CURRENCY_MAP.get(currency_code)

        payload = {
            "externalId": ext_id,
            "companyName": company_name,
            "isPerson": False,
            "subsidiary": {"id": subsidiary_id},
            "email": row.get("Email", "").strip() or None,
            "phone": row.get("Phone", "").strip() or None,
            "altPhone": row.get("Alt. Phone", "").strip() or None,
        }

        # Currency
        if currency_id:
            payload["currency"] = {"id": currency_id}

        # Terms — requires NS internal ID lookup.
        # TODO: Replace with actual internal ID from your NetSuite instance.
        #   Run SuiteQL: SELECT id, name FROM term WHERE name LIKE '%Z030%'
        #   Then hardcode the ID here or resolve dynamically.
        terms_value = row.get("Terms", "").strip()
        if terms_value:
            payload["terms"] = {"refName": terms_value}
            # ↑ refName *may* work for matching; if not, replace with {"id": "..."}

        # Job title (maps to the primary entity "title" field)
        title = row.get("Job Title", "").strip()
        if title:
            payload["title"] = title

        # ── Address Book ────────────────────────────────────────────────
        address = self._build_address(row)
        if address:
            payload["addressBook"] = {"items": [address]}

        # ── Custom Fields ───────────────────────────────────────────────
        _apply_custom_fields(row, payload, ext_id)

        # Remove None values to keep payload clean
        payload = {k: v for k, v in payload.items() if v is not None}
        logger.info(f"Customer Payload: {payload}")
        return payload

    def build_patch_payload(self, row: dict) -> Optional[dict]:
        """
        Build a partial payload containing only custom fields.
        Used with --patch to update already-loaded customers without
        re-sending standard fields.
        """
        ext_id = self.get_external_id(row)
        payload: dict = {}
        _apply_custom_fields(row, payload, ext_id)
        if not payload:
            logger.warning(f"Patch payload for {ext_id} is empty — nothing to update")
            return None
        return payload

    def patch_all(self, dry_run: bool = False, limit: int = None) -> dict:
        """PATCH all customer records with custom field values."""
        rows = self.read_csv()
        if limit is not None:
            rows = rows[:limit]
        total = len(rows)
        success = 0
        failed = 0
        skipped = 0

        logger.info(f"=== Patching {self.ENTITY_TYPE}: {total} records ===")

        for i, row in enumerate(rows, 1):
            ext_id = self.get_external_id(row)
            if not ext_id:
                logger.warning(f"[{i}/{total}] Skipping row with no external ID")
                skipped += 1
                continue

            payload = self.build_patch_payload(row)
            if payload is None:
                logger.warning(f"[{i}/{total}] SKIP {ext_id}: no patch fields")
                skipped += 1
                continue

            if dry_run:
                logger.info(
                    f"[{i}/{total}] DRY RUN PATCH {ext_id}:\n"
                    f"{json.dumps(payload, indent=2)}"
                )
                success += 1
                continue

            logger.info(f"[{i}/{total}] Patching customer: {ext_id}")
            resp = self.client.patch_record(self.RECORD_TYPE, ext_id, payload)

            if resp.status_code in (200, 204):
                logger.info(f"  ✓ Patched {ext_id} (HTTP {resp.status_code})")
                success += 1
            else:
                error_msg = f"HTTP {resp.status_code}: {resp.text[:500]}"
                logger.error(f"  ✗ Patch failed for {ext_id}: {error_msg}")
                failed += 1

        summary = {
            "total": total,
            "success": success,
            "failed": failed,
            "skipped": skipped,
        }
        logger.info(f"=== customer patch complete: {summary} ===")
        return summary

    def _build_address(self, row: dict) -> Optional[dict]:
        """Build a NetSuite addressBook entry from CSV address fields."""
        addr1 = row.get("Address 1 : Address 1", "").strip()
        addr2 = row.get("Address 1 : Address 2", "").strip()
        city = row.get("Address 1 : City", "").strip()
        state = row.get("Address 1 : County", "").strip()
        zip_code = row.get("Address 1 : Post Code", "").strip()
        country_raw = row.get("Address 1 : Country (Req) 1", "").strip()
        addressee = row.get("addressee", "").strip()
        attention_first = row.get("Attention First Name", "").strip()
        attention_last = row.get("Attention Last Name", "").strip()

        # Need at least some address data
        if not any([addr1, city, zip_code]):
            return None

        country_code = COUNTRY_MAP.get(country_raw.lower(), "")
        # if not country_code and country_raw:
        #     logger.error(
        #         f"Unmapped country value '{country_raw}' — cannot default. "
        #         f"Add it to COUNTRY_MAP in loaders/customer.py."
        #     )
        #     return None
        if not country_code:
            subsidiary = row.get("Primary Entity (Req)", "").strip()
            country_code = "IE" if subsidiary == "66" else "GB"
            logger.warning(
                f"Country '{country_raw or '(blank)'}' → defaulted to {country_code} "
                f"(subsidiary={subsidiary})"
            )

        address_entry = {
            "defaultBilling": True,
            "defaultShipping": True,
            "label": "Primary",
            "addressBookAddress": {
                "addressee": addressee or None,
                "attention": f"{attention_first} {attention_last}".strip() or None,
                "addr1": addr1 or None,
                "addr2": addr2 or None,
                "city": city or None,
                "state": state or None,
                "zip": zip_code or None,
                "country": {"id": country_code} if country_code else None,
            },
        }

        # Clean nested nulls
        address_entry["addressBookAddress"] = {
            k: v
            for k, v in address_entry["addressBookAddress"].items()
            if v is not None
        }

        return address_entry
