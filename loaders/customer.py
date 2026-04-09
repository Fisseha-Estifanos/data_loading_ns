"""
Customer Loader
================
Maps customerskleeneexport CSV → NetSuite Customer records.
"""

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


class CustomerLoader(BaseLoader):
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
        # The following fields are MoorePay-specific custom fields in NetSuite.
        # You need to find their custom field IDs (custentity_xxx) from your NS instance.
        # Run: SELECT scriptid, label FROM customfield WHERE recordtype = 'customer'
        #
        # Once you have the IDs, add them to the payload like:
        #   payload["custentity_dunning_procedure"] = "Moorepay | Dunning Procedure (Customer)"
        #
        # Fields requiring custom field mapping:
        #   - Company Reg Number      → custentity_???
        #   - Segment                 → custentity_???  (value: "Moorepay")
        #   - Direct Debit            → custentity_???
        #   - Business/Class          → custentity_???  (value: "Managed Services")
        #   - Dunning Procedure       → custentity_???
        #   - Dunning Contact First Name → custentity_???
        #   - Dunning Contact Last Name  → custentity_???
        #   - Dunning Level (Req)     → custentity_???  (value: "Level 1 and Above")
        #   - Allow Letters to be Emailed → custentity_???
        #   - Email Preference        → custentity_???  (value: "PDF")
        #   - Electronic Email Recipients → custentity_???
        #   - Indexation Date         → custentity_???
        #   - PO Mandatory            → custentity_???
        #   - n/a 1 (Direct Debit)    → custentity_???
        #   - n/a 2 (NS Account No)   → custentity_???

        # Remove None values to keep payload clean
        payload = {k: v for k, v in payload.items() if v is not None}

        return payload

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
        if not country_code and country_raw:
            logger.warning(f"Unmapped country: '{country_raw}', defaulting to GB")
            country_code = "GB"

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
