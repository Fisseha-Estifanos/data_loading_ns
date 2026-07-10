"""
Customer → NetSuite internal id resolution, by C-number (entityid) ONLY
========================================================================
Shared by the subscription and billing-account loaders so both reach a customer
the SAME way the pre-load validator does — directly from NetSuite, not via the
state tracker.

CONVENTION (since 2026-07-09): the ONLY key used to resolve a customer in NS is
its **C-number** (NS ``entityid``, e.g. C43928). No externalId resolution, no
company-name matching against NS. Everything else — the customer CSV's
"Company Name" / "External ID 2" columns, the billing CSV's
``customer_externalId`` — is only ever a *bridge to a C-number*, never an NS
lookup key.

Where the C-number comes from (in order):
  • Subscriptions: the subs export's own ``NETSUITE_ACCOUNT_NUMBER`` then
    ``NETSUITE_ACCOUNT_NUMBER_COMPANY_LEVEL`` columns; else the customer CSV's
    C-number column, bridged by Customer name (alias-resolved).
  • Billing accounts: the owning SUB's C-number (BA externalId minus ``_BA`` →
    sub External ID → subs export columns); else the customer CSV's C-number,
    bridged by ``customer_externalId`` → "External ID 2".

Consequence to be aware of: a customer CREATED by this loader gets an
NS-assigned C-number that the CSVs don't know yet — it will NOT resolve until
the warehouse export carries that C-number. That is deliberate: the C-number is
the single source of truth for attachment.
"""

import csv
import logging

import config

logger = logging.getLogger(__name__)

# NS C-number (entityid) column in the customer CSV. Matches
# validate.CUSTOMER_CNUM_COL — keep the two in sync if the column is renamed.
CUSTOMER_CNUM_COL = "C-number"


def _chunk(seq, size=200):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _sql_in(values):
    return ",".join("'" + v.replace("'", "''") + "'" for v in values)


class CustomerResolver:
    """Resolve a customer to its NS internal id by C-number (entityid) only.

    Build once per loader, call ``prefetch`` with all the C-numbers the load
    will touch (one batched SuiteQL query), then ``resolve`` locally per record.
    """

    def __init__(self, client):
        self.client = client
        # Bridges from the customer CSV — sources of a C-number, NEVER NS keys.
        self.name_to_cnum: dict[str, str] = {}  # Company Name (UPPER) → C-number
        self.ext2_to_cnum: dict[str, str] = {}  # External ID 2 → C-number
        try:
            with open(config.CUSTOMERS_CSV, encoding="utf-8-sig") as f:
                for r in csv.DictReader(f):
                    nm = (r.get("Company Name") or "").strip().upper()
                    ext = (r.get("External ID 2") or "").strip()
                    cnum = (r.get(CUSTOMER_CNUM_COL) or "").strip()
                    if nm:
                        self.name_to_cnum[nm] = cnum
                    if ext:
                        self.ext2_to_cnum[ext] = cnum
        except FileNotFoundError:
            logger.warning(
                "Customer CSV not found at %s; C-number bridges are empty "
                "(resolution will rely on the subs export's own C-numbers).",
                config.CUSTOMERS_CSV,
            )
        # NS lookup, populated by prefetch(). Each value is a record dict
        # {id, subsidiary} — resolve() returns the id; resolve_record() the dict.
        self.ns_by_cnum: dict[str, dict] = {}  # entityid → {id, subsidiary}

    # ── name bridge (alias-resolved, case-insensitive) ──────────────────────
    def name_cnum(self, raw_name: str) -> str:
        """Customer name → C-number (via the alias map + customer CSV), or ''."""
        name = (raw_name or "").strip()
        canonical = config.CUSTOMER_NAME_ALIASES.get(name.upper(), name)
        return self.name_to_cnum.get(canonical.upper(), "")

    # ── NS prefetch + resolve ────────────────────────────────────────────────
    def prefetch(self, cnums):
        """Fetch NS internal ids for the given C-numbers (chunked entityid IN).

        Safe to call more than once; unknown C-numbers simply don't land in the
        map and resolve() returns None for them.
        """
        cn = sorted({c for c in cnums if c})
        for chunk in _chunk(cn):
            q = (
                "SELECT id, entityid, subsidiary FROM customer "
                f"WHERE entityid IN ({_sql_in(chunk)})"
            )
            for row in self.client.suiteql_query(q):
                if row.get("entityid"):
                    self.ns_by_cnum[row["entityid"]] = {
                        "id": str(row.get("id")),
                        "subsidiary": row.get("subsidiary"),
                    }

    def resolve_record(self, cnum: str):
        """NS record {id, subsidiary} for a C-number, or None."""
        if cnum and cnum in self.ns_by_cnum:
            return self.ns_by_cnum[cnum]
        return None

    def resolve(self, cnum: str):
        """NS internal id for a C-number, or None."""
        rec = self.resolve_record(cnum)
        return rec["id"] if rec else None
