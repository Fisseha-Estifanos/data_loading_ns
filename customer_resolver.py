"""
Customer → NetSuite internal id resolution, by C-number (entityid) / externalId
================================================================================
Shared by the subscription and billing-account loaders so both reach a customer
the SAME way the pre-load validator does — directly from NetSuite, not via the
state tracker. Consequences:
  • Pre-existing NS customers resolve without any state-tracker seeding.
  • The customer CSV need only carry the customers you are actually creating.
  • "Validation clean" now means "the load will resolve every customer" — the
    validator and the loaders finally use one resolution path.

Two NS populations, two keys (first hit wins):
  • C-number (entityid)            — pre-existing customers. The customer CSV
    carries it in the "C-number" column; subs carry it in
    NETSUITE_ACCOUNT_NUMBER / NETSUITE_ACCOUNT_NUMBER_COMPANY_LEVEL.
  • externalId, revision then raw — customers THIS loader created under
    apply_revision(External ID 2) (e.g. MP_HubSpot_..._rvn_prod_01).

The CSV's "External ID 2" itself is a HubSpot id absent from NS, so it is never
used to resolve — only as a bridge to the C-number and (revision) as the
externalId a freshly-loaded customer lives under.
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
    """Resolve a customer to its NS internal id by C-number or externalId.

    Build once per loader, call ``prefetch`` with all the C-numbers / External
    ID 2s the load will touch (one batched SuiteQL query per key kind), then
    ``resolve`` locally per record.
    """

    def __init__(self, client):
        self.client = client
        # Bridges from the customer CSV.
        self.name_to_cnum: dict[str, str] = {}  # Company Name (UPPER) → C-number
        self.name_to_ext: dict[str, str] = {}  # Company Name (UPPER) → External ID 2
        self.ext2_to_cnum: dict[str, str] = {}  # External ID 2 → C-number
        try:
            with open(config.CUSTOMERS_CSV, encoding="utf-8-sig") as f:
                for r in csv.DictReader(f):
                    nm = (r.get("Company Name") or "").strip().upper()
                    ext = (r.get("External ID 2") or "").strip()
                    cnum = (r.get(CUSTOMER_CNUM_COL) or "").strip()
                    if nm and ext:
                        self.name_to_ext[nm] = ext
                    if nm:
                        self.name_to_cnum[nm] = cnum
                    if ext:
                        self.ext2_to_cnum[ext] = cnum
        except FileNotFoundError:
            logger.warning(
                "Customer CSV not found at %s; name/externalId bridges are empty "
                "(resolution will rely on subs C-numbers only).",
                config.CUSTOMERS_CSV,
            )
        # NS lookups, populated by prefetch(). Each value is a record dict
        # {id, subsidiary} — resolve() returns the id; resolve_record() the dict.
        self.ns_by_cnum: dict[str, dict] = {}  # entityid → {id, subsidiary}
        self.ns_by_ext: dict[str, dict] = {}  # externalid → {id, subsidiary}

    # ── name bridges (alias-resolved, case-insensitive) ─────────────────────
    def name_cnum(self, raw_name: str) -> str:
        """Customer name → C-number (via the alias map + customer CSV), or ''."""
        name = (raw_name or "").strip()
        canonical = config.CUSTOMER_NAME_ALIASES.get(name.upper(), name)
        return self.name_to_cnum.get(canonical.upper(), "")

    def name_ext(self, raw_name: str):
        """Customer name → External ID 2 (alias map + customer CSV), or None."""
        name = (raw_name or "").strip()
        canonical = config.CUSTOMER_NAME_ALIASES.get(name.upper(), name)
        return self.name_to_ext.get(canonical.upper())

    # ── NS prefetch + resolve ────────────────────────────────────────────────
    def prefetch(self, cnums, ext2s):
        """Fetch NS internal ids for the given C-numbers and External ID 2s.

        ``ext2s`` are RAW External ID 2 values; both the raw form and
        ``apply_revision(ext2)`` are queried, so customers loaded this run (under
        the revisioned externalId) and any pre-existing raw-keyed ones both land.
        Chunked ``WHERE ... IN (...)``. Safe to call more than once.
        """
        cn = sorted({c for c in cnums if c})
        for chunk in _chunk(cn):
            q = f"SELECT id, entityid, subsidiary FROM customer WHERE entityid IN ({_sql_in(chunk)})"
            for row in self.client.suiteql_query(q):
                if row.get("entityid"):
                    self.ns_by_cnum[row["entityid"]] = {
                        "id": str(row.get("id")),
                        "subsidiary": row.get("subsidiary"),
                    }

        ex = set()
        for e in ext2s:
            if e:
                ex.add(e)
                ex.add(config.apply_revision(e))
        for chunk in _chunk(sorted(ex)):
            q = f"SELECT id, externalid, subsidiary FROM customer WHERE externalid IN ({_sql_in(chunk)})"
            for row in self.client.suiteql_query(q):
                if row.get("externalid"):
                    self.ns_by_ext[row["externalid"]] = {
                        "id": str(row.get("id")),
                        "subsidiary": row.get("subsidiary"),
                    }

    def resolve_record(self, cnum: str, ext2):
        """NS record {id, subsidiary} by C-number, else (revisioned/raw) External
        ID 2, else None."""
        if cnum and cnum in self.ns_by_cnum:
            return self.ns_by_cnum[cnum]
        if ext2:
            for cand in (config.apply_revision(ext2), ext2):
                if cand in self.ns_by_ext:
                    return self.ns_by_ext[cand]
        return None

    def resolve(self, cnum: str, ext2):
        """NS internal id by C-number, else (revisioned/raw) External ID 2, else None."""
        rec = self.resolve_record(cnum, ext2)
        return rec["id"] if rec else None
