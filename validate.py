"""
Pre-Load Validator (WS1)
========================
Read-only. Runs BEFORE any POST and reports *every* data problem at once —
name mismatches, key mismatches, subsidiary conflicts, bad dates, unmapped
items — instead of discovering them one failed POST at a time.

Design notes (see refactors/REFACTOR-PLAN.md):
  - Standalone: uses its own light read-only SuiteQL queries (no NSIndex yet,
    no state DB). NetSuite is the single source of truth, queried by externalId.
  - LOAD_REVISION is frozen at _rvn_prod_01. We reconcile by *exact* externalId
    sets derived from the CSVs (chunked `WHERE externalid IN (...)`), not LIKE.
  - CUSTOMERS resolve by C-number (NS `entityid`) ONLY — convention since
    2026-07-09, mirroring customer_resolver.CustomerResolver. No externalId or
    company-name resolution against NS; names/External ID 2 are only bridges to
    a C-number. A customer created by this loader resolves only once the
    warehouse export carries its NS-assigned C-number (check 4 surfaces those).

Severity:
  BLOCKER  — would fail or silently skip the load. Exit code 1 if any present.
  WARNING  — informational; load proceeds.

Usage:
  python validate.py                      # validate the whole pipeline
  python validate.py --entity subscription
  (or via main.py:  python main.py --validate [--entity ...])
"""

import os
import re
import csv
import sys
import argparse
from collections import defaultdict
from dataclasses import dataclass

import config
from netsuite_client import NetSuiteClient

# Reuse the loaders' canonical maps so the validator never drifts from them.
from loaders.subscription import SUBSIDIARY_MAP as SUB_SUBSIDIARY_MAP

BLOCKER = "BLOCKER"
WARNING = "WARNING"

# The customer CSV carries the NetSuite C-number (entityid) in this oddly-named
# column. The CSV's "External ID 2" holds a HubSpot id (MP_HubSpot_*) that does
# NOT exist in NetSuite — confirmed by GET: 0/14 found by externalId, 10/10
# found by entityid. So the C-number is the authoritative key to resolve a
# customer to its NS record, used everywhere in this validator.
CUSTOMER_CNUM_COL = "C-number"

# Each check → which entity load it guards. Used to filter when --entity given.
CHECK_ENTITY = {
    1: "subscription",
    2: "subscription",
    3: "subscription",
    4: "subscription",
    5: "billingAccount",
    5.1: "billingAccount",
    6: "subscription",
    7: "subscription",
    8: "billingAccount",
    9: "subscription",
    9.1: "subscription",
    10: "subscription",
    11: "subscription",
    12: "billingAccount",
    13: "billingAccount",
    14: "billingAccount",
}

# Severity each check emits. Used so a "not evaluated" finding (missing input)
# carries the check's native severity — you can't confirm a BLOCKER check is
# safe if its input is absent, so that absence must itself block.
BLOCKER_CHECKS = {1, 2, 5, 5.1, 6, 7, 8, 9, 9.1, 12, 13}

# CSV inputs each check requires to produce a meaningful result. If any are
# missing (None from _read_csv — a bad path in config), run() skips the check
# with ONE "(input) not evaluated" finding instead of running it against empty
# data. Two failure modes this prevents, both seen/possible in this validator:
#   • false-finding STORM — a check comparing present records against an empty
#     reference set flags every record (check 11 did this for a missing pricing
#     CSV; check 1 would for a missing customer CSV).
#   • false CLEAN — a check that only ITERATES a missing CSV emits nothing and
#     the report prints "✓ clean", hiding that it never ran (checks 13/14 vs a
#     missing BA CSV).
# A CSV is listed whether the check iterates it (subject) or reads it to resolve
# references — either way an absent file makes the result meaningless.
CHECK_REQUIRED_CSVS = {
    1: {"subscription", "customer"},
    2: {"subscription"},
    3: {"subscription", "customer"},
    4: {"subscription", "customer"},
    5: {"billingAccount"},
    5.1: {"billingAccount"},
    6: {"subscription"},
    7: {"subscription", "customer"},
    8: {"subscription", "billingAccount"},
    9: {"subscription"},
    9.1: {"subscription"},
    10: {"subscription"},
    11: {"subscription", "pricing"},
    12: {"billingAccount"},
    13: {"billingAccount"},
    14: {"billingAccount"},
}

# Major group each check belongs to, in dispatch (sorted) order. The report
# prints a "++++" divider with the group name the first time a check from a new
# group appears, so the output can be skimmed by eye.
CHECK_GROUP = {
    1: "CUSTOMERS — subscription resolves to an NS customer",
    2: "CUSTOMERS — subscription resolves to an NS customer",
    3: "CUSTOMERS — subscription resolves to an NS customer",
    4: "CUSTOMERS — subscription resolves to an NS customer",
    5: "BILLING ACCOUNTS — customer link",
    5.1: "BILLING ACCOUNTS — customer link",
    6: "BILLING ACCOUNTS — every deal has one (load vs create)",
    7: "SUBSCRIPTIONS — subsidiary match",
    8: "BILLING ACCOUNTS — start dates",
    9: "SUBSCRIPTIONS — sales items",
    9.1: "PRICE PLANS — every included line is priced (mandatory)",
    10: "PRICE PLANS — coverage (NS + pricing CSV)",
    11: "PRICE PLANS — coverage (NS + pricing CSV)",
    12: "BILLING ACCOUNTS — addresses & required fields",
    13: "BILLING ACCOUNTS — addresses & required fields",
    14: "BILLING ACCOUNTS — addresses & required fields",
}

CHECK_TITLE = {
    1: "Sub customer resolves to the CORRECT NS customer (by C-number, name-checked)",
    2: "Subs export's own C-number is valid in NS",
    3: "Sub customer already in NS (skip load) vs must be created (load)",
    4: "Flagged customer: truly absent from NS vs in NS under a different id",
    5: "BA customer resolves to an NS customer (by C-number ONLY)",
    5.1: "BA subsidiary_id == its customer's NS subsidiary",
    6: "Every sub has its own Billing Account (<deal>_<subid>_BA), loaded or in NS",
    7: "Sub subsidiary == its customer's NS subsidiary",
    8: "BA startDate present and <= earliest sub Start Date for the deal",
    9: "Active sub line Sales Item is mapped (not blank / not NOT MAPPED)",
    9.1: "Every included sub line carries a Price Plan External ID",
    10: "Sub line Price Plan External ID exists in NS",
    11: "Sub line Price Plan External ID exists in the pricing CSV",
    12: "BA customer has default billing AND shipping address in NS",
    13: "BA required fields present (subsidiary_id, currency_id)",
    14: "externalId shape sanity (BA = <deal_id>_BA)",
}

# One- to two-line plain-language explainer per check, printed under its heading
# in the report so the output reads clearly without cross-referencing the code.
CHECK_EXPLAIN = {
    1: "Can each sub's customer be resolved to a real NS customer by C-number "
    "(subs C-number, else customers-CSV)? C-number is the ONLY key the loaders "
    "attach by. Clean = already in NS. Also WARNS if the resolved NS customer's "
    "NAME doesn't match — i.e. the C-number points at the wrong company.",
    2: "Validates the C-number the subs export ITSELF carries "
    "(NETSUITE_ACCOUNT_NUMBER*) actually exists in NS — catches a wrong/stale "
    "account number even when check 1 resolved the customer another way. Silent "
    "when the subs carry no C-number (check 1/3 own that).",
    3: "Per distinct customer: already in NS (skip — don't load it) or NOT in "
    "NS (must be created/loaded first). Warns only on the ones to create; clean "
    "= every sub customer already exists in NS.",
    4: "For customers check 3 couldn't resolve by C-number: is one in NS by "
    "company name anyway? Separates 'truly absent — create it' from 'in NS but "
    "the export lacks its C-number — carry it in the export, don't duplicate'. "
    "Informational only — names are never used to attach.",
    5: "Does each billing account's customer resolve by C-number? Sourced from "
    "its OWNING SUB's NETSUITE_ACCOUNT_NUMBER* (via the `<sub>_BA` key), else "
    "the customer CSV's C-number bridged from customer_externalId. C-number is "
    "the ONLY NS key — no externalId/name resolution.",
    5.1: "Does each BA's CSV subsidiary_id match the NS subsidiary of the "
    "customer it will bind to? A mismatch is a hard NS 400 at load ('Invalid "
    "Field Value ... subsidiary' — the B&M BA failure of 2026-07-09).",
    6: "Per SUB: is there a `<sub External ID>_BA` (the `<deal>_<subid>_BA` "
    "convention) — queued in the billing CSV or already in NS? BLOCKS for subs "
    "with none: a subscription is NEVER loaded without its BA (the loader "
    "refuses; NS would otherwise attach the customer's default BA). A "
    "customer's OTHER billing accounts — even a sibling sub's BA — don't "
    "count; the sub links by its own `<sub>_BA`.",
    7: "Does each sub's Subsidiary match its NS customer's subsidiary? A mismatch "
    "is a hard NS 400 at load time.",
    8: "Is each BA's startDate present and on/before its earliest subscription? "
    "A blank/late start breaks the subs it bills.",
    9: "Does every included sub line map to a real Sales Item (not blank / not "
    "'NOT MAPPED')? Unmapped lines are silently skipped at load.",
    9.1: "Does every included line carry a Price Plan External ID (per item, "
    "after the loader's same-item backfill)? The plan record holds ALL the "
    "pricing — currency, min/max, price tiers — so a line with no plan bills "
    "at the £0 book-default placeholder. BLOCKS: the loader refuses to load a "
    "sub with unpriced lines (mirror of the mandatory-BA gate).",
    10: "Does each line's price plan already exist in NS? Warn = not loaded yet; "
    "the line would fall back to the price-book default.",
    11: "Does each line's price plan exist in the pricing CSV (i.e. CAN be "
    "created)? Warn = no source row to push; line can only use the book "
    "default.",
    12: "Does each BA's customer have BOTH a default billing and shipping address "
    "in NS? If not, the BA loader can't resolve them and skips the BA.",
    13: "Does each BA have its mandatory NS reference fields (subsidiary_id, "
    "currency_id) filled in the CSV?",
    14: "Does each BA externalId follow the '<deal>_<subid>_BA' convention "
    "(ends in '_BA', prefix = its sub's External ID)? Drift here quietly "
    "misaligns BAs and subs.",
}


@dataclass
class Finding:
    """One problem reported by a single check.

    Attributes:
        check:    the check number that produced it (e.g. 1, 5, 14). Used to
                  group findings under their check heading in the report.
        severity: BLOCKER or WARNING. Any BLOCKER makes the run exit non-zero.
        ident:    the offending record's identifier — a subscription/deal
                  `External ID`, a BA `externalId`, or a bare deal id, depending
                  on the check. Lets the operator jump straight to the row.
        message:  human-readable description of what's wrong and its consequence.
    """

    check: int
    severity: str
    ident: str
    message: str


def _env_label() -> str:
    """Human label for the currently-active NS account (from NS_REALM)."""
    realm = os.environ.get("NS_REALM", "")
    return "sandbox" if "sb" in realm.lower() else "prod"


def _read_csv(path):
    """Read a CSV into a list of dict rows, using the loaders' encoding.

    Uses utf-8-sig so a leading BOM (Snowflake/Excel exports often have one) is
    stripped from the first column name. Returns the row list, or ``None`` if the
    file is missing — the caller uses ``None`` to mean "skip this entity's
    checks" and prints a warning, rather than crashing the way the old preflight
    script did on a missing CSV. An existing-but-empty file returns ``[]``.
    """
    try:
        with open(path, encoding="utf-8-sig") as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        return None  # caller distinguishes "missing file" from "empty file"


def _deal_id(external_id: str) -> str:
    """Reduce any deal-scoped external id to its bare deal id.

    The deal id is everything before the first underscore. This is the same rule
    the subscription loader uses to derive the shared billing-account key, so a
    sub `External ID` (`494812626113_a_27397`), a BA `externalId`
    (`494812626113_BA`), and the deal itself all collapse to `494812626113`.
    Used by the BA/subscription checks to line subscriptions and BAs up by deal.
    """
    return external_id.split("_", 1)[0]


def _chunk(seq, size=200):
    """Yield successive ``size``-length slices of ``seq``.

    Keeps each SuiteQL ``WHERE ... IN (...)`` list short enough to stay under
    NetSuite's query-length limits when resolving many external ids at once.
    """
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _sql_in(values):
    """Render ``values`` as a SQL ``IN (...)`` body with each item quoted.

    Single quotes inside a value are escaped by doubling (`'` → `''`) so company
    names / external ids containing apostrophes (e.g. ``AJ'S TRAINING LTD``)
    can't break the query or be used for injection. Returns the comma-joined
    quoted list WITHOUT the surrounding parentheses.
    """
    return ",".join("'" + v.replace("'", "''") + "'" for v in values)


class Validator:
    """Runs the read-only pre-load checks and accumulates Findings.

    Lifecycle:
        1. ``__init__`` loads the three CSVs (customer / BA / subscription) and
           builds the in-memory indexes the checks read from (the company-name →
           External ID 2 map and the subscription groups). It does NOT touch
           NetSuite.
        2. ``run(entities)`` prefetches only the NS data the requested checks
           need (one batched SuiteQL query per kind), then dispatches each check.
        3. ``report(entities)`` prints the grouped report and returns the blocker
           count.

    All NS access is read-only SuiteQL. State lives on the instance: the CSV
    rows, the lookup maps, and ``self.findings`` (the result list). Construct a
    fresh Validator per run; it is not designed to be re-run.
    """

    # A pricing CSV with ≥ this many times more plans than the subs reference is
    # almost certainly the unscoped full-book dump, not a per-batch export. Kept
    # in sync with check_pricing_coverage.UNSCOPED_RATIO.
    UNSCOPED_RATIO = 10

    def __init__(self, client=None):
        """Load the CSVs and build the lookup indexes (no NetSuite calls).

        Args:
            client: an optional NetSuiteClient to reuse (e.g. the one main.py
                already built). If omitted, a new client is created. The client
                is only USED later, in ``run()``; constructing the Validator
                issues no queries.

        Side effects: prints a one-line warning for any CSV that is missing
        (its checks are then silently skipped rather than crashing).
        """
        self.client = client or NetSuiteClient()
        self.findings: list[Finding] = []

        # --- Load CSVs --------------------------------------------------
        self.customers = _read_csv(config.CUSTOMERS_CSV)
        self.bas = _read_csv(config.BILLING_CSV)
        self.subs_rows = _read_csv(config.SUBSCRIPTIONS_CSV)

        # Which inputs actually loaded. A CSV that _read_csv returned None for is
        # MISSING (bad path in config) — distinct from present-but-empty ([]).
        # The dispatch loop in run() consults this so a check whose required CSV
        # is missing is reported as "not evaluated" rather than run against empty
        # data (which would either falsely flag every record — check 11's old
        # bug — or silently report a misleading "clean"). "pricing" is filled in
        # run()'s prefetch, since it's only read when check 11 is in scope.
        self.csv_loaded = {
            "customer": self.customers is not None,
            "billingAccount": self.bas is not None,
            "subscription": self.subs_rows is not None,
            "pricing": False,
        }
        for label, path, rows in [
            ("customer", config.CUSTOMERS_CSV, self.customers),
            ("billingAccount", config.BILLING_CSV, self.bas),
            ("subscription", config.SUBSCRIPTIONS_CSV, self.subs_rows),
        ]:
            if rows is None:
                # Guard against the preflight's hard crash on a missing CSV.
                print(f"  ⚠ {label} CSV not found, skipping its checks: {path}")

        # Customer-CSV bridges. The C-number (CUSTOMER_CNUM_COL) is the ONLY key
        # that resolves to NS (convention 2026-07-09); these maps exist purely to
        # *reach* a C-number from a name (subs) or from an External ID 2 /
        # MP_HubSpot id (billing accounts) — never to query NS themselves.
        #   name_to_cnum  : Company Name (UPPER) → C-number
        #   ext2_to_cnum  : External ID 2        → C-number
        #   ext2_to_name  : External ID 2        → Company Name (readable findings)
        # Both cnum maps keep rows whose C-number is blank (value ""), so a check
        # can tell "no customer-CSV row" (key absent) apart from "row exists but
        # has no C-number".
        self.name_to_cnum = {}
        self.ext2_to_cnum = {}
        self.ext2_to_name = {}
        for r in self.customers or []:
            raw_nm = (r.get("Company Name") or "").strip()
            nm = raw_nm.upper()
            ext = (r.get("External ID 2") or "").strip()
            cnum = (r.get(CUSTOMER_CNUM_COL) or "").strip()
            if nm:
                self.name_to_cnum[nm] = cnum
            if ext:
                self.ext2_to_cnum[ext] = cnum
                self.ext2_to_name[ext] = raw_nm

        # Group subs by deal external id (matches subscription loader grouping)
        self.sub_groups = defaultdict(list)
        for r in self.subs_rows or []:
            ext = (r.get("External ID") or "").strip()
            if ext:
                self.sub_groups[ext].append(r)

        # --- NS lookups (populated lazily in run()) ---------------------
        # Customers resolve by C-number (entityid) ONLY — convention since
        # 2026-07-09, matching customer_resolver.CustomerResolver. ns_by_cnum
        # holds {id, subsidiary, externalid, companyname, entityid} per
        # entityid. NOTE the consequence: a customer THIS loader just created
        # has an NS-assigned C-number the CSVs don't carry yet, so it will not
        # resolve until the warehouse export includes it — check 4's by-name
        # lookup surfaces those as "in NS — reconcile the C-number".
        self.ns_by_cnum: dict[str, dict] = {}
        # UPPER(companyname) → record. Last-resort lookup for check 4 only:
        # tells "truly absent from NS" apart from "in NS but the id bridge failed".
        self.ns_by_name: dict[str, dict] = {}
        # Revisioned `<deal>_BA` externalIds that already exist in NS — asked
        # directly of NS (check 6). Per-deal, because a sub links to its BA by
        # `<deal>_BA` externalId, not by "the customer has some BA".
        self.ns_ba_extids: set[str] = set()
        self.ns_priceplans: set[str] = set()  # revisioned extIds that exist
        self.pricing_csv_extids: set[str] = set()  # RAW extIds in the pricing CSV
        self.addr_billing: set[str] = set()  # customer internal ids w/ default bill
        self.addr_shipping: set[str] = set()  # customer internal ids w/ default ship

    # ── helpers ────────────────────────────────────────────────────────
    def add(self, check, severity, ident, message):
        """Record one problem. Thin wrapper that appends a Finding to
        ``self.findings``; every check calls this instead of building Findings
        inline, so the result list stays the single source of truth for the
        report and the exit code."""
        self.findings.append(Finding(check, severity, ident, message))

    def name_cnum(self, raw_name: str) -> str:
        """sub/BA Customer name → canonical (alias map) → C-number, or ''.

        Returns '' when the name matches no customer-CSV row, or matches a row
        whose C-number column is blank — callers that need to tell those apart
        test ``name in self.name_to_cnum`` (absent = no row) separately.
        """
        name = (raw_name or "").strip()
        canonical = config.CUSTOMER_NAME_ALIASES.get(name.upper(), name)
        return self.name_to_cnum.get(canonical.upper(), "")

    def sub_cnum(self, grp) -> str:
        """C-number for a subscription group, best source first.

        Prefer the deal-level C-number carried in the subs export
        (NETSUITE_ACCOUNT_NUMBER, then NETSUITE_ACCOUNT_NUMBER_COMPANY_LEVEL);
        fall back to the customer CSV's C-number resolved by Customer name. ''
        if none of those yields a C-number.
        """
        return (
            self._group_value(grp, "NETSUITE_ACCOUNT_NUMBER")
            or self._group_value(grp, "NETSUITE_ACCOUNT_NUMBER_COMPANY_LEVEL")
            or self.name_cnum(grp[0].get("Customer"))
        )

    @staticmethod
    def _group_value(grp, col: str) -> str:
        """First non-empty value of `col` across a sub group's rows."""
        return next(
            ((r.get(col) or "").strip() for r in grp if (r.get(col) or "").strip()),
            "",
        )

    # ── NS prefetch ─────────────────────────────────────────────────────
    def _fetch_ns_by_cnum(self, cnums):
        """Resolve customers in NS by their C-number (``entityid``) and cache them.

        Populates ``self.ns_by_cnum`` ({entityid → {id, subsidiary, externalid,
        companyname}}) — the customer map the customer-touching checks read
        from. Queried in chunks of ≤200 via ``WHERE
        entityid IN (...)``. The C-number is env-agnostic and is what actually
        exists in NS (the customer CSV's External ID 2 holds a HubSpot id that
        does not — see CUSTOMER_CNUM_COL). Only C-numbers that exist in the
        active env become keys; a missing one simply won't be present.
        """
        ids = sorted({c for c in cnums if c})
        for chunk in _chunk(ids):
            q = (
                "SELECT id, entityid, subsidiary, externalid, companyname "
                f"FROM customer WHERE entityid IN ({_sql_in(chunk)})"
            )
            for row in self.client.suiteql_query(q):
                self.ns_by_cnum[row["entityid"]] = {
                    "id": row.get("id"),
                    "subsidiary": row.get("subsidiary"),
                    "externalid": row.get("externalid"),
                    "companyname": row.get("companyname"),
                    "entityid": row.get("entityid"),
                }

    def _fetch_ns_by_name(self, names):
        """Resolve customers in NS by exact (case-insensitive) company name.

        Populates ``self.ns_by_name`` ({UPPER(companyname) → {id, entityid,
        externalid, companyname}}) for check 4's last-resort lookup. ``names``
        are already alias-resolved and uppercased by the caller. EXACT upper
        match (``WHERE UPPER(companyname) IN (...)``) — deliberately no fuzzy
        matching, so a hit is trustworthy; a miss means "not found by exact
        name" (NS may still hold it under a different spelling).
        """
        ids = sorted({n for n in names if n})
        for chunk in _chunk(ids):
            q = (
                "SELECT id, entityid, externalid, companyname FROM customer "
                f"WHERE UPPER(companyname) IN ({_sql_in(chunk)})"
            )
            for row in self.client.suiteql_query(q):
                key = (row.get("companyname") or "").strip().upper()
                if key:
                    self.ns_by_name[key] = {
                        "id": row.get("id"),
                        "entityid": row.get("entityid"),
                        "externalid": row.get("externalid"),
                        "companyname": row.get("companyname"),
                    }

    def _fetch_ns_ba_extids(self, ba_ext_ids):
        """Record which ``<deal>_BA`` billing accounts already exist in NS.

        ``ba_ext_ids`` are the REVISIONED ``<deal>_BA`` externalIds. Fills
        ``self.ns_ba_extids`` with the subset present, via chunked ``WHERE
        externalid IN (...)``. Check 6 reasons PER DEAL — a subscription links to
        its BA by ``<deal>_BA`` externalId, so the right question is "does THIS
        deal's BA exist (loaded already, or queued in the billing CSV)?", not
        "does the customer have some other BA?".
        """
        ids = sorted({e for e in ba_ext_ids if e})
        for chunk in _chunk(ids):
            q = (
                "SELECT externalid FROM billingaccount "
                f"WHERE externalid IN ({_sql_in(chunk)})"
            )
            for row in self.client.suiteql_query(q):
                if row.get("externalid"):
                    self.ns_ba_extids.add(row["externalid"])

    def customer_ns_record(self, cnum: str):
        """The NS customer record for a C-number, or None.

        C-number (entityid) is the ONLY resolution key — convention since
        2026-07-09; matches ``customer_resolver.CustomerResolver`` exactly. A
        customer counts as "in NS" iff this returns non-None. NOTE: a customer
        this loader just CREATED gets an NS-assigned C-number the CSVs don't
        carry yet, so it will NOT resolve until the warehouse export includes
        that C-number (check 4 surfaces those as "in NS by name — reconcile").
        """
        if cnum and cnum in self.ns_by_cnum:
            return self.ns_by_cnum[cnum]
        return None

    def ba_cnum(self, ba_row: dict) -> str:
        """C-number for a BA row: its sub's C-number, else the customer-CSV bridge.

        Mirrors ``BillingAccountLoader._ba_cnum``: `<sub External ID>_BA` →
        sub group in the subs CSV → its C-number (``sub_cnum``); falling back to
        ``customer_externalId`` → customer CSV row → its C-number. Returns ''
        when neither source yields one.
        """
        ba_ext = (ba_row.get("externalId") or "").strip()
        if ba_ext.endswith("_BA"):
            grp = self.sub_groups.get(ba_ext[: -len("_BA")])
            if grp:
                cnum = self.sub_cnum(grp)
                if cnum:
                    return cnum
        cust_ext = (ba_row.get("customer_externalId") or "").strip()
        return self.ext2_to_cnum.get(cust_ext, "") or ""

    def _fetch_ns_priceplans(self, rev_ext_ids):
        """Cache which price plans already exist in NS, for check 7.

        ``rev_ext_ids`` are the REVISIONED price-plan external ids (the raw subs
        CSV value already passed through ``config.apply_revision``). Populates
        ``self.ns_priceplans`` with the subset that actually exist, via chunked
        ``WHERE externalid IN (...)``. Membership is all check 7 needs, so this
        stores a set of external ids rather than a map to internal ids.
        """
        ids = sorted({e for e in rev_ext_ids if e})
        for chunk in _chunk(ids):
            q = (
                "SELECT externalid FROM pricePlan "
                f"WHERE externalid IN ({_sql_in(chunk)})"
            )
            for row in self.client.suiteql_query(q):
                self.ns_priceplans.add(row["externalid"])

    def _fetch_ns_addresses(self, customer_internal_ids):
        """Record which customers have a default billing / shipping address.

        For the given customer INTERNAL ids, queries ``customeraddressbook`` and
        fills two sets — ``self.addr_billing`` and ``self.addr_shipping`` — with
        the customer ids flagged ``defaultbilling='T'`` / ``defaultshipping='T'``
        respectively. Check 8 then asks "is this customer in both sets?". A
        customer with neither default address won't appear in either set.
        """
        ids = sorted({i for i in customer_internal_ids if i})
        for chunk in _chunk(ids):
            q = (
                "SELECT entity, defaultbilling, defaultshipping "
                "FROM customeraddressbook "
                f"WHERE entity IN ({_sql_in(chunk)})"
            )
            for row in self.client.suiteql_query(q):
                ent = row.get("entity")
                if (row.get("defaultbilling") or "").upper() == "T":
                    self.addr_billing.add(ent)
                if (row.get("defaultshipping") or "").upper() == "T":
                    self.addr_shipping.add(ent)

    @staticmethod
    def _names_match(sub_name: str, ns_name: str) -> bool:
        """Loose company-name equality for the wrong-customer guard.

        Lower-cases, drops common company suffixes (Ltd/Limited/LLP/PLC/…) and all
        non-alphanumerics, then checks either normalised name contains the other.
        Tolerant enough to pass legitimate variations ("Link Up Mitaka Ltd" vs
        "Link Up Mitaka Limited") while still catching a C-number that resolved to
        an unrelated company ("Care Shield Services Limited" vs "AMA Capital LLP").
        Returns True when it can't meaningfully compare (either side blank).
        """

        def norm(s: str) -> str:
            s = (s or "").lower()
            s = re.sub(
                r"\b(limited|ltd|llp|plc|inc|co|company|group|services?)\b", " ", s
            )
            return re.sub(r"[^a-z0-9]", "", s)

        a, b = norm(sub_name), norm(ns_name)
        if not a or not b:
            return True
        return a in b or b in a

    # ── checks ───────────────────────────────────────────────────────────
    def check_1_sub_customer(self):
        """Check 1: every subscription's customer resolves to the CORRECT NS customer.

        Resolution per sub group via ``customer_ns_record``: by C-number ONLY
        (``sub_cnum``: subs NETSUITE_ACCOUNT_NUMBER / _COMPANY_LEVEL, else the
        customer CSV's C-number by name). No externalId or name resolution —
        this mirrors the loaders exactly (convention 2026-07-09).

        Severities:
          • resolved AND the NS customer's name matches → clean.
          • resolved but the NS name does NOT match the sub's Customer → WARNING:
            the C-number points at the WRONG customer (e.g. a C-number that
            belongs to a different company in this env). Catches the wrong-
            binding the load would otherwise make silently.
          • a concrete C-number was supplied but it isn't in NS → BLOCKER (an id
            that should exist but doesn't — wrong/stale export).
          • no C-number at all → BLOCKER: with C-number-only attach, the sub
            CANNOT resolve its customer; the loader will skip it. Fix the export
            (or create the customer and re-export with its NS C-number).
        """
        for ext, grp in self.sub_groups.items():
            name = (grp[0].get("Customer") or "").strip()
            cnum = self.sub_cnum(grp)
            rec = self.customer_ns_record(cnum)
            if rec:
                ns_name = rec.get("companyname") or ""
                if ns_name and not self._names_match(name, ns_name):
                    self.add(
                        1,
                        WARNING,
                        ext,
                        f"customer {name!r} resolved to NS customer {ns_name!r} "
                        f"(id {rec.get('id')}, C-number {cnum}) — names DON'T "
                        f"match; the C-number likely points at the WRONG customer. "
                        f"Verify before loading (the sub would bind to the wrong "
                        f"account).",
                    )
                continue  # in NS by C-number
            if cnum:
                self.add(
                    1,
                    BLOCKER,
                    ext,
                    f"customer {name!r}: C-number {cnum} not found in NS — "
                    f"wrong/stale C-number; the sub cannot attach.",
                )
            else:
                has_row = (
                    config.CUSTOMER_NAME_ALIASES.get(name.upper(), name).upper()
                    in self.name_to_cnum
                )
                why = (
                    f"customer-CSV {CUSTOMER_CNUM_COL!r} is blank"
                    if has_row
                    else "no customer-CSV row"
                )
                self.add(
                    1,
                    BLOCKER,
                    ext,
                    f"customer {name!r}: no C-number anywhere (subs export blank; "
                    f"{why}). Customers attach by C-number ONLY — the loader "
                    f"would skip this sub. Fix the export.",
                )

    def check_2_subs_cnumber_valid(self):
        """Check 2 (BLOCKER): the C-number the SUBS EXPORT itself carries is real.

        The subs CSV carries two C-numbers: NETSUITE_ACCOUNT_NUMBER (deal/customer
        level) and NETSUITE_ACCOUNT_NUMBER_COMPANY_LEVEL (company/parent level).
        Unlike check 1, this does NOT fall back to the customer CSV — so it
        catches a WRONG/STALE account number in the subs export even when check 1
        resolved the customer another way (e.g. via the loaded externalId).

        Blocks only when the subs carry a C-number AND none of the present ones
        resolve in NS. Both-blank is SILENT here — that "no C-number" case is
        owned by checks 1 and 3, so we don't double-warn."""
        env = _env_label()
        for ext, grp in self.sub_groups.items():
            name = (grp[0].get("Customer") or "").strip()
            acct = self._group_value(grp, "NETSUITE_ACCOUNT_NUMBER")
            acct_co = self._group_value(grp, "NETSUITE_ACCOUNT_NUMBER_COMPANY_LEVEL")
            if acct and acct in self.ns_by_cnum:
                continue  # found at customer level — suffices
            if acct_co and acct_co in self.ns_by_cnum:
                continue  # found at company level
            tried = [v for v in (acct, acct_co) if v]
            if tried:  # a C-number is present but none resolved → wrong/stale id
                self.add(
                    2,
                    BLOCKER,
                    ext,
                    f"customer {name!r}: subs C-number(s) {', '.join(tried)} not "
                    f"found in {env} NS by entityid — wrong/stale account number "
                    f"in the subs export.",
                )

    def check_3_customer_load_status(self):
        """Check 3 (WARNING): which sub customers must be CREATED vs already exist.

        The explicit load-vs-skip view, deduplicated per customer (check 1 is
        per deal and frames this as resolution success/failure). For each
        distinct sub Customer, resolve via ``customer_ns_record`` (by C-number
        ONLY) and ask: is it in NS?
          • IN NS  → the customer already exists; skip loading it (no finding).
          • NOT resolvable → warns once, naming the customer: create/load it,
            then RE-EXPORT so the subs carry its NS-assigned C-number —
            with C-number-only attach, a freshly-created customer resolves only
            once the export knows its C-number.
        A customer counts as IN NS if ANY of its deals resolves. WARNING-only by
        design — it's a "here's your create list", not a gate (check 1 gates).
        Clean = every sub customer already exists in NS, nothing to create.
        """
        status: dict[str, bool] = {}  # customer name → in NS (any deal resolves)
        for ext, grp in self.sub_groups.items():
            name = (grp[0].get("Customer") or "").strip()
            if not name:
                continue
            resolved = self.customer_ns_record(self.sub_cnum(grp)) is not None
            status[name] = status.get(name, False) or resolved
        for name, in_ns in sorted(status.items()):
            if not in_ns:
                self.add(
                    3,
                    WARNING,
                    name,
                    f"customer {name!r}: not resolvable by C-number — create/load "
                    f"it, then re-export so the subs carry its NS C-number "
                    f"(otherwise its sub is skipped at load). Customers not "
                    f"listed here already exist in NS (skip load).",
                )

    def check_4_customer_name_in_ns(self):
        """Check 4 (WARNING): for customers UNRESOLVED by id, is one in NS by name?

        The companion to check 3 that tells two very different situations apart,
        for each customer that could NOT be resolved by C-number:
          • a customer with that company name EXISTS in NS → it IS in NS; the
            export just doesn't carry its C-number (typical for customers this
            loader created — NS assigned their C-number after the export). Do
            NOT create a duplicate — put the reported entityid into the export.
          • no NS customer with that exact name → genuinely absent; create/load it.
        Only customers check 3 would flag are considered (C-number-resolved ones
        are skipped). EXACT (case-insensitive) name match — see _fetch_ns_by_name.
        The by-name lookup is INFORMATIONAL only; it is never used to attach.
        """
        resolved_by_id: dict[str, bool] = {}
        for ext, grp in self.sub_groups.items():
            name = (grp[0].get("Customer") or "").strip()
            if not name:
                continue
            rec = self.customer_ns_record(self.sub_cnum(grp))
            resolved_by_id[name] = resolved_by_id.get(name, False) or (rec is not None)
        for name, by_id in sorted(resolved_by_id.items()):
            if by_id:
                continue  # in NS by C-number — fine (check 1 / 3 cover it)
            alias = config.CUSTOMER_NAME_ALIASES.get(name.upper(), name).upper()
            hit = self.ns_by_name.get(alias)
            if hit:
                self.add(
                    4,
                    WARNING,
                    name,
                    f"customer {name!r}: no C-number resolves, but a customer "
                    f"named {hit.get('companyname')!r} EXISTS in NS "
                    f"(id {hit.get('id')}, entityid {hit.get('entityid')}, "
                    f"externalid {hit.get('externalid')!r}). It IS in NS — carry "
                    f"C-number {hit.get('entityid')} in the export; do NOT create "
                    f"a duplicate.",
                )
            else:
                self.add(
                    4,
                    WARNING,
                    name,
                    f"customer {name!r}: not in NS by C-number OR exact company "
                    f"name — genuinely absent. Create/load it, then re-export with "
                    f"its NS C-number (if you expected it to exist, check the NS "
                    f"name spelling).",
                )

    def check_5_ba_customer(self):
        """Check 5 (BLOCKER): every BA's customer resolves by C-number.

        Mirrors the BA loader exactly (``ba_cnum``): the owning SUB's C-number
        (BA externalId minus ``_BA`` → sub group → NETSUITE_ACCOUNT_NUMBER*),
        else the customer CSV's C-number bridged from ``customer_externalId``.
        C-number is the ONLY NS key — a BA with no resolvable C-number is
        skipped by the loader, so it blocks here, with a named reason so the
        operator knows which link broke.
        """
        for r in self.bas or []:
            ba_ext = (r.get("externalId") or "").strip()
            cust_ext = (r.get("customer_externalId") or "").strip()
            cnum = self.ba_cnum(r)
            if self.customer_ns_record(cnum):
                continue  # resolved by C-number
            # Customer name for eyeballing (from the customer CSV by External ID 2).
            name = self.ext2_to_name.get(cust_ext)
            who = f"{name!r} ({cust_ext})" if name else f"{cust_ext!r}"
            if cnum:
                self.add(
                    5,
                    BLOCKER,
                    ba_ext,
                    f"BA customer {who} → C-number {cnum} not found in NS — "
                    f"wrong/stale C-number; the BA cannot attach.",
                )
            else:
                self.add(
                    5,
                    BLOCKER,
                    ba_ext,
                    f"BA customer {who}: no C-number anywhere (owning sub carries "
                    f"none; customer-CSV bridge yields none). BAs attach by "
                    f"C-number ONLY — the loader would skip this BA. Fix the "
                    f"export.",
                )

    def check_5_1_ba_subsidiary(self):
        """Check 5.1 (BLOCKER): BA subsidiary_id == its customer's NS subsidiary.

        NetSuite rejects a billing account whose ``subsidiary`` differs from its
        customer's ("Invalid Field Value 12 for the following field: subsidiary"
        — the exact 400 the B&M BA hit on 2026-07-09 when its C-number resolved
        to a subsidiary-13 customer). The loader deliberately does NOT rewrite
        the CSV subsidiary (data-integrity rule), so this check gates instead:
        for every BA whose customer resolves (check 5 owns the ones that don't),
        compare the CSV ``subsidiary_id`` to the NS customer's subsidiary and
        block on mismatch.
        """
        for r in self.bas or []:
            ba_ext = (r.get("externalId") or "").strip()
            rec = self.customer_ns_record(self.ba_cnum(r))
            if not rec:
                continue  # unresolvable customer — check 5 owns it
            csv_sub = (r.get("subsidiary_id") or "").strip()
            ns_sub = rec.get("subsidiary")
            if csv_sub and str(ns_sub) != csv_sub:
                self.add(
                    5.1,
                    BLOCKER,
                    ba_ext,
                    f"BA subsidiary_id {csv_sub} != customer's NS subsidiary "
                    f"{ns_sub} (customer id {rec.get('id')}) — NS rejects this "
                    f"with 'Invalid Field Value ... subsidiary'. Fix the export "
                    f"or the customer binding.",
                )

    def check_6_deal_has_ba(self):
        """Check 6 (BLOCKER): every sub has its own Billing Account (`<sub>_BA`).

        RULE (since 2026-07-09): a subscription is NEVER loaded without its
        billing account — the subscription loader hard-refuses (error + skip),
        because NS would otherwise silently attach the customer's DEFAULT BA.
        This check BLOCKS, mirroring the loader's mandatory-BA gate exactly.

        Convention: one BA per SUB, keyed on the FULL sub External ID —
        `<sub External ID>_BA` (i.e. `<deal>_<subid>_BA`, e.g.
        498500387059_27401 → 498500387059_27401_BA). The subscription loader
        looks its BA up by exactly this key, so this is asked PER SUB — not per
        deal, not per customer. A sub is covered when its `<sub>_BA` is either:
          • already in NS (revisioned externalId in ``ns_ba_extids``), or
          • queued in the billing CSV (a row whose externalId is `<sub>_BA`).
        A sub with NEITHER → BLOCKER. Fix by keeping the sub's BA row in the
        billing CSV, or `--create-missing-bas`. NB: a customer having some OTHER
        billing account — even a sibling sub's BA on the same deal — does NOT
        cover a sub; the loader needs THIS sub's `<sub>_BA`.
        """
        # Sub External IDs covered by a BA row in the billing CSV (strip `_BA`).
        ba_csv_subs = set()
        for r in self.bas or []:
            ba_ext = (r.get("externalId") or "").strip()
            if ba_ext.endswith("_BA"):
                ba_csv_subs.add(ba_ext[: -len("_BA")])
        # Only flag subs whose customer is actually in NS (others are owned by
        # checks 1/3/4).
        for ext in sorted(self.sub_groups):
            grp = self.sub_groups[ext]
            name = (grp[0].get("Customer") or "").strip()
            if not self.customer_ns_record(self.sub_cnum(grp)):
                continue  # customer not in NS — checks 1 / 3 / 4 own this
            if ext in ba_csv_subs:
                continue  # a `<sub>_BA` row is queued in the billing CSV
            if config.apply_revision(f"{ext}_BA") in self.ns_ba_extids:
                continue  # `<sub>_BA` already exists in NS
            self.add(
                6,
                BLOCKER,
                ext,
                f"sub {ext} (customer {name!r}) has NO `{ext}_BA` — neither in "
                f"the billing CSV nor in NS. Subs are NEVER loaded without a BA; "
                f"the loader will refuse this sub. Add the BA row, or run "
                f"--create-missing-bas.",
            )

    def check_7_subsidiary(self):
        """Check 7 (BLOCKER): sub subsidiary matches its customer's NS subsidiary.

        Maps the sub's ``Subsidiary`` name through SUBSIDIARY_MAP to an NS id and
        compares it against the customer's ``subsidiary`` as recorded in NS. A
        mismatch is a hard NS 400 at load time. Skips subs already flagged by
        check 1 (no resolvable customer) so each problem is reported once.
        Catches the B&M Mchugh case (CSV sends subsidiary 12; NS customer is 13).
        """
        for ext, grp in self.sub_groups.items():
            header = grp[0]
            name = (header.get("Customer") or "").strip()
            cnum = self.sub_cnum(grp)
            rec = self.customer_ns_record(cnum)
            if rec is None:
                continue  # already flagged by check 1
            sub_subsidiary_name = (header.get("Subsidiary") or "").strip()
            expected = SUB_SUBSIDIARY_MAP.get(sub_subsidiary_name)
            if expected is None:
                self.add(
                    7,
                    BLOCKER,
                    ext,
                    f"sub Subsidiary {sub_subsidiary_name!r} is not in "
                    f"SUBSIDIARY_MAP.",
                )
                continue
            ns_subsidiary = rec.get("subsidiary")
            if str(ns_subsidiary) != str(expected):
                self.add(
                    7,
                    BLOCKER,
                    ext,
                    f"sub subsidiary {expected} ({sub_subsidiary_name}) != "
                    f"customer {name!r} NS subsidiary {ns_subsidiary}.",
                )

    def check_8_startdates(self):
        """Check 8 (BLOCKER): BA startDate is present and not after its sub.

        Each BA is matched to ITS sub by stripping the `_BA` suffix from its
        externalId (`<sub External ID>_BA` → the sub's External ID — the
        `<deal>_<subid>_BA` convention). Blocks if the BA's ``startDate`` is
        blank (NS would default it to today, breaking a sub that starts
        earlier) or if it is later than that sub's earliest ``Start Date``. A
        BA must begin on/before the subscription it bills. Legacy `<deal>_BA`
        rows (no matching sub External ID) fall back to the earliest start
        across the whole deal. Catches the Zebra blank-startDate case and the
        505257036006 10/06-vs-12/06 400.
        """
        # earliest sub Start Date per sub External ID, and per deal (fallback
        # for legacy `<deal>_BA` rows).
        earliest_by_sub: dict[str, str] = {}
        earliest_by_deal: dict[str, str] = {}
        for ext, grp in self.sub_groups.items():
            deal = _deal_id(ext)
            for row in grp:
                sd = (row.get("Start Date") or "").strip()
                if not sd:
                    continue
                if ext not in earliest_by_sub or sd < earliest_by_sub[ext]:
                    earliest_by_sub[ext] = sd
                if deal not in earliest_by_deal or sd < earliest_by_deal[deal]:
                    earliest_by_deal[deal] = sd
        for r in self.bas or []:
            ba_ext = (r.get("externalId") or "").strip()
            # <sub External ID>_BA → sub External ID (legacy <deal>_BA → deal)
            target = ba_ext[: -len("_BA")] if ba_ext.endswith("_BA") else ba_ext
            ba_start = (r.get("startDate") or "").strip()
            if not ba_start:
                self.add(
                    8,
                    BLOCKER,
                    ba_ext,
                    "BA startDate is blank (NS would default to today, "
                    "breaking subs that start earlier).",
                )
                continue
            sub_start = earliest_by_sub.get(target) or earliest_by_deal.get(
                _deal_id(ba_ext)
            )
            if sub_start and ba_start > sub_start:
                self.add(
                    8,
                    BLOCKER,
                    ba_ext,
                    f"BA startDate {ba_start} > earliest sub Start Date "
                    f"{sub_start} for its sub {target}.",
                )

    def check_9_sales_item(self):
        """Check 9 (BLOCKER): every active sub line maps to a real Sales Item.

        Scans the included lines (``Lines: Include == 'T'``) of each sub group
        and counts those whose ``Sales Item`` is blank or the literal
        ``NOT MAPPED`` sentinel. The loader silently skips such lines, so the sub
        would load short some items — hence a blocker for sign-off. Reports one
        finding per sub with the counts (blank vs NOT MAPPED), not one per line,
        to keep the report readable. Catches e.g. the unmapped EAP line.
        """
        for ext, grp in self.sub_groups.items():
            n_blank = n_notmapped = 0
            for row in grp:
                if (row.get("Lines: Include") or "").strip() != "T":
                    continue
                item = (row.get("Sales Item") or "").strip().replace("​", "")
                if not item:
                    n_blank += 1
                elif item == "NOT MAPPED":
                    n_notmapped += 1
            if n_blank or n_notmapped:
                parts = []
                if n_notmapped:
                    parts.append(f"{n_notmapped} 'NOT MAPPED'")
                if n_blank:
                    parts.append(f"{n_blank} blank")
                self.add(
                    9,
                    BLOCKER,
                    ext,
                    f"{' + '.join(parts)} active line(s) have an unmapped "
                    f"Sales Item (those lines will be skipped).",
                )

    def check_9_1_line_has_price_plan(self):
        """Check 9.1 (BLOCKER): every included line carries a Price Plan External ID.

        The price plan record holds ALL the pricing (currency, pricePlanType,
        minimum/maximumAmount, priceTiers) — the sub line only references it by
        this external id. A line with no plan bills at the plan's price-book
        default, and Moorepay's standard books are £0 placeholders — so an
        unpriced line silently creates a £0 subscription line. The loader
        refuses to load such subs (pricing gate, mirror of the mandatory-BA
        gate); this check surfaces them pre-load.

        Replicates the loader's per-item semantics so it never false-blocks:
        rows are deduped by Sales Item name, and a blank Price Plan External ID
        on one row is backfilled from a later row with the same item name.
        Only items whose FINAL plan is still blank are reported — one finding
        per sub, listing the unpriced item names.
        """
        for ext, grp in self.sub_groups.items():
            items_seen: dict = {}
            items_order: list = []
            for row in grp:
                if (row.get("Lines: Include") or "").strip() != "T":
                    continue
                item = (row.get("Sales Item") or "").strip().replace("​", "")
                if not item or item == "NOT MAPPED":
                    continue  # check 9 owns unmapped items
                row_pp = (row.get("Price Plan External ID") or "").strip() or None
                if item not in items_seen:
                    items_seen[item] = row_pp
                    items_order.append(item)
                elif items_seen[item] is None and row_pp is not None:
                    items_seen[item] = row_pp
            unpriced = [i for i in items_order if items_seen[i] is None]
            if unpriced:
                self.add(
                    9.1,
                    BLOCKER,
                    ext,
                    f"{len(unpriced)} included line(s) have NO Price Plan "
                    f"External ID: {unpriced} — would bill at the £0 book "
                    f"default; the loader refuses to load this sub. Regenerate "
                    f"the subs/pricing exports with a plan per line.",
                )

    def check_10_price_plan(self):
        """Check 10 (WARNING): each line's price plan exists in NS.

        For every included line carrying a ``Price Plan External ID``, applies
        the load revision and checks the result against the price plans found in
        NS. A blank value is fine (the line intentionally uses the price book
        default). A non-blank value missing from NS warns that the line will fall
        back to the book default instead of its intended plan — informational,
        not a blocker.
        """
        for ext, grp in self.sub_groups.items():
            for row in grp:
                if (row.get("Lines: Include") or "").strip() != "T":
                    continue
                raw_pp = (row.get("Price Plan External ID") or "").strip()
                if not raw_pp:
                    continue  # blank = book default; not a problem
                rev_pp = config.apply_revision(raw_pp)
                if rev_pp not in self.ns_priceplans:
                    self.add(
                        10,
                        WARNING,
                        ext,
                        f"Price Plan {rev_pp!r} not in NS; line will fall "
                        f"back to book default.",
                    )

    def check_11_price_plan_csv(self):
        """Check 11 (WARNING): each line's price plan exists in the pricing CSV.

        Companion to check 10. Check 10 asks "is the price plan already in NS?";
        this asks "is it in the pricing CSV (``PRICE_PLAN_EXTERNAL_ID``), i.e.
        can it be CREATED?". Both use the RAW external id (the pricing CSV and
        the sub column are raw; NS stores the revisioned form). Reading the two
        together: a plan missing from NS (10) but present in the CSV (11 clean)
        is resolvable — run the pricePlan load and it lands in NS. A plan missing
        from BOTH is the real gap this surfaces: there is no source row to push,
        so the line can only ever fall back to the book default. Blank price-plan
        values are skipped (the line intentionally uses the book default).

        Guard: when MANY referenced plans are missing AND the pricing CSV holds
        far more plans than the subs reference, the export was almost certainly
        generated UNSCOPED (the full price book, capped at ~20k rows) rather than
        scoped to this batch. That is a different — and louder — problem than a
        stray missing plan: filter_to_subs.py can't fix it (you can't filter in
        rows that aren't there); the pricing DDL must be re-run scoped to the
        batch. A single up-front finding says so, so the per-line warnings below
        aren't mistaken for two dozen unrelated gaps.

        Empty-file guard: a MISSING pricing CSV is handled upstream by run()'s
        per-check input guard (this check never dispatches in that case). What
        remains is a present-but-EMPTY pricing CSV (loaded, zero plan rows):
        comparing referenced plans against an empty set would falsely flag ALL of
        them. So if there are referenced plans but the CSV held none, emit ONE
        finding and return, rather than two dozen phantom coverage gaps.
        """
        if not self.pricing_csv_extids and any(
            (row.get("Price Plan External ID") or "").strip()
            for grp in self.sub_groups.values()
            for row in grp
            if (row.get("Lines: Include") or "").strip() == "T"
        ):
            self.add(
                11,
                WARNING,
                "(pricing CSV)",
                f"pricing CSV {config.PRICE_PLANS_CSV!r} contains no price-plan "
                f"rows; coverage not evaluated.",
            )
            return

        # Collect the referenced raw plans first so we can judge coverage as a
        # whole before emitting the per-line findings.
        referenced = set()
        missing = []  # (sub ext, raw_pp) for each uncovered included line
        for ext, grp in self.sub_groups.items():
            for row in grp:
                if (row.get("Lines: Include") or "").strip() != "T":
                    continue
                raw_pp = (row.get("Price Plan External ID") or "").strip()
                if not raw_pp:
                    continue  # blank = book default; nothing to create
                referenced.add(raw_pp)
                if raw_pp not in self.pricing_csv_extids:
                    missing.append((ext, raw_pp))

        # Loud, single "looks unscoped" finding when the pricing CSV dwarfs the
        # batch and coverage is badly incomplete. UNSCOPED_RATIO mirrors the
        # standalone check_pricing_coverage.py gate.
        n_have = len(self.pricing_csv_extids)
        if referenced and missing and n_have >= self.UNSCOPED_RATIO * len(referenced):
            self.add(
                11,
                WARNING,
                "(pricing CSV scope)",
                f"pricing CSV holds {n_have} plans for only {len(referenced)} "
                f"referenced by the subs ({len(missing)} uncovered) — this looks "
                f"like an UNSCOPED bulk export (full price book, capped ~20k "
                f"rows), not a per-batch file. filter_to_subs.py cannot recover "
                f"missing plans; re-run the pricing DDL scoped to this batch.",
            )

        for ext, raw_pp in missing:
            self.add(
                11,
                WARNING,
                ext,
                f"Price Plan {raw_pp!r} referenced by sub line but not in "
                f"the pricing CSV; it cannot be created/pushed to NS.",
            )

    def check_12_addresses(self):
        """Check 12 (BLOCKER): each BA's customer has default bill+ship addresses.

        The BA loader resolves ``billAddressList`` / ``shipAddressList`` from the
        customer's default billing and shipping addresses; if either is missing
        the BA is skipped. So for each BA, this blocks when its NS customer lacks
        a default billing and/or shipping address (per ``customeraddressbook``).
        BAs whose customer wasn't found in NS are skipped here — already flagged
        by check 5.
        """
        for r in self.bas or []:
            ba_ext = (r.get("externalId") or "").strip()
            cust_ext = (r.get("customer_externalId") or "").strip()
            cust = self.customer_ns_record(self.ba_cnum(r))
            if not cust:
                continue  # flagged by check 5
            cid = cust.get("id")
            missing = []
            if cid not in self.addr_billing:
                missing.append("billing")
            if cid not in self.addr_shipping:
                missing.append("shipping")
            if missing:
                name = self.ext2_to_name.get(cust_ext)
                who = f"{name!r} ({cust_ext})" if name else f"{cust_ext}"
                self.add(
                    12,
                    BLOCKER,
                    ba_ext,
                    f"customer {who} (id {cid}) has no default "
                    f"{' and '.join(missing)} address in NS; BA load skips.",
                )

    def check_13_required_fields(self):
        """Check 13 (BLOCKER): required BA fields are present.

        Guards against silent skips by blocking any BA whose ``subsidiary_id`` or
        ``currency_id`` is blank — both are mandatory NS references the loader
        needs to build a valid payload.
        """
        for r in self.bas or []:
            ba_ext = (r.get("externalId") or "").strip()
            for field in ("subsidiary_id", "currency_id"):
                if not (r.get(field) or "").strip():
                    self.add(13, BLOCKER, ba_ext, f"BA {field} is blank.")

    def check_14_extid_shape(self):
        """Check 14 (WARNING): BA externalId follows the ``<deal>_<subid>_BA`` shape.

        Two sanity tests per BA row:
          • the externalId ends in ``_BA`` (bare convention drift), and
          • its prefix (externalId minus ``_BA``) is the External ID of a sub in
            the subs CSV — the subscription loader looks its BA up by exactly
            ``<sub External ID>_BA``, so a BA whose prefix matches no sub would
            load fine but NEVER be linked by any subscription (the Care Shield
            failure mode: BA ``…_27401_BA`` vs loader asking for the old
            ``<deal>_BA``). Warning only; skipped when the subs CSV is absent.
        """
        for r in self.bas or []:
            ba_ext = (r.get("externalId") or "").strip()
            if not ba_ext.endswith("_BA"):
                self.add(
                    14,
                    WARNING,
                    ba_ext,
                    "BA externalId does not end in '_BA' (convention drift).",
                )
                continue
            sub_ext = ba_ext[: -len("_BA")]
            if self.sub_groups and sub_ext not in self.sub_groups:
                self.add(
                    14,
                    WARNING,
                    ba_ext,
                    f"BA prefix {sub_ext!r} matches no sub External ID in the subs "
                    f"CSV — no subscription in this batch would link to this BA "
                    f"(the loader looks up `<sub External ID>_BA`).",
                )

    # ── orchestration ─────────────────────────────────────────────────────
    def run(self, entities=None):
        """Prefetch the needed NS data, then run the in-scope checks.

        Args:
            entities: iterable of entity types (e.g. {"subscription"}) to scope
                the run to only the checks guarding them (per CHECK_ENTITY), or
                None to run the whole catalog.

        Only the NS queries required by the active checks are issued — e.g. the
        customer maps are fetched only if a customer-touching check is in scope,
        the company-name map only for check 4, etc. — each as one batched,
        chunked SuiteQL call. Checks are dispatched in ascending number order.
        Returns ``self.findings`` (also kept on the instance for ``report``).
        """
        active_checks = [
            n
            for n in sorted(CHECK_ENTITY)
            if entities is None or CHECK_ENTITY[n] in entities
        ]

        # Prefetch NS data only for the checks we'll run. All customer-touching
        # checks resolve via the C-number map (the only NS key).
        needs_customers = any(
            c in active_checks for c in (1, 2, 3, 4, 5, 5.1, 6, 7, 12)
        )
        needs_names = 4 in active_checks
        needs_ba_extids = 6 in active_checks
        needs_priceplans = 10 in active_checks
        needs_pricing_csv = 11 in active_checks
        needs_addresses = 12 in active_checks

        if needs_customers:
            # Gather every C-number the in-scope checks might resolve: from the
            # subs (deal C-numbers + the customer-CSV C-number by name) and from
            # the BAs (customer_externalId bridged through the customer CSV).
            cnums = set()
            for grp in self.sub_groups.values():
                for col in (
                    "NETSUITE_ACCOUNT_NUMBER",
                    "NETSUITE_ACCOUNT_NUMBER_COMPANY_LEVEL",
                ):
                    v = self._group_value(grp, col)
                    if v:
                        cnums.add(v)
                nc = self.name_cnum(grp[0].get("Customer"))
                if nc:
                    cnums.add(nc)
            for r in self.bas or []:
                c = self.ba_cnum(r)
                if c:
                    cnums.add(c)
            print(
                f"  · querying NS ({_env_label()}) for {len(cnums)} customer "
                f"C-numbers (entityid)…"
            )
            self._fetch_ns_by_cnum(cnums)
            # C-number is the ONLY NS key (convention 2026-07-09) — no
            # externalId or name resolution here. Check 4's by-name lookup is
            # informational only ("in NS under a different id — reconcile").

        if needs_names:
            # For check 4: NS lookup by exact company name (alias-resolved,
            # uppercased) — the last-resort "is it in NS under a different id?".
            names = set()
            for grp in self.sub_groups.values():
                nm = (grp[0].get("Customer") or "").strip()
                if nm:
                    names.add(config.CUSTOMER_NAME_ALIASES.get(nm.upper(), nm).upper())
            print(f"  · querying NS for {len(names)} customer company names…")
            self._fetch_ns_by_name(names)

        if needs_ba_extids:
            # For check 6: which `<sub>_BA` billing accounts already exist in NS.
            # Per SUB (the sub links to its BA by `<sub External ID>_BA` — the
            # `<deal>_<subid>_BA` convention, since 2026-07-09).
            ba_extids = {config.apply_revision(f"{e}_BA") for e in self.sub_groups}
            print(f"  · querying NS for {len(ba_extids)} `<sub>_BA` externalIds…")
            self._fetch_ns_ba_extids(ba_extids)

        if needs_priceplans:
            rev_pps = set()
            for grp in self.sub_groups.values():
                for row in grp:
                    if (row.get("Lines: Include") or "").strip() != "T":
                        continue
                    raw_pp = (row.get("Price Plan External ID") or "").strip()
                    if raw_pp:
                        rev_pps.add(config.apply_revision(raw_pp))
            print(f"  · querying NS for {len(rev_pps)} price-plan externalIds…")
            self._fetch_ns_priceplans(rev_pps)

        if needs_pricing_csv:
            pricing_rows = _read_csv(config.PRICE_PLANS_CSV)
            if pricing_rows is None:
                print(
                    f"  ⚠ pricing CSV not found, skipping check 11: "
                    f"{config.PRICE_PLANS_CSV}"
                )
            else:
                self.csv_loaded["pricing"] = True
                self.pricing_csv_extids = {
                    (r.get("PRICE_PLAN_EXTERNAL_ID") or "").strip()
                    for r in pricing_rows
                    if (r.get("PRICE_PLAN_EXTERNAL_ID") or "").strip()
                }
                print(
                    f"  · read {len(self.pricing_csv_extids)} price-plan "
                    f"externalIds from the pricing CSV…"
                )

        if needs_addresses:
            cids = {c["id"] for c in self.ns_by_cnum.values() if c.get("id")}
            print(f"  · querying NS addresses for {len(cids)} customers…")
            self._fetch_ns_addresses(cids)

        dispatch = {
            1: self.check_1_sub_customer,
            2: self.check_2_subs_cnumber_valid,
            3: self.check_3_customer_load_status,
            4: self.check_4_customer_name_in_ns,
            5: self.check_5_ba_customer,
            5.1: self.check_5_1_ba_subsidiary,
            6: self.check_6_deal_has_ba,
            7: self.check_7_subsidiary,
            8: self.check_8_startdates,
            9: self.check_9_sales_item,
            9.1: self.check_9_1_line_has_price_plan,
            10: self.check_10_price_plan,
            11: self.check_11_price_plan_csv,
            12: self.check_12_addresses,
            13: self.check_13_required_fields,
            14: self.check_14_extid_shape,
        }
        for n in active_checks:
            # Missing-input guard (uniform across all checks): if a required CSV
            # didn't load, report the check as "not evaluated" with its native
            # severity, rather than running it against empty data.
            missing = sorted(
                name
                for name in CHECK_REQUIRED_CSVS.get(n, ())
                if not self.csv_loaded.get(name)
            )
            if missing:
                sev = BLOCKER if n in BLOCKER_CHECKS else WARNING
                names = ", ".join(missing)
                self.add(
                    n,
                    sev,
                    "(input)",
                    f"not evaluated — required {names} CSV missing/unreadable "
                    f"(fix the *_CSV path in config.py and re-run).",
                )
                continue
            dispatch[n]()
        return self.findings

    # ── reporting ──────────────────────────────────────────────────────────
    def report(self, entities=None):
        """Print the grouped report and return the blocker count.

        Groups ``self.findings`` by check and prints every check in scope under
        its title with a status mark — ``✓`` clean, ``⚠`` warnings only, ``✗`` has
        a blocker — followed by each finding. Ends with a summary line and an
        overall RESULT (blocked vs OK-to-load). ``entities`` scopes which checks
        are shown, matching the set passed to ``run``. Returns the number of
        blocker findings, which the caller maps to the process exit code (>0 → 1).
        Call after ``run``; on its own it would just print empty checks.
        """
        active_checks = [
            n
            for n in sorted(CHECK_ENTITY)
            if entities is None or CHECK_ENTITY[n] in entities
        ]
        by_check = defaultdict(list)
        for f in self.findings:
            by_check[f.check].append(f)

        print("\n" + "=" * 72)
        print("PRE-LOAD VALIDATION REPORT")
        print(f"  revision: {config.LOAD_REVISION}")
        scope = ", ".join(sorted(entities)) if entities else "full pipeline"
        print(f"  scope:    {scope}")
        print("=" * 72)

        n_block = n_warn = 0
        current_group = None
        for n in active_checks:
            # Print a "++++" divider whenever we enter a new major group, so the
            # report is skimmable by eye.
            group = CHECK_GROUP.get(n)
            if group != current_group:
                print(f"\n{'+' * 72}")
                print(f"++++ {group}")
                print("+" * 72)
                current_group = group
            items = by_check.get(n, [])
            if items:
                # A check may emit mixed severities (e.g. check 1): blocker wins.
                mark = "✗" if any(i.severity == BLOCKER for i in items) else "⚠"
            else:
                mark = "✓"
            print(f"\n[{mark}] Check {n}: {CHECK_TITLE[n]}")
            explain = CHECK_EXPLAIN.get(n)
            if explain:
                print(f"      ↳ {explain}")
            for it in items:
                print(f"      {it.severity:7} {it.ident}: {it.message}")
                if it.severity == BLOCKER:
                    n_block += 1
                else:
                    n_warn += 1
            if not items:
                print("      (clean)")

        print("\n" + "-" * 72)
        print(f"SUMMARY: {n_block} blocker(s), {n_warn} warning(s)")
        if n_block:
            print("RESULT:  ✗ BLOCKED — resolve blockers before loading.")
        else:
            print("RESULT:  ✓ OK to load" + (" (warnings present)" if n_warn else ""))
        print("-" * 72)
        return n_block


def run_validation(entities=None, client=None) -> int:
    """Entry point usable from main.py. Returns 0 if clean, 1 if blockers."""
    v = Validator(client=client)
    v.run(entities)
    n_block = v.report(entities)
    return 1 if n_block else 0


def main():
    """CLI entry point for ``python validate.py``.

    Parses ``--entity`` (repeatable; omit for the full pipeline), runs the
    validation, and exits with its status code (0 clean / 1 blockers) so the
    command can gate a shell pipeline. ``main.py --validate`` calls
    ``run_validation`` directly instead of going through here.
    """
    parser = argparse.ArgumentParser(description="Read-only pre-load validator.")
    parser.add_argument(
        "--entity",
        action="append",
        choices=["customer", "billingAccount", "subscription", "oneOff", "pricePlan"],
        help="Validate only checks guarding this entity (repeatable). "
        "Omit to validate the whole pipeline.",
    )
    args = parser.parse_args()
    entities = set(args.entity) if args.entity else None
    sys.exit(run_validation(entities))


if __name__ == "__main__":
    main()
