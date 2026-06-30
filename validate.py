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
  - CUSTOMERS are keyed in prod by their RAW `External ID 2` (NO revision
    suffix) — they pre-exist in NS. Only BA / subscription / pricePlan / oneOff
    carry the suffix. Customer resolution here is therefore RAW. See the memory
    note `customer-extid-raw-in-prod`.

Severity:
  BLOCKER  — would fail or silently skip the load. Exit code 1 if any present.
  WARNING  — informational; load proceeds.

Usage:
  python validate.py                      # validate the whole pipeline
  python validate.py --entity subscription
  (or via main.py:  python main.py --validate [--entity ...])
"""

import os
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
CUSTOMER_CNUM_COL = "n/a 2"

# Each check → which entity load it guards. Used to filter when --entity given.
CHECK_ENTITY = {
    1: "subscription",
    1.1: "subscription",
    2: "billingAccount",
    3: "subscription",
    4: "billingAccount",
    5: "subscription",
    6: "subscription",
    7: "subscription",
    7.1: "subscription",
    8: "billingAccount",
    9: "billingAccount",
    10: "billingAccount",
}

# Severity each check emits. Used so a "not evaluated" finding (missing input)
# carries the check's native severity — you can't confirm a BLOCKER check is
# safe if its input is absent, so that absence must itself block.
BLOCKER_CHECKS = {1, 1.1, 2, 3, 4, 5, 8, 9}

# CSV inputs each check requires to produce a meaningful result. If any are
# missing (None from _read_csv — a bad path in config), run() skips the check
# with ONE "(input) not evaluated" finding instead of running it against empty
# data. Two failure modes this prevents, both seen/possible in this validator:
#   • false-finding STORM — a check comparing present records against an empty
#     reference set flags every record (check 7.1 did this for a missing pricing
#     CSV; check 1 would for a missing customer CSV).
#   • false CLEAN — a check that only ITERATES a missing CSV emits nothing and
#     the report prints "✓ clean", hiding that it never ran (checks 9/10 vs a
#     missing BA CSV).
# A CSV is listed whether the check iterates it (subject) or reads it to resolve
# references — either way an absent file makes the result meaningless.
CHECK_REQUIRED_CSVS = {
    1: {"subscription", "customer"},
    1.1: {"subscription"},
    2: {"billingAccount"},
    3: {"subscription", "customer"},
    4: {"subscription", "billingAccount"},
    5: {"subscription"},
    6: {"subscription", "billingAccount"},
    7: {"subscription"},
    7.1: {"subscription", "pricing"},
    8: {"billingAccount"},
    9: {"billingAccount"},
    10: {"billingAccount"},
}

# Major group each check belongs to, in dispatch (sorted) order. The report
# prints a "++++" divider with the group name the first time a check from a new
# group appears, so the output can be skimmed by eye. Checks run 1, 1.1, 2, 3,
# 4, 5, 6, 7, 7.1, 8, 9, 10 — themes interleave by number, which is intentional.
CHECK_GROUP = {
    1: "CUSTOMERS — subscription resolves to an NS customer",
    1.1: "CUSTOMERS — subscription resolves to an NS customer",
    2: "BILLING ACCOUNTS — customer link",
    3: "SUBSCRIPTIONS — subsidiary match",
    4: "BILLING ACCOUNTS — start dates",
    5: "SUBSCRIPTIONS — sales items",
    6: "BILLING ACCOUNTS ↔ SUBSCRIPTIONS — deal alignment",
    7: "PRICE PLANS — coverage (NS + pricing CSV)",
    7.1: "PRICE PLANS — coverage (NS + pricing CSV)",
    8: "BILLING ACCOUNTS — addresses & required fields",
    9: "BILLING ACCOUNTS — addresses & required fields",
    10: "BILLING ACCOUNTS — addresses & required fields",
}

CHECK_TITLE = {
    1: "Sub customer resolves to an NS customer (by C-number)",
    1.1: "Sub customer's C-number is in the subs export AND exists in NS",
    2: "BA customer resolves to an NS customer (by C-number via customer CSV)",
    3: "Sub subsidiary == its customer's NS subsidiary",
    4: "BA startDate present and <= earliest sub Start Date for the deal",
    5: "Active sub line Sales Item is mapped (not blank / not NOT MAPPED)",
    6: "BA <-> sub deal alignment (every deal has both)",
    7: "Sub line Price Plan External ID exists in NS",
    7.1: "Sub line Price Plan External ID exists in the pricing CSV",
    8: "BA customer has default billing AND shipping address in NS",
    9: "BA required fields present (subsidiary_id, currency_id)",
    10: "externalId shape sanity (BA = <deal_id>_BA)",
}


@dataclass
class Finding:
    """One problem reported by a single check.

    Attributes:
        check:    the check number that produced it (e.g. 1, 1.1, 5). Used to
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
    Used by checks 4 and 6 to line subscriptions and BAs up by deal.
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
        # data (which would either falsely flag every record — check 7.1's old
        # bug — or silently report a misleading "clean"). "pricing" is filled in
        # run()'s prefetch, since it's only read when check 7.1 is in scope.
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

        # Customer-CSV bridges. The C-number (CUSTOMER_CNUM_COL) is the key that
        # actually resolves to NS; the other maps exist to *reach* it from a name
        # (subs) or from an External ID 2 / MP_HubSpot id (billing accounts).
        #   name_to_ext   : Company Name (UPPER) → External ID 2 (for messages /
        #                   "is this name even in the customer CSV?")
        #   name_to_cnum  : Company Name (UPPER) → C-number  (resolve subs)
        #   ext2_to_cnum  : External ID 2        → C-number  (resolve BAs, which
        #                   only carry customer_externalId = MP_HubSpot_*)
        # ext2_to_cnum maps every row that has an External ID 2, even when its
        # C-number is blank, so a check can tell "no customer-CSV row" (key
        # absent) apart from "row exists but has no C-number" (value "").
        self.name_to_ext = {}
        self.name_to_cnum = {}
        self.ext2_to_cnum = {}
        for r in self.customers or []:
            nm = (r.get("Company Name") or "").strip().upper()
            ext = (r.get("External ID 2") or "").strip()
            cnum = (r.get(CUSTOMER_CNUM_COL) or "").strip()
            if nm and ext:
                self.name_to_ext[nm] = ext
            if nm:
                self.name_to_cnum[nm] = cnum
            if ext:
                self.ext2_to_cnum[ext] = cnum

        # Group subs by deal external id (matches subscription loader grouping)
        self.sub_groups = defaultdict(list)
        for r in self.subs_rows or []:
            ext = (r.get("External ID") or "").strip()
            if ext:
                self.sub_groups[ext].append(r)

        # --- NS lookups (populated lazily in run()) ---------------------
        # Single customer map, keyed by C-number (entityid) — the authoritative
        # join to NS. Holds {id, subsidiary, externalid, companyname}. Used by
        # checks 1, 1.1, 2, 3 and 8.
        self.ns_by_cnum: dict[str, dict] = {}
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

    def resolve_customer_ext(self, raw_name: str):
        """sub/BA Customer name → canonical (via alias map) → External ID 2.

        Retained only for messages / "is this name in the customer CSV at all?"
        — NOT for NS resolution (External ID 2 is a HubSpot id absent from NS).
        Returns None if the name matches no customer-CSV row.
        """
        name = (raw_name or "").strip()
        canonical = config.CUSTOMER_NAME_ALIASES.get(name.upper(), name)
        return self.name_to_ext.get(canonical.upper())

    def name_cnum(self, raw_name: str) -> str:
        """sub/BA Customer name → canonical (alias map) → C-number, or ''.

        Returns '' when the name matches no customer-CSV row, or matches a row
        whose C-number column is blank — callers that need to tell those apart
        check ``resolve_customer_ext`` (None = no row) separately.
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
        companyname}}) — the one customer map every customer-touching check
        (1, 1.1, 2, 3, 8) reads from. Queried in chunks of ≤200 via ``WHERE
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
                }

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

    # ── checks ───────────────────────────────────────────────────────────
    def check_1_sub_customer(self):
        """Check 1: every subscription's customer resolves to NS by C-number.

        Resolution per sub group: ``sub_cnum`` (subs NETSUITE_ACCOUNT_NUMBER /
        _COMPANY_LEVEL, else the customer CSV's C-number by name) → look the
        C-number up in NS by entityid. The customer CSV's External ID 2 is a
        HubSpot id absent from NS, so it is NOT used to resolve (see
        CUSTOMER_CNUM_COL).

        Severities:
          • C-number present but not in NS → BLOCKER (a concrete id that should
            exist but doesn't — the sub can't attach to a customer).
          • no C-number anywhere → WARNING (the customer is unresolvable from the
            data we have; the sub will be skipped). Names the customer so it can
            be eyeballed against the customers CSV. Not a hard blocker — mirrors
            the check 1.1 blank-C-name policy.
        """
        for ext, grp in self.sub_groups.items():
            name = (grp[0].get("Customer") or "").strip()
            cnum = self.sub_cnum(grp)
            if not cnum:
                in_csv = self.resolve_customer_ext(name) is not None
                why = (
                    f"no C-number in subs data and customer-CSV "
                    f"{CUSTOMER_CNUM_COL!r} is blank"
                    if in_csv
                    else "no C-number in subs data and no customer-CSV row"
                )
                self.add(
                    1,
                    WARNING,
                    ext,
                    f"customer {name!r}: {why}; cannot resolve to an NS customer "
                    f"— sub would be skipped. Verify {name!r} manually.",
                )
            elif cnum not in self.ns_by_cnum:
                self.add(
                    1,
                    BLOCKER,
                    ext,
                    f"customer {name!r}: C-number {cnum} not found in NS.",
                )

    def check_1_1_customer_in_env(self):
        """Confirm the customer exists in the ACTIVE env using the C-number the
        SUBS EXPORT itself carries — the deal-level data-completeness companion
        to check 1. The subs CSV carries two C-numbers: NETSUITE_ACCOUNT_NUMBER
        (deal/customer level) and NETSUITE_ACCOUNT_NUMBER_COMPANY_LEVEL
        (company/parent level). Try the first; if it resolves, that suffices.
        Else try the second.

        Unlike check 1, this does NOT fall back to the customer CSV — it answers
        specifically "does the subs export stand on its own?". So:
          • both C-numbers BLANK → WARNING. The subs export carries no C-number
            for this deal (check 1 may still resolve it via the customer CSV).
            Names the customer so it can be eyeballed against the customers CSV.
          • a C-number IS present but not found in NS → BLOCKER. A concrete id
            that doesn't exist in the active env — a real "customer not in this
            NS account" problem."""
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
            if not tried:
                self.add(
                    1.1,
                    WARNING,
                    ext,
                    f"customer {name!r}: no C-number in subs data "
                    f"(NETSUITE_ACCOUNT_NUMBER and _COMPANY_LEVEL both blank); "
                    f"cannot confirm in {env} from the subs export alone. Check 1 "
                    f"may still resolve it via the customers CSV — verify {name!r} "
                    f"there.",
                )
            else:
                self.add(
                    1.1,
                    BLOCKER,
                    ext,
                    f"customer {name!r}: C-name(s) {', '.join(tried)} not found "
                    f"in {env} NS by entityid.",
                )

    def check_2_ba_customer(self):
        """Check 2 (BLOCKER): every BA points at a real NS customer.

        The billing CSV carries only ``customer_externalId`` (an MP_HubSpot id,
        absent from NS), so resolution bridges through the customer CSV:
        customer_externalId → External ID 2 row → its C-number → NS by entityid.
        Blocks if: the value is blank; it matches no customer-CSV row; the row
        has no C-number; or the C-number isn't in NS. Each is a distinct, named
        failure so the operator knows which link broke.
        """
        for r in self.bas or []:
            ba_ext = (r.get("externalId") or "").strip()
            cust_ext = (r.get("customer_externalId") or "").strip()
            if not cust_ext:
                self.add(2, BLOCKER, ba_ext, "BA customer_externalId is blank.")
                continue
            cnum = self.ext2_to_cnum.get(cust_ext)
            if cnum is None:
                self.add(
                    2,
                    BLOCKER,
                    ba_ext,
                    f"BA customer_externalId {cust_ext!r} matches no customer-CSV "
                    f"row (cannot map to a C-number).",
                )
            elif not cnum:
                self.add(
                    2,
                    BLOCKER,
                    ba_ext,
                    f"BA customer {cust_ext!r}: customer-CSV row has no C-number "
                    f"('{CUSTOMER_CNUM_COL}' blank); cannot resolve to NS.",
                )
            elif cnum not in self.ns_by_cnum:
                self.add(
                    2,
                    BLOCKER,
                    ba_ext,
                    f"BA customer {cust_ext!r} → C-number {cnum} not found in NS.",
                )

    def check_3_subsidiary(self):
        """Check 3 (BLOCKER): sub subsidiary matches its customer's NS subsidiary.

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
            if not cnum or cnum not in self.ns_by_cnum:
                continue  # already flagged by check 1
            sub_subsidiary_name = (header.get("Subsidiary") or "").strip()
            expected = SUB_SUBSIDIARY_MAP.get(sub_subsidiary_name)
            if expected is None:
                self.add(
                    3,
                    BLOCKER,
                    ext,
                    f"sub Subsidiary {sub_subsidiary_name!r} is not in "
                    f"SUBSIDIARY_MAP.",
                )
                continue
            ns_subsidiary = self.ns_by_cnum[cnum].get("subsidiary")
            if str(ns_subsidiary) != str(expected):
                self.add(
                    3,
                    BLOCKER,
                    ext,
                    f"sub subsidiary {expected} ({sub_subsidiary_name}) != "
                    f"customer {name!r} (C-number {cnum}) NS subsidiary "
                    f"{ns_subsidiary}.",
                )

    def check_4_startdates(self):
        """Check 4 (BLOCKER): BA startDate is present and not after its subs.

        For each deal, finds the earliest sub ``Start Date``, then for the deal's
        BA: blocks if ``startDate`` is blank (NS would default it to today,
        breaking any sub that starts earlier) or if it is later than the earliest
        sub start. A BA must begin on/before the first subscription it bills.
        Catches the Zebra blank-startDate case.
        """
        # earliest sub Start Date per deal
        earliest = {}
        for ext, grp in self.sub_groups.items():
            deal = _deal_id(ext)
            for row in grp:
                sd = (row.get("Start Date") or "").strip()
                if sd:
                    if deal not in earliest or sd < earliest[deal]:
                        earliest[deal] = sd
        for r in self.bas or []:
            ba_ext = (r.get("externalId") or "").strip()
            deal = _deal_id(ba_ext)  # <deal>_BA → deal
            ba_start = (r.get("startDate") or "").strip()
            if not ba_start:
                self.add(
                    4,
                    BLOCKER,
                    ba_ext,
                    "BA startDate is blank (NS would default to today, "
                    "breaking subs that start earlier).",
                )
                continue
            sub_start = earliest.get(deal)
            if sub_start and ba_start > sub_start:
                self.add(
                    4,
                    BLOCKER,
                    ba_ext,
                    f"BA startDate {ba_start} > earliest sub Start Date "
                    f"{sub_start} for deal {deal}.",
                )

    def check_5_sales_item(self):
        """Check 5 (BLOCKER): every active sub line maps to a real Sales Item.

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
                    5,
                    BLOCKER,
                    ext,
                    f"{' + '.join(parts)} active line(s) have an unmapped "
                    f"Sales Item (those lines will be skipped).",
                )

    def check_6_deal_alignment(self):
        """Check 6 (WARNING): every deal has both a BA and a subscription.

        Compares the set of deal ids seen in the subscription groups against the
        set seen in the BA rows (both reduced via ``_deal_id`` — the token before
        the first underscore). Warns for each deal that has subs but no BA row
        (the sub loads with no billing-account reference) and for each deal with
        a BA but no sub (an orphan BA). Warning, not blocker — the load can still
        proceed. Catches the Care Shield gap. NOTE: if the BA CSV is missing
        entirely, every sub deal trips this warning.
        """
        sub_deals = {_deal_id(e) for e in self.sub_groups}
        ba_deals = {
            _deal_id((r.get("externalId") or "").strip()) for r in (self.bas or [])
        }
        for d in sorted(sub_deals - ba_deals):
            self.add(6, WARNING, d, "deal has subscription(s) but no BA row.")
        for d in sorted(ba_deals - sub_deals):
            self.add(6, WARNING, d, "deal has a BA row but no subscription.")

    def check_7_price_plan(self):
        """Check 7 (WARNING): each line's price plan exists in NS.

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
                        7,
                        WARNING,
                        ext,
                        f"Price Plan {rev_pp!r} not in NS; line will fall "
                        f"back to book default.",
                    )

    def check_7_1_price_plan_csv(self):
        """Check 7.1 (WARNING): each line's price plan exists in the pricing CSV.

        Companion to check 7. Check 7 asks "is the price plan already in NS?";
        this asks "is it in the pricing CSV (``PRICE_PLAN_EXTERNAL_ID``), i.e.
        can it be CREATED?". Both use the RAW external id (the pricing CSV and
        the sub column are raw; NS stores the revisioned form). Reading the two
        together: a plan missing from NS (7) but present in the CSV (7.1 clean)
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
                7.1,
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
                7.1,
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
                7.1,
                WARNING,
                ext,
                f"Price Plan {raw_pp!r} referenced by sub line but not in "
                f"the pricing CSV; it cannot be created/pushed to NS.",
            )

    def check_8_addresses(self):
        """Check 8 (BLOCKER): each BA's customer has default bill+ship addresses.

        The BA loader resolves ``billAddressList`` / ``shipAddressList`` from the
        customer's default billing and shipping addresses; if either is missing
        the BA is skipped. So for each BA, this blocks when its NS customer lacks
        a default billing and/or shipping address (per ``customeraddressbook``).
        BAs whose customer wasn't found in NS are skipped here — already flagged
        by check 2.
        """
        for r in self.bas or []:
            ba_ext = (r.get("externalId") or "").strip()
            cust_ext = (r.get("customer_externalId") or "").strip()
            cnum = self.ext2_to_cnum.get(cust_ext)
            cust = self.ns_by_cnum.get(cnum) if cnum else None
            if not cust:
                continue  # flagged by check 2
            cid = cust.get("id")
            missing = []
            if cid not in self.addr_billing:
                missing.append("billing")
            if cid not in self.addr_shipping:
                missing.append("shipping")
            if missing:
                self.add(
                    8,
                    BLOCKER,
                    ba_ext,
                    f"customer {cust_ext} (C-number {cnum}, id {cid}) has no "
                    f"default {' and '.join(missing)} address in NS; BA load skips.",
                )

    def check_9_required_fields(self):
        """Check 9 (BLOCKER): required BA fields are present.

        Guards against silent skips by blocking any BA whose ``subsidiary_id`` or
        ``currency_id`` is blank — both are mandatory NS references the loader
        needs to build a valid payload.
        """
        for r in self.bas or []:
            ba_ext = (r.get("externalId") or "").strip()
            for field in ("subsidiary_id", "currency_id"):
                if not (r.get(field) or "").strip():
                    self.add(9, BLOCKER, ba_ext, f"BA {field} is blank.")

    def check_10_extid_shape(self):
        """Check 10 (WARNING): BA externalId follows the ``<deal_id>_BA`` shape.

        Sanity-checks the naming convention by warning when a BA ``externalId``
        doesn't end in ``_BA``. Since deal alignment (check 6) and the subscription
        loader's shared-BA key both derive the deal id from this shape, drift
        here would quietly misalign BAs and subs. Warning only.
        """
        for r in self.bas or []:
            ba_ext = (r.get("externalId") or "").strip()
            if not ba_ext.endswith("_BA"):
                self.add(
                    10,
                    WARNING,
                    ba_ext,
                    "BA externalId does not end in '_BA' (convention drift).",
                )

    # ── orchestration ─────────────────────────────────────────────────────
    def run(self, entities=None):
        """Prefetch the needed NS data, then run the in-scope checks.

        Args:
            entities: iterable of entity types (e.g. {"subscription"}) to scope
                the run to only the checks guarding them (per CHECK_ENTITY), or
                None to run the whole catalog.

        Only the NS queries required by the active checks are issued — e.g. the
        customer map is fetched only if any of checks 1/2/3/8 are in scope, the
        C-name map only for check 1.1, etc. — each as one batched, chunked
        SuiteQL call. Checks are then dispatched in sorted order so 1.1 runs
        right after 1. Returns ``self.findings`` (also kept on the instance for
        ``report``).
        """
        active_checks = [
            n
            for n in sorted(CHECK_ENTITY)
            if entities is None or CHECK_ENTITY[n] in entities
        ]

        # Prefetch NS data only for the checks we'll run. All customer-touching
        # checks (1, 1.1, 2, 3, 8) now resolve via the single C-number map.
        needs_customers = any(c in active_checks for c in (1, 1.1, 2, 3, 8))
        needs_priceplans = 7 in active_checks
        needs_pricing_csv = 7.1 in active_checks
        needs_addresses = 8 in active_checks

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
                c = self.ext2_to_cnum.get((r.get("customer_externalId") or "").strip())
                if c:
                    cnums.add(c)
            print(
                f"  · querying NS ({_env_label()}) for {len(cnums)} customer "
                f"C-numbers (entityid)…"
            )
            self._fetch_ns_by_cnum(cnums)

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
                    f"  ⚠ pricing CSV not found, skipping check 7.1: "
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
            1.1: self.check_1_1_customer_in_env,
            2: self.check_2_ba_customer,
            3: self.check_3_subsidiary,
            4: self.check_4_startdates,
            5: self.check_5_sales_item,
            6: self.check_6_deal_alignment,
            7: self.check_7_price_plan,
            7.1: self.check_7_1_price_plan_csv,
            8: self.check_8_addresses,
            9: self.check_9_required_fields,
            10: self.check_10_extid_shape,
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
                # A check may emit mixed severities (e.g. 1.1): blocker wins.
                mark = "✗" if any(i.severity == BLOCKER for i in items) else "⚠"
            else:
                mark = "✓"
            print(f"\n[{mark}] Check {n}: {CHECK_TITLE[n]}")
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
