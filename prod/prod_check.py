#!/usr/bin/env python3
"""
Prod Readiness Checker (READ-ONLY)
===================================
A safe, read-only toolkit for inspecting a NetSuite account *before* we load
anything into it. It answers the questions you need answered before pushing a
single subscription to prod:

  - Does the customer (e.g. C43837) already exist? What's its internal ID?
  - Is there already a billing account for that customer?
  - Do the price plans / sales items / subscription plan the sub references
    actually exist in this account?

It performs ONLY GET requests and SuiteQL SELECT queries — it never POSTs,
PATCHes, or DELETEs. It cannot create or modify anything in NetSuite.

Which account it hits is entirely determined by the NS_* environment variables
(same as the loader). Point your shell at the prod creds first:

    export $(grep -v '^#' .env.prod | xargs)     # prod creds from 1Password
    python prod/prod_check.py whoami              # confirm which account

Commands
--------
  whoami
      Show which NetSuite account the current env creds point at. ALWAYS run
      this first so you know whether you're looking at prod or sandbox.

  customer  [--entityid C43837] [--externalid ...] [--name "ACME LTD"]
      Find a customer by its C-number (entityid), externalId, or company name.
      Prints internal ID + key fields for every match.

  billing   (--customer-id 12345 | --externalid 495006175463_BA)
      List billing accounts for a customer (by internal ID) or look one up by
      externalId.

  item      --name "MP Payroll Processing"
      Check whether a sales item with that name/itemid exists.

  externalid  --type pricePlan --id MP_PP_27396_..._rvn_02
      Generic GET-by-externalId for any record type (pricePlan, subscription,
      billingAccount, customer, ...).

  sub-readiness  --deal 495006175463 [--c-number C43837]
      The big one. Reads the filtered subscription + customer CSVs (the paths
      in config.py), figures out everything the sub for that deal references,
      and checks each dependency against the live account. Prints a readiness
      matrix so you can see exactly what's already there and what's missing.

Examples
--------
    python prod/prod_check.py whoami
    python prod/prod_check.py customer --entityid C43837
    python prod/prod_check.py sub-readiness --deal 495006175463 --c-number C43837
"""

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

# Put the project root on sys.path so `import config` / `netsuite_client` work
# regardless of where this is run from.
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import config  # noqa: E402
from netsuite_client import NetSuiteClient  # noqa: E402

OK = "\033[92m✓\033[0m"
NO = "\033[91m✗\033[0m"
WARN = "\033[93m⚠\033[0m"


# ─── Read-only SuiteQL helper ────────────────────────────────────────────
# We use our own single-page query (rather than client.suiteql_query, which
# swallows HTTP errors and returns []) so we can tell the difference between
# "query ran, found nothing" and "query itself failed (bad table/column)".


def suiteql(client: NetSuiteClient, query: str) -> tuple[bool, list, str]:
    """Run one SuiteQL SELECT. Returns (ok, rows, error_text). Read-only."""
    q = query.strip()
    if not q.lower().startswith("select"):
        # Hard guard — this tool only ever issues SELECTs.
        raise ValueError(f"Refusing non-SELECT SuiteQL: {q[:60]!r}")
    url = f"{client.suiteql_url}?limit=1000&offset=0"
    headers = client._headers("POST", url)
    headers["Prefer"] = "transient"
    resp = client.session.post(url, headers=headers, json={"q": q}, timeout=60)
    if resp.status_code != 200:
        return (False, [], f"HTTP {resp.status_code}: {resp.text[:300]}")
    return (True, resp.json().get("items", []), "")


def sql_quote(value: str) -> str:
    """Escape a string literal for SuiteQL (double single-quotes)."""
    return value.replace("'", "''")


# ─── Environment banner ───────────────────────────────────────────────────


def show_environment() -> str:
    """Print which account the env points at. Returns the raw realm string."""
    realm_raw = config.BASE_URL.split("//")[1].split(".")[0]
    looks_sandbox = "sb" in realm_raw.lower() or "-sb" in realm_raw.lower()
    tag = f"{WARN} SANDBOX" if looks_sandbox else "PRODUCTION (no 'sb' in realm)"
    print("─" * 64)
    print(f"  NetSuite account : {realm_raw}")
    print(f"  Base URL         : {config.BASE_URL}")
    print(f"  State DB         : {config.STATE_DB}")
    print(f"  Looks like       : {tag}")
    print("─" * 64)
    if not config.CONSUMER_KEY:
        print(f"{NO} No credentials in env. Source your .env first.")
    return realm_raw


# ─── Lookups ───────────────────────────────────────────────────────────────


def find_customer(client, entityid=None, externalid=None, name=None) -> list:
    """Find customers by entityid (C-number), externalId, or company name."""
    conds = []
    if entityid:
        conds.append(f"entityid = '{sql_quote(entityid)}'")
    if externalid:
        conds.append(f"externalid = '{sql_quote(externalid)}'")
    if name:
        conds.append(f"UPPER(companyname) = '{sql_quote(name.upper())}'")
    if not conds:
        print(f"{NO} Provide at least one of --entityid / --externalid / --name")
        return []
    where = " OR ".join(conds)
    query = (
        "SELECT id, entityid, companyname, externalid, isinactive "
        f"FROM customer WHERE {where}"
    )
    ok, rows, err = suiteql(client, query)
    if not ok:
        print(f"{NO} Customer query failed: {err}")
        return []
    if not rows:
        print(f"{NO} No customer found for: {where}")
        return []
    for r in rows:
        flag = f" {WARN} INACTIVE" if str(r.get("isinactive")) == "T" else ""
        print(
            f"{OK} customer id={r.get('id')}  "
            f"entityid={r.get('entityid')}  "
            f"company={r.get('companyname')!r}  "
            f"externalId={r.get('externalid')}{flag}"
        )
    return rows


def find_billing(client, customer_id=None, externalid=None) -> list:
    """List billing accounts by customer internal ID, or look one up by extId."""
    if externalid:
        where = f"externalid = '{sql_quote(externalid)}'"
    elif customer_id:
        where = f"customer = '{sql_quote(str(customer_id))}'"
    else:
        print(f"{NO} Provide --customer-id or --externalid")
        return []
    query = (
        "SELECT id, name, externalid, customer, frequency, startdate, inactive "
        f"FROM billingaccount WHERE {where}"
    )
    ok, rows, err = suiteql(client, query)
    if not ok:
        print(f"{NO} Billing account query failed: {err}")
        return []
    if not rows:
        print(f"{NO} No billing account found for: {where}")
        return []
    for r in rows:
        print(
            f"{OK} billingAccount id={r.get('id')}  "
            f"name={r.get('name')!r}  "
            f"externalId={r.get('externalid')}  "
            f"customer={r.get('customer')}  start={r.get('startdate')}"
        )
    return rows


def find_item(client, name: str) -> list:
    """Check whether a sales item exists by itemid / displayname / fullname."""
    n = sql_quote(name)
    query = (
        "SELECT id, itemid, displayname, fullname, itemtype, isinactive "
        f"FROM item WHERE itemid = '{n}' OR displayname = '{n}' OR fullname = '{n}'"
    )
    ok, rows, err = suiteql(client, query)
    if not ok:
        print(f"{NO} Item query failed: {err}")
        return []
    if not rows:
        print(f"{NO} No item found matching {name!r}")
        return []
    for r in rows:
        flag = f" {WARN} INACTIVE" if str(r.get("isinactive")) == "T" else ""
        print(
            f"{OK} item id={r.get('id')}  itemid={r.get('itemid')!r}  "
            f"type={r.get('itemtype')}  display={r.get('displayname')!r}{flag}"
        )
    return rows


def find_by_externalid(client, record_type: str, external_id: str):
    """Generic GET-by-externalId for any record type."""
    rec = client.get_by_external_id(record_type, external_id)
    if rec and "id" in rec:
        print(f"{OK} {record_type} externalId={external_id} → internal id={rec['id']}")
        return rec
    print(f"{NO} No {record_type} found with externalId={external_id}")
    return None


def check_address(client, customer_id: str) -> list:
    """
    Show a customer's address book entries and whether any are flagged as
    default billing / default shipping. The billing-account loader needs a
    default billing AND a default shipping address for the customer, or it
    skips the record — so this is the de-risk check before loading a BA.
    """
    cid = sql_quote(str(customer_id))
    query = (
        "SELECT internalid, defaultbilling, defaultshipping "
        f"FROM customeraddressbook WHERE entity = '{cid}'"
    )
    ok, rows, err = suiteql(client, query)
    if not ok:
        print(f"{NO} Address book query failed: {err}")
        return []
    if not rows:
        print(f"{NO} Customer {customer_id} has NO address book entries — "
              "the billing-account load will FAIL (needs default bill + ship).")
        return []
    has_bill = any(str(r.get("defaultbilling")).upper() == "T" for r in rows)
    has_ship = any(str(r.get("defaultshipping")).upper() == "T" for r in rows)
    for r in rows:
        b = "billing" if str(r.get("defaultbilling")).upper() == "T" else ""
        s = "shipping" if str(r.get("defaultshipping")).upper() == "T" else ""
        flags = "+".join(filter(None, [b, s])) or "(neither default)"
        print(f"  address internalid={r.get('internalid')}  default={flags}")
    print(f"{OK if has_bill else NO} default BILLING address present: {has_bill}")
    print(f"{OK if has_ship else NO} default SHIPPING address present: {has_ship}")
    if not (has_bill and has_ship):
        print(f"{WARN} BA load needs BOTH. Set the missing default(s) on the "
              f"customer's address book in NS, then re-run.")
    return rows


def _resolve_internal_id(client, record_type, internal_id, external_id):
    """Return an internal id from either --id or --externalid (GET by eid)."""
    if internal_id:
        return str(internal_id)
    rec = client.get_by_external_id(record_type, external_id)
    if rec and "id" in rec:
        return str(rec["id"])
    print(f"{NO} No {record_type} found with externalId={external_id}")
    return None


def dump_subscription(client, internal_id=None, external_id=None) -> None:
    """
    GET a subscription with its lines and print, per line: include flag,
    quantity, and price plan — the fields that decide whether the sub shows
    a price. This is the recon for 'no price on the subscription'.
    """
    sid = _resolve_internal_id(client, "subscription", internal_id, external_id)
    if not sid:
        return
    url = f"{config.BASE_URL}/subscription/{sid}?expandSubResources=true"
    resp = client._request("GET", url)
    if resp.status_code != 200:
        print(f"{NO} GET subscription/{sid} → HTTP {resp.status_code}: {resp.text[:300]}")
        return
    sub = resp.json()
    pb = sub.get("priceBook", {})
    print(f"  subscription id={sub.get('id')}  name={sub.get('name')!r}  "
          f"externalId={sub.get('externalId')}")
    print(f"  status={sub.get('billingSubscriptionStatus', {}).get('refName')}  "
          f"priceBook={pb.get('refName')!r} (id={pb.get('id')})")
    print(f"  activeTotalContractValue={sub.get('activeTotalContractValue')}  "
          f"pendingTotalContractValue={sub.get('pendingTotalContractValue')}")
    lines = sub.get("subscriptionLine", {}).get("items", [])
    print(f"  {len(lines)} line(s):")
    for ln in lines:
        item = ln.get("item", {}).get("refName")
        pp = ln.get("pricePlan", {})
        print(
            f"    line {ln.get('lineNumber')}: item={item!r}  "
            f"isIncluded={ln.get('isIncluded')}  "
            f"qty={ln.get('quantity')}  "
            f"recurringAmt={ln.get('recurringAmount')}  "
            f"pricePlan id={pp.get('id')} {pp.get('refName', '')!r}"
        )


def dump_subline(client, sub_id: str, line_num: str) -> None:
    """
    Raw GET of one subscription line with everything expanded — shows where the
    price actually lives (priceInterval / pricePlan), which the summary view
    can't. Use this to confirm whether a line-level pricePlan PATCH stuck.
    """
    import json as _json
    url = (
        f"{config.BASE_URL}/subscription/{sub_id}/subscriptionLine/{line_num}"
        "?expandSubResources=true"
    )
    resp = client._request("GET", url)
    if resp.status_code != 200:
        print(f"{NO} GET subscriptionLine/{line_num} → HTTP {resp.status_code}: {resp.text[:300]}")
        return
    print(_json.dumps(resp.json(), indent=2))


def dump_sub_intervals(client, sub_id: str) -> None:
    """
    Dump the subscription's priceInterval collection — the sublist that ACTUALLY
    holds line prices (keyed by subscriptionPlanLineNumber), separate from
    subscriptionLine. Shows whether NS auto-created intervals (likely £0 from
    the book) that we'd PATCH, vs. an empty collection we'd POST into.
    """
    import json as _json
    url = f"{config.BASE_URL}/subscription/{sub_id}/priceInterval?expandSubResources=true"
    resp = client._request("GET", url)
    if resp.status_code != 200:
        print(f"{NO} GET subscription/{sub_id}/priceInterval → HTTP {resp.status_code}: {resp.text[:300]}")
        return
    data = resp.json()
    items = data.get("items", [])
    print(f"  {len(items)} price interval(s) on subscription {sub_id}:")
    for it in items:
        pp = it.get("pricePlan", {})
        item = it.get("item", {})
        print(
            f"    interval id={it.get('id')}  "
            f"planLineNo={it.get('subscriptionPlanLineNumber')}  "
            f"lineNo={it.get('lineNumber')}  "
            f"item={item.get('refName')!r}  "
            f"chargeType={it.get('chargeType', {}).get('refName')}  "
            f"pricePlan id={pp.get('id')}  "
            f"recurringAmt={it.get('recurringAmount')}  "
            f"status={it.get('status', {}).get('refName')}"
        )
    print("\n  Full JSON of first interval (for field shape):")
    if items:
        print(_json.dumps(items[0], indent=2))


def dump_record(client, record_type: str, internal_id=None, external_id=None) -> None:
    """Raw GET + pretty-print any record (e.g. pricePlan) for inspection."""
    rid = _resolve_internal_id(client, record_type, internal_id, external_id)
    if not rid:
        return
    url = f"{config.BASE_URL}/{record_type}/{rid}?expandSubResources=true"
    resp = client._request("GET", url)
    if resp.status_code != 200:
        print(f"{NO} GET {record_type}/{rid} → HTTP {resp.status_code}: {resp.text[:300]}")
        return
    import json as _json
    print(_json.dumps(resp.json(), indent=2))


# ─── Subscription readiness ──────────────────────────────────────────────


def _read_csv(path: str) -> list[dict]:
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        print(f"{NO} CSV not found: {path}")
        return []


def sub_readiness(client, deal_id: str, c_number: str = None) -> None:
    """
    Read the subscription + customer CSVs for `deal_id` and check every
    dependency the subscription loader would need against the live account.
    """
    print(f"\n{'=' * 64}\n  SUB READINESS — deal {deal_id}\n{'=' * 64}")

    sub_rows_all = _read_csv(config.SUBSCRIPTIONS_CSV)
    group = [r for r in sub_rows_all if r.get("External ID", "").strip() == deal_id]
    if not group:
        print(f"{NO} Deal {deal_id} not found in {config.SUBSCRIPTIONS_CSV}")
        print(
            "    Make sure the filtered subscription CSV is in place and "
            "config.SUBSCRIPTIONS_CSV points at it."
        )
        return
    print(f"  Found {len(group)} CSV row(s) for this deal.\n")

    header = group[0]
    customer_name = header.get("Customer", "").strip()

    # Map company name → External ID 2 from the customer CSV (loader's chain).
    cust_csv = _read_csv(config.CUSTOMERS_CSV)
    name_to_extid = {
        r.get("Company Name", "").strip().upper(): r.get("External ID 2", "").strip()
        for r in cust_csv
        if r.get("Company Name", "").strip() and r.get("External ID 2", "").strip()
    }
    customer_extid = name_to_extid.get(customer_name.upper())

    # 1) Customer
    print("── 1. CUSTOMER " + "─" * 49)
    print(f"  CSV says customer name: {customer_name!r}")
    print(
        f"  Customer CSV External ID 2: {customer_extid or '(not found in customer CSV)'}"
    )
    if c_number:
        print(f"  Checking by C-number {c_number} ...")
        find_customer(client, entityid=c_number)
    if customer_extid:
        print(f"  Checking by company name + (revisioned) externalId ...")
        find_customer(
            client,
            externalid=config.apply_revision(customer_extid),
            name=customer_name,
        )
    else:
        find_customer(client, name=customer_name)
    print("  → If the customer already exists, note its internal id: you'll seed")
    print("    the prod state DB with it so the sub links to the right record.")

    # 2) Billing account
    print("\n── 2. BILLING ACCOUNT " + "─" * 42)
    ba_extid = config.apply_revision(f"{deal_id}_BA")
    print(f"  Loader would look for billingAccount externalId={ba_extid}")
    find_by_externalid(client, "billingAccount", ba_extid)
    print("  (Also check by customer once you know the customer internal id:")
    print("    python prod/prod_check.py billing --customer-id <id>)")

    # 3) Price plans referenced by the lines
    print("\n── 3. PRICE PLANS (per line) " + "─" * 35)
    pp_seen = set()
    for r in group:
        pp = (r.get("Price Plan External ID") or "").strip()
        if pp and pp not in pp_seen:
            pp_seen.add(pp)
    if not pp_seen:
        print(
            f"  {WARN} No 'Price Plan External ID' values in the CSV rows "
            "(lines would use plan default rate)."
        )
    for pp in sorted(pp_seen):
        find_by_externalid(client, "pricePlan", config.apply_revision(pp))

    # 4) Sales items referenced by the lines
    print("\n── 4. SALES ITEMS (per line) " + "─" * 35)
    items = set()
    for r in group:
        if r.get("Lines: Include", "").strip() != "T":
            continue
        raw = r.get("Sales Item", "").strip()
        if raw and raw != "NOT MAPPED":
            for part in raw.split(","):
                nm = part.strip().replace("​", "")
                if nm:
                    items.add(nm)
    if not items:
        print(f"  {WARN} No included sales items found (Lines: Include = T).")
    for nm in sorted(items):
        find_item(client, nm)

    # 5) Subscription plan / price book (refName lookups in the loader)
    print("\n── 5. SUBSCRIPTION PLAN / PRICE BOOK " + "─" * 27)
    sub_plan = next(
        (
            r.get("Subscription Plan", "").strip()
            for r in group
            if r.get("Subscription Plan", "").strip()
        ),
        "",
    )
    price_book = next(
        (
            r.get("Price Book", "").strip()
            for r in group
            if r.get("Price Book", "").strip() not in ("", "NOT MAPPED")
        ),
        "",
    )
    print(f"  Subscription Plan (refName): {sub_plan or '(none)'}")
    if sub_plan:
        ok, rows, err = suiteql(
            client,
            f"SELECT id, name FROM subscriptionplan WHERE name = '{sql_quote(sub_plan)}'",
        )
        if ok and rows:
            for r in rows:
                print(f"    {OK} subscriptionPlan id={r['id']} name={r['name']!r}")
        elif ok:
            print(f"    {NO} No subscriptionPlan named {sub_plan!r}")
        else:
            print(f"    {WARN} Could not verify subscriptionPlan ({err})")
    print(f"  Price Book (refName): {price_book or '(none)'}")

    print(f"\n{'=' * 64}")
    print("  Readiness check complete. Resolve every ✗ before loading the sub.")
    print(f"{'=' * 64}\n")


# ─── CLI ────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Read-only NetSuite prod readiness checker.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("whoami", help="Show which NS account the env points at")

    p_cust = sub.add_parser("customer", help="Find a customer")
    p_cust.add_argument("--entityid", help="C-number, e.g. C43837")
    p_cust.add_argument("--externalid")
    p_cust.add_argument("--name", help="Company name")

    p_ba = sub.add_parser("billing", help="Find billing account(s)")
    p_ba.add_argument("--customer-id", help="Customer internal ID")
    p_ba.add_argument("--externalid")

    p_item = sub.add_parser("item", help="Check a sales item exists")
    p_item.add_argument("--name", required=True)

    p_eid = sub.add_parser("externalid", help="GET any record by externalId")
    p_eid.add_argument("--type", required=True, help="e.g. pricePlan, subscription")
    p_eid.add_argument("--id", required=True, help="the externalId")

    p_sub = sub.add_parser("sub-readiness", help="Full readiness check for a deal")
    p_sub.add_argument("--deal", required=True, help="Deal ID (subs External ID)")
    p_sub.add_argument("--c-number", help="Existing customer C-number, e.g. C43837")

    p_addr = sub.add_parser(
        "address", help="Show a customer's address book (default bill/ship)"
    )
    p_addr.add_argument("--customer-id", required=True, help="Customer internal ID")

    p_subdump = sub.add_parser(
        "subscription", help="Dump a subscription's lines (qty, price plan, amounts)"
    )
    p_subdump.add_argument("--id", help="Subscription internal ID")
    p_subdump.add_argument("--externalid", help="Subscription externalId")

    p_rec = sub.add_parser("record", help="Raw GET + JSON dump of any record")
    p_rec.add_argument("--type", required=True, help="e.g. pricePlan, billingAccount")
    p_rec.add_argument("--id", help="Internal ID")
    p_rec.add_argument("--externalid", help="externalId")

    p_sl = sub.add_parser(
        "subline", help="Raw dump of one subscription line (shows priceInterval/pricePlan)"
    )
    p_sl.add_argument("--sub-id", required=True, help="Subscription internal ID")
    p_sl.add_argument("--line", required=True, help="Line number, e.g. 1")

    p_si = sub.add_parser(
        "sub-intervals", help="Dump the subscription's priceInterval collection (where prices live)"
    )
    p_si.add_argument("--sub-id", required=True, help="Subscription internal ID")

    args = parser.parse_args()

    realm = show_environment()
    if not config.CONSUMER_KEY:
        sys.exit(1)

    client = NetSuiteClient()

    if args.command == "whoami":
        return
    elif args.command == "customer":
        find_customer(client, args.entityid, args.externalid, args.name)
    elif args.command == "billing":
        find_billing(client, args.customer_id, args.externalid)
    elif args.command == "item":
        find_item(client, args.name)
    elif args.command == "externalid":
        find_by_externalid(client, args.type, args.id)
    elif args.command == "sub-readiness":
        sub_readiness(client, args.deal, args.c_number)
    elif args.command == "address":
        check_address(client, args.customer_id)
    elif args.command == "subscription":
        dump_subscription(client, args.id, args.externalid)
    elif args.command == "record":
        dump_record(client, args.type, args.id, args.externalid)
    elif args.command == "subline":
        dump_subline(client, args.sub_id, args.line)
    elif args.command == "sub-intervals":
        dump_sub_intervals(client, args.sub_id)


if __name__ == "__main__":
    main()
