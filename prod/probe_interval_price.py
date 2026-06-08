#!/usr/bin/env python3
"""
Probe: how do we set the PRICE on a subscription's auto-created price interval?
==============================================================================
We now know: when NS creates a subscription from a plan + price book, it
auto-builds one price interval per line, each pointing at its OWN auto-generated
price plan copied from the (£0 placeholder) book. The line price comes from that
interval, NOT from a price plan bolted onto the subscriptionLine, and NOT from
our standalone MP_PP_ plan (which ends up orphaned).

So the fix is to set the correct amount on the existing interval. This script
tests the candidate ways and reads the interval back after each so we learn the
one that actually sticks.

Each write is gated behind --apply. Read-backs are harmless GETs.
Use on a sandbox throwaway sub (e.g. 59971).

The interval is addressed by its COMPOSITE KEY, e.g.
  subscriptionPlanLineNumber=1,startDate=2026-05-31
(get it from `prod_check.py sub-intervals --sub-id <id>` — the self link).

Usage
-----
    python prod/probe_interval_price.py --list

    python prod/probe_interval_price.py --sub-id 59971 \
        --interval-key "subscriptionPlanLineNumber=1,startDate=2026-05-31" \
        --our-plan-id 1573355 --autogen-plan-id 1573555 --amount 102 \
        --attempt all --apply
"""
import argparse
import json
import sys
from pathlib import Path

_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import config  # noqa: E402
from netsuite_client import NetSuiteClient  # noqa: E402

OK = "\033[92m✓\033[0m"
NO = "\033[91m✗\033[0m"
WARN = "\033[93m⚠\033[0m"


def build_attempts(sub_id, interval_key, our_plan_id, autogen_plan_id, amount):
    interval_url = f"{config.BASE_URL}/subscription/{sub_id}/priceInterval/{interval_key}"
    amt = float(amount)
    attempts = {
        # 1) Re-point the interval at our pre-built £102 plan.
        "interval_repoint_our_plan": {
            "method": "PATCH",
            "url": interval_url,
            "body": {"pricePlan": {"id": str(our_plan_id)}},
            "note": "point interval.pricePlan at our loaded plan",
        },
        # 2) Set the amount straight on the interval.
        "interval_set_recurring_amount": {
            "method": "PATCH",
            "url": interval_url,
            "body": {"recurringAmount": amt},
            "note": "set interval.recurringAmount directly",
        },
    }
    # 3) Fix the auto-generated plan the interval already points at (Fixed Amount,
    #    mirroring the book's own plans).
    if autogen_plan_id:
        attempts["autogen_plan_set_value"] = {
            "method": "PATCH",
            "url": f"{config.BASE_URL}/pricePlan/{autogen_plan_id}",
            "body": {
                "pricePlanType": {"id": "2"},  # Tiered, like the book
                "priceTiers": {
                    "items": [
                        {"fromVal": 0,
                         "pricingOption": {"id": "-102"},  # Fixed Amount
                         "value": amt}
                    ]
                },
            },
            "note": "set the interval's auto-generated plan tier to the amount",
        }
    return attempts


def read_back(client, sub_id, interval_key, autogen_plan_id):
    print(f"  {'-'*56}")
    url = f"{config.BASE_URL}/subscription/{sub_id}/priceInterval/{interval_key}?expandSubResources=true"
    resp = client._request("GET", url)
    if resp.status_code != 200:
        print(f"    {NO} GET interval → HTTP {resp.status_code}: {resp.text[:200]}")
    else:
        iv = resp.json()
        pp = iv.get("pricePlan", {})
        print(f"    interval: pricePlan id={pp.get('id')}  "
              f"recurringAmount={iv.get('recurringAmount')}  "
              f"totalintervalvalue={iv.get('totalintervalvalue')}  "
              f"status={iv.get('status', {}).get('refName')}")
    if autogen_plan_id:
        r2 = client._request("GET", f"{config.BASE_URL}/pricePlan/{autogen_plan_id}?expandSubResources=true")
        if r2.status_code == 200:
            tiers = r2.json().get("priceTiers", {}).get("items", [])
            vals = [t.get("value") for t in tiers]
            print(f"    autogen plan {autogen_plan_id} tier values: {vals}")
    print(f"  {'-'*56}")


def run_attempt(client, name, spec, apply):
    print(f"\n{'='*64}")
    print(f"  ATTEMPT: {name}  ({spec['note']})")
    print(f"  {spec['method']} {spec['url'].split('/v1/')[-1]}")
    print(f"  body: {json.dumps(spec['body'])}")
    if not apply:
        print(f"  {WARN} DRY RUN — not sending.")
        return
    resp = client._request(spec["method"], spec["url"], spec["body"])
    mark = OK if resp.status_code in (200, 201, 204) else NO
    print(f"  {mark} → HTTP {resp.status_code}  {resp.text[:300]}")


def main():
    p = argparse.ArgumentParser(
        description="Probe how to set the price on a subscription price interval.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--list", action="store_true")
    p.add_argument("--sub-id")
    p.add_argument("--interval-key", help="e.g. subscriptionPlanLineNumber=1,startDate=2026-05-31")
    p.add_argument("--our-plan-id", help="our pre-built price plan id (e.g. 1573355)")
    p.add_argument("--autogen-plan-id", help="the interval's auto-generated plan id (e.g. 1573555)")
    p.add_argument("--amount", default="0", help="the price to set (e.g. 102)")
    p.add_argument("--attempt", help="attempt name, or 'all'")
    p.add_argument("--apply", action="store_true")
    args = p.parse_args()

    attempts = build_attempts(args.sub_id or "SUB", args.interval_key or "KEY",
                              args.our_plan_id or "OUR", args.autogen_plan_id,
                              args.amount)
    if args.list:
        print("Available attempts:")
        for n, s in attempts.items():
            print(f"  {n:32s} {s['method']:6s} {s['note']}")
        return

    if not (args.sub_id and args.interval_key and args.attempt):
        print(f"{NO} Need --sub-id, --interval-key, --attempt (and --our-plan-id "
              f"/ --autogen-plan-id / --amount as relevant). Or --list.")
        sys.exit(1)

    realm = config.BASE_URL.split("//")[1].split(".")[0]
    print("─" * 64)
    print(f"  NetSuite account : {realm}")
    print(f"  Mode             : {'APPLY (writing)' if args.apply else 'DRY RUN'}")
    print(f"  Target interval  : sub {args.sub_id} / {args.interval_key}")
    print("─" * 64)

    client = NetSuiteClient()
    read_back(client, args.sub_id, args.interval_key, args.autogen_plan_id)

    to_run = list(attempts) if args.attempt == "all" else [args.attempt]
    for name in to_run:
        if name not in attempts:
            print(f"{NO} Unknown attempt {name!r}. Use --list.")
            continue
        run_attempt(client, name, attempts[name], args.apply)
        read_back(client, args.sub_id, args.interval_key, args.autogen_plan_id)

    print("\nThe attempt after which recurringAmount / totalintervalvalue / tier "
          "value becomes the amount is the correct mechanism.")


if __name__ == "__main__":
    main()
