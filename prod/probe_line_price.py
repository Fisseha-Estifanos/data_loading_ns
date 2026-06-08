#!/usr/bin/env python3
"""
Probe: how do we actually attach a price to a subscription line?
================================================================
We KNOW the £102 price plan exists (e.g. 1573355) but it never lands on the
subscription line. The loader's `PATCH subscriptionLine {pricePlan:...}` gets a
204 but the price doesn't stick. This script TESTS several candidate ways to
attach it and, after each, READS THE LINE BACK to report whether a price
actually appeared — so we learn the correct REST call empirically instead of
guessing.

Each attempt's write is gated behind --apply. Without --apply it only prints
what it WOULD send (and still does the read-backs, which are harmless GETs).

Use on a sandbox throwaway line only (e.g. sub 59971 line 1) — some attempts
WILL be wrong and may error; that's the point.

Usage
-----
    # list the attempts
    python prod/probe_line_price.py --list

    # dry-run a single attempt (prints payload, does read-back GETs)
    python prod/probe_line_price.py --sub-id 59971 --line 1 \
        --price-plan-id 1573355 --attempt subline_priceinterval_post

    # actually send it
    python prod/probe_line_price.py --sub-id 59971 --line 1 \
        --price-plan-id 1573355 --attempt subline_priceinterval_post --apply

    # run them all in sequence (apply), reading back after each
    python prod/probe_line_price.py --sub-id 59971 --line 1 \
        --price-plan-id 1573355 --attempt all --apply
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


def build_attempts(sub_id, line, pp_id):
    """Candidate ways to attach price plan `pp_id` to subscription line `line`.

    Shapes are modelled on the price-book priceInterval structure we observed
    (chargeType/frequency/startOffset...). They are GUESSES — the read-back is
    what tells us which (if any) actually works.
    """
    base = f"{config.BASE_URL}/subscription/{sub_id}"
    line_num = int(line) if str(line).isdigit() else line
    interval_body = {
        "pricePlan": {"id": str(pp_id)},
        "chargeType": {"id": "2"},          # Recurring (from the book intervals)
        "frequency": {"id": "MONTHLY"},
        "startOffsetValue": 1,
        "startOffsetUnit": {"id": "MONTH"},
    }
    return {
        # The current loader approach — included only to RE-CONFIRM it no-ops.
        "line_pricePlan_field": {
            "method": "PATCH",
            "url": f"{base}/subscriptionLine/{line}",
            "body": {"pricePlan": {"id": str(pp_id)}},
            "note": "current loader method; expected to be ignored",
        },
        # Price interval as a nested sublist on the line.
        "line_priceinterval_nested": {
            "method": "PATCH",
            "url": f"{base}/subscriptionLine/{line}",
            "body": {"priceInterval": {"items": [interval_body]}},
            "note": "priceInterval as nested sublist on the line",
        },
        # Price interval as a child sub-resource you POST to.
        "subline_priceinterval_post": {
            "method": "POST",
            "url": f"{base}/subscriptionLine/{line}/priceInterval",
            "body": interval_body,
            "note": "POST a price interval under the line",
        },
        # Whole-subscription PATCH carrying the line + nested interval.
        "sub_nested_line_interval": {
            "method": "PATCH",
            "url": base,
            "body": {
                "subscriptionLine": {
                    "items": [
                        {"lineNumber": line_num,
                         "priceInterval": {"items": [interval_body]}}
                    ]
                }
            },
            "note": "subscription PATCH with nested line + interval",
        },
    }


def read_back(client, sub_id, line):
    """GET the line (and its priceInterval sub-resource) and report price state."""
    print(f"  {'-'*56}")
    print(f"  READ-BACK of subscription {sub_id} line {line}:")
    url = f"{config.BASE_URL}/subscription/{sub_id}/subscriptionLine/{line}?expandSubResources=true"
    resp = client._request("GET", url)
    if resp.status_code != 200:
        print(f"    {NO} GET line → HTTP {resp.status_code}: {resp.text[:200]}")
    else:
        ln = resp.json()
        has_pp = "pricePlan" in ln
        has_pi = "priceInterval" in ln
        print(f"    line has 'pricePlan' field: {has_pp}  "
              f"value={ln.get('pricePlan')}")
        print(f"    line has 'priceInterval' field: {has_pi}")
        if has_pi:
            print("    priceInterval: " + json.dumps(ln.get("priceInterval"))[:400])
        for k in ("recurringAmount", "amount", "multiplierLineAmount"):
            if k in ln:
                print(f"    {k} = {ln.get(k)}")
    # Also probe the dedicated sub-resource endpoint
    url2 = f"{config.BASE_URL}/subscription/{sub_id}/subscriptionLine/{line}/priceInterval"
    resp2 = client._request("GET", url2)
    if resp2.status_code == 200:
        items = resp2.json().get("items", resp2.json())
        print(f"    {OK} .../priceInterval sub-resource exists: "
              f"{json.dumps(items)[:400]}")
    else:
        print(f"    .../priceInterval sub-resource → HTTP {resp2.status_code}")
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
    code = resp.status_code
    mark = OK if code in (200, 201, 204) else NO
    print(f"  {mark} {spec['method']} → HTTP {code}  {resp.text[:300]}")


def main():
    p = argparse.ArgumentParser(
        description="Probe how to attach a price plan to a subscription line.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--list", action="store_true", help="List attempts and exit")
    p.add_argument("--sub-id", help="Subscription internal ID (e.g. 59971)")
    p.add_argument("--line", help="Line number (e.g. 1)")
    p.add_argument("--price-plan-id", help="Price plan internal ID (e.g. 1573355)")
    p.add_argument("--attempt", help="Attempt name, or 'all'")
    p.add_argument("--apply", action="store_true", help="Actually send writes")
    args = p.parse_args()

    # Build with placeholders just to list names
    attempts = build_attempts(args.sub_id or "SUB", args.line or "LINE",
                              args.price_plan_id or "PP")
    if args.list:
        print("Available attempts:")
        for n, s in attempts.items():
            print(f"  {n:30s} {s['method']:6s} {s['note']}")
        return

    if not (args.sub_id and args.line and args.price_plan_id and args.attempt):
        print(f"{NO} Need --sub-id, --line, --price-plan-id, --attempt "
              f"(or --list).")
        sys.exit(1)

    realm = config.BASE_URL.split("//")[1].split(".")[0]
    print("─" * 64)
    print(f"  NetSuite account : {realm}")
    print(f"  Mode             : {'APPLY (writing)' if args.apply else 'DRY RUN'}")
    print(f"  Target           : sub {args.sub_id} line {args.line}  "
          f"pricePlan {args.price_plan_id}")
    print("─" * 64)

    client = NetSuiteClient()

    # Baseline read-back first
    read_back(client, args.sub_id, args.line)

    to_run = list(attempts) if args.attempt == "all" else [args.attempt]
    for name in to_run:
        if name not in attempts:
            print(f"{NO} Unknown attempt {name!r}. Use --list.")
            continue
        run_attempt(client, name, attempts[name], args.apply)
        read_back(client, args.sub_id, args.line)

    print("\nLook for the attempt after which the read-back shows a real "
          "priceInterval / price. That's the correct mechanism.")


if __name__ == "__main__":
    main()
