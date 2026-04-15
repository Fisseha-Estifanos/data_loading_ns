#!/usr/bin/env python3
"""
probe_field_ids.py
==================
Runs SuiteQL queries against NS SB3 to resolve internal IDs for
linked-record custom fields needed in the customer patch payload.

Usage:
    python probe_field_ids.py
"""
import sys
from pathlib import Path

_ROOT = str(Path(__file__).resolve().parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import config
from netsuite_client import NetSuiteClient

if not config.CONSUMER_KEY:
    print("ERROR: Credentials not set. Source your .env first:")
    print("  export $(grep -v '^#' .env | xargs)")
    sys.exit(1)

client = NetSuiteClient()

queries = [
    # CUSTOMLIST665 = "Dunning level"
    (
        "Dunning Level values (CUSTOMLIST665)",
        "SELECT id, name FROM customlist665 WHERE name LIKE 'Level 1%'",
    ),
    # CUSTOMLIST669 = "Dunning procedure"
    (
        "Dunning Procedure values (CUSTOMLIST669)",
        "SELECT id, name FROM customlist669 WHERE name LIKE 'Moorepay%'",
    ),
    # Show all values in both lists so we can see the full set
    ("All Dunning Level values", "SELECT id, name FROM customlist665"),
    ("All Dunning Procedure values", "SELECT id, name FROM customlist669"),
]

for label, q in queries:
    print(f"\n{'─'*70}")
    print(f"  {label}")
    print(f"  SQL: {q}")
    print()
    try:
        rows = client.suiteql_query(q)
        if rows:
            for r in rows:
                print(f"    {r}")
        else:
            print("    (no results)")
    except Exception as e:
        print(f"    ERROR: {e}")

print(f"\n{'─'*70}\n")
