"""
SuiteQL Query Runner
=====================
Run any SuiteQL query against the NS sandbox and print results as a table.
Credentials are loaded from .env automatically.

Usage:
    python suiteql.py "SELECT id, name FROM term WHERE name LIKE '%Z030%'"
    python suiteql.py "SELECT scriptid, label FROM customfield WHERE recordtype = 'ENTITY' ORDER BY label"
    python suiteql.py "SELECT id, name FROM subscriptionplan"
    python suiteql.py "SELECT id, itemid, displayname FROM item WHERE isinactive = 'F'"
"""

import os
import sys
from pathlib import Path

# Load .env (export KEY=value format)
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip().removeprefix("export").strip()
        os.environ.setdefault(key, val.strip().strip('"').strip("'"))

# Now safe to import project modules
sys.path.insert(0, str(Path(__file__).parent))
from netsuite_client import NetSuiteClient

if len(sys.argv) < 2:
    print(__doc__)
    sys.exit(1)

query = " ".join(sys.argv[1:])
print(f"Query: {query}\n")

client = NetSuiteClient()
rows = client.suiteql_query(query)

if not rows:
    print("No results.")
    sys.exit(0)

# Print as aligned table
headers = list(rows[0].keys())
col_widths = [max(len(h), max(len(str(r.get(h, ""))) for r in rows)) for h in headers]

header_row = "  ".join(h.ljust(w) for h, w in zip(headers, col_widths))
divider = "  ".join("-" * w for w in col_widths)
print(header_row)
print(divider)
for row in rows:
    print("  ".join(str(row.get(h, "")).ljust(w) for h, w in zip(headers, col_widths)))

print(f"\n{len(rows)} row(s)")
