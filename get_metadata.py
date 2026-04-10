"""
NS Metadata Catalog Fetcher
============================
Fetches the REST schema for a record type, extracts all custom fields
(custentity_xxx), and prints their script ID + label.

Usage:
    python get_metadata.py customer
    python get_metadata.py subscription
    python get_metadata.py billingaccount
"""

import os
import sys
import json
from pathlib import Path

# Load .env
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip().removeprefix("export").strip()
        os.environ.setdefault(key, val.strip().strip('"').strip("'"))

sys.path.insert(0, str(Path(__file__).parent))
from netsuite_client import NetSuiteClient
import config

record_type = sys.argv[1] if len(sys.argv) > 1 else "customer"
url = f"{config.BASE_URL}/metadata-catalog/{record_type}"

print(f"Fetching schema: GET {url}\n")

client = NetSuiteClient()
resp = client._request("GET", url, retries=1)

if resp.status_code != 200:
    print(f"ERROR {resp.status_code}: {resp.text[:500]}")
    sys.exit(1)

schema = resp.json()

# Extract all properties from the schema
properties = schema.get("properties", {})

custom = {
    k: v
    for k, v in properties.items()
    if k.startswith("custentity") or k.startswith("cseg_")
}
standard = {
    k: v
    for k, v in properties.items()
    if not k.startswith("custentity") and not k.startswith("cseg_")
}

print(f"=== Custom fields ({len(custom)}) ===")
for script_id, field_def in sorted(custom.items()):
    label = field_def.get("title", "—")
    field_type = field_def.get("type", field_def.get("$ref", "—").split("/")[-1])
    print(f"  {script_id:<45} {label:<50} [{field_type}]")

print(f"\n=== Standard fields ({len(standard)}) ===")
for script_id, field_def in sorted(standard.items()):
    label = field_def.get("title", "—")
    print(f"  {script_id:<45} {label}")

print(f"\nFull schema saved to: metadata_{record_type}.json")
Path(f"metadata_{record_type}.json").write_text(json.dumps(schema, indent=2))
