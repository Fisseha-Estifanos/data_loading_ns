"""
Auth Debug Script
=================
Loads credentials directly from .env and makes a minimal GET request
to the NS customer endpoint. Prints the full request and response so
you can compare against Postman.

Usage:
    python debug_auth.py
"""

import base64
import hashlib
import hmac
import sys
import time
import urllib.parse
import uuid

import requests

# ── Step 1: Load credentials from .env directly ──────────────────────────────


def load_dotenv(path=".env"):
    creds = {}
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip().removeprefix("export").strip()
                creds[key] = val.strip().strip('"').strip("'")
    except FileNotFoundError:
        print(f"ERROR: {path} not found")
        sys.exit(1)
    return creds


env = load_dotenv()

CONSUMER_KEY = env.get("NS_CONSUMER_KEY", "")
CONSUMER_SECRET = env.get("NS_CONSUMER_SECRET", "")
ACCESS_TOKEN = env.get("NS_ACCESS_TOKEN", "")
TOKEN_SECRET = env.get("NS_TOKEN_SECRET", "")
_realm_raw = env.get("NS_REALM", "4874529-sb3")
# NetSuite OAuth realm must use uppercase + underscores (e.g. 4874529_SB3), not the URL form
REALM = _realm_raw.upper().replace("-", "_")


# Mask values for safe printing
def mask(s):
    return s[:6] + "..." + s[-4:] if len(s) > 12 else "***"


print("── Credentials loaded from .env ─────────────────────────")
print(f"  CONSUMER_KEY    : {mask(CONSUMER_KEY)}")
print(f"  CONSUMER_SECRET : {mask(CONSUMER_SECRET)}")
print(f"  ACCESS_TOKEN    : {mask(ACCESS_TOKEN)}")
print(f"  TOKEN_SECRET    : {mask(TOKEN_SECRET)}")
print(f"  REALM           : {REALM}")

missing = [
    k
    for k, v in {
        "NS_CONSUMER_KEY": CONSUMER_KEY,
        "NS_CONSUMER_SECRET": CONSUMER_SECRET,
        "NS_ACCESS_TOKEN": ACCESS_TOKEN,
        "NS_TOKEN_SECRET": TOKEN_SECRET,
    }.items()
    if not v
]

if missing:
    print(f"\nERROR: missing values in .env: {missing}")
    sys.exit(1)

print()

# ── Step 2: Build OAuth 1.0 header ───────────────────────────────────────────

METHOD = "GET"
URL = (
    f"https://{_realm_raw}.suitetalk.api.netsuite.com/services/rest/record/v1/customer"
)
PARAMS = {"limit": "1"}  # query params

timestamp = str(int(time.time()))
nonce = uuid.uuid4().hex

oauth_params = {
    "oauth_consumer_key": CONSUMER_KEY,
    "oauth_token": ACCESS_TOKEN,
    "oauth_signature_method": "HMAC-SHA256",
    "oauth_timestamp": timestamp,
    "oauth_nonce": nonce,
    "oauth_version": "1.0",
}

# Merge OAuth params + query params for signature base string
all_params = {**oauth_params, **PARAMS}
param_string = "&".join(
    f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}"
    for k, v in sorted(all_params.items())
)

base_string = "&".join(
    [
        METHOD.upper(),
        urllib.parse.quote(URL, safe=""),
        urllib.parse.quote(param_string, safe=""),
    ]
)

signing_key = "&".join(
    [
        urllib.parse.quote(CONSUMER_SECRET, safe=""),
        urllib.parse.quote(TOKEN_SECRET, safe=""),
    ]
)

signature = base64.b64encode(
    hmac.new(
        signing_key.encode("utf-8"),
        base_string.encode("utf-8"),
        hashlib.sha256,
    ).digest()
).decode("utf-8")

auth_header = (
    f'OAuth realm="{REALM}",'
    f'oauth_consumer_key="{CONSUMER_KEY}",'
    f'oauth_token="{ACCESS_TOKEN}",'
    f'oauth_signature_method="HMAC-SHA256",'
    f'oauth_timestamp="{timestamp}",'
    f'oauth_nonce="{nonce}",'
    f'oauth_version="1.0",'
    f'oauth_signature="{urllib.parse.quote(signature, safe="")}"'
)

print("── OAuth header (truncated for display) ─────────────────")
print(f"  {auth_header[:120]}...")
print()

# ── Step 3: Fire the request ─────────────────────────────────────────────────

headers = {
    "Authorization": auth_header,
    "Content-Type": "application/json",
    "Accept": "application/json",
}

full_url = URL + "?" + urllib.parse.urlencode(PARAMS)
print(f"── GET {full_url}")
print()

resp = requests.get(full_url, headers=headers, timeout=30)

print(f"── Response ─────────────────────────────────────────────")
print(f"  Status : {resp.status_code}")
print(f"  Headers: {dict(resp.headers)}")
print()
print(f"  Body:")
print(resp.text[:2000])
