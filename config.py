"""
NetSuite REST API Configuration
================================
Credentials come from environment variables (sourced from .env).
"""

import os

# --- OAuth 1.0 Credentials ---
CONSUMER_KEY = os.environ.get("NS_CONSUMER_KEY", "")
CONSUMER_SECRET = os.environ.get("NS_CONSUMER_SECRET", "")
ACCESS_TOKEN = os.environ.get("NS_ACCESS_TOKEN", "")
TOKEN_SECRET = os.environ.get("NS_TOKEN_SECRET", "")

# NS_REALM in .env is the URL account ID format, e.g. "4874529-sb3".
# The OAuth Authorization header requires uppercase + underscores: "4874529_SB3".
_realm_raw = os.environ.get("NS_REALM", "4874529-sb3")
REALM = _realm_raw.upper().replace("-", "_")  # used in OAuth header only

# --- NetSuite API URLs (use original _realm_raw for the hostname) ---
BASE_URL = f"https://{_realm_raw}.suitetalk.api.netsuite.com/services/rest/record/v1"
SUITEQL_URL = (
    f"https://{_realm_raw}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"
)

# --- Data File Paths ---
CUSTOMERS_CSV = "data/customers-kleene-export-2026-04-09.csv"
BILLING_CSV = "data/billing-kleene-export-2026-04-14.csv"
SUBSCRIPTIONS_CSV = "data/not-set.csv"
ONEOFF_CSV = "data/not-set.csv"

# --- State Tracking ---
STATE_DB = "state/load_state.db"

# --- Retry / Rate Limit ---
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5
REQUEST_DELAY_SECONDS = 0.5
