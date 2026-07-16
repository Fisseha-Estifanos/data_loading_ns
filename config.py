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
_realm_raw = os.environ.get("NS_REALM", "")
if not _realm_raw:
    raise ValueError("NS_REALM environment variable is required (e.g. '4874529-sb3')")
REALM = _realm_raw.upper().replace("-", "_")  # used in OAuth header only

# --- NetSuite API URLs (use original _realm_raw for the hostname) ---
BASE_URL = f"https://{_realm_raw}.suitetalk.api.netsuite.com/services/rest/record/v1"
SUITEQL_URL = (
    f"https://{_realm_raw}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"
)

# --- Data File Paths ---
SUBSCRIPTIONS_CSV = "data/subs-kleene-export-2026-07-15.csv"
BILLING_CSV = "data/billing-account-kleene-export-2026-07-15.csv"

CUSTOMERS_CSV = "data/customers-all-kleene-export-2026-07-09.csv"
PRICE_PLANS_CSV = "data/pricing-json-all-kleene-export-2026-07-09.csv"

ONEOFF_CSV = "data/one-offs-kleene-export-2026-05-07.csv"

# --- Load Revision ---
# Suffix appended to every externalId (and every parent-lookup ext_id) for the
# current load batch. Bump this string for each fresh re-load so new records
# land in NS with new externalIds rather than colliding with prior revisions.
#
# Convention: the very last token of every ext_id we POST is this string.
#   customer:        <External ID 2>_rvn_02
#   billingAccount:  <deal_id>_BA_rvn_02
#   pricePlan:       MP_PP_<line_id>_<slug>_rvn_02
#   subscription:    <deal_id>_rvn_02
#   oneOff invoice:  <Invoice External ID>_rvn_02
#   EER record:      <customer ext>_EER_rvn_02
#
# Old revisions (e.g. _rvn_01) remain in NS untouched. State DB rows for prior
# revisions are preserved for audit; new revision rows are added alongside.
LOAD_REVISION = "_rvn_prod_01"


def apply_revision(raw_ext_id: str) -> str:
    """Append LOAD_REVISION to a raw externalId. No-op for empty strings."""
    if not raw_ext_id:
        return raw_ext_id
    return f"{raw_ext_id}{LOAD_REVISION}"


# --- Customer Name Aliases ---
# The subscription / billing CSVs spell some customer names differently from the
# loaded NS customer `Company Name`. This is a REVIEWED mapping (not a silent
# guess): each key is a source spelling seen in a sub/BA `Customer` column, the
# value is the canonical customer-CSV `Company Name` it resolves to. Matching is
# case-insensitive. Used by validate.py (checks 1 & 2) and — once WS2 lands — by
# the loaders' customer-name resolution. Add an entry only after confirming the
# two names are genuinely the same customer.
CUSTOMER_NAME_ALIASES = {
    "B.& M.MCHUGH LIMITED": "B&M Mchugh Limited",
    "FOREST HEALTHCARE LTD": "Forest Healthcare Limited",
    "NATIONAL ASSOCIATION OF HEADTEACHERS": "Naht",
}


# --- State Tracking ---
# The state DB is environment-specific: it stores NetSuite *internal IDs* for
# every loaded record, and those IDs only mean anything in the account they
# came from. NEVER point a prod load at the sandbox DB (or vice-versa) — the
# sandbox internal IDs would be written into prod payloads as parent
# references (customer.id, billingAccount.id, pricePlan.id) and link records
# to the wrong entities. Override NS_STATE_DB alongside the NS_* creds so the
# DB always travels with the account it describes.
#   sandbox: NS_STATE_DB=state/load_state.db
#   prod:    NS_STATE_DB=state/load_state_prod.db
STATE_DB = os.environ.get("NS_STATE_DB", "")
if not STATE_DB:
    raise ValueError(
        "NS_STATE_DB environment variable is required (e.g. 'state/load_state.db')"
    )
# --- Retry / Rate Limit ---
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5
REQUEST_DELAY_SECONDS = 0.5
