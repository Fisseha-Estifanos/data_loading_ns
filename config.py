"""
NetSuite REST API Configuration
================================
Fill in your OAuth 1.0 credentials below.
All secrets should ideally come from environment variables in production.
"""
import os

# --- NetSuite Account ---
ACCOUNT_ID = "4874529-sb3"
BASE_URL = f"https://{ACCOUNT_ID}.suitetalk.api.netsuite.com/services/rest/record/v1"
SUITEQL_URL = f"https://{ACCOUNT_ID}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"

# --- OAuth 1.0 Credentials ---
# Replace these with your actual values, or set them as environment variables.
CONSUMER_KEY = os.environ.get("NS_CONSUMER_KEY", "YOUR_CONSUMER_KEY")
CONSUMER_SECRET = os.environ.get("NS_CONSUMER_SECRET", "YOUR_CONSUMER_SECRET")
ACCESS_TOKEN = os.environ.get("NS_ACCESS_TOKEN", "YOUR_ACCESS_TOKEN")
TOKEN_SECRET = os.environ.get("NS_TOKEN_SECRET", "YOUR_TOKEN_SECRET")
REALM = os.environ.get("NS_REALM", ACCOUNT_ID)

# --- Data File Paths ---
CUSTOMERS_CSV = "data/customers-kleene-export-2026-04-09.csv"
BILLING_CSV = "data/billingkleeneexport20260409.csv"
SUBSCRIPTIONS_CSV = "data/subscriptionskleeneexport20260409.csv"
ONEOFF_CSV = "data/oneoffkleeneexport20260409.csv"

# --- State Tracking ---
STATE_DB = "state/load_state.db"

# --- Retry / Rate Limit ---
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5
REQUEST_DELAY_SECONDS = 0.5  # Delay between API calls to avoid rate limits
