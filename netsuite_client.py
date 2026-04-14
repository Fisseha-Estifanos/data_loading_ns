"""
NetSuite REST API Client
=========================
Handles OAuth 1.0 signing, record CRUD, and robust internal ID retrieval.
"""

import time
import json
import logging
import hashlib
import hmac
import base64
import urllib.parse
import uuid
from typing import Optional

import requests

import config

logger = logging.getLogger(__name__)


class NetSuiteClient:
    """REST client for NetSuite with OAuth 1.0 TBA and 3-tier ID retrieval."""

    def __init__(self):
        self.base_url = config.BASE_URL
        self.suiteql_url = config.SUITEQL_URL
        self.session = requests.Session()

    # ─── OAuth 1.0 Signature ────────────────────────────────────────────

    def _generate_oauth_header(self, method: str, url: str) -> str:
        """
        Build the OAuth 1.0 Authorization header using HMAC-SHA256.
        NetSuite TBA requires: consumer key/secret + token/secret + realm.
        """
        timestamp = str(int(time.time()))
        nonce = uuid.uuid4().hex

        oauth_params = {
            "oauth_consumer_key": config.CONSUMER_KEY,
            "oauth_token": config.ACCESS_TOKEN,
            "oauth_signature_method": "HMAC-SHA256",
            "oauth_timestamp": timestamp,
            "oauth_nonce": nonce,
            "oauth_version": "1.0",
        }

        # Build the signature base string
        # 1. Split URL into base + query params (both must be included per OAuth 1.0 spec)
        base_url_clean, _, query_string = url.partition("?")
        query_params = (
            dict(urllib.parse.parse_qsl(query_string)) if query_string else {}
        )

        # Merge OAuth params + request query params, then sort
        all_params = {**oauth_params, **query_params}
        param_string = "&".join(
            f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}"
            for k, v in sorted(all_params.items())
        )

        # 3. Signature base string
        base_string = "&".join(
            [
                method.upper(),
                urllib.parse.quote(base_url_clean, safe=""),
                urllib.parse.quote(param_string, safe=""),
            ]
        )

        # 4. Signing key
        signing_key = "&".join(
            [
                urllib.parse.quote(config.CONSUMER_SECRET, safe=""),
                urllib.parse.quote(config.TOKEN_SECRET, safe=""),
            ]
        )

        # 5. HMAC-SHA256 signature
        signature = base64.b64encode(
            hmac.new(
                signing_key.encode("utf-8"),
                base_string.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        ).decode("utf-8")

        # 6. Build header
        auth_header = (
            f'OAuth realm="{config.REALM}",'
            f'oauth_consumer_key="{config.CONSUMER_KEY}",'
            f'oauth_token="{config.ACCESS_TOKEN}",'
            f'oauth_signature_method="HMAC-SHA256",'
            f'oauth_timestamp="{timestamp}",'
            f'oauth_nonce="{nonce}",'
            f'oauth_version="1.0",'
            f'oauth_signature="{urllib.parse.quote(signature, safe="")}"'
        )
        return auth_header

    def _headers(self, method: str, url: str) -> dict:
        return {
            "Authorization": self._generate_oauth_header(method, url),
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Prefer": "transient",
        }

    # ─── Core HTTP Methods ──────────────────────────────────────────────

    def _request(
        self, method: str, url: str, payload: dict = None, retries: int = None
    ) -> requests.Response:
        """
        Execute an HTTP request with retry logic.
        Returns the raw Response object.
        """
        if retries is None:
            retries = config.MAX_RETRIES

        last_exc = None
        for attempt in range(1, retries + 1):
            try:
                headers = self._headers(method, url)
                resp = self.session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=payload if payload else None,
                    timeout=60,
                )

                # Log every response so NS feedback is always visible
                body_preview = resp.text[:2000] if resp.text else "(empty)"
                logger.info(
                    f"  → {method} {url.split('/v1/')[-1]} "
                    f"| HTTP {resp.status_code} "
                    f"| {body_preview}"
                )

                # Rate limiting: 429
                if resp.status_code == 429:
                    wait = config.RETRY_BACKOFF_SECONDS * attempt
                    logger.warning(
                        f"Rate limited (429). Waiting {wait}s before retry {attempt}/{retries}"
                    )
                    time.sleep(wait)
                    continue

                # Server errors: retry
                if resp.status_code >= 500:
                    wait = config.RETRY_BACKOFF_SECONDS * attempt
                    logger.warning(
                        f"Server error {resp.status_code}. Retry {attempt}/{retries} in {wait}s"
                    )
                    time.sleep(wait)
                    continue

                return resp

            except requests.exceptions.RequestException as e:
                last_exc = e
                wait = config.RETRY_BACKOFF_SECONDS * attempt
                logger.warning(
                    f"Request exception: {e}. Retry {attempt}/{retries} in {wait}s"
                )
                time.sleep(wait)

        # All retries exhausted
        raise ConnectionError(f"All {retries} retries failed. Last error: {last_exc}")

    # ─── Record Operations ──────────────────────────────────────────────

    def create_record(self, record_type: str, payload: dict) -> requests.Response:
        """POST a new record. Returns raw response for ID extraction."""
        url = f"{self.base_url}/{record_type}"
        time.sleep(config.REQUEST_DELAY_SECONDS)
        resp = self._request("POST", url, payload)
        return resp

    def get_record(self, record_type: str, internal_id: str) -> Optional[dict]:
        """GET a record by internal ID."""
        url = f"{self.base_url}/{record_type}/{internal_id}"
        resp = self._request("GET", url)
        if resp.status_code == 200:
            return resp.json()
        return None

    def get_by_external_id(self, record_type: str, external_id: str) -> Optional[dict]:
        """GET a record by externalId. Returns the record dict or None."""
        # NetSuite REST API: use query parameter q= with SuiteQL-style filter
        # Or use the eid: prefix for direct lookup
        url = f"{self.base_url}/{record_type}/eid:{urllib.parse.quote(external_id, safe='')}"
        time.sleep(config.REQUEST_DELAY_SECONDS)
        resp = self._request("GET", url)
        if resp.status_code == 200:
            return resp.json()
        logger.debug(
            f"GET by externalId returned {resp.status_code} for {record_type} eid:{external_id}"
        )
        return None

    def suiteql_query(self, query: str, page_size: int = 1000) -> list:
        """
        Execute a SuiteQL query. Automatically paginates until hasMore = false.
        Returns all result rows across all pages.
        """
        all_items = []
        offset = 0

        while True:
            time.sleep(config.REQUEST_DELAY_SECONDS)
            url = f"{self.suiteql_url}?limit={page_size}&offset={offset}"
            headers = self._headers("POST", url)
            headers["Prefer"] = "transient"
            resp = self.session.post(
                url,
                headers=headers,
                json={"q": query},
                timeout=60,
            )

            if resp.status_code != 200:
                logger.warning(f"SuiteQL query failed ({resp.status_code}): {resp.text[:500]}")
                break

            data = resp.json()
            items = data.get("items", [])
            all_items.extend(items)

            total = data.get("totalResults", len(all_items))
            has_more = data.get("hasMore", False)
            logger.debug(
                f"SuiteQL page offset={offset}: {len(items)} rows "
                f"(total={total}, hasMore={has_more})"
            )

            if not has_more:
                break
            offset += page_size

        return all_items

    # ─── 3-Tier Internal ID Retrieval ───────────────────────────────────

    def extract_id_from_response(self, resp: requests.Response) -> Optional[str]:
        """
        Tier 1: Extract internal ID from the POST response.
        NetSuite returns 204 with a Location header like:
          https://.../{record_type}/{internal_id}
        """
        # Check Location header first
        location = resp.headers.get("Location", "")
        if location:
            # Internal ID is the last path segment
            internal_id = location.rstrip("/").split("/")[-1]
            if internal_id.isdigit():
                logger.info(
                    f"Tier 1 success: extracted ID {internal_id} from Location header"
                )
                return internal_id

        # Check response body if present
        if resp.content:
            try:
                body = resp.json()
                if "id" in body:
                    logger.info(
                        f"Tier 1 success: extracted ID {body['id']} from response body"
                    )
                    return str(body["id"])
            except (json.JSONDecodeError, KeyError):
                pass

        logger.warning("Tier 1 failed: no ID in Location header or response body")
        return None

    def retrieve_id_by_external_id(
        self, record_type: str, external_id: str
    ) -> Optional[str]:
        """
        Tier 2: Look up the record by externalId via GET.
        """
        record = self.get_by_external_id(record_type, external_id)
        if record and "id" in record:
            logger.info(
                f"Tier 2 success: found {record_type} ID {record['id']} via externalId '{external_id}'"
            )
            return str(record["id"])
        logger.warning(
            f"Tier 2 failed: no {record_type} found for externalId '{external_id}'"
        )
        return None

    def retrieve_id_by_suiteql(
        self, record_type: str, field_name: str, field_value: str
    ) -> Optional[str]:
        """
        Tier 3: Last resort — search by a unique business field using SuiteQL.
        """
        query = f"SELECT id FROM {record_type} WHERE {field_name} = '{field_value}'"
        results = self.suiteql_query(query)
        if results:
            ns_id = str(results[0].get("id"))
            logger.info(
                f"Tier 3 success: found {record_type} ID {ns_id} via SuiteQL ({field_name}='{field_value}')"
            )
            return ns_id
        logger.warning(
            f"Tier 3 failed: SuiteQL found no {record_type} for {field_name}='{field_value}'"
        )
        return None

    def create_and_resolve_id(
        self,
        record_type: str,
        payload: dict,
        external_id: str,
        tier3_field: str = None,
        tier3_value: str = None,
    ) -> tuple[str, Optional[str], Optional[str]]:
        """
        Create a record and resolve its internal ID using the 3-tier strategy.

        Returns:
            (status, netsuite_id, error_message)
            status: 'success' | 'failed'
        """
        try:
            resp = self.create_record(record_type, payload)
        except ConnectionError as e:
            return ("failed", None, str(e))

        # Check for HTTP success (200, 201, 204)
        if resp.status_code not in (200, 201, 204):
            # Special case: NS says the record already exists (e.g. from a prior async 202 run).
            # Treat as a successful create — look up the existing record's ID via Tier 2/3.
            if resp.status_code == 400 and "already exists" in resp.text:
                logger.warning(
                    f"{record_type} '{external_id}' already exists in NS — looking up existing ID"
                )
                ns_id = self.retrieve_id_by_external_id(record_type, external_id)
                if ns_id:
                    return ("success", ns_id, None)
                if tier3_field and tier3_value:
                    ns_id = self.retrieve_id_by_suiteql(record_type, tier3_field, tier3_value)
                    if ns_id:
                        return ("success", ns_id, None)
                return ("success_no_id", None, "Record exists in NS but ID could not be resolved")

            error_msg = f"HTTP {resp.status_code}: {resp.text[:1000]}"
            logger.error(f"Create {record_type} failed: {error_msg}")
            return ("failed", None, error_msg)

        # Tier 1
        ns_id = self.extract_id_from_response(resp)
        if ns_id:
            return ("success", ns_id, None)

        # Tier 2
        ns_id = self.retrieve_id_by_external_id(record_type, external_id)
        if ns_id:
            return ("success", ns_id, None)

        # Tier 3
        if tier3_field and tier3_value:
            ns_id = self.retrieve_id_by_suiteql(record_type, tier3_field, tier3_value)
            if ns_id:
                return ("success", ns_id, None)

        # Record was created (2xx) but we can't find its ID — flag for manual review
        return (
            "success_no_id",
            None,
            "Record created (2xx) but internal ID could not be resolved via any tier",
        )
