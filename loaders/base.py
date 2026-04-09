"""
Base Loader
============
Abstract base class for all entity loaders.
"""

import csv
import hashlib
import json
import logging
from abc import ABC, abstractmethod
from typing import Optional

from netsuite_client import NetSuiteClient
from state_tracker import StateTracker

logger = logging.getLogger(__name__)


class BaseLoader(ABC):
    """Abstract base for entity loaders."""

    ENTITY_TYPE: str = ""  # Override in subclass: 'customer', 'billingAccount', etc.
    RECORD_TYPE: str = ""  # NetSuite REST record type name
    CSV_PATH: str = ""  # Path to source CSV

    def __init__(self, client: NetSuiteClient, tracker: StateTracker):
        self.client = client
        self.tracker = tracker

    # ─── Subclass must implement ────────────────────────────────────────

    @abstractmethod
    def get_external_id(self, row: dict) -> str:
        """Extract the external ID from a CSV row."""
        ...

    @abstractmethod
    def build_payload(self, row: dict) -> Optional[dict]:
        """
        Transform a CSV row (or group of rows) into a NetSuite JSON payload.
        Return None to skip the record (e.g., missing required data).
        """
        ...

    @abstractmethod
    def get_tier3_field(self) -> Optional[str]:
        """SuiteQL field name for Tier 3 fallback lookup. None if not applicable."""
        return None

    @abstractmethod
    def get_tier3_value(self, row: dict) -> Optional[str]:
        """SuiteQL field value for Tier 3 fallback lookup."""
        return None

    # ─── CSV Reading ────────────────────────────────────────────────────

    def read_csv(self) -> list[dict]:
        """Read the source CSV into a list of dicts."""
        with open(self.CSV_PATH, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            return list(reader)

    # ─── Payload Hashing (change detection) ─────────────────────────────

    @staticmethod
    def hash_payload(payload: dict) -> str:
        raw = json.dumps(payload, sort_keys=True, default=str)
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    # ─── Load Orchestration ─────────────────────────────────────────────

    def load_all(self, limit: int = None) -> dict:
        """
        Main entry point. Reads CSV, builds payloads, creates records,
        tracks state, and returns a summary.

        Args:
            limit: If set, process only the first N records (useful for testing).
        """
        records = self.prepare_records()
        if limit is not None:
            records = records[:limit]
            logger.info(f"--limit {limit}: processing {len(records)} record(s)")
        run_id = self.tracker.start_run(self.ENTITY_TYPE)

        total = len(records)
        success = 0
        failed = 0
        skipped = 0

        logger.info(f"=== Loading {self.ENTITY_TYPE}: {total} records ===")

        for i, (ext_id, payload, row) in enumerate(records, 1):
            # Skip if already loaded
            if self.tracker.is_already_loaded(self.ENTITY_TYPE, ext_id):
                logger.info(f"[{i}/{total}] SKIP {ext_id} (already loaded)")
                skipped += 1
                continue

            logger.info(f"[{i}/{total}] Creating {self.ENTITY_TYPE}: {ext_id}")

            status, ns_id, error = self.client.create_and_resolve_id(
                record_type=self.RECORD_TYPE,
                payload=payload,
                external_id=ext_id,
                tier3_field=self.get_tier3_field(),
                tier3_value=self.get_tier3_value(row) if row else None,
            )

            # Determine which tier resolved the ID
            tier_used = None
            if ns_id and status == "success":
                tier_used = "tier1_or_2_or_3"  # Simplified; client logs the exact tier

            self.tracker.upsert_state(
                entity_type=self.ENTITY_TYPE,
                external_id=ext_id,
                status=status,
                netsuite_id=ns_id,
                error_message=error,
                payload_hash=self.hash_payload(payload),
                tier_used=tier_used,
            )

            if status in ("success", "success_no_id"):
                success += 1
                if status == "success_no_id":
                    logger.warning(f"  ⚠ Record created but ID not resolved: {ext_id}")
            else:
                failed += 1
                logger.error(f"  ✗ Failed: {error}")

        self.tracker.finish_run(run_id, total, success, failed, skipped)

        summary = {
            "total": total,
            "success": success,
            "failed": failed,
            "skipped": skipped,
        }
        logger.info(f"=== {self.ENTITY_TYPE} complete: {summary} ===")
        return summary

    def prepare_records(self) -> list[tuple[str, dict, dict]]:
        """
        Read CSV and build payloads. Returns list of (external_id, payload, raw_row).
        Override this in subclasses that need grouping (e.g., subscriptions).
        """
        rows = self.read_csv()
        records = []
        for row in rows:
            ext_id = self.get_external_id(row)
            if not ext_id:
                logger.warning(f"Skipping row with no external ID: {row}")
                continue
            payload = self.build_payload(row)
            if payload is None:
                logger.warning(f"Skipping {ext_id}: build_payload returned None")
                continue
            records.append((ext_id, payload, row))
        return records
