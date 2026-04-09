"""
State Tracker
==============
SQLite-backed persistence for tracking load status, NetSuite internal IDs,
and enabling idempotent re-runs with relationship chaining.
"""
import os
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional

import config

logger = logging.getLogger(__name__)


class StateTracker:
    """Tracks load state per record across runs."""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or config.STATE_DB
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS load_state (
                entity_type   TEXT    NOT NULL,
                external_id   TEXT    NOT NULL,
                netsuite_id   TEXT,
                status        TEXT    NOT NULL DEFAULT 'pending',
                error_message TEXT,
                payload_hash  TEXT,
                tier_used     TEXT,
                attempted_at  TEXT,
                created_at    TEXT    DEFAULT (datetime('now')),
                PRIMARY KEY (entity_type, external_id)
            );

            CREATE INDEX IF NOT EXISTS idx_load_state_status
                ON load_state(entity_type, status);

            CREATE TABLE IF NOT EXISTS run_log (
                run_id        INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type   TEXT,
                started_at    TEXT,
                finished_at   TEXT,
                total         INTEGER,
                success       INTEGER,
                failed        INTEGER,
                skipped       INTEGER
            );
        """)
        self.conn.commit()

    # ─── Record State ───────────────────────────────────────────────────

    def get_status(self, entity_type: str, external_id: str) -> Optional[dict]:
        """Get the current state of a record. Returns dict or None."""
        row = self.conn.execute(
            "SELECT * FROM load_state WHERE entity_type = ? AND external_id = ?",
            (entity_type, external_id),
        ).fetchone()
        return dict(row) if row else None

    def get_netsuite_id(self, entity_type: str, external_id: str) -> Optional[str]:
        """Look up the NetSuite internal ID for a previously loaded record."""
        row = self.conn.execute(
            "SELECT netsuite_id FROM load_state WHERE entity_type = ? AND external_id = ? AND netsuite_id IS NOT NULL",
            (entity_type, external_id),
        ).fetchone()
        return row["netsuite_id"] if row else None

    def is_already_loaded(self, entity_type: str, external_id: str) -> bool:
        """Check if this record was already successfully loaded."""
        row = self.conn.execute(
            "SELECT status FROM load_state WHERE entity_type = ? AND external_id = ?",
            (entity_type, external_id),
        ).fetchone()
        return row is not None and row["status"] in ("success", "success_no_id")

    def upsert_state(
        self,
        entity_type: str,
        external_id: str,
        status: str,
        netsuite_id: str = None,
        error_message: str = None,
        payload_hash: str = None,
        tier_used: str = None,
    ):
        """Insert or update a record's load state."""
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """
            INSERT INTO load_state (entity_type, external_id, netsuite_id, status, error_message, payload_hash, tier_used, attempted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(entity_type, external_id) DO UPDATE SET
                netsuite_id   = COALESCE(excluded.netsuite_id, load_state.netsuite_id),
                status        = excluded.status,
                error_message = excluded.error_message,
                payload_hash  = excluded.payload_hash,
                tier_used     = COALESCE(excluded.tier_used, load_state.tier_used),
                attempted_at  = excluded.attempted_at
            """,
            (entity_type, external_id, netsuite_id, status, error_message, payload_hash, tier_used, now),
        )
        self.conn.commit()

    # ─── Run Logging ────────────────────────────────────────────────────

    def start_run(self, entity_type: str) -> int:
        cursor = self.conn.execute(
            "INSERT INTO run_log (entity_type, started_at) VALUES (?, ?)",
            (entity_type, datetime.now(timezone.utc).isoformat()),
        )
        self.conn.commit()
        return cursor.lastrowid

    def finish_run(self, run_id: int, total: int, success: int, failed: int, skipped: int):
        self.conn.execute(
            "UPDATE run_log SET finished_at = ?, total = ?, success = ?, failed = ?, skipped = ? WHERE run_id = ?",
            (datetime.now(timezone.utc).isoformat(), total, success, failed, skipped, run_id),
        )
        self.conn.commit()

    # ─── Reporting ──────────────────────────────────────────────────────

    def summary(self, entity_type: str) -> dict:
        """Get counts by status for an entity type."""
        rows = self.conn.execute(
            "SELECT status, COUNT(*) as cnt FROM load_state WHERE entity_type = ? GROUP BY status",
            (entity_type,),
        ).fetchall()
        return {row["status"]: row["cnt"] for row in rows}

    def get_failed(self, entity_type: str) -> list[dict]:
        """Get all failed records for an entity type."""
        rows = self.conn.execute(
            "SELECT * FROM load_state WHERE entity_type = ? AND status = 'failed' ORDER BY attempted_at",
            (entity_type,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_missing_ids(self, entity_type: str) -> list[dict]:
        """Get records that were created but whose NS ID couldn't be resolved."""
        rows = self.conn.execute(
            "SELECT * FROM load_state WHERE entity_type = ? AND status = 'success_no_id'",
            (entity_type,),
        ).fetchall()
        return [dict(r) for r in rows]

    def close(self):
        self.conn.close()
