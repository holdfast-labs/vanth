"""Client-side request journal (Phase 4).

Durable record of controller-initiated remote requests in
``client-requests.sqlite`` under the vanth home. The journal is OPTIONAL:
:class:`~vanth.remote.control.RemoteControl` accepts ``journal=`` and only
writes when one is attached, so existing call sites and tests are unchanged.

- ``submit`` journals every request it creates as ``pending``.
- ``run_request`` marks the entry ``resolved`` when the request reaches a
  terminal state (``completed``/``failed``); a lost/``submitting`` request
  stays ``pending`` and shows up in ``vanth remote pending`` for retry with
  the ORIGINAL idempotency key (replay-safe by construction).
- ``UNIQUE(remote_id, idempotency_key)`` mirrors the durable-request
  identity, so re-recording a replayed request cannot create duplicates.

Thread safety: the daemon's HTTP handler threads and CLI processes share the
connection, so every access is serialized behind an RLock and failures inside
journal writes never break the request itself (review rc14 P1-5).
"""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any

from ..server import now_iso

JOURNAL_DDL = """
CREATE TABLE IF NOT EXISTS client_requests (
  request_id TEXT PRIMARY KEY,
  remote_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  method TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  digest TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  expected_state_epoch INTEGER,
  expected_instance_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(remote_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_client_requests_status ON client_requests(status, updated_at);
"""


class RequestJournal:
    """Durable journal of client-initiated remote requests."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(self.path), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=30000")
        self.db.executescript(JOURNAL_DDL)
        self._ensure_columns()
        self.db.commit()
        self._lock = threading.RLock()

    def _ensure_columns(self) -> None:
        existing = {row[1] for row in self.db.execute("PRAGMA table_info(client_requests)").fetchall()}
        if "expected_state_epoch" not in existing:
            self.db.execute("ALTER TABLE client_requests ADD COLUMN expected_state_epoch INTEGER")
        if "expected_instance_id" not in existing:
            self.db.execute("ALTER TABLE client_requests ADD COLUMN expected_instance_id TEXT")

    # -- writes (called by RemoteControl; failures must never break requests) --

    def record(self, request: dict[str, Any]) -> None:
        """Journal one durable request as pending (idempotent on replay)."""
        stamp = now_iso()
        epoch = request.get("expected_state_epoch")
        instance_id = request.get("expected_instance_id")
        with self._lock:
            try:
                self.db.execute(
                    """
                    INSERT INTO client_requests(request_id, remote_id, idempotency_key, method,
                      payload_json, digest, status, expected_state_epoch, expected_instance_id, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?)
                    ON CONFLICT(request_id) DO NOTHING
                    """,
                    (
                        request["request_id"], request["remote_id"], request["idempotency_key"],
                        request["method"],
                        json.dumps(request.get("payload"), separators=(",", ":")),
                        request["digest"], epoch, instance_id, stamp, stamp,
                    ),
                )
                self.db.commit()
            except BaseException:
                # A failed commit (disk full, busy) must not leave an implicit
                # transaction open for the next journal write to inherit.
                self.db.rollback()
                raise

    def mark_resolved(self, request_id: str) -> None:
        """Mark an entry resolved after a terminal outcome."""
        stamp = now_iso()
        with self._lock:
            try:
                self.db.execute(
                    "UPDATE client_requests SET status='resolved', updated_at=? WHERE request_id=?",
                    (stamp, request_id),
                )
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise

    def prune_resolved(self, older_than_seconds: int) -> int:
        """Delete resolved entries past a TTL.

        Pending entries are kept — they are the retry handles `vanth remote
        pending` surfaces. The poll loop journals a fresh request per tick, so
        resolved entries would otherwise accumulate forever.
        """
        if older_than_seconds <= 0:
            return 0
        from datetime import datetime, timedelta, timezone

        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=older_than_seconds)).isoformat().replace("+00:00", "Z")
        with self._lock:
            try:
                cursor = self.db.execute(
                    "DELETE FROM client_requests WHERE status='resolved' AND updated_at <= ?", (cutoff,)
                )
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise
        return cursor.rowcount

    # -- reads (CLI) -------------------------------------------------------------

    def pending(self, remote_id: str | None = None) -> list[dict[str, Any]]:
        """Unresolved requests, optionally filtered by remote."""
        if remote_id:
            query = ("SELECT * FROM client_requests WHERE status='pending' AND remote_id=? "
                     "ORDER BY created_at ASC")
            args: tuple[Any, ...] = (remote_id,)
        else:
            query = "SELECT * FROM client_requests WHERE status='pending' ORDER BY created_at ASC"
            args = ()
        with self._lock:
            rows = self.db.execute(query, args).fetchall()
        return [self._entry_dict(row) for row in rows]

    def get(self, request_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self.db.execute(
                "SELECT * FROM client_requests WHERE request_id=?", (request_id,)
            ).fetchone()
        return self._entry_dict(row) if row else None

    @staticmethod
    def _entry_dict(row) -> dict[str, Any]:
        result = dict(row)
        payload = result.pop("payload_json", None)
        result["payload"] = json.loads(payload) if payload is not None else None
        return result

    def close(self) -> None:
        self.db.close()
